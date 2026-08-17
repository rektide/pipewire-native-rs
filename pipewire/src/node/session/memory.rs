// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: Copyright (c) 2026 Asymptotic Inc.

//! Adapts Core memory events to a ClientNode session memory pool.

use std::{
    collections::{BTreeMap, VecDeque},
    fmt, io,
    os::fd::OwnedFd,
    sync::{Arc, Weak},
};

use parking_lot::Mutex;
use pipewire_native_node::session::memory::{MemoryLease, MemoryPool, MemoryResolver};

use crate::{
    core::{Core, CoreMemoryImporter},
    Id,
};

pub use pipewire_native_node::{
    session::memory::{MemoryError, MemoryId, MemoryKey, MemoryMapping, RegionRef},
    shm::ShrinkPolicy,
};

/// Session-facing access to memory imported by one Core connection.
///
/// Operations lock the pool only for their duration and never invoke Core callbacks. Mappings
/// retain their exact imported generation after the lock is released.
#[derive(Clone)]
pub struct MemoryPoolHandle {
    inner: Arc<MemoryPoolInner>,
}

impl fmt::Debug for MemoryPoolHandle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MemoryPoolHandle")
            .field("active", &self.len())
            .finish_non_exhaustive()
    }
}

struct MemoryPoolInner {
    // When both locks are needed, always acquire `pool` before `events`. Event callbacks run only
    // after both locks are released. This makes pool mutations and subscription snapshots one
    // ordered stream without allowing callbacks to reenter either critical section.
    pool: Mutex<MemoryPool>,
    events: Mutex<MemoryEventDispatch>,
}

/// A successful Core memory-pool mutation delivered after the pool lock is released.
#[derive(Clone, Debug)]
pub(crate) enum MemoryPoolEvent {
    /// A numeric ID now resolves to this exact generation.
    Available(MemoryLease),
    /// This exact generation was retired and can no longer be resolved.
    Removed(MemoryKey),
    /// The Core importer was replaced or the connection disconnected.
    Disconnected,
}

type MemoryPoolEventHandler = Box<dyn FnMut(MemoryPoolEvent) + Send>;

#[derive(Default)]
struct MemoryEventDispatch {
    next_id: u64,
    subscribers: BTreeMap<u64, Option<MemoryPoolEventHandler>>,
    queued: VecDeque<QueuedMemoryEvent>,
    dispatching: bool,
    #[cfg(test)]
    quiescence_barriers: Option<(Arc<std::sync::Barrier>, Arc<std::sync::Barrier>)>,
}

struct QueuedMemoryEvent {
    event: MemoryPoolEvent,
    subscribers: Vec<u64>,
}

/// One independently removable internal memory lifecycle subscription.
pub(crate) struct MemoryPoolSubscription {
    inner: Weak<MemoryPoolInner>,
    id: u64,
}

impl Drop for MemoryPoolSubscription {
    fn drop(&mut self) {
        let Some(inner) = self.inner.upgrade() else {
            return;
        };
        let callback = inner.events.lock().subscribers.remove(&self.id).flatten();
        drop(callback);
    }
}

impl MemoryPoolHandle {
    /// Creates a pool, installs its private importer into `core`, and returns session access.
    ///
    /// Replacing the Core importer or disconnecting the Core permanently disconnects this pool.
    pub fn install(core: &Core, shrink_policy: ShrinkPolicy) -> Self {
        let inner = Arc::new(MemoryPoolInner {
            pool: Mutex::new(MemoryPool::new(shrink_policy)),
            events: Mutex::new(MemoryEventDispatch::default()),
        });
        core.set_memory_importer(Some(Box::new(MemoryPoolImporter {
            inner: Arc::clone(&inner),
        })));
        Self { inner }
    }

    /// Adds a subscription and atomically queues exact leases for every active generation.
    ///
    /// The replay precedes every lifecycle event linearized after this subscription.
    pub(crate) fn subscribe(&self, handler: MemoryPoolEventHandler) -> MemoryPoolSubscription {
        let (id, dispatch) = {
            // Lock order is pool -> events; importer mutations use the same order.
            let pool = self.inner.pool.lock();
            let mut events = self.inner.events.lock();
            let id = events.next_id;
            events.next_id = events
                .next_id
                .checked_add(1)
                .expect("memory subscription IDs exhausted");
            events.subscribers.insert(id, Some(handler));
            let mut keys = pool.active_keys();
            keys.sort_unstable_by_key(|key| (key.id.0, key.generation));
            for key in keys {
                let lease = pool
                    .lease(key)
                    .expect("active key must retain its exact lease");
                events.queued.push_back(QueuedMemoryEvent {
                    event: MemoryPoolEvent::Available(lease),
                    subscribers: vec![id],
                });
            }
            let dispatch = claim_dispatch(&mut events);
            (id, dispatch)
        };
        if dispatch {
            dispatch_events(&self.inner);
        }
        MemoryPoolSubscription {
            inner: Arc::downgrade(&self.inner),
            id,
        }
    }

    /// Returns the current generation key for an active Core memory ID.
    pub fn resolve(&self, id: MemoryId) -> Result<MemoryKey, MemoryError> {
        self.inner.pool.lock().resolve(id)
    }

    /// Resolves and maps a checked region against the current active generation.
    pub fn bind(&self, region: RegionRef, writable: bool) -> Result<MemoryMapping, MemoryError> {
        self.inner.pool.lock().bind(region, writable)
    }

    /// Maps a checked region only if `key` is still the active generation.
    pub fn map(
        &self,
        key: MemoryKey,
        offset: usize,
        len: usize,
        writable: bool,
    ) -> Result<MemoryMapping, MemoryError> {
        self.inner.pool.lock().map(key, offset, len, writable)
    }

    /// Returns the number of active Core memory IDs.
    pub fn len(&self) -> usize {
        self.inner.pool.lock().len()
    }

    /// Returns whether there are no active Core memory IDs.
    pub fn is_empty(&self) -> bool {
        self.inner.pool.lock().is_empty()
    }
}

impl MemoryResolver for MemoryPoolHandle {
    fn resolve(&self, id: MemoryId) -> Result<MemoryKey, MemoryError> {
        MemoryPoolHandle::resolve(self, id)
    }

    fn map(
        &self,
        key: MemoryKey,
        offset: usize,
        len: usize,
        writable: bool,
    ) -> Result<MemoryMapping, MemoryError> {
        MemoryPoolHandle::map(self, key, offset, len, writable)
    }
}

struct MemoryPoolImporter {
    inner: Arc<MemoryPoolInner>,
}

impl CoreMemoryImporter for MemoryPoolImporter {
    fn add_memory(&mut self, id: Id, type_: u32, fd: OwnedFd, flags: u32) -> io::Result<()> {
        let dispatch = {
            let mut pool = self.inner.pool.lock();
            let key = pool
                .add(MemoryId(id), type_, flags, fd)
                .map_err(import_error)?;
            let lease = pool.lease(key).map_err(import_error)?;
            queue_broadcast(
                &mut self.inner.events.lock(),
                MemoryPoolEvent::Available(lease),
            )
        };
        if dispatch {
            dispatch_events(&self.inner);
        }
        Ok(())
    }

    fn remove_memory(&mut self, id: Id) -> io::Result<()> {
        let dispatch = {
            let mut pool = self.inner.pool.lock();
            let key = pool.remove(MemoryId(id)).map_err(import_error)?;
            queue_broadcast(&mut self.inner.events.lock(), MemoryPoolEvent::Removed(key))
        };
        if dispatch {
            dispatch_events(&self.inner);
        }
        Ok(())
    }
}

impl Drop for MemoryPoolImporter {
    fn drop(&mut self) {
        let dispatch = {
            let mut pool = self.inner.pool.lock();
            let keys = pool.active_keys();
            pool.disconnect();
            let mut events = self.inner.events.lock();
            for key in keys {
                queue_broadcast_while_dispatching(&mut events, MemoryPoolEvent::Removed(key));
            }
            queue_broadcast_while_dispatching(&mut events, MemoryPoolEvent::Disconnected);
            claim_dispatch(&mut events)
        };
        if dispatch {
            dispatch_events(&self.inner);
        }
    }
}

#[cfg(test)]
fn notify(inner: &MemoryPoolInner, event: MemoryPoolEvent) {
    let dispatch = queue_broadcast(&mut inner.events.lock(), event);
    if dispatch {
        dispatch_events(inner);
    }
}

fn queue_broadcast(events: &mut MemoryEventDispatch, event: MemoryPoolEvent) -> bool {
    queue_broadcast_while_dispatching(events, event);
    claim_dispatch(events)
}

fn queue_broadcast_while_dispatching(events: &mut MemoryEventDispatch, event: MemoryPoolEvent) {
    events.queued.push_back(QueuedMemoryEvent {
        event,
        subscribers: events.subscribers.keys().copied().collect(),
    });
}

fn claim_dispatch(events: &mut MemoryEventDispatch) -> bool {
    if events.dispatching || events.queued.is_empty() {
        false
    } else {
        events.dispatching = true;
        true
    }
}

fn dispatch_events(inner: &MemoryPoolInner) {
    let mut dispatch = DispatchGuard { inner, armed: true };

    loop {
        let (event, subscribers) = {
            let mut events = inner.events.lock();
            let Some(queued) = events.queued.pop_front() else {
                #[cfg(test)]
                if let Some((reached, release)) = events.quiescence_barriers.take() {
                    reached.wait();
                    release.wait();
                }
                // Queue emptiness and dispatcher ownership release are one atomic state change.
                events.dispatching = false;
                dispatch.armed = false;
                return;
            };
            (queued.event, queued.subscribers)
        };
        for id in subscribers {
            let callback = inner
                .events
                .lock()
                .subscribers
                .get_mut(&id)
                .and_then(Option::take);
            let Some(callback) = callback else { continue };
            let mut active = ActiveSubscription {
                inner,
                id,
                callback: Some(callback),
            };
            active.callback.as_mut().expect("active callback")(event.clone());
        }
    }
}

struct DispatchGuard<'a> {
    inner: &'a MemoryPoolInner,
    armed: bool,
}

impl Drop for DispatchGuard<'_> {
    fn drop(&mut self) {
        if self.armed {
            self.inner.events.lock().dispatching = false;
        }
    }
}

struct ActiveSubscription<'a> {
    inner: &'a MemoryPoolInner,
    id: u64,
    callback: Option<MemoryPoolEventHandler>,
}

impl Drop for ActiveSubscription<'_> {
    fn drop(&mut self) {
        let callback = {
            let mut events = self.inner.events.lock();
            if let Some(slot) = events.subscribers.get_mut(&self.id) {
                if slot.is_none() {
                    *slot = self.callback.take();
                    None
                } else {
                    self.callback.take()
                }
            } else {
                self.callback.take()
            }
        };
        drop(callback);
    }
}

fn import_error(error: MemoryError) -> io::Error {
    let kind = match &error {
        MemoryError::DuplicateActiveId(_) => io::ErrorKind::AlreadyExists,
        MemoryError::UnknownMemory(_) | MemoryError::StaleGeneration { .. } => {
            io::ErrorKind::NotFound
        }
        // `Unsupported` is the protocol dispatcher's unknown-opcode sentinel and is non-terminal.
        MemoryError::UnknownMemoryType(_) | MemoryError::UnsupportedMemoryType(_) => {
            io::ErrorKind::InvalidData
        }
        MemoryError::Disconnected => io::ErrorKind::NotConnected,
        MemoryError::Inspect(source) => source.kind(),
        MemoryError::InvalidRegion { .. }
        | MemoryError::ShrinkableMemory(_)
        | MemoryError::Map { .. } => io::ErrorKind::InvalidData,
        MemoryError::GenerationExhausted => io::ErrorKind::Other,
    };
    io::Error::new(kind, error)
}

#[cfg(test)]
mod tests {
    use std::{
        os::fd::{AsRawFd, RawFd},
        sync::{Barrier, Mutex as StdMutex},
    };

    use pipewire_native_node::{session::memory::MemoryPool, shm::create_memfd};
    use pipewire_native_spa::buffer::data_type;

    use super::*;

    fn fd_identity(fd: RawFd) -> (libc::dev_t, libc::ino_t) {
        let mut stat = std::mem::MaybeUninit::<libc::stat>::uninit();
        assert_eq!(unsafe { libc::fstat(fd, stat.as_mut_ptr()) }, 0);
        let stat = unsafe { stat.assume_init() };
        (stat.st_dev, stat.st_ino)
    }

    fn fd_identity_is_open(fd: RawFd, identity: (libc::dev_t, libc::ino_t)) -> bool {
        let mut stat = std::mem::MaybeUninit::<libc::stat>::uninit();
        (unsafe { libc::fstat(fd, stat.as_mut_ptr()) == 0 }) && {
            let stat = unsafe { stat.assume_init() };
            (stat.st_dev, stat.st_ino) == identity
        }
    }

    fn inner() -> Arc<MemoryPoolInner> {
        Arc::new(MemoryPoolInner {
            pool: Mutex::new(MemoryPool::new(ShrinkPolicy::Allow)),
            events: Mutex::new(MemoryEventDispatch::default()),
        })
    }

    #[test]
    fn importer_errors_preserve_pool_error_and_close_candidate_fd() {
        let inner = inner();
        let mut importer = MemoryPoolImporter {
            inner: Arc::clone(&inner),
        };
        importer
            .add_memory(7, data_type::MEM_FD, create_memfd("active", 64).unwrap(), 3)
            .unwrap();

        let duplicate = create_memfd("duplicate", 64).unwrap();
        let duplicate_raw = duplicate.as_raw_fd();
        let duplicate_identity = fd_identity(duplicate_raw);
        let error = importer
            .add_memory(7, data_type::MEM_FD, duplicate, 0)
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::AlreadyExists);
        assert!(matches!(
            error
                .get_ref()
                .and_then(|error| error.downcast_ref::<MemoryError>()),
            Some(MemoryError::DuplicateActiveId(MemoryId(7)))
        ));
        assert!(!fd_identity_is_open(duplicate_raw, duplicate_identity));

        let unsupported = create_memfd("unsupported", 64).unwrap();
        let unsupported_raw = unsupported.as_raw_fd();
        let unsupported_identity = fd_identity(unsupported_raw);
        let error = importer
            .add_memory(8, data_type::DMA_BUF, unsupported, 0)
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(matches!(
            error
                .get_ref()
                .and_then(|error| error.downcast_ref::<MemoryError>()),
            Some(MemoryError::UnsupportedMemoryType(_))
        ));
        assert!(!fd_identity_is_open(unsupported_raw, unsupported_identity));
    }

    #[test]
    fn importer_drop_terminally_disconnects_shared_pool() {
        let inner = inner();
        let handle = MemoryPoolHandle {
            inner: Arc::clone(&inner),
        };
        let mut importer = MemoryPoolImporter { inner };
        let fd = create_memfd("disconnect", 64).unwrap();
        let raw = fd.as_raw_fd();
        let identity = fd_identity(raw);
        importer.add_memory(9, data_type::MEM_FD, fd, 0).unwrap();

        drop(importer);

        assert!(!fd_identity_is_open(raw, identity));
        assert!(matches!(
            handle.resolve(MemoryId(9)),
            Err(MemoryError::Disconnected)
        ));
    }

    #[test]
    fn notifications_follow_mutation_and_preserve_exact_generation() {
        let inner = inner();
        let handle = MemoryPoolHandle {
            inner: Arc::clone(&inner),
        };
        let observed = Arc::new(Mutex::new(Vec::new()));
        let callback_handle = handle.clone();
        let callback_observed = Arc::clone(&observed);
        let _subscription = handle.subscribe(Box::new(move |event| {
            // Resolving in the callback proves the pool lock is not held across notification.
            if let MemoryPoolEvent::Available(lease) = &event {
                assert_eq!(
                    callback_handle.resolve(lease.key().id).unwrap(),
                    lease.key()
                );
            }
            callback_observed.lock().push(match event {
                MemoryPoolEvent::Available(lease) => (0, Some(lease.key())),
                MemoryPoolEvent::Removed(key) => (1, Some(key)),
                MemoryPoolEvent::Disconnected => (2, None),
            });
        }));
        let mut importer = MemoryPoolImporter { inner };

        importer
            .add_memory(
                12,
                data_type::MEM_FD,
                create_memfd("notify", 64).unwrap(),
                0,
            )
            .unwrap();
        let key = handle.resolve(MemoryId(12)).unwrap();
        importer.remove_memory(12).unwrap();

        assert_eq!(*observed.lock(), [(0, Some(key)), (1, Some(key))]);
        assert!(matches!(
            handle.map(key, 0, 1, false),
            Err(MemoryError::StaleGeneration { requested, active: None }) if requested == key
        ));
    }

    #[test]
    fn subscriptions_are_independent_ordered_and_reentrant() {
        let inner = inner();
        let handle = MemoryPoolHandle { inner };
        let first_events = Arc::new(StdMutex::new(Vec::new()));
        let second_events = Arc::new(StdMutex::new(Vec::new()));
        let replacement_events = Arc::new(StdMutex::new(Vec::new()));
        let first_slot = Arc::new(StdMutex::new(None));
        let replacement_slot = Arc::new(StdMutex::new(None));

        let callback_handle = handle.clone();
        let callback_slot = Arc::clone(&first_slot);
        let callback_events = Arc::clone(&first_events);
        let callback_replacement = Arc::clone(&replacement_events);
        let callback_replacement_slot = Arc::clone(&replacement_slot);
        let first = handle.subscribe(Box::new(move |event| {
            callback_events.lock().unwrap().push(match &event {
                MemoryPoolEvent::Available(lease) => lease.key(),
                MemoryPoolEvent::Removed(key) => *key,
                MemoryPoolEvent::Disconnected => return,
            });
            if let MemoryPoolEvent::Available(lease) = event {
                callback_slot.lock().unwrap().take();
                let replacement = Arc::clone(&callback_replacement);
                let subscription = callback_handle.subscribe(Box::new(move |event| {
                    if let MemoryPoolEvent::Removed(key) = event {
                        replacement.lock().unwrap().push(key);
                    }
                }));
                *callback_replacement_slot.lock().unwrap() = Some(subscription);
                notify(
                    &callback_handle.inner,
                    MemoryPoolEvent::Removed(lease.key()),
                );
            }
        }));
        *first_slot.lock().unwrap() = Some(first);

        let second_observed = Arc::clone(&second_events);
        let second = handle.subscribe(Box::new(move |event| {
            second_observed.lock().unwrap().push(match event {
                MemoryPoolEvent::Available(lease) => Some(lease.key()),
                MemoryPoolEvent::Removed(key) => Some(key),
                MemoryPoolEvent::Disconnected => None,
            });
        }));

        let key = {
            let mut pool = handle.inner.pool.lock();
            let key = pool
                .add(
                    MemoryId(30),
                    data_type::MEM_FD,
                    0,
                    create_memfd("reentrant", 64).unwrap(),
                )
                .unwrap();
            let lease = pool.lease(key).unwrap();
            drop(pool);
            notify(&handle.inner, MemoryPoolEvent::Available(lease));
            key
        };

        assert_eq!(*first_events.lock().unwrap(), [key]);
        assert_eq!(*second_events.lock().unwrap(), [Some(key), Some(key)]);
        replacement_slot.lock().unwrap().take();
        assert_eq!(*replacement_events.lock().unwrap(), [key]);
        drop(second);
        notify(&handle.inner, MemoryPoolEvent::Removed(key));
        assert_eq!(*second_events.lock().unwrap(), [Some(key), Some(key)]);
    }

    #[test]
    fn subscription_replays_then_orders_racing_remove_and_reimport() {
        let inner = inner();
        let handle = MemoryPoolHandle {
            inner: Arc::clone(&inner),
        };
        let mut importer = MemoryPoolImporter { inner };
        importer
            .add_memory(
                41,
                data_type::MEM_FD,
                create_memfd("before-subscribe", 64).unwrap(),
                0,
            )
            .unwrap();
        let old = handle.resolve(MemoryId(41)).unwrap();

        let replay_reached = Arc::new(Barrier::new(2));
        let replay_release = Arc::new(Barrier::new(2));
        let observed = Arc::new(StdMutex::new(Vec::new()));
        let subscriber = {
            let handle = handle.clone();
            let replay_reached = Arc::clone(&replay_reached);
            let replay_release = Arc::clone(&replay_release);
            let observed = Arc::clone(&observed);
            std::thread::spawn(move || {
                handle.subscribe(Box::new(move |event| {
                    observed.lock().unwrap().push(match event {
                        MemoryPoolEvent::Available(lease) => (true, lease.key()),
                        MemoryPoolEvent::Removed(key) => (false, key),
                        MemoryPoolEvent::Disconnected => return,
                    });
                    if observed.lock().unwrap().len() == 1 {
                        replay_reached.wait();
                        replay_release.wait();
                    }
                }))
            })
        };

        replay_reached.wait();
        importer.remove_memory(41).unwrap();
        importer
            .add_memory(
                41,
                data_type::MEM_FD,
                create_memfd("during-subscribe", 64).unwrap(),
                0,
            )
            .unwrap();
        let new = handle.resolve(MemoryId(41)).unwrap();
        replay_release.wait();
        let _subscription = subscriber.join().unwrap();

        assert_eq!(
            *observed.lock().unwrap(),
            [(true, old), (false, old), (true, new)]
        );
        assert_ne!(old.generation, new.generation);
    }

    #[test]
    fn enqueue_at_quiescence_takes_dispatch_ownership() {
        let inner = inner();
        let handle = MemoryPoolHandle {
            inner: Arc::clone(&inner),
        };
        let observed = Arc::new(StdMutex::new(0));
        let callback_observed = Arc::clone(&observed);
        let _subscription = handle.subscribe(Box::new(move |_| {
            *callback_observed.lock().unwrap() += 1;
        }));
        let empty_reached = Arc::new(Barrier::new(2));
        let empty_release = Arc::new(Barrier::new(2));
        inner.events.lock().quiescence_barriers =
            Some((Arc::clone(&empty_reached), Arc::clone(&empty_release)));

        let first_inner = Arc::clone(&inner);
        let first = std::thread::spawn(move || {
            notify(&first_inner, MemoryPoolEvent::Disconnected);
        });
        empty_reached.wait();
        let second_inner = Arc::clone(&inner);
        let second = std::thread::spawn(move || {
            notify(&second_inner, MemoryPoolEvent::Disconnected);
        });
        empty_release.wait();
        first.join().unwrap();
        second.join().unwrap();

        assert_eq!(*observed.lock().unwrap(), 2);
        let events = inner.events.lock();
        assert!(!events.dispatching);
        assert!(events.queued.is_empty());
    }
}
