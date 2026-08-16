// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: Copyright (c) 2026 Asymptotic Inc.

//! Adapts Core memory events to a ClientNode session memory pool.

use std::{fmt, io, os::fd::OwnedFd, sync::Arc};

use parking_lot::Mutex;
use pipewire_native_node::session::memory::{MemoryPool, MemoryResolver};

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
    pool: Mutex<MemoryPool>,
    event_handler: Mutex<Option<MemoryPoolEventHandler>>,
}

/// A successful Core memory-pool mutation delivered after the pool lock is released.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MemoryPoolEvent {
    /// A numeric ID now resolves to this exact generation.
    Available(MemoryKey),
    /// This exact generation was retired and can no longer be resolved.
    Removed(MemoryKey),
}

/// Sole owner callback for connection memory lifecycle events.
pub type MemoryPoolEventHandler = Box<dyn FnMut(MemoryPoolEvent) + Send>;

impl MemoryPoolHandle {
    /// Creates a pool, installs its private importer into `core`, and returns session access.
    ///
    /// Replacing the Core importer or disconnecting the Core permanently disconnects this pool.
    pub fn install(core: &Core, shrink_policy: ShrinkPolicy) -> Self {
        let inner = Arc::new(MemoryPoolInner {
            pool: Mutex::new(MemoryPool::new(shrink_policy)),
            event_handler: Mutex::new(None),
        });
        core.set_memory_importer(Some(Box::new(MemoryPoolImporter {
            inner: Arc::clone(&inner),
        })));
        Self { inner }
    }

    /// Installs the sole owner of post-mutation memory lifecycle events.
    ///
    /// Replacing the handler drops the previous handler after releasing the handler lock.
    pub fn set_event_handler(&self, handler: Option<MemoryPoolEventHandler>) {
        let previous = std::mem::replace(&mut *self.inner.event_handler.lock(), handler);
        drop(previous);
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
        let key = self
            .inner
            .pool
            .lock()
            .add(MemoryId(id), type_, flags, fd)
            .map_err(import_error)?;
        notify(&self.inner, MemoryPoolEvent::Available(key));
        Ok(())
    }

    fn remove_memory(&mut self, id: Id) -> io::Result<()> {
        let key = self
            .inner
            .pool
            .lock()
            .remove(MemoryId(id))
            .map_err(import_error)?;
        notify(&self.inner, MemoryPoolEvent::Removed(key));
        Ok(())
    }
}

impl Drop for MemoryPoolImporter {
    fn drop(&mut self) {
        self.inner.pool.lock().disconnect();
    }
}

fn notify(inner: &MemoryPoolInner, event: MemoryPoolEvent) {
    if let Some(handler) = inner.event_handler.lock().as_mut() {
        handler(event);
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
    use std::os::fd::{AsRawFd, RawFd};

    use pipewire_native_node::{session::memory::MemoryPool, shm::create_memfd};
    use pipewire_native_spa::buffer::data_type;

    use super::*;

    fn fd_is_open(fd: RawFd) -> bool {
        unsafe { libc::fcntl(fd, libc::F_GETFD) >= 0 }
    }

    #[test]
    fn importer_errors_preserve_pool_error_and_close_candidate_fd() {
        let inner = Arc::new(MemoryPoolInner {
            pool: Mutex::new(MemoryPool::new(ShrinkPolicy::Allow)),
            event_handler: Mutex::new(None),
        });
        let mut importer = MemoryPoolImporter {
            inner: Arc::clone(&inner),
        };
        importer
            .add_memory(7, data_type::MEM_FD, create_memfd("active", 64).unwrap(), 3)
            .unwrap();

        let duplicate = create_memfd("duplicate", 64).unwrap();
        let duplicate_raw = duplicate.as_raw_fd();
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
        assert!(!fd_is_open(duplicate_raw));

        let unsupported = create_memfd("unsupported", 64).unwrap();
        let unsupported_raw = unsupported.as_raw_fd();
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
        assert!(!fd_is_open(unsupported_raw));
    }

    #[test]
    fn importer_drop_terminally_disconnects_shared_pool() {
        let inner = Arc::new(MemoryPoolInner {
            pool: Mutex::new(MemoryPool::new(ShrinkPolicy::Allow)),
            event_handler: Mutex::new(None),
        });
        let handle = MemoryPoolHandle {
            inner: Arc::clone(&inner),
        };
        let mut importer = MemoryPoolImporter { inner };
        let fd = create_memfd("disconnect", 64).unwrap();
        let raw = fd.as_raw_fd();
        importer.add_memory(9, data_type::MEM_FD, fd, 0).unwrap();

        drop(importer);

        assert!(!fd_is_open(raw));
        assert!(matches!(
            handle.resolve(MemoryId(9)),
            Err(MemoryError::Disconnected)
        ));
    }

    #[test]
    fn notifications_follow_mutation_and_preserve_exact_generation() {
        let inner = Arc::new(MemoryPoolInner {
            pool: Mutex::new(MemoryPool::new(ShrinkPolicy::Allow)),
            event_handler: Mutex::new(None),
        });
        let handle = MemoryPoolHandle {
            inner: Arc::clone(&inner),
        };
        let observed = Arc::new(Mutex::new(Vec::new()));
        let callback_handle = handle.clone();
        let callback_observed = Arc::clone(&observed);
        handle.set_event_handler(Some(Box::new(move |event| {
            // Resolving in the callback proves the pool lock is not held across notification.
            if let MemoryPoolEvent::Available(key) = event {
                assert_eq!(callback_handle.resolve(key.id).unwrap(), key);
            }
            callback_observed.lock().push(event);
        })));
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

        assert_eq!(
            *observed.lock(),
            [
                MemoryPoolEvent::Available(key),
                MemoryPoolEvent::Removed(key)
            ]
        );
        assert!(matches!(
            handle.map(key, 0, 1, false),
            Err(MemoryError::StaleGeneration { requested, active: None }) if requested == key
        ));
    }
}
