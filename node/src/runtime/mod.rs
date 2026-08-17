// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: Copyright (c) 2026 Asymptotic Inc.

//! Runtime adapters for the synchronous ClientNode session owner.

use std::{
    fmt, io,
    os::fd::{AsRawFd, RawFd},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use tokio::{
    io::unix::AsyncFd,
    sync::{mpsc, watch},
    task::{JoinError, JoinHandle},
};

use crate::{
    session::{
        memory::MemoryResolver,
        output::OutputProcess,
        owner::{
            ClientNodeSession, RuntimeRegistration, SessionCommand, SessionState, WakeOutcome,
        },
    },
    signal::EventFd,
};

/// Default bound for ownership-bearing commands awaiting the process owner.
pub const DEFAULT_COMMAND_CAPACITY: usize = 64;

/// Monotonic time source sampled at activation claim and completion.
pub trait RuntimeClock: Send + 'static {
    /// Returns monotonic nanoseconds.
    fn monotonic_ns(&mut self) -> u64;
}

/// Linux `CLOCK_MONOTONIC` runtime clock.
#[derive(Clone, Copy, Debug, Default)]
pub struct MonotonicClock;

impl RuntimeClock for MonotonicClock {
    fn monotonic_ns(&mut self) -> u64 {
        let mut time = std::mem::MaybeUninit::<libc::timespec>::uninit();
        let result = unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, time.as_mut_ptr()) };
        if result < 0 {
            return 0;
        }
        let time = unsafe { time.assume_init() };
        (time.tv_sec as u64)
            .saturating_mul(1_000_000_000)
            .saturating_add(time.tv_nsec as u64)
    }
}

/// Why a bounded command could not be transferred to its sole owner.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CommandSendError {
    /// The bounded queue had no remaining capacity. The runtime is terminated.
    Full,
    /// The runtime owner had already stopped. The command and any FDs were dropped.
    Closed,
}

impl fmt::Display for CommandSendError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{self:?}")
    }
}

impl std::error::Error for CommandSendError {}

/// Cloneable bounded command endpoint for protocol and memory callbacks.
#[derive(Clone)]
pub struct SessionCommandSender {
    commands: mpsc::Sender<SessionCommand>,
    stop: watch::Sender<bool>,
}

impl fmt::Debug for SessionCommandSender {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SessionCommandSender")
            .field("capacity", &self.commands.capacity())
            .finish_non_exhaustive()
    }
}

impl SessionCommandSender {
    /// Transfers one command without blocking the protocol loop.
    ///
    /// A full queue is terminal because dropping an accepted lifecycle event would desynchronize
    /// descriptor ownership from session state. In both failure cases `command`, including all
    /// owned descriptors, is dropped exactly once before this method returns.
    pub fn try_send(&self, command: SessionCommand) -> Result<(), CommandSendError> {
        match self.commands.try_send(command) {
            Ok(()) => Ok(()),
            Err(mpsc::error::TrySendError::Full(command)) => {
                drop(command);
                let _ = self.stop.send(true);
                Err(CommandSendError::Full)
            }
            Err(mpsc::error::TrySendError::Closed(command)) => {
                drop(command);
                Err(CommandSendError::Closed)
            }
        }
    }

    /// Terminates the runtime after a protocol conversion or bridge failure.
    pub fn terminate(&self) {
        let _ = self.stop.send(true);
    }
}

/// Atomic diagnostics shared with a runtime handle.
#[derive(Debug, Default)]
struct RuntimeCounters {
    callbacks: AtomicU64,
    missed_wakes: AtomicU64,
    stale_wakes: AtomicU64,
}

/// Point-in-time runtime diagnostics.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct RuntimeDiagnostics {
    /// Successfully completed callback cycles.
    pub callbacks: u64,
    /// Coalesced eventfd counts beyond the single permitted claim attempt.
    pub missed_wakes: u64,
    /// Readiness turns tagged with a retired transport generation.
    pub stale_wakes: u64,
}

/// Handle to one Tokio-owned ClientNode session.
pub struct TokioSessionHandle {
    sender: SessionCommandSender,
    stop: watch::Sender<bool>,
    join: Option<JoinHandle<io::Result<()>>>,
    counters: Arc<RuntimeCounters>,
}

impl fmt::Debug for TokioSessionHandle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TokioSessionHandle")
            .field("diagnostics", &self.diagnostics())
            .finish_non_exhaustive()
    }
}

impl TokioSessionHandle {
    /// Returns a cloneable bounded semantic command endpoint.
    pub fn command_sender(&self) -> SessionCommandSender {
        self.sender.clone()
    }

    /// Returns current adapter diagnostics.
    pub fn diagnostics(&self) -> RuntimeDiagnostics {
        RuntimeDiagnostics {
            callbacks: self.counters.callbacks.load(Ordering::Relaxed),
            missed_wakes: self.counters.missed_wakes.load(Ordering::Relaxed),
            stale_wakes: self.counters.stale_wakes.load(Ordering::Relaxed),
        }
    }

    /// Requests shutdown without waiting for task completion.
    pub fn request_shutdown(&self) {
        let _ = self.stop.send(true);
    }

    /// Waits for task completion without first requesting shutdown.
    pub async fn wait(mut self) -> io::Result<()> {
        let join = self
            .join
            .take()
            .expect("runtime join handle already consumed");
        join_result_to_io(join.await)
    }

    /// Requests deterministic session teardown and waits for task completion.
    pub async fn shutdown(mut self) -> io::Result<()> {
        let _ = self.stop.send(true);
        let join = self
            .join
            .take()
            .expect("runtime join handle already consumed");
        join_result_to_io(join.await)
    }
}

impl Drop for TokioSessionHandle {
    fn drop(&mut self) {
        let _ = self.stop.send(true);
        if let Some(join) = self.join.take() {
            // Tokio cancellation occurs only at an await, so a synchronous process callback is
            // never interrupted while it owns an OutputCycle.
            join.abort();
        }
    }
}

/// Spawns one session on the current Tokio runtime with the default monotonic clock.
pub fn spawn_tokio<R>(
    session: ClientNodeSession<R>,
    process: Box<dyn OutputProcess>,
    command_capacity: usize,
) -> io::Result<TokioSessionHandle>
where
    R: MemoryResolver + Send + 'static,
{
    spawn_tokio_with_clock(session, process, command_capacity, MonotonicClock)
}

/// Spawns one session with an explicit deterministic clock.
pub fn spawn_tokio_with_clock<R, C>(
    session: ClientNodeSession<R>,
    process: Box<dyn OutputProcess>,
    command_capacity: usize,
    clock: C,
) -> io::Result<TokioSessionHandle>
where
    R: MemoryResolver + Send + 'static,
    C: RuntimeClock,
{
    if command_capacity == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "ClientNode command capacity must be non-zero",
        ));
    }
    let (commands, receiver) = mpsc::channel(command_capacity);
    let (stop, stop_rx) = watch::channel(false);
    let counters = Arc::new(RuntimeCounters::default());
    let worker_counters = Arc::clone(&counters);
    let runtime = tokio::runtime::Handle::try_current().map_err(|error| {
        io::Error::new(
            io::ErrorKind::NotConnected,
            format!("ClientNode session requires a Tokio runtime: {error}"),
        )
    })?;
    let join = runtime.spawn(run_session(
        session,
        receiver,
        stop_rx,
        process,
        clock,
        worker_counters,
    ));
    let sender = SessionCommandSender {
        commands,
        stop: stop.clone(),
    };
    Ok(TokioSessionHandle {
        sender,
        stop,
        join: Some(join),
        counters,
    })
}

struct RegisteredTrigger {
    generation: u64,
    trigger: EventFd,
}

impl AsRawFd for RegisteredTrigger {
    fn as_raw_fd(&self) -> RawFd {
        self.trigger.as_raw_fd()
    }
}

async fn run_session<R, C>(
    mut session: ClientNodeSession<R>,
    mut commands: mpsc::Receiver<SessionCommand>,
    mut stop: watch::Receiver<bool>,
    mut process: Box<dyn OutputProcess>,
    mut clock: C,
    counters: Arc<RuntimeCounters>,
) -> io::Result<()>
where
    R: MemoryResolver + Send + 'static,
    C: RuntimeClock,
{
    let result: io::Result<()> = async {
        let mut registration = desired_registration(&session)?;
        loop {
            if *stop.borrow() {
                break;
            }
            if let Some(trigger) = registration.as_ref() {
                tokio::select! {
                    biased;
                    changed = stop.changed() => {
                        if changed.is_err() || *stop.borrow() {
                            break;
                        }
                    }
                    command = commands.recv() => {
                        let Some(command) = command else { break };
                        session.apply(command).map_err(session_error)?;
                        if session.state() == SessionState::Disconnected { break; }
                        registration = desired_registration(&session)?;
                    }
                    ready = trigger.readable() => {
                        let mut ready = ready?;
                        let generation = trigger.get_ref().generation;
                        let awake_ns = clock.monotonic_ns();
                        let outcome = session
                            .on_wake_with_finish(generation, awake_ns, || clock.monotonic_ns(), &mut *process)
                            .map_err(session_error)?;
                        ready.clear_ready();
                        record_outcome(&counters, outcome);
                        registration = desired_registration(&session)?;
                    }
                }
            } else {
                tokio::select! {
                    biased;
                    changed = stop.changed() => {
                        if changed.is_err() || *stop.borrow() {
                            break;
                        }
                    }
                    command = commands.recv() => {
                        let Some(command) = command else { break };
                        session.apply(command).map_err(session_error)?;
                        if session.state() == SessionState::Disconnected { break; }
                        registration = desired_registration(&session)?;
                    }
                }
            }
        }
        Ok(())
    }
    .await;
    let cleanup = session
        .apply(SessionCommand::Disconnect)
        .map(|_| ())
        .map_err(session_error);
    result.and(cleanup)
}

fn desired_registration<R: MemoryResolver>(
    session: &ClientNodeSession<R>,
) -> io::Result<Option<AsyncFd<RegisteredTrigger>>> {
    if session.state() != SessionState::Running {
        return Ok(None);
    }
    session
        .runtime_registration()
        .map_err(session_error)?
        .map(register)
        .transpose()
}

fn register(registration: RuntimeRegistration) -> io::Result<AsyncFd<RegisteredTrigger>> {
    AsyncFd::new(RegisteredTrigger {
        generation: registration.generation,
        trigger: registration.trigger,
    })
}

fn record_outcome(counters: &RuntimeCounters, outcome: WakeOutcome) {
    match outcome {
        WakeOutcome::Processed { missed, .. } => {
            counters.callbacks.fetch_add(1, Ordering::Relaxed);
            counters.missed_wakes.fetch_add(missed, Ordering::Relaxed);
        }
        WakeOutcome::NotClaimed { missed, .. } => {
            counters.missed_wakes.fetch_add(missed, Ordering::Relaxed);
        }
        WakeOutcome::Stale { .. } => {
            counters.stale_wakes.fetch_add(1, Ordering::Relaxed);
        }
        WakeOutcome::NoWake | WakeOutcome::NotRunning { .. } => {}
    }
}

fn session_error(error: impl std::error::Error + Send + Sync + 'static) -> io::Error {
    io::Error::other(error)
}

fn join_result_to_io(join: Result<io::Result<()>, JoinError>) -> io::Result<()> {
    match join {
        Ok(result) => result,
        Err(error) => Err(io::Error::other(format!(
            "node runtime join error: {error}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::VecDeque,
        os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd},
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc, Mutex,
        },
    };

    use pipewire_native_spa::buffer::data_type;

    use super::*;
    use crate::{
        session::{
            activation::{ActivationStatus, ActivationView},
            config::{
                BufferDescriptor, BufferSetDescriptor, MetaDescriptor, NegotiatedAudioFormat,
                PortId, PortIoDescriptor, TransportDescriptor,
            },
            cycle::{CommittedOutput, OutputCycle},
            memory::{MemoryError, MemoryId, MemoryKey, MemoryMapping, MemoryPool, RegionRef},
            output::ProcessError,
            owner::NodeCommandState,
            port::BufferStatus,
        },
        shm::{create_memfd, ShrinkPolicy},
    };

    #[derive(Clone, Debug)]
    struct SharedMemory(Arc<Mutex<MemoryPool>>);

    impl SharedMemory {
        fn new() -> Self {
            Self(Arc::new(Mutex::new(MemoryPool::new(
                ShrinkPolicy::RequireSealed,
            ))))
        }

        fn add(&self, id: u32, len: usize) -> MemoryKey {
            self.0
                .lock()
                .unwrap()
                .add(
                    MemoryId(id),
                    data_type::MEM_FD,
                    0,
                    create_memfd(&format!("runtime-{id}"), len).unwrap(),
                )
                .unwrap()
        }

        fn session(&self) -> ClientNodeSession<Self> {
            let mut session = ClientNodeSession::new(self.clone());
            let pool = self.0.lock().unwrap();
            for key in pool.active_keys() {
                session
                    .apply(SessionCommand::MemoryAvailable(pool.lease(key).unwrap()))
                    .unwrap();
            }
            session
        }
    }

    impl MemoryResolver for SharedMemory {
        fn resolve(&self, id: MemoryId) -> Result<MemoryKey, MemoryError> {
            self.0.lock().unwrap().resolve(id)
        }

        fn map(
            &self,
            key: MemoryKey,
            offset: usize,
            len: usize,
            writable: bool,
        ) -> Result<MemoryMapping, MemoryError> {
            self.0.lock().unwrap().map(key, offset, len, writable)
        }
    }

    struct CountingProcess(Arc<AtomicUsize>);

    impl OutputProcess for CountingProcess {
        fn process(&mut self, mut cycle: OutputCycle<'_>) -> Result<CommittedOutput, ProcessError> {
            self.0.fetch_add(1, Ordering::Relaxed);
            cycle.interleaved_pcm()[..4].copy_from_slice(&[1, 2, 3, 4]);
            cycle.commit(1).map_err(|error| ProcessError {
                message: error.to_string(),
            })
        }
    }

    struct FixedClock(VecDeque<u64>);

    impl RuntimeClock for FixedClock {
        fn monotonic_ns(&mut self) -> u64 {
            self.0.pop_front().unwrap_or(99)
        }
    }

    fn event_pair() -> (OwnedFd, EventFd) {
        let raw = unsafe { libc::eventfd(0, libc::EFD_CLOEXEC | libc::EFD_NONBLOCK) };
        assert!(raw >= 0);
        let owned = unsafe { OwnedFd::from_raw_fd(raw) };
        let observer = EventFd::from_owned_fd(owned.try_clone().unwrap()).unwrap();
        (owned, observer)
    }

    fn fdinfo(fd: RawFd) -> Option<String> {
        std::fs::read_to_string(format!("/proc/self/fdinfo/{fd}")).ok()
    }

    fn configure_running() -> (
        ClientNodeSession<SharedMemory>,
        SharedMemory,
        MemoryKey,
        EventFd,
        EventFd,
        RawFd,
        RawFd,
    ) {
        let memory = SharedMemory::new();
        let activation_key = memory.add(1, ActivationView::required_size());
        let metadata_key = memory.add(2, 16);
        let _media_key = memory.add(3, 64);
        let io_key = memory.add(4, 8);
        {
            let mut activation = memory
                .map(activation_key, 0, ActivationView::required_size(), true)
                .unwrap();
            let mut guard = activation.borrow();
            let bytes = unsafe { guard.bytes_mut() };
            bytes[0..4].copy_from_slice(&(ActivationStatus::Inactive as u32).to_ne_bytes());
            bytes[544..548].copy_from_slice(&1_u32.to_ne_bytes());
        }
        {
            let mut io = memory.map(io_key, 0, 8, true).unwrap();
            let mut guard = io.borrow();
            let bytes = unsafe { guard.bytes_mut() };
            bytes[0..4].copy_from_slice(&(BufferStatus::NeedData as i32).to_ne_bytes());
            bytes[4..8].copy_from_slice(&0_u32.to_ne_bytes());
        }
        // Initialize the chunk offset and keep metadata as a distinct sealed mapping.
        let _ = metadata_key;
        let (trigger_fd, trigger) = event_pair();
        let trigger_raw = trigger_fd.as_raw_fd();
        let (completion_fd, completion) = event_pair();
        let completion_raw = completion_fd.as_raw_fd();
        let mut session = memory.session();
        session
            .apply(SessionCommand::ReplaceTransport(TransportDescriptor {
                trigger_fd,
                completion_fd,
                activation: RegionRef {
                    memory: MemoryId(1),
                    offset: 0,
                    len: ActivationView::required_size(),
                },
            }))
            .unwrap();
        session
            .apply(SessionCommand::SetFormat(
                NegotiatedAudioFormat::pcm_s16le(48_000, 2).unwrap(),
            ))
            .unwrap();
        session
            .apply(SessionCommand::UseBuffers(BufferSetDescriptor {
                port: PortId(0),
                buffers: vec![BufferDescriptor {
                    metadata: RegionRef {
                        memory: MemoryId(2),
                        offset: 0,
                        len: 16,
                    },
                    metas: Vec::<MetaDescriptor>::new().into_boxed_slice(),
                    media_memory: MemoryId(3),
                    map_offset: 0,
                    max_size: 64,
                }]
                .into_boxed_slice(),
            }))
            .unwrap();
        session
            .apply(SessionCommand::SetPortIo(PortIoDescriptor {
                port: PortId(0),
                region: RegionRef {
                    memory: MemoryId(4),
                    offset: 0,
                    len: 8,
                },
            }))
            .unwrap();
        session.apply(SessionCommand::SetActive(true)).unwrap();
        session
            .apply(SessionCommand::SetNodeCommand(NodeCommandState::Start))
            .unwrap();
        (
            session,
            memory,
            activation_key,
            trigger,
            completion,
            trigger_raw,
            completion_raw,
        )
    }

    #[tokio::test(flavor = "current_thread")]
    async fn coalesced_readability_runs_one_claim_and_never_signals_completion() {
        let (session, memory, activation_key, trigger, completion, trigger_raw, completion_raw) =
            configure_running();
        let trigger_identity = fdinfo(trigger_raw).unwrap();
        let completion_identity = fdinfo(completion_raw).unwrap();
        {
            let mut activation = memory
                .map(activation_key, 0, ActivationView::required_size(), true)
                .unwrap();
            let mut guard = activation.borrow();
            (unsafe { guard.bytes_mut() })[0..4]
                .copy_from_slice(&(ActivationStatus::Triggered as u32).to_ne_bytes());
        }
        let calls = Arc::new(AtomicUsize::new(0));
        let handle = spawn_tokio_with_clock(
            session,
            Box::new(CountingProcess(Arc::clone(&calls))),
            8,
            FixedClock(VecDeque::from([11, 22])),
        )
        .unwrap();

        trigger.signal(3).unwrap();
        for _ in 0..10_000 {
            if calls.load(Ordering::Relaxed) == 1 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(calls.load(Ordering::Relaxed), 1);
        assert_eq!(
            handle.diagnostics(),
            RuntimeDiagnostics {
                callbacks: 1,
                missed_wakes: 2,
                stale_wakes: 0,
            }
        );
        assert_eq!(
            completion.drain().unwrap_err().kind(),
            io::ErrorKind::WouldBlock
        );

        handle.shutdown().await.unwrap();
        assert_ne!(
            fdinfo(trigger_raw).as_deref(),
            Some(trigger_identity.as_str())
        );
        assert_ne!(
            fdinfo(completion_raw).as_deref(),
            Some(completion_identity.as_str())
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn transport_replacement_deregisters_old_trigger_before_new_wakes() {
        let (session, memory, _old_key, old_trigger, _completion, _, _) = configure_running();
        let replacement_key = memory.add(5, ActivationView::required_size());
        {
            let mut activation = memory
                .map(replacement_key, 0, ActivationView::required_size(), true)
                .unwrap();
            let mut guard = activation.borrow();
            let bytes = unsafe { guard.bytes_mut() };
            bytes[0..4].copy_from_slice(&(ActivationStatus::Inactive as u32).to_ne_bytes());
            bytes[544..548].copy_from_slice(&1_u32.to_ne_bytes());
        }
        let (new_trigger_fd, new_trigger) = event_pair();
        let (new_completion_fd, new_completion) = event_pair();
        let calls = Arc::new(AtomicUsize::new(0));
        let handle = spawn_tokio_with_clock(
            session,
            Box::new(CountingProcess(Arc::clone(&calls))),
            8,
            FixedClock(VecDeque::from([31, 32])),
        )
        .unwrap();
        handle
            .command_sender()
            .try_send(SessionCommand::MemoryAvailable(
                memory.0.lock().unwrap().lease(replacement_key).unwrap(),
            ))
            .unwrap();
        handle
            .command_sender()
            .try_send(SessionCommand::ReplaceTransport(TransportDescriptor {
                trigger_fd: new_trigger_fd,
                completion_fd: new_completion_fd,
                activation: RegionRef {
                    memory: MemoryId(5),
                    offset: 0,
                    len: ActivationView::required_size(),
                },
            }))
            .unwrap();

        for _ in 0..10_000 {
            let mut activation = memory
                .map(replacement_key, 0, ActivationView::required_size(), true)
                .unwrap();
            let view = unsafe {
                ActivationView::from_raw_parts(activation.as_mut_ptr(), activation.len()).unwrap()
            };
            if view.status().unwrap() == ActivationStatus::Finished {
                break;
            }
            tokio::task::yield_now().await;
        }

        old_trigger.signal(1).unwrap();
        for _ in 0..20 {
            tokio::task::yield_now().await;
        }
        assert_eq!(calls.load(Ordering::Relaxed), 0);

        {
            let mut activation = memory
                .map(replacement_key, 0, ActivationView::required_size(), true)
                .unwrap();
            let mut guard = activation.borrow();
            (unsafe { guard.bytes_mut() })[0..4]
                .copy_from_slice(&(ActivationStatus::Triggered as u32).to_ne_bytes());
        }
        new_trigger.signal(1).unwrap();
        for _ in 0..100 {
            if calls.load(Ordering::Relaxed) == 1 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(calls.load(Ordering::Relaxed), 1);
        assert_eq!(
            new_completion.drain().unwrap_err().kind(),
            io::ErrorKind::WouldBlock
        );
        handle.shutdown().await.unwrap();
    }

    #[test]
    fn full_and_closed_queues_drop_owned_fds_once_and_terminate() {
        for closed in [false, true] {
            let (commands, receiver) = mpsc::channel(1);
            let (stop, _stop_rx) = watch::channel(false);
            let sender = SessionCommandSender { commands, stop };
            if closed {
                drop(receiver);
            } else {
                sender.try_send(SessionCommand::SetActive(true)).unwrap();
            }
            let (trigger, trigger_observer) = event_pair();
            let (completion, completion_observer) = event_pair();
            let trigger_raw = trigger.as_raw_fd();
            let completion_raw = completion.as_raw_fd();
            let trigger_identity = fdinfo(trigger_raw).unwrap();
            let completion_identity = fdinfo(completion_raw).unwrap();
            let error = sender
                .try_send(SessionCommand::ReplaceTransport(TransportDescriptor {
                    trigger_fd: trigger,
                    completion_fd: completion,
                    activation: RegionRef {
                        memory: MemoryId(1),
                        offset: 0,
                        len: ActivationView::required_size(),
                    },
                }))
                .unwrap_err();
            assert_eq!(
                error,
                if closed {
                    CommandSendError::Closed
                } else {
                    CommandSendError::Full
                }
            );
            assert_eq!(
                trigger_observer.drain().unwrap_err().kind(),
                io::ErrorKind::WouldBlock
            );
            assert_eq!(
                completion_observer.drain().unwrap_err().kind(),
                io::ErrorKind::WouldBlock
            );
            assert_ne!(
                fdinfo(trigger_raw).as_deref(),
                Some(trigger_identity.as_str())
            );
            assert_ne!(
                fdinfo(completion_raw).as_deref(),
                Some(completion_identity.as_str())
            );
        }
    }

    #[test]
    fn spawn_without_tokio_runtime_returns_error() {
        let memory = SharedMemory::new();
        let session = ClientNodeSession::new(memory);
        let result = spawn_tokio(
            session,
            Box::new(CountingProcess(Arc::new(AtomicUsize::new(0)))),
            1,
        );
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::NotConnected);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn handle_drop_and_runtime_error_deactivate_activation() {
        for fail_runtime in [false, true] {
            let (session, memory, activation_key, _trigger, _completion, _, _) =
                configure_running();
            let handle = spawn_tokio(
                session,
                Box::new(CountingProcess(Arc::new(AtomicUsize::new(0)))),
                8,
            )
            .unwrap();
            if fail_runtime {
                let bad_key = memory.add(80, 8);
                let (trigger_fd, _) = event_pair();
                let (completion_fd, _) = event_pair();
                let sender = handle.command_sender();
                sender
                    .try_send(SessionCommand::MemoryAvailable(
                        memory.0.lock().unwrap().lease(bad_key).unwrap(),
                    ))
                    .unwrap();
                sender
                    .try_send(SessionCommand::ReplaceTransport(TransportDescriptor {
                        trigger_fd,
                        completion_fd,
                        activation: RegionRef {
                            memory: MemoryId(80),
                            offset: 0,
                            len: 8,
                        },
                    }))
                    .unwrap();
                assert!(handle.wait().await.is_err());
            } else {
                drop(handle);
                tokio::task::yield_now().await;
            }
            let mut activation = memory
                .map(activation_key, 0, ActivationView::required_size(), true)
                .unwrap();
            let view = unsafe {
                ActivationView::from_raw_parts(activation.as_mut_ptr(), activation.len()).unwrap()
            };
            assert_eq!(view.status().unwrap(), ActivationStatus::Inactive);
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn early_start_converges_after_ordered_memory_and_configuration() {
        let memory = SharedMemory::new();
        let session = ClientNodeSession::new(memory.clone());
        let handle = spawn_tokio(
            session,
            Box::new(CountingProcess(Arc::new(AtomicUsize::new(0)))),
            16,
        )
        .unwrap();
        let sender = handle.command_sender();
        sender.try_send(SessionCommand::SetActive(true)).unwrap();
        sender
            .try_send(SessionCommand::SetNodeCommand(NodeCommandState::Start))
            .unwrap();

        let activation_key = memory.add(1, ActivationView::required_size());
        let metadata_key = memory.add(2, 16);
        let media_key = memory.add(3, 64);
        let io_key = memory.add(4, 8);
        {
            let mut activation = memory
                .map(activation_key, 0, ActivationView::required_size(), true)
                .unwrap();
            let mut guard = activation.borrow();
            let bytes = unsafe { guard.bytes_mut() };
            bytes[0..4].copy_from_slice(&(ActivationStatus::Inactive as u32).to_ne_bytes());
            bytes[544..548].copy_from_slice(&1_u32.to_ne_bytes());
        }
        for key in [activation_key, metadata_key, media_key, io_key] {
            sender
                .try_send(SessionCommand::MemoryAvailable(
                    memory.0.lock().unwrap().lease(key).unwrap(),
                ))
                .unwrap();
        }
        let (trigger_fd, _) = event_pair();
        let (completion_fd, _) = event_pair();
        sender
            .try_send(SessionCommand::ReplaceTransport(TransportDescriptor {
                trigger_fd,
                completion_fd,
                activation: RegionRef {
                    memory: MemoryId(1),
                    offset: 0,
                    len: ActivationView::required_size(),
                },
            }))
            .unwrap();
        sender
            .try_send(SessionCommand::SetFormat(
                NegotiatedAudioFormat::pcm_s16le(48_000, 2).unwrap(),
            ))
            .unwrap();
        sender
            .try_send(SessionCommand::UseBuffers(BufferSetDescriptor {
                port: PortId(0),
                buffers: vec![BufferDescriptor {
                    metadata: RegionRef {
                        memory: MemoryId(2),
                        offset: 0,
                        len: 16,
                    },
                    metas: Vec::<MetaDescriptor>::new().into_boxed_slice(),
                    media_memory: MemoryId(3),
                    map_offset: 0,
                    max_size: 64,
                }]
                .into_boxed_slice(),
            }))
            .unwrap();
        sender
            .try_send(SessionCommand::SetPortIo(PortIoDescriptor {
                port: PortId(0),
                region: RegionRef {
                    memory: MemoryId(4),
                    offset: 0,
                    len: 8,
                },
            }))
            .unwrap();

        let mut running = false;
        for _ in 0..10_000 {
            let mut activation = memory
                .map(activation_key, 0, ActivationView::required_size(), true)
                .unwrap();
            let view = unsafe {
                ActivationView::from_raw_parts(activation.as_mut_ptr(), activation.len()).unwrap()
            };
            if view.status().unwrap() == ActivationStatus::Finished {
                running = true;
                break;
            }
            tokio::task::yield_now().await;
        }
        assert!(
            running,
            "early Start intent did not converge after configuration"
        );
        handle.shutdown().await.unwrap();
    }
}
