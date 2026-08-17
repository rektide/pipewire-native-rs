// SPDX-License-Identifier: MIT

//! Shared resources for linked ClientNode process-cycle integration tests.

use std::{
    ffi::CString,
    io,
    os::fd::{AsRawFd, FromRawFd, OwnedFd},
    ptr,
    sync::{Arc, Condvar, Mutex},
    time::{Duration, Instant},
};

use pipewire_native_protocol::wire::client_node::{ActivationStatus, BufferStatus};

/// Size of the x86_64 GNU/Linux `pw_node_activation` v1 record.
pub const ACTIVATION_SIZE: usize = 2312;
/// Fixed downstream node used by the fixture.
pub const PEER_NODE_ID: u32 = 55;
/// Exact PCM bytes written by the linked-cycle callback.
pub const PCM_BYTES: [u8; 16] = [
    0x01, 0x10, 0x02, 0x20, 0x03, 0x30, 0x04, 0x40, 0x05, 0x50, 0x06, 0x60, 0x07, 0x70, 0x08, 0x7f,
];

const STATUS: usize = 0;
const PROCESS_STATUS: usize = 8;
const REQUIRED: usize = 12;
const PENDING: usize = 16;
const SIGNAL_TIME: usize = 32;
const AWAKE_TIME: usize = 40;
const FINISH_TIME: usize = 48;
const CLIENT_VERSION: usize = 540;
const SERVER_VERSION: usize = 544;

/// Stable imported-memory roles used by [`ClientNodeFixture`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MemoryRole {
    /// Node's own activation.
    OwnActivation,
    /// Two 16-byte chunk records.
    Metadata,
    /// Two 64-byte media planes.
    Media,
    /// One synchronous `spa_io_buffers` record.
    Io,
    /// Downstream activation.
    PeerActivation,
}

impl MemoryRole {
    /// Stable Core memory ID.
    pub const fn id(self) -> u32 {
        match self {
            Self::OwnActivation => 101,
            Self::Metadata => 102,
            Self::Media => 103,
            Self::Io => 104,
            Self::PeerActivation => 105,
        }
    }

    const fn index(self) -> usize {
        self.id() as usize - 101
    }

    /// Allocated byte length.
    pub const fn size(self) -> usize {
        match self {
            Self::OwnActivation | Self::PeerActivation => ACTIVATION_SIZE,
            Self::Metadata => 32,
            Self::Media => 128,
            Self::Io => 8,
        }
    }
}

/// Copied final state retained after fixture descriptors and mappings are released.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct CycleSnapshot {
    /// Number of entered callbacks.
    pub callbacks: u64,
    /// Runtime missed-wake count reported by the client.
    pub missed_wakes: u64,
    /// Runtime stale-wake count reported by the client.
    pub stale_wakes: u64,
    /// Own activation status.
    pub own_status: u32,
    /// Own process result.
    pub process_status: i32,
    /// Own awake timestamp.
    pub awake_time: u64,
    /// Own finish timestamp.
    pub finish_time: u64,
    /// Synchronous IO status.
    pub io_status: i32,
    /// Synchronous IO selected buffer.
    pub io_buffer_id: u32,
    /// Selected chunk fields `(offset, size, stride, flags)`.
    pub chunk: (u32, u32, i32, u32),
    /// Downstream pending count.
    pub peer_pending: i32,
    /// Downstream activation status.
    pub peer_status: u32,
    /// Downstream signal timestamp.
    pub peer_signal_time: u64,
    /// Downstream eventfd counter.
    pub peer_event_count: u64,
    /// Hidden own completion eventfd counter.
    pub completion_event_count: u64,
    /// Selected media bytes.
    pub media: Vec<u8>,
}

#[derive(Debug, Default)]
struct Coordination {
    callbacks: u64,
    completed_callbacks: u64,
    hold_second: bool,
    second_entered: bool,
    release_second: bool,
    stale_confirmed: bool,
    media_removed: bool,
    teardown_sent: bool,
    snapshot: Option<CycleSnapshot>,
}

#[derive(Debug)]
struct Shared {
    resources: Mutex<Option<Resources>>,
    coordination: Mutex<Coordination>,
    changed: Condvar,
}

/// Cloneable fixture retaining the server-side views and handles needed for observation.
#[derive(Clone, Debug)]
pub struct ClientNodeFixture(Arc<Shared>);

impl ClientNodeFixture {
    /// Creates sealed, shared, exactly initialized fixture resources.
    pub fn new(name: &str) -> io::Result<Self> {
        let memories = [
            MemoryRole::OwnActivation,
            MemoryRole::Metadata,
            MemoryRole::Media,
            MemoryRole::Io,
            MemoryRole::PeerActivation,
        ]
        .map(|role| MappedMemory::new(name, role));
        let memories = memories.into_iter().collect::<io::Result<Vec<_>>>()?;
        let mut resources = Resources {
            memories,
            trigger: eventfd()?,
            completion: eventfd()?,
            peer_signal: eventfd()?,
        };
        resources.initialize();
        Ok(Self(Arc::new(Shared {
            resources: Mutex::new(Some(resources)),
            coordination: Mutex::new(Coordination::default()),
            changed: Condvar::new(),
        })))
    }

    /// Duplicates one memory descriptor for a frame-local FD table.
    pub fn memory_fd(&self, role: MemoryRole) -> io::Result<OwnedFd> {
        self.with_resources(|resources| resources.memory(role).fd.try_clone())?
    }

    /// Duplicates the trigger and hidden completion descriptors.
    pub fn transport_fds(&self) -> io::Result<Vec<OwnedFd>> {
        self.with_resources(|resources| {
            Ok(vec![
                resources.trigger.try_clone()?,
                resources.completion.try_clone()?,
            ])
        })?
    }

    /// Duplicates the downstream signaling descriptor.
    pub fn peer_signal_fd(&self) -> io::Result<OwnedFd> {
        self.with_resources(|resources| resources.peer_signal.try_clone())?
    }

    /// Called by the application callback before borrowing cycle media.
    pub fn before_callback(&self) {
        let mut state = self.0.coordination.lock().unwrap();
        state.callbacks += 1;
        if state.callbacks == 2 && state.hold_second {
            state.second_entered = true;
            self.0.changed.notify_all();
            while !state.release_second {
                state = self.0.changed.wait(state).unwrap();
            }
        }
        self.0.changed.notify_all();
    }

    /// Called by the application callback immediately before returning its committed output.
    pub fn after_callback(&self) {
        let mut state = self.0.coordination.lock().unwrap();
        state.completed_callbacks += 1;
        let completed = state.completed_callbacks;
        if let Some(snapshot) = state.snapshot.as_mut() {
            snapshot.callbacks = completed;
        }
        self.0.changed.notify_all();
    }

    /// Reports runtime diagnostics so the server can bound the coalesced-wake proof.
    pub fn report_diagnostics(&self, missed: u64, stale: u64) {
        let mut state = self.0.coordination.lock().unwrap();
        if missed > 0 || stale > 0 {
            state.stale_confirmed = true;
        }
        if let Some(snapshot) = state.snapshot.as_mut() {
            snapshot.missed_wakes = missed;
            snapshot.stale_wakes = stale;
        }
        self.0.changed.notify_all();
    }

    /// Reports that Core memory retirement is visible to the client importer.
    pub fn report_media_removed(&self) {
        self.0.coordination.lock().unwrap().media_removed = true;
        self.0.changed.notify_all();
    }

    /// Returns whether teardown commands have been emitted by the server driver.
    pub fn teardown_sent(&self) -> bool {
        self.0.coordination.lock().unwrap().teardown_sent
    }

    /// Returns the retained proof snapshot.
    pub fn snapshot(&self) -> Option<CycleSnapshot> {
        self.0.coordination.lock().unwrap().snapshot.clone()
    }

    pub(crate) fn wait_own_ready(&self, deadline: Instant, scenario: &str) -> io::Result<()> {
        self.wait_resources(deadline, scenario, "own activation FINISHED", |resources| {
            resources.read_u32(MemoryRole::OwnActivation, STATUS)
                == ActivationStatus::Finished as u32
                && resources.read_u32(MemoryRole::OwnActivation, CLIENT_VERSION) == 1
        })
    }

    pub(crate) fn trigger(&self, count: u64, buffer_id: u32) -> io::Result<()> {
        self.with_resources(|resources| {
            resources.write_i32(MemoryRole::Io, 0, BufferStatus::NeedData as i32);
            resources.write_u32(MemoryRole::Io, 4, buffer_id);
            resources.write_u32(
                MemoryRole::OwnActivation,
                STATUS,
                ActivationStatus::Triggered as u32,
            );
            write_eventfd(&resources.trigger, count)
        })?
    }

    pub(crate) fn wake_only(&self, count: u64) -> io::Result<()> {
        self.with_resources(|resources| write_eventfd(&resources.trigger, count))?
    }

    pub(crate) fn prepare_peer(&self) {
        let _ = self.with_resources(|resources| {
            resources.write_i32(MemoryRole::PeerActivation, REQUIRED, 1);
            resources.write_i32(MemoryRole::PeerActivation, PENDING, 1);
            resources.write_u32(
                MemoryRole::PeerActivation,
                STATUS,
                ActivationStatus::NotTriggered as u32,
            );
        });
    }

    pub(crate) fn wait_callbacks(
        &self,
        count: u64,
        deadline: Instant,
        scenario: &str,
    ) -> io::Result<()> {
        self.wait_coordination(
            deadline,
            scenario,
            &format!("callback count {count}"),
            |state| state.completed_callbacks >= count,
        )
    }

    pub(crate) fn wait_stale(&self, deadline: Instant, scenario: &str) -> io::Result<()> {
        self.wait_coordination(deadline, scenario, "missed/stale diagnostics", |state| {
            state.stale_confirmed
        })
    }

    pub(crate) fn hold_second_callback(&self) {
        self.0.coordination.lock().unwrap().hold_second = true;
    }

    pub(crate) fn wait_second_entered(&self, deadline: Instant, scenario: &str) -> io::Result<()> {
        self.wait_coordination(deadline, scenario, "held callback entry", |state| {
            state.second_entered
        })
    }

    pub(crate) fn wait_media_removed(&self, deadline: Instant, scenario: &str) -> io::Result<()> {
        self.wait_coordination(deadline, scenario, "client media retirement", |state| {
            state.media_removed
        })
    }

    pub(crate) fn release_second_callback(&self) {
        self.0.coordination.lock().unwrap().release_second = true;
        self.0.changed.notify_all();
    }

    pub(crate) fn assert_first_cycle(&self) -> io::Result<()> {
        let mut snapshot = self.with_resources(Resources::snapshot)?;
        let state = self.0.coordination.lock().unwrap();
        snapshot.callbacks = state.completed_callbacks;
        drop(state);
        let expected_chunk = (0, 16, 4, 0);
        if snapshot.media != PCM_BYTES
            || snapshot.chunk != expected_chunk
            || snapshot.io_status != BufferStatus::HaveData as i32
            || snapshot.io_buffer_id != 1
            || snapshot.process_status != BufferStatus::HaveData as i32
            || snapshot.own_status != ActivationStatus::Finished as u32
            || snapshot.awake_time == 0
            || snapshot.finish_time < snapshot.awake_time
            || snapshot.peer_pending != 0
            || snapshot.peer_status != ActivationStatus::Triggered as u32
            || snapshot.peer_signal_time != snapshot.finish_time
            || snapshot.peer_event_count != 1
            || snapshot.completion_event_count != 0
        {
            return Err(io::Error::other(format!(
                "linked cycle assertion failed: {snapshot:?} expected_media={PCM_BYTES:?} expected_chunk={expected_chunk:?}"
            )));
        }
        self.0.coordination.lock().unwrap().snapshot = Some(snapshot);
        Ok(())
    }

    pub(crate) fn assert_held_cycle_published(&self) -> io::Result<()> {
        let snapshot = self.with_resources(Resources::snapshot)?;
        if snapshot.media != PCM_BYTES
            || snapshot.chunk != (0, 16, 4, 0)
            || snapshot.io_status != BufferStatus::HaveData as i32
            || snapshot.io_buffer_id != 1
            || snapshot.peer_event_count != 1
        {
            return Err(io::Error::other(format!(
                "held cycle did not publish through its retained generation: {snapshot:?}"
            )));
        }
        Ok(())
    }

    pub(crate) fn wait_peer_signal(&self, deadline: Instant, scenario: &str) -> io::Result<()> {
        self.wait_resources(deadline, scenario, "held cycle peer signal", |resources| {
            peek_eventfd(&resources.peer_signal) == 1
        })
    }

    pub(crate) fn mark_teardown_sent(&self) {
        self.0.coordination.lock().unwrap().teardown_sent = true;
        self.0.changed.notify_all();
    }

    /// Drops all named memfd mappings and retained eventfd handles.
    pub fn release_resources(&self) {
        self.0.resources.lock().unwrap().take();
    }

    fn with_resources<T>(&self, operation: impl FnOnce(&Resources) -> T) -> io::Result<T> {
        let resources = self.0.resources.lock().unwrap();
        resources
            .as_ref()
            .map(operation)
            .ok_or_else(|| io::Error::other("ClientNode fixture resources were released"))
    }

    fn wait_resources(
        &self,
        deadline: Instant,
        scenario: &str,
        step: &str,
        predicate: impl Fn(&Resources) -> bool,
    ) -> io::Result<()> {
        loop {
            if self.with_resources(&predicate)? {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(self.timeout_error(scenario, step));
            }
            std::thread::sleep(Duration::from_millis(1));
        }
    }

    fn wait_coordination(
        &self,
        deadline: Instant,
        scenario: &str,
        step: &str,
        predicate: impl Fn(&Coordination) -> bool,
    ) -> io::Result<()> {
        let mut state = self.0.coordination.lock().unwrap();
        while !predicate(&state) {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                drop(state);
                return Err(self.timeout_error(scenario, step));
            }
            (state, _) = self.0.changed.wait_timeout(state, remaining).unwrap();
        }
        Ok(())
    }

    fn timeout_error(&self, scenario: &str, step: &str) -> io::Error {
        let coordination = self.0.coordination.lock().unwrap();
        let resources = self
            .0
            .resources
            .lock()
            .unwrap()
            .as_ref()
            .map(Resources::diagnostics)
            .unwrap_or_else(|| "resources=released".into());
        io::Error::new(
            io::ErrorKind::TimedOut,
            format!(
                "scenario={scenario} last_step={step} callbacks_entered={} callbacks_completed={} stale_confirmed={} media_removed={} {resources}; roles=own-activation,metadata,media,io,peer-activation,trigger,hidden-completion,peer-signal",
                coordination.callbacks,
                coordination.completed_callbacks,
                coordination.stale_confirmed,
                coordination.media_removed
            ),
        )
    }
}

#[derive(Debug)]
struct Resources {
    memories: Vec<MappedMemory>,
    trigger: OwnedFd,
    completion: OwnedFd,
    peer_signal: OwnedFd,
}

impl Resources {
    fn memory(&self, role: MemoryRole) -> &MappedMemory {
        &self.memories[role.index()]
    }

    fn initialize(&mut self) {
        self.write_u32(
            MemoryRole::OwnActivation,
            STATUS,
            ActivationStatus::Inactive as u32,
        );
        self.write_u32(MemoryRole::OwnActivation, SERVER_VERSION, 1);
        self.write_u32(
            MemoryRole::PeerActivation,
            STATUS,
            ActivationStatus::NotTriggered as u32,
        );
        self.write_u32(MemoryRole::PeerActivation, SERVER_VERSION, 1);
        self.write_i32(MemoryRole::PeerActivation, REQUIRED, 1);
        self.write_i32(MemoryRole::PeerActivation, PENDING, 1);
        self.write_i32(MemoryRole::Io, 0, BufferStatus::NeedData as i32);
        self.write_u32(MemoryRole::Io, 4, 1);
    }

    fn read_u32(&self, role: MemoryRole, offset: usize) -> u32 {
        unsafe {
            self.memory(role)
                .pointer
                .add(offset)
                .cast::<u32>()
                .read_volatile()
        }
    }

    fn read_i32(&self, role: MemoryRole, offset: usize) -> i32 {
        unsafe {
            self.memory(role)
                .pointer
                .add(offset)
                .cast::<i32>()
                .read_volatile()
        }
    }

    fn read_u64(&self, role: MemoryRole, offset: usize) -> u64 {
        unsafe {
            self.memory(role)
                .pointer
                .add(offset)
                .cast::<u64>()
                .read_volatile()
        }
    }

    fn write_u32(&self, role: MemoryRole, offset: usize, value: u32) {
        unsafe {
            self.memory(role)
                .pointer
                .add(offset)
                .cast::<u32>()
                .write_volatile(value)
        }
    }

    fn write_i32(&self, role: MemoryRole, offset: usize, value: i32) {
        unsafe {
            self.memory(role)
                .pointer
                .add(offset)
                .cast::<i32>()
                .write_volatile(value)
        }
    }

    fn snapshot(&self) -> CycleSnapshot {
        let media = unsafe {
            std::slice::from_raw_parts(self.memory(MemoryRole::Media).pointer.add(64), 16)
        };
        CycleSnapshot {
            own_status: self.read_u32(MemoryRole::OwnActivation, STATUS),
            process_status: self.read_i32(MemoryRole::OwnActivation, PROCESS_STATUS),
            awake_time: self.read_u64(MemoryRole::OwnActivation, AWAKE_TIME),
            finish_time: self.read_u64(MemoryRole::OwnActivation, FINISH_TIME),
            io_status: self.read_i32(MemoryRole::Io, 0),
            io_buffer_id: self.read_u32(MemoryRole::Io, 4),
            chunk: (
                self.read_u32(MemoryRole::Metadata, 16),
                self.read_u32(MemoryRole::Metadata, 20),
                self.read_i32(MemoryRole::Metadata, 24),
                self.read_u32(MemoryRole::Metadata, 28),
            ),
            peer_pending: self.read_i32(MemoryRole::PeerActivation, PENDING),
            peer_status: self.read_u32(MemoryRole::PeerActivation, STATUS),
            peer_signal_time: self.read_u64(MemoryRole::PeerActivation, SIGNAL_TIME),
            peer_event_count: read_eventfd(&self.peer_signal).unwrap_or(u64::MAX),
            completion_event_count: read_eventfd(&self.completion).unwrap_or(u64::MAX),
            media: media.to_vec(),
            ..CycleSnapshot::default()
        }
    }

    fn diagnostics(&self) -> String {
        format!(
            "own_status={} process_status={} awake={} finish={} io_status={} io_buffer={} peer_status={} peer_pending={} trigger_count={} completion_count={} peer_count={}",
            self.read_u32(MemoryRole::OwnActivation, STATUS),
            self.read_i32(MemoryRole::OwnActivation, PROCESS_STATUS),
            self.read_u64(MemoryRole::OwnActivation, AWAKE_TIME),
            self.read_u64(MemoryRole::OwnActivation, FINISH_TIME),
            self.read_i32(MemoryRole::Io, 0),
            self.read_u32(MemoryRole::Io, 4),
            self.read_u32(MemoryRole::PeerActivation, STATUS),
            self.read_i32(MemoryRole::PeerActivation, PENDING),
            peek_eventfd(&self.trigger),
            peek_eventfd(&self.completion),
            peek_eventfd(&self.peer_signal),
        )
    }
}

#[derive(Debug)]
struct MappedMemory {
    fd: OwnedFd,
    pointer: *mut u8,
    len: usize,
}

unsafe impl Send for MappedMemory {}

impl MappedMemory {
    fn new(name: &str, role: MemoryRole) -> io::Result<Self> {
        let name = CString::new(format!("{name}-{}", role.id()))
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "memfd name contained NUL"))?;
        let raw = unsafe {
            libc::memfd_create(name.as_ptr(), libc::MFD_CLOEXEC | libc::MFD_ALLOW_SEALING)
        };
        if raw < 0 {
            return Err(io::Error::last_os_error());
        }
        let fd = unsafe { OwnedFd::from_raw_fd(raw) };
        if unsafe { libc::ftruncate(fd.as_raw_fd(), role.size() as libc::off_t) } < 0 {
            return Err(io::Error::last_os_error());
        }
        let pointer = unsafe {
            libc::mmap(
                ptr::null_mut(),
                role.size(),
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_SHARED,
                fd.as_raw_fd(),
                0,
            )
        };
        if pointer == libc::MAP_FAILED {
            return Err(io::Error::last_os_error());
        }
        if unsafe { libc::fcntl(fd.as_raw_fd(), libc::F_ADD_SEALS, libc::F_SEAL_SHRINK) } < 0 {
            unsafe { libc::munmap(pointer, role.size()) };
            return Err(io::Error::last_os_error());
        }
        Ok(Self {
            fd,
            pointer: pointer.cast(),
            len: role.size(),
        })
    }
}

impl Drop for MappedMemory {
    fn drop(&mut self) {
        unsafe { libc::munmap(self.pointer.cast(), self.len) };
    }
}

fn eventfd() -> io::Result<OwnedFd> {
    let raw = unsafe { libc::eventfd(0, libc::EFD_CLOEXEC | libc::EFD_NONBLOCK) };
    if raw < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(unsafe { OwnedFd::from_raw_fd(raw) })
}

fn write_eventfd(fd: &OwnedFd, value: u64) -> io::Result<()> {
    let bytes = value.to_ne_bytes();
    let written = unsafe { libc::write(fd.as_raw_fd(), bytes.as_ptr().cast(), bytes.len()) };
    if written == bytes.len() as isize {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

fn read_eventfd(fd: &OwnedFd) -> io::Result<u64> {
    let mut value = 0_u64;
    let read = unsafe {
        libc::read(
            fd.as_raw_fd(),
            (&mut value as *mut u64).cast(),
            std::mem::size_of::<u64>(),
        )
    };
    if read == std::mem::size_of::<u64>() as isize {
        Ok(value)
    } else if read < 0 && io::Error::last_os_error().kind() == io::ErrorKind::WouldBlock {
        Ok(0)
    } else {
        Err(io::Error::last_os_error())
    }
}

fn peek_eventfd(fd: &OwnedFd) -> u64 {
    std::fs::read_to_string(format!("/proc/self/fdinfo/{}", fd.as_raw_fd()))
        .ok()
        .and_then(|text| {
            text.lines().find_map(|line| {
                line.strip_prefix("eventfd-count:")
                    .and_then(|value| u64::from_str_radix(value.trim(), 16).ok())
            })
        })
        .unwrap_or(u64::MAX)
}
