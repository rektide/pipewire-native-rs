// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: Copyright (c) 2026 Asymptotic Inc.

//! Runtime-independent single-owner ClientNode session state machine.

use std::collections::BTreeMap;

use pipewire_native_protocol::wire::client_node as wire;

use super::{
    config::{
        BufferSetDescriptor, NegotiatedAudioFormat, NodeId, PeerActivationDescriptor,
        PeerActivationUpdate, PortIoDescriptor, PortIoUpdate, TransportDescriptor,
    },
    error::SessionError,
    memory::{MemoryError, MemoryId, MemoryInterval, MemoryResolver},
    output::{OutputGeneration, OutputProcess},
    peer::{PeerActivation, PeerSet},
    transport::{ClaimCompletion, TransportGeneration},
};
use crate::signal::EventFd;

const MAX_SESSION_PEERS: usize = 64;

/// Node command intent retained independently from graph-active intent.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NodeCommandState {
    /// Not started or explicitly paused.
    Pause,
    /// Start once configuration and active intent are both present.
    Start,
    /// Configuration-discarding suspension.
    Suspend,
}

/// Observable state of one ClientNode session owner.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SessionState {
    /// Required configuration or memory is absent.
    Configuring,
    /// Complete configuration is bound but not scheduled.
    Ready,
    /// Active and accepting process claims.
    Running,
    /// Callback currently owns a cycle borrow.
    CycleClaimed,
    /// Configuration remains but scheduling is paused.
    Stopped,
    /// A claimed cycle encountered a fatal callback or propagation error.
    Failed,
    /// Terminal teardown completed.
    Disconnected,
}

/// Owned command seam between canonical protocol conversion and the session owner.
#[derive(Debug)]
pub enum SessionCommand {
    /// Install or replace transport.
    ReplaceTransport(TransportDescriptor),
    /// Install an exact supported format.
    SetFormat(NegotiatedAudioFormat),
    /// Clear format and dependent buffers/IO.
    ClearFormat,
    /// Install or replace output buffers.
    UseBuffers(BufferSetDescriptor),
    /// Clear output buffers and IO.
    ClearBuffers,
    /// Install synchronous Buffers IO.
    SetPortIo(PortIoDescriptor),
    /// Clear synchronous Buffers IO.
    ClearPortIo,
    /// Install/replace a downstream activation.
    SetPeerActivation(PeerActivationDescriptor),
    /// Idempotently remove a downstream activation.
    RemovePeerActivation(NodeId),
    /// Notify that an unresolved memory ID may now resolve.
    MemoryAvailable(MemoryId),
    /// Invalidate mappings retaining this exact retired memory generation.
    RemoveMemory(super::memory::MemoryKey),
    /// Join or leave graph scheduling.
    SetActive(bool),
    /// Apply Start, Pause, or Suspend intent.
    SetNodeCommand(NodeCommandState),
    /// Terminal idempotent teardown.
    Disconnect,
}

impl SessionCommand {
    /// Converts one canonical ClientNode event into a semantic session command.
    pub fn from_wire(event: wire::Event) -> Result<Self, SessionError> {
        match event {
            wire::Event::Transport(value) => Ok(Self::ReplaceTransport(value.try_into()?)),
            wire::Event::PortSetParam(value) => {
                if value.param.is_none() {
                    super::config::validate_port(value.direction, value.port_id, None)?;
                    if value.param_id != wire::SPA_PARAM_FORMAT {
                        return Err(SessionError::Unsupported(
                            super::config::UnsupportedFeature::Parameter(value.param_id),
                        ));
                    }
                    Ok(Self::ClearFormat)
                } else {
                    Ok(Self::SetFormat(NegotiatedAudioFormat::from_wire(&value)?))
                }
            }
            wire::Event::PortUseBuffers(value) => {
                let descriptor = BufferSetDescriptor::try_from(value)?;
                if descriptor.buffers.is_empty() {
                    Ok(Self::ClearBuffers)
                } else {
                    Ok(Self::UseBuffers(descriptor))
                }
            }
            wire::Event::PortSetIo(value) => match PortIoUpdate::try_from(value)? {
                PortIoUpdate::Set(value) => Ok(Self::SetPortIo(value)),
                PortIoUpdate::Clear => Ok(Self::ClearPortIo),
            },
            wire::Event::SetActivation(value) => match PeerActivationUpdate::try_from(value)? {
                PeerActivationUpdate::Set(value) => Ok(Self::SetPeerActivation(value)),
                PeerActivationUpdate::Remove(node) => Ok(Self::RemovePeerActivation(node)),
            },
            wire::Event::Command(value) => Ok(Self::SetNodeCommand(match value {
                wire::Command::Start => NodeCommandState::Start,
                wire::Command::Pause => NodeCommandState::Pause,
                wire::Command::Suspend => NodeCommandState::Suspend,
            })),
        }
    }
}

/// Result of applying a command between callbacks.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ApplyOutcome {
    /// Command applied and current dependencies are bound.
    Applied,
    /// Command is retained pending one or more memory IDs.
    PendingMemory,
    /// Disconnect had already completed.
    AlreadyDisconnected,
}

/// Result of consuming one runtime wake hint.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WakeOutcome {
    /// Nonblocking eventfd had no counter to drain.
    NoWake,
    /// Wake was tagged with a retired generation and was not drained by this owner.
    Stale {
        /// Current generation.
        expected: u64,
        /// Wake registration generation.
        received: u64,
    },
    /// Counter was drained while scheduling was stopped.
    NotRunning {
        /// Coalesced eventfd count.
        drained: u64,
    },
    /// Counter was drained but activation was not TRIGGERED.
    NotClaimed {
        /// Coalesced eventfd count.
        drained: u64,
        /// Additional counts that cannot authorize callbacks.
        missed: u64,
    },
    /// Exactly one callback completed.
    Processed {
        /// Coalesced eventfd count.
        drained: u64,
        /// Additional counts that did not run callbacks.
        missed: u64,
        /// Whether synchronous IO requested and received media.
        produced: bool,
    },
}

/// Runtime readiness registration for one exact transport generation.
#[derive(Debug)]
pub struct RuntimeRegistration {
    /// Generation that must be supplied with wakes from this handle.
    pub generation: u64,
    /// Safely duplicated trigger eventfd. Replacement snapshots remain valid handles
    /// but their generation is rejected by the session as stale.
    pub trigger: EventFd,
}

/// Coherent owner of one node's mappings, descriptors, state, and cycle authority.
#[derive(Debug)]
pub struct ClientNodeSession<R> {
    memory: R,
    next_generation: u64,
    transport: Option<TransportGeneration>,
    pending_transport: Option<TransportDescriptor>,
    format: Option<NegotiatedAudioFormat>,
    buffer_descriptor: Option<BufferSetDescriptor>,
    io_descriptor: Option<PortIoDescriptor>,
    output: Option<OutputGeneration>,
    peers: PeerSet,
    pending_peers: BTreeMap<NodeId, PeerActivationDescriptor>,
    active_requested: bool,
    command: NodeCommandState,
    state: SessionState,
}

impl<R: MemoryResolver> ClientNodeSession<R> {
    /// Creates an empty configuring session over a runtime-neutral resolver.
    pub fn new(memory: R) -> Self {
        Self {
            memory,
            next_generation: 1,
            transport: None,
            pending_transport: None,
            format: None,
            buffer_descriptor: None,
            io_descriptor: None,
            output: None,
            peers: PeerSet::default(),
            pending_peers: BTreeMap::new(),
            active_requested: false,
            command: NodeCommandState::Pause,
            state: SessionState::Configuring,
        }
    }

    /// Current runtime state.
    pub const fn state(&self) -> SessionState {
        self.state
    }

    /// Current transport generation for readiness tagging.
    pub fn transport_generation(&self) -> Option<u64> {
        self.transport.as_ref().map(TransportGeneration::id)
    }

    /// Clones the current trigger registration without exposing the completion fd.
    pub fn runtime_registration(&self) -> Result<Option<RuntimeRegistration>, SessionError> {
        self.transport
            .as_ref()
            .map(|transport| {
                Ok(RuntimeRegistration {
                    generation: transport.id(),
                    trigger: transport.try_clone_trigger()?,
                })
            })
            .transpose()
    }

    /// Number of installed downstream peers.
    pub fn peer_count(&self) -> usize {
        self.peers.len()
    }

    /// Applies one command while no callback borrow exists.
    pub fn apply(&mut self, command: SessionCommand) -> Result<ApplyOutcome, SessionError> {
        if self.state == SessionState::Disconnected {
            return if matches!(command, SessionCommand::Disconnect) {
                Ok(ApplyOutcome::AlreadyDisconnected)
            } else {
                Err(SessionError::Disconnected)
            };
        }
        if self.state == SessionState::CycleClaimed {
            return Err(SessionError::InvalidTransition(
                "configuration during callback",
            ));
        }
        let outcome = match command {
            SessionCommand::ReplaceTransport(value) => {
                self.pending_transport = Some(value);
                self.bind_transport()?
            }
            SessionCommand::SetFormat(value) => {
                let replacement = self
                    .format
                    .as_ref()
                    .is_some_and(|current| current != &value);
                self.format = Some(value);
                if replacement {
                    self.stop()?;
                    self.buffer_descriptor = None;
                    self.io_descriptor = None;
                    self.output = None;
                }
                self.bind_output()?
            }
            SessionCommand::ClearFormat => {
                self.stop()?;
                self.output = None;
                self.io_descriptor = None;
                self.buffer_descriptor = None;
                self.format = None;
                ApplyOutcome::Applied
            }
            SessionCommand::UseBuffers(value) => {
                let replacement = self
                    .buffer_descriptor
                    .as_ref()
                    .is_some_and(|current| current != &value);
                if replacement {
                    let format = self.format.clone();
                    let io = self.io_descriptor;
                    let pending = if let (Some(format), Some(io)) = (format, io) {
                        self.output_candidate(format, &value, io)?.is_none()
                    } else {
                        false
                    };
                    self.stop()?;
                    self.output = None;
                    self.io_descriptor = None;
                    self.buffer_descriptor = Some(value);
                    if pending {
                        ApplyOutcome::PendingMemory
                    } else {
                        ApplyOutcome::Applied
                    }
                } else {
                    self.buffer_descriptor = Some(value);
                    self.bind_output()?
                }
            }
            SessionCommand::ClearBuffers => {
                self.stop()?;
                self.output = None;
                self.io_descriptor = None;
                self.buffer_descriptor = None;
                ApplyOutcome::Applied
            }
            SessionCommand::SetPortIo(value) => {
                let format = self.format.clone();
                let buffers = self.buffer_descriptor.clone();
                if let (Some(format), Some(buffers)) = (format, buffers) {
                    match self.output_candidate(format, &buffers, value)? {
                        Some(candidate) => {
                            self.stop()?;
                            self.io_descriptor = Some(value);
                            self.output = Some(candidate);
                            ApplyOutcome::Applied
                        }
                        None => {
                            self.stop()?;
                            self.io_descriptor = Some(value);
                            self.output = None;
                            ApplyOutcome::PendingMemory
                        }
                    }
                } else {
                    self.io_descriptor = Some(value);
                    ApplyOutcome::Applied
                }
            }
            SessionCommand::ClearPortIo => {
                self.stop()?;
                self.output = None;
                self.io_descriptor = None;
                ApplyOutcome::Applied
            }
            SessionCommand::SetPeerActivation(value) => {
                let node = value.node;
                if !self.pending_peers.contains_key(&node)
                    && !self.peers.contains(node)
                    && self.pending_peers.len().saturating_add(self.peers.len())
                        >= MAX_SESSION_PEERS
                {
                    return Err(SessionError::TooManyDescriptors("peer activations"));
                }
                self.pending_peers.insert(node, value);
                self.bind_peer(node)?
            }
            SessionCommand::RemovePeerActivation(node) => {
                self.pending_peers.remove(&node);
                self.peers.remove(node);
                ApplyOutcome::Applied
            }
            SessionCommand::MemoryAvailable(_) => self.retry_pending()?,
            SessionCommand::RemoveMemory(key) => {
                self.remove_memory(key)?;
                ApplyOutcome::Applied
            }
            SessionCommand::SetActive(active) => {
                self.active_requested = active;
                if active {
                    self.ensure_running()?
                } else {
                    self.stop()?
                }
                ApplyOutcome::Applied
            }
            SessionCommand::SetNodeCommand(command) => {
                self.command = command;
                match command {
                    NodeCommandState::Start => self.ensure_running()?,
                    NodeCommandState::Pause => self.stop()?,
                    NodeCommandState::Suspend => {
                        self.stop()?;
                        self.output = None;
                        self.io_descriptor = None;
                        self.buffer_descriptor = None;
                        self.format = None;
                    }
                }
                ApplyOutcome::Applied
            }
            SessionCommand::Disconnect => {
                let _ = self.stop();
                self.pending_peers.clear();
                self.peers = PeerSet::default();
                self.output = None;
                self.io_descriptor = None;
                self.buffer_descriptor = None;
                self.format = None;
                self.pending_transport = None;
                self.transport = None;
                self.state = SessionState::Disconnected;
                ApplyOutcome::Applied
            }
        };
        self.refresh_state();
        if self.active_requested && self.command == NodeCommandState::Start && self.is_ready() {
            self.ensure_running()?;
        }
        Ok(outcome)
    }

    /// Drains and processes at most one callback for the current transport generation.
    pub fn on_wake(
        &mut self,
        generation: u64,
        now_ns: u64,
        callback: &mut dyn OutputProcess,
    ) -> Result<WakeOutcome, SessionError> {
        self.on_wake_with_finish(generation, now_ns, || now_ns, callback)
    }

    /// Processes a wake with a finish clock sampled after callback completion.
    pub fn on_wake_with_finish(
        &mut self,
        generation: u64,
        awake_ns: u64,
        finish_time: impl FnOnce() -> u64,
        callback: &mut dyn OutputProcess,
    ) -> Result<WakeOutcome, SessionError> {
        let expected = self
            .transport_generation()
            .ok_or(SessionError::NotReady("transport"))?;
        if generation != expected {
            return Ok(WakeOutcome::Stale {
                expected,
                received: generation,
            });
        }
        let drained = match self.transport.as_ref().unwrap().drain() {
            Ok(value) => value,
            Err(SessionError::Signal(error)) if error.kind() == std::io::ErrorKind::WouldBlock => {
                return Ok(WakeOutcome::NoWake)
            }
            Err(error) => return Err(error),
        };
        if self.state != SessionState::Running {
            return Ok(WakeOutcome::NotRunning { drained });
        }
        let missed = drained.saturating_sub(1);
        self.state = SessionState::CycleClaimed;
        let completion = {
            let transport = self.transport.as_mut().unwrap();
            let output = self.output.as_mut().unwrap();
            transport.claim_and_finish(awake_ns, finish_time, || output.process(callback))
        };
        let completion = match completion {
            Ok(value) => value,
            Err(error) => {
                self.fail();
                return Err(error);
            }
        };
        match completion {
            ClaimCompletion::NotClaimed => {
                self.state = SessionState::Running;
                Ok(WakeOutcome::NotClaimed { drained, missed })
            }
            ClaimCompletion::Finished { result, finish_ns } => match result {
                Ok(published) => match self.peers.trigger_all(finish_ns) {
                    Ok(()) => {
                        self.state = SessionState::Running;
                        Ok(WakeOutcome::Processed {
                            drained,
                            missed,
                            produced: published.is_some(),
                        })
                    }
                    Err(error) => {
                        self.fail();
                        Err(error)
                    }
                },
                Err(error) => {
                    if matches!(error, SessionError::FinishClockPanicked) {
                        let _ = self.output.as_mut().unwrap().abort_published();
                    }
                    self.fail();
                    Err(error)
                }
            },
        }
    }

    fn bind_transport(&mut self) -> Result<ApplyOutcome, SessionError> {
        let Some(descriptor) = self.pending_transport.as_ref() else {
            return Ok(ApplyOutcome::Applied);
        };
        if unresolved(self.memory.resolve(descriptor.activation.memory))? {
            return Ok(ApplyOutcome::PendingMemory);
        }
        let descriptor = self.pending_transport.take().unwrap();
        let generation = self.allocate_generation()?;
        let candidate = TransportGeneration::bind(generation, descriptor, &self.memory)?;
        let existing: Vec<_> = self
            .output
            .iter()
            .flat_map(OutputGeneration::intervals)
            .chain(self.peers.intervals())
            .collect();
        ensure_disjoint(std::iter::once(candidate.interval()), existing)?;
        self.stop()?;
        self.transport = Some(candidate);
        Ok(ApplyOutcome::Applied)
    }

    fn bind_output(&mut self) -> Result<ApplyOutcome, SessionError> {
        let (Some(format), Some(buffers), Some(io)) = (
            self.format.clone(),
            self.buffer_descriptor.clone(),
            self.io_descriptor,
        ) else {
            return Ok(ApplyOutcome::Applied);
        };
        let Some(candidate) = self.output_candidate(format, &buffers, io)? else {
            return Ok(ApplyOutcome::PendingMemory);
        };
        self.stop()?;
        self.output = Some(candidate);
        Ok(ApplyOutcome::Applied)
    }

    fn output_candidate(
        &mut self,
        format: NegotiatedAudioFormat,
        buffers: &BufferSetDescriptor,
        io: PortIoDescriptor,
    ) -> Result<Option<OutputGeneration>, SessionError> {
        for id in buffer_memory_ids(buffers).chain(std::iter::once(io.region.memory)) {
            if unresolved(self.memory.resolve(id))? {
                return Ok(None);
            }
        }
        let generation = self.allocate_generation()?;
        let candidate = OutputGeneration::bind(generation, format, buffers, io, &self.memory)?;
        let existing: Vec<_> = self
            .transport
            .iter()
            .map(TransportGeneration::interval)
            .chain(self.peers.intervals())
            .collect();
        ensure_disjoint(candidate.intervals(), existing)?;
        Ok(Some(candidate))
    }

    fn bind_peer(&mut self, node: NodeId) -> Result<ApplyOutcome, SessionError> {
        let descriptor = self.pending_peers.get(&node).unwrap();
        if unresolved(self.memory.resolve(descriptor.activation.memory))? {
            return Ok(ApplyOutcome::PendingMemory);
        }
        let descriptor = self.pending_peers.remove(&node).unwrap();
        let generation = self.allocate_generation()?;
        let candidate = PeerActivation::bind(generation, descriptor, &self.memory)?;
        let existing: Vec<_> = self
            .transport
            .iter()
            .map(TransportGeneration::interval)
            .chain(self.output.iter().flat_map(OutputGeneration::intervals))
            .chain(self.peers.intervals_except(node))
            .collect();
        ensure_disjoint(std::iter::once(candidate.interval()), existing)?;
        self.peers.insert(candidate);
        Ok(ApplyOutcome::Applied)
    }

    fn retry_pending(&mut self) -> Result<ApplyOutcome, SessionError> {
        let mut pending = false;
        if self.pending_transport.is_some() {
            pending |= self.bind_transport()? == ApplyOutcome::PendingMemory;
        }
        if self.buffer_descriptor.is_some() && self.io_descriptor.is_some() {
            pending |= self.bind_output()? == ApplyOutcome::PendingMemory;
        }
        let nodes: Vec<_> = self.pending_peers.keys().copied().collect();
        for node in nodes {
            pending |= self.bind_peer(node)? == ApplyOutcome::PendingMemory;
        }
        Ok(if pending {
            ApplyOutcome::PendingMemory
        } else {
            ApplyOutcome::Applied
        })
    }

    fn remove_memory(&mut self, key: super::memory::MemoryKey) -> Result<(), SessionError> {
        if self
            .transport
            .as_ref()
            .is_some_and(|transport| transport.activation_key() == key)
        {
            self.stop()?;
            self.transport = None;
        }
        if self
            .output
            .as_ref()
            .is_some_and(|output| output.depends_on(key))
        {
            self.stop()?;
            self.output = None;
        }
        self.peers.remove_memory(key);
        Ok(())
    }

    fn ensure_running(&mut self) -> Result<(), SessionError> {
        if !self.active_requested || self.command != NodeCommandState::Start {
            return Ok(());
        }
        if !self.is_ready() {
            self.state = SessionState::Configuring;
            return Err(SessionError::NotReady("Start has unresolved configuration"));
        }
        let transport = self.transport.as_mut().unwrap();
        match transport.status()? {
            super::activation::ActivationStatus::Inactive => transport.mark_ready()?,
            super::activation::ActivationStatus::Finished => {}
            _ => return Err(SessionError::InvalidTransition("Start activation status")),
        }
        self.state = SessionState::Running;
        Ok(())
    }

    fn stop(&mut self) -> Result<(), SessionError> {
        if let Some(transport) = &mut self.transport {
            transport.deactivate()?;
        }
        if self.state != SessionState::Disconnected && self.state != SessionState::Failed {
            self.state = if self.is_ready() {
                SessionState::Stopped
            } else {
                SessionState::Configuring
            };
        }
        Ok(())
    }

    fn fail(&mut self) {
        if let Some(transport) = &mut self.transport {
            let _ = transport.deactivate();
        }
        self.state = SessionState::Failed;
    }

    fn is_ready(&self) -> bool {
        self.transport.is_some() && self.format.is_some() && self.output.is_some()
    }

    fn refresh_state(&mut self) {
        if matches!(
            self.state,
            SessionState::Running
                | SessionState::CycleClaimed
                | SessionState::Failed
                | SessionState::Disconnected
        ) {
            return;
        }
        self.state = if self.is_ready() {
            SessionState::Ready
        } else {
            SessionState::Configuring
        };
    }

    fn allocate_generation(&mut self) -> Result<u64, SessionError> {
        let generation = self.next_generation;
        self.next_generation = generation
            .checked_add(1)
            .ok_or(SessionError::Overflow("session generation"))?;
        Ok(generation)
    }
}

fn unresolved(result: Result<super::memory::MemoryKey, MemoryError>) -> Result<bool, SessionError> {
    match result {
        Ok(_) => Ok(false),
        Err(MemoryError::UnknownMemory(_)) => Ok(true),
        Err(error) => Err(error.into()),
    }
}

fn buffer_memory_ids(value: &BufferSetDescriptor) -> impl Iterator<Item = MemoryId> + '_ {
    value
        .buffers
        .iter()
        .flat_map(|buffer| [buffer.metadata.memory, buffer.media_memory])
}

fn ensure_disjoint(
    candidate: impl IntoIterator<Item = MemoryInterval>,
    existing: impl IntoIterator<Item = MemoryInterval>,
) -> Result<(), SessionError> {
    let existing: Vec<_> = existing.into_iter().collect();
    for left in candidate {
        for right in &existing {
            if left.overlaps(*right) {
                return Err(SessionError::InvalidTransition(
                    "overlapping generation mappings",
                ));
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{
        cell::RefCell,
        os::fd::{AsRawFd, FromRawFd, OwnedFd},
        rc::Rc,
    };

    use pipewire_native_spa::buffer::data_type;

    use super::*;
    use crate::{
        session::{
            activation::{ActivationStatus, ActivationView},
            config::{BufferDescriptor, MetaDescriptor, PortId},
            cycle::{CommittedOutput, OutputCycle},
            memory::{MemoryKey, MemoryMapping, MemoryPool, RegionRef},
            output::ProcessError,
            port::{BufferStatus, BuffersIoView, ChunkState, ChunkView, PortIoType},
        },
        shm::{create_memfd, ShrinkPolicy},
        signal::EventFd,
    };

    #[derive(Clone, Debug)]
    struct FakeResolver(Rc<RefCell<MemoryPool>>);

    impl FakeResolver {
        fn new() -> Self {
            Self(Rc::new(RefCell::new(MemoryPool::new(
                ShrinkPolicy::RequireSealed,
            ))))
        }

        fn add(&self, id: u32, len: usize) -> MemoryKey {
            self.0
                .borrow_mut()
                .add(
                    MemoryId(id),
                    data_type::MEM_FD,
                    0,
                    create_memfd(&format!("session-{id}"), len).unwrap(),
                )
                .unwrap()
        }

        fn add_aliases(&self, ids: &[u32], len: usize) -> Vec<MemoryKey> {
            let fd = create_memfd("session-alias", len).unwrap();
            let mut pool = self.0.borrow_mut();
            ids.iter()
                .map(|id| {
                    let imported = fd.try_clone().unwrap();
                    pool.add(MemoryId(*id), data_type::MEM_FD, 0, imported)
                        .unwrap()
                })
                .collect()
        }

        fn bytes(&self, key: MemoryKey, len: usize) -> MemoryMapping {
            self.0.borrow().map(key, 0, len, true).unwrap()
        }

        fn remove(&self, id: MemoryId) -> MemoryKey {
            self.0.borrow_mut().remove(id).unwrap()
        }
    }

    impl MemoryResolver for FakeResolver {
        fn resolve(&self, id: MemoryId) -> Result<MemoryKey, MemoryError> {
            self.0.borrow().resolve(id)
        }

        fn map(
            &self,
            key: MemoryKey,
            offset: usize,
            len: usize,
            writable: bool,
        ) -> Result<MemoryMapping, MemoryError> {
            self.0.borrow().map(key, offset, len, writable)
        }
    }

    fn initialize_activation(
        resolver: &FakeResolver,
        id: u32,
        status: ActivationStatus,
        required: i32,
    ) -> MemoryKey {
        let key = resolver.add(id, ActivationView::required_size());
        let mut mapping = resolver.bytes(key, ActivationView::required_size());
        let mut guard = mapping.borrow();
        let bytes = unsafe { guard.bytes_mut() };
        bytes[0..4].copy_from_slice(&(status as u32).to_ne_bytes());
        bytes[12..16].copy_from_slice(&required.to_ne_bytes());
        bytes[16..20].copy_from_slice(&required.to_ne_bytes());
        bytes[544..548].copy_from_slice(&1_u32.to_ne_bytes());
        key
    }

    fn event_pair() -> (OwnedFd, EventFd) {
        let raw = unsafe { libc::eventfd(0, libc::EFD_CLOEXEC | libc::EFD_NONBLOCK) };
        assert!(raw >= 0);
        let owned = unsafe { OwnedFd::from_raw_fd(raw) };
        let observer = EventFd::from_owned_fd(owned.try_clone().unwrap()).unwrap();
        (owned, observer)
    }

    fn fdinfo(fd: i32) -> Option<String> {
        std::fs::read_to_string(format!("/proc/self/fdinfo/{fd}")).ok()
    }

    fn transport(activation: MemoryId) -> (TransportDescriptor, EventFd, EventFd, i32, i32) {
        let (trigger, trigger_observer) = event_pair();
        let trigger_raw = trigger.as_raw_fd();
        let (completion, completion_observer) = event_pair();
        let completion_raw = completion.as_raw_fd();
        (
            TransportDescriptor {
                trigger_fd: trigger,
                completion_fd: completion,
                activation: RegionRef {
                    memory: activation,
                    offset: 0,
                    len: ActivationView::required_size(),
                },
            },
            trigger_observer,
            completion_observer,
            trigger_raw,
            completion_raw,
        )
    }

    fn output_descriptors() -> (BufferSetDescriptor, PortIoDescriptor) {
        (
            BufferSetDescriptor {
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
            },
            PortIoDescriptor {
                port: PortId(0),
                region: RegionRef {
                    memory: MemoryId(4),
                    offset: 0,
                    len: 8,
                },
            },
        )
    }

    struct PatternProcess {
        calls: usize,
    }

    struct CommitThenFail {
        panic: bool,
    }

    impl OutputProcess for CommitThenFail {
        fn process(&mut self, cycle: OutputCycle<'_>) -> Result<CommittedOutput, ProcessError> {
            let committed = cycle.commit(1).unwrap();
            if self.panic {
                panic!("callback panic after commit");
            }
            let _ = committed;
            Err(ProcessError {
                message: "callback error after commit".into(),
            })
        }
    }

    impl OutputProcess for PatternProcess {
        fn process(&mut self, mut cycle: OutputCycle<'_>) -> Result<CommittedOutput, ProcessError> {
            self.calls += 1;
            assert_eq!(cycle.frame_capacity(), 16);
            cycle.interleaved_pcm()[..16]
                .copy_from_slice(&[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]);
            cycle.commit(4).map_err(|error| ProcessError {
                message: error.to_string(),
            })
        }
    }

    #[test]
    fn processes_exact_cycle_signals_peer_and_ignores_coalesced_stale_wake() {
        let resolver = FakeResolver::new();
        let own_key = initialize_activation(&resolver, 1, ActivationStatus::Inactive, 0);
        let metadata_key = resolver.add(2, 16);
        let media_key = resolver.add(3, 64);
        let io_key = resolver.add(4, 8);
        let peer_key = initialize_activation(&resolver, 5, ActivationStatus::NotTriggered, 1);

        {
            let mut io = resolver.bytes(io_key, 8);
            let mut guard = io.borrow();
            let bytes = unsafe { guard.bytes_mut() };
            bytes[0..4].copy_from_slice(&(BufferStatus::NeedData as i32).to_ne_bytes());
            bytes[4..8].copy_from_slice(&0_u32.to_ne_bytes());
        }

        let (transport, trigger, completion, trigger_raw, completion_raw) = transport(MemoryId(1));
        let trigger_identity = fdinfo(trigger_raw).unwrap();
        let completion_identity = fdinfo(completion_raw).unwrap();
        let (peer_signal_fd, peer_signal) = event_pair();
        let mut session = ClientNodeSession::new(resolver.clone());
        session
            .apply(SessionCommand::ReplaceTransport(transport))
            .unwrap();
        let generation = session.transport_generation().unwrap();
        let registration = session.runtime_registration().unwrap().unwrap();
        assert_eq!(registration.generation, generation);
        assert_ne!(registration.trigger.as_raw_fd(), trigger_raw);
        let (buffers, io) = output_descriptors();

        // IO and buffers may precede format and still converge.
        session.apply(SessionCommand::SetPortIo(io)).unwrap();
        session.apply(SessionCommand::UseBuffers(buffers)).unwrap();
        session
            .apply(SessionCommand::SetFormat(
                NegotiatedAudioFormat::pcm_s16le(48_000, 2).unwrap(),
            ))
            .unwrap();
        session
            .apply(SessionCommand::SetPeerActivation(
                PeerActivationDescriptor {
                    node: NodeId(9),
                    signal_fd: peer_signal_fd,
                    activation: RegionRef {
                        memory: MemoryId(5),
                        offset: 0,
                        len: ActivationView::required_size(),
                    },
                },
            ))
            .unwrap();
        session.apply(SessionCommand::SetActive(true)).unwrap();
        session
            .apply(SessionCommand::SetNodeCommand(NodeCommandState::Start))
            .unwrap();
        assert_eq!(session.state(), SessionState::Running);

        session.apply(SessionCommand::SetActive(false)).unwrap();
        {
            let mut own = resolver.bytes(own_key, ActivationView::required_size());
            let activation =
                unsafe { ActivationView::from_raw_parts(own.as_mut_ptr(), own.len()).unwrap() };
            assert_eq!(activation.status().unwrap(), ActivationStatus::Inactive);
        }
        session.apply(SessionCommand::SetActive(true)).unwrap();
        assert_eq!(session.state(), SessionState::Running);

        {
            let mut own = resolver.bytes(own_key, ActivationView::required_size());
            let activation =
                unsafe { ActivationView::from_raw_parts(own.as_mut_ptr(), own.len()).unwrap() };
            assert_eq!(activation.status().unwrap(), ActivationStatus::Finished);
            activation.deactivate().unwrap();
            let mut guard = own.borrow();
            let bytes = unsafe { guard.bytes_mut() };
            bytes[0..4].copy_from_slice(&(ActivationStatus::Triggered as u32).to_ne_bytes());
        }
        trigger.signal(3).unwrap();
        let mut process = PatternProcess { calls: 0 };
        assert_eq!(
            session
                .on_wake_with_finish(generation, 77, || 91, &mut process)
                .unwrap(),
            WakeOutcome::Processed {
                drained: 3,
                missed: 2,
                produced: true
            }
        );
        assert_eq!(process.calls, 1);

        let mut own = resolver.bytes(own_key, ActivationView::required_size());
        let own_view =
            unsafe { ActivationView::from_raw_parts(own.as_mut_ptr(), own.len()).unwrap() };
        assert_eq!(unsafe { own_view.awake_time() }, 77);
        assert_eq!(unsafe { own_view.finish_time() }, 91);

        let mut media = resolver.bytes(media_key, 64);
        let media_guard = media.borrow();
        assert_eq!(
            &unsafe { media_guard.bytes() }[..16],
            &[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]
        );
        let mut metadata = resolver.bytes(metadata_key, 16);
        let chunk = unsafe { ChunkView::from_raw_parts(metadata.as_mut_ptr(), 16).unwrap() };
        assert_eq!(
            chunk.state(),
            ChunkState {
                offset: 0,
                size: 16,
                stride: 4,
                flags: 0
            }
        );
        let mut io = resolver.bytes(io_key, 8);
        let io_view = unsafe {
            BuffersIoView::from_raw_parts(PortIoType::Buffers, io.as_mut_ptr(), 8).unwrap()
        };
        assert_eq!(io_view.state().status, BufferStatus::HaveData as i32);
        assert_eq!(peer_signal.drain().unwrap(), 1);
        assert_eq!(
            completion.drain().unwrap_err().kind(),
            std::io::ErrorKind::WouldBlock
        );
        let mut peer = resolver.bytes(peer_key, ActivationView::required_size());
        let peer_view =
            unsafe { ActivationView::from_raw_parts(peer.as_mut_ptr(), peer.len()).unwrap() };
        assert_eq!(peer_view.pending(), 0);
        assert_eq!(peer_view.status().unwrap(), ActivationStatus::Triggered);
        assert_eq!(unsafe { peer_view.signal_time() }, 91);

        trigger.signal(2).unwrap();
        assert_eq!(
            session.on_wake(generation, 88, &mut process).unwrap(),
            WakeOutcome::NotClaimed {
                drained: 2,
                missed: 1
            }
        );
        assert_eq!(process.calls, 1);
        assert_eq!(
            session.on_wake(generation + 1, 99, &mut process).unwrap(),
            WakeOutcome::Stale {
                expected: generation,
                received: generation + 1
            }
        );

        let retired_media = resolver.remove(MemoryId(3));
        session
            .apply(SessionCommand::RemoveMemory(retired_media))
            .unwrap();
        assert_eq!(session.state(), SessionState::Configuring);
        assert_eq!(unsafe { media_guard.bytes()[0] }, 0);

        session.apply(SessionCommand::Disconnect).unwrap();
        assert_eq!(
            session.apply(SessionCommand::Disconnect).unwrap(),
            ApplyOutcome::AlreadyDisconnected
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

    #[test]
    fn retains_unresolved_transport_and_preserves_old_generation_on_bad_replacement() {
        let resolver = FakeResolver::new();
        let (descriptor, _trigger, _completion, _, _) = transport(MemoryId(10));
        let mut session = ClientNodeSession::new(resolver.clone());
        assert_eq!(
            session
                .apply(SessionCommand::ReplaceTransport(descriptor))
                .unwrap(),
            ApplyOutcome::PendingMemory
        );
        assert!(session.transport_generation().is_none());
        session.apply(SessionCommand::SetActive(true)).unwrap();
        assert!(matches!(
            session.apply(SessionCommand::SetNodeCommand(NodeCommandState::Start)),
            Err(SessionError::NotReady(_))
        ));
        initialize_activation(&resolver, 10, ActivationStatus::Inactive, 0);
        session
            .apply(SessionCommand::MemoryAvailable(MemoryId(10)))
            .unwrap();
        let old = session.transport_generation().unwrap();

        let resolver_bad_key = resolver.add(11, 8);
        let (bad, _, _, _, _) = transport(MemoryId(11));
        let bad = TransportDescriptor {
            activation: RegionRef {
                memory: resolver_bad_key.id,
                offset: 0,
                len: 8,
            },
            ..bad
        };
        assert!(session
            .apply(SessionCommand::ReplaceTransport(bad))
            .is_err());
        assert_eq!(session.transport_generation(), Some(old));
    }

    #[test]
    fn duplicated_memfd_ids_cannot_alias_output_or_transport_regions() {
        let resolver = FakeResolver::new();
        let keys = resolver.add_aliases(&[1, 2, 3, 4], 4096);
        {
            let mut activation = resolver.bytes(keys[0], ActivationView::required_size());
            let mut guard = activation.borrow();
            let bytes = unsafe { guard.bytes_mut() };
            bytes[0..4].copy_from_slice(&(ActivationStatus::Inactive as u32).to_ne_bytes());
            bytes[544..548].copy_from_slice(&1_u32.to_ne_bytes());
        }
        let (transport, _, _, _, _) = transport(MemoryId(1));
        let mut session = ClientNodeSession::new(resolver.clone());
        session
            .apply(SessionCommand::ReplaceTransport(transport))
            .unwrap();
        session
            .apply(SessionCommand::SetFormat(
                NegotiatedAudioFormat::pcm_s16le(48_000, 2).unwrap(),
            ))
            .unwrap();
        let buffers = BufferSetDescriptor {
            port: PortId(0),
            buffers: vec![BufferDescriptor {
                metadata: RegionRef {
                    memory: MemoryId(2),
                    offset: 3000,
                    len: 16,
                },
                metas: Vec::new().into_boxed_slice(),
                media_memory: MemoryId(3),
                map_offset: 0,
                max_size: 64,
            }]
            .into_boxed_slice(),
        };
        session.apply(SessionCommand::UseBuffers(buffers)).unwrap();
        let io = PortIoDescriptor {
            port: PortId(0),
            region: RegionRef {
                memory: MemoryId(4),
                offset: 3500,
                len: 8,
            },
        };
        assert!(matches!(
            session.apply(SessionCommand::SetPortIo(io)),
            Err(SessionError::InvalidTransition(
                "overlapping generation mappings"
            ))
        ));

        let format = NegotiatedAudioFormat::pcm_s16le(48_000, 2).unwrap();
        let (mut buffers, io) = output_descriptors();
        buffers.buffers[0].metadata.memory = MemoryId(2);
        buffers.buffers[0].media_memory = MemoryId(3);
        let io = PortIoDescriptor {
            region: RegionRef {
                memory: MemoryId(4),
                offset: 8,
                len: 8,
            },
            ..io
        };
        assert!(matches!(
            OutputGeneration::bind(99, format, &buffers, io, &resolver),
            Err(SessionError::InvalidTransition(
                "overlapping writable memory regions"
            ))
        ));
    }

    #[test]
    fn duplicated_handles_reject_every_writable_plane_pair() {
        let resolver = FakeResolver::new();
        let keys = resolver.add_aliases(&[40, 41, 42, 43, 44], 4096);
        let mappings: Vec<_> = keys
            .into_iter()
            .map(|key| resolver.0.borrow().map(key, 0, 64, true).unwrap())
            .collect();
        let planes = ["transport", "metadata/chunk", "media", "IO", "peer"];
        for left in 0..planes.len() {
            for right in left + 1..planes.len() {
                assert!(
                    ensure_disjoint([mappings[left].interval()], [mappings[right].interval()])
                        .is_err(),
                    "{} unexpectedly aliased {}",
                    planes[left],
                    planes[right]
                );
            }
        }
    }

    #[test]
    fn commit_then_error_or_panic_aborts_output_and_deactivates() {
        for panic in [false, true] {
            let resolver = FakeResolver::new();
            let own_key = initialize_activation(&resolver, 1, ActivationStatus::Inactive, 0);
            let metadata_key = resolver.add(2, 16);
            resolver.add(3, 64);
            let io_key = resolver.add(4, 8);
            {
                let mut io = resolver.bytes(io_key, 8);
                let mut guard = io.borrow();
                let bytes = unsafe { guard.bytes_mut() };
                bytes[0..4].copy_from_slice(&(BufferStatus::NeedData as i32).to_ne_bytes());
            }
            let (descriptor, trigger, _, _, _) = transport(MemoryId(1));
            let mut session = ClientNodeSession::new(resolver.clone());
            session
                .apply(SessionCommand::ReplaceTransport(descriptor))
                .unwrap();
            let generation = session.transport_generation().unwrap();
            let (buffers, io) = output_descriptors();
            session.apply(SessionCommand::UseBuffers(buffers)).unwrap();
            session.apply(SessionCommand::SetPortIo(io)).unwrap();
            session
                .apply(SessionCommand::SetFormat(
                    NegotiatedAudioFormat::pcm_s16le(48_000, 2).unwrap(),
                ))
                .unwrap();
            session.apply(SessionCommand::SetActive(true)).unwrap();
            session
                .apply(SessionCommand::SetNodeCommand(NodeCommandState::Start))
                .unwrap();
            {
                let mut own = resolver.bytes(own_key, ActivationView::required_size());
                let mut guard = own.borrow();
                let bytes = unsafe { guard.bytes_mut() };
                bytes[0..4].copy_from_slice(&(ActivationStatus::Triggered as u32).to_ne_bytes());
            }
            trigger.signal(1).unwrap();
            let error = session
                .on_wake_with_finish(generation, 100, || 140, &mut CommitThenFail { panic })
                .unwrap_err();
            assert!(matches!(
                (panic, error),
                (false, SessionError::Callback(_)) | (true, SessionError::CallbackPanicked)
            ));
            assert_eq!(session.state(), SessionState::Failed);

            let mut own = resolver.bytes(own_key, ActivationView::required_size());
            let activation =
                unsafe { ActivationView::from_raw_parts(own.as_mut_ptr(), own.len()).unwrap() };
            assert_eq!(activation.status().unwrap(), ActivationStatus::Inactive);
            assert_eq!(activation.process_result(), -libc::EIO);
            assert_eq!(unsafe { activation.awake_time() }, 100);
            assert_eq!(unsafe { activation.finish_time() }, 140);

            let mut io = resolver.bytes(io_key, 8);
            let io = unsafe {
                BuffersIoView::from_raw_parts(PortIoType::Buffers, io.as_mut_ptr(), 8).unwrap()
            };
            assert_eq!(io.state().status, BufferStatus::NeedData as i32);
            let mut metadata = resolver.bytes(metadata_key, 16);
            let chunk = unsafe { ChunkView::from_raw_parts(metadata.as_mut_ptr(), 16).unwrap() };
            assert_eq!(chunk.state().size, 0);

            session.apply(SessionCommand::SetActive(false)).unwrap();
            session
                .apply(SessionCommand::SetNodeCommand(NodeCommandState::Pause))
                .unwrap();
            session
                .apply(SessionCommand::SetNodeCommand(NodeCommandState::Suspend))
                .unwrap();
            session.apply(SessionCommand::Disconnect).unwrap();
            assert_eq!(
                session.apply(SessionCommand::Disconnect).unwrap(),
                ApplyOutcome::AlreadyDisconnected
            );
        }
    }

    #[test]
    fn finish_clock_panic_aborts_output_publishes_failure_and_deactivates() {
        let resolver = FakeResolver::new();
        let own_key = initialize_activation(&resolver, 1, ActivationStatus::Inactive, 0);
        let metadata_key = resolver.add(2, 16);
        resolver.add(3, 64);
        let io_key = resolver.add(4, 8);
        {
            let mut io = resolver.bytes(io_key, 8);
            let mut guard = io.borrow();
            let bytes = unsafe { guard.bytes_mut() };
            bytes[0..4].copy_from_slice(&(BufferStatus::NeedData as i32).to_ne_bytes());
        }
        let (descriptor, trigger, _, _, _) = transport(MemoryId(1));
        let mut session = ClientNodeSession::new(resolver.clone());
        session
            .apply(SessionCommand::ReplaceTransport(descriptor))
            .unwrap();
        let generation = session.transport_generation().unwrap();
        let (buffers, io) = output_descriptors();
        session.apply(SessionCommand::UseBuffers(buffers)).unwrap();
        session.apply(SessionCommand::SetPortIo(io)).unwrap();
        session
            .apply(SessionCommand::SetFormat(
                NegotiatedAudioFormat::pcm_s16le(48_000, 2).unwrap(),
            ))
            .unwrap();
        session.apply(SessionCommand::SetActive(true)).unwrap();
        session
            .apply(SessionCommand::SetNodeCommand(NodeCommandState::Start))
            .unwrap();
        {
            let mut own = resolver.bytes(own_key, ActivationView::required_size());
            let mut guard = own.borrow();
            let bytes = unsafe { guard.bytes_mut() };
            bytes[0..4].copy_from_slice(&(ActivationStatus::Triggered as u32).to_ne_bytes());
        }
        trigger.signal(1).unwrap();

        let error = session
            .on_wake_with_finish(
                generation,
                100,
                || panic!("finish clock panic"),
                &mut PatternProcess { calls: 0 },
            )
            .unwrap_err();
        assert!(matches!(error, SessionError::FinishClockPanicked));
        assert_eq!(session.state(), SessionState::Failed);

        let mut own = resolver.bytes(own_key, ActivationView::required_size());
        let activation =
            unsafe { ActivationView::from_raw_parts(own.as_mut_ptr(), own.len()).unwrap() };
        assert_eq!(activation.status().unwrap(), ActivationStatus::Inactive);
        assert_eq!(activation.process_result(), -libc::EIO);
        assert_eq!(unsafe { activation.awake_time() }, 100);
        assert_eq!(unsafe { activation.finish_time() }, 100);

        let mut io = resolver.bytes(io_key, 8);
        let io = unsafe {
            BuffersIoView::from_raw_parts(PortIoType::Buffers, io.as_mut_ptr(), 8).unwrap()
        };
        assert_eq!(io.state().status, BufferStatus::NeedData as i32);
        let mut metadata = resolver.bytes(metadata_key, 16);
        let chunk = unsafe { ChunkView::from_raw_parts(metadata.as_mut_ptr(), 16).unwrap() };
        assert_eq!(chunk.state().size, 0);
    }

    #[test]
    fn peer_propagation_attempts_every_node_in_node_id_order_after_failure() {
        let resolver = FakeResolver::new();
        let failed = initialize_activation(&resolver, 30, ActivationStatus::NotTriggered, 0);
        let waiting = initialize_activation(&resolver, 31, ActivationStatus::NotTriggered, 2);
        let triggered = initialize_activation(&resolver, 32, ActivationStatus::NotTriggered, 1);
        let mut peers = PeerSet::default();
        let mut observers = Vec::new();
        for (generation, node, memory) in [(3, 3, 32), (2, 2, 31), (1, 1, 30)] {
            let (signal_fd, observer) = event_pair();
            observers.push((node, observer));
            peers.insert(
                PeerActivation::bind(
                    generation,
                    PeerActivationDescriptor {
                        node: NodeId(node),
                        signal_fd,
                        activation: RegionRef {
                            memory: MemoryId(memory),
                            offset: 0,
                            len: ActivationView::required_size(),
                        },
                    },
                    &resolver,
                )
                .unwrap(),
            );
        }
        assert!(matches!(
            peers.trigger_all(77),
            Err(SessionError::PeerTrigger {
                peer: NodeId(1),
                ..
            })
        ));

        let pending = |key| {
            let mut mapping = resolver.bytes(key, ActivationView::required_size());
            let activation = unsafe {
                ActivationView::from_raw_parts(mapping.as_mut_ptr(), mapping.len()).unwrap()
            };
            (activation.pending(), activation.status().unwrap())
        };
        assert_eq!(pending(failed), (0, ActivationStatus::NotTriggered));
        assert_eq!(pending(waiting), (1, ActivationStatus::NotTriggered));
        assert_eq!(pending(triggered), (0, ActivationStatus::Triggered));
        for (node, observer) in observers {
            let result = observer.drain();
            if node == 3 {
                assert_eq!(result.unwrap(), 1);
            } else {
                assert_eq!(result.unwrap_err().kind(), std::io::ErrorKind::WouldBlock);
            }
        }
    }

    #[test]
    fn output_replacement_rolls_back_rejection_and_quiesces_accepted_unresolved_state() {
        let resolver = FakeResolver::new();
        resolver.add(2, 16);
        resolver.add(3, 64);
        resolver.add(4, 8);
        let mut session = ClientNodeSession::new(resolver);
        let format = NegotiatedAudioFormat::pcm_s16le(48_000, 2).unwrap();
        let (buffers, io) = output_descriptors();
        session.apply(SessionCommand::SetFormat(format)).unwrap();
        session
            .apply(SessionCommand::UseBuffers(buffers.clone()))
            .unwrap();
        session.apply(SessionCommand::SetPortIo(io)).unwrap();
        let old_generation = session.output.as_ref().unwrap().id();

        let mut invalid = buffers.clone();
        invalid.buffers[0].metadata.len = 8;
        assert!(session.apply(SessionCommand::UseBuffers(invalid)).is_err());
        assert_eq!(session.output.as_ref().unwrap().id(), old_generation);
        assert_eq!(session.buffer_descriptor.as_ref(), Some(&buffers));
        assert_eq!(session.io_descriptor, Some(io));

        let mut unresolved = buffers;
        unresolved.buffers[0].media_memory = MemoryId(99);
        assert_eq!(
            session
                .apply(SessionCommand::UseBuffers(unresolved.clone()))
                .unwrap(),
            ApplyOutcome::PendingMemory
        );
        assert!(session.output.is_none());
        assert!(session.io_descriptor.is_none());
        assert_eq!(session.buffer_descriptor.as_ref(), Some(&unresolved));
        assert_eq!(session.state(), SessionState::Configuring);
    }

    #[test]
    fn unresolved_permutation_converges_and_exact_revocation_survives_id_reuse() {
        let resolver = FakeResolver::new();
        let (transport_descriptor, old_trigger, _, _, _) = transport(MemoryId(1));
        let (peer_fd, _) = event_pair();
        let (buffers, io) = output_descriptors();
        let mut session = ClientNodeSession::new(resolver.clone());

        assert_eq!(
            session
                .apply(SessionCommand::ReplaceTransport(transport_descriptor))
                .unwrap(),
            ApplyOutcome::PendingMemory
        );
        session.apply(SessionCommand::UseBuffers(buffers)).unwrap();
        session.apply(SessionCommand::SetPortIo(io)).unwrap();
        assert_eq!(
            session
                .apply(SessionCommand::SetPeerActivation(
                    PeerActivationDescriptor {
                        node: NodeId(8),
                        signal_fd: peer_fd,
                        activation: RegionRef {
                            memory: MemoryId(5),
                            offset: 0,
                            len: ActivationView::required_size(),
                        },
                    }
                ))
                .unwrap(),
            ApplyOutcome::PendingMemory
        );
        assert_eq!(
            session
                .apply(SessionCommand::SetFormat(
                    NegotiatedAudioFormat::pcm_s16le(48_000, 2).unwrap(),
                ))
                .unwrap(),
            ApplyOutcome::PendingMemory
        );
        session.apply(SessionCommand::SetActive(true)).unwrap();
        assert!(session
            .apply(SessionCommand::SetNodeCommand(NodeCommandState::Start))
            .is_err());

        initialize_activation(&resolver, 1, ActivationStatus::Inactive, 0);
        assert_eq!(
            session
                .apply(SessionCommand::MemoryAvailable(MemoryId(1)))
                .unwrap(),
            ApplyOutcome::PendingMemory
        );
        resolver.add(2, 16);
        resolver.add(4, 8);
        assert_eq!(
            session
                .apply(SessionCommand::MemoryAvailable(MemoryId(4)))
                .unwrap(),
            ApplyOutcome::PendingMemory
        );
        let old_media = resolver.add(3, 64);
        initialize_activation(&resolver, 5, ActivationStatus::NotTriggered, 1);
        session
            .apply(SessionCommand::MemoryAvailable(MemoryId(5)))
            .unwrap();
        assert_eq!(session.state(), SessionState::Running);

        resolver.remove(MemoryId(3));
        let replacement_media = resolver.add(3, 64);
        assert_ne!(old_media, replacement_media);
        session
            .apply(SessionCommand::RemoveMemory(old_media))
            .unwrap();
        assert!(session.output.is_none());
        session
            .apply(SessionCommand::MemoryAvailable(MemoryId(3)))
            .unwrap();
        let rebound = session.output.as_ref().unwrap().id();
        session
            .apply(SessionCommand::RemoveMemory(old_media))
            .unwrap();
        assert_eq!(session.output.as_ref().unwrap().id(), rebound);

        let old_generation = session.transport_generation().unwrap();
        initialize_activation(&resolver, 6, ActivationStatus::Inactive, 0);
        let (replacement, _, _, _, _) = transport(MemoryId(6));
        session
            .apply(SessionCommand::ReplaceTransport(replacement))
            .unwrap();
        let registration = session.runtime_registration().unwrap().unwrap();
        assert_ne!(registration.generation, old_generation);
        old_trigger.signal(1).unwrap();
        assert_eq!(
            session
                .on_wake(old_generation, 9, &mut PatternProcess { calls: 0 })
                .unwrap(),
            WakeOutcome::Stale {
                expected: registration.generation,
                received: old_generation,
            }
        );

        session
            .apply(SessionCommand::SetNodeCommand(NodeCommandState::Pause))
            .unwrap();
        session
            .apply(SessionCommand::SetNodeCommand(NodeCommandState::Suspend))
            .unwrap();
        assert_eq!(session.state(), SessionState::Configuring);
    }
}
