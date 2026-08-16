// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: Copyright (c) 2026 Asymptotic Inc.

use std::{
    ffi::CString,
    io,
    os::{
        fd::{AsFd, AsRawFd, FromRawFd, OwnedFd},
        unix::net::{UnixListener, UnixStream},
    },
    path::PathBuf,
    time::{Duration, Instant},
};

use bon::Builder;
use pipewire_native_protocol::native::frame::{
    FlushOutcome, FrameError, FrameLimits, FrameReceiver, FrameSender, OutboundFrame,
    ReceiveOutcome,
};
use tracing::{debug, info, trace, warn};

use crate::{
    protocol::{
        self, core_event, encode_core_add_mem_payload, encode_core_done_payload,
        encode_core_error_payload, encode_core_info_payload, encode_core_remove_mem_payload,
        encode_registry_global_payload, encode_registry_global_remove_payload,
    },
    script::{Action, Scenario},
    state::{ExecutionState, SyncState},
};

/// Runtime configuration for the scripted server.
#[derive(Debug, Clone, Builder)]
pub struct ServerConfig {
    /// Unix socket path to bind.
    pub socket_path: PathBuf,
    /// Optional override for single-client mode.
    pub single_client: Option<bool>,
    /// Optional label used for tracing.
    pub trace_name: Option<String>,
    /// Maximum duration for accepting and completing one scripted run.
    pub deadline: Option<Duration>,
}

impl ServerConfig {
    fn single_client_enabled(&self) -> bool {
        self.single_client.unwrap_or(true)
    }

    fn trace_name(&self) -> &str {
        self.trace_name
            .as_deref()
            .unwrap_or("pipewire-native-scripted-server")
    }

    fn run_timeout(&self) -> Duration {
        self.deadline.unwrap_or(Duration::from_secs(5))
    }
}

/// Scripted server entry point.
#[derive(Debug, Clone, Builder)]
pub struct ScriptedServer {
    /// Runtime configuration.
    pub config: ServerConfig,
    /// Deterministic scripted scenario.
    pub scenario: Scenario,
}

/// Summary of one server run.
#[derive(Debug, Clone)]
pub struct RunReport {
    /// Number of completed scripted steps.
    pub completed_steps: usize,
    /// Number of accepted client connections.
    pub accepted_clients: usize,
    /// Number of rejected extra client connections.
    pub rejected_clients: usize,
    /// Last sync observed from inbound messages.
    pub last_sync: Option<SyncState>,
    /// Last registry proxy id requested by client.
    pub last_registry_proxy_id: Option<u32>,
    /// Memory ids exported through scripted `Core::AddMem` actions.
    pub exported_mem_ids: Vec<u32>,
    /// Number of live routed client objects at scenario completion.
    pub live_object_routes: usize,
}

impl ScriptedServer {
    /// Runs the scripted server until all scenario steps complete.
    pub fn run(self) -> io::Result<RunReport> {
        let config = self.config;
        let scenario = self.scenario;
        let deadline = Instant::now() + config.run_timeout();

        if let Some(parent) = config.socket_path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        if config.socket_path.exists() {
            std::fs::remove_file(&config.socket_path)?;
        }

        let _socket_guard = SocketPathGuard {
            socket_path: config.socket_path.clone(),
        };

        info!(
            trace_name = config.trace_name(),
            socket = %config.socket_path.display(),
            "starting scripted server"
        );

        let listener = UnixListener::bind(&config.socket_path)?;
        listener.set_nonblocking(true)?;
        let limits = FrameLimits::default();
        let mut receiver = FrameReceiver::new(limits);
        let mut sender = FrameSender::new(limits);
        let mut last_frame_route = None;
        wait_until_readable(&listener, deadline).map_err(|err| {
            runtime_wait_error(
                &config,
                &scenario,
                None,
                0,
                "accept",
                TransportDiagnostics::new(&receiver, &sender, last_frame_route),
                err,
            )
        })?;
        let (client, addr) = listener.accept().map_err(|err| {
            runtime_wait_error(
                &config,
                &scenario,
                None,
                0,
                "accept",
                TransportDiagnostics::new(&receiver, &sender, last_frame_route),
                err,
            )
        })?;

        debug!(
            trace_name = config.trace_name(),
            peer = ?addr,
            "accepted first client connection"
        );

        let mut state = ExecutionState {
            accepted_clients: 1,
            ..Default::default()
        };
        for (step_index, step) in scenario.steps.iter().enumerate() {
            if config.single_client_enabled() {
                reject_pending_clients(&listener, &mut state)?;
            }

            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(runtime_wait_error(
                    &config,
                    &scenario,
                    Some(step_index),
                    state.completed_steps,
                    "read",
                    TransportDiagnostics::new(&receiver, &sender, last_frame_route),
                    io::Error::new(io::ErrorKind::TimedOut, "script deadline elapsed"),
                ));
            }
            let frame = loop {
                match receiver.receive(client.as_fd()).map_err(frame_error)? {
                    ReceiveOutcome::Frame(frame) => break frame,
                    ReceiveOutcome::WouldBlock => {
                        wait_until(client.as_fd(), libc::POLLIN, deadline).map_err(|err| {
                            runtime_wait_error(
                                &config,
                                &scenario,
                                Some(step_index),
                                state.completed_steps,
                                "read",
                                TransportDiagnostics::new(&receiver, &sender, last_frame_route),
                                err,
                            )
                        })?;
                    }
                    ReceiveOutcome::Closed => {
                        return Err(io::Error::new(
                            io::ErrorKind::UnexpectedEof,
                            "client closed while waiting for scripted inbound frame",
                        ));
                    }
                }
            };
            let header = frame.header();
            last_frame_route = Some((header.object_id, header.opcode, header.seq));
            if !frame.fds().is_empty() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "scenario step {step_index} expected no inbound fds but frame object:{} opcode:{} carried {}",
                        header.object_id,
                        header.opcode,
                        frame.fds().len()
                    ),
                ));
            }
            let inbound = protocol::decode_inbound_message_with_routes(
                header.object_id,
                header.opcode,
                frame.payload(),
                &state.object_routes,
            )?;

            trace!(
                trace_name = config.trace_name(),
                step_index,
                object_id = header.object_id,
                opcode = header.opcode,
                inbound = ?inbound,
                "received inbound message"
            );

            if !step
                .expect
                .matches(header.object_id, header.opcode, &inbound)
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "scenario step {} expectation {:?} did not match inbound object:{} opcode:{} message:{:?}",
                        step_index,
                        step.expect,
                        header.object_id,
                        header.opcode,
                        inbound,
                    ),
                ));
            }

            update_state_from_inbound(&mut state, &inbound)?;
            if config.single_client_enabled() {
                reject_pending_clients(&listener, &mut state)?;
            }

            for action in &step.actions {
                let keep_running = apply_action(&client, &mut sender, deadline, &mut state, action)
                    .map_err(|err| {
                        if matches!(
                            err.kind(),
                            io::ErrorKind::TimedOut | io::ErrorKind::WouldBlock
                        ) {
                            runtime_wait_error(
                                &config,
                                &scenario,
                                Some(step_index),
                                state.completed_steps,
                                "write",
                                TransportDiagnostics::new(&receiver, &sender, last_frame_route),
                                err,
                            )
                        } else {
                            err
                        }
                    })?;
                if !keep_running {
                    state.completed_steps += 1;
                    return Ok(to_run_report(state));
                }
            }

            state.completed_steps += 1;
        }

        Ok(to_run_report(state))
    }
}

fn wait_until_readable(listener: &UnixListener, deadline: Instant) -> io::Result<()> {
    wait_until(listener.as_fd(), libc::POLLIN, deadline)
}

fn wait_until(
    fd: std::os::fd::BorrowedFd<'_>,
    events: libc::c_short,
    deadline: Instant,
) -> io::Result<()> {
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "script deadline elapsed",
            ));
        }

        let timeout_ms = remaining.as_millis().min(i32::MAX as u128) as i32;
        let mut poll_fd = libc::pollfd {
            fd: fd.as_raw_fd(),
            events,
            revents: 0,
        };
        let result = unsafe { libc::poll(&mut poll_fd, 1, timeout_ms.max(1)) };
        if result > 0 {
            return Ok(());
        }
        if result == 0 {
            continue;
        }

        let err = io::Error::last_os_error();
        if err.kind() != io::ErrorKind::Interrupted {
            return Err(err);
        }
    }
}

fn runtime_wait_error(
    config: &ServerConfig,
    scenario: &Scenario,
    step_index: Option<usize>,
    completed_steps: usize,
    phase: &str,
    transport: TransportDiagnostics<'_>,
    source: io::Error,
) -> io::Error {
    let scenario_name = scenario.name.as_deref().unwrap_or("unnamed");
    let step = step_index
        .and_then(|index| scenario.steps.get(index).map(|step| (index, step)))
        .map(|(index, step)| {
            format!(
                "{index} name={} expectation={:?}",
                step.name.as_deref().unwrap_or("unnamed"),
                step.expect
            )
        })
        .unwrap_or_else(|| "not-started".to_string());

    io::Error::new(
        io::ErrorKind::TimedOut,
        format!(
            "scripted server timed out: trace={} scenario={} phase={} step={} completed_steps={} buffered_frame_state=shared-receiver descriptor_count=shared-receiver buffered_bytes={} pending_fds={} queued_bytes={} queued_fds={} last_frame_route={:?}: {}",
            config.trace_name(),
            scenario_name,
            phase,
            step,
            completed_steps,
            transport.receiver.buffered_bytes(),
            transport.receiver.pending_fds(),
            transport.sender.queued_bytes(),
            transport.sender.queued_fds(),
            transport.last_frame_route,
            source
        ),
    )
}

struct TransportDiagnostics<'a> {
    receiver: &'a FrameReceiver,
    sender: &'a FrameSender,
    last_frame_route: Option<(u32, u8, u32)>,
}

impl<'a> TransportDiagnostics<'a> {
    fn new(
        receiver: &'a FrameReceiver,
        sender: &'a FrameSender,
        last_frame_route: Option<(u32, u8, u32)>,
    ) -> Self {
        Self {
            receiver,
            sender,
            last_frame_route,
        }
    }
}

fn apply_action(
    client: &UnixStream,
    sender: &mut FrameSender,
    deadline: Instant,
    state: &mut ExecutionState,
    action: &Action,
) -> io::Result<bool> {
    match action {
        Action::SendCoreInfo(info) => {
            let payload = encode_core_info_payload(
                info.cookie,
                &info.user_name,
                &info.host_name,
                &info.version,
                &info.name,
                info.props.as_slice(),
            )?;
            send_event(
                client,
                sender,
                deadline,
                protocol::CORE_ID,
                core_event::INFO,
                payload,
            )?;
            Ok(true)
        }
        Action::SendCoreDoneFromLastSync => {
            let Some(sync) = state.last_sync else {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "SendCoreDoneFromLastSync requires a previously observed Core::Sync",
                ));
            };

            let payload = encode_core_done_payload(sync.id, sync.seq)?;
            send_event(
                client,
                sender,
                deadline,
                protocol::CORE_ID,
                core_event::DONE,
                payload,
            )?;
            Ok(true)
        }
        Action::SendCoreDone { id, seq } => {
            let payload = encode_core_done_payload(*id, *seq)?;
            send_event(
                client,
                sender,
                deadline,
                protocol::CORE_ID,
                core_event::DONE,
                payload,
            )?;
            Ok(true)
        }
        Action::SendCoreError(err) => {
            let payload = encode_core_error_payload(err.id, err.seq, err.res, &err.message)?;
            send_event(
                client,
                sender,
                deadline,
                protocol::CORE_ID,
                core_event::ERROR,
                payload,
            )?;
            Ok(true)
        }
        Action::SendCoreAddMem(mem) => {
            let payload =
                encode_core_add_mem_payload(mem.id, mem.memory_type, mem.fd_index, mem.flags)?;
            let fd = create_memfd_for_add_mem(mem.id, mem.size)?;
            send_event_with_fds(
                client,
                sender,
                deadline,
                protocol::CORE_ID,
                core_event::ADD_MEM,
                payload,
                vec![fd],
            )?;
            state.exported_mem_ids.push(mem.id);
            Ok(true)
        }
        Action::SendCoreRemoveMem { id } => {
            let payload = encode_core_remove_mem_payload(*id)?;
            send_event(
                client,
                sender,
                deadline,
                protocol::CORE_ID,
                core_event::REMOVE_MEM,
                payload,
            )?;
            Ok(true)
        }
        Action::SendClientNodeCommand(command) => {
            let object_id = last_client_node_id(state).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "SendClientNodeCommand requires previous ClientNode CreateObject",
                )
            })?;
            let payload = protocol::client_node::encode_command(*command)?;
            send_event(
                client,
                sender,
                deadline,
                object_id,
                protocol::client_node::event::COMMAND,
                payload,
            )?;
            Ok(true)
        }
        Action::SendClientNodeTransport {
            trigger_index,
            completion_index,
            activation,
        } => {
            let object_id = require_client_node_id(state, "SendClientNodeTransport")?;
            let payload = protocol::client_node::encode_transport(
                *trigger_index,
                *completion_index,
                *activation,
            )?;
            send_event_with_fds(
                client,
                sender,
                deadline,
                object_id,
                protocol::client_node::event::TRANSPORT,
                payload,
                vec![create_eventfd()?, create_eventfd()?],
            )?;
            Ok(true)
        }
        Action::SendClientNodePortSetParam(value) => {
            send_client_node_event(
                client,
                sender,
                deadline,
                state,
                protocol::client_node::event::PORT_SET_PARAM,
                protocol::client_node::encode_port_set_param(value)?,
            )?;
            Ok(true)
        }
        Action::SendClientNodePortUseBuffers(value) => {
            send_client_node_event(
                client,
                sender,
                deadline,
                state,
                protocol::client_node::event::PORT_USE_BUFFERS,
                protocol::client_node::encode_port_use_buffers(value)?,
            )?;
            Ok(true)
        }
        Action::SendClientNodePortSetIo(value) => {
            send_client_node_event(
                client,
                sender,
                deadline,
                state,
                protocol::client_node::event::PORT_SET_IO,
                protocol::client_node::encode_port_set_io(*value)?,
            )?;
            Ok(true)
        }
        Action::SendClientNodeSetActivation {
            node_id,
            activation,
        } => {
            let object_id = require_client_node_id(state, "SendClientNodeSetActivation")?;
            let payload = protocol::client_node::encode_set_activation(
                *node_id,
                activation.map(|region| (0, region)),
            )?;
            let fds = if activation.is_some() {
                vec![create_eventfd()?]
            } else {
                vec![]
            };
            send_event_with_fds(
                client,
                sender,
                deadline,
                object_id,
                protocol::client_node::event::SET_ACTIVATION,
                payload,
                fds,
            )?;
            Ok(true)
        }
        Action::SendRegistryGlobalOnLastRegistry(global) => {
            let Some(registry_id) = state.last_registry_proxy_id else {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "SendRegistryGlobalOnLastRegistry requires previous Core::GetRegistry",
                ));
            };

            let payload = encode_registry_global_payload(
                global.id,
                global.permissions,
                &global.type_,
                global.version,
                global.props.as_slice(),
            )?;
            send_event(
                client,
                sender,
                deadline,
                registry_id,
                protocol::registry_event::GLOBAL,
                payload,
            )?;
            Ok(true)
        }
        Action::SendRegistryGlobalRemoveOnLastRegistry { id } => {
            let Some(registry_id) = state.last_registry_proxy_id else {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "SendRegistryGlobalRemoveOnLastRegistry requires previous Core::GetRegistry",
                ));
            };

            let payload = encode_registry_global_remove_payload(*id)?;
            send_event(
                client,
                sender,
                deadline,
                registry_id,
                protocol::registry_event::GLOBAL_REMOVE,
                payload,
            )?;
            Ok(true)
        }
        Action::CloseConnection => Ok(false),
    }
}

fn send_event(
    client: &UnixStream,
    sender: &mut FrameSender,
    deadline: Instant,
    object_id: u32,
    opcode: u8,
    payload: Vec<u8>,
) -> io::Result<()> {
    send_event_with_fds(
        client,
        sender,
        deadline,
        object_id,
        opcode,
        payload,
        Vec::new(),
    )
}

fn send_client_node_event(
    client: &UnixStream,
    sender: &mut FrameSender,
    deadline: Instant,
    state: &ExecutionState,
    opcode: u8,
    payload: Vec<u8>,
) -> io::Result<()> {
    send_event(
        client,
        sender,
        deadline,
        require_client_node_id(state, "ClientNode event")?,
        opcode,
        payload,
    )
}

fn send_event_with_fds(
    client: &UnixStream,
    sender: &mut FrameSender,
    deadline: Instant,
    object_id: u32,
    opcode: u8,
    payload: Vec<u8>,
    fds: Vec<OwnedFd>,
) -> io::Result<()> {
    let frame = OutboundFrame::new(object_id, opcode, 0, payload, fds, FrameLimits::default())
        .map_err(frame_error)?;
    sender.enqueue(frame).map_err(frame_error)?;
    loop {
        match sender.flush(client.as_fd()).map_err(frame_error)? {
            FlushOutcome::Drained => return Ok(()),
            FlushOutcome::WouldBlock => wait_until(client.as_fd(), libc::POLLOUT, deadline)?,
        }
    }
}

fn frame_error(error: FrameError) -> io::Error {
    let kind = match &error {
        FrameError::Io(source) => source.kind(),
        FrameError::PayloadTooLarge { .. }
        | FrameError::TooManyFrameFds { .. }
        | FrameError::SendQueueFull { .. }
        | FrameError::SendFdQueueFull { .. } => io::ErrorKind::InvalidInput,
        FrameError::WriteZero => io::ErrorKind::WriteZero,
        _ => io::ErrorKind::InvalidData,
    };
    io::Error::new(kind, error)
}

fn update_state_from_inbound(
    state: &mut ExecutionState,
    inbound: &protocol::InboundMessage,
) -> io::Result<()> {
    match inbound {
        protocol::InboundMessage::CoreSync { id, seq } => {
            state.last_sync = Some(SyncState { id: *id, seq: *seq });
        }
        protocol::InboundMessage::CoreGetRegistry { new_id, .. } => {
            state.last_registry_proxy_id = Some(*new_id);
            insert_route(state, *new_id, "PipeWire:Interface:Registry", 3)?;
        }
        protocol::InboundMessage::CoreCreateObject {
            factory_name,
            type_,
            version,
            new_id,
        } => {
            let _ = factory_name;
            insert_route(state, *new_id, type_, *version)?;
        }
        protocol::InboundMessage::CoreDestroy { object_id } => {
            if state.object_routes.remove(object_id).is_none() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("Core::Destroy references unknown object id {object_id}"),
                ));
            }
        }
        _ => {}
    }
    Ok(())
}

fn insert_route(
    state: &mut ExecutionState,
    object_id: u32,
    interface: &str,
    version: u32,
) -> io::Result<()> {
    if state.object_routes.contains_key(&object_id) {
        return Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            format!("object route collision for id {object_id}"),
        ));
    }
    state.object_routes.insert(
        object_id,
        protocol::ObjectRoute {
            interface: interface.to_owned(),
            version,
        },
    );
    Ok(())
}

fn last_client_node_id(state: &ExecutionState) -> Option<u32> {
    state.object_routes.iter().rev().find_map(|(id, route)| {
        (route.interface == protocol::client_node::INTERFACE).then_some(*id)
    })
}

fn require_client_node_id(state: &ExecutionState, action: &str) -> io::Result<u32> {
    last_client_node_id(state).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("{action} requires previous live ClientNode CreateObject"),
        )
    })
}

fn reject_pending_clients(listener: &UnixListener, state: &mut ExecutionState) -> io::Result<()> {
    loop {
        match listener.accept() {
            Ok((stream, _)) => {
                drop(stream);
                state.rejected_clients += 1;
                warn!("rejected additional client connection in single-client mode");
            }
            Err(err) if err.kind() == io::ErrorKind::WouldBlock => return Ok(()),
            Err(err) => return Err(err),
        }
    }
}

fn to_run_report(state: ExecutionState) -> RunReport {
    RunReport {
        completed_steps: state.completed_steps,
        accepted_clients: state.accepted_clients,
        rejected_clients: state.rejected_clients,
        last_sync: state.last_sync,
        last_registry_proxy_id: state.last_registry_proxy_id,
        exported_mem_ids: state.exported_mem_ids,
        live_object_routes: state.object_routes.len(),
    }
}

fn create_memfd_for_add_mem(id: u32, size: usize) -> io::Result<OwnedFd> {
    if size == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "Core::AddMem size must be greater than zero",
        ));
    }

    let name = CString::new(format!("pipewire-native-server-add-mem-{id}"))
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "memfd name contained NUL"))?;

    let fd = unsafe { libc::memfd_create(name.as_ptr(), libc::MFD_CLOEXEC) };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }

    let fd = unsafe { OwnedFd::from_raw_fd(fd) };
    let res = unsafe { libc::ftruncate(fd.as_raw_fd(), size as libc::off_t) };
    if res < 0 {
        return Err(io::Error::last_os_error());
    }

    Ok(fd)
}

fn create_eventfd() -> io::Result<OwnedFd> {
    let fd = unsafe { libc::eventfd(0, libc::EFD_CLOEXEC | libc::EFD_NONBLOCK) };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(unsafe { OwnedFd::from_raw_fd(fd) })
}

struct SocketPathGuard {
    socket_path: PathBuf,
}

impl Drop for SocketPathGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.socket_path);
    }
}
