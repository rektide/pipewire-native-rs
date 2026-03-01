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
};

use bon::Builder;
use tracing::{debug, info, trace, warn};

use crate::{
    protocol::{
        self, core_event, decode_inbound_message, encode_core_add_mem_payload,
        encode_core_done_payload, encode_core_error_payload, encode_core_info_payload,
        encode_registry_global_payload, encode_registry_global_remove_payload, NativeHeader,
        NativePacket,
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
}

impl ScriptedServer {
    /// Runs the scripted server until all scenario steps complete.
    pub fn run(self) -> io::Result<RunReport> {
        let config = self.config;
        let scenario = self.scenario;

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
        let (mut client, addr) = listener.accept()?;

        debug!(
            trace_name = config.trace_name(),
            peer = ?addr,
            "accepted first client connection"
        );

        let mut state = ExecutionState {
            accepted_clients: 1,
            ..Default::default()
        };

        if config.single_client_enabled() {
            listener.set_nonblocking(true)?;
        }

        for (step_index, step) in scenario.steps.iter().enumerate() {
            if config.single_client_enabled() {
                reject_pending_clients(&listener, &mut state)?;
            }

            let packet = protocol::read_packet(&mut client)?;
            let inbound = decode_inbound_message(
                packet.header.object_id,
                packet.header.opcode,
                packet.payload.as_slice(),
            )?;

            trace!(
                trace_name = config.trace_name(),
                step_index,
                object_id = packet.header.object_id,
                opcode = packet.header.opcode,
                inbound = ?inbound,
                "received inbound message"
            );

            if !step
                .expect
                .matches(packet.header.object_id, packet.header.opcode, &inbound)
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "scenario step {} expectation {:?} did not match inbound object:{} opcode:{} message:{:?}",
                        step_index,
                        step.expect,
                        packet.header.object_id,
                        packet.header.opcode,
                        inbound,
                    ),
                ));
            }

            update_state_from_inbound(&mut state, &inbound);

            for action in &step.actions {
                let keep_running = apply_action(&mut client, &mut state, action)?;
                if !keep_running {
                    return Ok(to_run_report(state));
                }
            }

            state.completed_steps += 1;
        }

        Ok(to_run_report(state))
    }
}

fn apply_action(
    client: &mut UnixStream,
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
            send_event(client, protocol::CORE_ID, core_event::INFO, payload)?;
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
            send_event(client, protocol::CORE_ID, core_event::DONE, payload)?;
            Ok(true)
        }
        Action::SendCoreDone { id, seq } => {
            let payload = encode_core_done_payload(*id, *seq)?;
            send_event(client, protocol::CORE_ID, core_event::DONE, payload)?;
            Ok(true)
        }
        Action::SendCoreError(err) => {
            let payload = encode_core_error_payload(err.id, err.seq, err.res, &err.message)?;
            send_event(client, protocol::CORE_ID, core_event::ERROR, payload)?;
            Ok(true)
        }
        Action::SendCoreAddMem(mem) => {
            let payload = encode_core_add_mem_payload(mem.id, mem.memory_type, mem.flags)?;
            let fd = create_memfd_for_add_mem(mem.id, mem.size)?;
            send_event_with_fds(
                client,
                protocol::CORE_ID,
                core_event::ADD_MEM,
                payload,
                &[fd.as_fd()],
            )?;
            state.exported_mem_ids.push(mem.id);
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
    client: &mut UnixStream,
    object_id: u32,
    opcode: u8,
    payload: Vec<u8>,
) -> io::Result<()> {
    send_event_with_fds(client, object_id, opcode, payload, &[])
}

fn send_event_with_fds(
    client: &mut UnixStream,
    object_id: u32,
    opcode: u8,
    payload: Vec<u8>,
    fds: &[std::os::fd::BorrowedFd<'_>],
) -> io::Result<()> {
    let packet = NativePacket {
        header: NativeHeader {
            object_id,
            opcode,
            payload_size: payload.len() as u32,
            seq: 0,
            n_fds: fds.len() as u32,
        },
        payload,
    };

    protocol::write_packet_with_fds(client, &packet, fds)
}

fn update_state_from_inbound(state: &mut ExecutionState, inbound: &protocol::InboundMessage) {
    match inbound {
        protocol::InboundMessage::CoreSync { id, seq } => {
            state.last_sync = Some(SyncState { id: *id, seq: *seq });
        }
        protocol::InboundMessage::CoreGetRegistry { new_id, .. } => {
            state.last_registry_proxy_id = Some(*new_id);
        }
        _ => {}
    }
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

struct SocketPathGuard {
    socket_path: PathBuf,
}

impl Drop for SocketPathGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.socket_path);
    }
}
