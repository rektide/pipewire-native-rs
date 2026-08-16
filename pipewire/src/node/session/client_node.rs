// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: Copyright (c) 2026 Asymptotic Inc.

//! One-owner bridge from a typed ClientNode proxy into its process session.

use std::{io, sync::Arc};

use parking_lot::Mutex;
use pipewire_native_node::{
    runtime::{
        self, CommandSendError, RuntimeDiagnostics, SessionCommandSender, TokioSessionHandle,
    },
    session::{
        output::OutputProcess,
        owner::{ClientNodeSession, SessionCommand},
    },
};

use crate::{
    proxy::{client_node::ClientNode, HasProxy, ProxyEvents},
    HookId,
};

use super::memory::{MemoryPoolEvent, MemoryPoolHandle};

/// Semantic owner associating one typed proxy, connection memory pool, callback, and runtime task.
///
/// Canonical proxy events are converted and transferred through a bounded queue. The callback only
/// receives the node crate's lifetime-scoped `OutputCycle`; it never sees wire DTOs, memory IDs,
/// mappings, activation records, eventfds, or chunks.
pub struct ClientNodeSessionBridge {
    proxy: ClientNode,
    memory: MemoryPoolHandle,
    proxy_listener: Option<HookId>,
    runtime: Option<TokioSessionHandle>,
    failure: Arc<Mutex<Option<String>>>,
}

impl std::fmt::Debug for ClientNodeSessionBridge {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ClientNodeSessionBridge")
            .field("proxy_id", &self.proxy.proxy().id())
            .field("diagnostics", &self.diagnostics())
            .field("terminal_error", &self.terminal_error())
            .finish_non_exhaustive()
    }
}

impl ClientNodeSessionBridge {
    /// Spawns the process owner on the current Tokio runtime and installs the sole event owners.
    pub fn spawn_tokio(
        proxy: ClientNode,
        memory: MemoryPoolHandle,
        process: Box<dyn OutputProcess>,
        command_capacity: usize,
    ) -> io::Result<Self> {
        let session = ClientNodeSession::new(memory.clone());
        let runtime = runtime::spawn_tokio(session, process, command_capacity)?;
        let sender = runtime.command_sender();
        let failure = Arc::new(Mutex::new(None));

        let event_sender = sender.clone();
        let event_failure = Arc::clone(&failure);
        proxy.set_event_handler(Some(Box::new(move |event| {
            let command = match SessionCommand::from_wire(event) {
                Ok(command) => command,
                Err(error) => {
                    fail(&event_failure, &event_sender, error.to_string());
                    return;
                }
            };
            if let Err(error) = event_sender.try_send(command) {
                record_send_failure(&event_failure, error);
            }
        })));

        let memory_sender = sender.clone();
        let memory_failure = Arc::clone(&failure);
        memory.set_event_handler(Some(Box::new(move |event| {
            let command = match event {
                MemoryPoolEvent::Available(key) => SessionCommand::MemoryAvailable(key.id),
                MemoryPoolEvent::Removed(key) => SessionCommand::RemoveMemory(key),
            };
            if let Err(error) = memory_sender.try_send(command) {
                record_send_failure(&memory_failure, error);
            }
        })));

        let removed_sender = sender;
        let removed_failure = Arc::clone(&failure);
        let proxy_listener = proxy.proxy().add_listener(ProxyEvents {
            removed: Some(Box::new(move || {
                if let Err(error) = removed_sender.try_send(SessionCommand::Disconnect) {
                    record_send_failure(&removed_failure, error);
                }
            })),
            ..Default::default()
        });

        Ok(Self {
            proxy,
            memory,
            proxy_listener: Some(proxy_listener),
            runtime: Some(runtime),
            failure,
        })
    }

    /// Returns the typed ClientNode proxy for semantic advertisement methods.
    pub fn proxy(&self) -> &ClientNode {
        &self.proxy
    }

    /// Sends graph-active intent and applies it to the process owner after successful delivery.
    pub fn set_active(&self, active: bool) -> io::Result<()> {
        self.proxy.set_active(active)?;
        self.runtime
            .as_ref()
            .expect("runtime exists until bridge shutdown")
            .command_sender()
            .try_send(SessionCommand::SetActive(active))
            .map_err(command_error)
    }

    /// Returns current waiting-adapter diagnostics.
    pub fn diagnostics(&self) -> RuntimeDiagnostics {
        self.runtime
            .as_ref()
            .map(TokioSessionHandle::diagnostics)
            .unwrap_or_default()
    }

    /// Returns the first terminal bridge error, if event conversion or bounded transfer failed.
    pub fn terminal_error(&self) -> Option<String> {
        self.failure.lock().clone()
    }

    /// Clears protocol callbacks, requests terminal teardown, and joins the Tokio task.
    pub async fn shutdown(mut self) -> io::Result<()> {
        self.detach_callbacks();
        self.runtime
            .take()
            .expect("runtime exists until bridge shutdown")
            .shutdown()
            .await
    }

    fn detach_callbacks(&mut self) {
        self.proxy.set_event_handler(None);
        self.memory.set_event_handler(None);
        if let Some(listener) = self.proxy_listener.take() {
            self.proxy.proxy().remove_listener(listener);
        }
    }
}

impl Drop for ClientNodeSessionBridge {
    fn drop(&mut self) {
        self.detach_callbacks();
        // TokioSessionHandle::drop requests cancellation and drops all session-owned FDs.
        self.runtime.take();
    }
}

fn fail(failure: &Mutex<Option<String>>, sender: &SessionCommandSender, message: String) {
    let mut failure = failure.lock();
    if failure.is_none() {
        *failure = Some(message);
    }
    drop(failure);
    sender.terminate();
}

fn record_send_failure(failure: &Mutex<Option<String>>, error: CommandSendError) {
    let mut failure = failure.lock();
    if failure.is_none() {
        *failure = Some(format!("ClientNode command queue {error}"));
    }
}

fn command_error(error: CommandSendError) -> io::Error {
    io::Error::new(io::ErrorKind::BrokenPipe, error)
}

#[cfg(test)]
mod tests {
    use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};

    use pipewire_native_node::session::{
        cycle::{CommittedOutput, OutputCycle},
        output::{OutputProcess, ProcessError},
    };
    use pipewire_native_protocol::wire::client_node::{Command, Event, RegionRef, Transport};

    use super::*;
    use crate::{
        context::Context, main_loop::MainLoop, properties::Properties,
        proxy::client_node::ClientNode,
    };

    struct UncalledProcess;

    impl OutputProcess for UncalledProcess {
        fn process(&mut self, _cycle: OutputCycle<'_>) -> Result<CommittedOutput, ProcessError> {
            panic!("overflowed bridge must not invoke its process callback")
        }
    }

    fn eventfd() -> OwnedFd {
        let raw = unsafe { libc::eventfd(0, libc::EFD_CLOEXEC | libc::EFD_NONBLOCK) };
        assert!(raw >= 0);
        unsafe { OwnedFd::from_raw_fd(raw) }
    }

    fn fdinfo(fd: RawFd) -> Option<String> {
        std::fs::read_to_string(format!("/proc/self/fdinfo/{fd}")).ok()
    }

    #[tokio::test(flavor = "current_thread")]
    async fn canonical_event_queue_overflow_is_terminal_and_closes_fds() {
        crate::init();
        let main_loop = MainLoop::new(&Properties::new()).unwrap();
        let context = Context::new(&main_loop, Properties::new()).unwrap();
        let core = crate::core::Core::new_disconnected_for_test(&context);
        let memory = MemoryPoolHandle::install(
            &core,
            pipewire_native_node::shm::ShrinkPolicy::RequireSealed,
        );
        let proxy = ClientNode::new(&core);
        let bridge = ClientNodeSessionBridge::spawn_tokio(
            proxy.clone(),
            memory,
            Box::new(UncalledProcess),
            1,
        )
        .unwrap();

        // The current-thread runtime cannot poll the spawned owner until this test yields.
        proxy.dispatch(Event::Command(Command::Pause));
        let trigger = eventfd();
        let completion = eventfd();
        let trigger_raw = trigger.as_raw_fd();
        let completion_raw = completion.as_raw_fd();
        let trigger_identity = fdinfo(trigger_raw).unwrap();
        let completion_identity = fdinfo(completion_raw).unwrap();
        proxy.dispatch(Event::Transport(Transport {
            trigger_fd: trigger,
            completion_fd: completion,
            activation: RegionRef {
                memory_id: 1,
                offset: 0,
                size: 8,
            },
        }));

        assert_eq!(
            bridge.terminal_error().as_deref(),
            Some("ClientNode command queue Full")
        );
        assert_ne!(
            fdinfo(trigger_raw).as_deref(),
            Some(trigger_identity.as_str())
        );
        assert_ne!(
            fdinfo(completion_raw).as_deref(),
            Some(completion_identity.as_str())
        );
        bridge.shutdown().await.unwrap();
    }
}
