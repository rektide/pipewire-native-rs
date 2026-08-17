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

use super::memory::MemoryPoolSubscription;
use super::memory::{MemoryPoolEvent, MemoryPoolHandle};

/// Semantic owner associating one typed proxy, connection memory pool, callback, and runtime task.
///
/// Canonical proxy events are converted and transferred through a bounded queue. The callback only
/// receives the node crate's lifetime-scoped `OutputCycle`; it never sees wire DTOs, memory IDs,
/// mappings, activation records, eventfds, or chunks.
pub struct ClientNodeSessionBridge {
    proxy: ClientNode,
    memory_subscription: Option<MemoryPoolSubscription>,
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
    /// Spawns the process owner on the current Tokio runtime and installs private event owners.
    pub(crate) fn spawn_tokio(
        proxy: ClientNode,
        memory: MemoryPoolHandle,
        process: Box<dyn OutputProcess>,
        command_capacity: usize,
    ) -> io::Result<Self> {
        Self::spawn_tokio_with_clock(
            proxy,
            memory,
            process,
            command_capacity,
            pipewire_native_node::runtime::MonotonicClock,
        )
    }

    pub(crate) fn spawn_tokio_with_clock<C: pipewire_native_node::runtime::RuntimeClock>(
        proxy: ClientNode,
        memory: MemoryPoolHandle,
        process: Box<dyn OutputProcess>,
        command_capacity: usize,
        clock: C,
    ) -> io::Result<Self> {
        let session = ClientNodeSession::new(memory.clone());
        let runtime = runtime::spawn_tokio_with_clock(session, process, command_capacity, clock)?;
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
        let memory_subscription = memory.subscribe(Box::new(move |event| {
            let command = match event {
                MemoryPoolEvent::Available(lease) => SessionCommand::MemoryAvailable(lease),
                MemoryPoolEvent::Removed(key) => SessionCommand::RemoveMemory(key),
                MemoryPoolEvent::Disconnected => SessionCommand::MemoryDisconnected,
            };
            if let Err(error) = memory_sender.try_send(command) {
                record_send_failure(&memory_failure, error);
            }
        }));

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
            memory_subscription: Some(memory_subscription),
            proxy_listener: Some(proxy_listener),
            runtime: Some(runtime),
            failure,
        })
    }

    /// Experimentally advertises one node and one output port as one bridge operation.
    ///
    /// The raw protocol-shaped arguments are temporary until `OutputNodeSpec` is stabilized;
    /// callers cannot access the proxy or displace bridge callbacks.
    pub fn advertise_output(
        &self,
        node: pipewire_native_protocol::wire::client_node::Update,
        port: pipewire_native_protocol::wire::client_node::PortUpdate,
    ) -> io::Result<()> {
        self.proxy.update(node)?;
        self.proxy.port_update(port)
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

    /// Stops processing, destroys the Core object, and joins the runtime owner.
    pub async fn shutdown_and_destroy(mut self, core: &crate::core::Core) -> io::Result<()> {
        self.detach_callbacks();
        core.destroy(&self.proxy)?;
        self.runtime
            .take()
            .expect("runtime exists until bridge shutdown")
            .shutdown()
            .await
    }

    fn detach_callbacks(&mut self) {
        self.proxy.set_event_handler(None);
        self.memory_subscription.take();
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

    use pipewire_native_node::{
        session::{
            activation::ActivationView,
            cycle::{CommittedOutput, OutputCycle},
            output::{OutputProcess, ProcessError},
        },
        shm::create_memfd,
    };
    use pipewire_native_protocol::wire::client_node::{
        ActivationStatus, BufferDescriptor, Command, DataDescriptor, Direction, Event, PortSetIo,
        PortSetParam, PortUseBuffers, RegionRef, Transport,
    };
    use pipewire_native_spa::{buffer::data_type, pod::RawPodOwned};

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

    #[test]
    fn synchronous_constructors_return_error_without_tokio_runtime() {
        crate::init();
        let main_loop = MainLoop::new(&Properties::new()).unwrap();
        let context = Context::new(&main_loop, Properties::new()).unwrap();
        let core = crate::core::Core::new_disconnected_for_test(&context);
        let memory = MemoryPoolHandle::install(
            &core,
            pipewire_native_node::shm::ShrinkPolicy::RequireSealed,
        );
        let proxy = ClientNode::new(&core);
        let error = ClientNodeSessionBridge::spawn_tokio(
            proxy,
            memory.clone(),
            Box::new(UncalledProcess),
            1,
        )
        .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::NotConnected);

        let error = core
            .create_tokio_client_node_session(&Properties::new(), memory, Box::new(UncalledProcess))
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::NotConnected);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn proxy_removal_and_importer_disconnect_stop_every_bridge() {
        crate::init();
        let main_loop = MainLoop::new(&Properties::new()).unwrap();
        let context = Context::new(&main_loop, Properties::new()).unwrap();
        let core = crate::core::Core::new_disconnected_for_test(&context);
        let memory = MemoryPoolHandle::install(
            &core,
            pipewire_native_node::shm::ShrinkPolicy::RequireSealed,
        );

        let proxy = ClientNode::new(&core);
        let mut removed = ClientNodeSessionBridge::spawn_tokio(
            proxy.clone(),
            memory.clone(),
            Box::new(UncalledProcess),
            8,
        )
        .unwrap();
        pipewire_native_spa::emit_hook!(proxy.proxy().events(), removed);
        removed.runtime.take().unwrap().wait().await.unwrap();
        drop(removed);

        let first = ClientNodeSessionBridge::spawn_tokio(
            ClientNode::new(&core),
            memory.clone(),
            Box::new(UncalledProcess),
            8,
        )
        .unwrap();
        let mut second = ClientNodeSessionBridge::spawn_tokio(
            ClientNode::new(&core),
            memory,
            Box::new(UncalledProcess),
            8,
        )
        .unwrap();
        drop(first);
        core.set_memory_importer(None);
        second.runtime.take().unwrap().wait().await.unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn preimported_memory_is_replayed_before_configuration_and_converges() {
        crate::init();
        let main_loop = MainLoop::new(&Properties::new()).unwrap();
        let context = Context::new(&main_loop, Properties::new()).unwrap();
        let core = crate::core::Core::new_disconnected_for_test(&context);
        let memory =
            MemoryPoolHandle::install(&core, pipewire_native_node::shm::ShrinkPolicy::Allow);
        core.import_memory(
            50,
            data_type::MEM_FD,
            create_memfd("preimported-bridge", 8192).unwrap(),
            0,
        )
        .unwrap();
        let key = memory
            .resolve(pipewire_native_node::session::memory::MemoryId(50))
            .unwrap();
        {
            let mut mapping = memory
                .map(key, 0, ActivationView::required_size(), true)
                .unwrap();
            let mut bytes = mapping.borrow();
            let bytes = unsafe { bytes.bytes_mut() };
            bytes[0..4].copy_from_slice(&(ActivationStatus::Inactive as u32).to_ne_bytes());
            bytes[544..548].copy_from_slice(&1_u32.to_ne_bytes());
        }

        let proxy = ClientNode::new(&core);
        let bridge = ClientNodeSessionBridge::spawn_tokio(
            proxy.clone(),
            memory.clone(),
            Box::new(UncalledProcess),
            32,
        )
        .unwrap();
        let (trigger_fd, _) = std::io::pipe().unwrap();
        let (completion_fd, _) = std::io::pipe().unwrap();
        proxy.dispatch(Event::Transport(Transport {
            trigger_fd: trigger_fd.into(),
            completion_fd: completion_fd.into(),
            activation: RegionRef {
                memory_id: 50,
                offset: 0,
                size: ActivationView::required_size() as u32,
            },
        }));
        proxy.dispatch(Event::PortSetParam(PortSetParam {
            direction: Direction::Output,
            port_id: 0,
            param_id: 4,
            flags: 0,
            param: Some(RawPodOwned::wrap(format_fixture()).unwrap()),
        }));
        proxy.dispatch(Event::PortUseBuffers(PortUseBuffers {
            direction: Direction::Output,
            port_id: 0,
            mix_id: None,
            flags: 0,
            buffers: vec![BufferDescriptor {
                metadata: RegionRef {
                    memory_id: 50,
                    offset: 4096,
                    size: 16,
                },
                metas: vec![],
                datas: vec![DataDescriptor {
                    type_id: data_type::MEM_ID,
                    data_id: 50,
                    flags: 0,
                    map_offset: 4160,
                    max_size: 64,
                }],
            }],
        }));
        proxy.dispatch(Event::PortSetIo(PortSetIo::Set {
            direction: Direction::Output,
            port_id: 0,
            mix_id: None,
            io_id: 1,
            region: RegionRef {
                memory_id: 50,
                offset: 4224,
                size: 8,
            },
        }));
        bridge
            .runtime
            .as_ref()
            .unwrap()
            .command_sender()
            .try_send(SessionCommand::SetActive(true))
            .unwrap();
        proxy.dispatch(Event::Command(Command::Start));

        let mut converged = false;
        for _ in 0..10_000 {
            let mut mapping = memory
                .map(key, 0, ActivationView::required_size(), true)
                .unwrap();
            let view = unsafe {
                ActivationView::from_raw_parts(mapping.as_mut_ptr(), mapping.len()).unwrap()
            };
            if view.status().unwrap() == ActivationStatus::Finished {
                converged = true;
                break;
            }
            tokio::task::yield_now().await;
        }
        assert!(
            converged,
            "pre-imported memory did not reach the session owner"
        );
        assert_eq!(bridge.terminal_error(), None);
        bridge.shutdown().await.unwrap();
    }

    fn format_fixture() -> Vec<u8> {
        let line = include_str!("../../../../protocol/tests/fixtures/client-node-v6/upstream.hex")
            .lines()
            .find(|line| line.starts_with("format-s16le-48k-stereo "))
            .unwrap();
        line.split_once(' ')
            .unwrap()
            .1
            .as_bytes()
            .chunks_exact(2)
            .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
            .collect()
    }
}
