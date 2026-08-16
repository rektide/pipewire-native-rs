// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: Copyright (c) 2026 Asymptotic Inc.

use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
};

use crate::{
    core::Core,
    new_refcounted, object_invoke, protocol,
    proxy::{HasProxy, Proxy},
    refcounted, types,
};
use pipewire_native_protocol::wire::client_node::{self as wire, Method};

/// Sole owner callback for decoded ClientNode events and their transferred descriptors.
pub type ClientNodeEventHandler = Box<dyn FnMut(wire::Event) + Send>;
type SendMethod = Box<dyn FnMut(&ClientNode, Method) -> std::io::Result<()>>;

refcounted! {
    /// Typed proxy for a client-created ClientNode v6 object.
    pub struct ClientNode {
        proxy: Proxy,
        methods: Arc<Mutex<ClientNodeMethods>>,
        event_dispatch: Mutex<EventDispatch>,
    }
}

pub(crate) struct ClientNodeMethods {
    pub(crate) send: SendMethod,
}

#[derive(Default)]
struct EventDispatch {
    handler: Option<ClientNodeEventHandler>,
    queued: VecDeque<wire::Event>,
    dispatching: bool,
    generation: u64,
}

impl HasProxy for ClientNode {
    fn type_(&self) -> types::ObjectType {
        types::interface::CLIENT_NODE
    }

    fn version(&self) -> u32 {
        wire::INTERFACE_VERSION
    }

    fn proxy(&self) -> &Proxy {
        &self.inner.proxy
    }
}

impl ClientNode {
    pub(crate) fn new(core: &Core) -> Self {
        let this = Self {
            inner: new_refcounted(InnerClientNode {
                proxy: Proxy::new(core.next_proxy_id()),
                methods: Arc::new(Mutex::new(protocol::marshal::client_node::marshal(
                    core.connection(),
                ))),
                event_dispatch: Mutex::new(EventDispatch::default()),
            }),
        };
        core.add_proxy(&this);
        this
    }

    /// Send one canonical, bounded ClientNode method.
    pub fn send(&self, method: Method) -> std::io::Result<()> {
        object_invoke!(self, send, method)
    }

    /// Advertise node information and parameters.
    pub fn update(&self, update: wire::Update) -> std::io::Result<()> {
        self.send(Method::Update(update))
    }

    /// Advertise port information and parameters.
    pub fn port_update(&self, update: wire::PortUpdate) -> std::io::Result<()> {
        self.send(Method::PortUpdate(update))
    }

    /// Join or leave graph scheduling.
    pub fn set_active(&self, active: bool) -> std::io::Result<()> {
        self.send(Method::SetActive(wire::SetActive { active }))
    }

    /// Install the sole typed event owner. Replacing or clearing the handler drops
    /// any resources retained by the previous owner.
    pub fn set_event_handler(&self, handler: Option<ClientNodeEventHandler>) {
        set_event_handler(&self.inner.event_dispatch, handler);
    }

    pub(crate) fn methods(&self) -> Arc<Mutex<ClientNodeMethods>> {
        self.inner.methods.clone()
    }

    pub(crate) fn dispatch(&self, event: wire::Event) {
        dispatch_event(&self.inner.event_dispatch, event);
    }
}

fn set_event_handler(state: &Mutex<EventDispatch>, handler: Option<ClientNodeEventHandler>) {
    let displaced = {
        let mut state = state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.generation = state.generation.wrapping_add(1);
        std::mem::replace(&mut state.handler, handler)
    };
    drop(displaced);
}

fn dispatch_event(state: &Mutex<EventDispatch>, event: wire::Event) {
    let (handler, generation) = {
        let mut state = state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.dispatching {
            state.queued.push_back(event);
            return;
        }
        let Some(handler) = state.handler.take() else {
            return;
        };
        state.dispatching = true;
        state.queued.push_back(event);
        (handler, state.generation)
    };

    let mut active = ActiveHandler {
        state,
        handler: Some(handler),
        generation,
    };
    loop {
        let (generation_changed, replacement) = {
            let mut state = active
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if state.generation != active.generation {
                active.generation = state.generation;
                (true, state.handler.take())
            } else {
                (false, None)
            }
        };
        if generation_changed {
            active.handler = replacement;
            continue;
        }

        let event = {
            let mut state = active
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if active.handler.is_none() {
                let queued = std::mem::take(&mut state.queued);
                state.dispatching = false;
                drop(state);
                drop(queued);
                return;
            }
            let Some(event) = state.queued.pop_front() else {
                state.handler = active.handler.take();
                state.dispatching = false;
                return;
            };
            event
        };
        active.handler.as_mut().expect("checked handler")(event);
    }
}

struct ActiveHandler<'a> {
    state: &'a Mutex<EventDispatch>,
    handler: Option<ClientNodeEventHandler>,
    generation: u64,
}

impl Drop for ActiveHandler<'_> {
    fn drop(&mut self) {
        let displaced = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let displaced = if state.generation == self.generation && state.handler.is_none() {
                state.handler = self.handler.take();
                None
            } else {
                self.handler.take()
            };
            state.dispatching = false;
            displaced
        };
        drop(displaced);
    }
}

#[cfg(test)]
mod tests {
    use std::{
        io::{pipe, Read},
        panic::{catch_unwind, AssertUnwindSafe},
        sync::{mpsc, Arc, Mutex},
        time::Duration,
    };

    use pipewire_native_protocol::wire::client_node::{Command, Event, RegionRef, Transport};

    use super::{dispatch_event, set_event_handler, ClientNodeEventHandler, EventDispatch};

    const COMPLETION_TIMEOUT: Duration = Duration::from_secs(2);

    struct ReentrantHandlerDrop {
        state: Arc<Mutex<EventDispatch>>,
        replacement: Option<ClientNodeEventHandler>,
        dropped: mpsc::Sender<()>,
        _retained_fd: std::os::fd::OwnedFd,
    }

    impl Drop for ReentrantHandlerDrop {
        fn drop(&mut self) {
            set_event_handler(&self.state, self.replacement.take());
            self.dropped.send(()).unwrap();
        }
    }

    #[test]
    fn nested_dispatch_is_queued_in_order() {
        let state = Arc::new(Mutex::new(EventDispatch::default()));
        let observed = Arc::new(Mutex::new(Vec::new()));
        let nested_state = state.clone();
        let nested_observed = observed.clone();
        state.lock().unwrap().handler = Some(Box::new(move |event| {
            let Event::Command(command) = event else {
                panic!("wrong event")
            };
            nested_observed.lock().unwrap().push(command);
            if command == Command::Start {
                dispatch_event(&nested_state, Event::Command(Command::Pause));
            }
        }));

        dispatch_event(&state, Event::Command(Command::Start));
        assert_eq!(*observed.lock().unwrap(), [Command::Start, Command::Pause]);
    }

    #[test]
    fn displaced_handler_drop_can_reenter_during_generation_handoff() {
        let state = Arc::new(Mutex::new(EventDispatch::default()));
        let (dropped, drop_complete) = mpsc::channel();
        let (observed, observation) = mpsc::channel();
        let (mut retained_reader, retained_writer) = pipe().unwrap();
        let capture = ReentrantHandlerDrop {
            state: state.clone(),
            replacement: Some(Box::new(move |event| observed.send(event).unwrap())),
            dropped,
            _retained_fd: retained_writer.into(),
        };
        let callback_state = state.clone();
        set_event_handler(
            &state,
            Some(Box::new(move |event| {
                let _capture = &capture;
                assert!(matches!(event, Event::Command(Command::Start)));
                set_event_handler(&callback_state, Some(Box::new(|_| {})));
                dispatch_event(&callback_state, Event::Command(Command::Pause));
            })),
        );

        let dispatch_state = state.clone();
        let (finished, dispatch_complete) = mpsc::channel();
        std::thread::spawn(move || {
            dispatch_event(&dispatch_state, Event::Command(Command::Start));
            finished.send(()).unwrap();
        });

        drop_complete
            .recv_timeout(COMPLETION_TIMEOUT)
            .expect("handler destructor deadlocked while replacing the event handler");
        dispatch_complete
            .recv_timeout(COMPLETION_TIMEOUT)
            .expect("dispatch did not complete after generation replacement");
        assert!(matches!(
            observation.recv_timeout(COMPLETION_TIMEOUT).unwrap(),
            Event::Command(Command::Pause)
        ));
        assert_eq!(retained_reader.read(&mut [0]).unwrap(), 0);
        assert!(state.lock().unwrap().handler.is_some());
    }

    #[test]
    fn panic_restores_handler_and_retains_nested_queue() {
        let state = Arc::new(Mutex::new(EventDispatch::default()));
        let observed = Arc::new(Mutex::new(Vec::new()));
        let nested_state = state.clone();
        let nested_observed = observed.clone();
        let mut first = true;
        state.lock().unwrap().handler = Some(Box::new(move |event| {
            let Event::Command(command) = event else {
                panic!("wrong event")
            };
            nested_observed.lock().unwrap().push(command);
            if first {
                first = false;
                dispatch_event(&nested_state, Event::Command(Command::Pause));
                panic!("handler panic");
            }
        }));

        assert!(catch_unwind(AssertUnwindSafe(|| {
            dispatch_event(&state, Event::Command(Command::Start));
        }))
        .is_err());
        dispatch_event(&state, Event::Command(Command::Suspend));
        assert_eq!(
            *observed.lock().unwrap(),
            [Command::Start, Command::Pause, Command::Suspend]
        );
    }

    #[test]
    fn panic_drops_event_owned_fds() {
        let state = Mutex::new(EventDispatch::default());
        state.lock().unwrap().handler = Some(Box::new(|_| panic!("handler panic")));
        let (mut trigger_reader, trigger_writer) = pipe().unwrap();
        let (mut completion_reader, completion_writer) = pipe().unwrap();

        assert!(catch_unwind(AssertUnwindSafe(|| {
            dispatch_event(
                &state,
                Event::Transport(Transport {
                    trigger_fd: trigger_writer.into(),
                    completion_fd: completion_writer.into(),
                    activation: RegionRef {
                        memory_id: 1,
                        offset: 0,
                        size: 8,
                    },
                }),
            );
        }))
        .is_err());
        assert_eq!(trigger_reader.read(&mut [0]).unwrap(), 0);
        assert_eq!(completion_reader.read(&mut [0]).unwrap(), 0);
        assert!(state.lock().unwrap().handler.is_some());
    }
}
