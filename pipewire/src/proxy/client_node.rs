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
        let mut state = self.inner.event_dispatch.lock().unwrap();
        state.generation = state.generation.wrapping_add(1);
        state.handler = handler;
    }

    pub(crate) fn methods(&self) -> Arc<Mutex<ClientNodeMethods>> {
        self.inner.methods.clone()
    }

    pub(crate) fn dispatch(&self, event: wire::Event) {
        dispatch_event(&self.inner.event_dispatch, event);
    }
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
        let event = {
            let mut state = active
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if state.generation != active.generation {
                active.handler = state.handler.take();
                active.generation = state.generation;
            }
            let Some(_) = active.handler else {
                state.queued.clear();
                state.dispatching = false;
                return;
            };
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
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.generation == self.generation && state.handler.is_none() {
            state.handler = self.handler.take();
        }
        state.dispatching = false;
    }
}

#[cfg(test)]
mod tests {
    use std::{
        panic::{catch_unwind, AssertUnwindSafe},
        sync::{Arc, Mutex},
    };

    use pipewire_native_protocol::wire::client_node::{Command, Event};

    use super::{dispatch_event, EventDispatch};

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
}
