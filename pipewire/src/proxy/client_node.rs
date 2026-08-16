// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: Copyright (c) 2026 Asymptotic Inc.

use std::sync::{Arc, Mutex};

use crate::{
    core::Core,
    new_refcounted, object_invoke, protocol,
    proxy::{HasProxy, Proxy},
    refcounted, types,
};
use pipewire_native_protocol::wire::client_node::{self as wire, Method};

refcounted! {
    /// Typed proxy for a client-created ClientNode v6 object.
    pub struct ClientNode {
        proxy: Proxy,
        methods: Arc<Mutex<ClientNodeMethods>>,
        event_handler: Mutex<Option<Box<dyn FnMut(wire::Event) + Send>>>,
    }
}

pub(crate) struct ClientNodeMethods {
    pub(crate) send: Box<dyn FnMut(&ClientNode, Method) -> std::io::Result<()>>,
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
                event_handler: Mutex::new(None),
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
    pub fn set_event_handler(&self, handler: Option<Box<dyn FnMut(wire::Event) + Send>>) {
        *self.inner.event_handler.lock().unwrap() = handler;
    }

    pub(crate) fn methods(&self) -> Arc<Mutex<ClientNodeMethods>> {
        self.inner.methods.clone()
    }

    pub(crate) fn dispatch(&self, event: wire::Event) {
        let Some(mut handler) = self.inner.event_handler.lock().unwrap().take() else {
            return;
        };
        handler(event);
        let mut slot = self.inner.event_handler.lock().unwrap();
        if slot.is_none() {
            *slot = Some(handler);
        }
    }
}
