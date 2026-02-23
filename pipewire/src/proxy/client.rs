// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: Copyright (c) 2025 Asymptotic Inc.
// SPDX-FileCopyrightText: Copyright (c) 2025 Arun Raghavan

use std::sync::{Arc, Mutex};

use bitflags::bitflags;
use pipewire_native_spa as spa;

use crate::{
    core::Core,
    new_refcounted, object_invoke, permission,
    properties::Properties,
    protocol,
    proxy::{HasProxy, Proxy},
    refcounted, types, HookId, Id,
};

refcounted! {
    /// Proxy that represents a client that is connected to the server.
    pub struct Client {
        proxy: Proxy,
        methods: Arc<Mutex<ClientMethods<Client>>>,
        hooks: Arc<Mutex<spa::hook::HookList<ClientEvents>>>,
    }
}

#[allow(clippy::type_complexity)]
pub(crate) struct ClientMethods<T> {
    pub(crate) error: Box<dyn FnMut(&T, u32, u32, &str) -> std::io::Result<()>>,
    pub(crate) update_properties: Box<dyn FnMut(&T, &Properties) -> std::io::Result<()>>,
    pub(crate) get_permissions: Box<dyn FnMut(&T, u32, u32) -> std::io::Result<()>>,
    pub(crate) update_permissions:
        Box<dyn FnMut(&T, &[permission::Permission]) -> std::io::Result<()>>,
}

bitflags! {
    /// A bit mask of changes signalled in the [ClientEvents::info] event.
    #[repr(C)]
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct ClientChangeMask : u32 {
        /// Client properties changed.
        const PROPS = (1 << 0);
    }
}

/// Client information that is provided in a [ClientEvents::info] event.
pub struct ClientInfo<'a> {
    /// The ID of the client.
    pub id: Id,
    /// What changed since the last call.
    pub mask: ClientChangeMask,
    /// The client's properties.
    pub props: &'a Properties,
}

/// Client events that can be subscribed to.
#[allow(clippy::type_complexity)]
#[derive(Default)]
pub struct ClientEvents {
    /// Client information became available, or changed.
    pub info: Option<Box<dyn FnMut(&ClientInfo<'_>) + Send>>,
    /// Client permissions, notified due to a [Client::permissions()] call.
    pub permissions: Option<Box<dyn FnMut(u32, &[permission::Permission]) + Send>>,
}

impl HasProxy for Client {
    fn type_(&self) -> types::ObjectType {
        types::interface::CLIENT
    }

    fn version(&self) -> u32 {
        3
    }

    fn proxy(&self) -> &Proxy {
        &self.inner.proxy
    }
}

impl Client {
    pub(crate) fn new(core: &Core) -> Self {
        let this = Self {
            inner: new_refcounted(InnerClient::new(core)),
        };

        core.add_proxy(&this);

        this
    }

    /// Register for notifications of client events.
    pub fn add_listener(&self, events: ClientEvents) -> HookId {
        self.inner.hooks.lock().unwrap().append(events)
    }

    /// Remove a set of event listeners.
    pub fn remove_listener(&self, hook_id: HookId) {
        self.inner.hooks.lock().unwrap().remove(hook_id);
    }

    /// Signal an error to the client.
    pub fn error(&self, id: u32, res: u32, message: &str) -> std::io::Result<()> {
        object_invoke!(self, error, id, res, message)
    }

    /// Retrieve permissions of a client.
    pub fn permissions(&self, index: u32, num: u32) -> std::io::Result<()> {
        object_invoke!(self, get_permissions, index, num)
    }

    /// Update permissions of a client.
    pub fn update_permissions(
        &self,
        permissions: &[permission::Permission],
    ) -> std::io::Result<()> {
        object_invoke!(self, update_permissions, permissions)
    }

    pub(crate) fn methods(&self) -> Arc<Mutex<ClientMethods<Client>>> {
        self.inner.methods.clone()
    }

    pub(crate) fn events(&self) -> Arc<Mutex<spa::hook::HookList<ClientEvents>>> {
        self.inner.hooks.clone()
    }
}

impl InnerClient {
    fn new(core: &Core) -> Self {
        Self {
            proxy: Proxy::new(core.next_proxy_id()),
            methods: Arc::new(Mutex::new(protocol::marshal::client::Methods::marshal(
                core.connection(),
            ))),
            hooks: spa::hook::HookList::new(),
        }
    }
}
