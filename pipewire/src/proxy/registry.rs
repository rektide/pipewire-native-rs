// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: Copyright (c) 2025 Asymptotic Inc.
// SPDX-FileCopyrightText: Copyright (c) 2025 Arun Raghavan

use std::sync::{Arc, Mutex};

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
    /// The registry object allows clients to enumerate and interact with objects. For more
    /// details, see the [Proxy](super::Proxy) documentation.
    pub struct Registry {
        proxy: Proxy,
        core: Core,
        methods: Arc<Mutex<RegistryMethods<Registry>>>,
        hooks: Arc<Mutex<spa::hook::HookList<RegistryEvents>>>,
    }
}

#[allow(clippy::type_complexity)]
pub(crate) struct RegistryMethods<T> {
    pub bind: Box<dyn FnMut(&T, Id, &str, u32) -> std::io::Result<Box<dyn HasProxy>>>,
    pub destroy: Box<dyn FnMut(&T, Id) -> std::io::Result<()>>,
}

/// Events that might be emitted by a [Registry].
#[allow(clippy::type_complexity)]
#[derive(Default)]
pub struct RegistryEvents {
    /// A global object was exported by the server. The object may be tracked using
    /// [Registry::bind()].
    pub global:
        Option<Box<dyn FnMut(Id, permission::PermissionBits, &str, u32, &Properties) + Send>>,
    /// A global was removed by the server.
    pub global_remove: Option<Box<dyn FnMut(Id) + Send>>,
}

impl HasProxy for Registry {
    fn type_(&self) -> types::ObjectType {
        types::interface::REGISTRY
    }

    fn version(&self) -> u32 {
        3
    }

    fn proxy(&self) -> &Proxy {
        &self.inner.proxy
    }
}

impl Registry {
    pub(crate) fn new(core: &Core) -> Self {
        let this = Self {
            inner: new_refcounted(InnerRegistry::new(core)),
        };

        core.add_proxy(&this);

        this
    }

    pub(crate) fn core(&self) -> Core {
        self.inner.core.clone()
    }

    /// Register to be notified of events on the registry.
    pub fn add_listener(&self, events: RegistryEvents) -> HookId {
        self.inner.hooks.lock().unwrap().append(events)
    }

    /// Remove a set of event listeners.
    pub fn remove_listener(&self, hook_id: HookId) {
        self.inner.hooks.lock().unwrap().remove(hook_id);
    }

    /// "Bind" to a given object, creating a proxy for it that can be used for method calls and
    /// event notifications.
    pub fn bind(&self, id: Id, type_: &str, version: u32) -> std::io::Result<Box<dyn HasProxy>> {
        object_invoke!(self, bind, id, type_, version)
    }

    /// Try to destroy the global object corresponding to this proxy. This may fail if the client
    /// does not have sufficient permissions.
    pub fn destroy(&self, id: Id) -> std::io::Result<()> {
        object_invoke!(self, destroy, id)
    }

    pub(crate) fn methods(&self) -> Arc<Mutex<RegistryMethods<Registry>>> {
        self.inner.methods.clone()
    }

    pub(crate) fn events(&self) -> Arc<Mutex<spa::hook::HookList<RegistryEvents>>> {
        self.inner.hooks.clone()
    }
}

impl InnerRegistry {
    fn new(core: &Core) -> Self {
        Self {
            proxy: Proxy::new(core.next_proxy_id()),
            core: core.clone(),
            methods: Arc::new(Mutex::new(protocol::marshal::registry::Methods::marshal(
                core.connection(),
            ))),
            hooks: spa::hook::HookList::new(),
        }
    }
}
