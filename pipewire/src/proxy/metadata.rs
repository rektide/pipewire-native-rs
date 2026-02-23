// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: Copyright (c) 2025 Asymptotic Inc.
// SPDX-FileCopyrightText: Copyright (c) 2025 Arun Raghavan

use std::sync::{Arc, Mutex};

use pipewire_native_spa as spa;

use crate::{
    core::Core,
    new_refcounted, object_invoke, protocol,
    proxy::{HasProxy, Proxy},
    refcounted, types, HookId, Id,
};

refcounted! {
    /// Proxy that represents a metadata that is connected to the server.
    pub struct Metadata {
        proxy: Proxy,
        methods: Arc<Mutex<MetadataMethods<Metadata>>>,
        hooks: Arc<Mutex<spa::hook::HookList<MetadataEvents>>>,
    }
}

#[allow(clippy::type_complexity)]
pub(crate) struct MetadataMethods<T> {
    pub(crate) set_property:
        Box<dyn FnMut(&T, Id, Option<&str>, Option<&str>, Option<&str>) -> std::io::Result<()>>,
    pub(crate) clear: Box<dyn FnMut(&T) -> std::io::Result<()>>,
}

/// Metadata events that can be subscribed to.
#[allow(clippy::type_complexity)]
#[derive(Default)]
pub struct MetadataEvents {
    /// Metadata property was added, removed, or changed.
    pub property: Option<Box<dyn FnMut(Id, Option<&str>, Option<&str>, Option<&str>) + Send>>,
}

impl HasProxy for Metadata {
    fn type_(&self) -> types::ObjectType {
        types::interface::METADATA
    }

    fn version(&self) -> u32 {
        3
    }

    fn proxy(&self) -> &Proxy {
        &self.inner.proxy
    }
}

impl Metadata {
    pub(crate) fn new(core: &Core) -> Self {
        let this = Self {
            inner: new_refcounted(InnerMetadata::new(core)),
        };

        core.add_proxy(&this);

        this
    }

    /// Register for notifications of metadata events.
    pub fn add_listener(&self, events: MetadataEvents) -> HookId {
        self.inner.hooks.lock().unwrap().append(events)
    }

    /// Remove a set of event listeners.
    pub fn remove_listener(&self, hook_id: HookId) {
        self.inner.hooks.lock().unwrap().remove(hook_id);
    }

    /// Set a property on the metadata. A [None] `key` removes all properties on the `subject`. A
    /// [None] `value` clears the property specified by `key` on the `subject`. The `type_`
    /// parameter is optional.
    pub fn set_property(
        &self,
        subject: Id,
        key: Option<&str>,
        type_: Option<&str>,
        value: Option<&str>,
    ) -> std::io::Result<()> {
        object_invoke!(self, set_property, subject, key, type_, value)
    }

    /// Clear all metadata.
    pub fn clear(&self) -> std::io::Result<()> {
        object_invoke!(self, clear)
    }

    pub(crate) fn methods(&self) -> Arc<Mutex<MetadataMethods<Metadata>>> {
        self.inner.methods.clone()
    }

    pub(crate) fn events(&self) -> Arc<Mutex<spa::hook::HookList<MetadataEvents>>> {
        self.inner.hooks.clone()
    }
}

impl InnerMetadata {
    fn new(core: &Core) -> Self {
        Self {
            proxy: Proxy::new(core.next_proxy_id()),
            methods: Arc::new(Mutex::new(protocol::marshal::metadata::Methods::marshal(
                core.connection(),
            ))),
            hooks: spa::hook::HookList::new(),
        }
    }
}
