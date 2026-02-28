// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: Copyright (c) 2026 Asymptotic Inc.

use std::io;

use crate::{
    shm::MemoryRegistry,
    transport::{BoundTransport, TransportConfig},
};

mod events;

pub use events::{ActivationRegion, AddMemEvent, MemoryType, RemoveMemEvent, TransportEvent};

/// Mutable bridge state that accumulates control-plane data-plane events.
#[derive(Debug, Default)]
pub struct ControlPlaneState {
    memory: MemoryRegistry,
    pending_transport: Option<TransportConfig>,
}

impl ControlPlaneState {
    /// Creates an empty control-plane bridge state.
    pub fn new() -> Self {
        Self::default()
    }

    /// Applies an `AddMem` event to local memory registry state.
    pub fn on_add_mem(&mut self, event: AddMemEvent) {
        // For now we track all memory kinds. Later we can enforce memfd-only paths where needed.
        let _ = event.memory_type;
        let _ = event.flags;
        self.memory.insert(event.id, event.fd);
    }

    /// Applies a `RemoveMem` event to local memory registry state.
    pub fn on_remove_mem(&mut self, event: RemoveMemEvent) {
        self.memory.remove(event.id);
    }

    /// Stores latest transport descriptor.
    pub fn on_transport(&mut self, event: TransportEvent) {
        self.pending_transport = Some(event.into());
    }

    /// Tries to bind a pending transport against the current memory registry.
    pub fn try_bind_transport(&mut self) -> io::Result<Option<BoundTransport>> {
        let Some(pending) = self.pending_transport.as_ref() else {
            return Ok(None);
        };

        if !self.memory.contains(pending.activation.mem_id) {
            return Ok(None);
        }

        let pending = self
            .pending_transport
            .take()
            .expect("pending transport should exist");
        BoundTransport::bind(pending, &self.memory).map(Some)
    }

    /// Returns a shared reference to the current memory registry.
    pub fn memory_registry(&self) -> &MemoryRegistry {
        &self.memory
    }

    /// Returns a mutable reference to the current memory registry.
    pub fn memory_registry_mut(&mut self) -> &mut MemoryRegistry {
        &mut self.memory
    }
}
