// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: Copyright (c) 2026 Asymptotic Inc.

use std::os::fd::OwnedFd;

use pipewire_native_spa::buffer::data_type;

use crate::transport::{Activation, TransportConfig};

/// Memory kinds announced by `Core::AddMem`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MemoryType {
    /// Shared memory backed by `memfd`.
    MemFd,
    /// DMA-BUF memory.
    DmaBuf,
    /// Unknown memory kind.
    Unknown(u32),
}

impl From<u32> for MemoryType {
    fn from(value: u32) -> Self {
        match value {
            data_type::MEM_FD => Self::MemFd,
            data_type::DMA_BUF => Self::DmaBuf,
            other => Self::Unknown(other),
        }
    }
}

/// Adds a shared memory object to the local registry.
#[derive(Debug)]
pub struct AddMemEvent {
    /// The server-provided memory identifier.
    pub id: u32,
    /// Type of memory in this export.
    pub memory_type: MemoryType,
    /// Received fd for the memory object.
    pub fd: OwnedFd,
    /// Server flags associated with this memory object.
    pub flags: u32,
}

/// Removes a memory object from the local registry.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RemoveMemEvent {
    /// The server-provided memory identifier.
    pub id: u32,
}

/// Activation memory location shared by the transport peer.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ActivationRegion {
    /// Memory id announced through `AddMem`.
    pub mem_id: u32,
    /// Byte offset of the activation struct within the mem object.
    pub offset: usize,
    /// Size of the activation region in bytes.
    pub size: usize,
}

impl From<ActivationRegion> for Activation {
    fn from(value: ActivationRegion) -> Self {
        Self {
            mem_id: value.mem_id,
            offset: value.offset,
            size: value.size,
        }
    }
}

/// Transport descriptor containing node synchronization fds.
#[derive(Debug)]
pub struct TransportEvent {
    /// Eventfd the local node waits on for process triggers.
    pub read_fd: OwnedFd,
    /// Eventfd the local node writes to after processing.
    pub write_fd: OwnedFd,
    /// Activation memory location used for scheduling metadata.
    pub activation: ActivationRegion,
}

impl From<TransportEvent> for TransportConfig {
    fn from(value: TransportEvent) -> Self {
        Self {
            read_fd: value.read_fd,
            write_fd: value.write_fd,
            activation: value.activation.into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn memory_type_uses_spa_data_type_values() {
        assert_eq!(MemoryType::from(data_type::MEM_FD), MemoryType::MemFd);
        assert_eq!(MemoryType::from(data_type::DMA_BUF), MemoryType::DmaBuf);
    }

    #[test]
    fn memory_type_preserves_unhandled_values() {
        assert_eq!(MemoryType::from(data_type::INVALID), MemoryType::Unknown(0));
        assert_eq!(MemoryType::from(99), MemoryType::Unknown(99));
    }
}
