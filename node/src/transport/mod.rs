// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: Copyright (c) 2026 Asymptotic Inc.

use std::{io, os::fd::OwnedFd};

use crate::{
    shm::{MappedRegion, MemoryRegistry},
    signal::EventFd,
};

/// Activation memory location for a node transport.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Activation {
    /// Memory id that contains activation data.
    pub mem_id: u32,
    /// Byte offset of the activation data within the memory object.
    pub offset: usize,
    /// Activation mapping size in bytes.
    pub size: usize,
}

/// Raw transport descriptor from control-plane events.
#[derive(Debug)]
pub struct TransportConfig {
    /// Eventfd used to trigger local processing.
    pub read_fd: OwnedFd,
    /// Eventfd used to signal local processing completion.
    pub write_fd: OwnedFd,
    /// Activation memory location.
    pub activation: Activation,
}

/// Bound transport with mapped activation memory and signal handles.
#[derive(Debug)]
pub struct BoundTransport {
    trigger: EventFd,
    complete: EventFd,
    activation: MappedRegion,
}

impl BoundTransport {
    /// Binds a transport descriptor against imported shared memory.
    pub fn bind(config: TransportConfig, registry: &MemoryRegistry) -> io::Result<Self> {
        let activation = registry.map(
            config.activation.mem_id,
            config.activation.offset,
            config.activation.size,
            true,
        )?;

        Ok(Self {
            trigger: EventFd::from_owned_fd(config.read_fd)?,
            complete: EventFd::from_owned_fd(config.write_fd)?,
            activation,
        })
    }

    /// Waits for a processing trigger.
    pub async fn wait_cycle(&self) -> io::Result<u64> {
        self.trigger.wait().await
    }

    /// Signals processing completion.
    pub fn signal_complete(&self, count: u64) -> io::Result<()> {
        self.complete.signal(count)
    }

    /// Returns immutable activation bytes.
    pub fn activation(&self) -> &[u8] {
        self.activation.as_slice()
    }

    /// Returns mutable activation bytes.
    pub fn activation_mut(&mut self) -> &mut [u8] {
        self.activation.as_mut_slice()
    }
}
