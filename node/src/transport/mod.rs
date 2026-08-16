// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: Copyright (c) 2026 Asymptotic Inc.

use std::{io, os::fd::OwnedFd};

use crate::{
    shm::{MappedRegion, MemoryRegistry, ShrinkPolicy},
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
    ///
    /// PipeWire imports are permitted without a shrink seal. Use
    /// [`Self::bind_with_policy`] when the backing file must be shrink-safe.
    pub fn bind(config: TransportConfig, registry: &MemoryRegistry) -> io::Result<Self> {
        Self::bind_with_policy(config, registry, ShrinkPolicy::Allow)
    }

    /// Binds a transport descriptor with an explicit activation shrink policy.
    pub fn bind_with_policy(
        config: TransportConfig,
        registry: &MemoryRegistry,
        shrink_policy: ShrinkPolicy,
    ) -> io::Result<Self> {
        let activation = registry.map_with_policy(
            config.activation.mem_id,
            config.activation.offset,
            config.activation.size,
            true,
            shrink_policy,
        )?;

        Ok(Self {
            trigger: EventFd::from_owned_fd(config.read_fd)?,
            complete: EventFd::from_owned_fd(config.write_fd)?,
            activation,
        })
    }

    /// Returns the non-blocking process-wake eventfd for a runtime adapter.
    pub fn trigger(&self) -> &EventFd {
        &self.trigger
    }

    /// Drains one process-wake counter without waiting.
    pub fn drain_cycle(&self) -> io::Result<u64> {
        self.trigger.drain()
    }

    /// Signals processing completion.
    pub fn signal_complete(&self, count: u64) -> io::Result<()> {
        self.complete.signal(count)
    }

    /// Returns the raw activation mapping.
    ///
    /// Constructing references to its bytes remains unsafe because PipeWire may
    /// access the same shared memory and imported files may not be shrink-sealed.
    pub fn activation(&self) -> &MappedRegion {
        &self.activation
    }

    /// Returns the raw activation mapping with exclusive access to this mapping owner.
    ///
    /// This borrow does not prove exclusivity from duplicate mappings or PipeWire.
    pub fn activation_mut(&mut self) -> &mut MappedRegion {
        &mut self.activation
    }
}

#[cfg(test)]
mod tests {
    use std::os::fd::{FromRawFd, OwnedFd};

    use tokio::runtime::Builder;

    use crate::shm::{create_memfd, MemoryRegistry};

    use super::{Activation, BoundTransport, TransportConfig};

    #[test]
    fn bind_transport_maps_activation_region() {
        let runtime = Builder::new_current_thread().enable_io().build().unwrap();

        runtime.block_on(async {
            let memfd = create_memfd("pipewire-native-node-activation", 4096).unwrap();
            let mut registry = MemoryRegistry::default();
            registry.insert(42, memfd);

            let transport = BoundTransport::bind(
                TransportConfig {
                    read_fd: new_eventfd().unwrap(),
                    write_fd: new_eventfd().unwrap(),
                    activation: Activation {
                        mem_id: 42,
                        offset: 0,
                        size: 256,
                    },
                },
                &registry,
            )
            .unwrap();

            assert_eq!(transport.activation().len(), 256);
            assert!(transport.activation().seal_status().prevents_shrink());
        });
    }

    fn new_eventfd() -> std::io::Result<OwnedFd> {
        let fd = unsafe { libc::eventfd(0, libc::EFD_CLOEXEC | libc::EFD_NONBLOCK) };
        if fd < 0 {
            return Err(std::io::Error::last_os_error());
        }

        Ok(unsafe { OwnedFd::from_raw_fd(fd) })
    }
}
