// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: Copyright (c) 2026 Asymptotic Inc.

use std::{
    collections::HashMap,
    io,
    os::fd::{AsFd, OwnedFd},
    sync::Arc,
};

use super::MappedRegion;

/// Registry for memfd objects announced by PipeWire `Core::AddMem`.
#[derive(Debug, Default)]
pub struct MemoryRegistry {
    memory: HashMap<u32, Arc<OwnedFd>>,
}

impl MemoryRegistry {
    /// Inserts or replaces a memory fd by id.
    pub fn insert(&mut self, id: u32, fd: OwnedFd) -> Option<Arc<OwnedFd>> {
        self.memory.insert(id, Arc::new(fd))
    }

    /// Removes a memory fd by id.
    pub fn remove(&mut self, id: u32) -> Option<Arc<OwnedFd>> {
        self.memory.remove(&id)
    }

    /// Returns true when the registry has this memory id.
    pub fn contains(&self, id: u32) -> bool {
        self.memory.contains_key(&id)
    }

    /// Maps a byte range from a registered memory id.
    pub fn map(
        &self,
        id: u32,
        offset: usize,
        size: usize,
        writable: bool,
    ) -> io::Result<MappedRegion> {
        let fd = self.memory.get(&id).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("memory id {id} has not been registered"),
            )
        })?;

        MappedRegion::map_shared(fd.as_fd(), offset, size, writable)
    }
}
