// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: Copyright (c) 2026 Asymptotic Inc.

//! Adapts Core memory events to a ClientNode session memory pool.

use std::{io, os::fd::OwnedFd, sync::Arc};

use parking_lot::Mutex;
use pipewire_native_node::session::memory::MemoryPool;

use crate::{
    core::{Core, CoreMemoryImporter},
    Id,
};

pub use pipewire_native_node::{
    session::memory::{MemoryError, MemoryId, MemoryKey, MemoryMapping, RegionRef},
    shm::ShrinkPolicy,
};

/// Session-facing access to memory imported by one Core connection.
///
/// Operations lock the pool only for their duration and never invoke Core callbacks. Mappings
/// retain their exact imported generation after the lock is released.
#[derive(Clone, Debug)]
pub struct MemoryPoolHandle {
    pool: Arc<Mutex<MemoryPool>>,
}

impl MemoryPoolHandle {
    /// Creates a pool, installs its private importer into `core`, and returns session access.
    ///
    /// Replacing the Core importer or disconnecting the Core permanently disconnects this pool.
    pub fn install(core: &Core, shrink_policy: ShrinkPolicy) -> Self {
        let pool = Arc::new(Mutex::new(MemoryPool::new(shrink_policy)));
        core.set_memory_importer(Some(Box::new(MemoryPoolImporter {
            pool: Arc::clone(&pool),
        })));
        Self { pool }
    }

    /// Returns the current generation key for an active Core memory ID.
    pub fn resolve(&self, id: MemoryId) -> Result<MemoryKey, MemoryError> {
        self.pool.lock().resolve(id)
    }

    /// Resolves and maps a checked region against the current active generation.
    pub fn bind(&self, region: RegionRef, writable: bool) -> Result<MemoryMapping, MemoryError> {
        self.pool.lock().bind(region, writable)
    }

    /// Maps a checked region only if `key` is still the active generation.
    pub fn map(
        &self,
        key: MemoryKey,
        offset: usize,
        len: usize,
        writable: bool,
    ) -> Result<MemoryMapping, MemoryError> {
        self.pool.lock().map(key, offset, len, writable)
    }

    /// Returns the number of active Core memory IDs.
    pub fn len(&self) -> usize {
        self.pool.lock().len()
    }

    /// Returns whether there are no active Core memory IDs.
    pub fn is_empty(&self) -> bool {
        self.pool.lock().is_empty()
    }
}

struct MemoryPoolImporter {
    pool: Arc<Mutex<MemoryPool>>,
}

impl CoreMemoryImporter for MemoryPoolImporter {
    fn add_memory(&mut self, id: Id, type_: u32, fd: OwnedFd, flags: u32) -> io::Result<()> {
        self.pool
            .lock()
            .add(MemoryId(id), type_, flags, fd)
            .map(|_| ())
            .map_err(import_error)
    }

    fn remove_memory(&mut self, id: Id) -> io::Result<()> {
        self.pool
            .lock()
            .remove(MemoryId(id))
            .map(|_| ())
            .map_err(import_error)
    }
}

impl Drop for MemoryPoolImporter {
    fn drop(&mut self) {
        self.pool.lock().disconnect();
    }
}

fn import_error(error: MemoryError) -> io::Error {
    let kind = match &error {
        MemoryError::DuplicateActiveId(_) => io::ErrorKind::AlreadyExists,
        MemoryError::UnknownMemory(_) | MemoryError::StaleGeneration { .. } => {
            io::ErrorKind::NotFound
        }
        // `Unsupported` is the protocol dispatcher's unknown-opcode sentinel and is non-terminal.
        MemoryError::UnknownMemoryType(_) | MemoryError::UnsupportedMemoryType(_) => {
            io::ErrorKind::InvalidData
        }
        MemoryError::Disconnected => io::ErrorKind::NotConnected,
        MemoryError::Inspect(source) => source.kind(),
        MemoryError::InvalidRegion { .. }
        | MemoryError::ShrinkableMemory(_)
        | MemoryError::Map { .. } => io::ErrorKind::InvalidData,
        MemoryError::GenerationExhausted => io::ErrorKind::Other,
    };
    io::Error::new(kind, error)
}

#[cfg(test)]
mod tests {
    use std::os::fd::{AsRawFd, RawFd};

    use pipewire_native_node::{session::memory::MemoryPool, shm::create_memfd};
    use pipewire_native_spa::buffer::data_type;

    use super::*;

    fn fd_is_open(fd: RawFd) -> bool {
        unsafe { libc::fcntl(fd, libc::F_GETFD) >= 0 }
    }

    #[test]
    fn importer_errors_preserve_pool_error_and_close_candidate_fd() {
        let pool = Arc::new(Mutex::new(MemoryPool::new(ShrinkPolicy::Allow)));
        let mut importer = MemoryPoolImporter {
            pool: Arc::clone(&pool),
        };
        importer
            .add_memory(7, data_type::MEM_FD, create_memfd("active", 64).unwrap(), 3)
            .unwrap();

        let duplicate = create_memfd("duplicate", 64).unwrap();
        let duplicate_raw = duplicate.as_raw_fd();
        let error = importer
            .add_memory(7, data_type::MEM_FD, duplicate, 0)
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::AlreadyExists);
        assert!(matches!(
            error
                .get_ref()
                .and_then(|error| error.downcast_ref::<MemoryError>()),
            Some(MemoryError::DuplicateActiveId(MemoryId(7)))
        ));
        assert!(!fd_is_open(duplicate_raw));

        let unsupported = create_memfd("unsupported", 64).unwrap();
        let unsupported_raw = unsupported.as_raw_fd();
        let error = importer
            .add_memory(8, data_type::DMA_BUF, unsupported, 0)
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(matches!(
            error
                .get_ref()
                .and_then(|error| error.downcast_ref::<MemoryError>()),
            Some(MemoryError::UnsupportedMemoryType(_))
        ));
        assert!(!fd_is_open(unsupported_raw));
    }

    #[test]
    fn importer_drop_terminally_disconnects_shared_pool() {
        let pool = Arc::new(Mutex::new(MemoryPool::new(ShrinkPolicy::Allow)));
        let handle = MemoryPoolHandle {
            pool: Arc::clone(&pool),
        };
        let mut importer = MemoryPoolImporter { pool };
        let fd = create_memfd("disconnect", 64).unwrap();
        let raw = fd.as_raw_fd();
        importer.add_memory(9, data_type::MEM_FD, fd, 0).unwrap();

        drop(importer);

        assert!(!fd_is_open(raw));
        assert!(matches!(
            handle.resolve(MemoryId(9)),
            Err(MemoryError::Disconnected)
        ));
    }
}
