// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: Copyright (c) 2026 Asymptotic Inc.

//! Connection-scoped ownership for memory imported by PipeWire Core events.

use std::{
    cell::Cell,
    collections::HashMap,
    fmt,
    marker::PhantomData,
    os::fd::{AsFd, AsRawFd, OwnedFd},
    sync::Arc,
};

use pipewire_native_spa::buffer::data_type::DataType;

use crate::shm::{MappedRegion, SealStatus, ShrinkPolicy};

/// Connection-local identifier from `Core::AddMem`.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct MemoryId(pub u32);

/// Stable identity of one imported-memory generation.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct MemoryKey {
    /// Numeric Core memory identifier.
    pub id: MemoryId,
    /// Monotonically increasing generation assigned when the FD is imported.
    pub generation: u64,
}

/// A checked byte range referring to an active memory ID.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RegionRef {
    /// Imported memory identifier.
    pub memory: MemoryId,
    /// Byte offset from the start of the backing file.
    pub offset: usize,
    /// Number of bytes in the logical mapping.
    pub len: usize,
}

/// Memory kind accepted by this first session slice.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MemoryKind {
    /// `SPA_DATA_MemFd` shared memory.
    MemFd,
}

#[derive(Debug)]
struct MemoryEntry {
    key: MemoryKey,
    backing: BackingIdentity,
    kind: MemoryKind,
    flags: u32,
    file_len: usize,
    fd: OwnedFd,
}

/// Stable Linux identity of a shared-memory backing object.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct BackingIdentity {
    device: libc::dev_t,
    inode: libc::ino_t,
}

/// An absolute byte interval in one backing object.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct MemoryInterval {
    pub(crate) backing: BackingIdentity,
    pub(crate) start: usize,
    pub(crate) end: usize,
}

impl MemoryInterval {
    pub(crate) fn overlaps(self, other: Self) -> bool {
        self.backing == other.backing && self.start < other.end && other.start < self.end
    }
}

/// Failure to import, resolve, or map connection memory.
#[derive(Debug)]
pub enum MemoryError {
    /// The SPA memory type value is not canonical.
    UnknownMemoryType(u32),
    /// The SPA memory type is known but unsupported by this slice.
    UnsupportedMemoryType(DataType),
    /// An active generation already owns this numeric ID.
    DuplicateActiveId(MemoryId),
    /// No active generation owns this numeric ID.
    UnknownMemory(MemoryId),
    /// A key names a retired generation or a different active generation.
    StaleGeneration {
        /// Key requested by the binding.
        requested: MemoryKey,
        /// Currently active key, if the ID has been reused.
        active: Option<MemoryKey>,
    },
    /// The requested range is empty, overflows, or exceeds the imported file.
    InvalidRegion {
        /// Generation being mapped.
        key: MemoryKey,
        /// Requested byte offset.
        offset: usize,
        /// Requested byte length.
        len: usize,
        /// Imported file length observed by `fstat`.
        file_len: usize,
    },
    /// The generation counter cannot assign another unique key.
    GenerationExhausted,
    /// The configured policy rejects a file that can still be shrunk.
    ShrinkableMemory(MemoryId),
    /// An FD metadata operation failed.
    Inspect(std::io::Error),
    /// Checked mapping creation failed.
    Map {
        /// Generation being mapped.
        key: MemoryKey,
        /// Mapping failure from the shared-memory substrate.
        source: std::io::Error,
    },
    /// The connection memory pool has been disconnected.
    Disconnected,
}

impl fmt::Display for MemoryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

impl std::error::Error for MemoryError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Inspect(source) | Self::Map { source, .. } => Some(source),
            _ => None,
        }
    }
}

/// Sole connection owner of imported memory FDs and active ID generations.
///
/// The pool is movable but deliberately not shareable. A mapping retains its exact
/// generation after removal, while all new resolution is restricted to `live`.
#[derive(Debug)]
pub struct MemoryPool {
    next_generation: u64,
    live: HashMap<MemoryId, Arc<MemoryEntry>>,
    shrink_policy: ShrinkPolicy,
    disconnected: bool,
    _not_sync: PhantomData<Cell<()>>,
}

/// Runtime-neutral exact-generation memory resolution used by node sessions.
pub trait MemoryResolver {
    /// Returns the currently active generation for a numeric memory ID.
    fn resolve(&self, id: MemoryId) -> Result<MemoryKey, MemoryError>;

    /// Maps only the requested exact generation.
    fn map(
        &self,
        key: MemoryKey,
        offset: usize,
        len: usize,
        writable: bool,
    ) -> Result<MemoryMapping, MemoryError>;
}

impl MemoryResolver for MemoryPool {
    fn resolve(&self, id: MemoryId) -> Result<MemoryKey, MemoryError> {
        MemoryPool::resolve(self, id)
    }

    fn map(
        &self,
        key: MemoryKey,
        offset: usize,
        len: usize,
        writable: bool,
    ) -> Result<MemoryMapping, MemoryError> {
        MemoryPool::map(self, key, offset, len, writable)
    }
}

impl MemoryPool {
    /// Creates an empty pool with an explicit backing-file shrink policy.
    pub fn new(shrink_policy: ShrinkPolicy) -> Self {
        Self {
            next_generation: 1,
            live: HashMap::new(),
            shrink_policy,
            disconnected: false,
            _not_sync: PhantomData,
        }
    }

    /// Imports one owned FD under a unique active memory ID.
    ///
    /// Failure consumes and closes `fd`. Only canonical `SPA_DATA_MemFd` is
    /// supported; other canonical SPA kinds are distinguished from unknown values.
    pub fn add(
        &mut self,
        id: MemoryId,
        memory_type: u32,
        flags: u32,
        fd: OwnedFd,
    ) -> Result<MemoryKey, MemoryError> {
        if self.disconnected {
            return Err(MemoryError::Disconnected);
        }
        if self.live.contains_key(&id) {
            return Err(MemoryError::DuplicateActiveId(id));
        }

        let memory_type = DataType::try_from(memory_type)
            .map_err(|()| MemoryError::UnknownMemoryType(memory_type))?;
        let kind = match memory_type {
            DataType::MemFd => MemoryKind::MemFd,
            other => return Err(MemoryError::UnsupportedMemoryType(other)),
        };
        let (file_len, backing) = file_identity(fd.as_raw_fd()).map_err(MemoryError::Inspect)?;
        if self.shrink_policy == ShrinkPolicy::RequireSealed
            && !seal_status(fd.as_raw_fd()).prevents_shrink()
        {
            return Err(MemoryError::ShrinkableMemory(id));
        }
        let generation = self.next_generation;
        self.next_generation = generation
            .checked_add(1)
            .ok_or(MemoryError::GenerationExhausted)?;
        let key = MemoryKey { id, generation };
        self.live.insert(
            id,
            Arc::new(MemoryEntry {
                key,
                backing,
                kind,
                flags,
                file_len,
                fd,
            }),
        );
        Ok(key)
    }

    /// Returns the current generation key for an active memory ID.
    pub fn resolve(&self, id: MemoryId) -> Result<MemoryKey, MemoryError> {
        if self.disconnected {
            return Err(MemoryError::Disconnected);
        }
        self.live
            .get(&id)
            .map(|entry| entry.key)
            .ok_or(MemoryError::UnknownMemory(id))
    }

    /// Resolves and maps a region against the current active generation.
    pub fn bind(&self, region: RegionRef, writable: bool) -> Result<MemoryMapping, MemoryError> {
        let key = self.resolve(region.memory)?;
        self.map(key, region.offset, region.len, writable)
    }

    /// Maps a region only if `key` is still the active generation.
    pub fn map(
        &self,
        key: MemoryKey,
        offset: usize,
        len: usize,
        writable: bool,
    ) -> Result<MemoryMapping, MemoryError> {
        if self.disconnected {
            return Err(MemoryError::Disconnected);
        }
        let entry = self.live.get(&key.id).ok_or(MemoryError::StaleGeneration {
            requested: key,
            active: None,
        })?;
        if entry.key != key {
            return Err(MemoryError::StaleGeneration {
                requested: key,
                active: Some(entry.key),
            });
        }
        let valid = len > 0
            && len <= isize::MAX as usize
            && offset
                .checked_add(len)
                .is_some_and(|end| end <= entry.file_len);
        if !valid {
            return Err(MemoryError::InvalidRegion {
                key,
                offset,
                len,
                file_len: entry.file_len,
            });
        }

        let mapped = MappedRegion::map_shared_with_policy(
            entry.fd.as_fd(),
            offset,
            len,
            writable,
            self.shrink_policy,
        )
        .map_err(|source| MemoryError::Map { key, source })?;
        Ok(MemoryMapping {
            mapped,
            entry: Arc::clone(entry),
            offset,
        })
    }

    /// Retires an active ID immediately, preventing all subsequent bindings.
    pub fn remove(&mut self, id: MemoryId) -> Result<MemoryKey, MemoryError> {
        if self.disconnected {
            return Err(MemoryError::Disconnected);
        }
        self.live
            .remove(&id)
            .map(|entry| entry.key)
            .ok_or(MemoryError::UnknownMemory(id))
    }

    /// Retires every active ID while keeping the pool reusable.
    pub fn clear(&mut self) {
        self.live.clear();
    }

    /// Retires all IDs and permanently rejects further imports and bindings.
    pub fn disconnect(&mut self) {
        self.live.clear();
        self.disconnected = true;
    }

    /// Returns the number of active numeric IDs.
    pub fn len(&self) -> usize {
        self.live.len()
    }

    /// Returns whether there are no active numeric IDs.
    pub fn is_empty(&self) -> bool {
        self.live.is_empty()
    }
}

/// Owned checked mapping that pins its imported FD generation until drop.
///
/// This type exposes pointers and explicitly unsafe byte borrows only. It never
/// turns asynchronously shared memory into a safe ordinary Rust slice.
pub struct MemoryMapping {
    mapped: MappedRegion,
    entry: Arc<MemoryEntry>,
    offset: usize,
}

impl fmt::Debug for MemoryMapping {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MemoryMapping")
            .field("key", &self.entry.key)
            .field("kind", &self.entry.kind)
            .field("flags", &self.entry.flags)
            .field("len", &self.mapped.len())
            .finish_non_exhaustive()
    }
}

impl MemoryMapping {
    /// Returns the exact imported generation retained by this mapping.
    pub fn key(&self) -> MemoryKey {
        self.entry.key
    }

    /// Returns the stable identity of the mapped backing object.
    pub fn backing_identity(&self) -> BackingIdentity {
        self.entry.backing
    }

    /// Returns this mapping's checked absolute backing-file interval.
    pub(crate) fn interval(&self) -> MemoryInterval {
        MemoryInterval {
            backing: self.entry.backing,
            start: self.offset,
            end: self.offset + self.mapped.len(),
        }
    }

    /// Returns the canonical imported memory kind.
    pub fn kind(&self) -> MemoryKind {
        self.entry.kind
    }

    /// Returns flags supplied by `Core::AddMem`.
    pub fn flags(&self) -> u32 {
        self.entry.flags
    }

    /// Returns the checked logical region length.
    pub fn len(&self) -> usize {
        self.mapped.len()
    }

    /// Returns whether the checked logical region is empty.
    pub fn is_empty(&self) -> bool {
        self.mapped.is_empty()
    }

    /// Returns a raw pointer to the checked logical region.
    pub fn as_ptr(&self) -> *const u8 {
        self.mapped.as_ptr()
    }

    /// Returns a mutable raw pointer to the checked logical region.
    pub fn as_mut_ptr(&mut self) -> *mut u8 {
        self.mapped.as_mut_ptr()
    }

    /// Creates a cycle-scoped exclusive borrow of this mapping.
    pub fn borrow(&mut self) -> MemoryRegionGuard<'_> {
        MemoryRegionGuard {
            key: self.entry.key,
            mapped: &mut self.mapped,
        }
    }
}

/// Exclusive cycle-scoped access to one checked mapping.
pub struct MemoryRegionGuard<'a> {
    key: MemoryKey,
    mapped: &'a mut MappedRegion,
}

impl fmt::Debug for MemoryRegionGuard<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MemoryRegionGuard")
            .field("key", &self.key)
            .field("len", &self.mapped.len())
            .finish_non_exhaustive()
    }
}

impl MemoryRegionGuard<'_> {
    /// Returns the retained generation key.
    pub fn key(&self) -> MemoryKey {
        self.key
    }

    /// Returns the checked logical region length.
    pub fn len(&self) -> usize {
        self.mapped.len()
    }

    /// Returns whether the checked logical region is empty.
    pub fn is_empty(&self) -> bool {
        self.mapped.is_empty()
    }

    /// Returns a raw pointer to the checked logical region.
    pub fn as_ptr(&self) -> *const u8 {
        self.mapped.as_ptr()
    }

    /// Returns a mutable raw pointer to the checked logical region.
    pub fn as_mut_ptr(&mut self) -> *mut u8 {
        self.mapped.as_mut_ptr()
    }

    /// Borrows bytes after the caller establishes foreign-memory synchronization.
    ///
    /// # Safety
    ///
    /// The backing file must remain non-shrinkable, and no foreign or Rust writer
    /// may access these bytes for the returned reference's lifetime.
    pub unsafe fn bytes(&self) -> &[u8] {
        unsafe { self.mapped.as_slice() }
    }

    /// Exclusively borrows bytes after the caller claims the shared-memory cycle.
    ///
    /// # Safety
    ///
    /// The caller must have protocol-level exclusive access across every mapping and
    /// foreign process, and the backing file must remain non-shrinkable.
    pub unsafe fn bytes_mut(&mut self) -> &mut [u8] {
        unsafe { self.mapped.as_mut_slice() }
    }
}

fn file_identity(fd: std::os::fd::RawFd) -> std::io::Result<(usize, BackingIdentity)> {
    let mut stat = std::mem::MaybeUninit::<libc::stat>::uninit();
    if unsafe { libc::fstat(fd, stat.as_mut_ptr()) } < 0 {
        return Err(std::io::Error::last_os_error());
    }
    let stat = unsafe { stat.assume_init() };
    let len = usize::try_from(stat.st_size).map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "imported memory has a negative or unsupported size",
        )
    })?;
    Ok((
        len,
        BackingIdentity {
            device: stat.st_dev,
            inode: stat.st_ino,
        },
    ))
}

fn seal_status(fd: std::os::fd::RawFd) -> SealStatus {
    let seals = unsafe { libc::fcntl(fd, libc::F_GET_SEALS) };
    if seals < 0 {
        SealStatus::Unsupported
    } else {
        SealStatus::Available(seals)
    }
}

#[cfg(test)]
mod tests {
    use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};

    use pipewire_native_spa::buffer::data_type;
    use static_assertions::{assert_impl_all, assert_not_impl_any};

    use super::*;
    use crate::shm::create_memfd;

    assert_impl_all!(MemoryPool: Send);
    assert_not_impl_any!(MemoryPool: Sync);
    assert_impl_all!(MemoryMapping: Send);
    assert_not_impl_any!(MemoryMapping: Sync);

    fn sealed(name: &str, len: usize) -> OwnedFd {
        create_memfd(name, len).unwrap()
    }

    fn fd_is_open(fd: RawFd) -> bool {
        unsafe { libc::fcntl(fd, libc::F_GETFD) >= 0 }
    }

    fn fd_identity(fd: RawFd) -> (libc::dev_t, libc::ino_t) {
        let mut stat = std::mem::MaybeUninit::<libc::stat>::uninit();
        assert_eq!(unsafe { libc::fstat(fd, stat.as_mut_ptr()) }, 0);
        let stat = unsafe { stat.assume_init() };
        (stat.st_dev, stat.st_ino)
    }

    fn fd_identity_is_open(fd: RawFd, identity: (libc::dev_t, libc::ino_t)) -> bool {
        let mut stat = std::mem::MaybeUninit::<libc::stat>::uninit();
        (unsafe { libc::fstat(fd, stat.as_mut_ptr()) == 0 }) && {
            let stat = unsafe { stat.assume_init() };
            (stat.st_dev, stat.st_ino) == identity
        }
    }

    #[test]
    fn duplicate_active_ids_are_rejected_and_candidate_fd_is_closed() {
        let mut pool = MemoryPool::new(ShrinkPolicy::RequireSealed);
        pool.add(MemoryId(7), data_type::MEM_FD, 0, sealed("first", 64))
            .unwrap();
        let duplicate = sealed("duplicate", 64);
        let raw = duplicate.as_raw_fd();
        let identity = fd_identity(raw);
        assert!(matches!(
            pool.add(MemoryId(7), data_type::MEM_FD, 0, duplicate),
            Err(MemoryError::DuplicateActiveId(MemoryId(7)))
        ));
        assert!(!fd_identity_is_open(raw, identity));
    }

    #[test]
    fn unknown_and_unsupported_memory_types_are_distinct_and_close_fds() {
        let mut pool = MemoryPool::new(ShrinkPolicy::RequireSealed);
        let unsupported = sealed("unsupported", 64);
        let unsupported_raw = unsupported.as_raw_fd();
        let unsupported_identity = fd_identity(unsupported_raw);
        assert!(matches!(
            pool.add(MemoryId(1), data_type::DMA_BUF, 0, unsupported),
            Err(MemoryError::UnsupportedMemoryType(DataType::DmaBuf))
        ));
        assert!(!fd_identity_is_open(unsupported_raw, unsupported_identity));

        let unknown = sealed("unknown", 64);
        let unknown_raw = unknown.as_raw_fd();
        let unknown_identity = fd_identity(unknown_raw);
        assert!(matches!(
            pool.add(MemoryId(1), 99, 0, unknown),
            Err(MemoryError::UnknownMemoryType(99))
        ));
        assert!(!fd_identity_is_open(unknown_raw, unknown_identity));
    }

    #[test]
    fn checked_regions_reject_empty_overflow_and_out_of_bounds_ranges() {
        let mut pool = MemoryPool::new(ShrinkPolicy::RequireSealed);
        let key = pool
            .add(MemoryId(2), data_type::MEM_FD, 0, sealed("bounds", 64))
            .unwrap();
        for (offset, len) in [(0, 0), (usize::MAX, 2), (60, 5)] {
            assert!(matches!(
                pool.map(key, offset, len, true),
                Err(MemoryError::InvalidRegion { .. })
            ));
        }
        assert_eq!(pool.map(key, 3, 5, true).unwrap().len(), 5);
    }

    #[test]
    fn remove_with_active_mapping_retires_binding_but_preserves_guard() {
        let mut pool = MemoryPool::new(ShrinkPolicy::RequireSealed);
        let fd = sealed("retire", 64);
        let raw = fd.as_raw_fd();
        let identity = fd_identity(raw);
        let key = pool.add(MemoryId(3), data_type::MEM_FD, 0, fd).unwrap();
        let mut mapping = pool.map(key, 0, 64, true).unwrap();
        pool.remove(MemoryId(3)).unwrap();
        assert!(matches!(
            pool.map(key, 0, 1, true),
            Err(MemoryError::StaleGeneration { active: None, .. })
        ));
        assert!(fd_is_open(raw));
        {
            let mut guard = mapping.borrow();
            unsafe { guard.bytes_mut()[0] = 0x5a };
            assert_eq!(unsafe { guard.bytes()[0] }, 0x5a);
        }
        drop(mapping);
        assert!(!fd_identity_is_open(raw, identity));
    }

    #[test]
    fn reused_id_gets_new_generation_and_rejects_old_key() {
        let mut pool = MemoryPool::new(ShrinkPolicy::RequireSealed);
        let first = pool
            .add(MemoryId(4), data_type::MEM_FD, 0, sealed("old", 64))
            .unwrap();
        pool.remove(MemoryId(4)).unwrap();
        let second = pool
            .add(MemoryId(4), data_type::MEM_FD, 0, sealed("new", 64))
            .unwrap();
        assert_ne!(first, second);
        assert!(matches!(
            pool.map(first, 0, 1, false),
            Err(MemoryError::StaleGeneration {
                active: Some(active),
                ..
            }) if active == second
        ));
        assert_eq!(pool.map(second, 0, 1, false).unwrap().key(), second);
    }

    #[test]
    fn duplicated_handles_share_backing_identity_across_ids_and_generations() {
        let mut pool = MemoryPool::new(ShrinkPolicy::RequireSealed);
        let fd = sealed("aliased", 64);
        let duplicate = fd.try_clone().unwrap();
        let first = pool.add(MemoryId(20), data_type::MEM_FD, 0, fd).unwrap();
        let second = pool
            .add(MemoryId(21), data_type::MEM_FD, 0, duplicate)
            .unwrap();
        let first_mapping = pool.map(first, 4, 16, true).unwrap();
        let second_mapping = pool.map(second, 8, 16, true).unwrap();
        assert_ne!(first, second);
        assert_eq!(
            first_mapping.backing_identity(),
            second_mapping.backing_identity()
        );
        assert!(first_mapping.interval().overlaps(second_mapping.interval()));

        pool.remove(MemoryId(20)).unwrap();
        let reused_fd = pool
            .live
            .get(&MemoryId(21))
            .unwrap()
            .fd
            .try_clone()
            .unwrap();
        let reused = pool
            .add(MemoryId(20), data_type::MEM_FD, 0, reused_fd)
            .unwrap();
        assert_ne!(first, reused);
        assert_eq!(
            first_mapping.backing_identity(),
            pool.map(reused, 0, 1, true).unwrap().backing_identity()
        );
    }

    #[test]
    fn clear_is_reusable_and_disconnect_is_terminal() {
        let mut pool = MemoryPool::new(ShrinkPolicy::RequireSealed);
        let clear_fd = sealed("clear", 64);
        let clear_raw = clear_fd.as_raw_fd();
        let clear_identity = fd_identity(clear_raw);
        pool.add(MemoryId(5), data_type::MEM_FD, 0, clear_fd)
            .unwrap();
        pool.clear();
        assert!(pool.is_empty());
        assert!(!fd_identity_is_open(clear_raw, clear_identity));

        let reuse_fd = sealed("reuse", 64);
        let reuse_raw = reuse_fd.as_raw_fd();
        let reuse_identity = fd_identity(reuse_raw);
        let key = pool
            .add(MemoryId(5), data_type::MEM_FD, 0, reuse_fd)
            .unwrap();
        let mapping = pool.map(key, 0, 64, false).unwrap();
        pool.disconnect();
        assert!(pool.is_empty());
        assert!(fd_is_open(reuse_raw));
        assert!(matches!(
            pool.resolve(MemoryId(5)),
            Err(MemoryError::Disconnected)
        ));
        assert!(matches!(
            pool.add(MemoryId(6), data_type::MEM_FD, 0, sealed("late", 64)),
            Err(MemoryError::Disconnected)
        ));
        drop(mapping);
        assert!(!fd_identity_is_open(reuse_raw, reuse_identity));
    }

    #[test]
    fn repeated_retire_cycles_return_matching_proc_fds_to_baseline() {
        fn matching_fds() -> usize {
            std::fs::read_dir("/proc/self/fd")
                .unwrap()
                .filter_map(Result::ok)
                .filter(|entry| {
                    std::fs::read_link(entry.path())
                        .is_ok_and(|target| target.to_string_lossy().contains("memory-pool-cycle"))
                })
                .count()
        }

        let baseline = matching_fds();
        let mut pool = MemoryPool::new(ShrinkPolicy::RequireSealed);
        for generation in 0..64 {
            let name = format!("memory-pool-cycle-{generation}");
            let key = pool
                .add(MemoryId(9), data_type::MEM_FD, 0, sealed(&name, 4096))
                .unwrap();
            let mut mapping = pool.map(key, 0, 4096, true).unwrap();
            pool.remove(MemoryId(9)).unwrap();
            unsafe { mapping.borrow().bytes_mut()[0] = generation };
            drop(mapping);
        }
        assert_eq!(matching_fds(), baseline);
    }

    #[test]
    fn explicit_seal_policy_rejects_shrinkable_imports() {
        let name = std::ffi::CString::new("unsealed-session-memory").unwrap();
        let raw = unsafe { libc::memfd_create(name.as_ptr(), libc::MFD_CLOEXEC) };
        assert!(raw >= 0);
        let fd = unsafe { OwnedFd::from_raw_fd(raw) };
        let identity = fd_identity(raw);
        assert_eq!(unsafe { libc::ftruncate(raw, 64) }, 0);
        let mut pool = MemoryPool::new(ShrinkPolicy::RequireSealed);
        assert!(matches!(
            pool.add(MemoryId(10), data_type::MEM_FD, 0, fd),
            Err(MemoryError::ShrinkableMemory(MemoryId(10)))
        ));
        assert!(!fd_identity_is_open(raw, identity));
    }
}
