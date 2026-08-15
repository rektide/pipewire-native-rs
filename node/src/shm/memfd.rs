// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: Copyright (c) 2026 Asymptotic Inc.

use std::{
    ffi::CString,
    io,
    os::fd::{AsRawFd, BorrowedFd, FromRawFd, OwnedFd},
    ptr::NonNull,
};

/// Kernel seal information for a mapping's backing file.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SealStatus {
    /// `F_GET_SEALS` returned this seal bitmask.
    Available(i32),
    /// The backing file does not support querying seals.
    Unsupported,
}

impl SealStatus {
    /// Returns whether the backing file was sealed against shrinking when mapped.
    pub fn prevents_shrink(self) -> bool {
        matches!(self, Self::Available(seals) if seals & libc::F_SEAL_SHRINK != 0)
    }
}

/// Policy for mappings whose backing file can shrink after `mmap`.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum ShrinkPolicy {
    /// Permit unsealed or non-sealable imported files.
    ///
    /// Access remains unsafe because truncation can make the mapping raise `SIGBUS`.
    #[default]
    Allow,
    /// Require `F_SEAL_SHRINK` before creating the mapping.
    RequireSealed,
}

/// Creates a memfd-backed file descriptor and resizes it.
pub fn create_memfd(name: &str, size: usize) -> io::Result<OwnedFd> {
    let size = libc::off_t::try_from(size).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "memfd size is larger than supported off_t",
        )
    })?;
    let name = CString::new(name).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "memfd name cannot contain NUL bytes",
        )
    })?;

    let fd =
        unsafe { libc::memfd_create(name.as_ptr(), libc::MFD_CLOEXEC | libc::MFD_ALLOW_SEALING) };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }

    let fd = unsafe { OwnedFd::from_raw_fd(fd) };

    let res = unsafe { libc::ftruncate(fd.as_raw_fd(), size) };
    if res < 0 {
        return Err(io::Error::last_os_error());
    }

    // Mappings created from this fd cannot be invalidated by shrinking it. Imported
    // fds may not carry this seal; see `MappedRegion::map_shared`.
    let res = unsafe { libc::fcntl(fd.as_raw_fd(), libc::F_ADD_SEALS, libc::F_SEAL_SHRINK) };
    if res < 0 {
        return Err(io::Error::last_os_error());
    }

    Ok(fd)
}

/// Shared memory mapping backed by an imported memfd.
///
/// The requested logical region may start at an unaligned offset. The underlying
/// mapping is widened to a page boundary, while slice access remains restricted to
/// the requested region. Bounds are checked against the file size before `mmap`.
/// An imported fd that is not sealed against shrinking can still be truncated by
/// another process after this check, which may make later access raise `SIGBUS`.
#[derive(Debug)]
pub struct MappedRegion {
    mapping_ptr: NonNull<u8>,
    mapping_len: usize,
    ptr: NonNull<u8>,
    len: usize,
    seal_status: SealStatus,
}

// SAFETY: an mmap mapping may be unmapped from a thread other than the one that
// created it. Moving transfers ownership of this mapping object, and the type
// exposes no safe memory dereference; alias and foreign-access proofs are required
// by its unsafe slice methods.
unsafe impl Send for MappedRegion {}

impl MappedRegion {
    /// Maps a region from an fd with `MAP_SHARED`.
    pub fn map_shared(
        fd: BorrowedFd<'_>,
        offset: usize,
        len: usize,
        writable: bool,
    ) -> io::Result<Self> {
        Self::map_shared_with_policy(fd, offset, len, writable, ShrinkPolicy::Allow)
    }

    /// Maps a region from an fd with `MAP_SHARED` and an explicit shrink policy.
    pub fn map_shared_with_policy(
        fd: BorrowedFd<'_>,
        offset: usize,
        len: usize,
        writable: bool,
        shrink_policy: ShrinkPolicy,
    ) -> io::Result<Self> {
        if len == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "mapped region length must be greater than zero",
            ));
        }

        let end = offset.checked_add(len).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "mapped region offset and length overflow",
            )
        })?;
        if len > isize::MAX as usize {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "mapped region is too large for a Rust slice",
            ));
        }

        let mut stat = std::mem::MaybeUninit::<libc::stat>::uninit();
        let res = unsafe { libc::fstat(fd.as_raw_fd(), stat.as_mut_ptr()) };
        if res < 0 {
            return Err(io::Error::last_os_error());
        }
        let stat = unsafe { stat.assume_init() };
        let file_size = usize::try_from(stat.st_size).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "mapped file has a negative or unsupported size",
            )
        })?;
        if end > file_size {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "mapped region extends beyond the backing file",
            ));
        }

        let seal_status = seal_status(fd);
        if shrink_policy == ShrinkPolicy::RequireSealed && !seal_status.prevents_shrink() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "mapping policy requires a backing file sealed against shrinking",
            ));
        }

        let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
        let page_size = usize::try_from(page_size)
            .map_err(|_| io::Error::other("failed to determine system page size"))?;
        if page_size == 0 {
            return Err(io::Error::other("system page size is zero"));
        }

        let mapping_offset = offset / page_size * page_size;
        let logical_offset = offset - mapping_offset;
        let mapping_len = logical_offset.checked_add(len).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "physical mapping length overflow",
            )
        })?;
        if mapping_len > isize::MAX as usize {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "physical mapping is too large for pointer arithmetic",
            ));
        }
        let mapping_offset = libc::off_t::try_from(mapping_offset).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "mapping offset is larger than supported off_t",
            )
        })?;

        let prot = if writable {
            libc::PROT_READ | libc::PROT_WRITE
        } else {
            libc::PROT_READ
        };
        let mapping_ptr = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                mapping_len,
                prot,
                libc::MAP_SHARED,
                fd.as_raw_fd(),
                mapping_offset,
            )
        };

        if mapping_ptr == libc::MAP_FAILED {
            return Err(io::Error::last_os_error());
        }

        let Some(mapping_ptr) = NonNull::new(mapping_ptr.cast::<u8>()) else {
            let _ = unsafe { libc::munmap(mapping_ptr, mapping_len) };
            return Err(io::Error::other("mmap returned a null pointer"));
        };
        let ptr = unsafe { NonNull::new_unchecked(mapping_ptr.as_ptr().add(logical_offset)) };
        Ok(Self {
            mapping_ptr,
            mapping_len,
            ptr,
            len,
            seal_status,
        })
    }

    /// Returns mapping length in bytes.
    pub fn len(&self) -> usize {
        self.len
    }

    /// Returns true when mapping length is zero.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Returns the backing file's seal status observed immediately before `mmap`.
    pub fn seal_status(&self) -> SealStatus {
        self.seal_status
    }

    /// Returns a pointer to the first byte of the logical region.
    pub fn as_ptr(&self) -> *const u8 {
        self.ptr.as_ptr()
    }

    /// Returns a mutable pointer to the first byte of the logical region.
    pub fn as_mut_ptr(&mut self) -> *mut u8 {
        self.ptr.as_ptr()
    }

    /// Returns immutable bytes for the mapped region.
    ///
    /// # Safety
    ///
    /// For the returned reference's lifetime, the caller must guarantee that the
    /// backing object cannot shrink, that no thread or foreign process writes these
    /// bytes without synchronization valid for ordinary Rust memory, and that no
    /// mutable reference aliases the region. The backing object must also be ordinary
    /// byte-addressable memory; device and DMA mappings require their own access API.
    ///
    /// ```compile_fail
    /// # use pipewire_native_node::shm::MappedRegion;
    /// # fn read(region: &MappedRegion) {
    /// let _bytes = region.as_slice();
    /// # }
    /// ```
    pub unsafe fn as_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), self.len) }
    }

    /// Returns mutable bytes for the mapped region.
    ///
    /// # Safety
    ///
    /// For the returned reference's lifetime, the caller must guarantee exclusive
    /// access to these bytes across every mapping, thread, and foreign process. The
    /// backing object must be ordinary byte-addressable memory and cannot be allowed
    /// to shrink. Any foreign synchronization protocol must establish exclusive Rust
    /// access before this method is called and retain it until the reference expires.
    ///
    /// ```compile_fail
    /// # use pipewire_native_node::shm::MappedRegion;
    /// # fn write(region: &mut MappedRegion) {
    /// let _bytes = region.as_mut_slice();
    /// # }
    /// ```
    pub unsafe fn as_mut_slice(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr(), self.len) }
    }
}

fn seal_status(fd: BorrowedFd<'_>) -> SealStatus {
    let seals = unsafe { libc::fcntl(fd.as_raw_fd(), libc::F_GET_SEALS) };
    if seals < 0 {
        SealStatus::Unsupported
    } else {
        SealStatus::Available(seals)
    }
}

impl Drop for MappedRegion {
    fn drop(&mut self) {
        let _ = unsafe { libc::munmap(self.mapping_ptr.as_ptr().cast(), self.mapping_len) };
    }
}

#[cfg(test)]
mod tests {
    use std::{
        ffi::CString,
        io::ErrorKind,
        os::fd::{AsFd, AsRawFd, FromRawFd, OwnedFd},
    };

    use static_assertions::{assert_impl_all, assert_not_impl_any};

    use super::{create_memfd, MappedRegion, SealStatus, ShrinkPolicy};

    assert_impl_all!(MappedRegion: Send);
    assert_not_impl_any!(MappedRegion: Sync);

    #[test]
    fn map_and_mutate_memfd_region() {
        let fd = create_memfd("pipewire-native-node-test", 4096).unwrap();
        let mut region = MappedRegion::map_shared(fd.as_fd(), 0, 4096, true).unwrap();

        // SAFETY: this test owns the shrink-sealed memfd and has created no aliasing
        // mapping or foreign writer.
        unsafe {
            region.as_mut_slice()[0..4].copy_from_slice(&[0x11, 0x22, 0x33, 0x44]);
            assert_eq!(&region.as_slice()[0..4], &[0x11, 0x22, 0x33, 0x44]);
        }
        assert!(region.seal_status().prevents_shrink());
    }

    #[test]
    fn maps_unaligned_logical_region() {
        let fd = create_memfd("pipewire-native-node-unaligned", 4096).unwrap();
        let mut region = MappedRegion::map_shared(fd.as_fd(), 3, 5, true).unwrap();

        // SAFETY: accesses do not overlap in time, the backing memfd is shrink-sealed,
        // and this test is the only writer.
        unsafe { region.as_mut_slice().copy_from_slice(&[1, 2, 3, 4, 5]) };

        let full = MappedRegion::map_shared(fd.as_fd(), 0, 8, false).unwrap();
        assert_eq!(region.len(), 5);
        // SAFETY: the mutable borrow above ended and there are no foreign writers.
        assert_eq!(unsafe { &full.as_slice()[0..8] }, &[0, 0, 0, 1, 2, 3, 4, 5]);
    }

    #[test]
    fn rejects_region_past_end_of_file() {
        let fd = create_memfd("pipewire-native-node-short", 4096).unwrap();
        let error = MappedRegion::map_shared(fd.as_fd(), 4090, 7, false).unwrap_err();

        assert_eq!(error.kind(), ErrorKind::InvalidInput);
    }

    #[test]
    fn rejects_overflowing_region() {
        let fd = create_memfd("pipewire-native-node-overflow", 4096).unwrap();
        let error = MappedRegion::map_shared(fd.as_fd(), usize::MAX, 2, false).unwrap_err();

        assert_eq!(error.kind(), ErrorKind::InvalidInput);
    }

    #[test]
    fn rejects_empty_region() {
        let fd = create_memfd("pipewire-native-node-empty", 4096).unwrap();
        let error = MappedRegion::map_shared(fd.as_fd(), 0, 0, false).unwrap_err();

        assert_eq!(error.kind(), ErrorKind::InvalidInput);
    }

    #[cfg(target_pointer_width = "64")]
    #[test]
    fn rejects_memfd_size_that_does_not_fit_off_t() {
        let error = create_memfd("pipewire-native-node-too-large", usize::MAX).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidInput);
    }

    #[test]
    fn created_memfd_cannot_be_shrunk() {
        let fd = create_memfd("pipewire-native-node-sealed", 4096).unwrap();
        let result = unsafe { libc::ftruncate(fd.as_raw_fd(), 0) };

        assert_eq!(result, -1);
        assert_eq!(
            std::io::Error::last_os_error().raw_os_error(),
            Some(libc::EPERM)
        );
    }

    #[test]
    fn duplicate_mappings_only_expose_raw_pointers_safely() {
        let fd = create_memfd("pipewire-native-node-duplicate", 4096).unwrap();
        let mut first = MappedRegion::map_shared(fd.as_fd(), 0, 4096, true).unwrap();
        let second = MappedRegion::map_shared(fd.as_fd(), 0, 4096, false).unwrap();

        unsafe { first.as_mut_ptr().write_volatile(0x5a) };
        assert_eq!(unsafe { second.as_ptr().read_volatile() }, 0x5a);
        assert_ne!(first.as_ptr(), second.as_ptr());
    }

    #[test]
    fn sealed_policy_accepts_created_memfd() {
        let fd = create_memfd("pipewire-native-node-policy", 4096).unwrap();
        let region = MappedRegion::map_shared_with_policy(
            fd.as_fd(),
            0,
            4096,
            true,
            ShrinkPolicy::RequireSealed,
        )
        .unwrap();

        assert!(matches!(region.seal_status(), SealStatus::Available(_)));
        assert!(region.seal_status().prevents_shrink());
    }

    #[test]
    fn imported_unsealed_memfd_is_explicitly_permitted_or_rejected() {
        let name = CString::new("pipewire-native-node-unsealed").unwrap();
        let raw_fd = unsafe { libc::memfd_create(name.as_ptr(), libc::MFD_CLOEXEC) };
        assert!(raw_fd >= 0);
        let fd = unsafe { OwnedFd::from_raw_fd(raw_fd) };
        assert_eq!(unsafe { libc::ftruncate(fd.as_raw_fd(), 4096) }, 0);

        let allowed = MappedRegion::map_shared(fd.as_fd(), 0, 4096, true).unwrap();
        assert!(!allowed.seal_status().prevents_shrink());
        drop(allowed);

        let error = MappedRegion::map_shared_with_policy(
            fd.as_fd(),
            0,
            4096,
            true,
            ShrinkPolicy::RequireSealed,
        )
        .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidInput);
    }
}
