// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: Copyright (c) 2026 Asymptotic Inc.

use std::{
    ffi::CString,
    io,
    os::fd::{AsRawFd, BorrowedFd, FromRawFd, OwnedFd},
    ptr::NonNull,
};

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
}

// SAFETY: an mmap mapping may be accessed and unmapped from a thread other than
// the one that created it. Moving this value transfers its only Rust owner, and
// mutable slice access requires an exclusive borrow.
unsafe impl Send for MappedRegion {}

impl MappedRegion {
    /// Maps a region from an fd with `MAP_SHARED`.
    pub fn map_shared(
        fd: BorrowedFd<'_>,
        offset: usize,
        len: usize,
        writable: bool,
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

    /// Returns immutable bytes for the mapped region.
    pub fn as_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), self.len) }
    }

    /// Returns mutable bytes for the mapped region.
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr(), self.len) }
    }
}

impl Drop for MappedRegion {
    fn drop(&mut self) {
        let _ = unsafe { libc::munmap(self.mapping_ptr.as_ptr().cast(), self.mapping_len) };
    }
}

#[cfg(test)]
mod tests {
    use std::{io::ErrorKind, os::fd::AsFd};

    use static_assertions::{assert_impl_all, assert_not_impl_any};

    use super::{create_memfd, MappedRegion};

    assert_impl_all!(MappedRegion: Send);
    assert_not_impl_any!(MappedRegion: Sync);

    #[test]
    fn map_and_mutate_memfd_region() {
        let fd = create_memfd("pipewire-native-node-test", 4096).unwrap();
        let mut region = MappedRegion::map_shared(fd.as_fd(), 0, 4096, true).unwrap();

        region.as_mut_slice()[0..4].copy_from_slice(&[0x11, 0x22, 0x33, 0x44]);
        assert_eq!(&region.as_slice()[0..4], &[0x11, 0x22, 0x33, 0x44]);
    }

    #[test]
    fn maps_unaligned_logical_region() {
        let fd = create_memfd("pipewire-native-node-unaligned", 4096).unwrap();
        let mut region = MappedRegion::map_shared(fd.as_fd(), 3, 5, true).unwrap();

        region.as_mut_slice().copy_from_slice(&[1, 2, 3, 4, 5]);

        let full = MappedRegion::map_shared(fd.as_fd(), 0, 8, false).unwrap();
        assert_eq!(region.len(), 5);
        assert_eq!(&full.as_slice()[0..8], &[0, 0, 0, 1, 2, 3, 4, 5]);
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
        use std::os::fd::AsRawFd;

        let fd = create_memfd("pipewire-native-node-sealed", 4096).unwrap();
        let result = unsafe { libc::ftruncate(fd.as_raw_fd(), 0) };

        assert_eq!(result, -1);
        assert_eq!(
            std::io::Error::last_os_error().raw_os_error(),
            Some(libc::EPERM)
        );
    }
}
