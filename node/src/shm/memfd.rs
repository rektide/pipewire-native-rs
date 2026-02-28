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
    let name = CString::new(name).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "memfd name cannot contain NUL bytes",
        )
    })?;

    let fd = unsafe {
        libc::memfd_create(
            name.as_ptr(),
            (libc::MFD_CLOEXEC | libc::MFD_ALLOW_SEALING) as u32,
        )
    };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }

    let fd = unsafe { OwnedFd::from_raw_fd(fd) };

    let res = unsafe { libc::ftruncate(fd.as_raw_fd(), size as libc::off_t) };
    if res < 0 {
        return Err(io::Error::last_os_error());
    }

    Ok(fd)
}

/// Shared memory mapping backed by an imported memfd.
#[derive(Debug)]
pub struct MappedRegion {
    ptr: NonNull<u8>,
    len: usize,
}

unsafe impl Send for MappedRegion {}
unsafe impl Sync for MappedRegion {}

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

        if offset > libc::off_t::MAX as usize {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "mapping offset is larger than supported off_t",
            ));
        }

        let prot = if writable {
            libc::PROT_READ | libc::PROT_WRITE
        } else {
            libc::PROT_READ
        };
        let ptr = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                len,
                prot,
                libc::MAP_SHARED,
                fd.as_raw_fd(),
                offset as libc::off_t,
            )
        };

        if ptr == libc::MAP_FAILED {
            return Err(io::Error::last_os_error());
        }

        let ptr = NonNull::new(ptr.cast::<u8>()).expect("mmap returned null pointer");
        Ok(Self { ptr, len })
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
        let _ = unsafe { libc::munmap(self.ptr.as_ptr().cast(), self.len) };
    }
}

#[cfg(test)]
mod tests {
    use std::os::fd::AsFd;

    use super::{create_memfd, MappedRegion};

    #[test]
    fn map_and_mutate_memfd_region() {
        let fd = create_memfd("pipewire-native-node-test", 4096).unwrap();
        let mut region = MappedRegion::map_shared(fd.as_fd(), 0, 4096, true).unwrap();

        region.as_mut_slice()[0..4].copy_from_slice(&[0x11, 0x22, 0x33, 0x44]);
        assert_eq!(&region.as_slice()[0..4], &[0x11, 0x22, 0x33, 0x44]);
    }
}
