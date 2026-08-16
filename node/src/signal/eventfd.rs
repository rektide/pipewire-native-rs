// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: Copyright (c) 2026 Asymptotic Inc.

use std::{
    io,
    os::fd::{AsFd, AsRawFd, BorrowedFd, FromRawFd, OwnedFd, RawFd},
};

/// Runtime-independent owner of a non-blocking `eventfd`.
#[derive(Debug)]
pub struct EventFd {
    fd: OwnedFd,
}

impl EventFd {
    /// Creates a fresh non-blocking eventfd.
    pub fn new() -> io::Result<Self> {
        let fd = unsafe { libc::eventfd(0, libc::EFD_CLOEXEC | libc::EFD_NONBLOCK) };
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }

        let fd = unsafe { OwnedFd::from_raw_fd(fd) };
        Self::from_owned_fd(fd)
    }

    /// Wraps an existing eventfd.
    pub fn from_owned_fd(fd: OwnedFd) -> io::Result<Self> {
        set_nonblocking(fd.as_raw_fd())?;
        Ok(Self { fd })
    }

    /// Duplicates this handle while retaining the same kernel event counter.
    pub fn try_clone(&self) -> io::Result<Self> {
        let fd = self.fd.try_clone()?;
        Self::from_owned_fd(fd)
    }

    /// Returns the underlying file descriptor.
    pub fn raw_fd(&self) -> RawFd {
        self.fd.as_raw_fd()
    }

    /// Drains one counter value without waiting.
    pub fn drain(&self) -> io::Result<u64> {
        read_eventfd(self.raw_fd())
    }

    /// Writes a value to the eventfd counter.
    pub fn signal(&self, value: u64) -> io::Result<()> {
        write_eventfd(self.raw_fd(), value)
    }
}

impl AsFd for EventFd {
    fn as_fd(&self) -> BorrowedFd<'_> {
        self.fd.as_fd()
    }
}

impl AsRawFd for EventFd {
    fn as_raw_fd(&self) -> RawFd {
        self.fd.as_raw_fd()
    }
}

fn set_nonblocking(fd: RawFd) -> io::Result<()> {
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFL) };
    if flags < 0 {
        return Err(io::Error::last_os_error());
    }

    if flags & libc::O_NONBLOCK == libc::O_NONBLOCK {
        return Ok(());
    }

    let res = unsafe { libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK) };
    if res < 0 {
        return Err(io::Error::last_os_error());
    }

    Ok(())
}

fn read_eventfd(fd: RawFd) -> io::Result<u64> {
    let mut value = 0u64;
    let read = unsafe {
        libc::read(
            fd,
            (&mut value as *mut u64).cast(),
            std::mem::size_of::<u64>(),
        )
    };

    if read < 0 {
        let err = io::Error::last_os_error();
        if err.raw_os_error() == Some(libc::EAGAIN) || err.raw_os_error() == Some(libc::EWOULDBLOCK)
        {
            return Err(io::Error::from(io::ErrorKind::WouldBlock));
        }
        return Err(err);
    }

    if read as usize != std::mem::size_of::<u64>() {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "short read from eventfd",
        ));
    }

    Ok(value)
}

fn write_eventfd(fd: RawFd, value: u64) -> io::Result<()> {
    let written = unsafe {
        libc::write(
            fd,
            (&value as *const u64).cast(),
            std::mem::size_of::<u64>(),
        )
    };

    if written < 0 {
        return Err(io::Error::last_os_error());
    }

    if written as usize != std::mem::size_of::<u64>() {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "short write to eventfd",
        ));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::EventFd;

    #[test]
    fn signals_and_drains_counter() {
        let event = EventFd::new().unwrap();
        assert_eq!(
            event.drain().unwrap_err().kind(),
            std::io::ErrorKind::WouldBlock
        );
        event.signal(7).unwrap();
        assert_eq!(event.drain().unwrap(), 7);
    }
}
