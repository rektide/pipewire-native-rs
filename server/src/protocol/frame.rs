// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: Copyright (c) 2026 Asymptotic Inc.

use std::{
    io,
    os::{
        fd::{AsFd, BorrowedFd, OwnedFd},
        unix::net::UnixStream,
    },
};

use pipewire_native_protocol::native::frame::{
    FlushOutcome, FrameError, FrameLimits, FrameReceiver, FrameSender, Header, OutboundFrame,
    ReceiveOutcome,
};

/// Number of bytes in the native protocol message header.
pub use pipewire_native_protocol::native::frame::HEADER_LEN;

/// Native protocol message header.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NativeHeader {
    /// Proxy/object id for this message.
    pub object_id: u32,
    /// Opcode within the object interface.
    pub opcode: u8,
    /// Payload size in bytes (24-bit field on wire).
    pub payload_size: u32,
    /// Sequence number.
    pub seq: u32,
    /// Number of attached file descriptors.
    pub n_fds: u32,
}

impl NativeHeader {
    /// Decodes a native protocol header from 16 bytes.
    pub fn decode(bytes: [u8; HEADER_LEN]) -> Self {
        Header::decode(bytes).into()
    }

    /// Encodes this header to native protocol wire bytes.
    pub fn encode(self) -> io::Result<[u8; HEADER_LEN]> {
        Header::from(self).encode().map_err(frame_error)
    }
}

impl From<Header> for NativeHeader {
    fn from(header: Header) -> Self {
        Self {
            object_id: header.object_id,
            opcode: header.opcode,
            payload_size: header.payload_len,
            seq: header.seq,
            n_fds: header.n_fds,
        }
    }
}

impl From<NativeHeader> for Header {
    fn from(header: NativeHeader) -> Self {
        Self {
            object_id: header.object_id,
            opcode: header.opcode,
            payload_len: header.payload_size,
            seq: header.seq,
            n_fds: header.n_fds,
        }
    }
}

/// One inbound or outbound native protocol packet.
#[derive(Debug, Eq, PartialEq)]
pub struct NativePacket {
    /// Message header.
    pub header: NativeHeader,
    /// Message payload bytes.
    pub payload: Vec<u8>,
}

/// Stateful reader for native protocol packets on one Unix stream.
///
/// A reader must be retained for the lifetime of its stream so bytes and file
/// descriptors buffered beyond the current packet remain available to later
/// reads.
pub struct NativePacketReader {
    receiver: FrameReceiver,
}

impl NativePacketReader {
    /// Creates a reader using the default native frame limits.
    pub fn new() -> Self {
        Self {
            receiver: FrameReceiver::new(FrameLimits::default()),
        }
    }

    /// Reads one packet, rejecting packets with attached file descriptors.
    pub fn read_packet(&mut self, stream: &mut UnixStream) -> io::Result<NativePacket> {
        let (packet, fds) = self.read_packet_with_fds(stream)?;
        if !fds.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("expected packet without fds but received {} fds", fds.len()),
            ));
        }

        Ok(packet)
    }

    /// Reads one packet and any SCM_RIGHTS file descriptors attached to it.
    pub fn read_packet_with_fds(
        &mut self,
        stream: &mut UnixStream,
    ) -> io::Result<(NativePacket, Vec<OwnedFd>)> {
        loop {
            match self.receiver.receive(stream.as_fd()).map_err(frame_error)? {
                ReceiveOutcome::Frame(frame) => {
                    let (header, payload, mut frame_fds) = frame.into_parts();
                    let fds = (0..frame_fds.len())
                        .map(|index| frame_fds.take(index as u32).map_err(frame_error))
                        .collect::<io::Result<Vec<_>>>()?;
                    return Ok((
                        NativePacket {
                            header: header.into(),
                            payload,
                        },
                        fds,
                    ));
                }
                ReceiveOutcome::WouldBlock => wait(stream.as_fd(), libc::POLLIN)?,
                ReceiveOutcome::Closed => {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "socket closed while reading native frame",
                    ));
                }
            }
        }
    }
}

impl Default for NativePacketReader {
    fn default() -> Self {
        Self::new()
    }
}

/// Writes one packet to a Unix stream.
pub fn write_packet(stream: &mut UnixStream, packet: &NativePacket) -> io::Result<()> {
    write_packet_with_fds(stream, packet, &[])
}

/// Writes one packet to a Unix stream with optional SCM_RIGHTS descriptors.
pub fn write_packet_with_fds(
    stream: &mut UnixStream,
    packet: &NativePacket,
    fds: &[BorrowedFd<'_>],
) -> io::Result<()> {
    let limits = FrameLimits::default();
    let frame = OutboundFrame::duplicate_fds(
        packet.header.object_id,
        packet.header.opcode,
        packet.header.seq,
        packet.payload.clone(),
        fds,
        limits,
    )
    .map_err(frame_error)?;
    let mut sender = FrameSender::new(limits);
    sender.enqueue(frame).map_err(frame_error)?;
    loop {
        match sender.flush(stream.as_fd()).map_err(frame_error)? {
            FlushOutcome::Drained => return Ok(()),
            FlushOutcome::WouldBlock => wait(stream.as_fd(), libc::POLLOUT)?,
        }
    }
}

fn wait(fd: BorrowedFd<'_>, events: libc::c_short) -> io::Result<()> {
    let mut poll_fd = libc::pollfd {
        fd: std::os::fd::AsRawFd::as_raw_fd(&fd),
        events,
        revents: 0,
    };
    loop {
        let result = unsafe { libc::poll(&mut poll_fd, 1, -1) };
        if result > 0 {
            return Ok(());
        }
        let error = io::Error::last_os_error();
        if error.kind() != io::ErrorKind::Interrupted {
            return Err(error);
        }
    }
}

fn frame_error(error: FrameError) -> io::Error {
    let kind = match &error {
        FrameError::Io(source) => source.kind(),
        FrameError::PayloadTooLarge { .. }
        | FrameError::TooManyFrameFds { .. }
        | FrameError::SendQueueFull { .. }
        | FrameError::SendFdQueueFull { .. } => io::ErrorKind::InvalidInput,
        FrameError::WriteZero => io::ErrorKind::WriteZero,
        _ => io::ErrorKind::InvalidData,
    };
    io::Error::new(kind, error)
}

#[cfg(test)]
mod tests {
    use std::os::fd::{AsFd, AsRawFd, FromRawFd};

    use super::{write_packet_with_fds, NativeHeader, NativePacket, NativePacketReader};

    #[test]
    fn roundtrip_header_encoding() {
        let header = NativeHeader {
            object_id: 42,
            opcode: 7,
            payload_size: 1234,
            seq: 9001,
            n_fds: 2,
        };

        let bytes = header.encode().unwrap();
        let decoded = NativeHeader::decode(bytes);
        assert_eq!(decoded, header);
    }

    #[test]
    fn sends_and_receives_one_fd() {
        let (mut tx, mut rx) = std::os::unix::net::UnixStream::pair().unwrap();

        let fd = create_memfd(4096).unwrap();

        let packet = NativePacket {
            header: NativeHeader {
                object_id: 7,
                opcode: 8,
                payload_size: 4,
                seq: 9,
                n_fds: 0,
            },
            payload: vec![1, 2, 3, 4],
        };

        write_packet_with_fds(&mut tx, &packet, &[fd.as_fd()]).unwrap();
        let (decoded, fds) = NativePacketReader::new()
            .read_packet_with_fds(&mut rx)
            .unwrap();

        assert_eq!(decoded.header.object_id, 7);
        assert_eq!(decoded.header.opcode, 8);
        assert_eq!(decoded.payload, vec![1, 2, 3, 4]);
        assert_eq!(fds.len(), 1);

        let mut stat = unsafe { std::mem::zeroed::<libc::stat>() };
        let res = unsafe { libc::fstat(fds[0].as_raw_fd(), &mut stat) };
        assert_eq!(res, 0);
        assert_eq!(stat.st_size, 4096);
    }

    #[test]
    fn retains_coalesced_second_frame_and_its_fd() {
        let (mut tx, mut rx) = std::os::unix::net::UnixStream::pair().unwrap();
        let first = packet(1, vec![1, 2, 3]);
        let mut second = packet(2, vec![4, 5, 6]);
        second.header.n_fds = 1;
        let fd = create_memfd(8192).unwrap();
        let mut wire = first.header.encode().unwrap().to_vec();
        wire.extend_from_slice(&first.payload);
        wire.extend_from_slice(&second.header.encode().unwrap());
        wire.extend_from_slice(&second.payload);

        send_with_fd(&mut tx, &wire, &fd);

        let mut reader = NativePacketReader::new();
        let (decoded_first, first_fds) = reader.read_packet_with_fds(&mut rx).unwrap();
        let (decoded_second, second_fds) = reader.read_packet_with_fds(&mut rx).unwrap();

        assert_eq!(decoded_first, first);
        assert!(first_fds.is_empty());
        assert_eq!(decoded_second, second);
        assert_eq!(second_fds.len(), 1);

        let received_raw_fd = second_fds[0].as_raw_fd();
        let mut stat = unsafe { std::mem::zeroed::<libc::stat>() };
        assert_eq!(unsafe { libc::fstat(received_raw_fd, &mut stat) }, 0);
        assert_eq!(stat.st_size, 8192);
        drop(second_fds);
        assert_eq!(unsafe { libc::fcntl(received_raw_fd, libc::F_GETFD) }, -1);
        assert_eq!(
            std::io::Error::last_os_error().raw_os_error(),
            Some(libc::EBADF)
        );
    }

    fn packet(seq: u32, payload: Vec<u8>) -> NativePacket {
        NativePacket {
            header: NativeHeader {
                object_id: 7,
                opcode: 8,
                payload_size: payload.len() as u32,
                seq,
                n_fds: 0,
            },
            payload,
        }
    }

    fn send_with_fd(
        stream: &mut std::os::unix::net::UnixStream,
        bytes: &[u8],
        fd: &std::os::fd::OwnedFd,
    ) {
        let mut iov = libc::iovec {
            iov_base: bytes.as_ptr().cast_mut().cast(),
            iov_len: bytes.len(),
        };
        let mut control = [0_usize; 4];
        let header_len = std::mem::size_of::<libc::cmsghdr>();
        unsafe {
            std::ptr::write(
                control.as_mut_ptr().cast::<libc::cmsghdr>(),
                libc::cmsghdr {
                    cmsg_len: header_len + std::mem::size_of::<libc::c_int>(),
                    cmsg_level: libc::SOL_SOCKET,
                    cmsg_type: libc::SCM_RIGHTS,
                },
            );
            std::ptr::write_unaligned(
                control
                    .as_mut_ptr()
                    .cast::<u8>()
                    .add(header_len)
                    .cast::<libc::c_int>(),
                fd.as_raw_fd(),
            );
        }
        let mut message: libc::msghdr = unsafe { std::mem::zeroed() };
        message.msg_iov = &mut iov;
        message.msg_iovlen = 1;
        message.msg_control = control.as_mut_ptr().cast();
        message.msg_controllen =
            (header_len + std::mem::size_of::<libc::c_int>() + std::mem::size_of::<usize>() - 1)
                & !(std::mem::size_of::<usize>() - 1);

        assert_eq!(
            unsafe { libc::sendmsg(stream.as_raw_fd(), &message, libc::MSG_NOSIGNAL) },
            bytes.len() as isize
        );
    }

    fn create_memfd(size: usize) -> std::io::Result<std::os::fd::OwnedFd> {
        let name = std::ffi::CString::new("pipewire-native-server-frame-test").unwrap();
        let fd = unsafe { libc::memfd_create(name.as_ptr(), libc::MFD_CLOEXEC) };
        if fd < 0 {
            return Err(std::io::Error::last_os_error());
        }

        let fd = unsafe { std::os::fd::OwnedFd::from_raw_fd(fd) };
        let res = unsafe { libc::ftruncate(fd.as_raw_fd(), size as libc::off_t) };
        if res < 0 {
            return Err(std::io::Error::last_os_error());
        }

        Ok(fd)
    }
}
