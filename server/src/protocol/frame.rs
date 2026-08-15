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

/// Reads one packet from a Unix stream.
pub fn read_packet(stream: &mut UnixStream) -> io::Result<NativePacket> {
    let (packet, fds) = read_packet_with_fds(stream)?;
    if !fds.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("expected packet without fds but received {} fds", fds.len()),
        ));
    }

    Ok(packet)
}

/// Reads one packet and any SCM_RIGHTS file descriptors attached to it.
pub fn read_packet_with_fds(stream: &mut UnixStream) -> io::Result<(NativePacket, Vec<OwnedFd>)> {
    let mut receiver = FrameReceiver::new(FrameLimits::default());
    loop {
        match receiver.receive(stream.as_fd()).map_err(frame_error)? {
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

    use super::{read_packet_with_fds, write_packet_with_fds, NativeHeader, NativePacket};

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
        let (decoded, fds) = read_packet_with_fds(&mut rx).unwrap();

        assert_eq!(decoded.header.object_id, 7);
        assert_eq!(decoded.header.opcode, 8);
        assert_eq!(decoded.payload, vec![1, 2, 3, 4]);
        assert_eq!(fds.len(), 1);

        let mut stat = unsafe { std::mem::zeroed::<libc::stat>() };
        let res = unsafe { libc::fstat(fds[0].as_raw_fd(), &mut stat) };
        assert_eq!(res, 0);
        assert_eq!(stat.st_size, 4096);
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
