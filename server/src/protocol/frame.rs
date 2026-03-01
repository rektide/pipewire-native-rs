// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: Copyright (c) 2026 Asymptotic Inc.

use std::{
    io::{self, Read, Write},
    os::{
        fd::{AsRawFd, BorrowedFd, FromRawFd, OwnedFd, RawFd},
        unix::net::UnixStream,
    },
};

/// Number of bytes in the native protocol message header.
pub const HEADER_LEN: usize = 16;

/// Maximum payload size that fits in the protocol's 24-bit size field.
pub const MAX_PAYLOAD_SIZE: usize = (1 << 24) - 1;

const MAX_CONTROL_FDS: usize = 16;

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
        let object_id = u32::from_ne_bytes(bytes[0..4].try_into().expect("slice length is 4"));
        let word = u32::from_ne_bytes(bytes[4..8].try_into().expect("slice length is 4"));
        let opcode = (word >> 24) as u8;
        let payload_size = word & ((1 << 24) - 1);
        let seq = u32::from_ne_bytes(bytes[8..12].try_into().expect("slice length is 4"));
        let n_fds = u32::from_ne_bytes(bytes[12..16].try_into().expect("slice length is 4"));

        Self {
            object_id,
            opcode,
            payload_size,
            seq,
            n_fds,
        }
    }

    /// Encodes this header to native protocol wire bytes.
    pub fn encode(self) -> io::Result<[u8; HEADER_LEN]> {
        if self.payload_size as usize > MAX_PAYLOAD_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "payload size {} exceeds max {}",
                    self.payload_size, MAX_PAYLOAD_SIZE
                ),
            ));
        }

        let mut bytes = [0u8; HEADER_LEN];
        bytes[0..4].copy_from_slice(&self.object_id.to_ne_bytes());

        let word = ((self.opcode as u32) << 24) | (self.payload_size & ((1 << 24) - 1));
        bytes[4..8].copy_from_slice(&word.to_ne_bytes());

        bytes[8..12].copy_from_slice(&self.seq.to_ne_bytes());
        bytes[12..16].copy_from_slice(&self.n_fds.to_ne_bytes());
        Ok(bytes)
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
    let mut header_bytes = [0u8; HEADER_LEN];

    let mut control = vec![
        0u8;
        unsafe {
            libc::CMSG_SPACE((MAX_CONTROL_FDS * std::mem::size_of::<RawFd>()) as u32) as usize
        }
    ];

    let mut iov = libc::iovec {
        iov_base: header_bytes.as_mut_ptr().cast::<libc::c_void>(),
        iov_len: HEADER_LEN,
    };
    let mut msg = libc::msghdr {
        msg_name: std::ptr::null_mut(),
        msg_namelen: 0,
        msg_iov: &mut iov,
        msg_iovlen: 1,
        msg_control: control.as_mut_ptr().cast::<libc::c_void>(),
        msg_controllen: control.len(),
        msg_flags: 0,
    };

    let read = loop {
        let read = unsafe { libc::recvmsg(stream.as_raw_fd(), &mut msg, libc::MSG_CMSG_CLOEXEC) };
        if read < 0 {
            let err = io::Error::last_os_error();
            if err.kind() == io::ErrorKind::Interrupted {
                continue;
            }

            return Err(err);
        }

        break read;
    };

    if read == 0 {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "socket closed while reading native header",
        ));
    }

    if read as usize != HEADER_LEN {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            format!(
                "short read for native header: expected {} got {}",
                HEADER_LEN, read
            ),
        ));
    }

    if msg.msg_flags & libc::MSG_CTRUNC == libc::MSG_CTRUNC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "control data truncated while receiving packet",
        ));
    }

    let mut payload = {
        let header = NativeHeader::decode(header_bytes);
        vec![0u8; header.payload_size as usize]
    };
    stream.read_exact(&mut payload)?;

    let fds = parse_received_fds(&msg)?;
    let header = NativeHeader::decode(header_bytes);

    if header.n_fds as usize != fds.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "header says {} fds but {} were received",
                header.n_fds,
                fds.len(),
            ),
        ));
    }

    Ok((NativePacket { header, payload }, fds))
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
    if packet.payload.len() > MAX_PAYLOAD_SIZE {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "payload size {} exceeds max {}",
                packet.payload.len(),
                MAX_PAYLOAD_SIZE
            ),
        ));
    }

    if fds.len() > MAX_CONTROL_FDS {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "too many fds {}; max supported is {}",
                fds.len(),
                MAX_CONTROL_FDS
            ),
        ));
    }

    let header = NativeHeader {
        payload_size: packet.payload.len() as u32,
        n_fds: fds.len() as u32,
        ..packet.header
    };
    let header_bytes = header.encode()?;

    if fds.is_empty() {
        stream.write_all(&header_bytes)?;
        stream.write_all(&packet.payload)?;
        stream.flush()?;
        return Ok(());
    }

    let mut iovs = [
        libc::iovec {
            iov_base: header_bytes.as_ptr().cast::<libc::c_void>() as *mut libc::c_void,
            iov_len: header_bytes.len(),
        },
        libc::iovec {
            iov_base: packet.payload.as_ptr().cast::<libc::c_void>() as *mut libc::c_void,
            iov_len: packet.payload.len(),
        },
    ];

    let mut control =
        vec![
            0u8;
            unsafe { libc::CMSG_SPACE((fds.len() * std::mem::size_of::<RawFd>()) as u32) as usize }
        ];
    let mut msg = libc::msghdr {
        msg_name: std::ptr::null_mut(),
        msg_namelen: 0,
        msg_iov: iovs.as_mut_ptr(),
        msg_iovlen: iovs.len(),
        msg_control: control.as_mut_ptr().cast::<libc::c_void>(),
        msg_controllen: control.len(),
        msg_flags: 0,
    };

    let cmsg = unsafe { libc::CMSG_FIRSTHDR((&msg) as *const libc::msghdr) };
    if cmsg.is_null() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "failed to build control message header",
        ));
    }

    unsafe {
        (*cmsg).cmsg_level = libc::SOL_SOCKET;
        (*cmsg).cmsg_type = libc::SCM_RIGHTS;
        (*cmsg).cmsg_len = libc::CMSG_LEN((fds.len() * std::mem::size_of::<RawFd>()) as u32) as _;

        let data = libc::CMSG_DATA(cmsg).cast::<RawFd>();
        for (idx, fd) in fds.iter().enumerate() {
            *data.add(idx) = fd.as_raw_fd();
        }

        msg.msg_controllen = (*cmsg).cmsg_len;
    }

    let written = unsafe { libc::sendmsg(stream.as_raw_fd(), &msg, 0) };
    if written < 0 {
        return Err(io::Error::last_os_error());
    }

    let expected = header_bytes.len() + packet.payload.len();
    if written as usize != expected {
        return Err(io::Error::new(
            io::ErrorKind::WriteZero,
            format!(
                "partial sendmsg write: expected {} bytes got {}",
                expected, written
            ),
        ));
    }

    Ok(())
}

fn parse_received_fds(msg: &libc::msghdr) -> io::Result<Vec<OwnedFd>> {
    let mut out = Vec::new();

    let msg_ptr = msg as *const libc::msghdr;
    let mut cmsg = unsafe { libc::CMSG_FIRSTHDR(msg_ptr) };
    while !cmsg.is_null() {
        let level = unsafe { (*cmsg).cmsg_level };
        let type_ = unsafe { (*cmsg).cmsg_type };

        if level == libc::SOL_SOCKET && type_ == libc::SCM_RIGHTS {
            let data = unsafe { libc::CMSG_DATA(cmsg) }.cast::<RawFd>();
            let data_len =
                unsafe { (*cmsg).cmsg_len } as usize - unsafe { libc::CMSG_LEN(0) } as usize;
            let n_fds = data_len / std::mem::size_of::<RawFd>();

            for idx in 0..n_fds {
                let fd = unsafe { *data.add(idx) };
                if fd < 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("received invalid fd {fd}"),
                    ));
                }
                out.push(unsafe { OwnedFd::from_raw_fd(fd) });
            }
        }

        cmsg = unsafe { libc::CMSG_NXTHDR(msg_ptr, cmsg) };
    }

    Ok(out)
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
