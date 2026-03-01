// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: Copyright (c) 2026 Asymptotic Inc.

use std::{
    io::{self, Read, Write},
    os::unix::net::UnixStream,
};

/// Number of bytes in the native protocol message header.
pub const HEADER_LEN: usize = 16;

/// Maximum payload size that fits in the protocol's 24-bit size field.
pub const MAX_PAYLOAD_SIZE: usize = (1 << 24) - 1;

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
    let mut header_bytes = [0u8; HEADER_LEN];
    stream.read_exact(&mut header_bytes)?;
    let header = NativeHeader::decode(header_bytes);

    let mut payload = vec![0u8; header.payload_size as usize];
    stream.read_exact(&mut payload)?;

    Ok(NativePacket { header, payload })
}

/// Writes one packet to a Unix stream.
pub fn write_packet(stream: &mut UnixStream, packet: &NativePacket) -> io::Result<()> {
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

    let header = NativeHeader {
        payload_size: packet.payload.len() as u32,
        ..packet.header
    };
    let header_bytes = header.encode()?;

    stream.write_all(&header_bytes)?;
    stream.write_all(&packet.payload)?;
    stream.flush()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::NativeHeader;

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
}
