use super::FrameError;

pub const HEADER_LEN: usize = 16;
pub const WIRE_MAX_PAYLOAD: usize = 0x00ff_ffff;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Header {
    pub object_id: u32,
    pub opcode: u8,
    pub payload_len: u32,
    pub seq: u32,
    pub n_fds: u32,
}

impl Header {
    #[must_use]
    pub fn decode(bytes: [u8; HEADER_LEN]) -> Self {
        let object_id = u32::from_ne_bytes(bytes[0..4].try_into().expect("fixed header slice"));
        let opcode_size = u32::from_ne_bytes(bytes[4..8].try_into().expect("fixed header slice"));
        let seq = u32::from_ne_bytes(bytes[8..12].try_into().expect("fixed header slice"));
        let n_fds = u32::from_ne_bytes(bytes[12..16].try_into().expect("fixed header slice"));
        Self {
            object_id,
            opcode: (opcode_size >> 24) as u8,
            payload_len: opcode_size & WIRE_MAX_PAYLOAD as u32,
            seq,
            n_fds,
        }
    }

    pub fn encode(self) -> Result<[u8; HEADER_LEN], FrameError> {
        let payload_len = self.payload_len as usize;
        if payload_len > WIRE_MAX_PAYLOAD {
            return Err(FrameError::PayloadTooLarge {
                declared: payload_len,
                limit: WIRE_MAX_PAYLOAD,
            });
        }
        let mut bytes = [0; HEADER_LEN];
        bytes[0..4].copy_from_slice(&self.object_id.to_ne_bytes());
        bytes[4..8]
            .copy_from_slice(&((u32::from(self.opcode) << 24) | self.payload_len).to_ne_bytes());
        bytes[8..12].copy_from_slice(&self.seq.to_ne_bytes());
        bytes[12..16].copy_from_slice(&self.n_fds.to_ne_bytes());
        Ok(bytes)
    }
}
