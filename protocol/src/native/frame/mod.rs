mod header;
mod recv;
mod send;

use std::fmt;
use std::io;
use std::os::fd::{BorrowedFd, OwnedFd};

pub use header::{Header, HEADER_LEN, WIRE_MAX_PAYLOAD};
pub use recv::{FrameReceiver, ReceiveOutcome};
pub use send::{FlushOutcome, FrameSender, OutboundFrame};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FrameLimits {
    pub max_payload: usize,
    pub max_frame_fds: usize,
    pub max_pending_fds: usize,
    pub recv_chunk_bytes: usize,
    pub recv_control_fds: usize,
    pub max_queued_bytes: usize,
    pub max_queued_fds: usize,
}

impl Default for FrameLimits {
    fn default() -> Self {
        Self {
            max_payload: WIRE_MAX_PAYLOAD,
            max_frame_fds: 1024,
            max_pending_fds: 1024,
            recv_chunk_bytes: 32 * 1024,
            recv_control_fds: 64,
            max_queued_bytes: WIRE_MAX_PAYLOAD * 4,
            max_queued_fds: 1024,
        }
    }
}

#[derive(Debug)]
pub enum FrameError {
    Io(io::Error),
    PayloadTooLarge {
        declared: usize,
        limit: usize,
    },
    TooManyFrameFds {
        declared: usize,
        limit: usize,
    },
    TooManyPendingFds {
        received: usize,
        limit: usize,
    },
    SendQueueFull {
        queued: usize,
        additional: usize,
        limit: usize,
    },
    SendFdQueueFull {
        queued: usize,
        additional: usize,
        limit: usize,
    },
    TruncatedControl,
    MalformedControl,
    MissingFds {
        declared: usize,
        available: usize,
    },
    UnexpectedFdsAtEof {
        count: usize,
    },
    TruncatedFrame {
        buffered: usize,
        needed: usize,
    },
    InvalidFdIndex {
        index: u32,
        count: usize,
    },
    FdAlreadyTaken {
        index: u32,
    },
    WriteZero,
    Poisoned,
}

impl fmt::Display for FrameError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

impl std::error::Error for FrameError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            _ => None,
        }
    }
}

impl From<io::Error> for FrameError {
    fn from(value: io::Error) -> Self {
        Self::Io(value)
    }
}

#[derive(Debug)]
pub struct ReceivedFrame {
    header: Header,
    payload: Vec<u8>,
    fds: FrameFds,
}

impl ReceivedFrame {
    #[must_use]
    pub fn header(&self) -> Header {
        self.header
    }
    #[must_use]
    pub fn payload(&self) -> &[u8] {
        &self.payload
    }
    #[must_use]
    pub fn fds(&self) -> &FrameFds {
        &self.fds
    }
    pub fn fds_mut(&mut self) -> &mut FrameFds {
        &mut self.fds
    }
    #[must_use]
    pub fn into_parts(self) -> (Header, Vec<u8>, FrameFds) {
        (self.header, self.payload, self.fds)
    }
}

#[derive(Debug)]
pub struct FrameFds(Vec<Option<OwnedFd>>);

impl FrameFds {
    #[must_use]
    pub fn len(&self) -> usize {
        self.0.len()
    }
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
    pub fn get(&self, index: u32) -> Result<BorrowedFd<'_>, FrameError> {
        let count = self.len();
        self.0
            .get(index as usize)
            .ok_or(FrameError::InvalidFdIndex { index, count })?
            .as_ref()
            .map(OwnedFd::as_fd)
            .ok_or(FrameError::FdAlreadyTaken { index })
    }
    pub fn take(&mut self, index: u32) -> Result<OwnedFd, FrameError> {
        let count = self.len();
        self.0
            .get_mut(index as usize)
            .ok_or(FrameError::InvalidFdIndex { index, count })?
            .take()
            .ok_or(FrameError::FdAlreadyTaken { index })
    }
}

use std::os::fd::AsFd;

pub struct FrameTransport {
    pub receiver: FrameReceiver,
    pub sender: FrameSender,
}

impl FrameTransport {
    #[must_use]
    pub fn new(limits: FrameLimits) -> Self {
        Self {
            receiver: FrameReceiver::new(limits),
            sender: FrameSender::new(limits),
        }
    }
}
