use super::{FrameError, FrameLimits, Header, HEADER_LEN, WIRE_MAX_PAYLOAD};
use std::collections::VecDeque;
use std::io;
use std::mem::{align_of, size_of};
use std::os::fd::{AsRawFd, BorrowedFd, OwnedFd, RawFd};

#[derive(Debug, Eq, PartialEq)]
pub enum FlushOutcome {
    Drained,
    WouldBlock,
}

#[derive(Debug)]
pub struct OutboundFrame {
    header: Header,
    bytes: Vec<u8>,
    fds: Vec<OwnedFd>,
}

impl OutboundFrame {
    pub fn new(
        object_id: u32,
        opcode: u8,
        seq: u32,
        payload: Vec<u8>,
        fds: Vec<OwnedFd>,
        limits: FrameLimits,
    ) -> Result<Self, FrameError> {
        if payload.len() > limits.max_payload.min(WIRE_MAX_PAYLOAD) {
            return Err(FrameError::PayloadTooLarge {
                declared: payload.len(),
                limit: limits.max_payload.min(WIRE_MAX_PAYLOAD),
            });
        }
        if fds.len() > limits.max_frame_fds {
            return Err(FrameError::TooManyFrameFds {
                declared: fds.len(),
                limit: limits.max_frame_fds,
            });
        }
        let header = Header {
            object_id,
            opcode,
            payload_len: payload.len() as u32,
            seq,
            n_fds: fds.len() as u32,
        };
        let mut bytes = Vec::with_capacity(HEADER_LEN + payload.len());
        bytes.extend(header.encode()?);
        bytes.extend(payload);
        Ok(Self { header, bytes, fds })
    }
    pub fn duplicate_fds(
        object_id: u32,
        opcode: u8,
        seq: u32,
        payload: Vec<u8>,
        fds: &[BorrowedFd<'_>],
        limits: FrameLimits,
    ) -> Result<Self, FrameError> {
        let duplicates = fds
            .iter()
            .map(BorrowedFd::try_clone_to_owned)
            .collect::<Result<Vec<_>, _>>()?;
        Self::new(object_id, opcode, seq, payload, duplicates, limits)
    }
    #[must_use]
    pub fn header(&self) -> Header {
        self.header
    }
}

struct SendFrame {
    bytes: Vec<u8>,
    offset: usize,
    fds: Vec<OwnedFd>,
    ancillary_sent: bool,
}
pub struct FrameSender {
    limits: FrameLimits,
    queue: VecDeque<SendFrame>,
    queued_bytes: usize,
    queued_fds: usize,
    failed: bool,
}

impl FrameSender {
    #[must_use]
    pub fn new(limits: FrameLimits) -> Self {
        Self {
            limits,
            queue: VecDeque::new(),
            queued_bytes: 0,
            queued_fds: 0,
            failed: false,
        }
    }
    pub fn enqueue(&mut self, frame: OutboundFrame) -> Result<(), FrameError> {
        if self.failed {
            return Err(FrameError::Poisoned);
        }
        if self.queued_bytes.saturating_add(frame.bytes.len()) > self.limits.max_queued_bytes {
            return Err(FrameError::SendQueueFull {
                queued: self.queued_bytes,
                additional: frame.bytes.len(),
                limit: self.limits.max_queued_bytes,
            });
        }
        if self.queued_fds.saturating_add(frame.fds.len()) > self.limits.max_queued_fds {
            return Err(FrameError::SendFdQueueFull {
                queued: self.queued_fds,
                additional: frame.fds.len(),
                limit: self.limits.max_queued_fds,
            });
        }
        self.queued_bytes += frame.bytes.len();
        self.queued_fds += frame.fds.len();
        self.queue.push_back(SendFrame {
            bytes: frame.bytes,
            offset: 0,
            fds: frame.fds,
            ancillary_sent: false,
        });
        Ok(())
    }
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }
    #[must_use]
    pub fn queued_bytes(&self) -> usize {
        self.queued_bytes
    }
    #[must_use]
    pub fn queued_fds(&self) -> usize {
        self.queued_fds
    }
    pub fn clear(&mut self) {
        self.queue.clear();
        self.queued_bytes = 0;
        self.queued_fds = 0;
    }
    pub fn flush(&mut self, socket: BorrowedFd<'_>) -> Result<FlushOutcome, FrameError> {
        if self.failed {
            return Err(FrameError::Poisoned);
        }
        while let Some(frame) = self.queue.front_mut() {
            let include_fds = !frame.ancillary_sent && !frame.fds.is_empty();
            let sent = loop {
                match send_once(
                    socket,
                    &frame.bytes[frame.offset..],
                    include_fds.then_some(&frame.fds),
                ) {
                    Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                        return Ok(FlushOutcome::WouldBlock)
                    }
                    Err(error) => return self.fail(FrameError::Io(error)),
                    Ok(value) => break value,
                }
            };
            if sent == 0 {
                return self.fail(FrameError::WriteZero);
            }
            if include_fds {
                frame.ancillary_sent = true;
                self.queued_fds -= frame.fds.len();
                frame.fds.clear();
            }
            frame.offset += sent;
            self.queued_bytes -= sent;
            if frame.offset == frame.bytes.len() {
                self.queue.pop_front();
            }
        }
        Ok(FlushOutcome::Drained)
    }
    fn fail<T>(&mut self, error: FrameError) -> Result<T, FrameError> {
        self.clear();
        self.failed = true;
        Err(error)
    }
}

fn align(value: usize) -> usize {
    (value + align_of::<libc::cmsghdr>() - 1) & !(align_of::<libc::cmsghdr>() - 1)
}
fn send_once(socket: BorrowedFd<'_>, bytes: &[u8], fds: Option<&[OwnedFd]>) -> io::Result<usize> {
    let mut iov = libc::iovec {
        iov_base: bytes.as_ptr().cast_mut().cast(),
        iov_len: bytes.len(),
    };
    let data_offset = align(size_of::<libc::cmsghdr>());
    let control_len = fds.map_or(0, |fds| align(data_offset + std::mem::size_of_val(fds)));
    let mut control = vec![0_usize; control_len.div_ceil(size_of::<usize>())];
    if let Some(fds) = fds {
        let header = libc::cmsghdr {
            cmsg_len: data_offset + std::mem::size_of_val(fds),
            cmsg_level: libc::SOL_SOCKET,
            cmsg_type: libc::SCM_RIGHTS,
        };
        unsafe {
            std::ptr::write(control.as_mut_ptr().cast::<libc::cmsghdr>(), header);
            let data = control
                .as_mut_ptr()
                .cast::<u8>()
                .add(data_offset)
                .cast::<RawFd>();
            for (index, fd) in fds.iter().enumerate() {
                std::ptr::write_unaligned(data.add(index), fd.as_raw_fd());
            }
        }
    }
    let mut msg: libc::msghdr = unsafe { std::mem::zeroed() };
    msg.msg_iov = &mut iov;
    msg.msg_iovlen = 1;
    if control_len != 0 {
        msg.msg_control = control.as_mut_ptr().cast();
        msg.msg_controllen = control_len;
    }
    let result = unsafe {
        libc::sendmsg(
            socket.as_raw_fd(),
            &msg,
            libc::MSG_NOSIGNAL | libc::MSG_DONTWAIT,
        )
    };
    if result < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(result as usize)
    }
}
