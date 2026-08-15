use super::{
    FrameError, FrameFds, FrameLimits, Header, ReceivedFrame, HEADER_LEN, WIRE_MAX_PAYLOAD,
};
use std::collections::VecDeque;
use std::io;
use std::mem::{align_of, size_of};
use std::os::fd::{BorrowedFd, FromRawFd, OwnedFd, RawFd};

#[derive(Debug)]
pub enum ReceiveOutcome {
    Frame(ReceivedFrame),
    WouldBlock,
    Closed,
}

struct FdBatch {
    at: u64,
    fds: VecDeque<OwnedFd>,
}
enum RecvState {
    Header,
    Payload {
        header: Header,
        frame_start: u64,
        frame_end: u64,
    },
    Eof,
    Failed,
}

pub struct FrameReceiver {
    limits: FrameLimits,
    state: RecvState,
    bytes: VecDeque<u8>,
    byte_base: u64,
    stream_end: u64,
    fd_batches: VecDeque<FdBatch>,
}

impl FrameReceiver {
    #[must_use]
    pub fn new(limits: FrameLimits) -> Self {
        Self {
            limits,
            state: RecvState::Header,
            bytes: VecDeque::new(),
            byte_base: 0,
            stream_end: 0,
            fd_batches: VecDeque::new(),
        }
    }
    #[must_use]
    pub fn buffered_bytes(&self) -> usize {
        self.bytes.len()
    }
    #[must_use]
    pub fn pending_fds(&self) -> usize {
        self.fd_batches.iter().map(|batch| batch.fds.len()).sum()
    }
    pub fn clear(&mut self) {
        let terminal = match self.state {
            RecvState::Failed => Some(RecvState::Failed),
            RecvState::Eof => Some(RecvState::Eof),
            _ => None,
        };
        self.bytes.clear();
        self.fd_batches.clear();
        self.byte_base = self.stream_end;
        self.state = terminal.unwrap_or(RecvState::Header);
    }

    pub fn receive(&mut self, socket: BorrowedFd<'_>) -> Result<ReceiveOutcome, FrameError> {
        if matches!(self.state, RecvState::Failed) {
            return Err(FrameError::Poisoned);
        }
        if matches!(self.state, RecvState::Eof) {
            return Ok(ReceiveOutcome::Closed);
        }
        loop {
            if matches!(self.state, RecvState::Header) && self.bytes.len() >= HEADER_LEN {
                let raw: [u8; HEADER_LEN] = self
                    .bytes
                    .iter()
                    .take(HEADER_LEN)
                    .copied()
                    .collect::<Vec<_>>()
                    .try_into()
                    .expect("length checked");
                let header = Header::decode(raw);
                let payload = header.payload_len as usize;
                if payload > self.limits.max_payload.min(WIRE_MAX_PAYLOAD) {
                    return self.fail(FrameError::PayloadTooLarge {
                        declared: payload,
                        limit: self.limits.max_payload.min(WIRE_MAX_PAYLOAD),
                    });
                }
                let fds = header.n_fds as usize;
                if fds > self.limits.max_frame_fds {
                    return self.fail(FrameError::TooManyFrameFds {
                        declared: fds,
                        limit: self.limits.max_frame_fds,
                    });
                }
                let frame_start = self.byte_base;
                let frame_len =
                    HEADER_LEN
                        .checked_add(payload)
                        .ok_or(FrameError::PayloadTooLarge {
                            declared: payload,
                            limit: self.limits.max_payload,
                        })?;
                let Some(frame_end) = frame_start.checked_add(frame_len as u64) else {
                    return self.fail(FrameError::TruncatedFrame {
                        buffered: self.bytes.len(),
                        needed: frame_len,
                    });
                };
                self.state = RecvState::Payload {
                    header,
                    frame_start,
                    frame_end,
                };
            }
            if let RecvState::Payload {
                header,
                frame_start,
                frame_end,
            } = self.state
            {
                let needed = usize::try_from(frame_end - frame_start).expect("frame size is usize");
                if self.bytes.len() >= needed {
                    let available = self
                        .fd_batches
                        .iter()
                        .filter(|batch| batch.at < frame_end)
                        .map(|batch| batch.fds.len())
                        .sum::<usize>();
                    let declared = header.n_fds as usize;
                    if available < declared {
                        return self.fail(FrameError::MissingFds {
                            declared,
                            available,
                        });
                    }
                    self.bytes.drain(..HEADER_LEN);
                    let payload = self.bytes.drain(..header.payload_len as usize).collect();
                    self.byte_base = frame_end;
                    let mut fds = Vec::with_capacity(declared);
                    while fds.len() < declared {
                        let batch = self.fd_batches.front_mut().expect("eligible count checked");
                        if batch.at >= frame_end {
                            unreachable!("eligible count checked");
                        }
                        while fds.len() < declared {
                            let Some(fd) = batch.fds.pop_front() else {
                                break;
                            };
                            fds.push(Some(fd));
                        }
                        if batch.fds.is_empty() {
                            self.fd_batches.pop_front();
                        }
                    }
                    self.state = RecvState::Header;
                    return Ok(ReceiveOutcome::Frame(ReceivedFrame {
                        header,
                        payload,
                        fds: FrameFds(fds),
                    }));
                }
            }

            let max_buffer = HEADER_LEN.saturating_add(self.limits.max_payload);
            let room = max_buffer.saturating_sub(self.bytes.len());
            let chunk_len = self.limits.recv_chunk_bytes.max(1).min(room.max(1));
            match recv_once(socket, chunk_len, self.limits.recv_control_fds) {
                Ok((bytes, fds)) if bytes.is_empty() => {
                    debug_assert!(fds.is_empty());
                    if !self.bytes.is_empty() {
                        let needed = match self.state {
                            RecvState::Payload {
                                frame_start,
                                frame_end,
                                ..
                            } => (frame_end - frame_start) as usize,
                            _ => HEADER_LEN,
                        };
                        return self.fail(FrameError::TruncatedFrame {
                            buffered: self.bytes.len(),
                            needed,
                        });
                    }
                    let pending = self.pending_fds();
                    if pending != 0 {
                        return self.fail(FrameError::UnexpectedFdsAtEof { count: pending });
                    }
                    self.state = RecvState::Eof;
                    return Ok(ReceiveOutcome::Closed);
                }
                Ok((bytes, fds)) => {
                    let pending = self.pending_fds();
                    if pending.saturating_add(fds.len()) > self.limits.max_pending_fds {
                        return self.fail(FrameError::TooManyPendingFds {
                            received: pending + fds.len(),
                            limit: self.limits.max_pending_fds,
                        });
                    }
                    let at = self.stream_end;
                    let Some(stream_end) = self.stream_end.checked_add(bytes.len() as u64) else {
                        return self
                            .fail(FrameError::Io(io::Error::other("stream position overflow")));
                    };
                    self.stream_end = stream_end;
                    self.bytes.extend(bytes);
                    if !fds.is_empty() {
                        self.fd_batches.push_back(FdBatch {
                            at,
                            fds: fds.into(),
                        });
                    }
                }
                Err(FrameError::Io(error)) if error.kind() == io::ErrorKind::WouldBlock => {
                    return Ok(ReceiveOutcome::WouldBlock)
                }
                Err(error) => return self.fail(error),
            }
        }
    }

    fn fail<T>(&mut self, error: FrameError) -> Result<T, FrameError> {
        self.bytes.clear();
        self.fd_batches.clear();
        self.state = RecvState::Failed;
        Err(error)
    }
}

fn cmsg_align(value: usize) -> Option<usize> {
    value
        .checked_add(align_of::<libc::cmsghdr>() - 1)
        .map(|v| v & !(align_of::<libc::cmsghdr>() - 1))
}

fn recv_once(
    socket: BorrowedFd<'_>,
    chunk_len: usize,
    control_fds: usize,
) -> Result<(Vec<u8>, Vec<OwnedFd>), FrameError> {
    let mut bytes = vec![0_u8; chunk_len];
    let data_offset = cmsg_align(size_of::<libc::cmsghdr>()).ok_or(FrameError::MalformedControl)?;
    let control_len = cmsg_align(
        data_offset
            .checked_add(control_fds.saturating_mul(size_of::<RawFd>()))
            .ok_or(FrameError::MalformedControl)?,
    )
    .ok_or(FrameError::MalformedControl)?;
    let words = control_len.div_ceil(size_of::<usize>());
    let mut control = vec![0_usize; words];
    let mut iov = libc::iovec {
        iov_base: bytes.as_mut_ptr().cast(),
        iov_len: bytes.len(),
    };
    let mut msg: libc::msghdr = unsafe { std::mem::zeroed() };
    msg.msg_iov = &mut iov;
    msg.msg_iovlen = 1;
    msg.msg_control = control.as_mut_ptr().cast();
    msg.msg_controllen = control.len() * size_of::<usize>();
    let count = loop {
        let result = unsafe {
            libc::recvmsg(
                socket.as_raw_fd(),
                &mut msg,
                libc::MSG_CMSG_CLOEXEC | libc::MSG_DONTWAIT,
            )
        };
        if result >= 0 {
            break result as usize;
        }
        let error = io::Error::last_os_error();
        if error.kind() != io::ErrorKind::Interrupted {
            return Err(FrameError::Io(error));
        }
    };
    let visible = msg.msg_controllen.min(control.len() * size_of::<usize>());
    let owned = parse_control(
        unsafe { std::slice::from_raw_parts(msg.msg_control.cast::<u8>(), visible) },
        msg.msg_flags,
    )?;
    bytes.truncate(count);
    Ok((bytes, owned))
}

fn parse_control(control: &[u8], flags: libc::c_int) -> Result<Vec<OwnedFd>, FrameError> {
    let data_offset = cmsg_align(size_of::<libc::cmsghdr>()).ok_or(FrameError::MalformedControl)?;
    let visible = control.len();
    let mut owned = Vec::new();
    let mut offset = 0;
    while offset < visible {
        if visible - offset < size_of::<libc::cmsghdr>() {
            return Err(FrameError::MalformedControl);
        }
        let header = unsafe {
            std::ptr::read_unaligned(control.as_ptr().add(offset).cast::<libc::cmsghdr>())
        };
        let length = header.cmsg_len;
        if length < data_offset || length > visible - offset {
            return Err(FrameError::MalformedControl);
        }
        if header.cmsg_level == libc::SOL_SOCKET && header.cmsg_type == libc::SCM_RIGHTS {
            let data_len = length - data_offset;
            if data_len % size_of::<RawFd>() != 0 {
                return Err(FrameError::MalformedControl);
            }
            for index in 0..data_len / size_of::<RawFd>() {
                let fd = unsafe {
                    std::ptr::read_unaligned(
                        control
                            .as_ptr()
                            .add(offset + data_offset + index * size_of::<RawFd>())
                            .cast::<RawFd>(),
                    )
                };
                if fd < 0 {
                    return Err(FrameError::MalformedControl);
                }
                owned.push(unsafe { OwnedFd::from_raw_fd(fd) });
            }
        }
        let Some(next) = cmsg_align(length).and_then(|length| offset.checked_add(length)) else {
            return Err(FrameError::MalformedControl);
        };
        if next <= offset {
            return Err(FrameError::MalformedControl);
        }
        offset = next;
    }
    if flags & libc::MSG_CTRUNC != 0 {
        return Err(FrameError::TruncatedControl);
    }
    Ok(owned)
}

use std::os::fd::AsRawFd;

#[cfg(test)]
mod tests {
    use super::*;

    fn control(cmsg_len: usize, level: libc::c_int, kind: libc::c_int, data: &[u8]) -> Vec<u8> {
        let header_len = size_of::<libc::cmsghdr>();
        let mut bytes = vec![0_u8; header_len + data.len()];
        unsafe {
            std::ptr::write_unaligned(
                bytes.as_mut_ptr().cast::<libc::cmsghdr>(),
                libc::cmsghdr {
                    cmsg_len,
                    cmsg_level: level,
                    cmsg_type: kind,
                },
            );
        }
        bytes[header_len..].copy_from_slice(data);
        bytes
    }

    #[test]
    fn rejects_short_oversized_and_unaligned_rights_metadata() {
        let header_len = size_of::<libc::cmsghdr>();
        assert!(matches!(
            parse_control(&control(header_len - 1, 0, 0, &[]), 0),
            Err(FrameError::MalformedControl)
        ));
        assert!(matches!(
            parse_control(&control(header_len + 8, 0, 0, &[]), 0),
            Err(FrameError::MalformedControl)
        ));
        assert!(matches!(
            parse_control(
                &control(header_len + 1, libc::SOL_SOCKET, libc::SCM_RIGHTS, &[0]),
                0
            ),
            Err(FrameError::MalformedControl)
        ));
    }

    #[test]
    fn rejects_negative_rights_and_ignores_unknown_control() {
        let header_len = size_of::<libc::cmsghdr>();
        let negative = (-1_i32).to_ne_bytes();
        assert!(matches!(
            parse_control(
                &control(
                    header_len + negative.len(),
                    libc::SOL_SOCKET,
                    libc::SCM_RIGHTS,
                    &negative
                ),
                0
            ),
            Err(FrameError::MalformedControl)
        ));
        assert!(parse_control(&control(header_len, 123, 456, &[]), 0)
            .unwrap()
            .is_empty());
    }
}
