// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: Copyright (c) 2025 Asymptotic Inc.
// SPDX-FileCopyrightText: Copyright (c) 2025 Arun Raghavan

use std::{
    collections::VecDeque,
    io::Write,
    os::{
        fd::{AsRawFd, FromRawFd, OwnedFd, RawFd},
        unix::net::UnixStream,
    },
    sync::{Arc, Mutex, RwLock},
};

use pipewire_native_spa::{self as spa, pod::Pod};

use crate::{
    debug, default_topic, log, new_refcounted, protocol::ASYNC_SEQ_MASK, refcounted, trace, Id,
};

use super::marshal::{
    self,
    message::{ClientFooter, Header, Message},
    message::{ClientFooterPayload, ClientGeneration, CoreFooter, CoreFooterPayload},
    Marshallable,
};

default_topic!(log::topic::CONNECTION);

const MAX_MESSAGE_SIZE: usize = 16_777_216;
const MAX_CONTROL_FDS: usize = 16;

refcounted! {
    pub(crate) struct Connection {
        stream: RwLock<Option<UnixStream>>,
        hooks: Arc<Mutex<spa::hook::HookList<ConnectionEvents>>>,
        // Data received
        in_buf: RwLock<Vec<u8>>,
        in_size: RwLock<usize>,
        in_offset: RwLock<usize>,
        in_fds: RwLock<VecDeque<OwnedFd>>,
        last_recv_generation: RwLock<i64>,
        // Data to send
        out_seq: RwLock<u32>,
        out_buf: RwLock<Vec<u8>>,
        out_size: RwLock<usize>,
        out_fds: RwLock<VecDeque<RawFd>>,
        last_sent_generation: RwLock<i64>,
    }
}

#[allow(unused)]
pub(crate) struct ConnectionEvents {
    pub(crate) destroy: Option<Box<dyn FnMut()>>,
    pub(crate) error: Option<Box<dyn FnMut(u32)>>,
    pub(crate) need_flush: Option<Box<dyn FnMut()>>,
    pub(crate) start: Option<Box<dyn FnMut(u32)>>,
}

impl Connection {
    pub(crate) fn new(stream: Option<UnixStream>) -> Self {
        debug!("Creating new connection to {stream:?}");
        Self {
            inner: new_refcounted(InnerConnection::new(stream)),
        }
    }

    pub(crate) fn next_seq(&self) -> u32 {
        *self.inner.out_seq.read().unwrap()
    }

    pub(crate) fn set_stream(&self, stream: UnixStream) {
        self.inner.stream.write().unwrap().replace(stream);
    }

    pub(crate) fn disconnect(&self) {
        self.inner.stream.write().unwrap().take();
        self.clear_buffers();
    }

    fn clear_buffers(&self) {
        self.inner.in_buf.write().unwrap().fill(0);
        *self.inner.in_size.write().unwrap() = 0;
        *self.inner.in_offset.write().unwrap() = 0;
        self.inner.in_fds.write().unwrap().clear();
        *self.inner.last_recv_generation.write().unwrap() = 0;
        *self.inner.out_seq.write().unwrap() = 0;
        self.inner.out_buf.write().unwrap().fill(0);
        *self.inner.out_size.write().unwrap() = 0;
        self.inner.out_fds.write().unwrap().clear();
        *self.inner.last_sent_generation.write().unwrap() = 0;
    }

    pub(crate) fn add_listener(&self, events: ConnectionEvents) -> spa::hook::HookId {
        self.inner.hooks.lock().unwrap().append(events)
    }

    pub(crate) fn remove_listener(&self, listener: spa::hook::HookId) {
        let _ = self.inner.hooks.lock().unwrap().remove(listener);
    }

    pub(crate) fn push<T: Marshallable + std::fmt::Debug>(
        &self,
        id: Id,
        object: T,
    ) -> std::io::Result<()> {
        let seq = *self.inner.out_seq.read().unwrap();

        let recv_generation = self.inner.last_recv_generation.read().unwrap();
        let mut sent_generation = self.inner.last_sent_generation.write().unwrap();

        // TODO: support CoreGeneration as well when we implement server
        let footer = if *recv_generation > *sent_generation {
            *sent_generation = *recv_generation;
            trace!("sending client generation {}", *recv_generation);
            let mut footer = ClientFooter::new();
            footer.push(ClientFooterPayload::Generation(ClientGeneration {
                client_generation: *recv_generation,
            }));
            Some(footer)
        } else {
            None
        };

        let message = Message {
            header: Header {
                id,
                opcode: object.opcode(),
                seq,
                size: 0,  // filled by encode
                n_fds: 0, // TOOO
            },
            object,
            footer,
        };

        let mut buf = self.inner.out_buf.write().unwrap();
        let mut size = self.inner.out_size.write().unwrap();

        loop {
            let rest = &mut buf.as_mut_slice()[*size..];
            match message.encode(rest) {
                Ok(written) => {
                    *size += written;
                    break;
                }
                Err(spa::pod::Error::NoSpace) => {
                    let capacity = buf.len();
                    if capacity > MAX_MESSAGE_SIZE {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::InvalidInput,
                            format!("cannot send message > {MAX_MESSAGE_SIZE}"),
                        ));
                    }
                    buf.resize(capacity * 2, 0);
                    // And now we try again
                }
                _ => unreachable!(),
            }
        }

        trace!(
            "pushed message id:{id} opcode:{} seq:{seq} payload:{:?} (filled: {size})",
            message.header.opcode,
            message.object
        );

        *self.inner.out_seq.write().unwrap() = (seq + 1) & ASYNC_SEQ_MASK;
        spa::emit_hook!(self.inner.hooks, need_flush);

        Ok(())
    }

    pub(crate) fn flush(&self) -> std::io::Result<()> {
        let mut o_stream = self.inner.stream.write().unwrap();
        let stream = o_stream.as_mut().unwrap();
        let mut buf = self.inner.out_buf.write().unwrap();
        let mut size = self.inner.out_size.write().unwrap();
        let mut idx = 0;
        let mut res = Ok(());

        trace!("flushing {} bytes", *size);

        while idx < *size {
            let sent = match stream.write(&buf[idx..*size]) {
                Ok(size) => size,
                Err(err) => {
                    if err.kind() == std::io::ErrorKind::Interrupted {
                        continue;
                    } else {
                        res = Err(err);
                        break;
                    }
                }
            };

            idx += sent;
        }

        if idx == buf.len() {
            buf.clear();
            *size = 0;
        } else {
            buf.copy_within(idx.., 0);
            *size -= idx;
        }

        res
    }

    pub(crate) fn next_message(&self) -> std::io::Result<Header> {
        loop {
            let (wanted_capacity, header) = self.parse_next()?;
            trace!(
                "we need {wanted_capacity}, got header: {}",
                header.is_some()
            );

            let capacity = self.inner.in_buf.read().unwrap().len();
            if capacity < wanted_capacity {
                // Not enough space for header or message, make some space, try to fill some data,
                // and then retry
                trace!(
                    "expanding capacity to {}",
                    wanted_capacity.max(capacity * 2)
                );
                self.inner
                    .in_buf
                    .write()
                    .unwrap()
                    .resize(wanted_capacity.max(2 * capacity), 0);
                self.read()?;
            } else if let Some(header) = header {
                // We had enough space, and got the header.
                trace!(
                    "got message id:{} opcode:{} seq:{} size:{}",
                    header.id,
                    header.opcode,
                    header.seq,
                    header.size
                );

                // Let's make sure we also have the body
                let available =
                    *self.inner.in_size.read().unwrap() - *self.inner.in_offset.read().unwrap();
                if available >= header.size as usize {
                    return Ok(header);
                } else {
                    // We read the header but not the data, so continue reading.
                    self.read()?;
                }
            } else {
                // We had enough space, but don't have the data, let's try to read data into the
                // buffer
                self.read()?;
            }
        }
    }

    pub(crate) fn decode_message<T: Marshallable, F: Pod<DecodesTo = F>>(
        &self,
        header: &Header,
    ) -> std::io::Result<(T, Option<F>)> {
        let buf = self.inner.in_buf.read().unwrap();
        let mut size = self.inner.in_size.write().unwrap();
        let mut offset = self.inner.in_offset.write().unwrap();

        let start = *offset + marshal::HEADER_LEN;
        let end = start + header.size as usize;

        // Update the external offset and size before possibly bailing out with an error, otherwise
        // we could be reading the same chunk again and again when that happens.
        *offset += marshal::HEADER_LEN + header.size as usize;
        if *offset == *size {
            // We've consumed all the data
            *offset = 0;
            *size = 0;
        }

        let (body, body_size) = T::decode(header.opcode, &buf[start..end]).map_err(|e| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Could not decode message body: {e:?}"),
            )
        })?;

        let (footer, footer_size) = if body_size < header.size as usize {
            let (f, fs) = F::decode(&buf[start + body_size..]).map_err(|e| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("Could not decode message footer: {e:?}"),
                )
            })?;
            (Some(f), fs)
        } else {
            (None, 0)
        };

        if body_size + footer_size != header.size as usize {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "Mismatched message size({}) and body size({}) + footer_size({})",
                    header.size, body_size, footer_size
                ),
            ));
        }

        Ok((body, footer))
    }

    pub(crate) fn decode_core_message<T: Marshallable>(
        &self,
        header: &Header,
    ) -> std::io::Result<T> {
        let (object, footer) = self.decode_message(header)?;

        self.update_generation(footer.as_ref());

        Ok(object)
    }

    pub(crate) fn pop_fd(&self) -> Option<OwnedFd> {
        self.inner.in_fds.write().unwrap().pop_front()
    }

    // TODO: support CoreGeneration as well when we implement server
    pub fn update_generation(&self, footer: Option<&CoreFooter>) {
        if let Some(footer) = footer {
            for p in &footer.payloads {
                match p {
                    CoreFooterPayload::Generation(g) => {
                        trace!("updating core generation to {}", g.registry_generation);
                        *self.inner.last_recv_generation.write().unwrap() = g.registry_generation;
                    }
                }
            }
        }
    }

    fn parse_next(&self) -> std::io::Result<(usize, Option<Header>)> {
        let size = *self.inner.in_size.read().unwrap();
        let offset = *self.inner.in_offset.read().unwrap();

        if size - offset < marshal::HEADER_LEN {
            return Ok((offset + marshal::HEADER_LEN, None));
        }

        trace!("looking for message header from [{offset}..{size}]");

        let buf = self.inner.in_buf.read().unwrap();
        let header = match Header::decode(&buf[offset..size]) {
            Ok((header, _)) => header,
            Err(e) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("Failed to parse message: {e:?}"),
                ))
            }
        };

        Ok((
            offset + marshal::HEADER_LEN + header.size as usize,
            Some(header),
        ))
    }

    fn read(&self) -> std::io::Result<()> {
        let stream = self.inner.stream.read().unwrap();
        let stream = stream.as_ref().unwrap();
        let mut buf = self.inner.in_buf.write().unwrap();
        let mut size = self.inner.in_size.write().unwrap();
        let mut control = vec![
            0u8;
            unsafe {
                libc::CMSG_SPACE((MAX_CONTROL_FDS * std::mem::size_of::<RawFd>()) as u32) as usize
            }
        ];

        let mut iov = libc::iovec {
            iov_base: buf[*size..].as_mut_ptr().cast::<libc::c_void>(),
            iov_len: buf.len() - *size,
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
            let read =
                unsafe { libc::recvmsg(stream.as_raw_fd(), &mut msg, libc::MSG_CMSG_CLOEXEC) };
            if read < 0 {
                let err = std::io::Error::last_os_error();
                if err.kind() == std::io::ErrorKind::Interrupted {
                    continue;
                }

                return Err(err);
            }

            break read;
        };

        trace!("read {read} bytes at {size}");

        if msg.msg_flags & libc::MSG_CTRUNC == libc::MSG_CTRUNC {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "control message was truncated",
            ));
        }

        let msg_ptr = &msg as *const libc::msghdr;
        let mut cmsg = unsafe { libc::CMSG_FIRSTHDR(msg_ptr) };
        while !cmsg.is_null() {
            let level = unsafe { (*cmsg).cmsg_level };
            let type_ = unsafe { (*cmsg).cmsg_type };

            if level == libc::SOL_SOCKET && type_ == libc::SCM_RIGHTS {
                let data_ptr = unsafe { libc::CMSG_DATA(cmsg) }.cast::<RawFd>();
                let data_len =
                    unsafe { (*cmsg).cmsg_len } as usize - unsafe { libc::CMSG_LEN(0) } as usize;
                let n_fds = data_len / std::mem::size_of::<RawFd>();

                let mut in_fds = self.inner.in_fds.write().unwrap();
                for idx in 0..n_fds {
                    let fd = unsafe { *data_ptr.add(idx) };
                    if fd >= 0 {
                        let fd = unsafe { OwnedFd::from_raw_fd(fd) };
                        in_fds.push_back(fd);
                    }
                }
            }

            cmsg = unsafe { libc::CMSG_NXTHDR(msg_ptr, cmsg) };
        }

        if read > 0 {
            *size += read as usize;
            Ok(())
        } else {
            // Nothing to process, we're done
            Err(std::io::Error::from_raw_os_error(libc::EAGAIN))
        }
    }
}

impl InnerConnection {
    pub(crate) fn new(stream: Option<UnixStream>) -> Self {
        InnerConnection {
            stream: RwLock::new(stream),
            hooks: spa::hook::HookList::new(),
            in_buf: RwLock::new(vec![0; 16384]),
            in_size: RwLock::new(0),
            in_offset: RwLock::new(0),
            in_fds: RwLock::new(VecDeque::new()),
            last_recv_generation: RwLock::new(0),
            out_seq: RwLock::new(0),
            out_buf: RwLock::new(vec![0; 16384]),
            out_size: RwLock::new(0),
            out_fds: RwLock::new(VecDeque::new()),
            last_sent_generation: RwLock::new(0),
        }
    }
}
