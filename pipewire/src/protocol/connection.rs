// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: Copyright (c) 2025 Asymptotic Inc.
// SPDX-FileCopyrightText: Copyright (c) 2025 Arun Raghavan

use std::{
    os::{
        fd::{AsFd, OwnedFd},
        unix::net::UnixStream,
    },
    sync::{Arc, Mutex, RwLock},
};

use pipewire_native_protocol::native::frame::{
    FlushOutcome, FrameError, FrameLimits, FrameReceiver, FrameSender, OutboundFrame,
    ReceiveOutcome, ReceivedFrame,
};
use pipewire_native_spa::{self as spa, pod::Pod};

use crate::{
    debug, default_topic, log, new_refcounted, protocol::ASYNC_SEQ_MASK, refcounted, trace, Id,
};

use super::marshal::{
    message::ClientFooter,
    message::{ClientFooterPayload, ClientGeneration, CoreFooter, CoreFooterPayload},
    Marshallable,
};

default_topic!(log::topic::CONNECTION);

const MAX_MESSAGE_SIZE: usize = 16_777_216;
refcounted! {
    pub(crate) struct Connection {
        stream: RwLock<Option<UnixStream>>,
        hooks: Arc<Mutex<spa::hook::HookList<ConnectionEvents>>>,
        receiver: RwLock<FrameReceiver>,
        last_recv_generation: RwLock<i64>,
        // Data to send
        out_seq: RwLock<u32>,
        sender: RwLock<FrameSender>,
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
        *self.inner.receiver.write().unwrap() = FrameReceiver::new(FrameLimits::default());
        *self.inner.last_recv_generation.write().unwrap() = 0;
        *self.inner.out_seq.write().unwrap() = 0;
        *self.inner.sender.write().unwrap() = FrameSender::new(FrameLimits::default());
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
        self.push_with_owned_fds(id, object, Vec::new())
    }

    pub(crate) fn push_with_owned_fds<T: Marshallable + std::fmt::Debug>(
        &self,
        id: Id,
        object: T,
        fds: Vec<OwnedFd>,
    ) -> std::io::Result<()> {
        let seq = *self.inner.out_seq.read().unwrap();

        let recv_generation = *self.inner.last_recv_generation.read().unwrap();
        let sent_generation = *self.inner.last_sent_generation.read().unwrap();

        // TODO: support CoreGeneration as well when we implement server
        let footer = if recv_generation > sent_generation {
            trace!("sending client generation {recv_generation}");
            let mut footer = ClientFooter::new();
            footer.push(ClientFooterPayload::Generation(ClientGeneration {
                client_generation: recv_generation,
            }));
            Some(footer)
        } else {
            None
        };

        let opcode = object.opcode();
        let mut payload = vec![0; 16384];

        loop {
            match object.encode(&mut payload) {
                Ok(object_size) => {
                    let footer_size = if let Some(footer) = &footer {
                        match footer.encode(&mut payload[object_size..]) {
                            Ok(size) => size,
                            Err(spa::pod::Error::NoSpace) => {
                                grow_payload_buffer(&mut payload)?;
                                continue;
                            }
                            Err(error) => return Err(pod_encode_error(error)),
                        }
                    } else {
                        0
                    };
                    payload.truncate(object_size + footer_size);
                    break;
                }
                Err(spa::pod::Error::NoSpace) => {
                    grow_payload_buffer(&mut payload)?;
                }
                Err(error) => return Err(pod_encode_error(error)),
            }
        }

        let limits = FrameLimits::default();
        let frame =
            OutboundFrame::new(id, opcode, seq, payload, fds, limits).map_err(frame_error)?;
        self.inner
            .sender
            .write()
            .unwrap()
            .enqueue(frame)
            .map_err(frame_error)?;

        if footer.is_some() {
            *self.inner.last_sent_generation.write().unwrap() = recv_generation;
        }

        trace!("pushed message id:{id} opcode:{opcode} seq:{seq} payload:{object:?}");

        *self.inner.out_seq.write().unwrap() = (seq + 1) & ASYNC_SEQ_MASK;
        spa::emit_hook!(self.inner.hooks, need_flush);

        Ok(())
    }

    pub(crate) fn flush(&self) -> std::io::Result<()> {
        let stream = self.inner.stream.read().unwrap();
        let stream = stream.as_ref().ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::NotConnected, "connection has no stream")
        })?;
        let mut sender = self.inner.sender.write().unwrap();

        trace!("flushing {} bytes", sender.queued_bytes());

        match sender.flush(stream.as_fd()).map_err(frame_error)? {
            FlushOutcome::Drained => Ok(()),
            FlushOutcome::WouldBlock => Err(std::io::Error::from_raw_os_error(libc::EAGAIN)),
        }
    }

    pub(crate) fn receive_frame(&self) -> std::io::Result<ReceivedFrame> {
        let stream = self.inner.stream.read().unwrap();
        let stream = stream.as_ref().ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::NotConnected, "connection has no stream")
        })?;
        match self
            .inner
            .receiver
            .write()
            .unwrap()
            .receive(stream.as_fd())
            .map_err(frame_error)?
        {
            ReceiveOutcome::Frame(frame) => Ok(frame),
            ReceiveOutcome::WouldBlock => Err(std::io::Error::from_raw_os_error(libc::EAGAIN)),
            ReceiveOutcome::Closed => Err(std::io::Error::from(std::io::ErrorKind::BrokenPipe)),
        }
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
}

impl InnerConnection {
    pub(crate) fn new(stream: Option<UnixStream>) -> Self {
        InnerConnection {
            stream: RwLock::new(stream),
            hooks: spa::hook::HookList::new(),
            receiver: RwLock::new(FrameReceiver::new(FrameLimits::default())),
            last_recv_generation: RwLock::new(0),
            out_seq: RwLock::new(0),
            sender: RwLock::new(FrameSender::new(FrameLimits::default())),
            last_sent_generation: RwLock::new(0),
        }
    }
}

fn grow_payload_buffer(payload: &mut Vec<u8>) -> std::io::Result<()> {
    let capacity = payload.len();
    if capacity > MAX_MESSAGE_SIZE / 2 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("cannot send message > {MAX_MESSAGE_SIZE}"),
        ));
    }
    payload.resize(capacity * 2, 0);
    Ok(())
}

fn pod_encode_error(error: spa::pod::Error) -> std::io::Error {
    std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        format!("could not encode message payload: {error:?}"),
    )
}

fn frame_error(error: FrameError) -> std::io::Error {
    match error {
        FrameError::Io(error) => error,
        error => std::io::Error::new(std::io::ErrorKind::InvalidData, error),
    }
}

#[cfg(test)]
mod tests {
    use std::io::{pipe, Read};
    use std::os::fd::AsRawFd;

    use pipewire_native_protocol::native::frame::{Header, HEADER_LEN};

    use crate::protocol::marshal::message::CoreGeneration;

    use super::*;

    #[derive(Debug)]
    struct TestMessage(Vec<u8>);

    fn connection(stream: UnixStream) -> Connection {
        crate::init();
        Connection {
            inner: new_refcounted(InnerConnection::new(Some(stream))),
        }
    }

    fn send_fd_byte(stream: &UnixStream, fd: &impl AsRawFd) {
        let byte = [0_u8];
        let mut iov = libc::iovec {
            iov_base: byte.as_ptr().cast_mut().cast(),
            iov_len: byte.len(),
        };
        let mut control = [0_usize; 4];
        let mut message: libc::msghdr = unsafe { std::mem::zeroed() };
        message.msg_iov = &mut iov;
        message.msg_iovlen = 1;
        message.msg_control = control.as_mut_ptr().cast();
        message.msg_controllen = unsafe { libc::CMSG_SPACE(size_of::<libc::c_int>() as _) as _ };
        unsafe {
            let header = libc::CMSG_FIRSTHDR(&message);
            (*header).cmsg_level = libc::SOL_SOCKET;
            (*header).cmsg_type = libc::SCM_RIGHTS;
            (*header).cmsg_len = libc::CMSG_LEN(size_of::<libc::c_int>() as _) as _;
            std::ptr::write(libc::CMSG_DATA(header).cast(), fd.as_raw_fd());
            assert_eq!(
                libc::sendmsg(stream.as_raw_fd(), &message, libc::MSG_NOSIGNAL),
                1
            );
        }
    }

    impl Marshallable for TestMessage {
        fn opcode(&self) -> u8 {
            7
        }

        fn encode(&self, data: &mut [u8]) -> Result<usize, spa::pod::Error> {
            if data.len() < self.0.len() {
                return Err(spa::pod::Error::NoSpace);
            }
            data[..self.0.len()].copy_from_slice(&self.0);
            Ok(self.0.len())
        }

        fn decode(_opcode: u8, _data: &[u8]) -> Result<(Self, usize), spa::pod::Error> {
            unreachable!()
        }
    }

    #[test]
    fn sender_wire_bytes_match_legacy_message_header() {
        let (tx, mut rx) = UnixStream::pair().unwrap();
        let connection = connection(tx);
        let payload = vec![0x11, 0x22, 0x33, 0x44, 0x55];

        connection.push(42, TestMessage(payload.clone())).unwrap();
        connection.flush().unwrap();

        let mut expected = vec![0; HEADER_LEN + payload.len()];
        let header = Header {
            object_id: 42,
            opcode: 7,
            payload_len: payload.len() as u32,
            seq: 0,
            n_fds: 0,
        };
        expected[..HEADER_LEN].copy_from_slice(&header.encode().unwrap());
        expected[HEADER_LEN..].copy_from_slice(&payload);

        let mut actual = vec![0; expected.len()];
        rx.read_exact(&mut actual).unwrap();
        assert_eq!(actual, expected);
        assert_eq!(connection.next_seq(), 1);
    }

    #[test]
    fn would_block_preserves_queued_frame_for_later_flush() {
        let (tx, mut rx) = UnixStream::pair().unwrap();
        let fill = [0_u8; 8192];
        loop {
            let sent = unsafe {
                libc::send(
                    tx.as_raw_fd(),
                    fill.as_ptr().cast(),
                    fill.len(),
                    libc::MSG_DONTWAIT | libc::MSG_NOSIGNAL,
                )
            };
            if sent < 0 {
                assert_eq!(
                    std::io::Error::last_os_error().kind(),
                    std::io::ErrorKind::WouldBlock
                );
                break;
            }
        }

        let connection = connection(tx);
        connection.push(3, TestMessage(vec![0x5a; 64])).unwrap();
        assert_eq!(
            connection.flush().unwrap_err().raw_os_error(),
            Some(libc::EAGAIN)
        );
        assert!(!connection.inner.sender.read().unwrap().is_empty());

        rx.set_nonblocking(true).unwrap();
        let mut drain = vec![0; 64 * 1024];
        let mut drained = false;
        for _ in 0..16 {
            match rx.read(&mut drain) {
                Ok(_) => {}
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
                Err(error) => panic!("failed to drain socket: {error}"),
            }
            match connection.flush() {
                Ok(()) => {
                    drained = true;
                    break;
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
                Err(error) => panic!("failed to resume flush: {error}"),
            }
        }

        assert!(
            drained,
            "queued frame did not drain after socket became writable"
        );
        assert!(connection.inner.sender.read().unwrap().is_empty());
    }

    #[test]
    fn enqueue_rejection_preserves_generation_for_the_next_frame() {
        let (tx, rx) = UnixStream::pair().unwrap();
        let connection = connection(tx);
        let mut footer = CoreFooter::new();
        footer.push(CoreFooterPayload::Generation(CoreGeneration {
            registry_generation: 41,
        }));
        connection.update_generation(Some(&footer));

        *connection.inner.sender.write().unwrap() = FrameSender::new(FrameLimits {
            max_queued_bytes: 0,
            ..FrameLimits::default()
        });
        assert!(connection.push(3, TestMessage(vec![0x5a])).is_err());
        assert_eq!(*connection.inner.last_sent_generation.read().unwrap(), 0);

        *connection.inner.sender.write().unwrap() = FrameSender::new(FrameLimits::default());
        connection.push(3, TestMessage(vec![0x5a])).unwrap();
        connection.flush().unwrap();

        let mut receiver = FrameReceiver::new(FrameLimits::default());
        let ReceiveOutcome::Frame(frame) = receiver.receive(rx.as_fd()).unwrap() else {
            panic!("expected queued frame");
        };
        let (_, payload, _) = frame.into_parts();
        let (footer, size) = ClientFooter::decode(&payload[1..]).unwrap();
        assert_eq!(size, payload.len() - 1);
        let ClientFooterPayload::Generation(generation) = &footer.payloads[0];
        assert_eq!(generation.client_generation, 41);
    }

    #[test]
    fn semantic_decode_error_leaves_coalesced_next_frame_aligned() {
        let (tx, rx) = UnixStream::pair().unwrap();
        let limits = FrameLimits::default();
        let mut sender = FrameSender::new(limits);
        sender
            .enqueue(OutboundFrame::new(0, 255, 9, vec![0xff], Vec::new(), limits).unwrap())
            .unwrap();
        sender
            .enqueue(OutboundFrame::new(0, 1, 10, Vec::new(), Vec::new(), limits).unwrap())
            .unwrap();
        sender.flush(tx.as_fd()).unwrap();

        let connection = connection(rx);
        let malformed = connection.receive_frame().unwrap();
        let (_, payload, mut fds) = malformed.into_parts();
        let mut message =
            crate::protocol::marshal::message::InboundMessage::new(255, &payload, &mut fds);
        assert!(message
            .decode::<crate::protocol::marshal::core::Events>()
            .is_err());

        assert_eq!(connection.receive_frame().unwrap().header().seq, 10);
    }

    #[test]
    fn disconnect_closes_stream_and_buffered_and_queued_descriptors() {
        let (stream, mut peer) = UnixStream::pair().unwrap();
        stream.set_nonblocking(true).unwrap();
        let connection = connection(stream);

        let (mut inbound_reader, inbound_writer) = pipe().unwrap();
        send_fd_byte(&peer, &inbound_writer);
        drop(inbound_writer);
        assert_eq!(
            connection.receive_frame().unwrap_err().raw_os_error(),
            Some(libc::EAGAIN)
        );
        assert_eq!(
            connection.inner.receiver.read().unwrap().buffered_bytes(),
            1
        );
        assert_eq!(connection.inner.receiver.read().unwrap().pending_fds(), 1);

        let (mut outbound_reader, outbound_writer) = pipe().unwrap();
        connection
            .push_with_owned_fds(3, TestMessage(vec![0x5a]), vec![outbound_writer.into()])
            .unwrap();
        assert_eq!(connection.inner.sender.read().unwrap().queued_fds(), 1);

        connection.disconnect();
        connection.disconnect();

        assert!(connection.inner.stream.read().unwrap().is_none());
        assert_eq!(
            connection.inner.receiver.read().unwrap().buffered_bytes(),
            0
        );
        assert_eq!(connection.inner.receiver.read().unwrap().pending_fds(), 0);
        assert!(connection.inner.sender.read().unwrap().is_empty());
        assert_eq!(connection.inner.sender.read().unwrap().queued_fds(), 0);

        let mut byte = [0_u8];
        assert_eq!(peer.read(&mut byte).unwrap(), 0);
        assert_eq!(inbound_reader.read(&mut byte).unwrap(), 0);
        assert_eq!(outbound_reader.read(&mut byte).unwrap(), 0);
    }
}
