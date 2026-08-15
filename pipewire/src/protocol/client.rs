// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: Copyright (c) 2025 Asymptotic Inc.
// SPDX-FileCopyrightText: Copyright (c) 2025 Arun Raghavan

use std::{
    os::{
        fd::{AsRawFd, RawFd},
        unix::net::UnixStream,
    },
    path::PathBuf,
    sync::RwLock,
};

use pipewire_native_spa as spa;

use crate::{
    closure,
    core::{self, Core, WeakCore},
    debug, default_topic, keys, log, main_loop, new_refcounted,
    protocol::connection::{Connection, ConnectionEvents},
    proxy::{self, HasProxy},
    proxy_notify, refcounted, some_closure, trace, types, warn, Id,
};

default_topic!(log::topic::PROTOCOL);

fn get_runtime_dir() -> Option<String> {
    std::env::var("PIPEWIRE_RUNTIME_DIR")
        .or(std::env::var("XDG_RUNTIME_DIR"))
        .or(std::env::var("USERPROFILEDIR"))
        .ok()
}

fn get_system_dir() -> String {
    "/run/pipewire".to_owned()
}

refcounted! {
    pub(crate) struct Client {
        core: RwLock<Option<WeakCore>>,
        stream: RwLock<Option<UnixStream>>,
        connection: Connection,
        connected: RwLock<bool>,
        need_flush: RwLock<bool>,
        last_in_seq: RwLock<u32>,
        source: RwLock<Option<main_loop::Source>>,
        hooks: RwLock<Option<spa::hook::HookId>>,
    }
}

impl Client {
    pub(crate) fn new() -> Self {
        debug!("Creating new client");
        let this = Self {
            inner: new_refcounted(InnerClient::new()),
        };

        let listener = this.inner.connection.add_listener(ConnectionEvents {
            destroy: some_closure!([this] {
                this.on_destroy();
            }),
            error: None,
            need_flush: some_closure!([this] {
                this.on_need_flush();
            }),
            start: None,
        });

        this.inner.hooks.write().unwrap().replace(listener);

        this
    }

    pub(crate) fn connection(&self) -> Connection {
        self.inner.connection.clone()
    }

    pub(crate) fn core(&self) -> Core {
        self.inner
            .core
            .read()
            .unwrap()
            .clone()
            .and_then(|w| w.upgrade())
            .expect("Client shoud have core initialised on creation")
    }

    pub(crate) fn set_core(&self, core: WeakCore) {
        self.inner.set_core(core);
    }

    pub(crate) fn connect(
        &self,
        props: Option<&spa::dict::Dict>,
        done_cb: Option<Box<dyn Fn(std::io::Result<()>)>>,
    ) -> std::io::Result<()> {
        // TODO: Implement PW_KEY_REMOTE_INTENTION != "generic" (i.e. screencast and internal remotes)
        self.connect_local_socket(props, done_cb)
    }

    pub(crate) fn disconnect(&self) {
        self.clear_connection();
    }

    pub(crate) fn set_stream(&self, stream: UnixStream) -> std::io::Result<()> {
        debug!("Setting fd on connection: {stream:?}");

        let fd = stream.as_raw_fd();

        self.inner
            .connection
            .set_stream(stream.try_clone().expect("unix stream should be cloneable"));
        self.inner.stream.write().unwrap().replace(stream);
        *self.inner.connected.write().unwrap() = false;

        let main_loop = self.core().context().main_loop();

        let source = main_loop.add_io(
            fd,
            spa::flags::Io::all(),
            false,
            closure!([client <- self] fd, mask, {
                client.on_remote_data(fd, spa::flags::Io::from_bits_truncate(mask));
            }),
        );

        let Some(source) = source else {
            self.inner.stream.write().unwrap().take();
            self.inner.connection.disconnect();
            return Err(std::io::Error::other("failed to add connection I/O source"));
        };

        self.inner.source.write().unwrap().replace(source);
        *self.inner.connected.write().unwrap() = true;

        Ok(())
    }

    fn on_destroy(&self) {
        self.inner
            .connection
            .remove_listener(self.inner.hooks.read().unwrap().unwrap());
    }

    fn on_need_flush(&self) {
        *self.inner.need_flush.write().unwrap() = true;

        if let Some(source) = self.inner.source.write().unwrap().as_mut() {
            let main_loop = self.core().context().main_loop();
            let _ = main_loop.update_io(source, source.mask() | spa::flags::Io::OUT);
        }
    }

    fn on_remote_data(&self, _fd: RawFd, mask: spa::flags::Io) {
        trace!("on remote data: {mask:?}");

        if mask.intersects(spa::flags::Io::IN | spa::flags::Io::HUP) {
            loop {
                if let Err(err) = self.process_messages() {
                    // We use EAGAIN to signify there are no more messages pending
                    if err.raw_os_error() == Some(libc::EAGAIN) {
                        break;
                    } else {
                        self.on_connection_error(err, "failed to read messages");
                        return;
                    }
                }
            }
        }

        if mask.intersects(spa::flags::Io::ERR | spa::flags::Io::HUP) {
            self.on_connection_error(
                std::io::Error::from(std::io::ErrorKind::BrokenPipe),
                "I/O error",
            );
            return;
        }

        if mask.contains(spa::flags::Io::OUT) || *self.inner.need_flush.read().unwrap() {
            *self.inner.need_flush.write().unwrap() = true;

            match self
                .inner
                .stream
                .read()
                .unwrap()
                .as_ref()
                .unwrap()
                .take_error()
            {
                Ok(None) => { /* all good, nothing to do */ }
                Ok(Some(err)) => {
                    self.on_connection_error(err, "connection error");
                    return;
                }
                Err(err) => {
                    self.on_connection_error(err, "getsockopt failed");
                    return;
                }
            }

            match self.inner.connection.flush() {
                Ok(_) => {
                    *self.inner.need_flush.write().unwrap() = false;
                    let main_loop = self.core().context().main_loop();
                    let mut source_ref = self.inner.source.write().unwrap();
                    if let Some(source) = source_ref.as_mut() {
                        let _ = main_loop.update_io(source, source.mask() & !spa::flags::Io::OUT);
                    }
                }
                Err(err) => {
                    if err.raw_os_error() != Some(libc::EAGAIN) {
                        self.on_connection_error(err, "flush failed");
                    }
                }
            }
        }
    }

    fn process_messages(&self) -> std::io::Result<()> {
        let core = self.core();
        let frame = self.inner.connection.receive_frame()?;
        let header = frame.header();
        let object_type = match core.find_proxy_type(header.object_id as Id) {
            Some(type_) => type_,
            None => {
                warn!(
                    "Got message id:{} opcode:{} seq:{}",
                    header.object_id, header.opcode, header.seq
                );
                return Ok(());
            }
        };
        let (_, payload, mut fds) = frame.into_parts();
        let update_generation = |footer: &super::marshal::message::CoreFooter| {
            self.inner.connection.update_generation(Some(footer));
        };
        let mut message = super::marshal::message::InboundMessage::with_footer_handler(
            header.opcode,
            &payload,
            &mut fds,
            &update_generation,
        );

        let result = match object_type {
            types::interface::CORE => {
                let core = core.find_object::<Core>(header.object_id).unwrap();
                super::marshal::core::Events::demarshal(&mut message, core)
            }
            types::interface::CLIENT => {
                let client = core
                    .find_object::<proxy::client::Client>(header.object_id)
                    .unwrap();
                super::marshal::client::Events::demarshal(&mut message, client)
            }
            types::interface::DEVICE => {
                let device = core
                    .find_object::<proxy::device::Device>(header.object_id)
                    .unwrap();
                super::marshal::device::Events::demarshal(&mut message, device)
            }
            types::interface::FACTORY => {
                let factory = core
                    .find_object::<proxy::factory::Factory>(header.object_id)
                    .unwrap();
                super::marshal::factory::Events::demarshal(&mut message, factory)
            }
            types::interface::LINK => {
                let link = core
                    .find_object::<proxy::link::Link>(header.object_id)
                    .unwrap();
                super::marshal::link::Events::demarshal(&mut message, link)
            }
            types::interface::METADATA => {
                let metadata = core
                    .find_object::<proxy::metadata::Metadata>(header.object_id)
                    .unwrap();
                super::marshal::metadata::Events::demarshal(&mut message, metadata)
            }
            types::interface::MODULE => {
                let module = core
                    .find_object::<proxy::module::Module>(header.object_id)
                    .unwrap();
                super::marshal::module::Events::demarshal(&mut message, module)
            }
            types::interface::NODE => {
                let node = core
                    .find_object::<proxy::node::Node>(header.object_id)
                    .unwrap();
                super::marshal::node::Events::demarshal(&mut message, node)
            }
            types::interface::PORT => {
                let port = core
                    .find_object::<proxy::port::Port>(header.object_id)
                    .unwrap();
                super::marshal::port::Events::demarshal(&mut message, port)
            }
            types::interface::PROFILER => {
                let profiler = core
                    .find_object::<proxy::profiler::Profiler>(header.object_id)
                    .unwrap();
                super::marshal::profiler::Events::demarshal(&mut message, profiler)
            }
            types::interface::REGISTRY => {
                let registry = core
                    .find_object::<proxy::registry::Registry>(header.object_id)
                    .unwrap();
                super::marshal::registry::Events::demarshal(&mut message, registry)
            }
            _ => unreachable!(),
        };

        if let Err(error) = result {
            if error.kind() == std::io::ErrorKind::Unsupported {
                warn!(
                    "Ignoring unknown opcode {} for object {} ({object_type})",
                    header.opcode, header.object_id
                );
                *self.inner.last_in_seq.write().unwrap() = header.seq;
                return Ok(());
            }
            return Err(error);
        }

        *self.inner.last_in_seq.write().unwrap() = header.seq;

        Ok(())
    }

    fn on_connection_error(&self, err: std::io::Error, msg: &str) {
        let seq = *self.inner.last_in_seq.read().unwrap();
        if !self.clear_connection() {
            return;
        }

        warn!("Got connection error: {:?}", err);

        let core = &self.core();
        let res = err
            .raw_os_error()
            .unwrap_or(err.kind() as i32)
            .unsigned_abs();

        proxy_notify!(core, error, seq, res, msg);
    }

    fn clear_connection(&self) -> bool {
        let was_connected = std::mem::replace(&mut *self.inner.connected.write().unwrap(), false);

        if let Some(source) = self.inner.source.write().unwrap().take() {
            let main_loop = self.core().context().main_loop();
            main_loop.destroy_source(source);
        }

        self.inner.stream.write().unwrap().take();
        self.inner.connection.disconnect();
        *self.inner.need_flush.write().unwrap() = false;
        *self.inner.last_in_seq.write().unwrap() = 0;

        was_connected
    }

    fn connect_local_socket(
        &self,
        props: Option<&spa::dict::Dict>,
        done_cb: Option<Box<dyn Fn(std::io::Result<()>)>>,
    ) -> std::io::Result<()> {
        let manager = props.and_then(|p| p.lookup(keys::REMOTE_INTENTION)) == Some("manager");
        let mut remote_name = core::get_remote(props);

        // TODO: remote can be a list of remotes

        if manager && !remote_name.ends_with("-manager") {
            remote_name = format!("{remote_name}-manager");
        }

        if remote_name.starts_with("/") || remote_name.starts_with("@") {
            // Absolute path
            self.try_connect_local_socket(None, &remote_name, &done_cb)
        } else {
            // Relative path
            if let Some(runtime_dir) = get_runtime_dir() {
                if self
                    .try_connect_local_socket(Some(&runtime_dir), &remote_name, &done_cb)
                    .is_ok()
                {
                    // Connect via runtime dir worked
                    return Ok(());
                }
            }

            // Fallback to connect via system dir
            self.try_connect_local_socket(Some(&get_system_dir()), &remote_name, &done_cb)
        }
    }

    fn try_connect_local_socket(
        &self,
        path: Option<&str>,
        name: &str,
        done_cb: &Option<Box<dyn Fn(std::io::Result<()>)>>,
    ) -> std::io::Result<()> {
        let mut socket_path = PathBuf::new();

        if let Some(path) = path {
            socket_path.push(path);
        }

        socket_path.push(name);

        debug!("Trying to connect to {:?}", socket_path);

        // Rust sockets are implicitly CLOEXEC
        let stream = UnixStream::connect(socket_path)?;
        stream.set_nonblocking(true)?;

        let res = self.set_stream(stream);

        if let Some(cb) = done_cb {
            cb(res);
        }

        Ok(())
    }
}

impl InnerClient {
    fn new() -> Self {
        Self {
            core: RwLock::new(None),
            stream: RwLock::new(None),
            connection: Connection::new(None),
            connected: RwLock::new(false),
            need_flush: RwLock::new(false),
            last_in_seq: RwLock::new(0),
            source: RwLock::new(None),
            hooks: RwLock::new(None),
        }
    }

    fn set_core(&self, core: WeakCore) {
        self.core.write().unwrap().replace(core);
    }
}

#[cfg(test)]
mod tests {
    use std::{
        io::{pipe, Read},
        os::fd::{AsFd, AsRawFd},
        os::unix::net::{UnixListener, UnixStream},
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        },
    };

    use serial_test::serial;
    use spa::pod::Pod;

    use crate::{
        context::Context,
        core::CoreEvents,
        properties::Properties,
        protocol::marshal::{
            core::Methods,
            message::{
                ClientFooter, ClientFooterPayload, CoreFooter, CoreFooterPayload, CoreGeneration,
            },
            Marshallable,
        },
        proxy::{HasProxy, ProxyEvents},
    };
    use pipewire_native_protocol::native::frame::{
        FrameLimits, FrameReceiver, FrameSender, OutboundFrame, ReceiveOutcome,
    };

    use super::*;

    #[derive(Debug)]
    struct TestMessage;

    impl Marshallable for TestMessage {
        fn opcode(&self) -> u8 {
            7
        }

        fn encode(&self, data: &mut [u8]) -> Result<usize, spa::pod::Error> {
            data[0] = 0x5a;
            Ok(1)
        }

        fn decode(_opcode: u8, _data: &[u8]) -> Result<(Self, usize), spa::pod::Error> {
            unreachable!()
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
        message.msg_controllen =
            unsafe { libc::CMSG_SPACE(std::mem::size_of::<libc::c_int>() as _) as _ };
        unsafe {
            let header = libc::CMSG_FIRSTHDR(&message);
            (*header).cmsg_level = libc::SOL_SOCKET;
            (*header).cmsg_type = libc::SCM_RIGHTS;
            (*header).cmsg_len = libc::CMSG_LEN(std::mem::size_of::<libc::c_int>() as _) as _;
            std::ptr::write(libc::CMSG_DATA(header).cast(), fd.as_raw_fd());
            assert_eq!(
                libc::sendmsg(stream.as_raw_fd(), &message, libc::MSG_NOSIGNAL),
                1
            );
        }
    }

    fn two_int_payload(first: i32, second: i32) -> Vec<u8> {
        let mut payload = vec![0; 128];
        let size = spa::pod::builder::Builder::new(&mut payload)
            .push_struct(|builder| builder.push_int(first).push_int(second))
            .build()
            .unwrap()
            .len();
        payload.truncate(size);
        payload
    }

    fn append_generation(payload: &mut Vec<u8>, generation: i64) {
        let mut footer = CoreFooter::new();
        footer.push(CoreFooterPayload::Generation(CoreGeneration {
            registry_generation: generation,
        }));
        let offset = payload.len();
        payload.resize(offset + 128, 0);
        let size = footer.encode(&mut payload[offset..]).unwrap();
        payload.truncate(offset + size);
    }

    fn test_core() -> (
        tempfile::TempDir,
        main_loop::MainLoop,
        Context,
        Core,
        UnixStream,
    ) {
        crate::init();
        let runtime = tempfile::tempdir().unwrap();
        let socket_path = runtime.path().join("pipewire-test");
        let listener = UnixListener::bind(&socket_path).unwrap();
        let previous_remote = std::env::var_os("PIPEWIRE_REMOTE");
        unsafe { std::env::set_var("PIPEWIRE_REMOTE", &socket_path) };

        let properties = Properties::new_vec(vec![(
            "loop.name".to_string(),
            "pw-client-terminal-test".to_string(),
        )]);
        let main_loop = main_loop::MainLoop::new(&properties).unwrap();
        let context = Context::new(&main_loop, Properties::new()).unwrap();
        let core = context.connect(None).unwrap();
        let (peer, _) = listener.accept().unwrap();

        if let Some(remote) = previous_remote {
            unsafe { std::env::set_var("PIPEWIRE_REMOTE", remote) };
        } else {
            unsafe { std::env::remove_var("PIPEWIRE_REMOTE") };
        }

        (runtime, main_loop, context, core, peer)
    }

    #[test]
    #[serial]
    fn generation_bearing_ping_yields_generation_bearing_pong() {
        let (_runtime, main_loop, _context, core, mut peer) = test_core();
        core.connection().flush().unwrap();
        peer.set_nonblocking(true).unwrap();
        let mut drain = [0; 4096];
        loop {
            match peer.read(&mut drain) {
                Ok(0) => panic!("client connection closed"),
                Ok(_) => {}
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => break,
                Err(error) => panic!("failed to drain setup frames: {error}"),
            }
        }

        let limits = FrameLimits::default();
        let mut payload = two_int_payload(0, 73);
        append_generation(&mut payload, 19);
        let mut sender = FrameSender::new(limits);
        sender
            .enqueue(OutboundFrame::new(0, 2, 8, payload, Vec::new(), limits).unwrap())
            .unwrap();
        sender.flush(peer.as_fd()).unwrap();

        main_loop
            .iterate(Some(std::time::Duration::from_millis(100)))
            .unwrap();
        core.connection().flush().unwrap();

        let mut receiver = FrameReceiver::new(limits);
        let frame = loop {
            match receiver.receive(peer.as_fd()).unwrap() {
                ReceiveOutcome::Frame(frame) => break frame,
                ReceiveOutcome::WouldBlock => continue,
                ReceiveOutcome::Closed => panic!("client connection closed before Pong"),
            }
        };
        assert_eq!(frame.header().opcode, 3);
        let (_, payload, _) = frame.into_parts();
        let (_, body_size) = Methods::decode(3, &payload).unwrap();
        let (footer, footer_size) = ClientFooter::decode(&payload[body_size..]).unwrap();
        assert_eq!(body_size + footer_size, payload.len());
        let ClientFooterPayload::Generation(generation) = &footer.payloads[0];
        assert_eq!(generation.client_generation, 19);
    }

    #[test]
    #[serial]
    fn unknown_opcode_drops_its_fds_and_following_done_dispatches() {
        let (_runtime, _main_loop, _context, core, _core_peer) = test_core();
        let client = Client::new();
        client.set_core(core.downgrade());
        let (stream, peer) = UnixStream::pair().unwrap();
        client.set_stream(stream).unwrap();

        let done_count = Arc::new(AtomicUsize::new(0));
        let done_count_cb = done_count.clone();
        core.add_listener(CoreEvents::new(
            None,
            Some(Box::new(move |_, _| {
                done_count_cb.fetch_add(1, Ordering::Relaxed);
            })),
            None,
        ));

        let (mut fd_reader, fd_writer) = pipe().unwrap();
        let limits = FrameLimits::default();
        let mut sender = FrameSender::new(limits);
        sender
            .enqueue(
                OutboundFrame::new(0, 255, 10, Vec::new(), vec![fd_writer.into()], limits).unwrap(),
            )
            .unwrap();
        sender
            .enqueue(
                OutboundFrame::new(0, 1, 11, two_int_payload(0, 74), Vec::new(), limits).unwrap(),
            )
            .unwrap();
        sender.flush(peer.as_fd()).unwrap();

        client.process_messages().unwrap();
        let mut byte = [0];
        assert_eq!(fd_reader.read(&mut byte).unwrap(), 0);
        client.process_messages().unwrap();
        assert_eq!(done_count.load(Ordering::Relaxed), 1);
        assert_eq!(*client.inner.last_in_seq.read().unwrap(), 11);
    }

    #[test]
    #[serial]
    fn hup_terminal_cleanup_closes_both_streams_and_all_descriptors_once() {
        let (_runtime, _main_loop, _context, core, _core_peer) = test_core();
        let client = Client::new();
        client.set_core(core.downgrade());
        let (stream, mut peer) = UnixStream::pair().unwrap();
        client.set_stream(stream).unwrap();

        let notifications = Arc::new(AtomicUsize::new(0));
        let notifications_cb = notifications.clone();
        core.proxy().add_listener(ProxyEvents {
            error: Some(Box::new(move |_, _, _| {
                notifications_cb.fetch_add(1, Ordering::Relaxed);
            })),
            ..Default::default()
        });

        let (mut inbound_reader, inbound_writer) = pipe().unwrap();
        send_fd_byte(&peer, &inbound_writer);
        drop(inbound_writer);

        let (mut outbound_reader, outbound_writer) = pipe().unwrap();
        client
            .connection()
            .push_with_owned_fds(3, TestMessage, vec![outbound_writer.into()])
            .unwrap();

        peer.shutdown(std::net::Shutdown::Write).unwrap();
        client.on_remote_data(-1, spa::flags::Io::IN | spa::flags::Io::HUP);

        assert!(!*client.inner.connected.read().unwrap());
        assert!(client.inner.source.read().unwrap().is_none());
        assert!(client.inner.stream.read().unwrap().is_none());
        assert_eq!(notifications.load(Ordering::Relaxed), 1);

        let mut byte = [0_u8];
        assert_eq!(peer.read(&mut byte).unwrap(), 0);
        assert_eq!(inbound_reader.read(&mut byte).unwrap(), 0);
        assert_eq!(outbound_reader.read(&mut byte).unwrap(), 0);

        client.on_remote_data(-1, spa::flags::Io::HUP);
        assert_eq!(notifications.load(Ordering::Relaxed), 1);
    }
}
