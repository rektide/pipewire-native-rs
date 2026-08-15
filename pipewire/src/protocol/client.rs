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
        let _ = self.inner.source.write().unwrap().take();
        let _ = self.inner.stream.write().unwrap().take();

        self.inner.connection.disconnect();
        *self.inner.connected.write().unwrap() = false;
        *self.inner.need_flush.write().unwrap() = false;

        *self.inner.last_in_seq.write().unwrap() = 0;
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

        *self.inner.source.write().unwrap() = source;

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
                    let main_loop = self.core().context().main_loop();
                    let mut source_ref = self.inner.source.write().unwrap();
                    let source = source_ref.as_mut().unwrap();
                    let _ = main_loop.update_io(source, source.mask() & !spa::flags::Io::OUT);
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
        let mut message =
            super::marshal::message::InboundMessage::new(header.opcode, &payload, &mut fds);

        match object_type {
            types::interface::CORE => {
                let core = core.find_object::<Core>(header.object_id).unwrap();
                super::marshal::core::Events::demarshal(&mut message, core)?;
            }
            types::interface::CLIENT => {
                let client = core
                    .find_object::<proxy::client::Client>(header.object_id)
                    .unwrap();
                super::marshal::client::Events::demarshal(&mut message, client)?;
            }
            types::interface::DEVICE => {
                let device = core
                    .find_object::<proxy::device::Device>(header.object_id)
                    .unwrap();
                super::marshal::device::Events::demarshal(&mut message, device)?;
            }
            types::interface::FACTORY => {
                let factory = core
                    .find_object::<proxy::factory::Factory>(header.object_id)
                    .unwrap();
                super::marshal::factory::Events::demarshal(&mut message, factory)?;
            }
            types::interface::LINK => {
                let link = core
                    .find_object::<proxy::link::Link>(header.object_id)
                    .unwrap();
                super::marshal::link::Events::demarshal(&mut message, link)?;
            }
            types::interface::METADATA => {
                let metadata = core
                    .find_object::<proxy::metadata::Metadata>(header.object_id)
                    .unwrap();
                super::marshal::metadata::Events::demarshal(&mut message, metadata)?;
            }
            types::interface::MODULE => {
                let module = core
                    .find_object::<proxy::module::Module>(header.object_id)
                    .unwrap();
                super::marshal::module::Events::demarshal(&mut message, module)?;
            }
            types::interface::NODE => {
                let node = core
                    .find_object::<proxy::node::Node>(header.object_id)
                    .unwrap();
                super::marshal::node::Events::demarshal(&mut message, node)?;
            }
            types::interface::PORT => {
                let port = core
                    .find_object::<proxy::port::Port>(header.object_id)
                    .unwrap();
                super::marshal::port::Events::demarshal(&mut message, port)?;
            }
            types::interface::PROFILER => {
                let profiler = core
                    .find_object::<proxy::profiler::Profiler>(header.object_id)
                    .unwrap();
                super::marshal::profiler::Events::demarshal(&mut message, profiler)?;
            }
            types::interface::REGISTRY => {
                let registry = core
                    .find_object::<proxy::registry::Registry>(header.object_id)
                    .unwrap();
                super::marshal::registry::Events::demarshal(&mut message, registry)?;
            }
            _ => unreachable!(),
        }

        self.inner.connection.update_generation(message.footer());
        *self.inner.last_in_seq.write().unwrap() = header.seq;

        Ok(())
    }

    fn on_connection_error(&self, err: std::io::Error, msg: &str) {
        warn!("Got connection error: {:?}", err);

        if let Some(source) = self.inner.source.write().unwrap().take() {
            let main_loop = self.core().context().main_loop();
            main_loop.destroy_source(source);
        }

        let core = &self.core();
        let seq = *self.inner.last_in_seq.read().unwrap();
        let res = err
            .raw_os_error()
            .unwrap_or(err.kind() as i32)
            .unsigned_abs();

        proxy_notify!(core, error, seq, res, msg);
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
