// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: Copyright (c) 2026 Asymptotic Inc.

use std::{
    io::Read,
    os::fd::{AsFd, OwnedFd},
    os::unix::net::UnixListener,
    sync::{
        atomic::{AtomicBool, AtomicU32, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

use pipewire_native::{
    self as pipewire,
    context::Context,
    core::{CoreEvents, CoreMemoryImporter},
    main_loop::MainLoop,
    properties::Properties,
};
use pipewire_native_protocol::native::frame::{
    FrameLimits, FrameReceiver, FrameSender, OutboundFrame, ReceiveOutcome, ReceivedFrame,
};
use pipewire_native_server::{
    protocol::{
        core_event, decode_inbound_message, encode_core_done_payload, encode_core_info_payload,
        InboundMessage, CORE_ID,
    },
    testkit,
};
use pipewire_native_spa::pod::builder::Builder;
use serial_test::serial;

struct Importer {
    memory: Arc<Mutex<Vec<(u32, u32, OwnedFd, u32)>>>,
    added: Arc<AtomicU32>,
    removed: Arc<Mutex<Vec<u32>>>,
}

impl CoreMemoryImporter for Importer {
    fn add_memory(&mut self, id: u32, type_: u32, fd: OwnedFd, flags: u32) -> std::io::Result<()> {
        self.added.fetch_add(1, Ordering::Relaxed);
        self.memory.lock().unwrap().push((id, type_, fd, flags));
        Ok(())
    }

    fn remove_memory(&mut self, id: u32) -> std::io::Result<()> {
        self.memory.lock().unwrap().retain(|entry| entry.0 != id);
        self.removed.lock().unwrap().push(id);
        Ok(())
    }
}

fn add_mem_payload(id: u32, memory_type: u32, fd_index: i32, flags: u32) -> Vec<u8> {
    let mut storage = vec![0; 256];
    let output = Builder::new(&mut storage)
        .push_struct(|sb| {
            sb.push_int(id as i32)
                .push_id(pipewire_native_spa::pod::types::Id(memory_type))
                .push_fd(fd_index)
                .push_int(flags as i32)
        })
        .build()
        .unwrap();
    output.to_vec()
}

fn remove_mem_payload(id: u32) -> Vec<u8> {
    let mut storage = vec![0; 64];
    Builder::new(&mut storage)
        .push_struct(|sb| sb.push_int(id as i32))
        .build()
        .unwrap()
        .to_vec()
}

fn receive(receiver: &mut FrameReceiver, stream: &std::os::unix::net::UnixStream) -> ReceivedFrame {
    for _ in 0..3_000 {
        match receiver.receive(stream.as_fd()).unwrap() {
            ReceiveOutcome::Frame(frame) => return frame,
            ReceiveOutcome::WouldBlock => std::thread::sleep(Duration::from_millis(1)),
            ReceiveOutcome::Closed => panic!("peer closed before expected frame"),
        }
    }
    panic!("timed out waiting for peer frame")
}

#[test]
#[serial]
fn client_imports_indexed_add_mem_before_coalesced_done_and_close() {
    pipewire::init();
    let socket_path = testkit::unique_socket_path("pipewire-native-indexed-add-mem");
    let listener = UnixListener::bind(&socket_path).unwrap();
    let server = std::thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(3)))
            .unwrap();
        let mut receiver = FrameReceiver::new(FrameLimits::default());

        let hello = receive(&mut receiver, &stream);
        let hello_header = hello.header();
        assert!(matches!(
            decode_inbound_message(hello_header.object_id, hello_header.opcode, hello.payload(),)
                .unwrap(),
            InboundMessage::CoreHello { .. }
        ));
        let info = OutboundFrame::new(
            CORE_ID,
            core_event::INFO,
            0,
            encode_core_info_payload(1, "tester", "localhost", "1.0-test", "peer", &[]).unwrap(),
            Vec::new(),
            FrameLimits::default(),
        )
        .unwrap();
        let mut sender = FrameSender::new(FrameLimits::default());
        sender.enqueue(info).unwrap();
        sender.flush(stream.as_fd()).unwrap();

        let update = receive(&mut receiver, &stream);
        let update_header = update.header();
        assert!(matches!(
            decode_inbound_message(
                update_header.object_id,
                update_header.opcode,
                update.payload(),
            )
            .unwrap(),
            InboundMessage::ClientUpdateProperties { .. }
        ));
        let sync = receive(&mut receiver, &stream);
        let sync_header = sync.header();
        let sync_seq =
            match decode_inbound_message(sync_header.object_id, sync_header.opcode, sync.payload())
                .unwrap()
            {
                InboundMessage::CoreSync { seq, .. } => seq,
                message => panic!("expected CoreSync, got {message:?}"),
            };

        let file = tempfile::tempfile().unwrap();
        let (unknown_fd, mut unknown_fd_observer) = std::os::unix::net::UnixStream::pair().unwrap();
        unknown_fd_observer
            .set_read_timeout(Some(Duration::from_secs(3)))
            .unwrap();
        let unknown = OutboundFrame::duplicate_fds(
            999,
            42,
            1,
            Vec::new(),
            &[unknown_fd.as_fd()],
            FrameLimits::default(),
        )
        .unwrap();
        let add_mem = OutboundFrame::duplicate_fds(
            CORE_ID,
            core_event::ADD_MEM,
            2,
            add_mem_payload(55, 1, 0, 0),
            &[file.as_fd()],
            FrameLimits::default(),
        )
        .unwrap();
        let done = OutboundFrame::new(
            CORE_ID,
            core_event::DONE,
            4,
            encode_core_done_payload(CORE_ID, sync_seq).unwrap(),
            Vec::new(),
            FrameLimits::default(),
        )
        .unwrap();
        let remove_mem = OutboundFrame::new(
            CORE_ID,
            core_event::REMOVE_MEM,
            3,
            remove_mem_payload(55),
            Vec::new(),
            FrameLimits::default(),
        )
        .unwrap();
        let mut sender = FrameSender::new(FrameLimits::default());
        sender.enqueue(unknown).unwrap();
        sender.enqueue(add_mem).unwrap();
        sender.enqueue(remove_mem).unwrap();
        sender.enqueue(done).unwrap();
        sender.flush(stream.as_fd()).unwrap();
        drop(unknown_fd);
        let mut byte = [0];
        unknown_fd_observer.read(&mut byte).unwrap() == 0
    });

    let old_remote = std::env::var("PIPEWIRE_REMOTE").ok();
    unsafe { std::env::set_var("PIPEWIRE_REMOTE", socket_path.as_os_str()) };
    let props = Properties::new_vec(vec![("loop.name".to_string(), "pw-main-loop".to_string())]);
    let main_loop = MainLoop::new(&props).unwrap();
    let context = Context::new(&main_loop, Properties::new()).unwrap();
    let core = context.connect(None).unwrap();
    let imported = Arc::new(Mutex::new(Vec::new()));
    let added = Arc::new(AtomicU32::new(0));
    let removed = Arc::new(Mutex::new(Vec::new()));
    core.set_memory_importer(Some(Box::new(Importer {
        memory: imported.clone(),
        added: added.clone(),
        removed: removed.clone(),
    })));

    let done_seen = Arc::new(AtomicBool::new(false));
    let expected_seq = Arc::new(AtomicU32::new(0));
    let done_seen_cb = done_seen.clone();
    let expected_seq_cb = expected_seq.clone();
    let main_loop_cb = main_loop.clone();
    core.add_listener(CoreEvents::new(
        None,
        Some(Box::new(move |_id, seq| {
            if seq == expected_seq_cb.load(Ordering::Relaxed) {
                done_seen_cb.store(true, Ordering::Relaxed);
                main_loop_cb.quit();
            }
        })),
        None,
    ));
    expected_seq.store(core.sync().unwrap(), Ordering::Relaxed);

    let timed_out = Arc::new(AtomicBool::new(false));
    let timed_out_cb = timed_out.clone();
    let main_loop_timer = main_loop.clone();
    let mut timer = main_loop
        .add_timer(Box::new(move |_| {
            timed_out_cb.store(true, Ordering::Relaxed);
            main_loop_timer.quit();
        }))
        .unwrap();
    main_loop
        .update_timer(
            &mut timer,
            &libc::timespec {
                tv_sec: 3,
                tv_nsec: 0,
            },
            None,
            false,
        )
        .unwrap();
    main_loop.run();

    assert!(server.join().unwrap(), "unknown frame FD remained open");
    assert!(!timed_out.load(Ordering::Relaxed));
    assert!(done_seen.load(Ordering::Relaxed));
    assert_eq!(added.load(Ordering::Relaxed), 1);
    assert!(imported.lock().unwrap().is_empty());
    assert_eq!(*removed.lock().unwrap(), vec![55]);

    if let Some(remote) = old_remote {
        unsafe { std::env::set_var("PIPEWIRE_REMOTE", remote) };
    } else {
        unsafe { std::env::remove_var("PIPEWIRE_REMOTE") };
    }
}
