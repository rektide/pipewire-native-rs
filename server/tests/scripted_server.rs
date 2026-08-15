// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: Copyright (c) 2026 Asymptotic Inc.

use std::{
    io::Write,
    os::fd::AsFd,
    os::unix::net::UnixStream,
    thread,
    time::{Duration, Instant},
};

use pipewire_native_server::{
    protocol::{
        self, decode_core_add_mem_payload, encode_client_update_properties_empty_payload,
        encode_core_get_registry_payload, encode_core_hello_payload, encode_core_sync_payload,
        read_packet, read_packet_with_fds, NativeHeader, NativePacket,
    },
    runtime::{ScriptedServer, ServerConfig},
    script::{Action, CoreAddMemAction, Expectation, RegistryGlobalAction, Scenario, ScriptStep},
    testkit,
};

#[test]
fn scripted_bootstrap_flow_is_deterministic() {
    let deadline = testkit::TestDeadline::after(Duration::from_secs(2));
    let socket_path = testkit::unique_socket_path("pipewire-native-server-bootstrap");

    let scenario = Scenario::builder()
        .steps(vec![
            ScriptStep::builder()
                .expect(Expectation::CoreHello)
                .actions(vec![])
                .build(),
            ScriptStep::builder()
                .expect(Expectation::ClientUpdateProperties)
                .actions(vec![])
                .build(),
            ScriptStep::builder()
                .expect(Expectation::CoreGetRegistry)
                .actions(vec![Action::SendRegistryGlobalOnLastRegistry(
                    RegistryGlobalAction::builder()
                        .id(100)
                        .permissions(0xFFFF_FFFF)
                        .type_("PipeWire:Interface:Node".to_string())
                        .version(3)
                        .props(vec![("node.name".to_string(), "scripted-node".to_string())])
                        .build(),
                )])
                .build(),
            ScriptStep::builder()
                .expect(Expectation::CoreSync)
                .actions(vec![Action::SendCoreDoneFromLastSync])
                .build(),
        ])
        .name("bootstrap-flow".to_string())
        .build();

    let server = ScriptedServer::builder()
        .config(
            ServerConfig::builder()
                .socket_path(socket_path.clone())
                .single_client(true)
                .trace_name("bootstrap-test".to_string())
                .build(),
        )
        .scenario(scenario)
        .build();

    let handle = testkit::spawn(server);

    let mut stream = deadline.connect(&socket_path).unwrap();

    send_client_method(
        &mut stream,
        protocol::CORE_ID,
        protocol::core_method::HELLO,
        0,
        encode_core_hello_payload(3).unwrap(),
    );

    send_client_method(
        &mut stream,
        protocol::CLIENT_ID,
        protocol::client_method::UPDATE_PROPERTIES,
        0,
        encode_client_update_properties_empty_payload().unwrap(),
    );

    send_client_method(
        &mut stream,
        protocol::CORE_ID,
        protocol::core_method::GET_REGISTRY,
        1,
        encode_core_get_registry_payload(3, 77).unwrap(),
    );

    let global = read_packet(&mut stream).unwrap();
    assert_eq!(global.header.object_id, 77);
    assert_eq!(global.header.opcode, protocol::registry_event::GLOBAL);

    send_client_method(
        &mut stream,
        protocol::CORE_ID,
        protocol::core_method::SYNC,
        2,
        encode_core_sync_payload(0, 0xBEEF).unwrap(),
    );

    let done = read_packet(&mut stream).unwrap();
    assert_eq!(done.header.object_id, protocol::CORE_ID);
    assert_eq!(done.header.opcode, protocol::core_event::DONE);

    let report = handle.wait(deadline).unwrap();
    assert_eq!(report.completed_steps, 4);
    assert_eq!(report.accepted_clients, 1);
    assert_eq!(report.rejected_clients, 0);
    assert_eq!(report.last_registry_proxy_id, Some(77));
    assert_eq!(report.last_sync.unwrap().seq, 0xBEEF);
}

#[test]
fn second_client_is_rejected_in_single_client_mode() {
    let deadline = testkit::TestDeadline::after(Duration::from_secs(2));
    let socket_path = testkit::unique_socket_path("pipewire-native-server-reject");

    let scenario = Scenario::builder()
        .steps(vec![
            ScriptStep::builder()
                .expect(Expectation::CoreHello)
                .actions(vec![])
                .build(),
            ScriptStep::builder()
                .expect(Expectation::CoreSync)
                .actions(vec![Action::SendCoreDoneFromLastSync])
                .build(),
        ])
        .build();

    let server = ScriptedServer::builder()
        .config(
            ServerConfig::builder()
                .socket_path(socket_path.clone())
                .single_client(true)
                .build(),
        )
        .scenario(scenario)
        .build();

    let handle = testkit::spawn(server);

    let mut primary = deadline.connect(&socket_path).unwrap();
    send_client_method(
        &mut primary,
        protocol::CORE_ID,
        protocol::core_method::HELLO,
        0,
        encode_core_hello_payload(3).unwrap(),
    );

    let secondary = deadline.connect(&socket_path).unwrap();
    drop(secondary);

    send_client_method(
        &mut primary,
        protocol::CORE_ID,
        protocol::core_method::SYNC,
        1,
        encode_core_sync_payload(0, 99).unwrap(),
    );

    let _ = read_packet(&mut primary).unwrap();

    let report = handle.wait(deadline).unwrap();
    assert_eq!(report.accepted_clients, 1);
    assert!(report.rejected_clients >= 1);
}

#[test]
fn core_add_mem_emits_fd_and_payload() {
    let deadline = testkit::TestDeadline::after(Duration::from_secs(2));
    let socket_path = testkit::unique_socket_path("pipewire-native-server-addmem");

    let scenario = Scenario::builder()
        .steps(vec![
            ScriptStep::builder()
                .expect(Expectation::CoreHello)
                .actions(vec![])
                .build(),
            ScriptStep::builder()
                .expect(Expectation::CoreSync)
                .actions(vec![
                    Action::SendCoreAddMem(
                        CoreAddMemAction::builder()
                            .id(321)
                            .memory_type(protocol::spa_data_type::MEM_FD)
                            .flags(0)
                            .size(4096)
                            .build(),
                    ),
                    Action::SendCoreDoneFromLastSync,
                ])
                .build(),
        ])
        .build();

    let server = ScriptedServer::builder()
        .config(
            ServerConfig::builder()
                .socket_path(socket_path.clone())
                .single_client(true)
                .build(),
        )
        .scenario(scenario)
        .build();

    let handle = testkit::spawn(server);

    let mut stream = deadline.connect(&socket_path).unwrap();
    send_client_method(
        &mut stream,
        protocol::CORE_ID,
        protocol::core_method::HELLO,
        0,
        encode_core_hello_payload(3).unwrap(),
    );
    send_client_method(
        &mut stream,
        protocol::CORE_ID,
        protocol::core_method::SYNC,
        1,
        encode_core_sync_payload(0, 777).unwrap(),
    );

    let (add_mem, fds) = read_packet_with_fds(&mut stream).unwrap();
    assert_eq!(add_mem.header.object_id, protocol::CORE_ID);
    assert_eq!(add_mem.header.opcode, protocol::core_event::ADD_MEM);
    assert_eq!(fds.len(), 1);

    let payload = decode_core_add_mem_payload(add_mem.payload.as_slice()).unwrap();
    assert_eq!(payload.id, 321);
    assert_eq!(payload.memory_type, protocol::spa_data_type::MEM_FD);
    assert_eq!(payload.flags, 0);

    let mut stat = unsafe { std::mem::zeroed::<libc::stat>() };
    let fstat_res = unsafe { libc::fstat(std::os::fd::AsRawFd::as_raw_fd(&fds[0]), &mut stat) };
    assert_eq!(fstat_res, 0);
    assert_eq!(stat.st_size, 4096);

    let done = read_packet(&mut stream).unwrap();
    assert_eq!(done.header.opcode, protocol::core_event::DONE);

    let report = handle.wait(deadline).unwrap();
    assert_eq!(report.exported_mem_ids, vec![321]);
    assert_eq!(report.last_sync.unwrap().seq, 777);
}

fn send_client_method(
    stream: &mut UnixStream,
    object_id: u32,
    opcode: u8,
    seq: u32,
    payload: Vec<u8>,
) {
    let packet = NativePacket {
        header: NativeHeader {
            object_id,
            opcode,
            payload_size: payload.len() as u32,
            seq,
            n_fds: 0,
        },
        payload,
    };

    protocol::write_packet(stream, &packet).unwrap();
}

#[test]
fn scripted_peer_receives_every_frame_byte_split() {
    let deadline = testkit::TestDeadline::after(Duration::from_secs(3));
    let socket_path = testkit::unique_socket_path("pipewire-native-server-frame-splits");
    let payload = encode_core_hello_payload(3).unwrap();
    let frame_len = protocol::HEADER_LEN + payload.len();
    let scenario = Scenario::builder()
        .steps(
            (1..frame_len)
                .map(|split| {
                    ScriptStep::builder()
                        .name(format!("frame-split-{split}"))
                        .expect(Expectation::CoreHello)
                        .actions(vec![])
                        .build()
                })
                .collect(),
        )
        .name("frame-byte-splits".to_string())
        .build();
    let server = ScriptedServer::builder()
        .config(
            ServerConfig::builder()
                .socket_path(socket_path.clone())
                .deadline(Duration::from_secs(2))
                .build(),
        )
        .scenario(scenario)
        .build();
    let handle = testkit::spawn(server);
    let mut stream = deadline.connect(&socket_path).unwrap();

    for split in 1..frame_len {
        let header = NativeHeader {
            object_id: protocol::CORE_ID,
            opcode: protocol::core_method::HELLO,
            payload_size: payload.len() as u32,
            seq: split as u32,
            n_fds: 0,
        };
        let mut wire = header.encode().unwrap().to_vec();
        wire.extend_from_slice(&payload);
        stream.write_all(&wire[..split]).unwrap();
        thread::sleep(Duration::from_millis(1));
        stream.write_all(&wire[split..]).unwrap();
    }

    let report = handle.wait(deadline).unwrap();
    assert_eq!(report.completed_steps, frame_len - 1);
}

#[test]
fn scripted_peer_rejects_unexpected_inbound_fds() {
    let deadline = testkit::TestDeadline::after(Duration::from_secs(2));
    let socket_path = testkit::unique_socket_path("pipewire-native-server-unexpected-fd");
    let scenario = Scenario::builder()
        .steps(vec![ScriptStep::builder()
            .expect(Expectation::CoreHello)
            .actions(vec![])
            .build()])
        .build();
    let server = ScriptedServer::builder()
        .config(
            ServerConfig::builder()
                .socket_path(socket_path.clone())
                .build(),
        )
        .scenario(scenario)
        .build();
    let handle = testkit::spawn(server);
    let mut stream = deadline.connect(&socket_path).unwrap();
    let packet = NativePacket {
        header: NativeHeader {
            object_id: protocol::CORE_ID,
            opcode: protocol::core_method::HELLO,
            payload_size: 0,
            seq: 0,
            n_fds: 1,
        },
        payload: encode_core_hello_payload(3).unwrap(),
    };
    let fd = tempfile::tempfile().unwrap();

    protocol::write_packet_with_fds(&mut stream, &packet, &[fd.as_fd()]).unwrap();
    let err = handle.wait(deadline).unwrap_err();
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
    assert!(err.to_string().contains("expected no inbound fds"), "{err}");
}

#[test]
fn server_accept_timeout_is_bounded_and_diagnostic() {
    let socket_path = testkit::unique_socket_path("pipewire-native-server-accept-timeout");
    let scenario = Scenario::builder()
        .steps(vec![])
        .name("accept-timeout".to_string())
        .build();
    let server = ScriptedServer::builder()
        .config(
            ServerConfig::builder()
                .socket_path(socket_path)
                .deadline(Duration::from_millis(75))
                .build(),
        )
        .scenario(scenario)
        .build();

    let started = Instant::now();
    let err = server.run().unwrap_err();
    assert!(started.elapsed() < Duration::from_secs(1));
    assert_eq!(err.kind(), std::io::ErrorKind::TimedOut);
    let diagnostic = err.to_string();
    assert!(
        diagnostic.contains("scenario=accept-timeout"),
        "{diagnostic}"
    );
    assert!(diagnostic.contains("phase=accept"), "{diagnostic}");
    assert!(diagnostic.contains("step=not-started"), "{diagnostic}");
    assert!(diagnostic.contains("buffered_frame_state="), "{diagnostic}");
    assert!(diagnostic.contains("descriptor_count="), "{diagnostic}");
}

#[test]
fn server_read_timeout_reports_last_script_step() {
    let deadline = testkit::TestDeadline::after(Duration::from_secs(1));
    let socket_path = testkit::unique_socket_path("pipewire-native-server-read-timeout");
    let scenario = Scenario::builder()
        .steps(vec![ScriptStep::builder()
            .name("waiting-for-hello".to_string())
            .expect(Expectation::CoreHello)
            .actions(vec![])
            .build()])
        .name("read-timeout".to_string())
        .build();
    let server = ScriptedServer::builder()
        .config(
            ServerConfig::builder()
                .socket_path(socket_path.clone())
                .deadline(Duration::from_millis(100))
                .build(),
        )
        .scenario(scenario)
        .build();
    let handle = testkit::spawn(server);
    let _stream = deadline.connect(&socket_path).unwrap();

    let err = handle.wait(deadline).unwrap_err();
    assert_eq!(err.kind(), std::io::ErrorKind::TimedOut);
    let diagnostic = err.to_string();
    assert!(diagnostic.contains("scenario=read-timeout"), "{diagnostic}");
    assert!(diagnostic.contains("phase=read"), "{diagnostic}");
    assert!(
        diagnostic.contains("step=0 name=waiting-for-hello"),
        "{diagnostic}"
    );
    assert!(diagnostic.contains("expectation=CoreHello"), "{diagnostic}");
    assert!(diagnostic.contains("completed_steps=0"), "{diagnostic}");
}
