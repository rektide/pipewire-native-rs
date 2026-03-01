// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: Copyright (c) 2026 Asymptotic Inc.

use std::os::unix::net::UnixStream;

use pipewire_native_server::{
    protocol::{
        self, encode_client_update_properties_empty_payload, encode_core_get_registry_payload,
        encode_core_hello_payload, encode_core_sync_payload, read_packet, NativeHeader,
        NativePacket,
    },
    runtime::{ScriptedServer, ServerConfig},
    script::{Action, Expectation, RegistryGlobalAction, Scenario, ScriptStep},
    testkit,
};

#[test]
fn scripted_bootstrap_flow_is_deterministic() {
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

    let mut stream = connect_with_retry(&socket_path);

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

    let report = handle.join().unwrap().unwrap();
    assert_eq!(report.completed_steps, 4);
    assert_eq!(report.accepted_clients, 1);
    assert_eq!(report.rejected_clients, 0);
    assert_eq!(report.last_registry_proxy_id, Some(77));
    assert_eq!(report.last_sync.unwrap().seq, 0xBEEF);
}

#[test]
fn second_client_is_rejected_in_single_client_mode() {
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

    let mut primary = connect_with_retry(&socket_path);
    send_client_method(
        &mut primary,
        protocol::CORE_ID,
        protocol::core_method::HELLO,
        0,
        encode_core_hello_payload(3).unwrap(),
    );

    let secondary = connect_with_retry(&socket_path);
    drop(secondary);

    send_client_method(
        &mut primary,
        protocol::CORE_ID,
        protocol::core_method::SYNC,
        1,
        encode_core_sync_payload(0, 99).unwrap(),
    );

    let _ = read_packet(&mut primary).unwrap();

    let report = handle.join().unwrap().unwrap();
    assert_eq!(report.accepted_clients, 1);
    assert!(report.rejected_clients >= 1);
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

fn connect_with_retry(socket_path: &std::path::Path) -> UnixStream {
    let mut attempts = 0u32;
    loop {
        match UnixStream::connect(socket_path) {
            Ok(stream) => return stream,
            Err(err) if attempts < 60 => {
                attempts += 1;
                if err.kind() != std::io::ErrorKind::NotFound
                    && err.kind() != std::io::ErrorKind::ConnectionRefused
                {
                    std::thread::sleep(std::time::Duration::from_millis(10));
                } else {
                    std::thread::sleep(std::time::Duration::from_millis(10));
                }
            }
            Err(err) => panic!("failed to connect to {}: {err}", socket_path.display()),
        }
    }
}
