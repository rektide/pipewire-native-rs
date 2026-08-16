use std::{
    io::{pipe, Read},
    os::fd::{AsFd, OwnedFd},
    os::unix::net::UnixStream,
};

use pipewire_native_protocol::{
    native::frame::{
        FrameFds, FrameLimits, FrameReceiver, FrameSender, OutboundFrame, ReceiveOutcome,
    },
    wire::client_node::{
        self as wire, BufferDescriptor, Command, DataDescriptor, Direction, Event, Limits,
        MetaDescriptor, Method, NodeInfo, ParamInfo, PortInfo, PortSetIo, PortSetParam, PortUpdate,
        PortUseBuffers, RegionRef, SetActive, Update,
    },
};
use pipewire_native_spa::pod::{types::Type, RawPodOwned};

#[test]
fn canonical_interface_and_shared_state_values_match_v6() {
    assert_eq!(wire::FACTORY_NAME, "client-node");
    assert_eq!(wire::INTERFACE, "PipeWire:Interface:ClientNode");
    assert_eq!(wire::INTERFACE_VERSION, 6);
    assert_eq!(wire::ACTIVATION_VERSION, 1);
    assert_eq!(wire::ActivationStatus::NotTriggered as u32, 0);
    assert_eq!(wire::ActivationStatus::Triggered as u32, 1);
    assert_eq!(wire::ActivationStatus::Awake as u32, 2);
    assert_eq!(wire::ActivationStatus::Finished as u32, 3);
    assert_eq!(wire::ActivationStatus::Inactive as u32, 4);
    assert_eq!(wire::PortIoType::Buffers as u32, 1);
    assert_eq!(wire::BufferStatus::NeedData as i32, 1);
    assert_eq!(wire::BufferStatus::HaveData as i32, 2);
}

fn raw_object(type_id: u32, object_id: u32) -> RawPodOwned {
    let mut bytes = vec![0_u8; 16];
    bytes[0..4].copy_from_slice(&8_u32.to_ne_bytes());
    bytes[4..8].copy_from_slice(&(Type::Object as u32).to_ne_bytes());
    bytes[8..12].copy_from_slice(&type_id.to_ne_bytes());
    bytes[12..16].copy_from_slice(&object_id.to_ne_bytes());
    RawPodOwned::wrap(bytes).unwrap()
}

fn frame_fds(fds: Vec<OwnedFd>) -> FrameFds {
    let (tx, rx) = UnixStream::pair().unwrap();
    let limits = FrameLimits::default();
    let mut sender = FrameSender::new(limits);
    sender
        .enqueue(OutboundFrame::new(7, 0, 0, Vec::new(), fds, limits).unwrap())
        .unwrap();
    sender.flush(tx.as_fd()).unwrap();
    let mut receiver = FrameReceiver::new(limits);
    let ReceiveOutcome::Frame(frame) = receiver.receive(rx.as_fd()).unwrap() else {
        panic!("expected frame");
    };
    frame.into_parts().2
}

fn pipe_fd() -> (impl Read, OwnedFd) {
    let (reader, writer) = pipe().unwrap();
    (reader, writer.into())
}

#[test]
fn selected_methods_round_trip_and_set_active_matches_pinned_bytes() {
    let format = raw_object(0x40003, 4);
    let methods = [
        Method::Update(Update {
            change_mask: 3,
            params: vec![format.clone()],
            info: Some(NodeInfo {
                max_input_ports: 0,
                max_output_ports: 1,
                change_mask: 9,
                flags: 2,
                properties: vec![("media.class".into(), "Audio/Source".into())],
                params: vec![ParamInfo { id: 3, flags: 2 }],
            }),
        }),
        Method::PortUpdate(PortUpdate {
            direction: Direction::Output,
            port_id: 0,
            change_mask: 7,
            params: vec![format],
            info: Some(PortInfo {
                change_mask: 5,
                flags: 0,
                rate_num: 1,
                rate_denom: 48_000,
                properties: vec![],
                params: vec![ParamInfo { id: 4, flags: 2 }],
            }),
        }),
        Method::SetActive(SetActive { active: true }),
    ];

    for method in methods {
        let payload = wire::encode_method(&method).unwrap();
        let decoded = wire::decode_method(method.opcode(), &payload, Limits::default()).unwrap();
        assert_eq!(decoded.opcode(), method.opcode());
        assert_eq!(wire::encode_method(&decoded).unwrap(), payload);
    }

    assert_eq!(
        wire::encode_method(&Method::SetActive(SetActive { active: true })).unwrap(),
        [0x10, 0, 0, 0, 0x0e, 0, 0, 0, 4, 0, 0, 0, 2, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0,]
    );
}

#[test]
fn selected_non_fd_events_round_trip_with_clear_forms() {
    let format = raw_object(0x40003, 4);
    let events = [
        (
            wire::event::PORT_SET_PARAM,
            wire::encode_port_set_param(&PortSetParam {
                direction: Direction::Output,
                port_id: 0,
                param_id: 4,
                flags: 0,
                param: Some(format),
            })
            .unwrap(),
        ),
        (
            wire::event::PORT_SET_PARAM,
            wire::encode_port_set_param(&PortSetParam {
                direction: Direction::Output,
                port_id: 0,
                param_id: 4,
                flags: 0,
                param: None,
            })
            .unwrap(),
        ),
        (
            wire::event::PORT_USE_BUFFERS,
            wire::encode_port_use_buffers(&PortUseBuffers {
                direction: Direction::Output,
                port_id: 0,
                mix_id: None,
                flags: 0,
                buffers: vec![BufferDescriptor {
                    metadata: RegionRef {
                        memory_id: 11,
                        offset: 64,
                        size: 128,
                    },
                    metas: vec![MetaDescriptor {
                        type_id: 1,
                        size: 16,
                    }],
                    datas: vec![DataDescriptor {
                        type_id: 3,
                        data_id: 12,
                        flags: 1,
                        map_offset: 32,
                        max_size: 4096,
                    }],
                }],
            })
            .unwrap(),
        ),
        (
            wire::event::PORT_USE_BUFFERS,
            wire::encode_port_use_buffers(&PortUseBuffers {
                direction: Direction::Output,
                port_id: 0,
                mix_id: None,
                flags: 0,
                buffers: vec![],
            })
            .unwrap(),
        ),
        (
            wire::event::PORT_SET_IO,
            wire::encode_port_set_io(PortSetIo::Set {
                direction: Direction::Output,
                port_id: 0,
                mix_id: None,
                io_id: 1,
                region: RegionRef {
                    memory_id: 13,
                    offset: 8,
                    size: 8,
                },
            })
            .unwrap(),
        ),
        (
            wire::event::PORT_SET_IO,
            wire::encode_port_set_io(PortSetIo::Clear {
                direction: Direction::Output,
                port_id: 0,
                mix_id: None,
                io_id: 1,
            })
            .unwrap(),
        ),
        (
            wire::event::COMMAND,
            wire::encode_command(Command::Start).unwrap(),
        ),
        (
            wire::event::COMMAND,
            wire::encode_command(Command::Pause).unwrap(),
        ),
    ];

    for (opcode, payload) in events {
        let mut fds = frame_fds(vec![]);
        wire::decode_event(opcode, &payload, &mut fds, Limits::default()).unwrap();
    }
}

#[test]
fn descriptor_events_resolve_exact_frame_local_indices_and_clear_without_fd() {
    let (mut trigger_reader, trigger_writer) = pipe_fd();
    let (mut completion_reader, completion_writer) = pipe_fd();
    let payload = wire::encode_transport(
        1,
        0,
        RegionRef {
            memory_id: 4,
            offset: 0,
            size: 2312,
        },
    )
    .unwrap();
    let mut fds = frame_fds(vec![completion_writer, trigger_writer]);
    let Event::Transport(transport) = wire::decode_event(
        wire::event::TRANSPORT,
        &payload,
        &mut fds,
        Limits::default(),
    )
    .unwrap() else {
        panic!("wrong event");
    };
    drop(transport);
    let mut byte = [0];
    assert_eq!(trigger_reader.read(&mut byte).unwrap(), 0);
    assert_eq!(completion_reader.read(&mut byte).unwrap(), 0);

    let (mut signal_reader, signal_writer) = pipe_fd();
    let payload = wire::encode_set_activation(
        55,
        Some((
            0,
            RegionRef {
                memory_id: 8,
                offset: 16,
                size: 2312,
            },
        )),
    )
    .unwrap();
    let mut fds = frame_fds(vec![signal_writer]);
    let Event::SetActivation(wire::SetActivation::Set { signal_fd, .. }) = wire::decode_event(
        wire::event::SET_ACTIVATION,
        &payload,
        &mut fds,
        Limits::default(),
    )
    .unwrap() else {
        panic!("wrong activation event");
    };
    drop(signal_fd);
    assert_eq!(signal_reader.read(&mut byte).unwrap(), 0);

    let payload = wire::encode_set_activation(55, None).unwrap();
    let mut fds = frame_fds(vec![]);
    assert!(matches!(
        wire::decode_event(
            wire::event::SET_ACTIVATION,
            &payload,
            &mut fds,
            Limits::default()
        )
        .unwrap(),
        Event::SetActivation(wire::SetActivation::Remove { node_id: 55 })
    ));
}

#[test]
fn hostile_shapes_counts_types_ranges_and_descriptor_tables_fail() {
    let set_active = wire::encode_method(&Method::SetActive(SetActive { active: true })).unwrap();
    for end in 0..set_active.len() {
        assert!(wire::decode_method(
            wire::method::SET_ACTIVE,
            &set_active[..end],
            Limits::default()
        )
        .is_err());
    }
    let mut trailing = set_active.clone();
    trailing.extend_from_slice(&[0; 8]);
    assert!(wire::decode_method(wire::method::SET_ACTIVE, &trailing, Limits::default()).is_err());

    let mut buffers = wire::encode_port_use_buffers(&PortUseBuffers {
        direction: Direction::Output,
        port_id: 0,
        mix_id: None,
        flags: 0,
        buffers: vec![],
    })
    .unwrap();
    buffers[80..84].copy_from_slice(&65_i32.to_ne_bytes());
    assert!(wire::decode_event(
        wire::event::PORT_USE_BUFFERS,
        &buffers,
        &mut frame_fds(vec![]),
        Limits::default()
    )
    .is_err());

    let mut wrong_param = wire::encode_port_set_param(&PortSetParam {
        direction: Direction::Output,
        port_id: 0,
        param_id: wire::SPA_PARAM_FORMAT,
        flags: 0,
        param: None,
    })
    .unwrap();
    wrong_param[48..52].copy_from_slice(&3_u32.to_ne_bytes());
    assert!(wire::decode_event(
        wire::event::PORT_SET_PARAM,
        &wrong_param,
        &mut frame_fds(vec![]),
        Limits::default()
    )
    .is_err());

    let mut payload = wire::encode_transport(
        0,
        1,
        RegionRef {
            memory_id: 4,
            offset: 0,
            size: 8,
        },
    )
    .unwrap();
    payload[64..68].copy_from_slice(&(u32::MAX - 2).to_ne_bytes());
    let (_, one) = pipe_fd();
    let (_, two) = pipe_fd();
    assert!(wire::decode_event(
        wire::event::TRANSPORT,
        &payload,
        &mut frame_fds(vec![one, two]),
        Limits::default()
    )
    .is_err());

    for carried in [1, 3] {
        let mut owned = Vec::new();
        let mut readers = Vec::new();
        for _ in 0..carried {
            let (reader, writer) = pipe_fd();
            readers.push(reader);
            owned.push(writer);
        }
        let mut fds = frame_fds(owned);
        assert!(wire::decode_event(
            wire::event::TRANSPORT,
            &wire::encode_transport(
                0,
                1,
                RegionRef {
                    memory_id: 4,
                    offset: 0,
                    size: 2312
                }
            )
            .unwrap(),
            &mut fds,
            Limits::default()
        )
        .is_err());
        drop(fds);
        for mut reader in readers {
            assert_eq!(reader.read(&mut [0]).unwrap(), 0);
        }
    }

    let (_, one) = pipe_fd();
    let (_, two) = pipe_fd();
    assert!(wire::decode_event(
        wire::event::TRANSPORT,
        &wire::encode_transport(
            0,
            0,
            RegionRef {
                memory_id: 4,
                offset: 0,
                size: 2312
            }
        )
        .unwrap(),
        &mut frame_fds(vec![one, two]),
        Limits::default()
    )
    .is_err());

    let (_, one) = pipe_fd();
    let (_, two) = pipe_fd();
    assert!(wire::decode_event(
        wire::event::TRANSPORT,
        &wire::encode_transport(
            0,
            2,
            RegionRef {
                memory_id: 4,
                offset: 0,
                size: 2312,
            },
        )
        .unwrap(),
        &mut frame_fds(vec![one, two]),
        Limits::default(),
    )
    .is_err());

    let (_, unexpected) = pipe_fd();
    assert!(wire::decode_event(
        wire::event::COMMAND,
        &wire::encode_command(Command::Start).unwrap(),
        &mut frame_fds(vec![unexpected]),
        Limits::default(),
    )
    .is_err());
}

#[test]
fn unknown_event_is_frame_local_and_drops_its_descriptors() {
    let (mut reader, writer) = pipe_fd();
    let mut fds = frame_fds(vec![writer]);
    let error = wire::decode_event(255, &[0xff], &mut fds, Limits::default()).unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::Unsupported);
    drop(fds);
    assert_eq!(reader.read(&mut [0]).unwrap(), 0);

    let payload = wire::encode_command(Command::Start).unwrap();
    let mut next_fds = frame_fds(vec![]);
    assert!(matches!(
        wire::decode_event(
            wire::event::COMMAND,
            &payload,
            &mut next_fds,
            Limits::default()
        )
        .unwrap(),
        Event::Command(Command::Start)
    ));
}
