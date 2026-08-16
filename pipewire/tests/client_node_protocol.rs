use std::{ffi::OsString, sync::mpsc, time::Duration};

use pipewire_native::{
    self as pipewire,
    context::Context,
    main_loop::MainLoop,
    properties::Properties,
    proxy::{HasProxy, ProxyEvents},
};
use pipewire_native_protocol::wire::client_node::{
    BufferDescriptor, Command, DataDescriptor, Direction, Event, MetaDescriptor, NodeInfo,
    PortInfo, PortSetIo, PortSetParam, PortUpdate, PortUseBuffers, RegionRef, SetActive, Update,
};
use pipewire_native_server::{
    runtime::{ScriptedServer, ServerConfig},
    script::{Action, CoreErrorAction, CoreInfoAction, Expectation, Scenario, ScriptStep},
    testkit,
};
use pipewire_native_spa::pod::RawPodOwned;
use serial_test::serial;

struct RemoteGuard(Option<OsString>);

impl RemoteGuard {
    fn set(path: &std::path::Path) -> Self {
        let previous = std::env::var_os("PIPEWIRE_REMOTE");
        unsafe { std::env::set_var("PIPEWIRE_REMOTE", path) };
        Self(previous)
    }
}

impl Drop for RemoteGuard {
    fn drop(&mut self) {
        if let Some(remote) = self.0.take() {
            unsafe { std::env::set_var("PIPEWIRE_REMOTE", remote) };
        } else {
            unsafe { std::env::remove_var("PIPEWIRE_REMOTE") };
        }
    }
}

#[test]
#[serial]
fn typed_client_node_full_wire_transcript_reaches_scripted_peer() {
    pipewire::init();
    let deadline = testkit::TestDeadline::after(Duration::from_secs(3));
    let socket_path = testkit::unique_socket_path("pipewire-native-client-node-wire");
    let format = RawPodOwned::wrap(upstream_fixture("format-s16le-48k-stereo")).unwrap();
    let scenario = Scenario::builder()
        .steps(vec![
            ScriptStep::builder()
                .expect(Expectation::CoreHello)
                .actions(vec![Action::SendCoreInfo(
                    CoreInfoAction::builder()
                        .cookie(1)
                        .user_name("tester".into())
                        .host_name("localhost".into())
                        .version("1.0-test".into())
                        .name("client-node-wire".into())
                        .props(vec![])
                        .build(),
                )])
                .build(),
            ScriptStep::builder()
                .expect(Expectation::ClientUpdateProperties)
                .actions(vec![])
                .build(),
            ScriptStep::builder()
                .expect(Expectation::ClientNodeCreate)
                .actions(vec![])
                .build(),
            ScriptStep::builder()
                .expect(Expectation::ClientNodeUpdate {
                    info: true,
                    min_params: 1,
                })
                .actions(vec![])
                .build(),
            ScriptStep::builder()
                .expect(Expectation::ClientNodePortUpdate {
                    direction: Direction::Output,
                    port_id: 0,
                    info: true,
                    min_params: 1,
                })
                .actions(vec![])
                .build(),
            ScriptStep::builder()
                .expect(Expectation::ClientNodeSetActive { active: true })
                .actions(vec![
                    Action::SendClientNodeTransport {
                        trigger_index: 0,
                        completion_index: 1,
                        activation: RegionRef {
                            memory_id: 4,
                            offset: 0,
                            size: 2312,
                        },
                    },
                    Action::SendClientNodePortSetParam(PortSetParam {
                        direction: Direction::Output,
                        port_id: 0,
                        param_id: 4,
                        flags: 0,
                        param: Some(format.clone()),
                    }),
                    Action::SendClientNodePortUseBuffers(PortUseBuffers {
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
                    }),
                    Action::SendClientNodePortSetIo(PortSetIo::Set {
                        direction: Direction::Output,
                        port_id: 0,
                        mix_id: None,
                        io_id: 1,
                        region: RegionRef {
                            memory_id: 13,
                            offset: 8,
                            size: 8,
                        },
                    }),
                    Action::SendClientNodeSetActivation {
                        node_id: 55,
                        activation: Some(RegionRef {
                            memory_id: 8,
                            offset: 16,
                            size: 2312,
                        }),
                    },
                    Action::SendClientNodeCommand(Command::Start),
                ])
                .build(),
            ScriptStep::builder()
                .expect(Expectation::ClientNodeSetActive { active: false })
                .actions(vec![])
                .build(),
            ScriptStep::builder()
                .expect(Expectation::ClientNodeUpdate {
                    info: false,
                    min_params: 0,
                })
                .actions(vec![])
                .build(),
            ScriptStep::builder()
                .expect(Expectation::ClientNodePortUpdate {
                    direction: Direction::Output,
                    port_id: 0,
                    info: false,
                    min_params: 0,
                })
                .actions(vec![])
                .build(),
            ScriptStep::builder()
                .expect(Expectation::Exact {
                    object_id: 0,
                    opcode: 7,
                })
                .actions(vec![Action::SendCoreError(
                    CoreErrorAction::builder()
                        .id(0)
                        .seq(0)
                        .res(-libc::EPIPE)
                        .message("scenario complete".into())
                        .build(),
                )])
                .build(),
        ])
        .name("typed-client-node-wire".into())
        .build();
    let server = ScriptedServer::builder()
        .config(
            ServerConfig::builder()
                .socket_path(socket_path.clone())
                .single_client(true)
                .deadline(Duration::from_secs(3))
                .build(),
        )
        .scenario(scenario)
        .build();
    let server = testkit::spawn(server);

    let _remote = RemoteGuard::set(&socket_path);
    let main_loop = MainLoop::new(&Properties::new()).unwrap();
    let context = Context::new(&main_loop, Properties::new()).unwrap();
    let core = context.connect(None).unwrap();
    let quit_loop = main_loop.clone();
    core.proxy().add_listener(ProxyEvents {
        error: Some(Box::new(move |_, _, _| quit_loop.quit())),
        ..Default::default()
    });

    let node = core.create_client_node(&Properties::new()).unwrap();
    assert_eq!(node.version(), 6);
    let (events, received_event) = mpsc::channel();
    let teardown_node = node.clone();
    let teardown_core = core.clone();
    let mut event_count = 0;
    node.set_event_handler(Some(Box::new(move |event| {
        events.send(event).unwrap();
        event_count += 1;
        if event_count == 6 {
            teardown_node.set_active(false).unwrap();
            teardown_node
                .update(Update {
                    change_mask: 0,
                    params: vec![],
                    info: None,
                })
                .unwrap();
            teardown_node
                .port_update(PortUpdate {
                    direction: Direction::Output,
                    port_id: 0,
                    change_mask: 0,
                    params: vec![],
                    info: None,
                })
                .unwrap();
            teardown_core.destroy(&teardown_node).unwrap();
        }
    })));
    node.update(Update {
        change_mask: 1,
        params: vec![format.clone()],
        info: Some(NodeInfo {
            max_input_ports: 0,
            max_output_ports: 1,
            change_mask: 0,
            flags: 0,
            properties: vec![],
            params: vec![],
        }),
    })
    .unwrap();
    node.port_update(PortUpdate {
        direction: Direction::Output,
        port_id: 0,
        change_mask: 1,
        params: vec![format],
        info: Some(PortInfo {
            change_mask: 0,
            flags: 0,
            rate_num: 1,
            rate_denom: 48_000,
            properties: vec![],
            params: vec![],
        }),
    })
    .unwrap();
    node.send(
        pipewire_native_protocol::wire::client_node::Method::SetActive(SetActive { active: true }),
    )
    .unwrap();

    main_loop.run();
    let report = server.wait(deadline).unwrap();
    assert_eq!(report.completed_steps, 10);
    assert_eq!(report.live_object_routes, 0);
    let received: Vec<_> = (0..6)
        .map(|_| received_event.recv_timeout(Duration::from_secs(1)).unwrap())
        .collect();
    assert!(matches!(received[0], Event::Transport(_)));
    assert!(matches!(received[1], Event::PortSetParam(_)));
    assert!(matches!(received[2], Event::PortUseBuffers(_)));
    assert!(matches!(received[3], Event::PortSetIo(_)));
    assert!(matches!(received[4], Event::SetActivation(_)));
    assert!(matches!(received[5], Event::Command(Command::Start)));
}

fn upstream_fixture(name: &str) -> Vec<u8> {
    let line = include_str!("../../protocol/tests/fixtures/client-node-v6/upstream.hex")
        .lines()
        .find(|line| line.starts_with(&format!("{name} ")))
        .unwrap();
    line.split_once(' ')
        .unwrap()
        .1
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
        .collect()
}
