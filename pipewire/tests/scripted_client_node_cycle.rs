// SPDX-License-Identifier: MIT

use std::{
    collections::VecDeque,
    ffi::OsString,
    sync::Mutex,
    time::{Duration, Instant},
};

use pipewire_native::{
    self as pipewire,
    context::Context,
    main_loop::MainLoop,
    node::session::memory::{MemoryError, MemoryId, MemoryPoolHandle, ShrinkPolicy},
    properties::Properties,
};
use pipewire_native_node::{
    runtime::RuntimeClock,
    session::{
        cycle::{CommittedOutput, OutputCycle},
        output::{OutputProcess, ProcessError},
    },
};
use pipewire_native_protocol::wire::client_node::{
    BufferDescriptor, Command, DataDescriptor, Direction, MetaDescriptor, NodeInfo, PortInfo,
    PortSetIo, PortSetParam, PortUpdate, PortUseBuffers, RegionRef, Update, SPA_IO_BUFFERS,
    SPA_PARAM_FORMAT,
};
use pipewire_native_server::{
    runtime::{ScriptedServer, ServerConfig},
    script::{Action, CoreInfoAction, Expectation, Scenario, ScriptStep},
    testkit::{
        self,
        client_node::{ClientNodeFixture, MemoryRole, PCM_BYTES, PEER_NODE_ID},
    },
};
use pipewire_native_spa::{buffer::data_type, pod::RawPodOwned};
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

struct FixedClock(Mutex<VecDeque<u64>>);

impl FixedClock {
    fn new() -> Self {
        Self(Mutex::new(VecDeque::from([100, 140, 200, 240])))
    }
}

impl RuntimeClock for FixedClock {
    fn monotonic_ns(&mut self) -> u64 {
        self.0.lock().unwrap().pop_front().unwrap_or(240)
    }
}

struct FixedOutput {
    fixture: ClientNodeFixture,
}

impl OutputProcess for FixedOutput {
    fn process(&mut self, mut cycle: OutputCycle<'_>) -> Result<CommittedOutput, ProcessError> {
        self.fixture.before_callback();
        assert_eq!(cycle.buffer_id(), 1);
        assert_eq!(cycle.format().rate.get(), 48_000);
        assert_eq!(cycle.format().channels.len(), 2);
        cycle.interleaved_pcm()[..PCM_BYTES.len()].copy_from_slice(&PCM_BYTES);
        cycle.commit(4).map_err(|error| ProcessError {
            message: error.to_string(),
        })
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn linked_tokio_client_node_cycle_is_process_correct_and_generation_safe() {
    pipewire::init();
    let fd_baseline = fd_count();
    let deadline = testkit::TestDeadline::after(Duration::from_secs(8));
    let socket_path = testkit::unique_socket_path("pipewire-native-client-node-cycle");
    let fixture = ClientNodeFixture::new("pipewire-native-client-node-cycle").unwrap();
    let format_bytes = upstream_fixture("format-s16le-48k-stereo");
    let format = RawPodOwned::wrap(format_bytes.clone()).unwrap();
    let node_info = NodeInfo {
        max_input_ports: 0,
        max_output_ports: 1,
        change_mask: 0,
        flags: 0,
        properties: vec![],
        params: vec![],
    };
    let port_info = PortInfo {
        change_mask: 0,
        flags: 0,
        rate_num: 1,
        rate_denom: 48_000,
        properties: vec![],
        params: vec![],
    };

    let fixture_memories = [
        MemoryRole::OwnActivation,
        MemoryRole::Metadata,
        MemoryRole::Media,
        MemoryRole::Io,
        MemoryRole::PeerActivation,
    ];
    let memory_action = |role| Action::SendClientNodeFixtureMemory {
        fixture: fixture.clone(),
        role,
    };
    let scenario = Scenario::builder()
        .name("linked-tokio-client-node-cycle".into())
        .steps(vec![
            ScriptStep::builder()
                .name("hello".into())
                .expect(Expectation::CoreHello)
                .actions(vec![Action::SendCoreInfo(
                    CoreInfoAction::builder()
                        .cookie(1)
                        .user_name("tester".into())
                        .host_name("localhost".into())
                        .version("1.0-test".into())
                        .name("linked-cycle".into())
                        .props(vec![])
                        .build(),
                )])
                .build(),
            ScriptStep::builder()
                .name("client-properties".into())
                .expect(Expectation::ClientUpdateProperties)
                .actions(vec![])
                .build(),
            ScriptStep::builder()
                .name("create-v6".into())
                .expect(Expectation::ClientNodeCreate)
                .actions(vec![])
                .build(),
            ScriptStep::builder()
                .name("exact-node-update".into())
                .expect(Expectation::ClientNodeUpdateExact {
                    change_mask: 1,
                    param: format_bytes.clone(),
                    info: node_info.clone(),
                })
                .actions(vec![])
                .build(),
            ScriptStep::builder()
                .name("exact-port-update-and-configuration".into())
                .expect(Expectation::ClientNodePortUpdateExact {
                    change_mask: 1,
                    param: format_bytes,
                    info: port_info.clone(),
                })
                .actions(vec![
                    memory_action(MemoryRole::OwnActivation),
                    Action::SendClientNodeFixtureTransport(fixture.clone()),
                    Action::SendClientNodePortSetParam(PortSetParam {
                        direction: Direction::Output,
                        port_id: 0,
                        param_id: SPA_PARAM_FORMAT,
                        flags: 0,
                        param: Some(format.clone()),
                    }),
                    memory_action(MemoryRole::Metadata),
                    memory_action(MemoryRole::Media),
                    memory_action(MemoryRole::Io),
                    Action::SendClientNodePortUseBuffers(buffer_descriptors()),
                    Action::SendClientNodePortSetIo(PortSetIo::Set {
                        direction: Direction::Output,
                        port_id: 0,
                        mix_id: None,
                        io_id: SPA_IO_BUFFERS,
                        region: RegionRef {
                            memory_id: MemoryRole::Io.id(),
                            offset: 0,
                            size: 8,
                        },
                    }),
                    memory_action(MemoryRole::PeerActivation),
                    Action::SendClientNodeFixturePeerActivation(fixture.clone()),
                    Action::SendClientNodeCommand(Command::Start),
                ])
                .build(),
            ScriptStep::builder()
                .name("active-cycle-race-and-teardown".into())
                .expect(Expectation::ClientNodeSetActive { active: true })
                .actions(vec![
                    Action::ProveClientNodeFixtureCycle(fixture.clone()),
                    Action::ProveClientNodeFixtureRemovalRace(fixture.clone()),
                    Action::SendClientNodeSetActivation {
                        node_id: PEER_NODE_ID,
                        activation: None,
                    },
                    Action::SendClientNodePortSetIo(PortSetIo::Clear {
                        direction: Direction::Output,
                        port_id: 0,
                        mix_id: None,
                        io_id: SPA_IO_BUFFERS,
                    }),
                    Action::SendClientNodePortUseBuffers(PortUseBuffers {
                        direction: Direction::Output,
                        port_id: 0,
                        mix_id: None,
                        flags: 0,
                        buffers: vec![],
                    }),
                    Action::SendClientNodePortSetParam(PortSetParam {
                        direction: Direction::Output,
                        port_id: 0,
                        param_id: SPA_PARAM_FORMAT,
                        flags: 0,
                        param: None,
                    }),
                    Action::SendClientNodeCommand(Command::Pause),
                    Action::SendCoreRemoveMem {
                        id: MemoryRole::Metadata.id(),
                    },
                    Action::SendCoreRemoveMem {
                        id: MemoryRole::Io.id(),
                    },
                    Action::SendCoreRemoveMem {
                        id: MemoryRole::PeerActivation.id(),
                    },
                    Action::SendCoreRemoveMem {
                        id: MemoryRole::OwnActivation.id(),
                    },
                    Action::MarkClientNodeFixtureTeardown(fixture.clone()),
                ])
                .build(),
            ScriptStep::builder()
                .name("inactive".into())
                .expect(Expectation::ClientNodeSetActive { active: false })
                .actions(vec![])
                .build(),
            ScriptStep::builder()
                .name("destroy".into())
                .expect(Expectation::Exact {
                    object_id: 0,
                    opcode: 7,
                })
                .actions(vec![
                    Action::ReleaseClientNodeFixture(fixture.clone()),
                    Action::CloseConnection,
                ])
                .build(),
        ])
        .build();
    let server = testkit::spawn(
        ScriptedServer::builder()
            .config(
                ServerConfig::builder()
                    .socket_path(socket_path.clone())
                    .single_client(true)
                    .deadline(Duration::from_secs(8))
                    .build(),
            )
            .scenario(scenario)
            .build(),
    );

    let remote = RemoteGuard::set(&socket_path);
    let main_loop = MainLoop::new(&Properties::new()).unwrap();
    let context = Context::new(&main_loop, Properties::new()).unwrap();
    let core = context.connect(None).unwrap();
    let memory = MemoryPoolHandle::install(&core, ShrinkPolicy::RequireSealed);
    let bridge = core
        .create_tokio_client_node_session_with_clock(
            &Properties::new(),
            memory.clone(),
            Box::new(FixedOutput {
                fixture: fixture.clone(),
            }),
            FixedClock::new(),
        )
        .unwrap();
    bridge
        .advertise_output(
            Update {
                change_mask: 1,
                params: vec![format.clone()],
                info: Some(node_info),
            },
            PortUpdate {
                direction: Direction::Output,
                port_id: 0,
                change_mask: 1,
                params: vec![format],
                info: Some(port_info),
            },
        )
        .unwrap();
    bridge.set_active(true).unwrap();

    let client_deadline = Instant::now() + Duration::from_secs(8);
    while !fixture.teardown_sent() || !memory.is_empty() {
        if server.is_finished() {
            panic!("server failed before teardown: {:?}", server.wait(deadline));
        }
        assert!(
            Instant::now() < client_deadline,
            "client-side cycle deadline elapsed"
        );
        main_loop.iterate(Some(Duration::from_millis(2))).unwrap();
        let diagnostics = bridge.diagnostics();
        fixture.report_diagnostics(diagnostics.missed_wakes, diagnostics.stale_wakes);
        if matches!(
            memory.resolve(MemoryId(MemoryRole::Media.id())),
            Err(MemoryError::UnknownMemory(_))
        ) {
            fixture.report_media_removed();
        }
        tokio::task::yield_now().await;
    }

    bridge.set_active(false).unwrap();
    for _ in 0..10 {
        main_loop.iterate(Some(Duration::from_millis(1))).unwrap();
        tokio::task::yield_now().await;
    }
    bridge.shutdown_and_destroy(&core).await.unwrap();
    let completion_deadline = Instant::now() + Duration::from_secs(2);
    while !server.is_finished() && Instant::now() < completion_deadline {
        main_loop.iterate(Some(Duration::from_millis(1))).unwrap();
        tokio::task::yield_now().await;
    }
    let report = server.wait(deadline).unwrap();
    assert_eq!(report.completed_steps, 8);
    assert_eq!(
        report.exported_mem_ids,
        fixture_memories.map(MemoryRole::id)
    );
    assert_eq!(report.live_object_routes, 0);

    let snapshot = fixture.snapshot().expect("cycle proof snapshot");
    assert_eq!(snapshot.callbacks, 2);
    assert_eq!(snapshot.awake_time, 100);
    assert_eq!(snapshot.finish_time, 140);
    assert!(snapshot.missed_wakes >= 2 || snapshot.stale_wakes >= 1);
    assert_eq!(snapshot.peer_event_count, 1);
    assert_eq!(snapshot.completion_event_count, 0);
    assert!(memory.is_empty());

    core.disconnect();
    drop(memory);
    drop(core);
    drop(context);
    drop(main_loop);
    drop(remote);
    fixture.release_resources();
    assert!(!has_named_memfd("pipewire-native-client-node-cycle"));
    let final_fds = fd_count();
    assert_eq!(
        final_fds,
        fd_baseline,
        "descriptor leak: {:?}",
        fd_targets()
    );
}

fn buffer_descriptors() -> PortUseBuffers {
    PortUseBuffers {
        direction: Direction::Output,
        port_id: 0,
        mix_id: None,
        flags: 0,
        buffers: (0..2)
            .map(|index| BufferDescriptor {
                metadata: RegionRef {
                    memory_id: MemoryRole::Metadata.id(),
                    offset: index * 16,
                    size: 16,
                },
                metas: Vec::<MetaDescriptor>::new(),
                datas: vec![DataDescriptor {
                    type_id: data_type::MEM_ID,
                    data_id: MemoryRole::Media.id(),
                    flags: 0,
                    map_offset: index * 64,
                    max_size: 64,
                }],
            })
            .collect(),
    }
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

fn fd_count() -> usize {
    std::fs::read_dir("/proc/self/fd").unwrap().count()
}

fn fd_targets() -> Vec<String> {
    std::fs::read_dir("/proc/self/fd")
        .unwrap()
        .filter_map(Result::ok)
        .map(|entry| {
            format!(
                "{} -> {}",
                entry.file_name().to_string_lossy(),
                std::fs::read_link(entry.path())
                    .map(|path| path.to_string_lossy().into_owned())
                    .unwrap_or_else(|error| error.to_string())
            )
        })
        .collect()
}

fn has_named_memfd(name: &str) -> bool {
    std::fs::read_dir("/proc/self/fd")
        .unwrap()
        .filter_map(Result::ok)
        .filter_map(|entry| std::fs::read_link(entry.path()).ok())
        .any(|target| target.to_string_lossy().contains(name))
}
