// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: Copyright (c) 2026 Asymptotic Inc.

use std::{
    ffi::OsString,
    sync::{
        atomic::{AtomicBool, AtomicU32, Ordering},
        Arc,
    },
    time::Duration,
};

use pipewire_native::{
    self as pipewire,
    context::Context,
    core::CoreEvents,
    main_loop::MainLoop,
    node::session::memory::{MemoryError, MemoryId, MemoryPoolHandle, RegionRef, ShrinkPolicy},
    properties::Properties,
};
use pipewire_native_server::{
    protocol::spa_data_type,
    runtime::{ScriptedServer, ServerConfig},
    script::{Action, CoreAddMemAction, CoreInfoAction, Expectation, Scenario, ScriptStep},
    testkit,
};
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

fn add_mem_scenario(id: u32, fd_index: i32) -> Scenario {
    let add = Action::SendCoreAddMem(
        CoreAddMemAction::builder()
            .id(id)
            .memory_type(spa_data_type::MEM_FD)
            .fd_index(fd_index)
            .flags(0)
            .size(4096)
            .build(),
    );
    let (first_actions, remove_step) = if fd_index == 0 {
        (
            vec![add, Action::SendCoreDoneFromLastSync],
            Some(
                ScriptStep::builder()
                    .expect(Expectation::CoreSync)
                    .actions(vec![
                        Action::SendCoreRemoveMem { id },
                        Action::SendCoreDoneFromLastSync,
                    ])
                    .build(),
            ),
        )
    } else {
        (
            vec![
                add,
                Action::SendCoreRemoveMem { id },
                Action::SendCoreDoneFromLastSync,
            ],
            None,
        )
    };

    let mut steps = vec![
        ScriptStep::builder()
            .expect(Expectation::CoreHello)
            .actions(vec![Action::SendCoreInfo(
                CoreInfoAction::builder()
                    .cookie(1)
                    .user_name("tester".to_string())
                    .host_name("localhost".to_string())
                    .version("1.0-test".to_string())
                    .name("scripted-add-mem".to_string())
                    .props(vec![])
                    .build(),
            )])
            .build(),
        ScriptStep::builder()
            .expect(Expectation::ClientUpdateProperties)
            .actions(vec![])
            .build(),
        ScriptStep::builder()
            .expect(Expectation::CoreSync)
            .actions(first_actions)
            .build(),
    ];
    steps.extend(remove_step);

    Scenario::builder()
        .steps(steps)
        .name("client-add-mem".to_string())
        .build()
}

fn spawn_server(id: u32, fd_index: i32, socket_path: &std::path::Path) -> testkit::ServerHandle {
    let server = ScriptedServer::builder()
        .config(
            ServerConfig::builder()
                .socket_path(socket_path.to_path_buf())
                .single_client(true)
                .deadline(Duration::from_secs(3))
                .build(),
        )
        .scenario(add_mem_scenario(id, fd_index))
        .build();
    testkit::spawn(server)
}

fn importer_failure_scenario(id: u32, memory_type: u32, duplicate: bool) -> Scenario {
    let add_mem = |id, memory_type| {
        Action::SendCoreAddMem(
            CoreAddMemAction::builder()
                .id(id)
                .memory_type(memory_type)
                .fd_index(0)
                .flags(0)
                .size(4096)
                .build(),
        )
    };
    let mut actions = Vec::new();
    if duplicate {
        actions.push(add_mem(id, spa_data_type::MEM_FD));
    }
    actions.push(add_mem(id, memory_type));
    actions.push(add_mem(id + 1, spa_data_type::MEM_FD));

    Scenario::builder()
        .steps(vec![
            ScriptStep::builder()
                .expect(Expectation::CoreHello)
                .actions(vec![Action::SendCoreInfo(
                    CoreInfoAction::builder()
                        .cookie(1)
                        .user_name("tester".to_string())
                        .host_name("localhost".to_string())
                        .version("1.0-test".to_string())
                        .name("scripted-import-error".to_string())
                        .props(vec![])
                        .build(),
                )])
                .build(),
            ScriptStep::builder()
                .expect(Expectation::ClientUpdateProperties)
                .actions(vec![])
                .build(),
            ScriptStep::builder()
                .expect(Expectation::CoreSync)
                .actions(actions)
                .build(),
        ])
        .name("client-import-error".to_string())
        .build()
}

fn spawn_importer_failure_server(
    id: u32,
    memory_type: u32,
    duplicate: bool,
    socket_path: &std::path::Path,
) -> testkit::ServerHandle {
    let server = ScriptedServer::builder()
        .config(
            ServerConfig::builder()
                .socket_path(socket_path.to_path_buf())
                .single_client(true)
                .deadline(Duration::from_secs(3))
                .build(),
        )
        .scenario(importer_failure_scenario(id, memory_type, duplicate))
        .build();
    testkit::spawn(server)
}

fn new_client(
    socket_path: &std::path::Path,
) -> (RemoteGuard, MainLoop, Context, pipewire::core::Core) {
    let remote = RemoteGuard::set(socket_path);
    let props = Properties::new_vec(vec![("loop.name".to_string(), "pw-main-loop".to_string())]);
    let main_loop = MainLoop::new(&props).unwrap();
    let context = Context::new(&main_loop, Properties::new()).unwrap();
    let core = context.connect(None).unwrap();
    (remote, main_loop, context, core)
}

fn install_timeout(
    main_loop: &MainLoop,
    timed_out: Arc<AtomicBool>,
    duration: Duration,
) -> pipewire::main_loop::Source {
    let main_loop_timer = main_loop.clone();
    let mut timer = main_loop
        .add_timer(Box::new(move |_| {
            timed_out.store(true, Ordering::Relaxed);
            main_loop_timer.quit();
        }))
        .unwrap();
    main_loop
        .update_timer(
            &mut timer,
            &libc::timespec {
                tv_sec: duration.as_secs() as libc::time_t,
                tv_nsec: duration.subsec_nanos() as libc::c_long,
            },
            None,
            false,
        )
        .unwrap();
    timer
}

fn process_has_memfd(id: u32) -> bool {
    let needle = format!("pipewire-native-server-add-mem-{id}");
    std::fs::read_dir("/proc/self/fd")
        .unwrap()
        .filter_map(Result::ok)
        .filter_map(|entry| std::fs::read_link(entry.path()).ok())
        .any(|target| target.to_string_lossy().contains(&needle))
}

#[test]
#[serial]
fn client_imports_indexed_add_mem_from_scripted_server() {
    pipewire::init();
    let deadline = testkit::TestDeadline::after(Duration::from_secs(3));
    let socket_path = testkit::unique_socket_path("pipewire-native-indexed-add-mem");
    let server = spawn_server(55, 0, &socket_path);
    let (_remote, main_loop, _context, core) = new_client(&socket_path);
    let memory = MemoryPoolHandle::install(&core, ShrinkPolicy::Allow);

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
    {
        let _timer = install_timeout(&main_loop, timed_out.clone(), Duration::from_secs(3));
        main_loop.run();
    }

    assert!(!timed_out.load(Ordering::Relaxed));
    assert!(done_seen.load(Ordering::Relaxed));
    assert_eq!(memory.len(), 1);
    let key = memory.resolve(MemoryId(55)).unwrap();
    let mapping = memory
        .bind(
            RegionRef {
                memory: MemoryId(55),
                offset: 0,
                len: 4096,
            },
            true,
        )
        .unwrap();
    assert_eq!(mapping.key(), key);
    assert_eq!(mapping.flags(), 0);

    done_seen.store(false, Ordering::Relaxed);
    expected_seq.store(core.sync().unwrap(), Ordering::Relaxed);
    {
        let _timer = install_timeout(&main_loop, timed_out.clone(), Duration::from_secs(3));
        main_loop.run();
    }

    let report = server.wait(deadline).unwrap();
    assert!(!timed_out.load(Ordering::Relaxed));
    assert!(done_seen.load(Ordering::Relaxed));
    assert_eq!(report.exported_mem_ids, vec![55]);
    assert!(memory.is_empty());
    assert!(matches!(
        memory.resolve(MemoryId(55)),
        Err(MemoryError::UnknownMemory(MemoryId(55)))
    ));
    drop(mapping);
    assert!(!process_has_memfd(55), "removed AddMem descriptor leaked");
}

#[test]
#[serial]
fn wrong_add_mem_fd_index_rejects_import_and_closes_descriptor() {
    pipewire::init();
    let deadline = testkit::TestDeadline::after(Duration::from_secs(3));
    let socket_path = testkit::unique_socket_path("pipewire-native-wrong-add-mem-index");
    let server = spawn_server(56, 1, &socket_path);
    let (_remote, main_loop, _context, core) = new_client(&socket_path);
    let memory = MemoryPoolHandle::install(&core, ShrinkPolicy::Allow);

    core.sync().unwrap();

    let processing_window_elapsed = Arc::new(AtomicBool::new(false));
    let _timer = install_timeout(
        &main_loop,
        processing_window_elapsed.clone(),
        Duration::from_millis(100),
    );
    main_loop.run();

    let report = server.wait(deadline).unwrap();
    assert!(processing_window_elapsed.load(Ordering::Relaxed));
    assert_eq!(report.exported_mem_ids, vec![56]);
    assert!(memory.is_empty());
    assert!(!process_has_memfd(56), "rejected AddMem descriptor leaked");
}

#[test]
#[serial]
fn duplicate_add_mem_closes_candidate_and_later_frame_resources() {
    pipewire::init();
    let deadline = testkit::TestDeadline::after(Duration::from_secs(3));
    let socket_path = testkit::unique_socket_path("pipewire-native-duplicate-add-mem");
    let server = spawn_importer_failure_server(57, spa_data_type::MEM_FD, true, &socket_path);
    let (_remote, main_loop, _context, core) = new_client(&socket_path);
    let memory = MemoryPoolHandle::install(&core, ShrinkPolicy::Allow);

    core.sync().unwrap();
    let processing_window_elapsed = Arc::new(AtomicBool::new(false));
    let _timer = install_timeout(
        &main_loop,
        processing_window_elapsed.clone(),
        Duration::from_millis(100),
    );
    main_loop.run();

    let report = server.wait(deadline).unwrap();
    assert!(processing_window_elapsed.load(Ordering::Relaxed));
    assert_eq!(report.exported_mem_ids, vec![57, 57, 58]);
    assert_eq!(memory.resolve(MemoryId(57)).unwrap().id, MemoryId(57));
    assert!(matches!(
        memory.resolve(MemoryId(58)),
        Err(MemoryError::UnknownMemory(MemoryId(58)))
    ));

    core.disconnect();
    assert!(matches!(
        memory.resolve(MemoryId(57)),
        Err(MemoryError::Disconnected)
    ));
    assert!(!process_has_memfd(57), "duplicate AddMem descriptor leaked");
    assert!(!process_has_memfd(58), "later frame descriptor leaked");
}

#[test]
#[serial]
fn unsupported_add_mem_closes_candidate_and_later_frame_resources() {
    pipewire::init();
    let deadline = testkit::TestDeadline::after(Duration::from_secs(3));
    let socket_path = testkit::unique_socket_path("pipewire-native-unsupported-add-mem");
    let server = spawn_importer_failure_server(59, spa_data_type::DMA_BUF, false, &socket_path);
    let (_remote, main_loop, _context, core) = new_client(&socket_path);
    let memory = MemoryPoolHandle::install(&core, ShrinkPolicy::Allow);

    core.sync().unwrap();
    let processing_window_elapsed = Arc::new(AtomicBool::new(false));
    let _timer = install_timeout(
        &main_loop,
        processing_window_elapsed.clone(),
        Duration::from_millis(100),
    );
    main_loop.run();

    let report = server.wait(deadline).unwrap();
    assert!(processing_window_elapsed.load(Ordering::Relaxed));
    assert_eq!(report.exported_mem_ids, vec![59, 60]);
    assert!(memory.is_empty());

    core.disconnect();
    assert!(matches!(
        memory.resolve(MemoryId(59)),
        Err(MemoryError::Disconnected)
    ));
    assert!(
        !process_has_memfd(59),
        "unsupported AddMem descriptor leaked"
    );
    assert!(!process_has_memfd(60), "later frame descriptor leaked");
}
