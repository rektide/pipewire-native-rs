// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: Copyright (c) 2026 Asymptotic Inc.

use std::{
    ffi::OsString,
    os::fd::OwnedFd,
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
use pipewire_native_server::{
    protocol::spa_data_type,
    runtime::{ScriptedServer, ServerConfig},
    script::{Action, CoreAddMemAction, CoreInfoAction, Expectation, Scenario, ScriptStep},
    testkit,
};
use serial_test::serial;

type ImportedMemory = Arc<Mutex<Vec<(u32, u32, OwnedFd, u32)>>>;
type ObservedMemory = Arc<Mutex<Vec<(u32, u32, u32)>>>;

struct Importer {
    memory: ImportedMemory,
    observed: ObservedMemory,
    added: Arc<AtomicU32>,
    removed: Arc<Mutex<Vec<u32>>>,
}

impl CoreMemoryImporter for Importer {
    fn add_memory(&mut self, id: u32, type_: u32, fd: OwnedFd, flags: u32) -> std::io::Result<()> {
        self.added.fetch_add(1, Ordering::Relaxed);
        self.observed.lock().unwrap().push((id, type_, flags));
        self.memory.lock().unwrap().push((id, type_, fd, flags));
        Ok(())
    }

    fn remove_memory(&mut self, id: u32) -> std::io::Result<()> {
        self.memory.lock().unwrap().retain(|entry| entry.0 != id);
        self.removed.lock().unwrap().push(id);
        Ok(())
    }
}

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
    let actions = vec![
        Action::SendCoreAddMem(
            CoreAddMemAction::builder()
                .id(id)
                .memory_type(spa_data_type::MEM_FD)
                .fd_index(fd_index)
                .flags(0)
                .size(4096)
                .build(),
        ),
        Action::SendCoreRemoveMem { id },
        Action::SendCoreDoneFromLastSync,
    ];

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
                .actions(actions)
                .build(),
        ])
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

fn install_importer(
    core: &pipewire::core::Core,
) -> (
    ImportedMemory,
    ObservedMemory,
    Arc<AtomicU32>,
    Arc<Mutex<Vec<u32>>>,
) {
    let imported = Arc::new(Mutex::new(Vec::new()));
    let observed = Arc::new(Mutex::new(Vec::new()));
    let added = Arc::new(AtomicU32::new(0));
    let removed = Arc::new(Mutex::new(Vec::new()));
    core.set_memory_importer(Some(Box::new(Importer {
        memory: imported.clone(),
        observed: observed.clone(),
        added: added.clone(),
        removed: removed.clone(),
    })));
    (imported, observed, added, removed)
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
    let (imported, observed, added, removed) = install_importer(&core);

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
    let _timer = install_timeout(&main_loop, timed_out.clone(), Duration::from_secs(3));
    main_loop.run();

    let report = server.wait(deadline).unwrap();
    assert!(!timed_out.load(Ordering::Relaxed));
    assert!(done_seen.load(Ordering::Relaxed));
    assert_eq!(report.exported_mem_ids, vec![55]);
    assert_eq!(added.load(Ordering::Relaxed), 1);
    assert_eq!(
        observed.lock().unwrap().as_slice(),
        &[(55, spa_data_type::MEM_FD, 0)]
    );
    assert!(imported.lock().unwrap().is_empty());
    assert_eq!(removed.lock().unwrap().as_slice(), &[55]);
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
    let (imported, observed, added, removed) = install_importer(&core);

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
    assert_eq!(added.load(Ordering::Relaxed), 0);
    assert!(observed.lock().unwrap().is_empty());
    assert!(imported.lock().unwrap().is_empty());
    assert!(removed.lock().unwrap().is_empty());
    assert!(!process_has_memfd(56), "rejected AddMem descriptor leaked");
}
