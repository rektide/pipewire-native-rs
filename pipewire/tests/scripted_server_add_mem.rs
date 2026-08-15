// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: Copyright (c) 2026 Asymptotic Inc.

use std::sync::{
    atomic::{AtomicBool, AtomicU32, Ordering},
    Arc,
};
use std::time::Duration;

use pipewire_native::{
    self as pipewire, context::Context, core::CoreEvents, main_loop::MainLoop,
    properties::Properties,
};
use pipewire_native_server::{
    runtime::{ScriptedServer, ServerConfig},
    script::{Action, CoreAddMemAction, CoreInfoAction, Expectation, Scenario, ScriptStep},
    testkit,
};
use serial_test::serial;

#[test]
#[serial]
fn client_handles_scripted_core_add_mem_fd_event() {
    pipewire::init();
    let deadline = testkit::TestDeadline::after(Duration::from_secs(5));

    let socket_path = testkit::unique_socket_path("pipewire-native-scripted-add-mem");

    let scenario = Scenario::builder()
        .name("core-add-mem".to_string())
        .steps(vec![
            ScriptStep::builder()
                .name("expect-hello".to_string())
                .expect(Expectation::CoreHello)
                .actions(vec![Action::SendCoreInfo(
                    CoreInfoAction::builder()
                        .cookie(1)
                        .user_name("tester".to_string())
                        .host_name("localhost".to_string())
                        .version("1.0-test".to_string())
                        .name("scripted-server".to_string())
                        .props(vec![])
                        .build(),
                )])
                .build(),
            ScriptStep::builder()
                .name("expect-update-properties".to_string())
                .expect(Expectation::ClientUpdateProperties)
                .actions(vec![])
                .build(),
            ScriptStep::builder()
                .name("expect-sync-send-addmem".to_string())
                .expect(Expectation::CoreSync)
                .actions(vec![
                    Action::SendCoreAddMem(
                        CoreAddMemAction::builder()
                            .id(55)
                            .memory_type(pipewire_native_server::protocol::spa_data_type::MEM_FD)
                            .flags(0)
                            .size(4096)
                            .build(),
                    ),
                    Action::SendCoreDoneFromLastSync,
                ])
                .build(),
            ScriptStep::builder()
                .name("expect-client-observed-done".to_string())
                .expect(Expectation::CoreSync)
                .actions(vec![Action::CloseConnection])
                .build(),
        ])
        .build();

    let server = ScriptedServer::builder()
        .config(
            ServerConfig::builder()
                .socket_path(socket_path.clone())
                .single_client(true)
                .trace_name("pipewire-test-addmem".to_string())
                .build(),
        )
        .scenario(scenario)
        .build();

    let server_thread = testkit::spawn(server);

    let old_remote = std::env::var("PIPEWIRE_REMOTE").ok();
    unsafe {
        std::env::set_var("PIPEWIRE_REMOTE", socket_path.as_os_str());
    }

    let props = Properties::new_vec(vec![("loop.name".to_string(), "pw-main-loop".to_string())]);
    let main_loop = MainLoop::new(&props).unwrap();
    let context = Context::new(&main_loop, Properties::new()).unwrap();
    let core = context.connect(None).unwrap();

    let done_seen = Arc::new(AtomicBool::new(false));
    let timed_out = Arc::new(AtomicBool::new(false));
    let done_seq = Arc::new(AtomicU32::new(0));
    let acknowledgement_seq = Arc::new(AtomicU32::new(0));
    let expected_seq = Arc::new(AtomicU32::new(0));

    let done_seen_cb = done_seen.clone();
    let done_seq_cb = done_seq.clone();
    let expected_seq_cb = expected_seq.clone();
    let main_loop_cb = main_loop.clone();
    let core_cb = core.clone();
    let acknowledgement_seq_cb = acknowledgement_seq.clone();
    core.add_listener(CoreEvents::new(
        None,
        Some(Box::new(move |_id, seq| {
            if seq == expected_seq_cb.load(Ordering::Relaxed) {
                done_seq_cb.store(seq, Ordering::Relaxed);
                done_seen_cb.store(true, Ordering::Relaxed);
                acknowledgement_seq_cb.store(core_cb.sync().unwrap(), Ordering::Relaxed);
                main_loop_cb.quit();
            }
        })),
        Some(Box::new(|id, seq, res, msg| {
            panic!("unexpected core error id={id} seq={seq} res={res} msg={msg}");
        })),
    ));

    let main_loop_timer = main_loop.clone();
    let timed_out_cb = timed_out.clone();
    let mut timer = main_loop
        .add_timer(Box::new(move |_expirations| {
            timed_out_cb.store(true, Ordering::Relaxed);
            main_loop_timer.quit();
        }))
        .unwrap();
    let timeout = libc::timespec {
        tv_sec: 3,
        tv_nsec: 0,
    };
    main_loop
        .update_timer(&mut timer, &timeout, None, false)
        .unwrap();

    let seq = core.sync().unwrap();
    expected_seq.store(seq, Ordering::Relaxed);

    main_loop.run();
    core.disconnect();

    if let Some(remote) = old_remote {
        unsafe {
            std::env::set_var("PIPEWIRE_REMOTE", remote);
        }
    } else {
        unsafe {
            std::env::remove_var("PIPEWIRE_REMOTE");
        }
    }

    let report = server_thread.wait(deadline).unwrap();
    assert!(
        !timed_out.load(Ordering::Relaxed),
        "main loop deadline elapsed before matching Core::Done; server_report={report:?}"
    );
    assert!(done_seen.load(Ordering::Relaxed));
    assert_eq!(done_seq.load(Ordering::Relaxed), seq);
    assert_eq!(report.completed_steps, 3);
    assert_eq!(report.exported_mem_ids, vec![55]);
    assert_eq!(
        report.last_sync.unwrap().seq,
        acknowledgement_seq.load(Ordering::Relaxed)
    );
}
