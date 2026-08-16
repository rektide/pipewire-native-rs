use std::{ffi::OsString, sync::mpsc, time::Duration};

use pipewire_native::{
    self as pipewire,
    context::Context,
    main_loop::MainLoop,
    properties::Properties,
    proxy::{HasProxy, ProxyEvents},
};
use pipewire_native_protocol::wire::client_node::{
    Command, Direction, Event, NodeInfo, PortInfo, PortUpdate, SetActive, Update,
};
use pipewire_native_server::{
    runtime::{ScriptedServer, ServerConfig},
    script::{Action, CoreErrorAction, CoreInfoAction, Expectation, Scenario, ScriptStep},
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

#[test]
#[serial]
fn typed_client_node_creation_and_advertisement_reach_scripted_peer() {
    pipewire::init();
    let deadline = testkit::TestDeadline::after(Duration::from_secs(3));
    let socket_path = testkit::unique_socket_path("pipewire-native-client-node-wire");
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
                .expect(Expectation::ClientNodeUpdate)
                .actions(vec![])
                .build(),
            ScriptStep::builder()
                .expect(Expectation::ClientNodePortUpdate)
                .actions(vec![])
                .build(),
            ScriptStep::builder()
                .expect(Expectation::ClientNodeSetActive)
                .actions(vec![
                    Action::SendClientNodeCommand(Command::Start),
                    Action::SendCoreError(
                        CoreErrorAction::builder()
                            .id(0)
                            .seq(0)
                            .res(-libc::EPIPE)
                            .message("scenario complete".into())
                            .build(),
                    ),
                ])
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
    let event_loop = main_loop.clone();
    node.set_event_handler(Some(Box::new(move |event| {
        events.send(event).unwrap();
        event_loop.quit();
    })));
    node.update(Update {
        change_mask: 1,
        params: vec![],
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
        params: vec![],
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
    assert_eq!(report.completed_steps, 6);
    assert!(matches!(
        received_event.recv_timeout(Duration::from_secs(1)).unwrap(),
        Event::Command(Command::Start)
    ));
}
