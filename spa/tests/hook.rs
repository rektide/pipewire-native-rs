// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: Copyright (c) 2025 Asymptotic Inc.
// SPDX-FileCopyrightText: Copyright (c) 2025 Arun Raghavan

use std::{
    panic::{self, AssertUnwindSafe},
    rc::Rc,
    sync::{mpsc, Arc, Mutex},
    thread,
    time::Duration,
};

use pipewire_native_spa::{emit_hook, hook::HookList};

#[allow(clippy::type_complexity)]
struct TestEvents {
    constie: Option<Box<dyn FnMut(i32)>>,
    normie: Option<Box<dyn FnMut(&TestStruct, i32, &str)>>,
    mutie: Option<Box<dyn FnMut(&mut TestStruct, i32, &str)>>,
}

struct TestStruct {
    value: i32,
    name: String,
    hooks: Arc<Mutex<HookList<TestEvents>>>,
}

struct ReentrantEvents {
    event: Option<Box<dyn FnMut()>>,
}

struct ConcurrentEvents {
    event: Option<Box<dyn FnMut() + Send>>,
}

#[test]
fn test_hooks() {
    let accum = Rc::new(Mutex::new(0i32));

    let accum1 = accum.clone();
    let accum2 = accum.clone();

    let events = TestEvents {
        // Increment accumulator by callback value
        constie: Some(Box::new(move |i| *accum1.lock().unwrap() += i)),
        // Increment accumulator by struct value * callback value
        normie: Some(Box::new(move |this, i, s| {
            *accum2.lock().unwrap() += this.value * i;
            assert_eq!(s, &this.name);
        })),
        // Set struct value to callback value
        mutie: Some(Box::new(move |this, i, s| {
            this.value = i;
            this.name = s.to_string();
        })),
    };

    let mut this = TestStruct {
        value: 1,
        name: "First".to_string(),
        hooks: HookList::new(),
    };

    let id = this.hooks.lock().unwrap().append(events);

    emit_hook!(this.hooks, constie, 1);
    assert_eq!(*accum.lock().unwrap(), 1);

    emit_hook!(this.hooks, normie, &this, 2, "First");
    assert_eq!(*accum.lock().unwrap(), 3);

    emit_hook!(this.hooks, mutie, &mut this, 4, "Second");
    assert_eq!(this.value, 4);
    assert_eq!(this.name, "Second");

    this.hooks.lock().unwrap().remove(id);

    emit_hook!(this.hooks, constie, 1);
    assert_eq!(*accum.lock().unwrap(), 3);
}

#[test]
fn hook_can_remove_itself_during_dispatch() {
    let hooks = HookList::<ReentrantEvents>::new();
    let id = Arc::new(Mutex::new(None));
    let calls = Arc::new(Mutex::new(0));

    let callback_hooks = hooks.clone();
    let callback_id = id.clone();
    let callback_calls = calls.clone();
    let hook_id = hooks.lock().unwrap().append(ReentrantEvents {
        event: Some(Box::new(move || {
            *callback_calls.lock().unwrap() += 1;
            callback_hooks
                .lock()
                .unwrap()
                .remove(callback_id.lock().unwrap().unwrap());
        })),
    });
    *id.lock().unwrap() = Some(hook_id);

    emit_hook!(hooks, event);
    emit_hook!(hooks, event);

    assert_eq!(*calls.lock().unwrap(), 1);
}

#[test]
fn hook_added_during_dispatch_runs_on_the_next_emission() {
    let hooks = HookList::<ReentrantEvents>::new();
    let calls = Arc::new(Mutex::new(Vec::new()));
    let added = Arc::new(Mutex::new(false));

    let callback_hooks = hooks.clone();
    let callback_calls = calls.clone();
    let callback_added = added.clone();
    hooks.lock().unwrap().append(ReentrantEvents {
        event: Some(Box::new(move || {
            callback_calls.lock().unwrap().push("first");
            let mut added = callback_added.lock().unwrap();
            if !*added {
                *added = true;
                let added_calls = callback_calls.clone();
                callback_hooks.lock().unwrap().append(ReentrantEvents {
                    event: Some(Box::new(move || {
                        added_calls.lock().unwrap().push("added");
                    })),
                });
            }
        })),
    });

    let second_calls = calls.clone();
    hooks.lock().unwrap().append(ReentrantEvents {
        event: Some(Box::new(move || {
            second_calls.lock().unwrap().push("second");
        })),
    });

    emit_hook!(hooks, event);
    assert_eq!(*calls.lock().unwrap(), ["first", "second"]);

    emit_hook!(hooks, event);
    assert_eq!(
        *calls.lock().unwrap(),
        ["first", "second", "first", "second", "added"]
    );
}

#[test]
fn nested_emission_skips_the_active_hook() {
    let hooks = HookList::<ReentrantEvents>::new();
    let calls = Arc::new(Mutex::new(Vec::new()));
    let nested = Arc::new(Mutex::new(false));

    let callback_hooks = hooks.clone();
    let callback_calls = calls.clone();
    let callback_nested = nested.clone();
    hooks.lock().unwrap().append(ReentrantEvents {
        event: Some(Box::new(move || {
            callback_calls.lock().unwrap().push("first-start");
            let should_emit = {
                let mut nested = callback_nested.lock().unwrap();
                let should_emit = !*nested;
                *nested = true;
                should_emit
            };
            if should_emit {
                emit_hook!(callback_hooks, event);
            }
            callback_calls.lock().unwrap().push("first-end");
        })),
    });

    let second_calls = calls.clone();
    hooks.lock().unwrap().append(ReentrantEvents {
        event: Some(Box::new(move || {
            second_calls.lock().unwrap().push("second");
        })),
    });

    emit_hook!(hooks, event);

    assert_eq!(
        *calls.lock().unwrap(),
        ["first-start", "second", "first-end", "second"]
    );
}

#[test]
fn concurrent_emissions_are_serialized_without_skipping_callbacks() {
    let hooks = HookList::<ConcurrentEvents>::new();
    let calls = Arc::new(Mutex::new(0));
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();

    let callback_calls = calls.clone();
    hooks.lock().unwrap().append(ConcurrentEvents {
        event: Some(Box::new(move || {
            let mut calls = callback_calls.lock().unwrap();
            *calls += 1;
            if *calls == 1 {
                entered_tx.send(()).unwrap();
                drop(calls);
                release_rx.recv().unwrap();
            }
        })),
    });

    let first_hooks = hooks.clone();
    let first = thread::spawn(move || emit_hook!(first_hooks, event));
    entered_rx.recv().unwrap();

    let second_hooks = hooks.clone();
    let (started_tx, started_rx) = mpsc::channel();
    let (finished_tx, finished_rx) = mpsc::channel();
    let second = thread::spawn(move || {
        started_tx.send(()).unwrap();
        emit_hook!(second_hooks, event);
        finished_tx.send(()).unwrap();
    });

    started_rx.recv().unwrap();
    assert!(finished_rx
        .recv_timeout(Duration::from_millis(100))
        .is_err());
    release_tx.send(()).unwrap();
    first.join().unwrap();
    second.join().unwrap();
    finished_rx.recv().unwrap();

    assert_eq!(*calls.lock().unwrap(), 2);
}

#[test]
fn callback_panic_restores_callbacks_when_hook_list_is_poisoned() {
    let hooks = HookList::<ConcurrentEvents>::new();
    let poison_hooks = hooks.clone();
    assert!(thread::spawn(move || {
        let _hooks = poison_hooks.lock().unwrap();
        panic!("poison hook list");
    })
    .join()
    .is_err());

    let calls = Arc::new(Mutex::new(0));
    let callback_calls = calls.clone();
    hooks
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .append(ConcurrentEvents {
            event: Some(Box::new(move || {
                let should_panic = {
                    let mut calls = callback_calls.lock().unwrap();
                    *calls += 1;
                    *calls == 1
                };
                if should_panic {
                    panic!("callback panic");
                }
            })),
        });

    let panic = panic::catch_unwind(AssertUnwindSafe(|| emit_hook!(hooks, event)))
        .expect_err("the callback panic should propagate");
    assert_eq!(panic.downcast_ref::<&str>(), Some(&"callback panic"));

    emit_hook!(hooks, event);
    assert_eq!(*calls.lock().unwrap(), 2);
}

#[test]
fn removing_an_active_hook_prevents_its_restoration() {
    let hooks = HookList::<ConcurrentEvents>::new();
    let calls = Arc::new(Mutex::new(0));
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();

    let callback_calls = calls.clone();
    let id = hooks.lock().unwrap().append(ConcurrentEvents {
        event: Some(Box::new(move || {
            *callback_calls.lock().unwrap() += 1;
            entered_tx.send(()).unwrap();
            release_rx.recv().unwrap();
        })),
    });

    let emission_hooks = hooks.clone();
    let emission = thread::spawn(move || emit_hook!(emission_hooks, event));
    entered_rx.recv().unwrap();
    assert!(hooks.lock().unwrap().remove(id).is_none());
    release_tx.send(()).unwrap();
    emission.join().unwrap();

    emit_hook!(hooks, event);
    assert_eq!(*calls.lock().unwrap(), 1);
}
