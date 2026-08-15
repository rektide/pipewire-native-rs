// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: Copyright (c) 2025 Asymptotic Inc.
// SPDX-FileCopyrightText: Copyright (c) 2025 Arun Raghavan

use std::{
    rc::Rc,
    sync::{Arc, Mutex},
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
