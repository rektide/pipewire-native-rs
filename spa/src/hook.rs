// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: Copyright (c) 2025 Asymptotic Inc.
// SPDX-FileCopyrightText: Copyright (c) 2025 Arun Raghavan

use std::{
    collections::LinkedList,
    sync::{Arc, Condvar, Mutex},
    thread::{self, ThreadId},
};

pub type HookId = u32;

pub struct Hook<T> {
    id: HookId,
    callbacks: Option<T>,
}

impl<T> Hook<T> {
    pub fn callbacks(&mut self) -> &mut T {
        self.callbacks
            .as_mut()
            .expect("callbacks are unavailable while a hook is being dispatched")
    }
}

#[doc(hidden)]
pub struct HookDispatch<T> {
    hook_list: Arc<Mutex<HookList<T>>>,
    id: HookId,
    callbacks: Option<T>,
}

impl<T> HookDispatch<T> {
    pub fn callbacks(&mut self) -> &mut T {
        self.callbacks.as_mut().unwrap()
    }
}

impl<T> Drop for HookDispatch<T> {
    fn drop(&mut self) {
        let callbacks = self.callbacks.take().unwrap();
        let mut hook_list = self
            .hook_list
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        if let Some(hook) = hook_list.hooks.iter_mut().find(|hook| hook.id == self.id) {
            hook.callbacks = Some(callbacks);
        }
    }
}

#[doc(hidden)]
pub struct HookEmission<T> {
    hook_list: Arc<Mutex<HookList<T>>>,
}

impl<T> Drop for HookEmission<T> {
    fn drop(&mut self) {
        let mut hook_list = self
            .hook_list
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        hook_list.emission_depth -= 1;
        if hook_list.emission_depth == 0 {
            hook_list.emission_owner = None;
            hook_list.emission_ready.notify_one();
        }
    }
}

pub struct HookList<T> {
    hooks: LinkedList<Hook<T>>,
    next_id: HookId,
    emission_owner: Option<ThreadId>,
    emission_depth: usize,
    emission_ready: Arc<Condvar>,
}

impl<T> HookList<T> {
    // The return value is an Arc<Mutex<...>> for a few reasons.
    //
    // Firstly, the hook list is usually stored in the structure on which the hook is being emitted
    // -- that is, the structure is usually being captued for the closure that is going into the
    // hooks. This means that the closure might need to have a mutable borrow of the object itself,
    // like this example from tests/hook.rs
    //
    // ```
    //   // The hook list is contained in `this`, and the hook takes an &mut this as the first
    //   // argument
    //   emit_hook!(this.hooks, mutie, &mut this, ...);
    // ```
    //
    // If the hook list was not Clone, then we would need to take a reference to `this` above, and
    // that would not let us also take a mutable reference for the closure argument.
    //
    // Now given we need to be able to clone the hook list, the inner structures need to be mutable
    // (so we can add and remove hooks), which means a Mutex is necessary.
    //
    // A second reason for that mutability is that closure functions might be FnMut, which means
    // we need to mutably borrow the callbacks list in order to call the function at all.
    pub fn new() -> Arc<Mutex<HookList<T>>> {
        Arc::new(Mutex::new(HookList {
            hooks: LinkedList::new(),
            next_id: 0,
            emission_owner: None,
            emission_depth: 0,
            emission_ready: Arc::new(Condvar::new()),
        }))
    }

    pub fn prepend(&mut self, callbacks: T) -> HookId {
        let id = self.next_id;
        let hook = Hook {
            id,
            callbacks: Some(callbacks),
        };

        self.hooks.push_front(hook);
        self.next_id += 1;

        id
    }

    pub fn append(&mut self, callbacks: T) -> HookId {
        let id = self.next_id;
        let hook = Hook {
            id,
            callbacks: Some(callbacks),
        };

        self.hooks.push_back(hook);
        self.next_id += 1;

        id
    }

    // We only implement `iter_mut()` because we expect T to contain `FnMut`s, which need to be
    // borrowed mutably while being called (as they might mutate captured variables in their
    // context)
    pub fn iter_mut(&mut self) -> impl Iterator<Item = &mut Hook<T>> {
        self.hooks.iter_mut()
    }

    pub fn remove(&mut self, id: HookId) -> Option<T> {
        self.hooks
            .extract_if(|h| h.id == id)
            .next()
            .and_then(|h| h.callbacks)
    }

    #[doc(hidden)]
    pub fn begin_dispatch(hook_list: &Arc<Mutex<Self>>) -> HookEmission<T> {
        // External emissions serialize for their full snapshot, but same-thread recursion is
        // allowed so nested emissions can run inactive hooks without holding the list mutex.
        let thread_id = thread::current().id();
        let mut list = hook_list
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        loop {
            match list.emission_owner {
                None => {
                    list.emission_owner = Some(thread_id);
                    list.emission_depth = 1;
                    break;
                }
                Some(owner) if owner == thread_id => {
                    list.emission_depth += 1;
                    break;
                }
                Some(_) => {
                    let emission_ready = list.emission_ready.clone();
                    list = emission_ready
                        .wait(list)
                        .unwrap_or_else(|poisoned| poisoned.into_inner());
                }
            }
        }

        drop(list);
        HookEmission {
            hook_list: hook_list.clone(),
        }
    }

    #[doc(hidden)]
    pub fn dispatch_ids(hook_list: &Arc<Mutex<Self>>) -> Vec<HookId> {
        // Dispatch uses a stable ID snapshot. Removals take effect immediately, while additions
        // wait for the next emission.
        hook_list
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .hooks
            .iter()
            .map(|hook| hook.id)
            .collect()
    }

    #[doc(hidden)]
    pub fn dispatch(hook_list: &Arc<Mutex<Self>>, id: HookId) -> Option<HookDispatch<T>> {
        let callbacks = hook_list
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .hooks
            .iter_mut()
            .find(|hook| hook.id == id)?
            .callbacks
            .take()?;

        Some(HookDispatch {
            hook_list: hook_list.clone(),
            id,
            callbacks: Some(callbacks),
        })
    }
}

#[macro_export]
macro_rules! emit_hook {
    ($hook_list:expr, $method:ident) => {
        {
            let _h = $hook_list.clone();
            let _emission = $crate::hook::HookList::begin_dispatch(&_h);
            let _ids = $crate::hook::HookList::dispatch_ids(&_h);

            for _id in _ids {
                let Some(mut _hook) = $crate::hook::HookList::dispatch(&_h, _id) else {
                    continue;
                };
                if let Some(_method) = _hook.callbacks().$method.as_deref_mut() {
                    (_method)();
                }
            }
        }
    };
    ($hook_list:expr, $method:ident, $($args:tt)*) => {
        {
            let _h = $hook_list.clone();
            let _emission = $crate::hook::HookList::begin_dispatch(&_h);
            let _ids = $crate::hook::HookList::dispatch_ids(&_h);

            for _id in _ids {
                let Some(mut _hook) = $crate::hook::HookList::dispatch(&_h, _id) else {
                    continue;
                };
                if let Some(_method) = _hook.callbacks().$method.as_deref_mut() {
                    (_method)($($args)*);
                }
            }
        }
    };
}
