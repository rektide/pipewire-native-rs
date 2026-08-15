// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: Copyright (c) 2025 Asymptotic Inc.
// SPDX-FileCopyrightText: Copyright (c) 2025 Arun Raghavan

use std::{
    any::Any,
    ffi::{c_int, c_void, CString},
};

use crate::{
    dict::Dict,
    interface::{
        self,
        ffi::CInterface,
        thread::{Thread, ThreadUtilsImpl},
    },
};

use super::c_string;

#[repr(C)]
struct CThread {}

#[repr(C)]
struct CThreadUtils {
    iface: CInterface,
}

#[repr(C)]
#[derive(Copy, Clone)]
struct CThreadUtilsMethods {
    version: u32,

    create: extern "C" fn(
        object: *mut c_void,
        props: *const Dict,
        start: extern "C" fn(*mut c_void) -> *mut c_void,
        arg: *mut c_void,
    ) -> *mut CThread,
    join:
        extern "C" fn(object: *mut c_void, thread: *mut CThread, retval: *mut *mut c_void) -> c_int,

    get_rt_range: extern "C" fn(
        object: *mut c_void,
        props: *const Dict,
        min: *mut c_int,
        max: *mut c_int,
    ) -> c_int,

    acquire_rt: extern "C" fn(object: *mut c_void, thread: *mut CThread, priority: c_int) -> c_int,
    drop_rt: extern "C" fn(object: *mut c_void, thread: *mut CThread) -> c_int,
}

pub(crate) unsafe fn make_native(thread_utils: &ThreadUtilsImpl) -> *mut CInterface {
    let c_thread_utils: *mut CThreadUtils = unsafe {
        libc::calloc(1, std::mem::size_of::<CThreadUtils>() as libc::size_t) as *mut CThreadUtils
    };
    let c_thread_utils = unsafe { &mut *c_thread_utils };

    c_thread_utils.iface.version = 0;
    c_thread_utils.iface.type_ = c_string(interface::THREAD_UTILS).into_raw();
    c_thread_utils.iface.cb.funcs =
        &THREAD_UTILS_METHODS as *const CThreadUtilsMethods as *mut c_void;
    c_thread_utils.iface.cb.data = thread_utils as *const ThreadUtilsImpl as *mut c_void;

    c_thread_utils as *mut CThreadUtils as *mut CInterface
}

pub(crate) unsafe fn free_native(c_thread_utils: *mut CInterface) {
    unsafe {
        let _ = CString::from_raw((*c_thread_utils).type_ as *mut std::ffi::c_char);
        libc::free(c_thread_utils as *mut c_void);
    }
}

static THREAD_UTILS_METHODS: CThreadUtilsMethods = CThreadUtilsMethods {
    version: 0,

    create: ThreadUtilsCIface::create,
    join: ThreadUtilsCIface::join,

    get_rt_range: ThreadUtilsCIface::get_rt_range,
    acquire_rt: ThreadUtilsCIface::acquire_rt,
    drop_rt: ThreadUtilsCIface::drop_rt,
};

struct ThreadUtilsCIface {}

// Transfers a C-owned argument into the newly created thread, or its return
// value back to the thread that successfully joins it.
struct SendablePtr {
    value: *mut c_void,
}

// SAFETY: each pointer is moved across a thread boundary exactly once. The
// creating side does not dereference the argument after successful creation,
// and the returned pointer stays in the joined thread's owned result until join.
unsafe impl Send for SendablePtr {}

impl ThreadUtilsCIface {
    fn c_to_thread_utils_impl(object: *mut c_void) -> &'static ThreadUtilsImpl {
        unsafe { &*(object as *mut ThreadUtilsImpl) }
    }

    extern "C" fn create(
        object: *mut c_void,
        props: *const Dict,
        start: extern "C" fn(*mut c_void) -> *mut c_void,
        arg: *mut c_void,
    ) -> *mut CThread {
        let impl_ = Self::c_to_thread_utils_impl(object);
        let props = unsafe { props.as_ref() };
        let send_arg = SendablePtr { value: arg };

        let thread = impl_.create(props, move || {
            // Get the argument to the function
            let arg = send_arg;

            // Run the function
            let retval = start(arg.value);

            // Pack the return value for send
            Box::new(SendablePtr { value: retval })
        });

        match thread {
            Some(t) => Box::into_raw(t.inner) as *mut c_void as *mut CThread,
            None => std::ptr::null_mut(),
        }
    }

    extern "C" fn join(
        object: *mut c_void,
        thread: *mut CThread,
        retval: *mut *mut c_void,
    ) -> c_int {
        let impl_ = Self::c_to_thread_utils_impl(object);
        let thread = unsafe { Box::from_raw(thread as *mut c_void as *mut dyn Any) };

        match impl_.join(Thread { inner: thread }) {
            Ok(send_retval) => {
                unsafe { *retval = send_retval.downcast::<SendablePtr>().unwrap().value };
                0
            }
            Err(e) => e.raw_os_error().unwrap_or(-1),
        }
    }

    extern "C" fn get_rt_range(
        object: *mut c_void,
        props: *const Dict,
        min: *mut c_int,
        max: *mut c_int,
    ) -> c_int {
        let impl_ = Self::c_to_thread_utils_impl(object);

        match impl_.get_rt_range(unsafe { props.as_ref() }) {
            Ok((min_, max_)) => unsafe {
                *min = min_;
                *max = max_;
                0
            },
            Err(e) => e.raw_os_error().unwrap_or(-1),
        }
    }

    extern "C" fn acquire_rt(object: *mut c_void, thread: *mut CThread, priority: c_int) -> c_int {
        let impl_ = Self::c_to_thread_utils_impl(object);
        let t = Thread {
            inner: unsafe { Box::from_raw(thread as *mut c_void as *mut dyn Any) },
        };

        let ret = impl_.acquire_rt(&t, priority);

        // Drop ownership of the pointer until we do a join
        let _ = Box::into_raw(t.inner);

        match ret {
            Ok(_) => 0,
            Err(e) => e.raw_os_error().unwrap_or(-1),
        }
    }

    extern "C" fn drop_rt(object: *mut c_void, thread: *mut CThread) -> c_int {
        let impl_ = Self::c_to_thread_utils_impl(object);
        let t = Thread {
            inner: unsafe { Box::from_raw(thread as *mut c_void as *mut dyn Any) },
        };

        let ret = impl_.drop_rt(&t);

        // Drop ownership of the pointer until we do a join
        let _ = Box::into_raw(t.inner);

        match ret {
            Ok(_) => 0,
            Err(e) => e.raw_os_error().unwrap_or(-1),
        }
    }
}

#[cfg(test)]
mod tests {
    use static_assertions::{assert_impl_all, assert_not_impl_any};

    use super::SendablePtr;

    assert_impl_all!(SendablePtr: Send);
    assert_not_impl_any!(SendablePtr: Sync);
}
