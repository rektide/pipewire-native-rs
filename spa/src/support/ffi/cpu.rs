// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: Copyright (c) 2025 Asymptotic Inc.
// SPDX-FileCopyrightText: Copyright (c) 2025 Sanchayan Maity

use std::ffi::{c_int, c_uint, c_void, CString};

use crate::interface;
use crate::interface::cpu::{CpuFlags, CpuImpl, CpuVm};
use crate::interface::ffi::CInterface;

#[cfg(any(target_arch = "aarch64", target_arch = "arm"))]
use crate::interface::cpu::ArmCpuFlags;
#[cfg(any(target_arch = "powerpc64", target_arch = "powerpc"))]
use crate::interface::cpu::PpcCpuFlags;
#[cfg(target_arch = "riscv64")]
use crate::interface::cpu::RiscvCpuFlags;
#[cfg(any(target_arch = "x86_64", target_arch = "x86"))]
use crate::interface::cpu::X86CpuFlags;

use super::c_string;

#[repr(C)]
#[derive(Copy, Clone, Debug)]
struct CCpuMethods {
    version: u32,

    get_flags: extern "C" fn(object: *mut c_void) -> c_uint,
    force_flags: extern "C" fn(object: *mut c_void, flags: c_uint) -> c_int,
    get_count: extern "C" fn(object: *mut c_void) -> c_uint,
    get_max_align: extern "C" fn(object: *mut c_void) -> c_uint,
    get_vm_type: extern "C" fn(object: *mut c_void) -> c_uint,
    zero_denormals: extern "C" fn(object: *mut c_void, flags: bool) -> c_int,
}

#[repr(C)]
struct CCpu {
    iface: CInterface,
}

struct CCpuImpl {}

pub(super) fn new_impl(interface: *mut CInterface) -> CpuImpl {
    CpuImpl {
        inner: Box::pin(interface as *mut CCpu),

        get_flags: CCpuImpl::get_flags,
        force_flags: CCpuImpl::force_flags,
        get_count: CCpuImpl::get_count,
        get_max_align: CCpuImpl::get_max_align,
        get_vm_type: CCpuImpl::get_vm_type,
        zero_denormals: CCpuImpl::zero_denormals,
    }
}

impl CCpuImpl {
    fn from_cpu(this: &CpuImpl) -> &CCpu {
        unsafe {
            this.inner
                .as_ref()
                .downcast_ref::<*mut CCpu>()
                .unwrap()
                .as_ref()
                .unwrap()
        }
    }

    fn arch_cpu_flags(flags: u32) -> CpuFlags {
        #[cfg(any(target_arch = "x86_64", target_arch = "x86"))]
        return CpuFlags::X86(X86CpuFlags::from_bits(flags).expect("Expected valid CPU flags"));
        #[cfg(any(target_arch = "aarch64", target_arch = "arm"))]
        return CpuFlags::Arm(ArmCpuFlags::from_bits(flags).expect("Expected valid CPU flags"));
        #[cfg(any(target_arch = "powerpc64", target_arch = "powerpc"))]
        return CpuFlags::Ppc(PpcCpuFlags::from_bits(flags).expect("Expected valid CPU flags"));
        #[cfg(target_arch = "riscv64")]
        return CpuFlags::Riscv(RiscvCpuFlags::from_bits(flags).expect("Expected valid CPU flags"));
    }

    fn get_flags(this: &CpuImpl) -> CpuFlags {
        unsafe {
            let cpu = Self::from_cpu(this);
            let funcs = cpu.iface.cb.funcs as *const CCpuMethods;

            Self::arch_cpu_flags(((*funcs).get_flags)(cpu.iface.cb.data))
        }
    }

    fn force_flags(this: &CpuImpl, flags: CpuFlags) -> i32 {
        unsafe {
            let cpu = Self::from_cpu(this);
            let funcs = cpu.iface.cb.funcs as *const CCpuMethods;

            ((*funcs).force_flags)(cpu.iface.cb.data, u32::from(flags))
        }
    }

    fn get_count(this: &CpuImpl) -> u32 {
        unsafe {
            let cpu = Self::from_cpu(this);
            let funcs = cpu.iface.cb.funcs as *const CCpuMethods;

            ((*funcs).get_count)(cpu.iface.cb.data)
        }
    }

    fn get_max_align(this: &CpuImpl) -> u32 {
        unsafe {
            let cpu = Self::from_cpu(this);
            let funcs = cpu.iface.cb.funcs as *const CCpuMethods;

            ((*funcs).get_max_align)(cpu.iface.cb.data)
        }
    }

    fn get_vm_type(this: &CpuImpl) -> CpuVm {
        unsafe {
            let cpu = Self::from_cpu(this);
            let funcs = cpu.iface.cb.funcs as *const CCpuMethods;

            CpuVm::try_from(((*funcs).get_vm_type)(cpu.iface.cb.data))
                .expect("Expected valid VM type")
        }
    }

    fn zero_denormals(this: &CpuImpl, enable: bool) -> i32 {
        unsafe {
            let cpu = Self::from_cpu(this);
            let funcs = cpu.iface.cb.funcs as *const CCpuMethods;

            ((*funcs).zero_denormals)(cpu.iface.cb.data, enable)
        }
    }
}

struct CpuImplCIface {}

impl CpuImplCIface {
    fn c_to_cpu_impl(object: *mut c_void) -> &'static mut CpuImpl {
        unsafe { (object as *mut CpuImpl).as_mut().unwrap() }
    }

    extern "C" fn get_flags(object: *mut c_void) -> c_uint {
        let cpu_impl = Self::c_to_cpu_impl(object);

        u32::from(cpu_impl.get_flags())
    }

    extern "C" fn force_flags(object: *mut c_void, flags: c_uint) -> c_int {
        let cpu_impl = Self::c_to_cpu_impl(object);
        let f = CpuFlags::try_from(flags).expect("Expected valid CPU flags");

        cpu_impl.force_flags(f)
    }

    extern "C" fn get_count(object: *mut c_void) -> c_uint {
        let cpu_impl = Self::c_to_cpu_impl(object);

        cpu_impl.get_count()
    }

    extern "C" fn get_max_align(object: *mut c_void) -> c_uint {
        let cpu_impl = Self::c_to_cpu_impl(object);

        cpu_impl.get_max_align()
    }

    extern "C" fn get_vm_type(object: *mut c_void) -> c_uint {
        let cpu_impl = Self::c_to_cpu_impl(object);

        cpu_impl.get_vm_type() as u32
    }

    extern "C" fn zero_denormals(object: *mut c_void, flags: bool) -> c_int {
        let cpu_impl = Self::c_to_cpu_impl(object);

        cpu_impl.zero_denormals(flags)
    }
}

static CPU_METHODS: CCpuMethods = CCpuMethods {
    version: 0,

    get_flags: CpuImplCIface::get_flags,
    force_flags: CpuImplCIface::force_flags,
    get_count: CpuImplCIface::get_count,
    get_max_align: CpuImplCIface::get_max_align,
    get_vm_type: CpuImplCIface::get_vm_type,
    zero_denormals: CpuImplCIface::zero_denormals,
};

pub(crate) unsafe fn make_native(cpu: &CpuImpl) -> *mut CInterface {
    let c_cpu: *mut CCpu =
        unsafe { libc::calloc(1, std::mem::size_of::<CCpu>() as libc::size_t) as *mut CCpu };
    let c_cpu = unsafe { &mut *c_cpu };

    c_cpu.iface.version = 0;
    c_cpu.iface.type_ = c_string(interface::CPU).into_raw();
    c_cpu.iface.cb.funcs = &CPU_METHODS as *const CCpuMethods as *mut c_void;
    c_cpu.iface.cb.data = cpu as *const CpuImpl as *mut c_void;

    c_cpu as *mut CCpu as *mut CInterface
}

pub(crate) unsafe fn free_native(c_cpu: *mut CInterface) {
    unsafe {
        let _ = CString::from_raw((*c_cpu).type_ as *mut std::ffi::c_char);
        libc::free(c_cpu as *mut c_void);
    }
}
