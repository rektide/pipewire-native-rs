// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: Copyright (c) 2025 Asymptotic Inc.
// SPDX-FileCopyrightText: Copyright (c) 2025 Arun Raghavan

use std::ffi::{c_char, c_int, c_void, CStr};
use std::path::PathBuf;
use std::sync::Arc;

use libloading::os::unix::{Library, Symbol, RTLD_NOW};

use crate::dict::Dict;
use crate::interface::ffi::{CInterface, CSupport};
use crate::interface::plugin::{Handle, HandleFactory, Interface, InterfaceInfo};
use crate::interface::{self, Support};

use super::cpu;
use super::log;
use super::system;
use super::{c_string, r#loop};

const ENTRYPOINT: &str = "spa_handle_factory_enum";
type EntryPointFn = unsafe extern "C" fn(*mut *const CHandleFactory, index: *mut u32) -> c_int;

pub struct Plugin {
    inner: Arc<PluginInner>,
}

struct PluginInner {
    _library: Library,
    factories: Vec<*const CHandleFactory>,
}

unsafe impl Send for Plugin {}
unsafe impl Sync for Plugin {}
unsafe impl Send for PluginInner {}
unsafe impl Sync for PluginInner {}

impl Plugin {
    pub fn find_factory(&self, name: &str) -> Option<Box<dyn HandleFactory>> {
        for f in &self.inner.factories {
            let f_name = unsafe {
                let factory = f.as_ref().unwrap();
                CStr::from_ptr(factory.name).to_str()
            };

            if f_name == Ok(name) {
                return Some(Box::new(CHandleFactoryImpl {
                    factory: *f,
                    plugin: self.inner.clone(),
                }));
            }
        }

        None
    }
}

#[repr(C)]
#[derive(Copy)]
pub struct CInterfaceInfo {
    pub type_: *const c_char,
}

impl Clone for CInterfaceInfo {
    fn clone(&self) -> Self {
        /*
         * Making the implementation be like below, would be a non-canonical
         * implementation of Clone since Copy is already implemented.
         * See https://rust-lang.github.io/rust-clippy/master/index.html#non_canonical_clone_impl

           CInterfaceInfo {
               type_: unsafe { libc::strdup(self.type_) },
           }
        */
        *self
    }
}

#[repr(C)]
pub struct CHandleFactory {
    pub version: u32,
    pub name: *const c_char,
    pub info: *const Dict,

    pub get_size:
        unsafe extern "C" fn(factory: *const CHandleFactory, params: *const Dict) -> usize,
    pub init: unsafe extern "C" fn(
        factory: *const CHandleFactory,
        handle: *mut CHandle,
        params: *const Dict,
        support: *const CSupport,
        n_support: u32,
    ) -> c_int,
    pub enum_interface_info: unsafe extern "C" fn(
        factory: *const CHandleFactory,
        info: *mut *const CInterfaceInfo,
        index: *mut u32,
    ) -> c_int,
}

struct CHandleFactoryImpl {
    factory: *const CHandleFactory,
    plugin: Arc<PluginInner>,
}

impl HandleFactory for CHandleFactoryImpl {
    fn version(&self) -> u32 {
        unsafe { self.factory.as_ref().unwrap().version }
    }

    fn name(&self) -> &str {
        unsafe {
            CStr::from_ptr(self.factory.as_ref().unwrap().name)
                .to_str()
                .unwrap()
        }
    }

    fn info(&self) -> Option<&Dict> {
        unsafe { self.factory.as_ref().unwrap().info.as_ref() }
    }

    fn init(
        &self,
        info: Option<Dict>,
        support: &Support,
    ) -> std::io::Result<Box<dyn Handle + Send + Sync>> {
        unsafe {
            let info_ptr = match &info {
                Some(i) => i.as_raw(),
                None => std::ptr::null(),
            };
            let size = (self.factory.as_ref().unwrap().get_size)(self.factory, info_ptr);
            let handle = libc::calloc(1, size) as *mut CHandle;
            if handle.is_null() {
                return Err(std::io::Error::last_os_error());
            }
            let (support, n_support) = {
                let c_support = support.c_support();
                (c_support.as_ptr(), c_support.len())
            };
            let ret = (self.factory.as_ref().unwrap().init)(
                self.factory,
                handle,
                info_ptr,
                support,
                n_support as u32,
            );

            match ret {
                0 => Ok(Box::new(CHandleImpl {
                    owner: Arc::new(CHandleOwner {
                        handle,
                        _plugin: self.plugin.clone(),
                    }),
                })),
                err => {
                    libc::free(handle as *mut c_void);
                    Err(std::io::Error::from_raw_os_error(-err))
                }
            }
        }
    }

    fn enum_interface_info(&self) -> std::io::Result<Vec<InterfaceInfo>> {
        unsafe { enum_interface_info(self.factory) }
    }
}

unsafe fn enum_interface_info(
    factory: *const CHandleFactory,
) -> std::io::Result<Vec<InterfaceInfo>> {
    let mut interfaces = vec![];
    let mut info: *const CInterfaceInfo = std::ptr::null();
    let mut index = 0;

    loop {
        let ret = ((*factory).enum_interface_info)(factory, &mut info, &mut index);
        match ret {
            1 => {
                if info.is_null() || (*info).type_.is_null() {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "SPA factory returned null interface metadata",
                    ));
                }
                interfaces.push(InterfaceInfo {
                    type_: CStr::from_ptr((*info).type_).to_string_lossy().into_owned(),
                });
            }
            0 => return Ok(interfaces),
            err if err < 0 => return Err(std::io::Error::from_raw_os_error(-err)),
            err => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("SPA factory returned invalid enumeration status {err}"),
                ));
            }
        }
    }
}

#[repr(C)]
pub struct CHandle {
    pub version: u32,
    pub get_interface: unsafe extern "C" fn(
        handle: *mut CHandle,
        type_: *const c_char,
        iface: *mut *mut CInterface,
    ) -> c_int,
    pub clear: unsafe extern "C" fn(handle: *mut CHandle) -> c_int,
}

struct CHandleImpl {
    owner: Arc<CHandleOwner>,
}

pub(super) struct CHandleOwner {
    handle: *mut CHandle,
    _plugin: Arc<PluginInner>,
}

unsafe impl Send for CHandleImpl {}
unsafe impl Sync for CHandleImpl {}
unsafe impl Send for CHandleOwner {}
unsafe impl Sync for CHandleOwner {}

impl Drop for CHandleOwner {
    fn drop(&mut self) {
        unsafe {
            (self.handle.as_ref().unwrap().clear)(self.handle);
            libc::free(self.handle as *mut c_void);
        }
    }
}

impl Handle for CHandleImpl {
    fn version(&self) -> u32 {
        unsafe { self.owner.handle.as_ref().unwrap().version }
    }

    fn get_interface(&self, type_: &str) -> std::io::Result<Option<Box<dyn Interface>>> {
        let iface = unsafe { get_interface(self.owner.handle, type_)? };

        Ok(match type_ {
            interface::CPU => Some(Box::new(cpu::new_impl(iface, self.owner.clone()))),
            interface::LOG => Some(Box::new(log::new_impl(iface, self.owner.clone()))),
            interface::LOOP => Some(Box::new(r#loop::new_impl(iface, self.owner.clone()))),
            interface::LOOP_CONTROL => Some(Box::new(r#loop::control::new_impl(
                iface,
                self.owner.clone(),
            ))),
            interface::LOOP_UTILS => {
                Some(Box::new(r#loop::utils::new_impl(iface, self.owner.clone())))
            }
            interface::SYSTEM => Some(Box::new(system::new_impl(iface, self.owner.clone()))),
            _ => None,
        })
    }
}

unsafe fn get_interface(handle: *mut CHandle, type_: &str) -> std::io::Result<*mut CInterface> {
    let mut iface: *mut CInterface = std::ptr::null_mut();
    let ret = ((*handle).get_interface)(handle, c_string(type_).as_ptr(), &mut iface);

    if ret < 0 {
        return Err(std::io::Error::from_raw_os_error(-ret));
    }
    if ret != 0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("SPA handle returned invalid get_interface status {ret}"),
        ));
    }
    if iface.is_null() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "SPA handle returned a null interface on success",
        ));
    }

    Ok(iface)
}

pub fn load(path: &PathBuf) -> Result<Plugin, String> {
    unsafe {
        let library = Library::open(Some(path), RTLD_NOW).map_err(|e| format!("{}", e))?;
        let entrypoint: Symbol<EntryPointFn> = library
            .get(ENTRYPOINT.as_bytes())
            .map_err(|e| format!("{}", e))?;

        let mut h: *const CHandleFactory = std::ptr::null();
        let mut i: u32 = 0;
        let i_ptr: *mut u32 = &mut i;
        let mut factories = vec![];

        loop {
            match entrypoint(&mut h, i_ptr) {
                1 if h.is_null() => {
                    return Err("Could not load plugin: factory enumeration returned null".into());
                }
                1 if (*h).name.is_null() => {
                    return Err("Could not load plugin: factory has a null name".into());
                }
                1 => factories.push(h),
                0 => break,
                err => return Err(format!("Could not load plugin: {}", err)),
            }
        }

        Ok(Plugin {
            inner: Arc::new(PluginInner {
                _library: library,
                factories,
            }),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    unsafe extern "C" fn get_size(_: *const CHandleFactory, _: *const Dict) -> usize {
        0
    }

    unsafe extern "C" fn init(
        _: *const CHandleFactory,
        _: *mut CHandle,
        _: *const Dict,
        _: *const CSupport,
        _: u32,
    ) -> c_int {
        0
    }

    unsafe extern "C" fn enum_then_error(
        _: *const CHandleFactory,
        info: *mut *const CInterfaceInfo,
        index: *mut u32,
    ) -> c_int {
        if *index == 0 {
            *info = Box::into_raw(Box::new(CInterfaceInfo {
                type_: c"test.interface".as_ptr(),
            }));
            *index += 1;
            1
        } else {
            -libc::EIO
        }
    }

    unsafe extern "C" fn enum_null_info(
        _: *const CHandleFactory,
        info: *mut *const CInterfaceInfo,
        _: *mut u32,
    ) -> c_int {
        *info = std::ptr::null();
        1
    }

    fn factory(
        enum_interface_info: unsafe extern "C" fn(
            *const CHandleFactory,
            *mut *const CInterfaceInfo,
            *mut u32,
        ) -> c_int,
    ) -> CHandleFactory {
        CHandleFactory {
            version: 1,
            name: c"test.factory".as_ptr(),
            info: std::ptr::null(),
            get_size,
            init,
            enum_interface_info,
        }
    }

    unsafe extern "C" fn get_interface_error_with_output(
        _: *mut CHandle,
        _: *const c_char,
        iface: *mut *mut CInterface,
    ) -> c_int {
        *iface = std::ptr::NonNull::dangling().as_ptr();
        -libc::EIO
    }

    unsafe extern "C" fn clear(_: *mut CHandle) -> c_int {
        0
    }

    #[test]
    fn get_interface_rejects_error_with_non_null_output() {
        let mut handle = CHandle {
            version: 0,
            get_interface: get_interface_error_with_output,
            clear,
        };

        let error = unsafe { get_interface(&mut handle, "test.interface") }.unwrap_err();

        assert_eq!(error.raw_os_error(), Some(libc::EIO));
    }

    #[test]
    fn enumeration_does_not_return_partial_results_on_error() {
        let factory = factory(enum_then_error);

        let error = unsafe { enum_interface_info(&factory) }
            .err()
            .expect("enumeration should fail");

        assert_eq!(error.raw_os_error(), Some(libc::EIO));
    }

    #[test]
    fn enumeration_rejects_null_interface_metadata() {
        let factory = factory(enum_null_info);

        let error = unsafe { enum_interface_info(&factory) }
            .err()
            .expect("enumeration should fail");

        assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
    }
}
