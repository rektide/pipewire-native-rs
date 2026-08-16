// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: Copyright (c) 2025 Asymptotic Inc.
// SPDX-FileCopyrightText: Copyright (c) 2025 Arun Raghavan

use std::ffi::c_void;

use bitflags::bitflags;
use pipewire_native_macros::EnumU32;

// spa/utils/type.h: Basic SPA_TYPE_*
#[repr(u32)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, EnumU32)]
pub enum Type {
    Start = 0,
    None,
    Bool,
    Id,
    Int,
    Long,
    Float,
    Double,
    String,
    Bytes,
    Rectangle,
    Fraction,
    Bitmap,
    Array,
    Struct,
    Object,
    Sequence,
    Pointer,
    Fd,
    Choice,
    Pod,
}

impl Type {
    pub fn is_primitive(&self) -> bool {
        matches!(
            self,
            Type::None
                | Type::Bool
                | Type::Id
                | Type::Int
                | Type::Long
                | Type::Float
                | Type::Double
                | Type::Rectangle
                | Type::Fraction
                | Type::Fd
        )
    }
}

#[repr(u32)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, EnumU32)]
pub enum ObjectType {
    Start = 0x40000,
    PropInfo,
    Props,
    Format,
    ParamBuffers,
    ParamMeta,
    ParamIo,
    ParamProfile,
    ParamPortConfig,
    ParamRoute,
    Profiler,
    ParamLatency,
    ParamProcessLatency,
    ParamTag,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Id<T>(pub T);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Pointer {
    pub type_: Type,
    pub ptr: *const c_void,
}

/// Full signed SPA_TYPE_Fd wire value.
///
/// This is an index or sentinel in native-protocol messages, not necessarily an
/// operating-system file descriptor, and SPA encodes it as a signed 64-bit body.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Fd(pub i64);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Rectangle {
    pub width: u32,
    pub height: u32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Fraction {
    pub num: u32,
    pub denom: u32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Choice<T> {
    None(T),
    Range { default: T, min: T, max: T },
    Step { default: T, min: T, max: T, step: T },
    Enum { default: T, alternatives: Vec<T> },
    Flags { default: T, flags: T },
}

bitflags! {
    #[repr(C)]
    #[derive(Debug, Eq, PartialEq)]
    pub struct PropertyFlags: u32 {
        const READ_ONLY = 0x0000_0001;
        const HARDWARE = 0x0000_0002;
        const HINT_DICT = 0x0000_0004;
        const MANDATORY = 0x0000_0008;
        const DONT_FIXATE = 0x0000_000F;
    }
}

#[derive(Debug, Eq, PartialEq)]
pub struct Property<K, V> {
    pub key: K,
    pub flags: PropertyFlags,
    pub value: V,
}
