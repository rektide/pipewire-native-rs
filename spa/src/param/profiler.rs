// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: Copyright (c) 2025 Asymptotic Inc.
// SPDX-FileCopyrightText: Copyright (c) 2025 Arun Raghavan

use pipewire_native_macros::EnumU32;

use crate::pod::types::ObjectType;

use super::ParamObject;

#[repr(u32)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, EnumU32)]
pub enum Profiler {
    Start,
    StartDriver = 0x10000,
    Info,
    Clock,
    DriverBlock,
    StartFollower = 0x20000,
    FollowerBlock,
    FollowerClock,
    StartCustom = 0x1000000,
}

impl ParamObject for Profiler {
    const TYPE: ObjectType = ObjectType::Profiler;
}
