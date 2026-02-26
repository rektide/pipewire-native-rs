// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: Copyright (c) 2025 Asymptotic Inc.
// SPDX-FileCopyrightText: Copyright (c) 2025 Arun Raghavan

use std::{
    ops::{Deref, DerefMut},
    sync::{Arc, Mutex},
};

use pipewire_native_spa::{self as spa, pod::types::Fraction};

use crate::{
    core::Core,
    new_refcounted,
    proxy::{HasProxy, Proxy},
    refcounted,
    types::{self},
    HookId, Id,
};

refcounted! {
    /// Proxy that represents a profiler that is connected to the server.
    pub struct Profiler {
        proxy: Proxy,
        hooks: Arc<Mutex<spa::hook::HookList<ProfilerEvents>>>,
    }
}

/// Profiling information provided by the [ProfilerEvents::profile] event. A snapshot of statistics
/// and timing information of a graph (a set of connected nodes - either by direct links or
/// implicitly). Has one driver (+ its clock) and a number of followers (with optional clocks).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ProfilerSample<'a> {
    /// General info about this sample.
    pub info: SampleInfo,
    /// Profile of the driver node.
    pub driver: NodeSample<'a>,
    /// Profile of the clock that is driving this graph.
    pub driver_clock: DriverClockSample<'a>,
    /// Profile of follower nodes in the graph.
    pub followers: &'a [FollowerNodeSample<'a>],
    /// Profile of follower clocks. Not every follower has its own clock (application nodes usually
    /// don't have one).
    pub follower_clocks: &'a [ClockSample<'a>],
}

/// General information about the profiling sample.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SampleInfo {
    /// Ordinal number of the profiling sample. Monotonically increasing.
    pub counter: i64,
    /// CPU load (short time window).
    pub cpu_load_fast: f32,
    /// CPU load (medium time window).
    pub cpu_load_medium: f32,
    /// CPU load (long time window).
    pub cpu_load_slow: f32,
    /// Number of XRuns of the driver Node so far.
    pub xrun_count: u32,
}

/// Profiling sample of a Node participating in the graph, either in the driver or follower role.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct NodeSample<'a> {
    /// ID of the Node.
    pub id: Id,
    /// `node.name` property of the Node.
    pub name: &'a str,
    /// Previous time at which the node was triggered, against CLOCK_MONOTONIC in nanoseconds.
    pub prev_signal_time_ns: u64,
    /// Time at which the node was triggered (i.e. marked as ready to start processing in the
    /// current loop iteration), against CLOCK_MONOTONIC in nanoseconds.
    pub signal_time_ns: u64,
    /// Time at which processing actually started, against CLOCK_MONOTONIC in nanoseconds.
    pub awake_time_ns: u64,
    /// Time at which processing was completed, against CLOCK_MONOTONIC in nanoseconds.
    pub finish_time_ns: u64,
    /// Node status, as defined near
    /// <https://gitlab.freedesktop.org/pipewire/pipewire/-/blob/master/src/pipewire/private.h#L575>
    pub status: u32,
    /// Node's latency.
    pub latency: Fraction,
    /// Total number of xruns of this Node.
    pub xrun_count: u32,
}

/// Profiling sample of a follower node.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct FollowerNodeSample<'a> {
    /// Fields common for both driver and follower nodes. [`Self`] dereferences to this field.
    pub node_sample: NodeSample<'a>,
    /// Whether the follower is asynchronous.
    pub async_: bool,
}

impl<'a> Deref for FollowerNodeSample<'a> {
    type Target = NodeSample<'a>;

    fn deref(&self) -> &Self::Target {
        &self.node_sample
    }
}

impl<'a> DerefMut for FollowerNodeSample<'a> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.node_sample
    }
}

/// Profiling sample of a clock, either in the driver or follower role.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ClockSample<'a> {
    /// ID of the clock (usually matches Node ID that this clock belongs to).
    pub id: Id,
    /// Name of the clock.
    pub name: &'a str,
    /// Time in nanoseconds against monotonic clock (CLOCK_MONOTONIC). This fields reflects a real
    /// time instant in the past, when the current cycle started. The value may have jitter.
    pub nsec: u64,
    /// Rate for position/duration/delay/xrun.
    pub rate: Fraction,
    /// Current position, in samples @ [`Self::rate`].
    pub position: u64,
    /// Duration of current cycle, in samples @ [`Self::rate`].
    pub duration: u64,
    /// Delay between position and hardware, in samples @ [`Self::rate`].
    pub delay: i64,
    /// Rate difference between clock and monotonic time, as a ratio of clock speeds. A value higher
    /// than 1.0 means that the driver's internal clock is faster than the monotonic clock (by that
    /// factor), and vice versa.
    pub rate_diff: f64,
    /// Estimated next wakeup time in nanoseconds. This time is a logical start time of the next
    /// cycle, and is not necessarily in the future.
    pub next_nsec: u64,
    /// Estimated accumulated xrun duration, supposedly in samples @ [`Self::rate`].
    pub xrun_duration: u64,
}

/// Profiling sample of the driver's clock.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct DriverClockSample<'a> {
    /// Fields common for both driver and follower clocks. [`Self`] dereferences to this field.
    pub clock_sample: ClockSample<'a>,
    /// Driver clock flags, as defined near
    /// <https://gitlab.freedesktop.org/pipewire/pipewire/-/blob/master/spa/include/spa/node/io.h#L130>
    pub flags: u32,
    /// I/O position state, one of spa_io_position_state
    /// <https://docs.pipewire.org/group__spa__node.html#ga12aed5ed2a6ecb69734aa3eb7da4056b>
    pub state: u32,
    /// Cycle of the graph, starts at 0, incremented on each graph tick.
    pub cycle: u32,
}

impl<'a> Deref for DriverClockSample<'a> {
    type Target = ClockSample<'a>;

    fn deref(&self) -> &Self::Target {
        &self.clock_sample
    }
}

impl<'a> DerefMut for DriverClockSample<'a> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.clock_sample
    }
}

/// Profiler events that can be subscribed to.
#[allow(clippy::type_complexity)]
#[derive(Default)]
pub struct ProfilerEvents {
    /// Profiling information emitted.
    pub profile: Option<Box<dyn FnMut(&ProfilerSample<'_>) + Send>>,
}

impl HasProxy for Profiler {
    fn type_(&self) -> types::ObjectType {
        types::interface::PROFILER
    }

    fn version(&self) -> u32 {
        3
    }

    fn proxy(&self) -> &Proxy {
        &self.inner.proxy
    }
}

impl Profiler {
    pub(crate) fn new(core: &Core) -> Self {
        let this = Self {
            inner: new_refcounted(InnerProfiler::new(core)),
        };

        core.add_proxy(&this);

        this
    }

    /// Register for notifications of profiler events.
    pub fn add_listener(&self, events: ProfilerEvents) -> HookId {
        self.inner.hooks.lock().unwrap().append(events)
    }

    /// Remove a set of event listeners.
    pub fn remove_listener(&self, hook_id: HookId) {
        self.inner.hooks.lock().unwrap().remove(hook_id);
    }

    pub(crate) fn events(&self) -> Arc<Mutex<spa::hook::HookList<ProfilerEvents>>> {
        self.inner.hooks.clone()
    }
}

impl InnerProfiler {
    fn new(core: &Core) -> Self {
        Self {
            proxy: Proxy::new(core.next_proxy_id()),
            hooks: spa::hook::HookList::new(),
        }
    }
}
