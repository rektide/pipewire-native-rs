// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: Copyright (c) 2025 Asymptotic Inc.
// SPDX-FileCopyrightText: Copyright (c) 2025 Arun Raghavan

use pipewire_native_macros as macros;
use pipewire_native_spa::{
    self as spa, param,
    pod::{parser::Parser, types::Fraction, Error, Pod, RawPodOwned},
};

use crate::{
    default_topic, log, object_notify,
    proxy::profiler::{
        ClockSample, DriverClockSample, FollowerNodeSample, NodeSample, Profiler, ProfilerSample,
        SampleInfo,
    },
    trace, warn, Id,
};

default_topic!(log::topic::PROTOCOL);

// No methods

#[derive(Debug, macros::Marshallable)]
pub(crate) enum Events {
    Profile(ProfileEvent),
}

/// Payload of the Profiler profile event.
#[derive(Debug, macros::PodStruct)]
pub(crate) struct ProfileEvent {
    /// The pod may contain multiple struct fields, each containing a SPA Object (key -> value
    /// mapping, allows duplicate keys). We parse it programmatically into the individual objects,
    /// and then the object properties get parsed into [`Info`], [`Clock`] etc. Documentation
    /// https://docs.pipewire.org/group__spa__param.html#ga99c6341a3416bdaacc42f77fc4869821
    pod: RawPodOwned,
}

/// Generic info, counter and CPU load
/// (Struct( Long : counter, Float : cpu_load fast, Float : cpu_load medium, Float : cpu_load slow,
///          Int : xrun-count))
#[derive(Debug, macros::PodStruct)]
pub(crate) struct Info {
    counter: i64,
    cpu_load_fast: f32,
    cpu_load_medium: f32,
    cpu_load_slow: f32,
    xrun_count: i32,
}

/// Clock information
/// (Struct( Int : clock flags, Int : clock id, String: clock name, Long : clock nsec,
///          Fraction : clock rate, Long : clock position, Long : clock duration,
///          Long : clock delay, Double : clock rate_diff, Long : clock next_nsec,
///          Int : transport_state, Int : clock cycle, Long : xrun duration))
#[derive(Debug, macros::PodStruct)]
pub(crate) struct Clock {
    flags: i32,
    id: i32,
    name: String,
    nsec: i64,
    rate: Fraction,
    position: i64,
    duration: i64,
    delay: i64,
    rate_diff: f64,
    next_nsec: i64,
    transport_state: i32,
    cycle: i32,
    xrun_duration: i64,
}

/// Follower clock information
/// (Struct( Int : clock id, String: clock name, Long : clock nsec, Fraction : clock rate,
///          Long : clock position, Long : clock duration, Long : clock delay,
///          Double : clock rate_diff, Long : clock next_nsec, Long : xrun duration))
///
/// The whole block was added in PipeWire 1.3.81, but we handle fine if it is missing. Commit:
/// https://gitlab.freedesktop.org/pipewire/pipewire/-/commit/fa1ec61cf0b1e8c07304241309c3ee5ba9268e5b
#[derive(Debug, macros::PodStruct)]
pub(crate) struct FollowerClock {
    id: i32,
    name: String,
    nsec: i64,
    rate: Fraction,
    position: i64,
    duration: i64,
    delay: i64,
    rate_diff: f64,
    next_nsec: i64,
    xrun_duration: i64,
}

/// Generic driver info block
/// (Struct( Int : driver_id, String : name, Long : driver prev_signal, Long : driver signal,
///          Long : driver awake, Long : driver finish, Int : driver status, Fraction : latency,
///          Int : xrun_count))
#[derive(Debug, macros::PodStruct)]
pub(crate) struct Driver {
    id: i32,
    name: String,
    prev_signal: i64,
    signal: i64,
    awake: i64,
    finish: i64,
    status: i32,
    latency: Fraction,
    xrun_count: i32,
}

/// Generic follower info block
/// (Struct( Int : id, String : name, Long : prev_signal, Long : signal, Long : awake,
///          Long : finish, Int : status, Fraction : latency, Int : xrun_count, Bool : async))
#[derive(Debug, macros::PodStruct)]
pub(crate) struct Follower {
    id: i32,
    name: String,
    prev_signal: i64,
    signal: i64,
    awake: i64,
    finish: i64,
    status: i32,
    latency: Fraction,
    xrun_count: i32,
    async_: bool,
}

impl Events {
    pub(crate) fn demarshal(
        message: &mut super::message::InboundMessage<'_>,
        profiler: Profiler,
    ) -> std::io::Result<()> {
        let event = message.decode::<Events>()?;

        trace!("got event: {event:?}");

        match event {
            Events::Profile(profile_event) => {
                Self::demarshal_profile_event(profiler, profile_event)?;
            }
        }

        Ok(())
    }

    fn demarshal_profile_event(
        profiler: Profiler,
        profile_event: ProfileEvent,
    ) -> std::io::Result<()> {
        let mut profile_event_parser = Parser::new(profile_event.pod.data());
        let result = profile_event_parser.pop_struct(|struct_parser| {
            let mut count = 0;
            while struct_parser.available() > 0 {
                struct_parser.pop_object(|object_parser, _id: Id| {
                    let mut info = None;
                    let mut clock = None;
                    let mut driver = None;
                    let mut followers = vec![];
                    let mut follower_clocks = vec![];

                    while let Some((key, _flags, data)) = object_parser.pop_property()? {
                        match key {
                            param::profiler::Profiler::Start
                            | param::profiler::Profiler::StartDriver
                            | param::profiler::Profiler::StartFollower
                            | param::profiler::Profiler::StartCustom => {
                                warn!("Unexpected key {key:?} when parsing Profiler profile event");
                            }
                            param::profiler::Profiler::Info => {
                                let (parsed, _size) = Info::decode(data.data())?;
                                trace!("Profiler parsing Info {data}: {parsed:?}");
                                info = Some(parsed);
                            }
                            param::profiler::Profiler::Clock => {
                                let (parsed, _size) = Clock::decode(data.data())?;
                                trace!("Profiler parsing Clock {data}: {parsed:?}");
                                clock = Some(parsed);
                            }
                            param::profiler::Profiler::DriverBlock => {
                                let (parsed, _size) = Driver::decode(data.data())?;
                                trace!("Profiler parsing DriverBlock {data}: {parsed:?}");
                                driver = Some(parsed);
                            }
                            param::profiler::Profiler::FollowerBlock => {
                                let (parsed, _size) = Follower::decode(data.data())?;
                                trace!("Profiler parsing FollowerBlock {data}: {parsed:?}");
                                followers.push(parsed);
                            }
                            param::profiler::Profiler::FollowerClock => {
                                let (parsed, _size) = FollowerClock::decode(data.data())?;
                                trace!("Profiler parsing FollowerClock {data}: {parsed:?}");
                                follower_clocks.push(parsed);
                            }
                        }
                    }

                    let info = &info
                        .ok_or_else(|| Error::Invalid("Info section not found".to_string()))?;
                    let clock = &clock
                        .ok_or_else(|| Error::Invalid("Clock section not found".to_string()))?;
                    let driver = &driver
                        .ok_or_else(|| Error::Invalid("Driver section not found".to_string()))?;

                    let followers: Vec<_> =
                        followers.iter().map(FollowerNodeSample::from).collect();
                    let follower_clocks: Vec<_> =
                        follower_clocks.iter().map(ClockSample::from).collect();
                    let profiler_sample = ProfilerSample {
                        info: info.into(),
                        driver: driver.into(),
                        driver_clock: clock.into(),
                        followers: &followers,
                        follower_clocks: &follower_clocks,
                    };

                    object_notify!(profiler, profile, &profiler_sample);

                    Ok(())
                })?;

                count += 1;
            }

            trace!("Processed {count} profile samples from a single Profiler profile event");
            Ok(())
        });
        result.map_err(|e| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Could not parse Profiler profile event as struct of SPA Objects: {e:?}"),
            )
        })?;

        Ok(())
    }
}

impl From<&Info> for SampleInfo {
    fn from(info: &Info) -> Self {
        let &Info {
            counter,
            cpu_load_fast,
            cpu_load_medium,
            cpu_load_slow,
            xrun_count,
        } = info;

        SampleInfo {
            counter,
            cpu_load_fast,
            cpu_load_medium,
            cpu_load_slow,
            // SPA PODs don't have unsigned int types; module-profiler.c passes uint32_t here.
            xrun_count: xrun_count as u32,
        }
    }
}

impl<'a> From<&'a Clock> for DriverClockSample<'a> {
    fn from(clock: &'a Clock) -> Self {
        let &Clock {
            flags,
            id,
            ref name,
            nsec,
            rate,
            position,
            duration,
            delay,
            rate_diff,
            next_nsec,
            transport_state,
            cycle,
            xrun_duration,
        } = clock;

        let clock_sample = ClockSample {
            // module-profiler.c passes unsigned ints here, cannot be represented by a SPA POD.
            id: id as u32,
            name: name.as_str(),
            nsec: nsec as u64,
            rate,
            position: position as u64,
            duration: duration as u64,
            delay,
            rate_diff,
            next_nsec: next_nsec as u64,
            xrun_duration: xrun_duration as u64,
        };
        DriverClockSample {
            clock_sample,
            flags: flags as u32,
            state: transport_state as u32,
            cycle: cycle as u32,
        }
    }
}

impl<'a> From<&'a FollowerClock> for ClockSample<'a> {
    fn from(follower_clock: &'a FollowerClock) -> Self {
        let &FollowerClock {
            id,
            ref name,
            nsec,
            rate,
            position,
            duration,
            delay,
            rate_diff,
            next_nsec,
            xrun_duration,
        } = follower_clock;

        ClockSample {
            // module-profiler.c passes unsigned ints here, cannot be represented by a SPA POD.
            id: id as u32,
            name: name.as_str(),
            nsec: nsec as u64,
            rate,
            position: position as u64,
            duration: duration as u64,
            delay,
            rate_diff,
            next_nsec: next_nsec as u64,
            xrun_duration: xrun_duration as u64,
        }
    }
}

impl<'a> From<&'a Driver> for NodeSample<'a> {
    fn from(driver: &'a Driver) -> Self {
        let &Driver {
            id,
            ref name,
            prev_signal,
            signal,
            awake,
            finish,
            status,
            latency,
            xrun_count,
        } = driver;

        NodeSample {
            // module-profiler.c passes unsigned ints here, cannot be represented by a SPA POD.
            id: id as u32,
            name: name.as_str(),
            prev_signal_time_ns: prev_signal as u64,
            signal_time_ns: signal as u64,
            awake_time_ns: awake as u64,
            finish_time_ns: finish as u64,
            status: status as u32,
            latency,
            xrun_count: xrun_count as u32,
        }
    }
}

impl<'a> From<&'a Follower> for FollowerNodeSample<'a> {
    fn from(follower: &'a Follower) -> Self {
        let &Follower {
            id,
            ref name,
            prev_signal,
            signal,
            awake,
            finish,
            status,
            latency,
            xrun_count,
            async_,
        } = follower;

        let node_sample = NodeSample {
            // module-profiler.c passes unsigned ints here, cannot be represented by a SPA POD.
            id: id as u32,
            name: name.as_str(),
            prev_signal_time_ns: prev_signal as u64,
            signal_time_ns: signal as u64,
            awake_time_ns: awake as u64,
            finish_time_ns: finish as u64,
            status: status as u32,
            latency,
            xrun_count: xrun_count as u32,
        };
        FollowerNodeSample {
            node_sample,
            async_,
        }
    }
}
