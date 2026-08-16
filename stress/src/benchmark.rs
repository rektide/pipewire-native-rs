//! Stable benchmark scenarios shared by Criterion and unit tests.

use std::str::FromStr;

use crate::{frame, memory, pod, LoadPolicy, Verified, Workload};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Profile {
    Smoke,
    Ci,
    Full,
}

impl Profile {
    pub const fn name(self) -> &'static str {
        match self {
            Self::Smoke => "smoke",
            Self::Ci => "ci",
            Self::Full => "full",
        }
    }
}

impl FromStr for Profile {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "smoke" => Ok(Self::Smoke),
            "ci" => Ok(Self::Ci),
            "full" => Ok(Self::Full),
            _ => Err(format!(
                "invalid PW_CRITERION_PROFILE {value:?}; expected smoke, ci, or full"
            )),
        }
    }
}

#[derive(Clone, Debug)]
pub enum Case {
    Frame(frame::Config),
    Pod(pod::Config),
    Memory(memory::Config),
}

impl Case {
    pub fn group(&self) -> &'static str {
        match self {
            Self::Frame(_) => "frame",
            Self::Pod(_) => "pod",
            Self::Memory(_) => "memory",
        }
    }

    /// Includes every resolved field that can affect execution.
    pub fn id(&self) -> String {
        match self {
            Self::Frame(c) => format!(
                "frames={}-payload={}-fds={}-density={}-progress={}-chunk={}-batches={}-iterations={}-seed={}",
                c.frames_per_batch,
                c.payload_bytes,
                c.fds_per_frame,
                c.fd_density,
                match c.pattern {
                    frame::ProgressPattern::Coalesced => "coalesced",
                    frame::ProgressPattern::Segmented => "segmented",
                },
                c.recv_chunk_bytes,
                c.load.batches,
                c.load.iterations,
                c.seed
            ),
            Self::Pod(c) => format!(
                "mode={}-depth={}-width={}-values={}-payload={}-batches={}-iterations={}-seed={}",
                match c.mode {
                    pod::Mode::DecodeOnly => "decode-only",
                    pod::Mode::EncodeDecode => "encode-decode",
                },
                c.depth,
                c.width,
                c.values_per_container,
                c.payload_bytes,
                c.load.batches,
                c.load.iterations,
                c.seed
            ),
            Self::Memory(c) => format!(
                "region={}-live={}-cycles={}-batches={}-iterations={}-seed={}",
                c.region_bytes,
                c.live_mappings,
                c.retire_cycles,
                c.load.batches,
                c.load.iterations,
                c.seed
            ),
        }
    }

    pub fn prepare(&self) -> Result<PreparedCase, String> {
        match self {
            Self::Frame(config) => {
                frame::FrameWorkload::validate(config)?;
                let scenario = frame::FrameWorkload::generate(config)?;
                let verified = execute_verify::<frame::FrameWorkload>(config, &scenario)?;
                Ok(PreparedCase::Frame {
                    config: config.clone(),
                    scenario,
                    operations: verified.operations,
                })
            }
            Self::Pod(config) => {
                pod::PodWorkload::validate(config)?;
                let scenario = pod::PodWorkload::generate(config)?;
                let verified = execute_verify::<pod::PodWorkload>(config, &scenario)?;
                Ok(PreparedCase::Pod {
                    config: config.clone(),
                    scenario,
                    operations: verified.operations,
                })
            }
            Self::Memory(config) => {
                memory::MemoryWorkload::validate(config)?;
                let scenario = memory::MemoryWorkload::generate(config)?;
                let verified = execute_verify::<memory::MemoryWorkload>(config, &scenario)?;
                Ok(PreparedCase::Memory {
                    config: config.clone(),
                    scenario,
                    operations: verified.operations,
                })
            }
        }
    }
}

pub enum PreparedCase {
    Frame {
        config: frame::Config,
        scenario: frame::Scenario,
        operations: usize,
    },
    Pod {
        config: pod::Config,
        scenario: pod::Scenario,
        operations: usize,
    },
    Memory {
        config: memory::Config,
        scenario: memory::Scenario,
        operations: usize,
    },
}

impl PreparedCase {
    pub fn operations(&self) -> usize {
        match self {
            Self::Frame { operations, .. }
            | Self::Pod { operations, .. }
            | Self::Memory { operations, .. } => *operations,
        }
    }

    pub fn execute_verify(&self) -> Result<Verified, String> {
        match self {
            Self::Frame {
                config, scenario, ..
            } => execute_verify::<frame::FrameWorkload>(config, scenario),
            Self::Pod {
                config, scenario, ..
            } => execute_verify::<pod::PodWorkload>(config, scenario),
            Self::Memory {
                config, scenario, ..
            } => execute_verify::<memory::MemoryWorkload>(config, scenario),
        }
    }
}

pub fn execute_verify<W: Workload>(
    config: &W::Config,
    scenario: &W::Scenario,
) -> Result<Verified, String> {
    let observation = W::execute(config, scenario)?;
    W::verify(config, scenario, &observation)
}

pub fn cases(profile: Profile) -> Vec<Case> {
    let mut cases = smoke_cases();
    if matches!(profile, Profile::Ci | Profile::Full) {
        cases.extend(ci_cases());
    }
    if profile == Profile::Full {
        cases.extend(full_cases());
    }
    cases
}

fn frame_case(
    frames: usize,
    payload: usize,
    fds: usize,
    density: usize,
    pattern: frame::ProgressPattern,
    chunk: usize,
    repeats: usize,
) -> Case {
    Case::Frame(frame::Config {
        load: LoadPolicy {
            batches: repeats,
            iterations: 1,
        },
        frames_per_batch: frames,
        payload_bytes: payload,
        fds_per_frame: fds,
        fd_density: density,
        recv_chunk_bytes: chunk,
        pattern,
        seed: 1,
    })
}

fn pod_case(
    mode: pod::Mode,
    depth: usize,
    width: usize,
    values: usize,
    payload: usize,
    repeats: usize,
) -> Case {
    Case::Pod(pod::Config {
        load: LoadPolicy {
            batches: repeats,
            iterations: 1,
        },
        depth,
        width,
        values_per_container: values,
        payload_bytes: payload,
        mode,
        seed: 2,
    })
}

fn memory_case(region: usize, live: usize, cycles: usize, repeats: usize) -> Case {
    Case::Memory(memory::Config {
        load: LoadPolicy {
            batches: repeats,
            iterations: 1,
        },
        region_bytes: region,
        live_mappings: live,
        retire_cycles: cycles,
        seed: 3,
    })
}

fn smoke_cases() -> Vec<Case> {
    vec![
        frame_case(4, 256, 0, 0, frame::ProgressPattern::Coalesced, 4096, 1),
        frame_case(4, 2048, 2, 50, frame::ProgressPattern::Segmented, 64, 1),
        pod_case(pod::Mode::DecodeOnly, 2, 3, 8, 64, 2),
        pod_case(pod::Mode::EncodeDecode, 3, 4, 16, 256, 1),
        memory_case(4096, 1, 1, 1),
        memory_case(16384, 2, 2, 1),
    ]
}

fn ci_cases() -> Vec<Case> {
    vec![
        frame_case(16, 16384, 1, 25, frame::ProgressPattern::Coalesced, 4096, 1),
        frame_case(8, 4096, 4, 100, frame::ProgressPattern::Segmented, 256, 1),
        pod_case(pod::Mode::DecodeOnly, 6, 16, 256, 4096, 16),
        pod_case(pod::Mode::EncodeDecode, 4, 8, 64, 1024, 4),
        memory_case(256 * 1024, 4, 2, 1),
        memory_case(4096, 16, 8, 1),
    ]
}

fn full_cases() -> Vec<Case> {
    vec![
        frame_case(
            64,
            65536,
            2,
            25,
            frame::ProgressPattern::Coalesced,
            32768,
            2,
        ),
        frame_case(16, 16384, 4, 100, frame::ProgressPattern::Segmented, 1, 1),
        pod_case(pod::Mode::DecodeOnly, 12, 64, 2048, 65536, 32),
        pod_case(pod::Mode::EncodeDecode, 8, 32, 512, 16384, 8),
        memory_case(4 * 1024 * 1024, 8, 4, 1),
        memory_case(64 * 1024, 64, 16, 1),
    ]
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;

    #[test]
    fn profiles_expand_monotonically_with_stable_unique_ids() {
        assert_eq!(cases(Profile::Smoke).len(), 6);
        assert_eq!(cases(Profile::Ci).len(), 12);
        assert_eq!(cases(Profile::Full).len(), 18);
        for profile in [Profile::Smoke, Profile::Ci, Profile::Full] {
            let cases = cases(profile);
            let ids: HashSet<_> = cases
                .iter()
                .map(|case| format!("{}/{}", case.group(), case.id()))
                .collect();
            assert_eq!(ids.len(), cases.len());
        }
    }

    #[test]
    fn invalid_profile_is_clear() {
        assert_eq!("ci".parse::<Profile>().unwrap(), Profile::Ci);
        assert!("quick"
            .parse::<Profile>()
            .unwrap_err()
            .contains("smoke, ci, or full"));
    }

    #[test]
    fn operation_counts_and_execute_verify_fixture_match() {
        for case in cases(Profile::Smoke) {
            let expected = match &case {
                Case::Frame(c) => c.load.operations(c.frames_per_batch).unwrap(),
                Case::Pod(c) => c.load.operations(1).unwrap(),
                Case::Memory(c) => c
                    .load
                    .operations(c.retire_cycles * c.live_mappings)
                    .unwrap(),
            };
            let prepared = case.prepare().unwrap();
            assert_eq!(prepared.operations(), expected);
            assert_eq!(prepared.execute_verify().unwrap().operations, expected);
        }
    }
}
