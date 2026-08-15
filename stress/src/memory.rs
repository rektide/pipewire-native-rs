use pipewire_native_node::session::memory::{
    MemoryError, MemoryId, MemoryKey, MemoryMapping, MemoryPool,
};
use pipewire_native_node::shm::{create_memfd, ShrinkPolicy};
use pipewire_native_spa::buffer::data_type;

use crate::{
    checked_product, checksum, mix, nonzero, run_workload, LoadPolicy, Preset, Verified, Workload,
};

const MAX_LIVE_MAPPINGS: usize = 4096;
const MAX_LIVE_BYTES: usize = 2 * 1024 * 1024 * 1024;

#[derive(Clone, Debug)]
pub struct Config {
    pub load: LoadPolicy,
    pub region_bytes: usize,
    pub live_mappings: usize,
    pub retire_cycles: usize,
    pub seed: u64,
}

#[derive(Clone, Debug, Default)]
pub struct Overrides {
    pub batches: Option<usize>,
    pub iterations: Option<usize>,
    pub region_bytes: Option<usize>,
    pub live_mappings: Option<usize>,
    pub retire_cycles: Option<usize>,
    pub seed: Option<u64>,
}

impl Config {
    pub fn resolve(preset: Preset, o: Overrides) -> Self {
        let mut c = match preset {
            Preset::Smoke => Self {
                load: LoadPolicy {
                    batches: 1,
                    iterations: 1,
                },
                region_bytes: 4096,
                live_mappings: 2,
                retire_cycles: 2,
                seed: 3,
            },
            Preset::Medium => Self {
                load: LoadPolicy {
                    batches: 4,
                    iterations: 4,
                },
                region_bytes: 256 * 1024,
                live_mappings: 16,
                retire_cycles: 8,
                seed: 3,
            },
            Preset::Large => Self {
                load: LoadPolicy {
                    batches: 8,
                    iterations: 4,
                },
                region_bytes: 4 * 1024 * 1024,
                live_mappings: 32,
                retire_cycles: 16,
                seed: 3,
            },
            Preset::Pathological => Self {
                load: LoadPolicy {
                    batches: 2,
                    iterations: 2,
                },
                region_bytes: 64 * 1024 * 1024,
                live_mappings: 16,
                retire_cycles: 32,
                seed: 3,
            },
        };
        c.load.batches = o.batches.unwrap_or(c.load.batches);
        c.load.iterations = o.iterations.unwrap_or(c.load.iterations);
        c.region_bytes = o.region_bytes.unwrap_or(c.region_bytes);
        c.live_mappings = o.live_mappings.unwrap_or(c.live_mappings);
        c.retire_cycles = o.retire_cycles.unwrap_or(c.retire_cycles);
        c.seed = o.seed.unwrap_or(c.seed);
        c
    }
}

pub struct Scenario {
    cycles: usize,
}
pub struct Observation {
    mappings: usize,
    bytes: usize,
    checksum: u64,
    baseline_fds: usize,
    final_fds: usize,
    generations: usize,
}
pub struct MemoryWorkload;
pub fn run(config: &Config) -> Result<Verified, String> {
    run_workload::<MemoryWorkload>(config)
}

impl Workload for MemoryWorkload {
    type Config = Config;
    type Scenario = Scenario;
    type Observation = Observation;
    fn validate(c: &Config) -> Result<(), String> {
        c.load.validate()?;
        nonzero(c.region_bytes, "region-bytes")?;
        nonzero(c.live_mappings, "live-mappings")?;
        nonzero(c.retire_cycles, "retire-cycles")?;
        if c.live_mappings > MAX_LIVE_MAPPINGS {
            return Err(format!(
                "live-mappings exceeds safety limit {MAX_LIVE_MAPPINGS}"
            ));
        }
        let live_bytes = checked_product(&[c.region_bytes, c.live_mappings], "live mapped bytes")?;
        if live_bytes > MAX_LIVE_BYTES {
            return Err(format!(
                "live mapped bytes exceeds safety limit {MAX_LIVE_BYTES}"
            ));
        }
        checked_product(
            &[
                c.load.batches,
                c.load.iterations,
                c.retire_cycles,
                c.live_mappings,
            ],
            "mapping operation count",
        )?;
        Ok(())
    }
    fn generate(c: &Config) -> Result<Scenario, String> {
        Ok(Scenario {
            cycles: checked_product(
                &[c.load.batches, c.load.iterations, c.retire_cycles],
                "retire cycle count",
            )?,
        })
    }
    fn execute(c: &Config, s: &Scenario) -> Result<Observation, String> {
        let baseline_fds = fd_count()?;
        let mut pool = MemoryPool::new(ShrinkPolicy::RequireSealed);
        let mut state = c.seed;
        let mut mappings_count = 0usize;
        let mut bytes_count = 0usize;
        let mut observed_checksum = 0u64;
        let mut previous: Vec<Option<MemoryKey>> = vec![None; c.live_mappings];
        let mut generations = 0usize;
        for cycle in 0..s.cycles {
            let mut mappings: Vec<(MemoryId, MemoryMapping)> = Vec::with_capacity(c.live_mappings);
            for (slot, previous_key) in previous.iter_mut().enumerate() {
                let id = MemoryId(u32::try_from(slot).map_err(|_| "mapping ID exceeds u32")?);
                let fd = create_memfd(&format!("pw-stress-memory-{slot}"), c.region_bytes)
                    .map_err(|e| e.to_string())?;
                let key = pool
                    .add(id, data_type::MEM_FD, cycle as u32, fd)
                    .map_err(|e| e.to_string())?;
                if let Some(old) = *previous_key {
                    if key.generation <= old.generation {
                        return Err("memory generation did not increase".into());
                    }
                    if !matches!(pool.map(old, 0, 1, false), Err(MemoryError::StaleGeneration { active: Some(active), .. }) if active == key)
                    {
                        return Err("old generation was not rejected after ID reuse".into());
                    }
                }
                let mapping = pool
                    .map(key, 0, c.region_bytes, true)
                    .map_err(|e| e.to_string())?;
                if mapping.key() != key || mapping.len() != c.region_bytes {
                    return Err("mapped metadata mismatch".into());
                }
                *previous_key = Some(key);
                generations = generations
                    .checked_add(1)
                    .ok_or("generation count overflow")?;
                mappings.push((id, mapping));
            }
            if pool.len() != c.live_mappings {
                return Err("active memory pool count mismatch".into());
            }
            for (id, mut mapping) in mappings {
                let (sum, len) = {
                    let mut guard = mapping.borrow();
                    let (sum, len) = {
                        let bytes = unsafe { guard.bytes_mut() };
                        for byte in bytes.iter_mut() {
                            *byte = mix(&mut state) as u8;
                        }
                        (checksum(bytes), bytes.len())
                    };
                    if checksum(unsafe { guard.bytes() }) != sum {
                        return Err("mapped write/read checksum mismatch".into());
                    }
                    (sum, len)
                };
                observed_checksum = observed_checksum.wrapping_add(sum);
                bytes_count = bytes_count
                    .checked_add(len)
                    .ok_or("mapped byte count overflow")?;
                pool.remove(id).map_err(|e| e.to_string())?;
                if pool.resolve(id).is_ok() {
                    return Err("retired memory ID still resolves".into());
                }
                mappings_count = mappings_count
                    .checked_add(1)
                    .ok_or("mapping count overflow")?;
            }
            if !pool.is_empty() {
                return Err("memory pool not empty after retire cycle".into());
            }
        }
        drop(pool);
        let final_fds = fd_count()?;
        Ok(Observation {
            mappings: mappings_count,
            bytes: bytes_count,
            checksum: observed_checksum,
            baseline_fds,
            final_fds,
            generations,
        })
    }
    fn verify(c: &Config, s: &Scenario, o: &Observation) -> Result<Verified, String> {
        let expected_mappings =
            checked_product(&[s.cycles, c.live_mappings], "expected mapping count")?;
        let expected_bytes = checked_product(
            &[expected_mappings, c.region_bytes],
            "expected mapped bytes",
        )?;
        if o.mappings != expected_mappings
            || o.generations != expected_mappings
            || o.bytes != expected_bytes
        {
            return Err("memory count verification failed".into());
        }
        if o.final_fds != o.baseline_fds {
            return Err(format!(
                "FD leak detected: baseline={} final={}",
                o.baseline_fds, o.final_fds
            ));
        }
        Ok(Verified {
            operations: expected_mappings,
            bytes: expected_bytes,
            checksum: o.checksum,
            aux_count: o.generations,
        })
    }
}

fn fd_count() -> Result<usize, String> {
    std::fs::read_dir("/proc/self/fd")
        .map_err(|e| e.to_string())?
        .try_fold(0usize, |n, entry| {
            let entry = entry.map_err(|e| e.to_string())?;
            let is_stress_fd = std::fs::read_link(entry.path())
                .is_ok_and(|target| target.to_string_lossy().contains("pw-stress-memory-"));
            n.checked_add(usize::from(is_stress_fd))
                .ok_or_else(|| "FD count overflow".into())
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn generations_mapping_and_fd_baseline() {
        let c = Config::resolve(
            Preset::Smoke,
            Overrides {
                region_bytes: Some(257),
                ..Overrides::default()
            },
        );
        let result = run(&c).unwrap();
        assert_eq!(result.operations, 4);
        assert_eq!(result.bytes, 1028);
    }
    #[test]
    fn verification_detects_leak_or_bad_counts() {
        let c = Config::resolve(Preset::Smoke, Overrides::default());
        let s = MemoryWorkload::generate(&c).unwrap();
        let o = Observation {
            mappings: 0,
            bytes: 0,
            checksum: 0,
            baseline_fds: 1,
            final_fds: 2,
            generations: 0,
        };
        assert!(MemoryWorkload::verify(&c, &s, &o).is_err());
    }
}
