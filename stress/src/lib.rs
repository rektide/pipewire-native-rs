//! Reusable correctness-first stress workloads for pipewire-native subsystems.

pub mod frame;
pub mod memory;
pub mod pod;
pub mod report;

use std::hint::black_box;

/// Named starting points. Explicit CLI values are applied after the preset.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, clap::ValueEnum)]
pub enum Preset {
    #[default]
    Smoke,
    Medium,
    Large,
    Pathological,
}

/// Iteration policy shared by workloads.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LoadPolicy {
    pub batches: usize,
    pub iterations: usize,
}

impl LoadPolicy {
    pub fn operations(self, per_batch: usize) -> Result<usize, String> {
        checked_product(
            &[self.batches, self.iterations, per_batch],
            "operation count",
        )
    }

    pub fn validate(self) -> Result<(), String> {
        nonzero(self.batches, "batches")?;
        nonzero(self.iterations, "iterations")
    }
}

/// Generation, production execution, and independent verification are separate,
/// injectable stages. Unit tests and CLI runners call the same `run_workload` path.
pub trait Workload {
    type Config;
    type Scenario;
    type Observation;

    fn validate(config: &Self::Config) -> Result<(), String>;
    fn generate(config: &Self::Config) -> Result<Self::Scenario, String>;
    fn execute(
        config: &Self::Config,
        scenario: &Self::Scenario,
    ) -> Result<Self::Observation, String>;
    fn verify(
        config: &Self::Config,
        scenario: &Self::Scenario,
        observation: &Self::Observation,
    ) -> Result<Verified, String>;
}

/// Correctness values emitted by every successful workload.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Verified {
    pub operations: usize,
    pub bytes: usize,
    pub checksum: u64,
    pub aux_count: usize,
}

/// Runs all stages and only black-boxes values after verification succeeds.
pub fn run_workload<W: Workload>(config: &W::Config) -> Result<Verified, String> {
    W::validate(config)?;
    let scenario = W::generate(config)?;
    let observation = W::execute(config, &scenario)?;
    let verified = W::verify(config, &scenario, &observation)?;
    Ok(black_box(verified))
}

pub fn checked_product(values: &[usize], label: &str) -> Result<usize, String> {
    values.iter().try_fold(1usize, |product, value| {
        product
            .checked_mul(*value)
            .ok_or_else(|| format!("{label} is too large"))
    })
}

pub fn checked_sum(values: &[usize], label: &str) -> Result<usize, String> {
    values.iter().try_fold(0usize, |sum, value| {
        sum.checked_add(*value)
            .ok_or_else(|| format!("{label} is too large"))
    })
}

pub fn nonzero(value: usize, label: &str) -> Result<(), String> {
    if value == 0 {
        Err(format!("{label} must be greater than zero"))
    } else {
        Ok(())
    }
}

/// Stable, inexpensive checksum used for generated and observed data.
pub fn checksum(bytes: &[u8]) -> u64 {
    bytes.iter().fold(0xcbf2_9ce4_8422_2325, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(0x0000_0100_0000_01b3)
    })
}

pub fn mix(state: &mut u64) -> u64 {
    *state ^= *state << 13;
    *state ^= *state >> 7;
    *state ^= *state << 17;
    *state
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn arithmetic_and_generation_are_deterministic() {
        assert_eq!(checked_product(&[2, 3, 4], "test").unwrap(), 24);
        assert!(checked_product(&[usize::MAX, 2], "test").is_err());
        let mut first = 42;
        let mut second = 42;
        assert_eq!(mix(&mut first), mix(&mut second));
        assert_eq!(checksum(b"same"), checksum(b"same"));
    }
}
