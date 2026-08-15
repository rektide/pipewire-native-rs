use pipewire_native_spa::pod::builder::{Builder, StructBuilder};
use pipewire_native_spa::pod::parser::Parser;
use pipewire_native_spa::pod::types::Choice;

use crate::{
    checked_product, checksum, mix, nonzero, run_workload, LoadPolicy, Preset, Verified, Workload,
};

const MAX_POD_BYTES: usize = 512 * 1024 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq, clap::ValueEnum)]
pub enum Mode {
    EncodeDecode,
    DecodeOnly,
}

#[derive(Clone, Debug)]
pub struct Config {
    pub load: LoadPolicy,
    pub depth: usize,
    pub width: usize,
    pub values_per_container: usize,
    pub payload_bytes: usize,
    pub mode: Mode,
    pub seed: u64,
}

#[derive(Clone, Debug, Default)]
pub struct Overrides {
    pub batches: Option<usize>,
    pub iterations: Option<usize>,
    pub depth: Option<usize>,
    pub width: Option<usize>,
    pub values_per_container: Option<usize>,
    pub payload_bytes: Option<usize>,
    pub mode: Option<Mode>,
    pub seed: Option<u64>,
}

impl Config {
    pub fn resolve(preset: Preset, o: Overrides) -> Self {
        let mut c = match preset {
            Preset::Smoke => Self {
                load: LoadPolicy {
                    batches: 2,
                    iterations: 1,
                },
                depth: 2,
                width: 3,
                values_per_container: 8,
                payload_bytes: 64,
                mode: Mode::EncodeDecode,
                seed: 2,
            },
            Preset::Medium => Self {
                load: LoadPolicy {
                    batches: 64,
                    iterations: 8,
                },
                depth: 4,
                width: 16,
                values_per_container: 128,
                payload_bytes: 4096,
                mode: Mode::EncodeDecode,
                seed: 2,
            },
            Preset::Large => Self {
                load: LoadPolicy {
                    batches: 128,
                    iterations: 16,
                },
                depth: 8,
                width: 64,
                values_per_container: 1024,
                payload_bytes: 64 * 1024,
                mode: Mode::EncodeDecode,
                seed: 2,
            },
            Preset::Pathological => Self {
                load: LoadPolicy {
                    batches: 16,
                    iterations: 4,
                },
                depth: 32,
                width: 128,
                values_per_container: 16 * 1024,
                payload_bytes: 1024 * 1024,
                mode: Mode::DecodeOnly,
                seed: 2,
            },
        };
        c.load.batches = o.batches.unwrap_or(c.load.batches);
        c.load.iterations = o.iterations.unwrap_or(c.load.iterations);
        c.depth = o.depth.unwrap_or(c.depth);
        c.width = o.width.unwrap_or(c.width);
        c.values_per_container = o.values_per_container.unwrap_or(c.values_per_container);
        c.payload_bytes = o.payload_bytes.unwrap_or(c.payload_bytes);
        c.mode = o.mode.unwrap_or(c.mode);
        c.seed = o.seed.unwrap_or(c.seed);
        c
    }
}

pub struct Scenario {
    encoded: Vec<u8>,
    semantic_checksum: u64,
}
pub struct Observation {
    operations: usize,
    bytes: usize,
    semantic_checksum: u64,
}
pub struct PodWorkload;
pub fn run(config: &Config) -> Result<Verified, String> {
    run_workload::<PodWorkload>(config)
}

impl Workload for PodWorkload {
    type Config = Config;
    type Scenario = Scenario;
    type Observation = Observation;
    fn validate(c: &Config) -> Result<(), String> {
        c.load.validate()?;
        nonzero(c.depth, "depth")?;
        nonzero(c.width, "width")?;
        nonzero(c.values_per_container, "values-per-container")?;
        nonzero(c.payload_bytes, "payload-bytes")?;
        if c.depth > 64 {
            return Err("depth exceeds safe recursion limit 64".into());
        }
        let estimate = checked_product(
            &[
                c.depth,
                c.width
                    .checked_mul(16)
                    .ok_or("POD estimate overflow")?
                    .checked_add(c.payload_bytes)
                    .and_then(|v| v.checked_add(c.values_per_container.checked_mul(8)?))
                    .ok_or("POD estimate overflow")?,
            ],
            "POD allocation estimate",
        )?;
        if estimate > MAX_POD_BYTES {
            return Err(format!(
                "POD allocation estimate exceeds safety limit {MAX_POD_BYTES}"
            ));
        }
        checked_product(&[c.load.batches, c.load.iterations], "POD operation count")?;
        Ok(())
    }
    fn generate(c: &Config) -> Result<Scenario, String> {
        let (encoded, semantic_checksum) = encode(c)?;
        Ok(Scenario {
            encoded,
            semantic_checksum,
        })
    }
    fn execute(c: &Config, s: &Scenario) -> Result<Observation, String> {
        let operations =
            checked_product(&[c.load.batches, c.load.iterations], "POD operation count")?;
        let mut total_checksum = 0u64;
        let mut bytes = 0usize;
        for _ in 0..operations {
            let owned;
            let input = if c.mode == Mode::EncodeDecode {
                owned = encode(c)?.0;
                owned.as_slice()
            } else {
                s.encoded.as_slice()
            };
            let decoded = decode(c, input)?;
            if decoded != s.semantic_checksum {
                return Err(format!(
                    "POD semantic checksum mismatch: {decoded} != {}",
                    s.semantic_checksum
                ));
            }
            total_checksum = total_checksum.wrapping_add(decoded);
            bytes = bytes
                .checked_add(input.len())
                .ok_or("decoded byte count overflow")?;
        }
        Ok(Observation {
            operations,
            bytes,
            semantic_checksum: total_checksum,
        })
    }
    fn verify(_: &Config, s: &Scenario, o: &Observation) -> Result<Verified, String> {
        let expected = s.semantic_checksum.wrapping_mul(o.operations as u64);
        if o.semantic_checksum != expected {
            return Err("POD aggregate checksum verification failed".into());
        }
        Ok(Verified {
            operations: o.operations,
            bytes: o.bytes,
            checksum: expected,
            aux_count: s.encoded.len(),
        })
    }
}

fn encode(c: &Config) -> Result<(Vec<u8>, u64), String> {
    let per_level = c
        .width
        .checked_mul(16)
        .and_then(|v| v.checked_add(c.payload_bytes))
        .and_then(|v| v.checked_add(c.values_per_container.checked_mul(8)?))
        .ok_or("POD buffer size overflow")?;
    let capacity = c
        .depth
        .checked_mul(per_level)
        .and_then(|v| v.checked_add(4096))
        .ok_or("POD buffer size overflow")?;
    let mut buffer = vec![0u8; capacity];
    let mut state = c.seed;
    let mut semantic = 0xcbf2_9ce4_8422_2325;
    let built = Builder::new(&mut buffer)
        .push_struct(|b| build_level(b, c.depth, c, &mut state, &mut semantic))
        .build()
        .map_err(|e| format!("{e:?}"))?;
    let len = built.len();
    buffer.truncate(len);
    Ok((buffer, semantic))
}

fn build_level<'a>(
    mut b: StructBuilder<'a>,
    depth: usize,
    c: &Config,
    state: &mut u64,
    semantic: &mut u64,
) -> StructBuilder<'a> {
    for _ in 0..c.width {
        let value = mix(state) as i32;
        *semantic = semantic.wrapping_add(value as u32 as u64);
        b = b.push_int(value);
    }
    let mut blob = vec![0u8; c.payload_bytes];
    for byte in &mut blob {
        *byte = mix(state) as u8;
    }
    *semantic = semantic.wrapping_add(checksum(&blob));
    b = b.push_bytes(&blob);
    let text_len = c.payload_bytes.min(1024);
    let text = format!("{:016x}", mix(state)).repeat(text_len.div_ceil(16));
    let text = &text[..text_len];
    *semantic = semantic.wrapping_add(checksum(text.as_bytes()));
    b = b.push_string(text);
    let mut values = Vec::with_capacity(c.values_per_container);
    for _ in 0..c.values_per_container {
        let value = mix(state) as i32;
        *semantic = semantic.wrapping_add(value as u32 as u64);
        values.push(value);
    }
    b = b.push_array(&values);
    let default = mix(state) as i64;
    *semantic = semantic.wrapping_add(default as u64);
    b = b.push_choice(Choice::Range {
        default,
        min: default.wrapping_sub(1),
        max: default.wrapping_add(1),
    });
    if depth > 1 {
        b = b.push_struct(|nested| build_level(nested, depth - 1, c, state, semantic));
    }
    b
}

fn decode(c: &Config, bytes: &[u8]) -> Result<u64, String> {
    let mut parser = Parser::new(bytes);
    let mut semantic = 0xcbf2_9ce4_8422_2325;
    parser
        .pop_struct(|p| decode_level(p, c.depth, c, &mut semantic))
        .map_err(|e| format!("{e:?}"))?;
    if parser.available() != 0 {
        return Err("POD parser left trailing bytes".into());
    }
    Ok(semantic)
}

fn decode_level(
    parser: &mut Parser<'_>,
    depth: usize,
    c: &Config,
    semantic: &mut u64,
) -> Result<(), pipewire_native_spa::pod::Error> {
    for _ in 0..c.width {
        let value = parser.pop_int()?;
        *semantic = semantic.wrapping_add(value as u32 as u64);
    }
    let blob = parser.pop_bytes()?;
    *semantic = semantic.wrapping_add(checksum(&blob));
    let text = parser.pop_string()?;
    *semantic = semantic.wrapping_add(checksum(text.as_bytes()));
    for value in parser.pop_array::<i32>()? {
        *semantic = semantic.wrapping_add(value as u32 as u64);
    }
    let choice = parser.pop_choice::<i64>()?;
    if let Choice::Range { default, .. } = choice {
        *semantic = semantic.wrapping_add(default as u64);
    }
    if depth > 1 {
        parser.pop_struct(|nested| decode_level(nested, depth - 1, c, semantic))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn preset_overrides_and_round_trip() {
        let c = Config::resolve(
            Preset::Smoke,
            Overrides {
                batches: Some(1),
                depth: Some(3),
                ..Overrides::default()
            },
        );
        assert_eq!(c.depth, 3);
        let a = PodWorkload::generate(&c).unwrap();
        let b = PodWorkload::generate(&c).unwrap();
        assert_eq!(a.encoded, b.encoded);
        assert_eq!(run(&c).unwrap().operations, 1);
    }
    #[test]
    fn corrupted_pod_fails() {
        let c = Config::resolve(
            Preset::Smoke,
            Overrides {
                mode: Some(Mode::DecodeOnly),
                ..Overrides::default()
            },
        );
        let mut s = PodWorkload::generate(&c).unwrap();
        s.encoded[4..8].fill(0);
        assert!(PodWorkload::execute(&c, &s).is_err());
    }
}
