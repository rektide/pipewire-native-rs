use std::os::fd::{AsFd, FromRawFd, OwnedFd};
use std::os::unix::net::UnixStream;

use pipewire_native_protocol::native::frame::{
    FlushOutcome, FrameLimits, FrameReceiver, FrameSender, OutboundFrame, ReceiveOutcome,
    HEADER_LEN, WIRE_MAX_PAYLOAD,
};

use crate::{
    checked_product, checked_sum, checksum, mix, nonzero, run_workload, LoadPolicy, Preset,
    Verified, Workload,
};

const MAX_BATCH_BYTES: usize = 512 * 1024 * 1024;
const MAX_BATCH_FDS: usize = 4096;

#[derive(Clone, Copy, Debug, Eq, PartialEq, clap::ValueEnum)]
pub enum ProgressPattern {
    Coalesced,
    Segmented,
}

#[derive(Clone, Debug)]
pub struct Config {
    pub load: LoadPolicy,
    pub frames_per_batch: usize,
    pub payload_bytes: usize,
    pub fds_per_frame: usize,
    pub fd_density: usize,
    pub recv_chunk_bytes: usize,
    pub pattern: ProgressPattern,
    pub seed: u64,
}

#[derive(Clone, Debug, Default)]
pub struct Overrides {
    pub batches: Option<usize>,
    pub iterations: Option<usize>,
    pub frames_per_batch: Option<usize>,
    pub payload_bytes: Option<usize>,
    pub fds_per_frame: Option<usize>,
    pub fd_density: Option<usize>,
    pub recv_chunk_bytes: Option<usize>,
    pub pattern: Option<ProgressPattern>,
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
                frames_per_batch: 16,
                payload_bytes: 1024,
                fds_per_frame: 1,
                fd_density: 25,
                recv_chunk_bytes: 256,
                pattern: ProgressPattern::Segmented,
                seed: 1,
            },
            Preset::Medium => Self {
                load: LoadPolicy {
                    batches: 8,
                    iterations: 4,
                },
                frames_per_batch: 128,
                payload_bytes: 16 * 1024,
                fds_per_frame: 2,
                fd_density: 20,
                recv_chunk_bytes: 4096,
                pattern: ProgressPattern::Coalesced,
                seed: 1,
            },
            Preset::Large => Self {
                load: LoadPolicy {
                    batches: 16,
                    iterations: 8,
                },
                frames_per_batch: 256,
                payload_bytes: 64 * 1024,
                fds_per_frame: 2,
                fd_density: 25,
                recv_chunk_bytes: 32 * 1024,
                pattern: ProgressPattern::Coalesced,
                seed: 1,
            },
            Preset::Pathological => Self {
                load: LoadPolicy {
                    batches: 4,
                    iterations: 2,
                },
                frames_per_batch: 128,
                payload_bytes: 1024 * 1024,
                fds_per_frame: 4,
                fd_density: 100,
                recv_chunk_bytes: 1,
                pattern: ProgressPattern::Segmented,
                seed: 1,
            },
        };
        c.load.batches = o.batches.unwrap_or(c.load.batches);
        c.load.iterations = o.iterations.unwrap_or(c.load.iterations);
        c.frames_per_batch = o.frames_per_batch.unwrap_or(c.frames_per_batch);
        c.payload_bytes = o.payload_bytes.unwrap_or(c.payload_bytes);
        c.fds_per_frame = o.fds_per_frame.unwrap_or(c.fds_per_frame);
        c.fd_density = o.fd_density.unwrap_or(c.fd_density);
        c.recv_chunk_bytes = o.recv_chunk_bytes.unwrap_or(c.recv_chunk_bytes);
        c.pattern = o.pattern.unwrap_or(c.pattern);
        c.seed = o.seed.unwrap_or(c.seed);
        c
    }
}

#[derive(Clone, Debug)]
pub struct FrameSpec {
    payload: Vec<u8>,
    fds: usize,
    checksum: u64,
}
pub struct Scenario {
    frames: Vec<FrameSpec>,
    expected_bytes: usize,
    expected_fds: usize,
    expected_checksum: u64,
}
pub struct Observation {
    frames: usize,
    bytes: usize,
    fds: usize,
    checksum: u64,
}
pub struct FrameWorkload;

pub fn run(config: &Config) -> Result<Verified, String> {
    run_workload::<FrameWorkload>(config)
}

impl Workload for FrameWorkload {
    type Config = Config;
    type Scenario = Scenario;
    type Observation = Observation;

    fn validate(c: &Config) -> Result<(), String> {
        c.load.validate()?;
        nonzero(c.frames_per_batch, "frames-per-batch")?;
        nonzero(c.payload_bytes, "payload-bytes")?;
        nonzero(c.recv_chunk_bytes, "recv-chunk-bytes")?;
        if c.payload_bytes > WIRE_MAX_PAYLOAD {
            return Err(format!(
                "payload-bytes exceeds wire limit {WIRE_MAX_PAYLOAD}"
            ));
        }
        if c.fd_density > 100 {
            return Err("fd-density must be between 0 and 100".into());
        }
        let batch_bytes = checked_product(
            &[
                c.frames_per_batch,
                checked_sum(&[HEADER_LEN, c.payload_bytes], "frame bytes")?,
            ],
            "batch bytes",
        )?;
        if batch_bytes > MAX_BATCH_BYTES {
            return Err(format!(
                "batch allocation exceeds safety limit {MAX_BATCH_BYTES}"
            ));
        }
        let batch_fds = checked_product(&[c.frames_per_batch, c.fds_per_frame], "batch FD count")?;
        if batch_fds > MAX_BATCH_FDS {
            return Err(format!(
                "batch FD count exceeds safety limit {MAX_BATCH_FDS}"
            ));
        }
        c.load.operations(c.frames_per_batch)?;
        Ok(())
    }

    fn generate(c: &Config) -> Result<Scenario, String> {
        let mut state = c.seed;
        let mut frames = Vec::with_capacity(c.frames_per_batch);
        let mut expected_bytes = 0usize;
        let mut expected_fds = 0usize;
        let mut expected_checksum = 0u64;
        for index in 0..c.frames_per_batch {
            let size = 1 + (mix(&mut state) as usize % c.payload_bytes);
            let mut payload = vec![0; size];
            for byte in &mut payload {
                *byte = mix(&mut state) as u8;
            }
            let fds = if (mix(&mut state) % 100) < c.fd_density as u64 {
                c.fds_per_frame
            } else {
                0
            };
            let sum = checksum(&payload) ^ index as u64;
            expected_bytes = expected_bytes
                .checked_add(size)
                .ok_or("expected byte count overflow")?;
            expected_fds = expected_fds
                .checked_add(fds)
                .ok_or("expected FD count overflow")?;
            expected_checksum = expected_checksum.wrapping_add(sum);
            frames.push(FrameSpec {
                payload,
                fds,
                checksum: sum,
            });
        }
        Ok(Scenario {
            frames,
            expected_bytes,
            expected_fds,
            expected_checksum,
        })
    }

    fn execute(c: &Config, s: &Scenario) -> Result<Observation, String> {
        let repeats = checked_product(&[c.load.batches, c.load.iterations], "repeat count")?;
        let mut observed = Observation {
            frames: 0,
            bytes: 0,
            fds: 0,
            checksum: 0,
        };
        for _ in 0..repeats {
            transport_batch(c, s, &mut observed)?;
        }
        Ok(observed)
    }

    fn verify(c: &Config, s: &Scenario, o: &Observation) -> Result<Verified, String> {
        let repeats = checked_product(&[c.load.batches, c.load.iterations], "repeat count")?;
        let expected_frames = checked_product(&[repeats, s.frames.len()], "frame count")?;
        let expected_bytes = checked_product(&[repeats, s.expected_bytes], "byte count")?;
        let expected_fds = checked_product(&[repeats, s.expected_fds], "FD count")?;
        let expected_checksum = s.expected_checksum.wrapping_mul(repeats as u64);
        if (o.frames, o.bytes, o.fds, o.checksum)
            != (
                expected_frames,
                expected_bytes,
                expected_fds,
                expected_checksum,
            )
        {
            return Err(format!("frame verification failed: observed=({},{},{},{}) expected=({expected_frames},{expected_bytes},{expected_fds},{expected_checksum})", o.frames, o.bytes, o.fds, o.checksum));
        }
        Ok(Verified {
            operations: expected_frames,
            bytes: expected_bytes,
            checksum: expected_checksum,
            aux_count: expected_fds,
        })
    }
}

fn transport_batch(c: &Config, s: &Scenario, o: &mut Observation) -> Result<(), String> {
    let (tx, rx) = UnixStream::pair().map_err(|e| e.to_string())?;
    tx.set_nonblocking(true).map_err(|e| e.to_string())?;
    rx.set_nonblocking(true).map_err(|e| e.to_string())?;
    let queue_bytes = checked_product(
        &[c.frames_per_batch, HEADER_LEN + c.payload_bytes],
        "send queue bytes",
    )?;
    let queue_fds = checked_product(&[c.frames_per_batch, c.fds_per_frame], "send queue FDs")?;
    let limits = FrameLimits {
        max_payload: c.payload_bytes,
        max_frame_fds: c.fds_per_frame,
        max_pending_fds: queue_fds.max(1),
        recv_chunk_bytes: c.recv_chunk_bytes,
        recv_control_fds: queue_fds.max(1),
        max_queued_bytes: queue_bytes,
        max_queued_fds: queue_fds,
    };
    let mut sender = FrameSender::new(limits);
    let mut receiver = FrameReceiver::new(limits);
    let mut sent = 0usize;
    let mut received = 0usize;
    while received < s.frames.len() {
        let enqueue_limit = if c.pattern == ProgressPattern::Segmented {
            (received + 1).min(s.frames.len())
        } else {
            s.frames.len()
        };
        while sent < enqueue_limit {
            let spec = &s.frames[sent];
            let mut fds = Vec::with_capacity(spec.fds);
            for _ in 0..spec.fds {
                fds.push(eventfd()?);
            }
            let frame = OutboundFrame::new(
                7,
                (sent % 255) as u8,
                sent as u32,
                spec.payload.clone(),
                fds,
                limits,
            )
            .map_err(|e| e.to_string())?;
            sender.enqueue(frame).map_err(|e| e.to_string())?;
            sent += 1;
        }
        let _ = sender.flush(tx.as_fd()).map_err(|e| e.to_string())?;
        loop {
            match receiver.receive(rx.as_fd()).map_err(|e| e.to_string())? {
                ReceiveOutcome::Frame(frame) => {
                    let spec = &s.frames[received];
                    let actual = checksum(frame.payload()) ^ received as u64;
                    if frame.payload() != spec.payload
                        || actual != spec.checksum
                        || frame.fds().len() != spec.fds
                    {
                        return Err(format!("frame {received} content or FD mismatch"));
                    }
                    o.frames += 1;
                    o.bytes += frame.payload().len();
                    o.fds += frame.fds().len();
                    o.checksum = o.checksum.wrapping_add(actual);
                    received += 1;
                    if c.pattern == ProgressPattern::Segmented || received == s.frames.len() {
                        break;
                    }
                }
                ReceiveOutcome::WouldBlock => break,
                ReceiveOutcome::Closed => {
                    return Err("transport closed before all frames arrived".into())
                }
            }
        }
        if sent == s.frames.len()
            && received < s.frames.len()
            && sender.flush(tx.as_fd()).map_err(|e| e.to_string())? == FlushOutcome::WouldBlock
        {
            std::thread::yield_now();
        }
    }
    if !sender.is_empty() || receiver.pending_fds() != 0 {
        return Err("transport retained queued bytes or FDs".into());
    }
    Ok(())
}

fn eventfd() -> Result<OwnedFd, String> {
    let fd = unsafe { libc::eventfd(0, libc::EFD_CLOEXEC | libc::EFD_NONBLOCK) };
    if fd < 0 {
        Err(std::io::Error::last_os_error().to_string())
    } else {
        Ok(unsafe { OwnedFd::from_raw_fd(fd) })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn deterministic_and_end_to_end() {
        let c = Config::resolve(
            Preset::Smoke,
            Overrides {
                frames_per_batch: Some(3),
                payload_bytes: Some(31),
                ..Overrides::default()
            },
        );
        let a = FrameWorkload::generate(&c).unwrap();
        let b = FrameWorkload::generate(&c).unwrap();
        assert_eq!(a.expected_checksum, b.expected_checksum);
        assert_eq!(run(&c).unwrap().operations, 3);
    }
    #[test]
    fn verifier_rejects_corruption() {
        let c = Config::resolve(Preset::Smoke, Overrides::default());
        let s = FrameWorkload::generate(&c).unwrap();
        assert!(FrameWorkload::verify(
            &c,
            &s,
            &Observation {
                frames: 0,
                bytes: 0,
                fds: 0,
                checksum: 0
            }
        )
        .is_err());
    }
}
