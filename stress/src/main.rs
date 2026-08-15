use clap::{Args, Parser, Subcommand};
use pipewire_native_stress::{frame, memory, pod, Preset, Verified};

#[derive(Parser)]
#[command(
    name = "pw-stress",
    about = "Correctness-verifying PipeWire subsystem stress workloads",
    version
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Native Unix frame transport with optional SCM_RIGHTS descriptors.
    Frame(FrameArgs),
    /// SPA POD Builder and Parser encode/decode throughput.
    Pod(PodArgs),
    /// Session MemoryPool import/map/retire/generation throughput.
    Memory(MemoryArgs),
}

#[derive(Args)]
struct Common {
    /// Named baseline; every explicitly supplied option overrides it.
    #[arg(long, value_enum, default_value_t = Preset::Smoke)]
    preset: Preset,
    #[arg(long)]
    batches: Option<usize>,
    #[arg(long)]
    iterations: Option<usize>,
    #[arg(long)]
    seed: Option<u64>,
}

#[derive(Args)]
struct FrameArgs {
    #[command(flatten)]
    common: Common,
    #[arg(long)]
    frames_per_batch: Option<usize>,
    #[arg(long)]
    payload_bytes: Option<usize>,
    #[arg(long)]
    fds_per_frame: Option<usize>,
    /// Percentage of frames carrying `fds-per-frame` descriptors (0..=100).
    #[arg(long)]
    fd_density: Option<usize>,
    #[arg(long)]
    recv_chunk_bytes: Option<usize>,
    #[arg(long, value_enum)]
    pattern: Option<frame::ProgressPattern>,
}

#[derive(Args)]
struct PodArgs {
    #[command(flatten)]
    common: Common,
    #[arg(long)]
    depth: Option<usize>,
    #[arg(long)]
    width: Option<usize>,
    #[arg(long)]
    values_per_container: Option<usize>,
    /// Bytes in each byte/string field.
    #[arg(long)]
    payload_bytes: Option<usize>,
    #[arg(long, value_enum)]
    mode: Option<pod::Mode>,
}

#[derive(Args)]
struct MemoryArgs {
    #[command(flatten)]
    common: Common,
    #[arg(long)]
    region_bytes: Option<usize>,
    #[arg(long)]
    live_mappings: Option<usize>,
    #[arg(long)]
    retire_cycles: Option<usize>,
}

fn main() {
    if let Err(error) = execute(Cli::parse()) {
        eprintln!("error: {error}");
        std::process::exit(2);
    }
}

fn execute(cli: Cli) -> Result<(), String> {
    let (name, preset, result) = match cli.command {
        Command::Frame(a) => {
            let preset = a.common.preset;
            let config = frame::Config::resolve(
                preset,
                frame::Overrides {
                    batches: a.common.batches,
                    iterations: a.common.iterations,
                    frames_per_batch: a.frames_per_batch,
                    payload_bytes: a.payload_bytes,
                    fds_per_frame: a.fds_per_frame,
                    fd_density: a.fd_density,
                    recv_chunk_bytes: a.recv_chunk_bytes,
                    pattern: a.pattern,
                    seed: a.common.seed,
                },
            );
            ("frame", preset, frame::run(&config)?)
        }
        Command::Pod(a) => {
            let preset = a.common.preset;
            let config = pod::Config::resolve(
                preset,
                pod::Overrides {
                    batches: a.common.batches,
                    iterations: a.common.iterations,
                    depth: a.depth,
                    width: a.width,
                    values_per_container: a.values_per_container,
                    payload_bytes: a.payload_bytes,
                    mode: a.mode,
                    seed: a.common.seed,
                },
            );
            ("pod", preset, pod::run(&config)?)
        }
        Command::Memory(a) => {
            let preset = a.common.preset;
            let config = memory::Config::resolve(
                preset,
                memory::Overrides {
                    batches: a.common.batches,
                    iterations: a.common.iterations,
                    region_bytes: a.region_bytes,
                    live_mappings: a.live_mappings,
                    retire_cycles: a.retire_cycles,
                    seed: a.common.seed,
                },
            );
            ("memory", preset, memory::run(&config)?)
        }
    };
    print_summary(name, preset, result);
    Ok(())
}

fn print_summary(workload: &str, preset: Preset, v: Verified) {
    println!("{{\"workload\":\"{workload}\",\"preset\":\"{}\",\"operations\":{},\"bytes\":{},\"checksum\":{},\"aux_count\":{},\"verified\":true}}", format!("{preset:?}").to_lowercase(), v.operations, v.bytes, v.checksum, v.aux_count);
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn clap_expands_explicit_parameters() {
        let cli = Cli::try_parse_from([
            "pw-stress",
            "frame",
            "--preset",
            "medium",
            "--batches",
            "3",
            "--fd-density",
            "0",
        ])
        .unwrap();
        let Command::Frame(args) = cli.command else {
            panic!("wrong command")
        };
        let c = frame::Config::resolve(
            args.common.preset,
            frame::Overrides {
                batches: args.common.batches,
                fd_density: args.fd_density,
                ..frame::Overrides::default()
            },
        );
        assert_eq!(c.load.batches, 3);
        assert_eq!(c.fd_density, 0);
        assert_eq!(c.payload_bytes, 16 * 1024);
    }
}
