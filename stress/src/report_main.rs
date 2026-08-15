use clap::Parser;
use pipewire_native_stress::report::{
    build_report, load_argv_overrides, write_outputs, ArgvOverrides, ProcessRunner,
};
use std::path::PathBuf;

#[derive(Parser)]
#[command(
    name = "pw-stress-report",
    version,
    about = "Create verified operations/second reports from Hyperfine 1.20 JSON",
    after_help = "EXAMPLES:\n  pw-stress-report --output-prefix stress/results/report stress/results/frame.json stress/results/pod.json stress/results/memory.json\n  pw-stress-report --argv-overrides argv.json --output-prefix stress/results/frame-report stress/results/frame.json\n\nHyperfine JSON stores a command string, not argv. Commands are split using POSIX shell quoting but executed directly without a shell. For ambiguous command strings (notably unquoted paths containing spaces), provide a JSON object mapping each exact command string to an argv string array with --argv-overrides."
)]
struct Cli {
    /// Hyperfine 1.20 JSON exports to combine.
    #[arg(required = true)]
    inputs: Vec<PathBuf>,
    /// Output path without extension; .json, .csv, and .md are written.
    #[arg(long, default_value = "stress/results/report")]
    output_prefix: PathBuf,
    /// Exact command string to argv array JSON map.
    #[arg(long)]
    argv_overrides: Option<PathBuf>,
}

fn main() {
    if let Err(error) = run(Cli::parse()) {
        eprintln!("error: {error}");
        std::process::exit(2);
    }
}

fn run(cli: Cli) -> Result<(), String> {
    let overrides = match cli.argv_overrides {
        Some(path) => load_argv_overrides(&path)?,
        None => ArgvOverrides::new(),
    };
    let report = build_report(&cli.inputs, &overrides, &ProcessRunner)?;
    write_outputs(&report, &cli.output_prefix)?;
    eprintln!(
        "Wrote {} verified result(s) to {}.{{json,csv,md}}",
        report.results.len(),
        cli.output_prefix.display()
    );
    Ok(())
}
