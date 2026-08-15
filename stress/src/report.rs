//! Hyperfine JSON reporting backed by independently verified workload metadata.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::process::Command;

#[derive(Debug, Deserialize)]
pub struct HyperfineExport {
    pub results: Vec<HyperfineResult>,
}

#[derive(Debug, Deserialize)]
pub struct HyperfineResult {
    pub command: String,
    pub mean: f64,
    pub stddev: f64,
    pub median: f64,
    pub min: f64,
    pub max: f64,
    pub times: Vec<f64>,
    #[serde(default)]
    pub exit_codes: Vec<i32>,
    #[serde(default)]
    pub parameters: BTreeMap<String, String>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct VerifiedMetadata {
    pub workload: String,
    pub preset: String,
    pub operations: u64,
    pub bytes: u64,
    pub checksum: u64,
    pub aux_count: u64,
    pub verified: bool,
}

#[derive(Debug, Serialize)]
pub struct Report {
    pub schema_version: u32,
    pub units: Units,
    pub rate_definitions: RateDefinitions,
    pub comparability: &'static str,
    pub results: Vec<ReportRow>,
}

#[derive(Debug, Serialize)]
pub struct Units {
    pub duration: &'static str,
    pub operations_rate: &'static str,
    pub decimal_throughput: &'static str,
    pub binary_throughput: &'static str,
}

#[derive(Debug, Serialize)]
pub struct RateDefinitions {
    pub mean: &'static str,
    pub conservative_low: &'static str,
    pub conservative_high: &'static str,
}

#[derive(Debug, Serialize)]
pub struct ReportRow {
    pub workload: String,
    pub preset: String,
    pub command: String,
    pub parameters: BTreeMap<String, String>,
    pub samples: usize,
    pub checksum: u64,
    pub counts: Counts,
    pub duration_seconds: Durations,
    pub rates: Rates,
}

#[derive(Debug, Serialize)]
pub struct Counts {
    pub operations: u64,
    pub bytes: u64,
    pub aux_count: u64,
    pub aux_unit: &'static str,
}

#[derive(Debug, Serialize)]
pub struct Durations {
    pub mean: f64,
    pub stddev: f64,
    pub median: f64,
    pub min: f64,
    pub max: f64,
}

#[derive(Debug, Serialize)]
pub struct Rates {
    pub operations_per_second: f64,
    pub operations_per_second_low: f64,
    pub operations_per_second_high: f64,
    pub megabytes_per_second: f64,
    pub mebibytes_per_second: f64,
    pub aux_per_second: f64,
    pub aux_rate_unit: &'static str,
}

pub trait MetadataRunner {
    fn run(&self, argv: &[OsString]) -> Result<String, String>;
}

pub struct ProcessRunner;

impl MetadataRunner for ProcessRunner {
    fn run(&self, argv: &[OsString]) -> Result<String, String> {
        let (program, args) = argv
            .split_first()
            .ok_or_else(|| "verified command has no executable".to_string())?;
        let output = Command::new(program)
            .args(args)
            .output()
            .map_err(|e| format!("could not execute verified command: {e}"))?;
        if !output.status.success() {
            return Err(format!(
                "verified command failed with {}: {}",
                output.status,
                String::from_utf8_lossy(&output.stderr).trim()
            ));
        }
        String::from_utf8(output.stdout)
            .map_err(|e| format!("verified command stdout is not UTF-8: {e}"))
    }
}

pub type ArgvOverrides = BTreeMap<String, Vec<String>>;

pub fn load_argv_overrides(path: &Path) -> Result<ArgvOverrides, String> {
    let bytes = std::fs::read(path)
        .map_err(|e| format!("failed to read argv overrides {}: {e}", path.display()))?;
    serde_json::from_slice(&bytes)
        .map_err(|e| format!("failed to parse argv overrides {}: {e}", path.display()))
}

pub fn build_report(
    inputs: &[PathBuf],
    overrides: &ArgvOverrides,
    runner: &dyn MetadataRunner,
) -> Result<Report, String> {
    let mut rows = Vec::new();
    for path in inputs {
        let bytes = std::fs::read(path)
            .map_err(|e| format!("failed to read Hyperfine JSON {}: {e}", path.display()))?;
        let export: HyperfineExport = serde_json::from_slice(&bytes)
            .map_err(|e| format!("failed to parse Hyperfine JSON {}: {e}", path.display()))?;
        for result in export.results {
            rows.push(
                build_row(result, overrides, runner)
                    .map_err(|e| format!("reporting Hyperfine export {}: {e}", path.display()))?,
            );
        }
    }
    rows.sort_by(|a, b| {
        (&a.workload, &a.command, &a.parameters).cmp(&(&b.workload, &b.command, &b.parameters))
    });
    Ok(Report {
        schema_version: 1,
        units: Units {
            duration: "seconds",
            operations_rate: "operations/second",
            decimal_throughput: "MB/second (1 MB = 1,000,000 bytes)",
            binary_throughput: "MiB/second (1 MiB = 1,048,576 bytes)",
        },
        rate_definitions: RateDefinitions {
            mean: "count / Hyperfine mean elapsed seconds",
            conservative_low: "operations / Hyperfine maximum elapsed seconds",
            conservative_high: "operations / Hyperfine minimum elapsed seconds",
        },
        comparability: "Rates include process startup, generation, execution, and verification. Compare only equivalent workloads and resolved counts; rates across workload types are not directly comparable.",
        results: rows,
    })
}

fn build_row(
    result: HyperfineResult,
    overrides: &ArgvOverrides,
    runner: &dyn MetadataRunner,
) -> Result<ReportRow, String> {
    validate_timing(&result)?;
    let argv = command_argv(&result.command, overrides)?;
    let output = runner
        .run(&argv)
        .map_err(|e| format!("verification failed for {:?}: {e}", result.command))?;
    let metadata = parse_metadata(&output).map_err(|e| {
        format!(
            "malformed verification output for {:?}: {e}",
            result.command
        )
    })?;
    let (aux_unit, aux_rate_unit) = aux_units(&metadata.workload)?;
    let mean = result.mean;
    Ok(ReportRow {
        workload: metadata.workload,
        preset: metadata.preset,
        command: result.command,
        parameters: result.parameters,
        samples: result.times.len(),
        checksum: metadata.checksum,
        counts: Counts {
            operations: metadata.operations,
            bytes: metadata.bytes,
            aux_count: metadata.aux_count,
            aux_unit,
        },
        duration_seconds: Durations {
            mean,
            stddev: result.stddev,
            median: result.median,
            min: result.min,
            max: result.max,
        },
        rates: Rates {
            operations_per_second: metadata.operations as f64 / mean,
            operations_per_second_low: metadata.operations as f64 / result.max,
            operations_per_second_high: metadata.operations as f64 / result.min,
            megabytes_per_second: metadata.bytes as f64 / mean / 1_000_000.0,
            mebibytes_per_second: metadata.bytes as f64 / mean / 1_048_576.0,
            aux_per_second: metadata.aux_count as f64 / mean,
            aux_rate_unit,
        },
    })
}

fn command_argv(command: &str, overrides: &ArgvOverrides) -> Result<Vec<OsString>, String> {
    let words = if let Some(argv) = overrides.get(command) {
        argv.clone()
    } else {
        shell_words::split(command).map_err(|e| {
            format!("cannot split command: {e}; provide --argv-overrides for exact argv")
        })?
    };
    if words.is_empty() {
        return Err("command is empty".into());
    }
    Ok(words.into_iter().map(OsString::from).collect())
}

fn parse_metadata(output: &str) -> Result<VerifiedMetadata, String> {
    let mut lines = output.lines();
    let line = lines.next().ok_or("stdout was empty")?;
    if lines.next().is_some() {
        return Err("expected exactly one JSON line on stdout".into());
    }
    let metadata: VerifiedMetadata =
        serde_json::from_str(line).map_err(|e| format!("invalid JSON: {e}"))?;
    if !metadata.verified {
        return Err("workload did not report verified=true".into());
    }
    if metadata.workload.is_empty() || metadata.preset.is_empty() {
        return Err("workload and preset must be non-empty".into());
    }
    Ok(metadata)
}

fn validate_timing(result: &HyperfineResult) -> Result<(), String> {
    for (name, value, allow_zero) in [
        ("mean", result.mean, false),
        ("stddev", result.stddev, true),
        ("median", result.median, false),
        ("min", result.min, false),
        ("max", result.max, false),
    ] {
        if !value.is_finite() || value < 0.0 || (!allow_zero && value == 0.0) {
            return Err(format!("invalid Hyperfine {name} duration {value}"));
        }
    }
    if result.times.is_empty() || result.times.iter().any(|v| !v.is_finite() || *v <= 0.0) {
        return Err("Hyperfine times must contain positive finite samples".into());
    }
    if result.exit_codes.iter().any(|code| *code != 0) {
        return Err("Hyperfine recorded a nonzero sample exit code".into());
    }
    Ok(())
}

fn aux_units(workload: &str) -> Result<(&'static str, &'static str), String> {
    match workload {
        "frame" => Ok(("fds", "fds/second")),
        "pod" => Ok(("fixture-bytes", "fixture-bytes/second")),
        "memory" => Ok(("generations", "generations/second")),
        other => Err(format!(
            "unknown workload {other:?}; cannot label aux_count honestly"
        )),
    }
}

pub fn write_outputs(report: &Report, prefix: &Path) -> Result<(), String> {
    if let Some(parent) = prefix.parent().filter(|p| !p.as_os_str().is_empty()) {
        std::fs::create_dir_all(parent)
            .map_err(|e| format!("failed to create {}: {e}", parent.display()))?;
    }
    write(prefix.with_extension("json"), json(report)?)?;
    write(prefix.with_extension("csv"), csv(report))?;
    write(prefix.with_extension("md"), markdown(report))
}

fn write(path: PathBuf, contents: String) -> Result<(), String> {
    std::fs::write(&path, contents)
        .map_err(|e| format!("failed to write report {}: {e}", path.display()))
}

pub fn json(report: &Report) -> Result<String, String> {
    let mut value = serde_json::to_string_pretty(report).map_err(|e| e.to_string())?;
    value.push('\n');
    Ok(value)
}

pub fn csv(report: &Report) -> String {
    let mut out = String::from("workload,preset,command,parameters,samples,checksum,operations,bytes,aux_count,aux_unit,aux_rate_unit,mean_seconds,stddev_seconds,median_seconds,min_seconds,max_seconds,operations_per_second,operations_per_second_low,operations_per_second_high,MB_per_second,MiB_per_second,aux_per_second\n");
    for row in &report.results {
        let parameters = row
            .parameters
            .iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect::<Vec<_>>()
            .join(";");
        let fields = vec![
            row.workload.clone(),
            row.preset.clone(),
            row.command.clone(),
            parameters,
            row.samples.to_string(),
            row.checksum.to_string(),
            row.counts.operations.to_string(),
            row.counts.bytes.to_string(),
            row.counts.aux_count.to_string(),
            row.counts.aux_unit.to_string(),
            row.rates.aux_rate_unit.to_string(),
            row.duration_seconds.mean.to_string(),
            row.duration_seconds.stddev.to_string(),
            row.duration_seconds.median.to_string(),
            row.duration_seconds.min.to_string(),
            row.duration_seconds.max.to_string(),
            row.rates.operations_per_second.to_string(),
            row.rates.operations_per_second_low.to_string(),
            row.rates.operations_per_second_high.to_string(),
            row.rates.megabytes_per_second.to_string(),
            row.rates.mebibytes_per_second.to_string(),
            row.rates.aux_per_second.to_string(),
        ];
        out.push_str(
            &fields
                .into_iter()
                .map(|v| csv_field(&v))
                .collect::<Vec<_>>()
                .join(","),
        );
        out.push('\n');
    }
    out
}

fn csv_field(value: &str) -> String {
    if value.chars().any(|c| matches!(c, ',' | '"' | '\n' | '\r')) {
        format!("\"{}\"", value.replace('"', "\"\""))
    } else {
        value.to_string()
    }
}

pub fn markdown(report: &Report) -> String {
    let mut out = String::from("# Stress operations report\n\nRates include process startup, scenario generation, execution, and verification. Cross-workload rates are not directly comparable. The conservative ops/s range is `operations / max elapsed` through `operations / min elapsed`.\n\n| Workload | Preset | Parameters | Samples | Operations | Mean ± stddev (s) | Ops/s (mean) | Conservative ops/s | MB/s | MiB/s | Auxiliary rate | Checksum | Command |\n|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|\n");
    for row in &report.results {
        let parameters = row
            .parameters
            .iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect::<Vec<_>>()
            .join(", ");
        out.push_str(&format!(
            "| {} | {} | {} | {} | {} | {:.6} ± {:.6} | {:.3} | {:.3}–{:.3} | {:.3} | {:.3} | {:.3} {} | {} | `{}` |\n",
            md(&row.workload), md(&row.preset), md(&parameters), row.samples, row.counts.operations,
            row.duration_seconds.mean, row.duration_seconds.stddev, row.rates.operations_per_second,
            row.rates.operations_per_second_low, row.rates.operations_per_second_high,
            row.rates.megabytes_per_second, row.rates.mebibytes_per_second,
            row.rates.aux_per_second, row.rates.aux_rate_unit, row.checksum, md(&row.command)
        ));
    }
    out
}

fn md(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('|', "\\|")
        .replace('`', "\\`")
        .replace('\n', "<br>")
}

#[cfg(test)]
mod tests {
    use super::*;

    struct FakeRunner {
        output: Result<String, String>,
    }

    struct WorkloadRunner;
    impl MetadataRunner for WorkloadRunner {
        fn run(&self, argv: &[OsString]) -> Result<String, String> {
            let command = argv[0].to_string_lossy();
            let workload = if command.contains("frame") {
                "frame"
            } else {
                "pod"
            };
            Ok(metadata(workload))
        }
    }
    impl MetadataRunner for FakeRunner {
        fn run(&self, _: &[OsString]) -> Result<String, String> {
            self.output.clone()
        }
    }

    fn result(command: &str) -> HyperfineResult {
        HyperfineResult {
            command: command.into(),
            mean: 2.0,
            stddev: 0.25,
            median: 1.9,
            min: 1.0,
            max: 4.0,
            times: vec![1.0, 4.0],
            exit_codes: vec![0, 0],
            parameters: BTreeMap::from([("z".into(), "pipe|tick`".into())]),
        }
    }
    fn metadata(workload: &str) -> String {
        format!("{{\"workload\":\"{workload}\",\"preset\":\"smoke\",\"operations\":100,\"bytes\":2000000,\"checksum\":7,\"aux_count\":20,\"verified\":true}}\n")
    }
    fn row(workload: &str) -> ReportRow {
        build_row(
            result("fake 'arg with space'"),
            &BTreeMap::new(),
            &FakeRunner {
                output: Ok(metadata(workload)),
            },
        )
        .unwrap()
    }

    #[test]
    fn formulas_and_labels_are_explicit() {
        let frame = row("frame");
        assert_eq!(frame.rates.operations_per_second, 50.0);
        assert_eq!(frame.rates.operations_per_second_low, 25.0);
        assert_eq!(frame.rates.operations_per_second_high, 100.0);
        assert_eq!(frame.rates.megabytes_per_second, 1.0);
        assert_eq!(
            frame.rates.mebibytes_per_second,
            2_000_000.0 / 2.0 / 1_048_576.0
        );
        assert_eq!(frame.rates.aux_rate_unit, "fds/second");
        assert_eq!(row("pod").rates.aux_rate_unit, "fixture-bytes/second");
        assert_eq!(row("memory").rates.aux_rate_unit, "generations/second");
    }

    #[test]
    fn rejects_invalid_timing_metadata_and_command_failure() {
        let mut bad = result("fake");
        bad.mean = 0.0;
        assert!(build_row(
            bad,
            &BTreeMap::new(),
            &FakeRunner {
                output: Ok(metadata("frame"))
            }
        )
        .unwrap_err()
        .contains("mean"));
        let error = build_row(
            result("fake"),
            &BTreeMap::new(),
            &FakeRunner {
                output: Err("exit 9".into()),
            },
        )
        .unwrap_err();
        assert!(error.contains("exit 9"));
        let error = build_row(
            result("fake"),
            &BTreeMap::new(),
            &FakeRunner {
                output: Ok("not json\n".into()),
            },
        )
        .unwrap_err();
        assert!(error.contains("malformed verification output"));
    }

    #[test]
    fn output_escaping_and_json_are_valid() {
        let mut report = empty_report();
        report.results.push(row("frame"));
        report.results[0].command = "a,\"b|`c\nnext".into();
        let csv = csv(&report);
        assert!(csv.contains("\"a,\"\"b|`c\nnext\""));
        let md = markdown(&report);
        assert!(md.contains("a,\"b\\|\\`c<br>next"));
        serde_json::from_str::<serde_json::Value>(&json(&report).unwrap()).unwrap();
    }

    #[test]
    fn stable_sort_uses_workload_then_command_and_parameters() {
        let dir = std::env::temp_dir().join(format!("pw-stress-report-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("fixture.json");
        std::fs::write(&path, include_str!("../tests/fixtures/hyperfine-1.20.json")).unwrap();
        let report = build_report(&[path], &BTreeMap::new(), &WorkloadRunner).unwrap();
        assert_eq!(
            report
                .results
                .iter()
                .map(|r| r.command.as_str())
                .collect::<Vec<_>>(),
            ["fake-frame --name 'space value'", "fake-pod"]
        );
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn argv_override_preserves_ambiguous_arguments() {
        let command = "path with spaces --value also spaces";
        let overrides = BTreeMap::from([(
            command.to_string(),
            vec![
                "path with spaces".to_string(),
                "--value".to_string(),
                "also spaces".to_string(),
            ],
        )]);
        let argv = command_argv(command, &overrides).unwrap();
        assert_eq!(argv, ["path with spaces", "--value", "also spaces"]);
    }

    #[test]
    fn process_runner_reports_nonzero_command() {
        let error = ProcessRunner
            .run(&[OsString::from("/bin/false")])
            .unwrap_err();
        assert!(error.contains("failed with"));
    }

    fn empty_report() -> Report {
        Report {
            schema_version: 1,
            units: Units {
                duration: "seconds",
                operations_rate: "operations/second",
                decimal_throughput: "MB/s",
                binary_throughput: "MiB/s",
            },
            rate_definitions: RateDefinitions {
                mean: "mean",
                conservative_low: "low",
                conservative_high: "high",
            },
            comparability: "none",
            results: vec![],
        }
    }
}
