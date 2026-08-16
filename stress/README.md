# Correctness-first stress harness

`pw-stress` drives production frame transport, SPA POD, and session memory APIs. Every timed invocation deterministically generates or selects a scenario, executes it, verifies counts/checksums/lifecycle state, and exits nonzero before printing anything if verification fails. A successful run prints one JSON line. Hyperfine measures end-to-end commands; Criterion tracks in-process subsystem performance over source history.

## Architecture

The reusable pipeline in [`src/lib.rs`](/stress/src/lib.rs) is deliberately small:

1. `Workload::validate` checks parameters, resource bounds, and arithmetic.
2. `Workload::generate` creates deterministic scenario data outside subsystem execution.
3. `Workload::execute` invokes production APIs and gathers an observation.
4. `Workload::verify` compares independent expected and observed state.
5. `run_workload` applies `black_box` only to the verified result.

Subsystem adapters are [`src/frame.rs`](/stress/src/frame.rs), [`src/pod.rs`](/stress/src/pod.rs), and [`src/memory.rs`](/stress/src/memory.rs). Their public `Config::resolve` and `run` functions are shared by fast tests and the release CLI. `smoke`, `medium`, `large`, and bounded `pathological` presets are starting points; explicit options override them.

## Direct runs

Build once, then invoke the binary directly so Cargo startup is never measured:

```sh
cargo build --release --manifest-path stress/Cargo.toml --bin pw-stress
stress/target/release/pw-stress --help
stress/target/release/pw-stress frame --preset medium --frames-per-batch 256 --payload-bytes 65536 --fd-density 25 --fds-per-frame 2 --recv-chunk-bytes 512 --pattern segmented
stress/target/release/pw-stress pod --preset medium --mode decode-only --depth 8 --width 32 --values-per-container 1024 --payload-bytes 16384 --seed 42
stress/target/release/pw-stress memory --preset medium --region-bytes 1048576 --live-mappings 32 --retire-cycles 16 --seed 42
```

Use subcommand help for all controls: `pw-stress frame --help`, `pw-stress pod --help`, and `pw-stress memory --help`.

## Hyperfine matrices

[`hyperfine.sh`](/stress/hyperfine.sh) validates hyperfine 1.20.x syntax, performs one release build preparation, and benchmarks the binary directly with `--shell=none`. Repeated `-L` lists create cartesian matrices. After each matrix, the separately built `pw-stress-report` re-executes every exact command once outside timing to obtain verified resolved metadata. It writes JSON, CSV, and Markdown per-workload reports plus combined `report.{json,csv,md}` under ignored `stress/results/`. Report generation is never part of a measured command.

```sh
stress/hyperfine.sh
WARMUP=0 RUNS=2 PRESETS=smoke FRAME_PAYLOADS=64,4096 FRAME_CHUNKS=1,1024 FRAME_FD_DENSITIES=0,100 POD_DEPTHS=1,4 POD_WIDTHS=2 POD_VALUES=4,64 POD_MODES=decode-only MEMORY_REGIONS=4096 MEMORY_LIVE=1,4 MEMORY_CYCLES=1,4 stress/hyperfine.sh
```

Regenerate reports from existing Hyperfine 1.20 exports without benchmarking:

```sh
cargo build --release --manifest-path stress/Cargo.toml --bin pw-stress-report
stress/target/release/pw-stress-report --output-prefix stress/results/report stress/results/frame.json stress/results/pod.json stress/results/memory.json
stress/target/release/pw-stress-report --help
```

Hyperfine exports a command string rather than an argv array. The reporter applies POSIX shell-word splitting and then executes the resulting argv directly without a shell. This correctly preserves quoted spaces, but an unquoted executable path containing spaces is inherently ambiguous. In that case, pass `--argv-overrides FILE`; the file is a JSON object mapping each exact exported command string to an array containing the executable and arguments.

## Criterion history

[`criterion.sh`](/stress/criterion.sh) wraps cargo-criterion 1.1.0 and Criterion 0.8.2. Unlike Hyperfine, each Criterion scenario is generated and validated once outside timing. Every measured iteration invokes the production `execute` stage and then the independent `verify` stage; only the successful `Verified` value is black-boxed. Compilation is outside cargo-criterion's measurements.

Select a bounded matrix with `PW_CRITERION_PROFILE`:

| Profile | Cases | Intended use |
|---|---:|---|
| `smoke` (default) | 6 | Fast local signal across two frame, two POD, and two memory cases. |
| `ci` | 12 | Smoke plus meaningful medium-size and descriptor/lifecycle variants. |
| `full` | 18 | CI plus bounded expensive payload, parser, mapping, and syscall-heavy cases. |

Frame IDs include frames per batch, payload bytes, FDs per frame, FD density, coalesced/segmented progress, receive chunk bytes, load policy, and seed. POD IDs include decode-only/encode-decode mode, depth, width, values per container, payload bytes, load policy, and seed. Memory IDs include region bytes, live mappings, retire cycles, load policy, and seed. This makes an ID's meaning stable even if profile membership changes.

```sh
# Inspect source correlation without benchmarking.
stress/criterion.sh --print-marker

# Run the smoke matrix without plots.
stress/criterion.sh --plotting-backend disabled

# Run CI cases matching a benchmark regex and override Criterion statistics.
PW_CRITERION_PROFILE=ci stress/criterion.sh --plotting-backend disabled -- 'frame|pod' --sample-size 20 --warm-up-time 1 --measurement-time 3

# Give release or CI runs durable names and descriptions.
HISTORY_ID=v0.2.0-linux-x86_64 HISTORY_DESCRIPTION='v0.2.0 dedicated runner' PW_CRITERION_PROFILE=full stress/criterion.sh
```

Arguments are preserved as an argv array without `eval`. Options before `--` are cargo-criterion options. A benchmark regex and Criterion binary options can follow `--`; inspect them with `cargo criterion --manifest-path stress/Cargo.toml --bench subsystems -- --help`. Invalid profiles fail clearly and nonzero.

The default history marker uses the jj working-copy commit `@`, because jj snapshots the filesystem Cargo actually builds into `@`. After a normal `jj commit`, the new empty `@` has the same tree as `@-`; metadata records that equality, both full commit/change IDs, parent IDs, bookmarks, first-line description, and dirty state. Selecting `@-` unconditionally would mislabel uncommitted measured files. Outside jj, a clean tree uses the Git `HEAD`; a dirty tree adds a deterministic fingerprint of tracked changes and untracked file names/content. `HISTORY_ID` is sanitized to a concise filesystem-safe value, while its original value remains in metadata. `HISTORY_DESCRIPTION` replaces the rich default description.

The wrapper passes `--history-id`, `--history-description`, and `--message-format json`. Human progress, confidence intervals, and change detection remain on stderr. Machine output is atomically installed at `stress/results/criterion/<history-id>/results.jsonl`; `metadata.json` records timestamp, profile, complete source IDs, tools, kernel, invoked args, and relevant build environment. Both `stress/results/` and `stress/target/` are ignored. cargo-criterion keeps baselines and history reports under `stress/target/criterion/`; retain or cache that directory between CI runs to preserve comparisons, and upload the marker directory as a CI artifact. Avoid sharing a target history cache between incompatible machines or compiler configurations.

Criterion reports `Elements/s` from the exact independently verified count per measured iteration:

- Frame elements are frames transported and verified.
- POD elements are complete POD records decoded and verified; encode-decode mode also rebuilds each record.
- Memory elements are mappings created, generation-checked, touched, retired, and verified. The generation count equals the mapping count.

Compare only identical benchmark IDs on comparable hosts. Confidence intervals describe sampling uncertainty, not portability; apparent changes on shared or frequency-scaling machines can be noise. A history description and metadata make a result attributable, but CPU affinity, a fixed governor, thermal control, and longer measurement settings are still needed for publishable claims.

## Interpreting Hyperfine results

Hyperfine measures full process execution, including deterministic generation and mandatory verification. Reports define mean operations/s as `verified operations / Hyperfine mean elapsed seconds`; the conservative range is `operations / max elapsed` through `operations / min elapsed`. Decimal MB/s uses 1,000,000 bytes and binary MiB/s uses 1,048,576 bytes. Auxiliary rates are `fds/s` for frame, `fixture-bytes/s` for POD (the encoded fixture size divided by elapsed time, not additional processed bytes), and `generations/s` for memory. POD `decode-only` isolates parser work more closely; `encode-decode` includes Builder allocation/encoding. Frame results include socket and descriptor creation plus nonblocking progress. Memory results include sealed memfd creation, import, mmap, touching every byte, retirement, generation checks, unmap, and an FD baseline check.

The report includes the exact command, sorted Hyperfine parameter map, sample count, checksum, resolved counts, timings, rates, and explicit units. Compare only commands with equivalent operation/byte counts. Operations mean different things for frames, POD iterations, and memory mappings, so cross-workload rates are not directly comparable. Pin CPU affinity (`taskset`), use a fixed performance governor, stop noisy services, avoid thermal throttling, and increase `WARMUP`/`RUNS` for publishable numbers. Record kernel, CPU, compiler, commit, and command matrix with exported results.

## Resource cautions

Large payloads are cloned into transport frames; FD-heavy runs consume process descriptor limits; byte-sized receive chunks intentionally amplify syscalls. Memory workloads dirty every mapped page. `pathological` is bounded but still expensive. Validation caps one generated POD/frame batch, live mappings, mapped bytes, recursion depth, and protocol sizes; OS limits can be lower.

## Adding a workload

Add a domain module implementing `Workload` with separate config, scenario, observation, and verification types. Expose a `run(&Config)` wrapper around `run_workload`, call that wrapper from both tests and a CLI subcommand, add checked aggregate bounds, and add one hyperfine matrix whose command invokes the release binary directly. Verification should derive expected values independently and make corruption observable as a nonzero exit.
