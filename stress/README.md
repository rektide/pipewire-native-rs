# Correctness-first stress harness

`pw-stress` drives production frame transport, SPA POD, and session memory APIs. Every timed invocation deterministically generates or selects a scenario, executes it, verifies counts/checksums/lifecycle state, and exits nonzero before printing anything if verification fails. A successful run prints one JSON line.

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

[`hyperfine.sh`](/stress/hyperfine.sh) validates hyperfine 1.20.x syntax, performs one release build preparation, and benchmarks the binary directly with `--shell=none`. Repeated `-L` lists create cartesian matrices. JSON under `stress/results/` records each expanded command and parameter values and is intentionally ignored by Git.

```sh
stress/hyperfine.sh
WARMUP=0 RUNS=2 PRESETS=smoke FRAME_PAYLOADS=64,4096 FRAME_CHUNKS=1,1024 FRAME_FD_DENSITIES=0,100 POD_DEPTHS=1,4 POD_WIDTHS=2 POD_VALUES=4,64 POD_MODES=decode-only MEMORY_REGIONS=4096 MEMORY_LIVE=1,4 MEMORY_CYCLES=1,4 stress/hyperfine.sh
```

## Interpreting results

Hyperfine measures full process execution, including deterministic generation and mandatory verification. POD `decode-only` isolates parser work more closely; `encode-decode` includes Builder allocation/encoding. Frame results include socket and descriptor creation plus nonblocking progress. Memory results include sealed memfd creation, import, mmap, touching every byte, retirement, generation checks, unmap, and an FD baseline check.

Compare only commands with equivalent operation/byte counts in their JSON summary. Pin CPU affinity (`taskset`), use a fixed performance governor, stop noisy services, avoid thermal throttling, and increase `WARMUP`/`RUNS` for publishable numbers. Record kernel, CPU, compiler, commit, and command matrix with exported results.

## Resource cautions

Large payloads are cloned into transport frames; FD-heavy runs consume process descriptor limits; byte-sized receive chunks intentionally amplify syscalls. Memory workloads dirty every mapped page. `pathological` is bounded but still expensive. Validation caps one generated POD/frame batch, live mappings, mapped bytes, recursion depth, and protocol sizes; OS limits can be lower.

## Adding a workload

Add a domain module implementing `Workload` with separate config, scenario, observation, and verification types. Expose a `run(&Config)` wrapper around `run_workload`, call that wrapper from both tests and a CLI subcommand, add checked aggregate bounds, and add one hyperfine matrix whose command invokes the release binary directly. Verification should derive expected values independently and make corruption observable as a nonzero exit.
