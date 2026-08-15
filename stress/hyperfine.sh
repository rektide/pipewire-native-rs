#!/usr/bin/env bash
set -euo pipefail

HERE=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
MANIFEST="$HERE/Cargo.toml"
BIN="$HERE/target/release/pw-stress"
REPORT_BIN="$HERE/target/release/pw-stress-report"
RESULTS="$HERE/results"
WARMUP=${WARMUP:-1}
RUNS=${RUNS:-10}
PRESETS=${PRESETS:-smoke,medium}

# Comma-separated lists are intentionally passed directly to hyperfine 1.20's
# repeatable -L option; repeated lists form a cartesian product.
FRAME_PAYLOADS=${FRAME_PAYLOADS:-1024,65536}
FRAME_CHUNKS=${FRAME_CHUNKS:-256,32768}
FRAME_FD_DENSITIES=${FRAME_FD_DENSITIES:-0,25}
POD_DEPTHS=${POD_DEPTHS:-2,8}
POD_WIDTHS=${POD_WIDTHS:-4,32}
POD_VALUES=${POD_VALUES:-16,512}
POD_MODES=${POD_MODES:-encode-decode,decode-only}
MEMORY_REGIONS=${MEMORY_REGIONS:-4096,1048576}
MEMORY_LIVE=${MEMORY_LIVE:-2,16}
MEMORY_CYCLES=${MEMORY_CYCLES:-2,16}

if ! hyperfine --version | grep -q '^hyperfine 1\.20\.'; then
  printf 'error: stress/hyperfine.sh is validated against hyperfine 1.20.x\n' >&2
  exit 2
fi

mkdir -p "$RESULTS"
printf 'Preparing release runner...\n' >&2
cargo build --release --manifest-path "$MANIFEST" --bin pw-stress --bin pw-stress-report

hyperfine --shell=none --warmup "$WARMUP" --runs "$RUNS" --style basic \
  -L preset "$PRESETS" -L payload "$FRAME_PAYLOADS" -L chunk "$FRAME_CHUNKS" -L density "$FRAME_FD_DENSITIES" \
  --export-json "$RESULTS/frame.json" \
  "$BIN frame --preset {preset} --payload-bytes {payload} --recv-chunk-bytes {chunk} --fd-density {density}"
"$REPORT_BIN" --output-prefix "$RESULTS/frame-report" "$RESULTS/frame.json"

hyperfine --shell=none --warmup "$WARMUP" --runs "$RUNS" --style basic \
  -L preset "$PRESETS" -L depth "$POD_DEPTHS" -L width "$POD_WIDTHS" -L values "$POD_VALUES" -L mode "$POD_MODES" \
  --export-json "$RESULTS/pod.json" \
  "$BIN pod --preset {preset} --depth {depth} --width {width} --values-per-container {values} --mode {mode}"
"$REPORT_BIN" --output-prefix "$RESULTS/pod-report" "$RESULTS/pod.json"

hyperfine --shell=none --warmup "$WARMUP" --runs "$RUNS" --style basic \
  -L preset "$PRESETS" -L region "$MEMORY_REGIONS" -L live "$MEMORY_LIVE" -L cycles "$MEMORY_CYCLES" \
  --export-json "$RESULTS/memory.json" \
  "$BIN memory --preset {preset} --region-bytes {region} --live-mappings {live} --retire-cycles {cycles}"
"$REPORT_BIN" --output-prefix "$RESULTS/memory-report" "$RESULTS/memory.json"

"$REPORT_BIN" --output-prefix "$RESULTS/report" \
  "$RESULTS/frame.json" "$RESULTS/pod.json" "$RESULTS/memory.json"
