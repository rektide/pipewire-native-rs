#!/usr/bin/env bash
set -euo pipefail

HERE=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
ROOT=$(CDPATH= cd -- "$HERE/.." && pwd)
PROFILE=${PW_CRITERION_PROFILE:-smoke}
PRINT_MARKER=false

if [[ ${1:-} == --print-marker || ${1:-} == --dry-run ]]; then
  PRINT_MARKER=true
  shift
fi

MARKER_TMP=$(mktemp "${TMPDIR:-/tmp}/pw-criterion-marker.XXXXXX")
JSON_TMP=
cleanup() {
  rm -f -- "$MARKER_TMP"
  if [[ -n $JSON_TMP ]]; then rm -f -- "$JSON_TMP"; fi
}
trap cleanup EXIT

(
  cd -- "$ROOT"
  cargo run --quiet --manifest-path stress/Cargo.toml --bin pw-criterion-marker -- "$PROFILE" "$@"
) >"$MARKER_TMP"
mapfile -t MARKER_LINES <"$MARKER_TMP"
if (( ${#MARKER_LINES[@]} != 3 )); then
  printf 'error: marker helper returned malformed output\n' >&2
  exit 2
fi
HISTORY_ID_RESOLVED=${MARKER_LINES[0]}
HISTORY_DESCRIPTION_RESOLVED=${MARKER_LINES[1]}
MARKER_JSON=${MARKER_LINES[2]}
RESULT_DIR="$HERE/results/criterion/$HISTORY_ID_RESOLVED"

if $PRINT_MARKER; then
  printf 'history-id: %s\nhistory-description: %s\nartifact-directory: %s\nmetadata: %s\n' \
    "$HISTORY_ID_RESOLVED" "$HISTORY_DESCRIPTION_RESOLVED" "$RESULT_DIR" "$MARKER_JSON"
  exit 0
fi

mkdir -p -- "$RESULT_DIR"
METADATA_TMP=$(mktemp "$RESULT_DIR/metadata.json.tmp.XXXXXX")
printf '%s\n' "$MARKER_JSON" >"$METADATA_TMP"
mv -f -- "$METADATA_TMP" "$RESULT_DIR/metadata.json"

JSON_TMP=$(mktemp "$RESULT_DIR/results.jsonl.tmp.XXXXXX")
printf 'Criterion marker %s (%s)\n' "$HISTORY_ID_RESOLVED" "$PROFILE" >&2
(
  cd -- "$ROOT"
  PW_CRITERION_PROFILE=$PROFILE cargo criterion \
    --manifest-path stress/Cargo.toml \
    --bench subsystems \
    --history-id "$HISTORY_ID_RESOLVED" \
    --history-description "$HISTORY_DESCRIPTION_RESOLVED" \
    --message-format json \
    "$@"
) >"$JSON_TMP"
mv -f -- "$JSON_TMP" "$RESULT_DIR/results.jsonl"
JSON_TMP=
printf 'Metadata: %s\nJSONL: %s\n' "$RESULT_DIR/metadata.json" "$RESULT_DIR/results.jsonl" >&2
