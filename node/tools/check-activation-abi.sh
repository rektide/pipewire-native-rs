#!/usr/bin/env bash
# SPDX-License-Identifier: MIT
# SPDX-FileCopyrightText: Copyright (c) 2026 Asymptotic Inc.

set -euo pipefail

host=$(rustc -vV | sed -n 's/^host: //p')
if [[ $host != x86_64-unknown-linux-gnu ]]; then
    echo "activation ABI differential probe supports only native x86_64-unknown-linux-gnu" >&2
    exit 2
fi

: "${PIPEWIRE_SOURCE_DIR:?set PIPEWIRE_SOURCE_DIR to a PipeWire source checkout}"
private_header="$PIPEWIRE_SOURCE_DIR/src/pipewire/private.h"
if [[ ! -f "$private_header" ]]; then
    echo "PIPEWIRE_SOURCE_DIR does not contain src/pipewire/private.h" >&2
    exit 2
fi

node_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
probe=$(mktemp "${TMPDIR:-/tmp}/pipewire-activation-abi.XXXXXX")
actual=$(mktemp "${TMPDIR:-/tmp}/pipewire-activation-abi-values.XXXXXX")
expected=$(mktemp "${TMPDIR:-/tmp}/pipewire-activation-abi-expected.XXXXXX")
trap 'rm -f -- "$probe" "$actual" "$expected"' EXIT
read -r -a pkg_cflags <<<"$(pkg-config --cflags libspa-0.2 libpipewire-0.3)"

# This is intentionally a native developer check, not part of cargo build or test.
"${CC:-cc}" \
    -std=c11 \
    -D_GNU_SOURCE \
    "$node_dir/c/activation_abi.c" \
    -I"$PIPEWIRE_SOURCE_DIR/src" \
    -I"$PIPEWIRE_SOURCE_DIR/spa/include" \
    "${pkg_cflags[@]}" \
    -o "$probe"

"$probe" >"$actual"
grep '^pub const ' \
    "$node_dir/src/session/abi/x86_64_unknown_linux_gnu.rs" >"$expected"
diff -u "$expected" "$actual"
echo "activation ABI matches the checked-in x86_64-unknown-linux-gnu table"
