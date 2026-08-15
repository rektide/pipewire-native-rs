#!/usr/bin/env bash
# SPDX-License-Identifier: MIT
# SPDX-FileCopyrightText: Copyright (c) 2026 Asymptotic Inc.

set -euo pipefail

host=$(rustc -vV | sed -n 's/^host: //p')
if [[ $host != x86_64-unknown-linux-gnu ]]; then
    echo "SPA buffer ABI differential probe supports only native x86_64-unknown-linux-gnu" >&2
    exit 2
fi

node_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
probe=$(mktemp "${TMPDIR:-/tmp}/pipewire-spa-buffer-abi.XXXXXX")
actual=$(mktemp "${TMPDIR:-/tmp}/pipewire-spa-buffer-abi-values.XXXXXX")
expected=$(mktemp "${TMPDIR:-/tmp}/pipewire-spa-buffer-abi-expected.XXXXXX")
trap 'rm -f -- "$probe" "$actual" "$expected"' EXIT
read -r -a pkg_cflags <<<"$(pkg-config --cflags libspa-0.2)"

"${CC:-cc}" -std=c11 -D_GNU_SOURCE "$node_dir/c/spa_buffer_abi.c" "${pkg_cflags[@]}" -o "$probe"
"$probe" >"$actual"
grep '^pub const ' \
    "$node_dir/src/session/port/abi/x86_64_unknown_linux_gnu.rs" >"$expected"
diff -u "$expected" "$actual"
echo "SPA buffer ABI matches the checked-in x86_64-unknown-linux-gnu table"
