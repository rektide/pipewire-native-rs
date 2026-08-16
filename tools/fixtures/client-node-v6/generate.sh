#!/bin/sh
# SPDX-License-Identifier: MIT
set -eu

expected_commit=30ff8da174121567c06a576bf2a83e71779ee991
source_dir=${1:?usage: generate.sh PATH_TO_PIPEWIRE_CHECKOUT}
actual_commit=$(git -C "$source_dir" rev-parse HEAD)

if [ "$actual_commit" != "$expected_commit" ]; then
	echo "expected PipeWire commit $expected_commit, found $actual_commit" >&2
	exit 1
fi

binary=$(mktemp "${TMPDIR:-/tmp}/client-node-v6-fixtures.XXXXXX")
trap 'rm -f "$binary"' EXIT HUP INT TERM

cc -D_GNU_SOURCE -std=c11 -Wall -Wextra -Werror -O2 \
	-I"$source_dir/spa/include" \
	"$(dirname "$0")/generate.c" \
	-o "$binary"
"$binary"
