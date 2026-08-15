# pipewire-native-node

This crate contains data-plane transport primitives for `pipewire-native`.

## Shared-memory ABI support

`pw_node_activation`, `spa_io_buffers`, and `spa_chunk` are target-native
shared-memory ABIs. Normal builds use checked-in tables and do not need PipeWire
source, SPA headers, `HOME`, a C compiler, or a build-time target executable.

The activation view currently supports only `x86_64-unknown-linux-gnu`. Other
targets fail at compile time rather than reusing an unverified layout. The checked-in
table in `src/session/abi/x86_64_unknown_linux_gnu.rs` was differentially verified
against PipeWire commit `69c1b4c8b6a1cfa95982e5ed740a3995d94c1308`.
The public SPA layouts have a separate checked-in table in
`src/session/port/abi/x86_64_unknown_linux_gnu.rs`.

There is no public-header or compile-only mechanism that exposes this private
layout, so extending target support requires obtaining that target's values and
checking them into a separate target-specific table. Do not infer support merely
from pointer width or architecture similarity.

## Differential probe

Developers with the pinned PipeWire source checkout can compare its private C
layout to the checked-in table on a native x86_64 GNU/Linux host:

```sh
PIPEWIRE_SOURCE_DIR=/path/to/pipewire node/tools/check-activation-abi.sh
node/tools/check-spa-buffer-abi.sh
```

This opt-in command compiles and executes `c/activation_abi.c`. It is deliberately
outside `build.rs` and normal tests because executing a target binary during a build
breaks cross-compilation. Run it when updating the pinned PipeWire revision or the
activation ABI table. A mismatch must be reviewed and committed as an explicit ABI
maintenance change; it must not be accepted by generating constants silently.

The SPA buffer probe uses installed public `libspa-0.2` headers. Both commands are
native opt-in checks and never execute a target binary during normal or cross builds.
