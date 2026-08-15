# pipewire-native-rs

This is a native implementation of the [PipeWire](https://pipewire.org) client
library in Rust. The primary objective is to provide a safe, idiomatic API for
PipeWire clients, with a secondary goal of providing a C wrapper for clients in
other languages to benefit from the safety guarantees in the longer term.

Currently, support for connecting to a PipeWire server, enumerating objects,
and creating server-side objects is supported. Further work is required for
sending and receiving audio/video.

Being a work-in-progress, the API will likely change as we iterate.

Also included is `pw-browse`, a TUI tool to interact with PipeWire. This is
also under development, and will gain features as we make improvements to the
library.

## Project status

This workspace contains several layers at different maturity levels:

| Area | Crate/directory | Status |
|---|---|---|
| SPA primitives and support interfaces | `spa/` | Implements POD codecs, parameter types, hooks, and hybrid Rust/C SPA support. Safety hardening is ongoing. |
| Native PipeWire client | `pipewire/` | Connects to a daemon, manages proxies, enumerates the graph, and creates server-side objects. |
| Native frame transport | `protocol/` | Implements incremental native frames with frame-owned SCM_RIGHTS descriptors. Client and scripted-peer migration is in progress. |
| Deterministic scripted peer | `server/` | Exercises native-protocol behavior over real Unix sockets without requiring a full daemon. It is test infrastructure, not a production server. |
| Node data-plane substrate | `node/` | Provides imported-memory, mapping, eventfd, and worker primitives. It is not yet a complete ClientNode implementation. |
| WAV playback target | `examples/wav-player/` | Parses PCM WAV files and connects to PipeWire; typed ClientNode buffer processing remains under development. |
| Object browser | `tools/` | Provides the experimental `pw-browse` TUI. |

The next product milestone is one ownership-safe ClientNode v6 process cycle with
typed activation, port IO, and media buffers. The architecture and active work are
described in the
[`architecture re-kickoff`](/.design/architecture-review/review-rekick0.oc.md).

## Documentation

Crate documentation can be found on
[docs.rs](https://docs.rs/pipewire-native/latest/pipewire_native/).

Architecture and discovery documents live under [`/.design`](/.design) and
[`/doc/discovery`](/doc/discovery). Start with the
[`architecture review index`](/.design/architecture-review/index.md) for current
direction and links to historical plans.

## Testing

Run deterministic workspace tests that do not launch a host PipeWire daemon with:

```sh
cargo test --workspace --exclude pipewire-native
cargo test -p pipewire-native --test scripted_server_add_mem
```

The node crate's private activation ABI support and opt-in upstream differential
probe are documented in [`node/README.md`](/node/README.md). Normal builds do not
require a PipeWire source checkout.

The complete `pipewire-native` integration suite also exercises an external
`pipewire` executable and installed SPA modules. These tests have bounded waits but
remain environment-dependent:

```sh
cargo test -p pipewire-native
```

## Issues

Issues and suggestions for improvements can be submitted on the
[freedesktop.org
Gitlab](https://gitlab.freedesktop.org/pipewire/pipewire-native-rs).

## Code structure

At the top-level, we have a native implementation of the
[PipeWire native
protocol](https://docs.pipewire.org/devel/page_native_protocol.html). This is
then exposed via the API in `pipewire/`, the entry point for this crate.

Similar to the C version, the `spa/` crate implements low-level primitives for
the PipeWire library.

There is a native implementation for some primitives, such as pod, for data
serialisation/deserialisation. There are also associated traits and macros (in
`macros/`) to reduce boilerplate.

A hybrid strategy is used for SPA plugins (which provide basic features such as
logging, event loops and a system call API). The SPA interfaces are exposed as
Rust interfaces, for use by Rust code. The underlying implementations use the C
plugin under the hood, with the option to be replaced by a Rust implementation
in the future if desired.

## Related Work

This project draws inspiration from other efforts like the current Rust
bindings in [pipewire-rs](https://gitlab.freedesktop.org/pipewire/pipewire-rs)
and the
[pipewire-native-protocol](https://github.com/Troels51/pipewire-native-protocol)
implementation. The goal is for these bindings to eventually be the official
PipeWire Rust API.
