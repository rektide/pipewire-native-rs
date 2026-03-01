# OSC Support Discovery: Initial Response and Plan

## Executive summary

`pipewire-native-rs-node` now has the beginnings of a data-plane (`node/` crate), but there is no OSC-specific API, no control-sequence helpers, and no end-to-end transport wiring from protocol events into node runtime yet.

The most promising path is to treat OSC as a control stream payload in existing PipeWire `application/control` transport instead of inventing a new media subtype. PipeWire already defines `SPA_CONTROL_OSC`, and the control stream model is already how PipeWire carries MIDI/UMP and related control events.

## Current state

### What exists now in this repo

- Control-plane client is functional for object lifecycle and event dispatch in [`/pipewire/src`](/pipewire/src).
- `Core::AddMem` / `Core::RemoveMem` now decode in [`/pipewire/src/protocol/marshal/core.rs`](/pipewire/src/protocol/marshal/core.rs).
- SCM_RIGHTS receive plumbing exists in [`/pipewire/src/protocol/connection.rs`](/pipewire/src/protocol/connection.rs).
- Data-plane primitives exist in [`/node/src`](/node/src):
  - memfd mapping (`shm`)
  - eventfd signaling (`signal`)
  - transport bind (`transport`)
  - runtime loop (`runtime`)

### Gaps relevant to OSC

- `Core` default `add_mem` callback currently closes received fd immediately in [`/pipewire/src/core.rs`](/pipewire/src/core.rs), so mem objects are not retained for a live registry bridge.
- `node/` is not wired into `pipewire/` yet; no end-to-end control-plane -> data-plane host orchestration.
- `spa` crate has `Format::ControlTypes` but no explicit Rust `ControlType` enum (OSC/Midi/UMP) and no typed sequence/control event helpers in [`/spa/src`](/spa/src).
- No OSC packet codec integration exists yet.
- No integration tests cover control-stream payload handling for OSC.

### Source consistency note

The prior prompt listed `node/src/host/mod.rs`, but that file is not present in the current tree. The source list should use existing modules from [`/node/src/lib.rs`](/node/src/lib.rs).

### Upstream findings that shape this plan

- PipeWire already defines `SPA_CONTROL_OSC` in control type declarations ([`pipewire/pipewire` `spa/include/spa/control/control.h`](https://gitlab.com/pipewire/pipewire/-/blob/master/spa/include/spa/control/control.h)).
- PipeWire's MIDI internals document explicitly describes `application/control` as a generic control stream and notes OSC messages can be interleaved in that stream ([`pipewire/pipewire` `doc/dox/internals/midi.dox`](https://gitlab.com/pipewire/pipewire/-/blob/master/doc/dox/internals/midi.dox)).
- Filter-side DSP shortcuts currently expose `"8 bit raw midi"`, `"8 bit raw control"`, and `"32 bit raw UMP"`; there is no dedicated `format.dsp` alias for OSC today ([`pipewire/pipewire` `src/pipewire/filter.c`](https://gitlab.com/pipewire/pipewire/-/blob/master/src/pipewire/filter.c)).
- Stream defaults classify `application/control` as `Midi`, which can mislabel OSC endpoints unless explicit properties are set ([`pipewire/pipewire` `src/pipewire/stream.c`](https://gitlab.com/pipewire/pipewire/-/blob/master/src/pipewire/stream.c), [`pipewire/pipewire` `src/pipewire/keys.h`](https://gitlab.com/pipewire/pipewire/-/blob/master/src/pipewire/keys.h)).

## MVP boundary for OSC support

MVP means:

- One OSC ingress path (UDP -> node output control stream).
- One OSC egress path (node input control stream -> UDP).
- OSC carried as control events in `application/control` using `SPA_CONTROL_OSC`.
- Deterministic, bounded handoff between non-RT socket tasks and RT process callback.
- End-to-end integration test with a minimal scripted server/client setup.

Non-goals for MVP:

- Full OSC 1.1 scheduling semantics.
- General-purpose OSC router/policy engine.
- New PipeWire media subtype.
- Multi-client orchestration and complex session policy.

## Architecture options

### Option A (recommended): Native control-stream OSC inside PipeWire graph

Represent OSC packets as `SPA_CONTROL_OSC` control events in `spa_pod_sequence` buffers over `application/control` links.

Pros:

- Reuses existing PipeWire transport (memfd + eventfd + cycle timing).
- Preserves graph-level timing alignment with audio/video/control cycles.
- Aligns with current `node/` crate direction.

Cons:

- Requires missing typed sequence/control helpers in `spa` crate.
- Requires transport/setup event bridge completion in `pipewire-native`.

### Option B: Side-channel OSC bridge (UDP/TCP) outside graph transport

Keep OSC over sockets as external side-channel and expose only high-level callbacks into node logic.

Pros:

- Fast to prototype.
- Minimal short-term coupling with PipeWire transport internals.

Cons:

- Weaker timing semantics relative to graph cycles.
- Harder to achieve deterministic integration behavior and synchronization.
- Duplicates flow-control and buffering concerns outside existing transport model.

### Option C: New media subtype for OSC

Define and negotiate a new media subtype and dedicated formatting path for OSC.

Pros:

- Strong conceptual separation from generic control stream.

Cons:

- Heavier, invasive changes across type mapping, negotiation, mixer/policy behavior.
- Not needed because control-stream OSC type already exists upstream.

## Recommendation and rationale

Choose **Option A**.

PipeWire already exposes OSC as a control type (`SPA_CONTROL_OSC`) and treats control streams as the generic substrate for timestamped control events. This gives the most direct route to a useful MVP with minimal protocol invention and maximal compatibility with existing graph scheduling and shared-memory transport.

## Control-plane design changes

1. Add typed control enums in `spa` crate
   - add `spa::control` module with `ControlType` (`Properties`, `Midi`, `Osc`, `Ump`, `Unknown`).
2. Add control-sequence encode/decode utilities
   - typed helpers for `spa_pod_sequence` + event offset + control type + bytes payload.
3. Bridge protocol events into node host state
   - stop closing `AddMem` fd by default in `Core` path.
   - retain memory ids and map lifetimes against transport setup.
4. Complete transport/activation event decoding in `pipewire-native`
   - feed decoded descriptors to node control state.
5. Add explicit OSC endpoint metadata policy
   - set explicit `media.class`/props for OSC nodes; do not rely on implicit defaults intended for MIDI.

## Data-plane design changes

1. Add OSC codec integration module
   - initial recommendation: `rosc` for packet encode/decode.
2. Ingress path (UDP -> PipeWire)
   - non-RT task receives OSC packets, validates size/type, pushes into bounded queue.
   - process callback drains queue and writes OSC control events into output sequence buffer.
3. Egress path (PipeWire -> UDP)
   - process callback reads sequence events, filters `Osc`, forwards payload to async sender task.
4. Timing model
   - MVP: immediate dispatch in-cycle with conservative offset policy.
   - future: map OSC timetags to cycle offsets when graph clock translation is available.
5. Backpressure model
   - bounded queues with explicit drop/coalesce policy and tracing counters.

## Risks and unknowns

- **Policy/classification risk**: upstream defaults map `application/control` to `Midi`; OSC nodes may be misrouted without explicit properties.
- **Transport completeness risk**: current repository still lacks full transport/setup event bridge into `node/`.
- **RT safety risk**: naive OSC parsing/allocations in callback path can violate RT constraints.
- **Interoperability risk**: OSC type-tag edge cases and bundle handling vary across clients.
- **Observability risk**: without trace metrics for drops/latency/queue depth, tuning will be guesswork.

## Validation strategy

### Unit

- OSC packet encode/decode roundtrip coverage (messages, bundles, typetags, malformed packets).
- Sequence control encode/decode coverage for `Osc` events.
- Queue and backpressure policy tests.

### Integration

- One scripted scenario: UDP sender -> OSC source node -> PipeWire control link -> OSC sink node -> UDP receiver.
- Verify negotiated format includes `application/control` and OSC control type mask.
- Verify deterministic behavior under burst load and empty-cycle conditions.

### Instrumentation

- `tracing` spans for ingress, process-cycle enqueue/dequeue, egress.
- Counters for accepted/dropped packets, queue depth high-watermark, parse failures.

## Stepwise implementation plan (repo-specific)

1. **Document + align source list**
   - keep this discovery updated and remove stale references.
2. **Add `spa::control` primitives**
   - introduce typed control enum and sequence helpers in `spa/`.
3. **Finish bridge wiring in `pipewire/`**
   - route `AddMem`/`RemoveMem` + transport setup events to node-facing state.
4. **Introduce `node::osc` domain module(s)**
   - `ingress`, `egress`, `codec`, `queue`, `endpoint` submodules.
5. **Implement MVP source/sink endpoints**
   - socket tasks + process callback integration.
6. **Add deterministic integration tests**
   - use scripted server harness from [`/doc/discovery/server.md`](/doc/discovery/server.md).
7. **Harden observability and error model**
   - finalize tracing, drop policies, and explicit error surfaces.

## References

- [`/README.md`](/README.md)
- [`/doc/discovery/node.md`](/doc/discovery/node.md)
- [`/node/src/lib.rs`](/node/src/lib.rs)
- [`/node/src/control/mod.rs`](/node/src/control/mod.rs)
- [`/node/src/runtime/mod.rs`](/node/src/runtime/mod.rs)
- [`/node/src/transport/mod.rs`](/node/src/transport/mod.rs)
- [`/node/src/shm/memfd.rs`](/node/src/shm/memfd.rs)
- [`/node/src/signal/eventfd.rs`](/node/src/signal/eventfd.rs)
- [`/pipewire/src/core.rs`](/pipewire/src/core.rs)
- [`/pipewire/src/protocol/connection.rs`](/pipewire/src/protocol/connection.rs)
- [`/pipewire/src/protocol/marshal/core.rs`](/pipewire/src/protocol/marshal/core.rs)
- [`OpenSoundControl` `spec-1_0`](https://opensoundcontrol.stanford.edu/spec-1_0.html)
- [`OpenSoundControl` `spec-1_1`](https://opensoundcontrol.stanford.edu/spec-1_1.html)
- [`pipewire/pipewire` `spa/include/spa/control/control.h`](https://gitlab.com/pipewire/pipewire/-/blob/master/spa/include/spa/control/control.h)
- [`pipewire/pipewire` `spa/include/spa/param/format.h`](https://gitlab.com/pipewire/pipewire/-/blob/master/spa/include/spa/param/format.h)
- [`pipewire/pipewire` `doc/dox/internals/midi.dox`](https://gitlab.com/pipewire/pipewire/-/blob/master/doc/dox/internals/midi.dox)
- [`pipewire/pipewire` `spa/plugins/control/mixer.c`](https://gitlab.com/pipewire/pipewire/-/blob/master/spa/plugins/control/mixer.c)
- [`pipewire/pipewire` `src/pipewire/filter.c`](https://gitlab.com/pipewire/pipewire/-/blob/master/src/pipewire/filter.c)
- [`pipewire/pipewire` `src/pipewire/stream.c`](https://gitlab.com/pipewire/pipewire/-/blob/master/src/pipewire/stream.c)
- [`pipewire/pipewire` `src/pipewire/keys.h`](https://gitlab.com/pipewire/pipewire/-/blob/master/src/pipewire/keys.h)
- [`klingtnet/rosc` `docs.rs`](https://docs.rs/rosc/latest/rosc/)
