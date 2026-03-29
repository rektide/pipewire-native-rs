# Node Integration: Bridging Control-Plane and Data-Plane

## Summary

The `pipewire-native` and `pipewire-native-node` crates have all the building
blocks for hosting a PipeWire node in Rust, but they are not yet wired
together. This document traces the exact gap and describes what must change for
a working audio-processing node (as exercised by `examples/wav-player`).

## Current state

### What works

- `pipewire-native` can connect to a PipeWire daemon, enumerate objects, and
  create nodes via `Core::create_object()`.
- The protocol layer decodes `Core::AddMem` events, including receiving the
  SCM\_RIGHTS file descriptor.
- `pipewire-native-node` implements memfd mapping, eventfd signaling, transport
  binding, and an async runtime worker that drives process callbacks.

### What is missing

The protocol side correctly receives shared memory file descriptors but the
`Core` object discards them. There is no path for `AddMem` fds, `RemoveMem`
events, or transport/activation descriptors to reach the `node` crate's
`ControlPlaneState`.

## The gap in detail

### 1. Core closes AddMem fds instead of forwarding them

File: `pipewire/src/core.rs:158-161`

```rust
add_mem: some_closure!([] id, type_, fd, flags, {
    debug!("got add_mem: {id} {type_} {flags}");
    let _ = unsafe { libc::close(fd) };
}),
```

The server sends shared memory regions via `Core::AddMem`. Each carries an fd
for a memfd that backs audio buffers. The closure receives the fd as a
`RawFd`, logs it, then closes it.

**What should happen instead:** The `RawFd` should be wrapped in an `OwnedFd`
and forwarded to the node's `ControlPlaneState::on_add_mem()`, which registers
it in the `MemoryRegistry`. Once registered, the transport can later mmap the
buffers.

The closure currently captures nothing (`[]`), so it has no way to reach a
`ControlPlaneState`. The `Core` needs either:

- an optional data-plane callback / listener for `AddMem`/`RemoveMem`, or
- a shared reference to a `ControlPlaneState` that interested nodes can
  register.

### 2. RemoveMem events are no-ops

File: `pipewire/src/core.rs:162-164`

```rust
remove_mem: some_closure!([] id, {
    debug!("got remove_mem: {id}");
}),
```

The server revokes shared memory via `Core::RemoveMem`. Currently the id is
logged and discarded. This should call
`ControlPlaneState::on_remove_mem(RemoveMemEvent { id })` to unmap and drop
the region.

### 3. No transport/activation event decode

PipeWire sends transport setup information (eventfd pair + activation memory
location) to nodes as part of the node lifecycle. The `pipewire-native`
protocol layer does not yet decode or surface these events.

The `pipewire-native-node` crate already has the receiving types:

- `node::control::TransportEvent` (read\_fd, write\_fd, activation region)
- `node::control::AddMemEvent` / `RemoveMemEvent`
- `node::control::ControlPlaneState` which accumulates state and exposes
  `try_bind_transport()`

But nothing populates them from the protocol side.

### 4. No adapter-factory node creation path

Creating a working audio node requires calling
`core.create_object("adapter-factory", ...)` with the right properties
(media type, subtype, format). The wav-player example includes the sketch in
comments but this has not been tested end-to-end.

## Data flow: what a complete path looks like

```
PipeWire daemon
    |
    | Core::AddMem(mem_id=0, fd=<memfd>, ...)
    v
pipewire-native protocol (marshal/core.rs)
    |  decodes pod + pops fd from SCM_RIGHTS
    v
Core add_mem callback
    |  wraps RawFd -> OwnedFd
    |  forwards AddMemEvent to ControlPlaneState
    v
ControlPlaneState::on_add_mem(AddMemEvent { id, fd, ... })
    |  inserts fd into MemoryRegistry
    v
    ... later ...
    |
    | Transport event (read_fd, write_fd, activation mem_id/offset/size)
    v
ControlPlaneState::on_transport(TransportEvent { ... })
    |  stores pending TransportConfig
    v
ControlPlaneState::try_bind_transport()
    |  maps activation region from MemoryRegistry
    |  creates BoundTransport with eventfds + mapped memory
    v
NodeRuntime::new(transport, process_callback)
    |  spawns on Tokio runtime
    v
runtime loop:
    wait_cycle()          -- blocks on read eventfd
    -> process_callback() -- user fills audio buffers
    -> signal_complete()  -- writes write eventfd
```

## Proposed changes

### pipewire-native changes

1. **Add data-plane event hooks to Core.**

   Add optional callbacks to `CoreEvents` (or a separate `DataPlaneEvents`
   struct) for `add_mem`, `remove_mem`. These fire *before* any default
   handling, giving the node crate a chance to claim the fd.

2. **Preserve the AddMem fd instead of closing it.**

   Change the `add_mem` closure in `Core::new()` to forward the `RawFd` to
   registered listeners rather than closing it. The listener is responsible for
   ownership.

3. **Decode and surface transport events.**

   Add marshal/demarshal support for node transport setup messages. These
   arrive after the node is created and the server has negotiated buffers.

4. **Expose activation/transport as node-level events.**

   Wire transport events through `NodeEvents` or a new `NodeDataPlaneEvents`
   so node users can feed them into `ControlPlaneState`.

### pipewire-native-node changes

None expected. The crate's API (`ControlPlaneState`, `BoundTransport`,
`NodeRuntime`, `spawn()`) is designed to receive these events. The gap is
entirely in getting events *to* it.

### Example crate (wav-player)

Once the above changes land, the wav-player needs:

1. Create an adapter node via `core.create_object("adapter-factory", ...)`.
2. Subscribe to node params and handle `EnumFormat`/`Format`/`Buffers`.
3. Register a data-plane listener on the core for `AddMem`/`RemoveMem`.
4. On transport event, call `ControlPlaneState::on_transport()`.
5. Once `try_bind_transport()` succeeds, spawn the `NodeRuntime` with a
   process callback that copies WAV PCM into the shared buffer.
6. Wire up format negotiation (sample rate, channels) and handle the case
   where the WAV format doesn't match the negotiated format.

## Existing building blocks

| Component | Location | Status |
|---|---|---|
| AddMem/RemoveMem decode | `pipewire/src/protocol/marshal/core.rs:292-311` | Decodes + receives fd |
| Core add\_mem callback | `pipewire/src/core.rs:158-161` | Closes fd (gap) |
| Core remove\_mem callback | `pipewire/src/core.rs:162-164` | No-op (gap) |
| SCM\_RIGHTS receive | `pipewire/src/protocol/connection.rs` | Working |
| ControlPlaneState | `node/src/control/mod.rs` | Ready |
| AddMemEvent / RemoveMemEvent | `node/src/control/events.rs` | Ready |
| TransportEvent | `node/src/control/events.rs` | Ready |
| MemoryRegistry | `node/src/shm/registry.rs` | Ready |
| MappedRegion (mmap) | `node/src/shm/memfd.rs` | Ready |
| EventFd (async) | `node/src/signal/eventfd.rs` | Ready |
| BoundTransport | `node/src/transport/mod.rs` | Ready |
| NodeRuntime + spawn | `node/src/runtime/mod.rs` | Ready |

## Example WAV files

Sample WAV files for testing are available at
[`~/archive/pdx-cs-sound/wavs`](file:///home/rektide/archive/pdx-cs-sound/wavs),
including `sine.wav`, `synth.wav`, `voice.wav`, and others.
