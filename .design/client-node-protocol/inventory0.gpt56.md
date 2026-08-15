---
type: ProtocolInventory
title: Minimum ClientNode output-cycle protocol inventory
description: Upstream-grounded native-wire and shared-memory sequence for one PipeWire ClientNode output/playback cycle.
resource: /.design/client-node-protocol/inventory0.gpt56.md
tags: [pipewire, client-node, native-protocol, playback, activation, buffers, rust]
status: draft
generated: { by: agent:gpt56, at: 2026-08-15T00:00:00Z }
stale_after: 2026-11-15
sources:
  - id: upstream-client-node-api
    resource: https://gitlab.freedesktop.org/pipewire/pipewire/-/blob/69c1b4c8b6a1cfa95982e5ed740a3995d94c1308/src/pipewire/extensions/client-node.h
    title: PipeWire ClientNode extension API
    author: project:pipewire
    last_modified: 2026-08-15
  - id: upstream-client-node-native
    resource: https://gitlab.freedesktop.org/pipewire/pipewire/-/blob/69c1b4c8b6a1cfa95982e5ed740a3995d94c1308/src/modules/module-client-node/protocol-native.c
    title: PipeWire ClientNode native marshal implementation
    author: project:pipewire
    last_modified: 2026-08-15
  - id: upstream-client-node-server
    resource: https://gitlab.freedesktop.org/pipewire/pipewire/-/blob/69c1b4c8b6a1cfa95982e5ed740a3995d94c1308/src/modules/module-client-node/client-node.c
    title: PipeWire server-side ClientNode implementation
    author: project:pipewire
    last_modified: 2026-08-15
  - id: upstream-remote-node
    resource: https://gitlab.freedesktop.org/pipewire/pipewire/-/blob/69c1b4c8b6a1cfa95982e5ed740a3995d94c1308/src/modules/module-client-node/remote-node.c
    title: PipeWire exported remote-node implementation
    author: project:pipewire
    last_modified: 2026-08-15
  - id: architecture-rekickoff
    resource: /.design/architecture-review/review-rekick0.oc.md
    title: PipeWire native Rust architecture re-kickoff
    author: agent:opencode
  - id: native-frame-design
    resource: /.design/native-frame/frame0.gpt56.md
    title: Native frame and file-descriptor transport architecture
    author: agent:gpt56
---

# Minimum ClientNode Output-Cycle Protocol Inventory

## Scope and baseline

This inventory resolves the protocol-discovery part of
`SU-client-node-cycle-min-protocol`. It traces a client-created, fixed-port
`ClientNode` that produces PCM on one `SPA_DIRECTION_OUTPUT` port and is linked
to a playback sink. The node is a source in SPA direction terms even though the
application's use case is playback.

All upstream claims refer to PipeWire commit
[`69c1b4c8`](https://gitlab.freedesktop.org/pipewire/pipewire/-/commit/69c1b4c8b6a1cfa95982e5ed740a3995d94c1308),
the local `~/archive/pipewire/pipewire` checkout. No `llms.txt` exists there.
The target is native protocol v3 framing plus `ClientNode` interface version 6.
Native framing and frame-owned FD tables are specified separately in
[`frame0.gpt56.md`](/.design/native-frame/frame0.gpt56.md).

The critical correction to the current Rust sketch is:

> For ClientNode v6, a cycle is not completed merely by writing the transport
> write eventfd. The client must implement the shared activation transitions and,
> for a non-driving synchronous output node, trigger configured peer activations.

The transport eventfd pair remains part of the ABI. The read FD wakes this node.
The server-side read of the other FD is legacy completion behavior for versions
below 5 and a completion path for client-scheduled/driving cases; normal v6
non-driving graph propagation is expressed through activation memory and the
`SetActivation` peer FD
([`client-node.c#L1180-L1207`](https://gitlab.freedesktop.org/pipewire/pipewire/-/blob/69c1b4c8b6a1cfa95982e5ed740a3995d94c1308/src/modules/module-client-node/client-node.c#L1180-1207),
[`impl-node.c#L1485-L1554`](https://gitlab.freedesktop.org/pipewire/pipewire/-/blob/69c1b4c8b6a1cfa95982e5ed740a3995d94c1308/src/pipewire/impl-node.c#L1485-1554)).

## Interface inventory

### Versions and object identity

| Item | Exact value | Evidence |
|---|---:|---|
| Native protocol header | v3, 16 bytes | [`protocol.dox#L12-57`](https://gitlab.freedesktop.org/pipewire/pipewire/-/blob/69c1b4c8b6a1cfa95982e5ed740a3995d94c1308/doc/dox/internals/protocol.dox#L12-57) |
| Core object ID | `0` | [`core.h#L53-60`](https://gitlab.freedesktop.org/pipewire/pipewire/-/blob/69c1b4c8b6a1cfa95982e5ed740a3995d94c1308/src/pipewire/core.h#L53-60) |
| Core interface version | `4` | [`core.h#L36-44`](https://gitlab.freedesktop.org/pipewire/pipewire/-/blob/69c1b4c8b6a1cfa95982e5ed740a3995d94c1308/src/pipewire/core.h#L36-44) |
| ClientNode type | `PipeWire:Interface:ClientNode` | [`client-node.h#L23-L30`](https://gitlab.freedesktop.org/pipewire/pipewire/-/blob/69c1b4c8b6a1cfa95982e5ed740a3995d94c1308/src/pipewire/extensions/client-node.h#L23-30) |
| Factory name | `client-node` | [`module-client-node.c#L93-L94`](https://gitlab.freedesktop.org/pipewire/pipewire/-/blob/69c1b4c8b6a1cfa95982e5ed740a3995d94c1308/src/modules/module-client-node.c#L93-94) |
| ClientNode interface version | `6` | [`client-node.h#L25-L30`](https://gitlab.freedesktop.org/pipewire/pipewire/-/blob/69c1b4c8b6a1cfa95982e5ed740a3995d94c1308/src/pipewire/extensions/client-node.h#L25-30) |
| ClientNode methods ABI version | `0` | [`client-node.h#L239-L247`](https://gitlab.freedesktop.org/pipewire/pipewire/-/blob/69c1b4c8b6a1cfa95982e5ed740a3995d94c1308/src/pipewire/extensions/client-node.h#L239-247) |
| ClientNode events ABI version | `1` | [`client-node.h#L61-L64`](https://gitlab.freedesktop.org/pipewire/pipewire/-/blob/69c1b4c8b6a1cfa95982e5ed740a3995d94c1308/src/pipewire/extensions/client-node.h#L61-64) |
| Activation ABI version | `1` | [`private.h#L547-L554`](https://gitlab.freedesktop.org/pipewire/pipewire/-/blob/69c1b4c8b6a1cfa95982e5ed740a3995d94c1308/src/pipewire/private.h#L547-554) |
| Output direction | `SPA_DIRECTION_OUTPUT == 1` | [`defs.h#L92-L98`](https://gitlab.freedesktop.org/pipewire/pipewire/-/blob/69c1b4c8b6a1cfa95982e5ed740a3995d94c1308/spa/include/spa/utils/defs.h#L92-98) |
| Invalid/mix sentinel | `SPA_ID_INVALID == 0xffffffff` | [`defs.h`](https://gitlab.freedesktop.org/pipewire/pipewire/-/blob/69c1b4c8b6a1cfa95982e5ed740a3995d94c1308/spa/include/spa/utils/defs.h) |

Version history matters operationally: v4 added `PortSetMixInfo`, v5 moved driver
scheduling to the client, and v6 requires the client to perform
`INACTIVE -> FINISHED` readiness. The first implementation should request exactly
version 6 rather than silently implementing v4 completion semantics.

### Client-to-server methods

Opcode 0 is local `AddListener` and has no wire demarshaller. All other payloads
are one outer SPA `Struct`, with fields in the order below
([`protocol-native.c#L1168-L1189`](https://gitlab.freedesktop.org/pipewire/pipewire/-/blob/69c1b4c8b6a1cfa95982e5ed740a3995d94c1308/src/modules/module-client-node/protocol-native.c#L1168-1189)).

| Opcode | Method | Exact POD field sequence | First cycle |
|---:|---|---|---|
| 1 | `GetNode` | `Int(version), Int(new_id)` | Optional; only needed for a separate `Node` proxy |
| 2 | `Update` | `Int(change_mask), Int(n_params), Pod[n_params], PodStruct(info or None)` | Mandatory advertisement |
| 3 | `PortUpdate` | `Int(direction), Int(port_id), Int(change_mask), Int(n_params), Pod[n_params], PodStruct(info or None)` | Mandatory output-port advertisement |
| 4 | `SetActive` | `Bool(active)` | Mandatory to join/leave graph |
| 5 | `Event` | `PodObject(event)` | Later |
| 6 | `PortBuffers` | `Int(direction), Int(port_id), Int(mix_id), Int(n_buffers)`, then per buffer `Int(n_datas)`, then per data `Id(type), Fd(fd_index), Int(flags), Int(mapoffset), Int(maxsize)` | Conditional response to server `ALLOC` |

`Update.info`, when present, is a nested `Struct`:

```text
Int(max_input_ports)
Int(max_output_ports)
Long(spa_node_info.change_mask)
Long(spa_node_info.flags)
Int(n_property_items)
repeat n_property_items: String(key), String(value)
Int(n_param_info)
repeat n_param_info: Id(param_id), Int(param_flags)
```

`PortUpdate.info`, when present, is a nested `Struct`:

```text
Long(spa_port_info.change_mask)
Long(spa_port_info.flags)
Int(rate.num)
Int(rate.denom)
Int(n_property_items)
repeat n_property_items: String(key), String(value)
Int(n_param_info)
repeat n_param_info: Id(param_id), Int(param_flags)
```

These layouts are directly paired in
[`protocol-native.c#L163-L285`](https://gitlab.freedesktop.org/pipewire/pipewire/-/blob/69c1b4c8b6a1cfa95982e5ed740a3995d94c1308/src/modules/module-client-node/protocol-native.c#L163-285)
and
[`protocol-native.c#L967-L1077`](https://gitlab.freedesktop.org/pipewire/pipewire/-/blob/69c1b4c8b6a1cfa95982e5ed740a3995d94c1308/src/modules/module-client-node/protocol-native.c#L967-1077).
The initial output port should advertise at least readable `EnumFormat`,
`Buffers`, `Meta`, and `IO` capabilities and a concrete audio `EnumFormat` POD.
The exact PCM format POD is application input, not hard-coded ClientNode wire.

### Server-to-client events

All payloads are one outer SPA `Struct`. The event opcode table is canonical at
[`client-node.h#L47-L59`](https://gitlab.freedesktop.org/pipewire/pipewire/-/blob/69c1b4c8b6a1cfa95982e5ed740a3995d94c1308/src/pipewire/extensions/client-node.h#L47-59)
and registration is at
[`protocol-native.c#L1191-L1234`](https://gitlab.freedesktop.org/pipewire/pipewire/-/blob/69c1b4c8b6a1cfa95982e5ed740a3995d94c1308/src/modules/module-client-node/protocol-native.c#L1191-1234).

| Opcode | Event | Exact POD field sequence | First linked output cycle |
|---:|---|---|---|
| 0 | `Transport` | `Fd(read_index), Fd(write_index), Int(mem_id), Int(offset), Int(size)` | Mandatory |
| 1 | `SetParam` | `Id(id), Int(flags), PodObject(param)` | Usually not needed for minimal fixed node |
| 2 | `SetIo` | `Id(id), Int(mem_id), Int(offset), Int(size)` | Optional; transport embeds Position/Clock |
| 3 | `Event` | `PodObject(event)` | Later |
| 4 | `Command` | `PodObject(command)` | Mandatory `Start`; `Pause`/`Suspend` for teardown |
| 5 | `AddPort` | `Int(direction), Int(port_id), Struct{Int(n_items), String pairs}` | Later; fixed port is client-advertised |
| 6 | `RemovePort` | `Int(direction), Int(port_id)` | Later/dynamic teardown |
| 7 | `PortSetParam` | `Int(direction), Int(port_id), Id(id), Int(flags), PodObject(param)` | Mandatory `SPA_PARAM_Format` |
| 8 | `PortUseBuffers` | layout below | Mandatory |
| 9 | `PortSetIo` | `Int(direction), Int(port_id), Int(mix_id), Id(id), Int(mem_id), Int(offset), Int(size)` | Mandatory (`Buffers` or `AsyncBuffers`) |
| 10 | `SetActivation` | `Int(node_id), Fd(signal_index), Int(mem_id), Int(offset), Int(size)` | Mandatory for a real downstream peer; removable form is required for teardown |
| 11 | `PortSetMixInfo` | `Int(direction), Int(port_id), Int(mix_id), Int(peer_id), Struct{Int(n_items), String pairs}` | Optional for one output; interface >=4 |

`PortUseBuffers` is exactly:

```text
Int(direction)
Int(port_id)
Int(mix_id)
Int(flags)
Int(n_buffers)
repeat n_buffers:
  Int(metadata_mem_id)
  Int(metadata_offset)
  Int(metadata_size)
  Int(n_metas)
  repeat n_metas: Id(meta_type), Int(meta_size)
  Int(n_datas)
  repeat n_datas:
    Id(data_type)
    Int(data_id_or_relative_offset)
    Int(data_flags)
    Int(mapoffset)
    Int(maxsize)
```

The buffer descriptor carries no SCM_RIGHTS itself. `metadata_mem_id` and a
`SPA_DATA_MemId` data ID refer to prior Core `AddMem` imports. A
`SPA_DATA_MemPtr` data value is a signed relative offset inside the metadata
mapping, represented through the integer/pointer conversion used upstream.
Metadata begins at mapping offset zero, followed by each metadata payload rounded
to 8 bytes, then one `spa_chunk` per data plane
([`remote-node.c#L615-L725`](https://gitlab.freedesktop.org/pipewire/pipewire/-/blob/69c1b4c8b6a1cfa95982e5ed740a3995d94c1308/src/modules/module-client-node/remote-node.c#L615-725)).

`flags & SPA_NODE_BUFFERS_FLAG_ALLOC` (`1 << 0`) reverses allocation: the client
must allocate and answer with method opcode 6 `PortBuffers`. The narrow first
slice should reject this mode explicitly and test it as deferred coverage; the
normal server-allocated memfd path needs no outbound FD-bearing ClientNode frame.

### Core messages on this path

Core method opcode 6 `CreateObject` is:

```text
Struct {
  String("client-node")
  String("PipeWire:Interface:ClientNode")
  Int(6)
  Struct { Int(n_properties), repeat String(key), String(value) }
  Int(new_client_object_id)
}
```

This exact layout is emitted at
[`protocol-native.c#L238-L270`](https://gitlab.freedesktop.org/pipewire/pipewire/-/blob/69c1b4c8b6a1cfa95982e5ed740a3995d94c1308/src/modules/module-protocol-native/protocol-native.c#L238-270).
Core method opcode 7 `Destroy` is `Struct{Int(object_id)}`.

Core event opcode 6 `AddMem` is
`Struct{Int(id), Id(spa_data_type), Fd(fd_index), Int(flags)}` and carries one
SCM_RIGHTS entry. Core event opcode 7 `RemoveMem` is `Struct{Int(id)}`
([`protocol-native.c#L422-L455`](https://gitlab.freedesktop.org/pipewire/pipewire/-/blob/69c1b4c8b6a1cfa95982e5ed740a3995d94c1308/src/modules/module-protocol-native/protocol-native.c#L422-455),
[`protocol-native.c#L572-L598`](https://gitlab.freedesktop.org/pipewire/pipewire/-/blob/69c1b4c8b6a1cfa95982e5ed740a3995d94c1308/src/modules/module-protocol-native/protocol-native.c#L572-598)).
The current Rust `AddMem` DTO omits the `Fd` POD and pops the next global FD
instead ([`/pipewire/src/protocol/marshal/core.rs#L224-L230`](/pipewire/src/protocol/marshal/core.rs#L224-L230),
[`/pipewire/src/protocol/marshal/core.rs#L292-L308`](/pipewire/src/protocol/marshal/core.rs#L292-L308)); this must be corrected before ClientNode memory references are trustworthy.

### FD table indices and ownership

Indices are message-local SPA `Fd` values into the native frame's ordered FD
table, not operating-system FD numbers and not a connection-global pop order.

| Message | First emission indices | Ownership use |
|---|---|---|
| Core `AddMem` | `fd_index = 0` | Transfer one `OwnedFd` to connection memory pool |
| ClientNode `Transport` | `read_index = 0`, `write_index = 1` | Transfer both into this node transport generation |
| ClientNode `SetActivation` | `signal_index = 0` | Transfer into peer activation entry |
| ClientNode `PortBuffers` method | insertion order over buffer/data; upstream helper may deduplicate repeated raw FDs | Server borrows received descriptors into imported buffers |

The listed numeric indices hold when each canonical message is emitted alone, as
upstream does here. Decoders must nevertheless resolve the encoded index, validate
it against that frame, and reject duplicate ownership-taking. They must not assume
the index from field position. A transport replacement atomically supersedes and
closes both prior transport FDs only after the new mapping and descriptors validate.

## Shared-memory ABI

### Activation record

`Transport.size` and `SetActivation.size` are
`sizeof(struct pw_node_activation)` in the server
([`client-node.c#L1307-L1384`](https://gitlab.freedesktop.org/pipewire/pipewire/-/blob/69c1b4c8b6a1cfa95982e5ed740a3995d94c1308/src/modules/module-client-node/client-node.c#L1307-1384)).
This is a native shared-memory C ABI, not a POD and not a stable network-endian
format. The Rust view must be target-ABI checked against the pinned C headers.

Relevant field order from
[`private.h#L508-L637`](https://gitlab.freedesktop.org/pipewire/pipewire/-/blob/69c1b4c8b6a1cfa95982e5ed740a3995d94c1308/src/pipewire/private.h#L508-637):

```c
struct pw_node_activation_state { int status; int32_t required; int32_t pending; };

struct pw_node_activation {
    uint32_t status;
    unsigned int version:1, pending_sync:1, pending_new_pos:1;
    struct pw_node_activation_state state[2];
    uint64_t signal_time, awake_time, finish_time, prev_signal_time;
    struct spa_io_segment reposition, segment;
    uint32_t segment_owner[16];
    uint64_t prev_awake_time, prev_finish_time;
    uint32_t padding[7];
    uint32_t client_version, server_version;
    uint32_t active_driver_id, driver_id, flags;
    struct spa_io_position position;
    uint64_t sync_timeout, sync_left;
    float cpu_load[3];
    uint32_t xrun_count;
    uint64_t xrun_time, xrun_delay, max_delay;
    uint32_t command, reposition_owner;
};
```

On x86_64 LP64 at this upstream commit, the stable prefix offsets derived from
the C layout are `status=0`, bitfield storage `=4`, `state[0]=8`, `state[1]=20`,
`signal_time=32`, `awake_time=40`, `finish_time=48`, and `prev_signal_time=56`.
Upstream asserts `sizeof(spa_io_segment)==184` and
`sizeof(spa_io_position)==1688`
([`test-spa-node.c#L13-L27`](https://gitlab.freedesktop.org/pipewire/pipewire/-/blob/69c1b4c8b6a1cfa95982e5ed740a3995d94c1308/test/test-spa-node.c#L13-27)); the resulting x86_64 LP64 activation size is 2312 bytes. The implementation must generate C differential `sizeof`, `_Alignof`, and `offsetof` assertions instead of treating those values as portable.

Activation statuses are exact:

| Value | Name |
|---:|---|
| 0 | `NOT_TRIGGERED` |
| 1 | `TRIGGERED` |
| 2 | `AWAKE` |
| 3 | `FINISHED` |
| 4 | `INACTIVE` |

Atomic/CAS access is required. Upstream documents and implements
`INACTIVE -> FINISHED`, `!INACTIVE -> NOT_TRIGGERED`,
`NOT_TRIGGERED -> TRIGGERED`, `TRIGGERED -> AWAKE`, and
`AWAKE -> FINISHED`
([`private.h#L555-L570`](https://gitlab.freedesktop.org/pipewire/pipewire/-/blob/69c1b4c8b6a1cfa95982e5ed740a3995d94c1308/src/pipewire/private.h#L555-570)).

### Port IO and media buffer

For the synchronous first fixture, `PortSetIo.id == SPA_IO_Buffers == 1` and
the mapped ABI is exactly 8 bytes:

```c
struct spa_io_buffers {
    int32_t status;       /* offset 0 */
    uint32_t buffer_id;   /* offset 4 */
};
```

The async alternative is `SPA_IO_AsyncBuffers == 10`, a 16-byte pair of
`spa_io_buffers`; writer uses `(cycle + 1) & 1`, reader uses `cycle & 1`
([`io.h#L30-L43`](https://gitlab.freedesktop.org/pipewire/pipewire/-/blob/69c1b4c8b6a1cfa95982e5ed740a3995d94c1308/spa/include/spa/node/io.h#L30-43),
[`io.h#L387-L391`](https://gitlab.freedesktop.org/pipewire/pipewire/-/blob/69c1b4c8b6a1cfa95982e5ed740a3995d94c1308/spa/include/spa/node/io.h#L387-391)).

IO status values are `OK=0`, `NEED_DATA=1`, `HAVE_DATA=2`, `STOPPED=4`, and
`DRAINED=8`. For output, the host requests a buffer with `NEED_DATA`; the client
produces into `buffer_id` and changes it to `HAVE_DATA`
([`io.h#L45-L84`](https://gitlab.freedesktop.org/pipewire/pipewire/-/blob/69c1b4c8b6a1cfa95982e5ed740a3995d94c1308/spa/include/spa/node/io.h#L45-84)).

Each data plane has one shared 16-byte `spa_chunk` with fields
`u32 offset, u32 size, i32 stride, i32 flags`. The first PCM cycle sets:

```text
chunk.offset = 0 (or validated ring offset modulo maxsize)
chunk.size   = frames_written * channels * bytes_per_sample
chunk.stride = channels * bytes_per_sample
chunk.flags  = 0 (or EMPTY for intentional silence)
io.buffer_id = selected configured buffer index
io.status    = HAVE_DATA
```

The media slice is the selected data mapping at
`mapoffset + (chunk.offset % maxsize)`, bounded by `maxsize`, while chunk memory
stays in the metadata mapping. `spa_data` semantics and flags are canonical at
[`buffer.h#L51-L99`](https://gitlab.freedesktop.org/pipewire/pipewire/-/blob/69c1b4c8b6a1cfa95982e5ed740a3995d94c1308/spa/include/spa/buffer/buffer.h#L51-99).

## One-cycle partial order

Wire frames preserve each sender's order, but memory, format negotiation, graph
linking, and activation callbacks can interleave. Implement a dependency-driven
session, not one exact transcript.

```mermaid
sequenceDiagram
    participant App as Rust application
    participant Core as Core object 0
    participant CN as ClientNode object N
    participant S as Server node/graph
    participant Peer as Playback peer activation

    App->>Core: CreateObject(6, client-node, ClientNode v6, N)
    App->>CN: Update(node info/capabilities)
    App->>CN: PortUpdate(OUTPUT, 0, params/info)
    Core-->>App: AddMem(activation mem, FD[0])
    CN-->>App: Transport(FD[0], FD[1], activation region)
    App->>App: map; set client_version=1
    CN-->>App: PortSetParam(OUTPUT, 0, Format, PCM POD)
    Core-->>App: AddMem(buffer/IO memory, FD[0])
    CN-->>App: PortUseBuffers(OUTPUT, 0, INVALID, descriptors)
    CN-->>App: PortSetIo(OUTPUT, 0, mix, Buffers, region)
    Core-->>App: AddMem(peer activation, FD[0])
    CN-->>App: SetActivation(peer node, FD[0], peer region)
    App->>CN: SetActive(true)
    CN-->>App: Command(Start)
    App->>App: CAS own INACTIVE -> FINISHED
    S->>App: own activation NOT_TRIGGERED -> TRIGGERED; write transport read FD
    App->>App: read eventfd; CAS TRIGGERED -> AWAKE
    App->>App: fill selected PCM buffer; chunk + IO HAVE_DATA
    App->>App: state[0].status=result; finish_time; CAS AWAKE -> FINISHED
    App->>Peer: decrement pending; CAS to TRIGGERED; write peer signal FD
```

### Required ordering edges

1. `CreateObject` allocates object ID `N` before any method addressed to `N`.
2. `Update` and `PortUpdate` must advertise the node/port before the server can
   negotiate format, allocate buffers, and link it.
3. Every memory-referencing event depends on the matching Core `AddMem` being
   imported. Upstream mempool import emits `AddMem` before the event that exposes
   its ID, but the Rust state machine should safely hold an unresolved descriptor
   until the import arrives or reject after a bounded barrier.
4. `PortSetParam(Format)` precedes non-empty `PortUseBuffers` for that format;
   format replacement invalidates prior buffers
   ([`client-node.c#L669-L698`](https://gitlab.freedesktop.org/pipewire/pipewire/-/blob/69c1b4c8b6a1cfa95982e5ed740a3995d94c1308/src/modules/module-client-node/client-node.c#L669-698)).
5. Non-empty `PortUseBuffers` precedes usable `PortSetIo` and processing. Upstream
   allocation configures buffers before installing port IO
   ([`impl-port.c#L2178-L2207`](https://gitlab.freedesktop.org/pipewire/pipewire/-/blob/69c1b4c8b6a1cfa95982e5ed740a3995d94c1308/src/pipewire/impl-port.c#L2178-2207)).
6. `Transport` mapping must exist before writing `client_version` or activation
   state. For v6, readiness must reach `FINISHED` before the server can schedule.
7. A process wake is valid only if CAS `TRIGGERED -> AWAKE` succeeds. A stale or
   coalesced eventfd count does not authorize multiple callbacks over one activation.
8. Buffer/chunk/IO writes happen-before `AWAKE -> FINISHED`; peer signaling happens
   only after successful completion of that transition for synchronous nodes.

The first fixture should require these edges while permitting unrelated
`BoundProps`, `Done`, `SetIo`, mix-info, and property/latency traffic.

## Mandatory minimum versus deferred coverage

### Mandatory for one truthful linked output cycle

| Domain | Messages/state |
|---|---|
| Creation | Core `CreateObject`; object route registered as ClientNode v6 |
| Advertisement | ClientNode `Update`; `PortUpdate(OUTPUT, 0)` with PCM and buffer/IO capabilities |
| Memory | Core `AddMem` with indexed owned FD; memory-ID registry |
| Transport | `Transport` with two indexed FDs and activation region; replacement-safe binding |
| Configuration | `PortSetParam(Format)`; `PortUseBuffers`; `PortSetIo(Buffers)` |
| Graph activation | `SetActivation` for downstream playback peer; `SetActive(true)`; `Command(Start)` |
| Process | v6 activation CAS sequence; eventfd drain; output IO selection; writable data mapping; chunk and `HAVE_DATA`; peer trigger |
| Teardown | `Command(Pause or Suspend)` and/or `SetActive(false)`; own status `INACTIVE`; clear IO/buffers/peer activation; Core `Destroy`; `RemoveMem`/disconnect cleanup |

For a scripted one-cycle fixture, `Update`/`PortUpdate` can use a deliberately
small fixed PCM capability set, but they cannot be omitted if the fixture claims
to model object creation rather than injecting an already-configured object.

### Optional or later

| Coverage | Reason to defer |
|---|---|
| `GetNode` and regular `Node` proxy | Introspection/control convenience, not data-plane bootstrap |
| Node-level `SetParam`, `SetIo`, and generic `Event` | Position is embedded in activation for this path; no node-specific parameter required |
| Dynamic `AddPort`/`RemovePort` | Fixed output port is advertised by client |
| `PortSetMixInfo` | Useful peer metadata, not required to identify the one output buffer/IO path |
| `SPA_IO_AsyncBuffers` | Separate cycle-index semantics; first fixture is synchronous |
| `SPA_NODE_BUFFERS_FLAG_ALLOC` and `PortBuffers` | Requires outbound FD table construction and client allocator policy |
| DMA-BUF and SyncObj | First slice accepts mappable memfd only and rejects unsupported types explicitly |
| Multiple ports, mixes, datas, metas, and buffers beyond a small set | Bounds must decode safely now; behavior parity can follow one data plane |
| Client-scheduled driver mode | Different completion/eventfd responsibilities introduced in ClientNode v5 |
| Reposition, transport sync, profiler, xrun accounting | Activation fields must be preserved, but first cycle need not mutate them |

## Teardown and replacement sequence

Teardown is not simply dropping the proxy. It is a reverse dependency operation:

```mermaid
stateDiagram-v2
    [*] --> Configuring
    Configuring --> Ready: transport + format + buffers + IO
    Ready --> Active: SetActive(true) + Start + own status FINISHED
    Active --> Processing: TRIGGERED -> AWAKE
    Processing --> Active: IO/chunk publish; AWAKE -> FINISHED; peer signal
    Active --> Stopping: Pause/Suspend or SetActive(false)
    Processing --> Stopping: teardown request (defer invalidation until cycle borrow ends)
    Stopping --> Ready: own status INACTIVE; remove peer targets; clear IO/buffers
    Ready --> Destroyed: Core Destroy / proxy removal / disconnect
    Destroyed --> [*]: close FDs and mappings exactly once
```

Expected wire cleanup forms are:

- `SetActivation(node_id, Fd(-1), SPA_ID_INVALID, 0, 0)` removes a peer; upstream
  emits this before unref
  ([`client-node.c#L1332-L1356`](https://gitlab.freedesktop.org/pipewire/pipewire/-/blob/69c1b4c8b6a1cfa95982e5ed740a3995d94c1308/src/modules/module-client-node/client-node.c#L1332-1356)). The encoded negative FD sentinel does not require SCM_RIGHTS.
- `PortSetIo(..., SPA_ID_INVALID, 0, 0)` clears an IO mapping.
- `PortUseBuffers(..., n_buffers=0)` clears negotiated buffers.
- `PortSetParam(..., Format, ..., None)` clears format and invalidates buffers.
- Core `RemoveMem(id)` makes the memory ID unavailable for new borrows; actual
  mapping destruction must wait for any active process-cycle guard.
- Core `Destroy(N)` requests object destruction; Core `RemoveId(N)` acknowledges
  when the local ID can be reused.

If teardown races a wake, safe code must either finish the already-borrowed cycle
and then invalidate, or transition to stopping without exposing a media slice.
It must never unmap behind `ProcessCycle<'a>`.

## Rust module proposal

Keep the layout domain-grouped and avoid placing session semantics in the frame
crate:

```text
pipewire/src/protocol/wire/
  core/
    memory.rs          # AddMem/RemoveMem indexed-FD DTOs
  client_node/
    mod.rs             # interface version and opcode tables
    methods.rs         # Update, PortUpdate, SetActive, optional PortBuffers
    events.rs          # Transport, format, IO, buffers, activation, command
    buffer.rs          # wire-only repeated buffer descriptors and limits

node/src/session/
  mod.rs               # ClientNodeSession state machine and dispatch inputs
  advertisement.rs     # typed node/port capability inputs
  memory.rs            # connection memory references, no FD-table knowledge
  transport.rs         # per-node transport generation and activation binding
  port.rs              # format, IO, and configured buffer generations
  activation.rs        # ABI-checked atomic activation access and peer targets
  cycle.rs             # cycle-scoped IO/chunk/media views and completion
  teardown.rs          # reverse dependency invalidation
```

`protocol::wire::client_node` owns only bounded POD conversion and exact opcode
compatibility. `node::session` owns ordering, memory resolution, generation,
mapping, atomics, and cycle lifetimes. The shared native frame layer owns
`FrameFds`; only wire demarshalling sees a frame FD index.

### Typed construction inputs

The application should provide semantic inputs rather than wire DTOs:

```rust
pub struct OutputNodeSpec {
    pub properties: Properties,
    pub port: OutputPortSpec,
}

pub struct OutputPortSpec {
    pub id: PortId,
    pub properties: Properties,
    pub formats: Vec<AudioFormatCapability>,
    pub buffer_policy: BufferPolicy,
}

pub struct AudioFormatCapability {
    pub sample_format: AudioSampleFormat,
    pub rate: u32,
    pub channels: Vec<AudioChannel>,
}

pub struct BufferPolicy {
    pub min_buffers: u32,
    pub max_buffers: u32,
    pub preferred_frames: Option<u32>,
    pub memory: MemoryPolicy, // first slice: MemFdMappable
}
```

Session dispatch should receive ownership-explicit typed events:

```rust
pub enum ClientNodeInput {
    MemoryAdded(ImportedMemory),
    MemoryRemoved(MemoryId),
    Transport(TransportDescriptor),
    PortFormat(PortFormatUpdate),
    PortBuffers(PortBufferSet),
    PortIo(PortIoDescriptor),
    PeerActivation(PeerActivationDescriptor),
    Command(NodeCommand),
    ProxyRemoved,
}
```

`TransportDescriptor` owns two FDs. `PeerActivationDescriptor` owns one FD.
`PortBufferSet` stores checked memory references, offsets, data IDs, and limits,
not borrowed PODs. Ready state should be obtainable only as a typed value such as
`ReadyOutputNode`; a cycle callback receives `OutputCycle<'_>` whose mutable media,
chunk, IO, and own-activation borrows cannot escape.

## Differential fixture strategy

### Fixture representation

Store each canonical message as three coordinated artifacts:

```text
fixtures/client-node-v6/<case>/
  frame.bin             # exact 16-byte native header + payload bytes
  semantic.json         # interface, opcode, ordered POD AST, normalized IDs
  fds.json              # ordered roles/kinds, never raw process FD numbers
```

For memory fixtures, include deterministic memfd contents in separate `.bin`
files and describe seals, size, and intended region in `fds.json`. For eventfds,
describe role (`transport-trigger`, `transport-complete`, `peer-signal`) and
initial counter. Normalize object IDs and native frame sequence/generation values
in semantic comparison, while raw-byte tests use fixed IDs.

### Upstream oracles

1. Build a small C fixture client against the pinned upstream tree. It uses public
   `pw_core_create_object("client-node", ..., 6)`, `pw_client_node_update`,
   `port_update`, and `set_active`. Connect it to the Rust scripted peer and capture
   upstream-marshalled client-to-server frames byte-for-byte.
2. Run the Rust client against a pinned upstream daemon/module and capture
   server-to-client events generated by real format/link/buffer setup. Decode each
   frame into the normalized POD AST plus FD role manifest.
3. Replay captured upstream event fixtures through Rust decode/session state and
   assert the same ready state and one-cycle memory effects.
4. Send Rust-encoded methods to the upstream daemon and require successful graph
   setup and one process cycle. This is the strongest server-demarshal oracle.
5. For event encoding, add a narrow upstream C harness around the pinned
   ClientNode protocol marshal or instrument a test daemon connection; compare raw
   payload bytes and FD indices. Do not hand-maintain “goldens” from this document.

### Required cases

| Case | Differential assertion |
|---|---|
| Create + advertise | Rust and upstream field ordering, masks, param lists, object route |
| AddMem | POD FD index resolves frame FD 0 and ownership transfers once |
| Transport | FD indices 0/1, mapping region, v1 client activation write |
| Buffer + IO | exact nested descriptor order; metadata/chunk/data region reconstruction |
| One synchronous cycle | same IO/chunk bytes, activation statuses, peer eventfd count |
| Every legal interleaving | dependency state converges without assuming one transcript |
| Missing/repeated FD index | upstream rejection category and Rust no-leak behavior |
| Replace transport/buffers/IO | old generation closes only after successful replacement |
| Teardown during wake | no stale mutable borrow, one completion policy, zero FD leaks |
| ABI probe | C and Rust agree on size/alignment/offsets for activation, IO, chunk |

Every socket test needs a deadline and diagnostics containing last object/opcode,
session state, unresolved memory IDs, transport/port generation, activation status,
buffer ID, buffered frame bytes, and owned FD count.

## Implementation commit sequence

Each item is intended as one small `jj` commit after its tests pass.

1. **Correct Core memory wire ownership.** Decode the explicit `Fd` POD index from
   `AddMem`, transfer one frame-owned FD to one importer, and cover `RemoveMem`.
2. **Add ClientNode opcode and DTO tables.** Implement bounded `Transport`,
   `PortSetParam`, `PortUseBuffers`, `PortSetIo`, `SetActivation`, and `Command`
   event decode plus `Update`, `PortUpdate`, and `SetActive` method encode.
3. **Add upstream-generated wire fixtures.** Check raw POD field order, local FD
   indices, limits, unknown opcode consumption, and no leaks.
4. **Add activation ABI probes and typed atomics.** Generate/verify C ABI size,
   alignment, and offsets; expose only legal CAS transitions and timestamps.
5. **Add per-node transport and peer generations.** Resolve memory IDs, own eventfds,
   replace atomically, and remove peer activations deterministically.
6. **Add port format/buffer/IO state.** Validate nested regions and build checked
   metadata, chunk, and media mappings for mappable memfd only.
7. **Add cycle-scoped output API.** Select output IO buffer, expose writable PCM,
   publish chunk/IO, transition activation, and signal downstream peer.
8. **Add deterministic one-cycle scripted scenario.** Use real memfds/eventfds,
   verify PCM bytes and all shared state, test interleavings and teardown, and assert
   descriptor baseline restoration.
9. **Add upstream-daemon differential gate.** Create/advertise the same fixed output
   node, complete one linked cycle, and compare semantic transcript and ABI effects.
10. **Integrate WAV player through typed node spec.** Remove raw activation/buffer
    TODOs and feed negotiated frame-sized PCM through the same callback path.

## Open questions

1. Does the first accepted fixture require a real downstream peer activation, or
   may an isolated scripted cycle use the transport completion FD? This inventory
   recommends the peer because it proves current v6 non-driving graph behavior.
2. Should the first node advertise one exact PCM format or a bounded rate/channel
   choice? One exact format minimizes negotiation while preserving truthful
   `PortSetParam(Format)` handling.
3. Is synchronous `SPA_IO_Buffers` guaranteed by the chosen scripted topology?
   The fixture can choose it, but the real-daemon gate must explicitly reject
   `AsyncBuffers` until that path is implemented rather than misread its first slot.
4. Which ClientNode properties are required to force a non-driving, autoconnected
   playback topology in CI? This belongs in the real-daemon fixture, not the wire
   DTO layer.
5. Should the initial implementation preserve unknown metadata descriptors as
   opaque checked regions or reject all but known metadata? Preserve descriptors
   without exposing typed access is more compatible, provided all sizes are bounded.
6. How should callback failure publish activation and IO state? It must not leave
   `AWAKE`; likely publish an error status and finish/trigger peers, but this needs
   an upstream behavior fixture before stabilization.
7. Can `RemoveMem` arrive while upstream still retains a mapped region referenced by
   a later event? The Rust ownership model should reject new bindings and defer
   unmap until generation guards drop; differential teardown should confirm ordering.

## Cross-references

- [`/.design/architecture-review/review-rekick0.oc.md`](/.design/architecture-review/review-rekick0.oc.md): defines the minimum-protocol and one-cycle tracer bullets. This inventory supplies their exact ClientNode messages and corrects the completion model.
- [`/.design/native-frame/frame0.gpt56.md`](/.design/native-frame/frame0.gpt56.md): owns native headers, frame-local FD tables, and SCM_RIGHTS lifecycle used by every indexed FD above.
- [`/examples/wav-player/src/main.rs#L100-L135`](/examples/wav-player/src/main.rs#L100-L135): current integration gap. Its transport-only callback sketch needs the port IO, buffer, chunk, and activation behavior inventoried here.
- [`/node/src/control/events.rs`](/node/src/control/events.rs): current ownership-shaped AddMem and Transport inputs; useful substrate but no buffers, peer activations, or v6 state transitions.
- [`/node/src/control/mod.rs`](/node/src/control/mod.rs): current single pending transport is connection-global in effect; this inventory requires per-ClientNode and per-generation state.
- [`/node/src/transport/mod.rs`](/node/src/transport/mod.rs): maps raw activation bytes and always models a completion eventfd; it must gain typed ABI access and version-aware completion semantics.
- [`/node/src/runtime/mod.rs`](/node/src/runtime/mod.rs): currently invokes a callback then writes completion without `TRIGGERED -> AWAKE -> FINISHED`, port IO, chunks, or downstream targets. It is substrate, not yet a v6 process cycle.
- [`pipewire/pipewire` ClientNode protocol marshal](https://gitlab.freedesktop.org/pipewire/pipewire/-/blob/69c1b4c8b6a1cfa95982e5ed740a3995d94c1308/src/modules/module-client-node/protocol-native.c#L128-1234): canonical opcode registration and every POD layout in this inventory.
- [`pipewire/pipewire` remote-node export](https://gitlab.freedesktop.org/pipewire/pipewire/-/blob/69c1b4c8b6a1cfa95982e5ed740a3995d94c1308/src/modules/module-client-node/remote-node.c#L181-220): canonical transport mapping and ClientNode v6 activation-version initialization.
- [`pipewire/pipewire` buffer import](https://gitlab.freedesktop.org/pipewire/pipewire/-/blob/69c1b4c8b6a1cfa95982e5ed740a3995d94c1308/src/modules/module-client-node/remote-node.c#L586-737): canonical reconstruction of metadata, chunks, and memory-ID data planes.
- [`pipewire/pipewire` process state machine](https://gitlab.freedesktop.org/pipewire/pipewire/-/blob/69c1b4c8b6a1cfa95982e5ed740a3995d94c1308/src/pipewire/impl-node.c#L1485-1597): canonical wake, activation CAS, process, result, finish, and target triggering sequence.
- [`pipewire/pipewire` JACK direct ClientNode path](https://gitlab.freedesktop.org/pipewire/pipewire/-/blob/69c1b4c8b6a1cfa95982e5ed740a3995d94c1308/pipewire-jack/src/pipewire-jack.c#L2031-2212): independent production evidence for the same activation and peer-trigger sequence without the generic remote-node wrapper.
