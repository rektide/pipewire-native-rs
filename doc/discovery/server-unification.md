# Server/ Unification Analysis

## Executive Summary

This document analyzes the `server/` test crate implementation against the existing `pipewire/` client library and upstream PipeWire C implementation to identify opportunities for code reuse, type sharing, and architectural alignment.

**Key Finding**: The `server/` crate duplicates significant protocol infrastructure that already exists in `pipewire/src/protocol/`. There are substantial opportunities to share:
- POD serialization types and utilities
- Protocol header definitions and constants
- Marshal/demarshal infrastructure
- Message type definitions

---

## Current State

### What server/ Implements Independently

#### Protocol Layer (`server/src/protocol/`)

1. **Frame I/O** (`frame.rs`):
   - `NativeHeader` struct (16 bytes: object_id, opcode, payload_size, seq, n_fds)
   - `NativePacket` struct
   - `read_packet()`, `read_packet_with_fds()` using SCM_RIGHTS
   - `write_packet()`, `write_packet_with_fds()` using SCM_RIGHTS
   - Header encode/decode logic

2. **Message Types** (`messages.rs`):
   - Protocol constants: `CORE_ID`, `CLIENT_ID`
   - Opcode modules: `core_method`, `core_event`, `client_method`, `registry_method`, `registry_event`
   - `spa_data_type` constants
   - `InboundMessage` enum for server-side parsing
   - Encoder functions: `encode_core_hello_payload()`, `encode_core_done_payload()`, etc.
   - `CoreAddMemPayload` struct
   - Uses `pipewire_native_spa` for POD building/parsing

3. **Runtime** (`runtime/mod.rs`):
   - `ServerConfig`, `ScriptedServer`, `RunReport`
   - Scenario execution loop
   - Action application logic
   - `create_memfd_for_add_mem()` helper

4. **Script/State** (`script/mod.rs`, `state/mod.rs`):
   - `Scenario`, `ScriptStep`, `Expectation`, `Action`
   - Action payload structs: `CoreInfoAction`, `RegistryGlobalAction`, etc.
   - `ExecutionState` tracking

### What pipewire/ Already Has

#### Protocol Infrastructure (`pipewire/src/protocol/`)

1. **Connection Layer** (`connection.rs`):
   - Buffer management for I/O
   - `read()` with SCM_RIGHTS fd receiving
   - `flush()` for outbound writes
   - `next_message()` header parsing
   - `decode_message<T>()` generic demarshaling
   - `push<T: Marshallable>()` for outbound
   - `pop_fd()` for received fds
   - Generation tracking (client/core generations)

2. **Marshal System** (`marshal/`):
   - `Marshallable` trait with `opcode()`, `encode()`, `decode()`
   - `PairList<K, V>` for dictionary encoding (used by both core and registry)
   - `Message<T, F>` wrapper with header + object + footer
   - `Header` struct (identical wire format to server's `NativeHeader`)
   - `CoreFooter`, `ClientFooter` for generation tracking

3. **Core Marshal** (`marshal/core.rs`):
   - `Methods` enum: `Hello`, `Sync`, `Pong`, `Error`, `GetRegistry`, `CreateObject`, `Destroy`
   - `Events` enum: `Info`, `Done`, `Ping`, `Error`, `RemoveId`, `BoundId`, `AddMem`, `RemoveMem`, `BoundProps`
   - `Hello`, `Sync`, `Pong`, `ErrorMethod`, `GetRegistry`, `CreateObject`, `Destroy` structs
   - `Info`, `Done`, `Ping`, `ErrorEvent`, `RemoveId`, `BoundId`, `AddMem`, `RemoveMem`, `BoundProps` structs
   - `Methods::marshal()` returns `CoreMethods<Core>` closures
   - `Events::demarshal()` for client-side event handling

4. **Registry Marshal** (`marshal/registry.rs`):
   - `Methods` enum: `Bind`, `Destroy`
   - `Events` enum: `Global`, `GlobalRemove`
   - `Bind`, `Destroy`, `Global`, `GlobalRemove` structs
   - `Methods::marshal()` returns `RegistryMethods<Registry>` closures
   - `Events::demarshal()` for client-side event handling

5. **Message Types** (`marshal/message.rs`):
   - `Header` struct: id, opcode, size (24-bit), seq, n_fds
   - `Message<T, F>` struct
   - Footer types: `CoreFooter`, `ClientFooter`, generation payloads

6. **Client Marshal** (`marshal/client.rs`):
   - `Methods` enum: `Error`, `UpdateProperties`, `GetPermissions`, `UpdatePermissions`
   - `Events` enum: `Info`, `Permissions`
   - Client info and permission structures

#### Macros Crate (`macros/`)

1. **`#[derive(Marshallable)]`**: Generates opcode mapping and encode/decode for enum variants
2. **`#[derive(PodStruct)]`**: Generates `Pod` trait impl for structs with named fields
3. **`#[derive(EnumU32)]`**: Generates `TryFrom<u32>` and `Into<u32>` for enums

#### SPA Crate (`spa/`)

1. **POD System** (`pod/mod.rs`, `pod/builder.rs`, `pod/parser.rs`):
   - `Pod` trait: `encode()`, `decode()` with `DecodesTo` associated type
   - `Primitive` trait for basic types
   - `RawPod`, `RawPodOwned` for dynamic POD handling
   - Builder pattern with `push_struct()`, `push_int()`, `push_string()`, etc.
   - Parser pattern with `pop_struct()`, `pop_int()`, `pop_string()`, etc.

---

## Duplication Analysis

### Direct Duplicates

| server/ Type | pipewire/ Equivalent | Duplication Level |
|--------------|----------------------|-------------------|
| `NativeHeader` | `marshal::message::Header` | **Exact duplicate** - same 16-byte wire format |
| `NativePacket` | `marshal::message::Message<T, F>` | Conceptual duplicate |
| `core_method::*` constants | `marshal::core::Methods` enum discriminants | Same opcodes |
| `core_event::*` constants | `marshal::core::Events` enum discriminants | Same opcodes |
| `registry_method::*` | `marshal::registry::Methods` | Same opcodes |
| `registry_event::*` | `marshal::registry::Events` | Same opcodes |
| `spa_data_type::*` | Should be in `spa/` | Missing from spa crate |
| Header encode/decode | `Header::encode()`, `Header::decode()` | Same logic |
| SCM_RIGHTS recv/send | `Connection::read()`, `write_packet_with_fds()` | Same libc calls |

### Re-implemented POD Helpers

The server crate directly uses `pipewire_native_spa` for POD operations, which is good. However:

- `push_string_pair_list()` in `messages.rs` duplicates the pattern in `PairList::encode()`
- `parse_struct()` wraps `Parser::pop_struct()` - could be a shared helper
- `encode_struct_payload()` wraps `Builder::push_struct()` - could be a shared helper

### Message Type Definitions

Server defines its own inbound message enum:

```rust
pub enum InboundMessage {
    CoreHello { version: u32 },
    CoreSync { id: u32, seq: u32 },
    CoreGetRegistry { version: u32, new_id: u32 },
    ClientUpdateProperties,
    RegistryBind { ... },
    RegistryDestroy { id: u32 },
    Unknown { object_id: u32, opcode: u8 },
}
```

pipewire/ has equivalent types:
- `marshal::core::Hello`, `Sync`, `GetRegistry`
- `marshal::client::UpdateProperties`
- `marshal::registry::Bind`, `Destroy`

---

## Reuse Opportunities

### High Priority: Shared Protocol Types

**Create a new shared module** (suggested: `pipewire/src/protocol/types/` or separate `protocol-types` crate):

```rust
// Should be shared between client and server
pub const HEADER_LEN: usize = 16;
pub const MAX_CONTROL_FDS: usize = 16;
pub const MAX_PAYLOAD_SIZE: usize = (1 << 24) - 1;

pub struct Header {
    pub id: u32,
    pub opcode: u8,
    pub size: u32, // 24-bit on wire
    pub seq: u32,
    pub n_fds: u32,
}

// Opcodes as constants (for raw matching) and enums (for typed access)
pub mod core_opcodes {
    pub const HELLO: u8 = 1;
    pub const SYNC: u8 = 2;
    // ...
}

pub mod registry_opcodes {
    pub const BIND: u8 = 1;
    pub const DESTROY: u8 = 2;
}
```

**Impact**: Eliminates `server/src/protocol/frame.rs` duplication, provides canonical source of truth.

### High Priority: Reuse Marshal Types for Server Demarshaling

The existing marshal types are already `Pod`-serializable. Server can reuse them:

```rust
// Instead of custom InboundMessage, reuse marshal types
use pipewire_native::protocol::marshal::core::Methods as CoreMethod;
use pipewire_native::protocol::marshal::registry::Methods as RegistryMethod;

pub enum ServerInbound {
    Core(CoreMethod),
    Client(ClientMethod),
    Registry(RegistryMethod),
    Unknown { object_id: u32, opcode: u8 },
}
```

**Challenge**: Current marshal types use `Marshallable` trait which requires knowing the opcode upfront. Server needs to decode based on object_id first.

**Solution**: Add a `decode_from_header(object_id: u32, opcode: u8, data: &[u8])` method or create server-specific wrappers.

### Medium Priority: Shared POD Utilities

Add to `pipewire-native-spa`:

```rust
// spa/src/pod/helpers.rs (new file)
pub fn encode_struct<T>(build: impl FnOnce(StructBuilder<'_>) -> StructBuilder<'_>) -> io::Result<Vec<u8>> {
    let mut data = vec![0u8; 8192];
    let out = Builder::new(data.as_mut_slice())
        .push_struct(build)
        .build()
        .map_err(pod_error)?;
    Ok(out.to_vec())
}

pub fn parse_struct<T>(payload: &[u8], parse: impl FnOnce(&mut Parser<'_>) -> Result<T, Error>) -> io::Result<T> {
    let mut parser = Parser::new(payload);
    parser.pop_struct(parse).map(|(out, _)| out).map_err(pod_error)
}
```

**Impact**: Simplifies both client and server code, provides consistent error handling.

### Medium Priority: Shared Frame I/O

The SCM_RIGHTS send/receive logic is duplicated:

- `server/src/protocol/frame.rs`: `read_packet_with_fds()`, `write_packet_with_fds()`
- `pipewire/src/protocol/connection.rs`: `read()` (recvmsg path)

**Create**: `pipewire/src/protocol/scm.rs` (or `io.rs`):

```rust
pub fn recv_msg_with_fds(stream: &UnixStream, buf: &mut [u8], max_fds: usize) -> io::Result<(usize, Vec<OwnedFd>)>;
pub fn send_msg_with_fds(stream: &UnixStream, buf: &[u8], fds: &[BorrowedFd<'_>]) -> io::Result<usize>;
```

**Impact**: Single source of truth for SCM_RIGHTS handling, easier security auditing.

### Low Priority: Server-Specific Event Encoders

The server needs to *encode* events that the client *decodes*. Currently:

- `server::encode_core_info_payload()` → client demarshals `Core::Events::Info`
- `server::encode_registry_global_payload()` → client demarshals `Registry::Events::Global`

**Opportunity**: Create symmetric encoder/decoder pairs:

```rust
// In marshal/core.rs or new server-events module
impl Info {
    pub fn encode(&self) -> io::Result<Vec<u8>> { ... }
}

impl Global {
    pub fn encode(&self) -> io::Result<Vec<u8>> { ... }
}
```

This could be derived automatically by enhancing the `PodStruct` macro or adding an `Encodable` trait.

### Future: Client-Node Protocol

The doc mentions needing `ClientNode` protocol support. Upstream has:

- [`module-client-node/protocol-native.c`](https://gitlab.com/pipewire/pipewire/-/blob/master/src/modules/module-client-node/protocol-native.c): ~1200 lines of marshal/demarshal
- Methods: `GetNode`, `Update`, `PortUpdate`, `SetActive`, `AddPort`, `RemovePort`, `PortUseBuffers`, `PortSetIO`, `SetBuffers`
- Events: `Transport`, `SetActivation`, `PortBuffers`, `SetParam`, `Event`, `Command`, `AddMem`, `RemoveMem`, `PeerAdded`, `PeerRemoved`

**Recommendation**: When adding ClientNode support to server/, first add marshal types to `pipewire/src/protocol/marshal/client_node.rs`, then reuse from server/.

---

## Recommended Architecture

### Option A: Protocol Types Crate (Recommended)

Create `pipewire-protocol-types` as a separate crate:

```
pipewire-protocol-types/
├── Cargo.toml
├── src/
│   ├── lib.rs
│   ├── header.rs       # Header struct, encode, decode
│   ├── constants.rs    # CORE_ID, CLIENT_ID, opcodes
│   ├── core.rs         # Core method/event structs
│   ├── registry.rs     # Registry method/event structs
│   ├── client.rs       # Client method/event structs
│   └── client_node.rs  # Future: ClientNode structs
```

Both `pipewire/` and `server/` depend on this. The `Marshallable` trait impls stay in `pipewire/` (client-specific), while raw types move to the shared crate.

**Pros**:
- Clean separation
- Server can use types without pulling in full client machinery
- Future wire protocol implementations can share

**Cons**:
- Another crate to maintain
- Need to carefully split what's wire-format vs client-specific

### Option B: Re-export from pipewire/

Add `pub mod server_types` in `pipewire/src/protocol/`:

```rust
// pipewire/src/protocol/server_types.rs
pub use super::marshal::core::{Hello, Sync, GetRegistry, ...};
pub use super::marshal::registry::{Bind, Destroy, Global, GlobalRemove};
// Server-specific helpers
pub fn encode_core_event(event: &core::Events) -> io::Result<Vec<u8>> { ... }
```

Server depends on `pipewire-native` with feature flag `server-types`.

**Pros**:
- No new crate
- Types stay near their usage

**Cons**:
- Server pulls in more client code
- Feature flag complexity

### Option C: Move Marshal Types to spa/

Move `Marshallable` trait and basic protocol types to `pipewire-native-spa`:

**Pros**:
- SPA already has POD infrastructure
- Logical home for serialization types

**Cons**:
- spa/ is meant to mirror upstream SPA library
- Would diverge from C structure

---

## Action Items

### Immediate (High Impact, Low Effort)

1. **Replace `NativeHeader` with `marshal::message::Header`**
   - Delete `server/src/protocol/frame.rs::NativeHeader`
   - Use `pipewire_native::protocol::marshal::message::Header`
   - Update encode/decode calls

2. **Use existing opcode constants**
   - Replace server's `core_method::*` with values from `marshal::core::Methods` enum
   - Or extract constants to shared location

3. **Add `spa_data_type` to spa crate**
   - Move `messages.rs::spa_data_type` to `pipewire-native-spa/src/param/` or `types.rs`

### Short Term

4. **Create shared SCM_RIGHTS helpers**
   - Extract `recv_msg_with_fds`, `send_msg_with_fds` to `pipewire/src/protocol/io.rs`
   - Update both connection.rs and server frame.rs to use them

5. **Add POD helper functions to spa**
   - `encode_struct_payload()` → `spa::pod::encode_struct()`
   - `parse_struct()` → `spa::pod::parse_struct()`

6. **Reuse marshal types in server decode**
   - Import `marshal::core::*`, `marshal::registry::*` structs
   - Create `ServerInbound` wrapper enum

### Medium Term

7. **Add event encoders to marshal types**
   - `Info::encode()`, `Done::encode()`, `Global::encode()`, etc.
   - Could be derived via macro enhancement

8. **Add ClientNode marshal types**
   - Create `pipewire/src/protocol/marshal/client_node.rs`
   - Reference upstream `module-client-node/protocol-native.c`
   - Server reuses for data-plane testing

### Long Term

9. **Evaluate protocol-types crate**
   - If server and client continue to diverge, extract shared types
   - Keep marshal logic with respective users

---

## Upstream Reference Points

When implementing server protocol support, refer to:

- **Core/Registry**: [`pipewire/pipewire` `src/modules/module-protocol-native/protocol-native.c`](https://gitlab.com/pipewire/pipewire/-/blob/master/src/modules/module-protocol-native/protocol-native.c)
  - Lines 35-290: Core method marshaling
  - Lines 292-450: Core event demarshaling
  - Lines 1200-1400: Registry marshaling

- **Client-Node**: [`pipewire/pipewire` `src/modules/module-client-node/protocol-native.c`](https://gitlab.com/pipewire/pipewire/-/blob/master/src/modules/module-client-node/protocol-native.c)
  - Full ClientNode protocol marshaling (~1200 lines)

- **Server-Side Implementation**: [`pipewire/pipewire` `src/pipewire/impl-core.c`](https://gitlab.com/pipewire/pipewire/-/blob/master/src/pipewire/impl-core.c)
  - `registry_bind()`: Server-side bind handling (lines 26-80)
  - `registry_destroy()`: Server-side destroy handling (lines 82-120)

- **Protocol Internals**: [`pipewire/pipewire` `doc/dox/internals/protocol.dox`](https://gitlab.com/pipewire/pipewire/-/blob/master/doc/dox/internals/protocol.dox)
  - Wire format documentation
  - Message sequence diagrams

---

## Risks

1. **Breaking existing client code**: Any changes to marshal types must maintain backward compatibility
2. **Cyclic dependencies**: Careful crate structure needed to avoid cycles
3. **Over-abstraction**: Don't generalize prematurely; wait for patterns to emerge
4. **Testing burden**: Shared code needs tests accessible from both client and server contexts

---

## Conclusion

The `server/` crate is well-structured for its test-server purpose but duplicates protocol infrastructure that should be shared. The highest-value immediate action is replacing `NativeHeader` with the existing `Header` type and using marshal structs instead of custom `InboundMessage` parsing. This reduces maintenance burden and ensures protocol fidelity between test server and real clients.

For future data-plane work, establishing a pattern of "marshal types in pipewire/, reuse in server/" will scale better than parallel implementations.
