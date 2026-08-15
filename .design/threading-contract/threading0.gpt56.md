---
type: ArchitectureDesign
title: PipeWire native Rust threading contract
description: Implementation-ready replacement for blanket Send and Sync assertions with loop-local ownership, explicit cross-thread handles, and audited SPA and node runtime invariants.
resource: /.design/threading-contract/threading0.gpt56.md
tags: [pipewire, rust, threading, send, sync, spa, event-loop, ffi, node-runtime, safety]
status: draft
generated: { by: agent:gpt56, at: 2026-08-15T06:10:33-04:00 }
sources:
  - id: architecture-review
    resource: /.design/architecture-review/review0.oc.md
    title: PipeWire native Rust architecture review
    author: agent:opencode
  - id: architecture-rekickoff
    resource: /.design/architecture-review/review-rekick0.oc.md
    title: PipeWire native Rust architecture re-kickoff
    author: agent:opencode
  - id: repository
    resource: https://gitlab.freedesktop.org/pipewire/pipewire-native-rs
    title: pipewire-native-rs source tree
    author: project:pipewire-native-rs
  - id: upstream-spa-loop
    resource: https://gitlab.freedesktop.org/pipewire/pipewire/-/blob/master/spa/include/spa/support/loop.h
    title: SPA loop interface contract
    author: project:pipewire
  - id: upstream-thread-loop
    resource: https://gitlab.freedesktop.org/pipewire/pipewire/-/blob/master/src/pipewire/thread-loop.h
    title: PipeWire thread loop contract
    author: project:pipewire
---

# PipeWire Native Rust Threading Contract

## Decision

The control-plane object graph is **loop-local**. `MainLoop`, `Context`, `Core`,
all proxies, protocol dispatch state, listeners, sources, and SPA loop-local
interfaces are neither `Send` nor `Sync`. They are created, used, dispatched,
and dropped on one loop thread.

Cross-thread interaction uses separate, capability-limited handles:

- `LoopWakeHandle` requests wake or shutdown and is `Send + Sync`.
- `LoopCommandHandle` submits bounded, owned commands and is `Send + Sync`.
- typed `CoreHandle` and `ProxyHandle<K>` values encode object identity and
  submit commands; they never expose the underlying proxy or callback tables.
- replies and event snapshots are owned data. No callback borrow, proxy, SPA
  source, or mapped mutable slice crosses a thread boundary.

SPA interfaces are split by their upstream contract rather than marked wholly
thread-safe. Source registration and loop control stay local. PipeWire command
handles wake a loop-local eventfd source and therefore retain no SPA pointer;
only a future direct SPA `invoke` API may earn a narrowly audited cross-thread
wrapper.

The node process runtime is a separate owner domain. `OwnedFd`, transport state,
and mappings move once from the control loop to the process runtime over a
bounded channel. The process runtime does not share control-plane proxies, and
the control loop does not share mutable process mappings.

This replaces both possible but weaker models:

- locks around freely shared proxies, which require every caller to remember an
  external lock and cannot express source/callback affinity;
- local unsafe `Send` implementations on each proxy, which would merely move the
  blanket assertion without proving the underlying C and callback contracts.

## Why the current model is unsound

The `refcounted!` macro emits unconditional unsafe implementations for both the
strong and weak wrappers ([`/pipewire/src/refcounted.rs#L81-L100`](/pipewire/src/refcounted.rs#L81-L100)).
The comment delegates safety to implementors, but macro invocations have no
place to state, check, or even select an invariant. The assertion bypasses all
inner-field auto-traits.

That bypass matters immediately:

- protocol method tables contain `Box<dyn FnMut(...)>` without `Send`, for
  example core methods ([`/pipewire/src/core.rs#L348-L359`](/pipewire/src/core.rs#L348-L359)),
  node methods ([`/pipewire/src/proxy/node.rs#L31-L60`](/pipewire/src/proxy/node.rs#L31-L60)),
  and registry methods ([`/pipewire/src/proxy/registry.rs#L29-L33`](/pipewire/src/proxy/registry.rs#L29-L33));
- connection callbacks are also non-`Send`
  ([`/pipewire/src/protocol/connection.rs#L52-L58`](/pipewire/src/protocol/connection.rs#L52-L58));
- SPA loop wrappers erase their implementation as `Pin<Box<dyn Any>>` and then
  manually assert `Send + Sync`
  ([`/spa/src/interface/loop.rs#L19-L36`](/spa/src/interface/loop.rs#L19-L36),
  [`/spa/src/interface/loop.rs#L77-L96`](/spa/src/interface/loop.rs#L77-L96),
  [`/spa/src/interface/loop.rs#L182-L230`](/spa/src/interface/loop.rs#L182-L230));
- source wrappers retain callbacks and an erased raw C source pointer
  ([`/spa/src/interface/loop.rs#L162-L180`](/spa/src/interface/loop.rs#L162-L180),
  [`/spa/src/support/ffi/loop/utils.rs#L132-L178`](/spa/src/support/ffi/loop/utils.rs#L132-L178));
- plugin, handle, support, and raw C interface wrappers each add further manual
  assertions around raw pointers and dynamically loaded code
  ([`/spa/src/support/ffi/plugin.rs#L24-L36`](/spa/src/support/ffi/plugin.rs#L24-L36),
  [`/spa/src/support/ffi/plugin.rs#L198-L217`](/spa/src/support/ffi/plugin.rs#L198-L217),
  [`/spa/src/interface/mod.rs#L38-L46`](/spa/src/interface/mod.rs#L38-L46),
  [`/pipewire/src/support.rs#L219-L220`](/pipewire/src/support.rs#L219-L220)).

The upstream requirements are narrower than the Rust assertions:

- SPA `add_source`, `update_source`, and `remove_source` must run on the loop's
  own thread; `invoke` may run from multiple threads
  ([`spa/support/loop.h#L79-L139`](https://gitlab.freedesktop.org/pipewire/pipewire/-/blob/master/spa/include/spa/support/loop.h#L79-139));
- `enter` and `leave` establish the iterate thread, and `check` tests that
  identity
  ([`spa/support/loop.h#L241-L312`](https://gitlab.freedesktop.org/pipewire/pipewire/-/blob/master/spa/include/spa/support/loop.h#L241-312));
- source destruction is permitted only while stopped or in loop context
  ([`spa/support/loop.h#L492-L495`](https://gitlab.freedesktop.org/pipewire/pipewire/-/blob/master/spa/include/spa/support/loop.h#L492-495));
- PipeWire objects do not permit concurrent access; every call involving an
  object associated with a thread loop requires that loop's lock, and callbacks
  run with the lock held
  ([`thread-loop.h#L44-L76`](https://gitlab.freedesktop.org/pipewire/pipewire/-/blob/master/src/pipewire/thread-loop.h#L44-76)).

A Rust mutex around a raw pointer does not broaden the C API's thread contract.
In particular, the `RwLock` around `CLoopImpl` serializes its Rust map but does
not make C `add_source` legal on another thread
([`/spa/src/support/ffi/loop/mod.rs#L59-L85`](/spa/src/support/ffi/loop/mod.rs#L59-L85),
[`/spa/src/support/ffi/loop/mod.rs#L119-L196`](/spa/src/support/ffi/loop/mod.rs#L119-L196)).

## Actual cross-thread use

The repository has only four production/test families that spawn tasks or
threads, and they do not justify sharing the whole control graph.

| Call site | Value crossing | Required capability | Finding |
|---|---|---|---|
| [`/pipewire/src/thread_loop.rs#L73-L104`](/pipewire/src/thread_loop.rs#L73-L104) | entire `ThreadLoop`, transitively `MainLoop` | move a loop owner into a newly created thread | Must instead construct the loop-local session in its final thread; a thread cannot soundly move arbitrary SPA handle state unless the plugin contract permits it. |
| [`/pipewire/tests/main_loop.rs#L97-L105`](/pipewire/tests/main_loop.rs#L97-L105) | cloned `MainLoop` | quit only | Replace with `LoopWakeHandle`; this test is evidence for a narrow handle, not `MainLoop: Send`. |
| [`/tools/browse/pw.rs#L163-L235`](/tools/browse/pw.rs#L163-L235), [`/tools/browse/main.rs#L608-L644`](/tools/browse/main.rs#L608-L644) | `Arc<State>` containing loop, context, core, registry, and all proxies | snapshots, create/destroy/link commands, shutdown | `State` and each detail type manually assert thread safety ([`/tools/browse/pw.rs#L64-L160`](/tools/browse/pw.rs#L64-L160)). UI calls `Core::create_object` through `link_nodes`/`link_ports` without `ThreadLoop::lock` ([`/tools/browse/pw.rs#L238-L281`](/tools/browse/pw.rs#L238-L281)); this violates upstream's thread-loop rule. Replace shared proxy-bearing state with owned snapshots and command handles. |
| [`/node/src/runtime/mod.rs#L91-L96`](/node/src/runtime/mod.rs#L91-L96) | `NodeRuntime` into `tokio::spawn` | single-owner process runtime plus shutdown sender | This is a separate data-plane domain. Keep the move, but do not share its mapping or import control proxies into the task. |

The scripted server thread ([`/server/src/testkit/mod.rs#L21-L28`](/server/src/testkit/mod.rs#L21-L28))
owns its independent socket/runtime state and is not affected. SPA thread support
deliberately transfers a C callback argument and return pointer
([`/spa/src/support/ffi/thread.rs#L89-L121`](/spa/src/support/ffi/thread.rs#L89-L121));
that is an FFI ownership proof local to thread creation, not precedent for making
all SPA pointers shareable.

The real-daemon test's `Objects` assertion
([`/pipewire/tests/lib.rs#L55-L84`](/pipewire/tests/lib.rs#L55-L84)) and the browser's
detail assertions are downstream symptoms. They disappear when snapshots do not
contain proxies.

## Complete `refcounted!` inventory

There are 17 invocations. Removing the macro assertions today would reveal the
following field-derived result. “Could auto-derive” does not mean “should be
publicly cross-thread”; event-only proxies must still carry loop affinity so all
proxies have one predictable contract.

| Generated type | Inner fields that determine auto-traits | Result without blanket assertions | Target |
|---|---|---|---|
| `MainLoop` | `LoopSupport` with C `Handle`, `LoopImpl`, `LoopUtilsImpl`, `LoopControlImpl`; hooks ([`/pipewire/src/main_loop.rs#L28-L35`](/pipewire/src/main_loop.rs#L28-L35), [`/pipewire/src/main_loop.rs#L64-L82`](/pipewire/src/main_loop.rs#L64-L82)) | blocked by erased SPA/plugin internals | local owner; separate wake/invoker handle |
| `ThreadLoop` | join handle, `MainLoop`, mutex ([`/pipewire/src/thread_loop.rs#L18-L25`](/pipewire/src/thread_loop.rs#L18-L25)) | transitively blocked by `MainLoop` | `ThreadLoopHandle: Send + Sync`; no shared owner object |
| `Context` | `MainLoop`, `Protocol`, pinned SPA system/loop interfaces ([`/pipewire/src/context.rs#L26-L42`](/pipewire/src/context.rs#L26-L42)) | transitively blocked | loop-local |
| `Protocol` | `WeakContext` ([`/pipewire/src/protocol/mod.rs#L23-L29`](/pipewire/src/protocol/mod.rs#L23-L29)) | transitively blocked | loop-local |
| protocol `Client` | weak core, stream, `Connection`, loop `Source`, connection hook ([`/pipewire/src/protocol/client.rs#L38-L49`](/pipewire/src/protocol/client.rs#L38-L49)) | blocked by source and callbacks | loop-local |
| `Connection` | stream, buffers/FD queues, non-`Send` `ConnectionEvents` hooks ([`/pipewire/src/protocol/connection.rs#L33-L58`](/pipewire/src/protocol/connection.rs#L33-L58)) | blocked by callbacks; remaining OS/value state is movable | loop-local session; frame transport may separately earn `Send` if moved, never concurrently shared |
| `Core` | proxy, weak context, protocol client, object arena, non-`Send` method table, hooks ([`/pipewire/src/core.rs#L43-L55`](/pipewire/src/core.rs#L43-L55), [`/pipewire/src/core.rs#L348-L379`](/pipewire/src/core.rs#L348-L379)) | blocked by methods and descendants | loop-local; `CoreHandle` for commands |
| proxy `Client` | `Proxy`, non-`Send` methods, `Send` event callbacks ([`/pipewire/src/proxy/client.rs#L19-L35`](/pipewire/src/proxy/client.rs#L19-L35)) | blocked by methods | loop-local; typed handle by object ID if needed |
| `Device` | `Proxy`, non-`Send` methods/builders, `Send` event callbacks ([`/pipewire/src/proxy/device.rs#L21-L53`](/pipewire/src/proxy/device.rs#L21-L53)) | blocked by methods | loop-local |
| `Factory` | `Proxy`, `Send` event callbacks ([`/pipewire/src/proxy/factory.rs#L18-L24`](/pipewire/src/proxy/factory.rs#L18-L24), [`/pipewire/src/proxy/factory.rs#L52-L58`](/pipewire/src/proxy/factory.rs#L52-L58)) | likely auto `Send + Sync` | add affinity marker; loop-local |
| `Link` | `Proxy`, `Send` event callbacks ([`/pipewire/src/proxy/link.rs#L19-L25`](/pipewire/src/proxy/link.rs#L19-L25), [`/pipewire/src/proxy/link.rs#L81-L87`](/pipewire/src/proxy/link.rs#L81-L87)) | likely auto `Send + Sync` | add affinity marker; loop-local |
| `Metadata` | `Proxy`, non-`Send` methods, `Send` callbacks ([`/pipewire/src/proxy/metadata.rs#L16-L30`](/pipewire/src/proxy/metadata.rs#L16-L30)) | blocked by methods | loop-local |
| `Module` | `Proxy`, `Send` event callbacks ([`/pipewire/src/proxy/module.rs#L18-L24`](/pipewire/src/proxy/module.rs#L18-L24), [`/pipewire/src/proxy/module.rs#L52-L58`](/pipewire/src/proxy/module.rs#L52-L58)) | likely auto `Send + Sync` | add affinity marker; loop-local |
| `Node` | `Proxy`, non-`Send` methods/builders, `Send` callbacks ([`/pipewire/src/proxy/node.rs#L22-L60`](/pipewire/src/proxy/node.rs#L22-L60)) | blocked by methods | loop-local; not the node process handle |
| `Port` | `Proxy`, non-`Send` methods, `Send` callbacks ([`/pipewire/src/proxy/port.rs#L22-L45`](/pipewire/src/proxy/port.rs#L22-L45)) | blocked by methods | loop-local |
| `Profiler` | `Proxy`, `Send` event callbacks ([`/pipewire/src/proxy/profiler.rs#L21-L27`](/pipewire/src/proxy/profiler.rs#L21-L27), [`/pipewire/src/proxy/profiler.rs#L168-L174`](/pipewire/src/proxy/profiler.rs#L168-L174)) | likely auto `Send + Sync` | add affinity marker; loop-local |
| `Registry` | `Proxy`, owning `Core`, non-`Send` methods, `Send` callbacks ([`/pipewire/src/proxy/registry.rs#L18-L33`](/pipewire/src/proxy/registry.rs#L18-L33)) | blocked transitively and directly | loop-local |

Every generated `Weak*` has the same target trait behavior as its strong type.
`Weak<T>` is not a loophole: upgrading on another thread would drop or use the
same loop-local inner there.

`HasProxy: Any + Send + Sync`
([`/pipewire/src/proxy/mod.rs#L131-L146`](/pipewire/src/proxy/mod.rs#L131-L146))
currently turns the unsafe wrapper assertion into an object-arena requirement.
The trait must become `HasProxy: Any`; the loop-local arena needs no concurrency
bound. Cross-thread handles use a separate trait over command DTOs, not
`dyn HasProxy`.

## Proposed type model

### Affinity marker

Use one explicit marker in every loop-local root and proxy, even where fields
would accidentally auto-derive today:

```rust
#[derive(Clone, Default)]
struct LoopLocal(std::marker::PhantomData<std::rc::Rc<()>>);
```

`Rc<()>` makes the containing type `!Send + !Sync` on stable Rust. This is an
intentional semantic assertion, not a workaround for a field. The marker belongs
in `Proxy` and loop/session roots so future callback or lock changes cannot
silently make public control types cross-thread.

The `refcounted!` macro should only generate `Arc`/`Weak`, cloning, and
`Refcounted`. It must generate no unsafe auto-trait implementations. `Arc` is
still useful for local ownership cycles and weak references; it does not imply
that the value is shareable between threads.

### Local session

```rust
pub struct LoopSession {
    main_loop: MainLoop,
    context: Context,
    core: Core,
    commands: CommandReceiver,
    _local: LoopLocal,
}

impl LoopSession {
    pub fn core(&self) -> &Core;
    pub fn run(self) -> Result<(), LoopError>;
}
```

For a caller-driven main loop, the application constructs and uses this session
on its current thread. For a thread loop, `ThreadLoopBuilder::spawn` creates the
SPA loop and the complete session **inside** the spawned thread, runs a setup
closure there, then returns handles to the caller. It does not construct plugin
handles on one thread and move them to another.

```rust
pub struct ThreadLoopBuilder { /* properties and queue limits */ }

impl ThreadLoopBuilder {
    pub fn spawn<F>(self, setup: F) -> Result<ThreadLoopHandle, StartError>
    where
        F: FnOnce(&mut LoopSession) -> Result<(), StartError> + Send + 'static;
}
```

The setup closure itself crosses the boundary, so its captures must be `Send`.
Once executing on the loop thread it may create and register non-`Send`
callbacks that capture loop-local `Rc` state.

### Cross-thread command handles

```rust
#[derive(Clone)]
pub struct LoopCommandHandle {
    tx: BoundedSender<LoopCommand>,
    wake: LoopWakeHandle,
}

#[derive(Clone)]
pub struct CoreHandle {
    loop_: LoopCommandHandle,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct ProxyKey<K> {
    id: Id,
    generation: u64,
    _kind: PhantomData<fn() -> K>,
}

#[derive(Clone)]
pub struct ProxyHandle<K> {
    key: ProxyKey<K>,
    loop_: LoopCommandHandle,
}
```

Commands contain owned input and an optional one-shot reply sender. The queue is
bounded by command count and owned byte/FD cost. `try_submit` reports `Full` or
`Closed`; async submission may wait for capacity outside realtime threads.
Blocking submission is not exposed because upstream warns that blocking invoke
can deadlock while a loop lock is held and must not run on a realtime thread.
`LoopWakeHandle` owns only a duplicated eventfd writer; it has no C or Rust loop
pointer. A source registered during local setup drains the command queue.

Object handles include a generation, not only an ID, because PipeWire IDs can be
reused ([`/pipewire/src/proxy/mod.rs#L55-L60`](/pipewire/src/proxy/mod.rs#L55-L60)).
The loop resolves the key immediately before execution and returns `StaleObject`
instead of invoking a replacement object.

The initial public command set should be concrete, not an unbounded
`Box<dyn FnOnce(&mut LoopSession)>` escape hatch:

```rust
enum LoopCommand {
    CoreSync { reply: Reply<Result<u32, Error>> },
    CreateObject { factory: String, type_: String, version: u32,
                   props: Properties, reply: Reply<Result<AnyProxyKey, Error>> },
    Destroy { key: AnyProxyKey, reply: Reply<Result<(), Error>> },
    Bind { registry: ProxyKey<RegistryKind>, global: Id, type_: String,
           version: u32, reply: Reply<Result<AnyProxyKey, Error>> },
    Node(NodeCommand),
    Metadata(MetadataCommand),
    Shutdown,
}
```

This preserves the domain API and makes each allowed cross-thread operation
reviewable. Internal tests may use a crate-private closure command, but it must
not become a public backdoor for retaining local references.

### Events and browser state

Listener callbacks remain loop-local and lose unnecessary `+ Send` bounds.
They translate borrowed event data into owned snapshots or application-specific
messages before sending across a channel:

```rust
pub struct NodeSnapshot {
    pub key: ProxyKey<NodeKind>,
    pub max_input_ports: u32,
    pub max_output_ports: u32,
    pub state: NodeState,
    pub props: Properties,
}
```

`pw-browse` should have:

- loop thread: `LoopSession`, proxies, listeners, and mutable object model;
- UI thread: owned render snapshots plus `CoreHandle`/typed keys;
- loop-to-UI channel: snapshot upsert/remove and terminal status;
- UI-to-loop channel: typed create/link/destroy requests.

The browser's `ClientDetails`, `NodeDetails`, and peers then contain keys and
owned values, not proxy objects, eliminating all manual `Send`/`Sync` blocks.

### SPA interface split

The current monolithic interfaces conflate capabilities. Replace them with
local facades. A direct invoker is optional and separate:

```rust
pub struct LoopLocalInterface { /* add/update/remove source; !Send + !Sync */ }
pub struct LoopControlLocal { /* enter/leave/iterate/check/hooks; !Send + !Sync */ }
pub struct LoopUtilsLocal { /* source lifecycle; !Send + !Sync */ }

#[derive(Clone)]
pub struct LoopInvoker { inner: Arc<LoopInvokerInner> } // optional; Send + Sync
```

The PipeWire control API does not require `LoopInvoker`: its eventfd wake handle
is simpler and has field-derived auto-traits. If SPA exposes `LoopInvoker`, its
inner is the only place that may contain a local unsafe `Send` and `Sync`
implementation around a C `spa_loop` pointer. It exposes only nonblocking
`invoke` with `Send + 'static` owned callback state. Its owner token retains the
plugin and C handle until all queued invocations are completed or cancelled.

Do not expose `CLoop`, `CSource`, `LoopImpl`, `LoopControlImpl`, or
`LoopUtilsImpl` as cross-thread types. Remove their current assertions
([`/spa/src/interface/ffi.rs#L85-L108`](/spa/src/interface/ffi.rs#L85-L108)).
`CSource` contains callback data and must be created, mutated, destroyed, and
dropped on the loop thread.

`LoopControl` synchronization methods require capability-specific treatment:

- `get_time` may be independently wrapped for any thread;
- `lock`/`unlock` form a pair and are not a general safe Rust guard unless the
  guard also owns the only route to associated objects;
- `wait`, `signal`, and `accept` require the loop lock under upstream's contract
  ([`spa/support/loop.h#L314-L376`](https://gitlab.freedesktop.org/pipewire/pipewire/-/blob/master/spa/include/spa/support/loop.h#L314-376));
- the command model does not need to expose these publicly. Keep them internal
  to thread-loop implementation.

Plugin and support traits must stop promising more than implementations prove:

- remove `Box<dyn Handle + Send + Sync>` from factory initialization and support
  loading ([`/spa/src/interface/plugin.rs#L84-L108`](/spa/src/interface/plugin.rs#L84-L108),
  [`/pipewire/src/support.rs#L122-L128`](/pipewire/src/support.rs#L122-L128));
- make generic `Interface`, `Handle`, `HandleFactory`, and `Support` local;
- if CPU or syscall interfaces need cross-thread use, define a separate sealed
  `ThreadSafeInterface` implemented only after each concrete C factory contract
  is verified; `Pin<Box<dyn Any>>` plus a function table is not such proof;
- retain the existing plugin -> factory -> handle -> interface ownership chain,
  but drop it on its owner thread
  ([`/spa/src/support/ffi/plugin.rs#L101-L155`](/spa/src/support/ffi/plugin.rs#L101-L155),
  [`/spa/src/support/ffi/plugin.rs#L198-L217`](/spa/src/support/ffi/plugin.rs#L198-L217)).

### Node runtime boundary

The node runtime already moves one `BoundTransport` and one `Send` callback into
a Tokio task ([`/node/src/runtime/mod.rs#L21-L38`](/node/src/runtime/mod.rs#L21-L38)).
Keep single ownership, but make the boundary explicit:

```rust
pub struct NodeProcessOwner { transport: BoundTransport, process: ProcessCallback }
#[derive(Clone)]
pub struct NodeProcessHandle { commands: BoundedSender<ProcessCommand> }
```

The control loop transfers `AddMemory`, `RemoveMemory`, and transport-generation
commands as owned values to a node-session owner. It does not put `Core`, `Node`,
or `ControlPlaneState` in an `Arc<Mutex<_>>` shared with the process task.

`MappedRegion` should be `Send` only and owned by one process owner. Remove its
`Sync` implementation ([`/node/src/shm/memfd.rs#L56-L65`](/node/src/shm/memfd.rs#L56-L65)).
This matches its mutable slice API and avoids claiming unsynchronized shared
access to externally mutable memory. If a future read-only mapping needs `Sync`,
represent it as a distinct type with an explicit foreign-write/atomicity model.

Tokio tasks may migrate between worker threads. That is acceptable for a
`NodeProcessOwner: Send` if eventfd and mappings are not thread-affine. A
real-time dedicated-thread adapter can own the same type. Neither adapter makes
the owner `Sync`.

Cycle borrows remain callback-scoped as required by the re-kickoff
([`/.design/architecture-review/review-rekick0.oc.md#L216-L220`](/.design/architecture-review/review-rekick0.oc.md#L216-L220)).
Transport replacement and memory removal are serialized as process-owner
commands between callbacks; they cannot invalidate a live `ProcessCycle<'_>`.

## Explicit unsafe invariants

After migration, blanket unsafe auto-trait implementations are forbidden. Each
remaining implementation must sit beside its type with a `// SAFETY:` argument
covering every item below.

### Optional `LoopInvokerInner: Send + Sync`

If this optional direct SPA handle is implemented:

1. The pointer refers to a live SPA loop whose concrete implementation supports
   `invoke` from any thread and multiple threads concurrently.
2. The wrapper exposes no source registration, source mutation, iteration,
   enter/leave, callback table, or arbitrary vtable access.
3. Plugin/library, C handle, and loop storage outlive every clone and every
   queued callback.
4. Callback state is `Send + 'static`, has one owner, and is reclaimed exactly
   once on completion, rejection, cancellation, or shutdown.
5. Nonblocking invoke is used across threads. No caller can request a blocking
   invoke while holding loop state or from a realtime path.
6. Shutdown closes submission before destroying the C loop, drains/cancels all
   accepted work, then drops the loop on its owner thread.

### Thread-start pointer transfer

The small wrapper in SPA thread FFI may remain `Send`, not `Sync`, only if:

1. C transfers the argument exclusively to exactly one new thread;
2. the source thread does not dereference it after successful creation;
3. the callback return pointer remains owned by the joined thread until exactly
   one successful join returns it;
4. create failure and join failure each have explicit reclamation behavior.

The current `SendablePtr` implements both traits
([`/spa/src/support/ffi/thread.rs#L89-L95`](/spa/src/support/ffi/thread.rs#L89-L95));
`Sync` is unnecessary and should be removed.

### `MappedRegion: Send`

1. The mapping remains valid independent of the thread on which it was created.
2. Moving transfers sole Rust ownership of mutable access; the old thread keeps
   no slice or pointer.
3. `munmap` may run on the destination thread.
4. file size/seal and external-writer constraints are documented separately;
   `Send` does not claim that non-atomic concurrent foreign writes are safe to
   interpret as Rust typed data.
5. No `Sync` implementation exists for the writable mapping.

### Dynamic plugin objects

No generic plugin pointer gets `Send` or `Sync`. A concrete exception requires:

1. upstream documentation for every invoked vtable method's thread behavior;
2. proof that library unload and handle clear cannot race a call;
3. proof that callbacks and user-data pointers obey the same thread contract;
4. owner-thread destruction when required;
5. a capability wrapper exposing only the proven methods.

### FFI callback rule

Every C trampoline catches no Rust aliasing exemption. It must be called only on
the documented thread, must not overlap another mutable call to the same
`FnMut`, and must not unwind across C. The current source trampolines directly
recover `&mut LoopUtilsSource`
([`/spa/src/support/ffi/loop/utils.rs#L120-L129`](/spa/src/support/ffi/loop/utils.rs#L120-L129),
[`/spa/src/support/ffi/loop/utils.rs#L236-L246`](/spa/src/support/ffi/loop/utils.rs#L236-L246));
loop ownership is what makes that unique mutable access valid.

## Migration sequence

Each item is one compile-safe commit. Existing unsafe assertions may remain
temporarily only while they are fenced by the migration and removed at the final
gate; do not add new proxy-specific assertions.

1. **Add auto-trait test infrastructure.** Add `trybuild` UI tests and private
   positive assertion helpers. Capture the desired contract for new handle and
   local marker test fixtures without changing production traits.
2. **Introduce loop-local and command primitives.** Add `LoopLocal`, bounded
   command/reply types, object keys with generations, and `LoopWakeHandle`.
   Test queue full/closed behavior and exact reply ownership. No existing API
   changes yet.
3. **Split SPA loop capabilities.** Introduce local loop/control/utils facades.
   Move `MainLoop::quit` onto a loop-local eventfd command source and return a
   writer-only `LoopWakeHandle`. Keep old facades as internal adapters during
   this commit; defer direct `LoopInvoker` unless a concrete consumer needs it.
4. **Construct thread-loop sessions in place.** Add `ThreadLoopBuilder::spawn`
   that creates plugin handles, main loop, context, and setup state inside its
   worker. Return `ThreadLoopHandle`; implement shutdown/join without moving the
   owner graph from the caller thread.
5. **Add typed control-plane handles.** Add concrete `CoreHandle` and proxy
   command DTOs for operations currently used outside callbacks: sync, bind,
   create, destroy, metadata changes, and node parameter commands. Resolve
   generation-tagged keys on the loop thread.
6. **Migrate `pw-browse`.** Replace proxy-bearing shared detail structs with
   snapshots and keys; send UI actions through handles. Delete all unsafe
   assertions in [`/tools/browse/pw.rs`](/tools/browse/pw.rs). This commit is the
   executable proof that the handle surface is sufficient.
7. **Migrate examples and tests.** Replace cross-thread `MainLoop::quit` clones
   with `LoopWakeHandle`, move threaded setup into `ThreadLoopBuilder`, and
   remove the test `Objects: Send` assertion. Preserve direct current-thread
   `MainLoop` tests as local-session tests.
8. **Make proxy storage local.** Remove `Send + Sync` from `HasProxy`, add
   `LoopLocal` to `Proxy`, and remove now-unnecessary `+ Send` from local event
   callbacks. The object arena remains `Box<dyn HasProxy>` on the loop thread.
9. **Remove blanket macro assertions.** Delete all four unsafe implementations
   from `refcounted!`. Strong and weak types now follow their fields, with
   `LoopLocal` enforcing the public contract even for event-only proxies. Run
   all compile-fail tests in this commit.
10. **Remove broad SPA/plugin assertions.** Delete `Send`/`Sync` from generic
    loop, C source, plugin, handle, and support types. Keep only audited
    an optional audited `LoopInvokerInner` and thread-transfer wrappers with
    adjacent safety text.
    Tighten `dyn Any` bounds or keep the erased inner local.
11. **Narrow node mapping traits.** Remove `MappedRegion: Sync`, assert
    `NodeProcessOwner: Send` and `!Sync`, and serialize replacement/removal
    commands between cycles. Exercise both Tokio migration and dedicated-thread
    adapters.
12. **Delete legacy thread-lock surface.** Remove or crate-private the old
    `ThreadLoop::main_loop` and unrestricted lock/guard API once no consumer can
    obtain loop-local objects on another thread. Remove the
    `arc_with_non_send_sync` rationale that describes caller synchronization
    ([`/pipewire/Cargo.toml#L26-L31`](/pipewire/Cargo.toml#L26-L31)); the new
    contract is compiler-enforced.

Commits 3, 4, 9, and 10 require focused review because they touch unsafe FFI or
drop order. Do not combine them. The browser migration precedes blanket removal
so the largest actual cross-thread consumer drives handle completeness rather
than prompting another unsafe escape hatch.

## Test plan

### Compile-fail contract

Use `trybuild` files under `pipewire/tests/ui/threading/`. Each must fail for the
intended auto-trait reason, with normalized stderr:

- moving `MainLoop`, `LoopSession`, `Context`, `Core`, `Registry`, and every one
  of the ten concrete proxies into `std::thread::spawn`;
- sharing each through `Arc` (proves `Arc` does not bypass `!Sync`);
- moving `Source`, `LoopLocalInterface`, `LoopControlLocal`, `LoopUtilsLocal`,
  generic `Plugin`, `Handle`, or C-backed interface to another thread;
- upgrading a `WeakCore` or weak proxy in another thread;
- retaining borrowed event data in a command or `'static` task;
- retaining `ProcessCycle` activation/buffer slices after callback return;
- sharing writable `MappedRegion` by `Arc`;
- submitting non-`Send` command input or callback state.

Positive compile assertions verify `Send + Sync + Clone + 'static` for
`LoopWakeHandle`, `LoopCommandHandle`, `ThreadLoopHandle`, `CoreHandle`, and
typed proxy handles; `NodeProcessOwner: Send`; and `NodeProcessHandle: Send +
Sync`. Use a helper such as `fn assert_send_sync<T: Send + Sync>() {}` rather
than unsafe implementations.

### Loop concurrency tests

1. Record the loop thread ID during setup and in every listener; assert every
   control callback, proxy operation, source mutation, and owner drop observes
   that ID.
2. Submit commands concurrently from many producer threads; assert each reply
   corresponds to exactly one command and per-producer order is preserved.
3. Fill the bounded queue; assert `Full` without allocation/FD leakage, then
   recover after the loop drains it.
4. Race shutdown with submission; every accepted command completes or receives
   explicit cancellation, every later command gets `Closed`, and join returns.
5. Destroy an object, reuse its numeric ID in a test arena, and prove the old
   generation key returns `StaleObject`.
6. Request quit from a foreign thread while the loop blocks in iterate; prove
   bounded wake and owner-thread destruction.
7. Trigger a command from inside a callback; define and test nonblocking
   reentrancy ordering rather than recursively dispatching it inline.
8. Panic in a Rust callback behind a C trampoline; verify the selected policy
   catches before the C boundary and transitions the session to failed shutdown.

### SPA C-interface tests

Extend the instrumented C test plugin used by [`/spa/tests/ffi.rs`](/spa/tests/ffi.rs):

- save the thread ID at `enter` and reject/record `add`, `update`, `remove`,
  utils mutation, or destroy from another thread;
- if direct `LoopInvoker` is implemented, invoke concurrently from at least four
  foreign threads and assert callbacks execute on the loop thread without
  overlapping the same `FnMut`;
- if direct `LoopInvoker` is implemented, hold clones during shutdown and verify
  submission closes before handle clear/library release;
- instrument clear, source destruction, and library sentinel order;
- run under AddressSanitizer/ThreadSanitizer where the C plugin permits it.

The pure-Rust loop implementation in
[`/spa/src/support/loop.rs`](/spa/src/support/loop.rs) gets the same affinity
tests. Its `Pin<Box<dyn Any>>` mutation currently uses unchecked downcasts
([`/spa/src/support/loop.rs#L49-L100`](/spa/src/support/loop.rs#L49-L100)); the
test must prove all accesses remain on the entered thread.

### Node concurrency tests

1. Run a `NodeProcessOwner` on a multi-thread Tokio runtime and force yields
   between waits; movement is allowed, concurrent calls are not.
2. Run the same owner on a dedicated thread and compare state transitions.
3. Race shutdown, trigger, transport replacement, and RemoveMem. Replacement or
   removal applies only between callbacks, and every accepted trigger receives
   one completion or an explicit terminal error.
4. Attempt concurrent process callbacks with a stress peer; assert maximum
   callback concurrency is one.
5. Move `MappedRegion` once to a worker, mutate it, return only an owned result,
   and drop/unmap on the worker. Pair this with the compile-fail `Arc` test.
6. Use descriptor counts before and after failed transfer, full queue,
   cancellation, panic, and shutdown.

Loom is useful only for a small pure-Rust model of command acceptance,
cancellation, and join state. It cannot validate C loop vtables, OS eventfd, or
plugin thread affinity; use the instrumented integration tests for those.

## Acceptance criteria

- `rg 'unsafe impl (Send|Sync)' pipewire spa tools node` finds only narrowly
  documented capability wrappers and `MappedRegion: Send`; no proxy, owner,
  plugin aggregate, or writable mapping is manually `Sync`.
- `refcounted!` emits no auto-trait implementation.
- all 17 strong and weak families have tested expected traits.
- control callbacks and drops execute on the loop owner thread.
- no public API requires callers to remember a thread-loop lock before using a
  freely shareable proxy.
- `pw-browse` performs create/link/destroy through bounded commands and stores no
  proxy in UI-thread state.
- SPA source lifecycle methods cannot compile across threads; concurrent invoke
  is tested through its dedicated handle.
- shutdown has explicit accepted/cancelled/closed outcomes and deterministic
  owner-thread drop order.
- node process mappings have one mutable Rust owner and cannot be shared safely
  with the control loop.

## Cross-references

- [`/.design/architecture-review/review0.oc.md`](/.design/architecture-review/review0.oc.md): identifies blanket concurrency assertions as a critical architecture defect and calls for explicit handles; this design supplies the complete inventory, exercised call sites, and migration.
- [`/.design/architecture-review/review-rekick0.oc.md`](/.design/architecture-review/review-rekick0.oc.md): establishes that thread traits must be earned and runtime choice must remain an adapter; this design defines the control-loop/process-loop boundary that those rules require.
- [`/.design/native-frame/frame0.gpt56.md`](/.design/native-frame/frame0.gpt56.md): its frame receiver/sender ownership model should live inside the loop-local protocol session; a transport may move to a new owner, but it is not concurrently shared merely because its fields use locks.
- [`/doc/discovery/merge.md`](/doc/discovery/merge.md): records deadlock/race risk between `ThreadLoop` and the async worker; the separate command and process-owner domains resolve that risk without coupling crate placement to threading policy.
- [`/doc/discovery/node.md`](/doc/discovery/node.md): provides the original synchronous callback and Tokio orchestration intent; this design preserves callback-scoped cycles while making Tokio one movable-owner adapter, not a reason for `Sync` mappings.
- [`/doc/discovery/node-integration.md`](/doc/discovery/node-integration.md): proposes forwarding control events into node state; this design narrows that bridge to owned, bounded commands instead of shared proxy or control state.
- [`/spa/src/hook.rs`](/spa/src/hook.rs): current callbacks run while the hook-list mutex is held; loop affinity does not fix reentrant-listener deadlock, but it removes cross-thread mutation as an additional dimension. A separate listener-dispatch redesign remains required.
- [`/README.md`](/README.md): promises a safe, idiomatic API and a future C wrapper; the explicit affinity and FFI capability contracts are prerequisites for making that safety claim across both Rust and C surfaces.
