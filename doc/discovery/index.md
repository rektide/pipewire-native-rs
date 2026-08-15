# Discovery Index

## Current Direction

- [`architecture re-kickoff`](/.design/architecture-review/review-rekick0.oc.md): active product direction centered on one ownership-safe, typed ClientNode v6 cycle.
- [`advancement wave`](/.design/architecture-review/advance0.oc.md): implementation and verification record for protocol, memory, SPA safety, testing, and focused designs landed after the re-kickoff.
- [`native frame design`](/.design/native-frame/frame0.gpt56.md): accepted working design for the shared frame and SCM_RIGHTS transport now implemented under [`/protocol`](/protocol).
- [`ClientNode protocol inventory`](/.design/client-node-protocol/inventory0.gpt56.md): exact upstream v6 wire, activation, buffer, and peer-signaling behavior for the first output cycle.
- [`typed ClientNode session`](/.design/client-node-session/session0.gpt56s.md): active session, cycle, generation, teardown, and runtime-adapter design.
- [`threading contract`](/.design/threading-contract/threading0.gpt56.md): active migration design for loop-local control objects and bounded cross-thread handles.

## Node And Data Plane

- [`node.md`](/doc/discovery/node.md): **historical, 2026-02-28**. Initial memfd/eventfd/runtime plan. Its opening gap list predates receive-side SCM_RIGHTS and AddMem decoding, while its progress section records those later changes.
- [`node-integration.md`](/doc/discovery/node-integration.md): **historical bridge analysis**. Correctly identifies AddMem forwarding and missing transport decode, but predates the full ClientNode v6 buffer, activation, and downstream-peer model.
- [`osc.md`](/doc/discovery/osc.md): **deferred downstream design**. OSC over SPA control streams remains relevant after the generic ClientNode cycle and buffer model work.

## Scripted Peer And Protocol Sharing

- [`server.md`](/doc/discovery/server.md): **implemented historical plan**. Establishes the rationale and original scope for the deterministic scripted native peer.
- [`server-unification.md`](/doc/discovery/server-unification.md): **historical options analysis**. Identifies duplicated header, opcode, POD, and SCM_RIGHTS machinery and proposes a shared protocol-types direction.
- [`merge.md`](/doc/discovery/merge.md): **superseded placement recommendation**. Proposes moving the scripted peer into `pipewire`; current work instead shares a narrow `pipewire-native-protocol` crate while retaining the peer as a separate test adapter.
- [`merge-impl-plan.md`](/doc/discovery/merge-impl-plan.md): **superseded implementation plan** associated with deleting `server`. Its protocol-unification goals survive through shared transport migration, but standalone peer removal is not current direction.

## Reading Order

1. Read the [`architecture re-kickoff`](/.design/architecture-review/review-rekick0.oc.md) for goals and invariants.
2. Read the [`advancement wave`](/.design/architecture-review/advance0.oc.md) for what has landed and what independent review found.
3. Use the focused frame, ClientNode protocol/session, and threading designs for implementation details.
4. Consult historical discovery documents for rationale and alternatives, not current-state claims.
