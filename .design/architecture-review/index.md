# Architecture Review

## Start Here

- [`review-rekick0.oc.md`](/.design/architecture-review/review-rekick0.oc.md): operational re-kickoff centered on one ownership-safe, typed ClientNode cycle.
- [`review0.oc.md`](/.design/architecture-review/review0.oc.md): full codebase review covering architecture, unsafe code, protocol framing, tests, documentation, and alternative module seams.
- [`advance0.oc.md`](/.design/architecture-review/advance0.oc.md): implementation, verification, independent-review feedback, and handoff record for the first multi-stream advancement wave.

## Focused Designs

- [`frame0.gpt56.md`](/.design/native-frame/frame0.gpt56.md): canonical native frame and SCM_RIGHTS ownership interface, state machines, migration sequence, and adversarial test matrix.
- [`threading0.gpt56.md`](/.design/threading-contract/threading0.gpt56.md): threading and auto-trait contract, including the loop-local ownership model, bounded cross-thread handles, unsafe-trait inventory, and migration sequence.
- [`inventory0.gpt56.md`](/.design/client-node-protocol/inventory0.gpt56.md): upstream ClientNode v6 opcode, POD, activation, buffer, processing, and peer-signaling inventory for the minimum playback path.
- [`session0.gpt56s.md`](/.design/client-node-session/session0.gpt56s.md): typed per-node session, generation retirement, activation atomics, port IO/buffer cycle, peer signaling, teardown, and runtime-adapter design.

## Developer Harnesses

- [`SPA POD fuzzing`](/fuzz/README.md): persistent coverage-guided harness and regression corpus for typed and raw POD decoding.

## Historical Discovery

- [`node.md`](/doc/discovery/node.md): initial node data-plane plan. Its opening current-state claims predate receive-side SCM_RIGHTS and AddMem decoding.
- [`node-integration.md`](/doc/discovery/node-integration.md): control/data-plane bridge analysis. It identifies the original forwarding gap but understates missing ClientNode buffer/session modeling.
- [`server.md`](/doc/discovery/server.md): rationale and original plan for the deterministic scripted native peer.
- [`server-unification.md`](/doc/discovery/server-unification.md): comparison of duplicated client/server protocol machinery and the shared-protocol option.
- [`merge.md`](/doc/discovery/merge.md): later proposal to move the scripted peer into `pipewire`; retained as an alternative placement rather than the accepted wire-module target.
- [`merge-impl-plan.md`](/doc/discovery/merge-impl-plan.md): implementation plan associated with the in-crate scripted-peer direction.
- [`osc.md`](/doc/discovery/osc.md): downstream control-stream application to revisit after the generic ClientNode cycle and buffer model work.

## Tracking

The active implementation graph is the `SU-client-node-cycle` beads epic. It supersedes the narrower `SU-epic` server-unification direction and tracks protocol safety, frame transport, memory ownership, minimum ClientNode coverage, typed session state, deterministic cycle validation, and WAV playback.
