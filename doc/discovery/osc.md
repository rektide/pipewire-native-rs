# OSC Support Discovery Prompt

Use this prompt to run a focused discovery subagent. The output should be an implementation-ready research brief for adding Open Sound Control (OSC) support to this workspace.

## Problem Statement

This project currently has meaningful PipeWire control-plane and emerging data-plane primitives, but no OSC-specific integration plan. We need a concrete design and execution path for OSC support that accounts for:

- control-plane concerns (object lifecycle, protocol events, routing, configuration, capability advertisement)
- data-plane concerns (real-time message flow, timing, memory/signal behavior, threading/backpressure)

The goal is to remove ambiguity before implementation starts.

## Discovery Objectives

Produce a written discovery that answers, at minimum:

1. What OSC support means in this codebase (minimum viable feature set vs. future expansion).
2. How OSC maps onto existing PipeWire-facing control-plane abstractions.
3. How OSC message flow intersects with node runtime/data-plane behavior.
4. What architectural options exist, and which option is recommended with clear tradeoffs.
5. What incremental implementation plan can be executed safely in this repository.

## Read First (Repository Sources)

Review these files before proposing architecture:

1. [`/README.md`](/README.md) - project goals and current capability boundaries.
2. [`/doc/discovery/node.md`](/doc/discovery/node.md) - current data-plane architecture and progress.
3. [`/node/src/lib.rs`](/node/src/lib.rs) - node crate boundaries.
4. [`/node/src/control/mod.rs`](/node/src/control/mod.rs) and [`/node/src/control/events.rs`](/node/src/control/events.rs) - control/data bridge model.
5. [`/node/src/host/mod.rs`](/node/src/host/mod.rs) - orchestration entry points.
6. [`/node/src/runtime/mod.rs`](/node/src/runtime/mod.rs), [`/node/src/transport/mod.rs`](/node/src/transport/mod.rs), [`/node/src/shm/memfd.rs`](/node/src/shm/memfd.rs), [`/node/src/signal/eventfd.rs`](/node/src/signal/eventfd.rs) - runtime transport primitives.
7. [`/pipewire/src/core.rs`](/pipewire/src/core.rs), [`/pipewire/src/protocol/marshal/core.rs`](/pipewire/src/protocol/marshal/core.rs), and [`/pipewire/src/protocol/connection.rs`](/pipewire/src/protocol/connection.rs) - protocol decode/callback/fd-passing behavior.

If any source appears stale or inconsistent, call it out explicitly in the findings.

## Scope

In scope:

- OSC integration strategy within this repository's architecture.
- Control-plane event/model changes needed to surface OSC endpoints and state.
- Data-plane transport/runtime implications for OSC message handling.
- API shape proposals for Rust users of this workspace.
- Testing strategy (unit + integration) and observability implications.

Out of scope (non-goals):

- Implementing OSC support.
- Building a full standalone OSC server unrelated to this crate architecture.
- Exhaustive OSC ecosystem survey beyond decisions needed here.
- Performance benchmarking implementation (only specify what should be benchmarked).

## Required Analysis Dimensions

### Control-plane

Cover:

- how OSC endpoints are created, registered, and discovered
- configuration surface (address patterns, namespaces, transport settings)
- lifecycle and error model (startup, reconfiguration, teardown)
- compatibility with current PipeWire object and callback structure

### Data-plane

Cover:

- OSC message ingestion/dispatch path relative to runtime cycle boundaries
- interaction with eventfd-driven processing and memory mapping assumptions
- latency/jitter/backpressure risks and mitigation options
- thread/async ownership model and real-time safety constraints

## Deliverables

Produce a single markdown document with the following sections:

1. Executive summary (short).
2. Current state (what exists now, what is missing).
3. Architecture options (at least 2), each with pros/cons.
4. Recommended approach and rationale.
5. Control-plane design changes.
6. Data-plane design changes.
7. Risks and unknowns.
8. Validation strategy (tests, instrumentation, failure cases).
9. Stepwise implementation plan (ordered, repository-specific).

## Acceptance Criteria

The discovery is acceptable only if all items below are true:

- It references the required repository sources above.
- It clearly separates control-plane and data-plane analysis.
- It provides at least two architecture options and selects one.
- It defines a concrete MVP boundary for OSC support.
- It includes explicit non-goals.
- It contains an actionable implementation sequence that could be turned into tickets.
- It lists measurable validation checks (what to test, and expected outcomes).
- It identifies major technical risks and unresolved questions.

## Output Quality Bar

- Be concise, technical, and specific to this repo.
- Prefer concrete interfaces, flow descriptions, and invariants over generic commentary.
- State assumptions explicitly.
- Flag any blocker that would require additional discovery before implementation.
