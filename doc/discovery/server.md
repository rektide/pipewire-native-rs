# Minimal PipeWire Test Server Crate - Planning and Implementation Prompt

Use this document as the execution prompt for a future implementation subagent.

## Objective

Design and implement a **small, deterministic Rust crate** that acts as a scripted PipeWire server for integration testing of this project.

This crate is not a production server and not a general-purpose simulation environment. It exists to provide stable, repeatable behavior that tests can rely on.

## Problem Statement

Current integration testing needs a controllable PipeWire peer with predictable behavior. Relying only on unit tests in the Node crate is insufficient for validating cross-process behavior, protocol sequencing, and client interaction boundaries.

We need a minimal server test harness that:
- runs deterministic scripted scenarios,
- accepts exactly one client node for now,
- and exposes enough structure to expand scenarios later without redesign.

## Architecture Intent

Implement a dedicated test-support crate (domain-grouped, not flat) focused on one responsibility: **scripted server behavior for integration tests**.

Design priorities:
- deterministic behavior over flexibility,
- explicit state transitions over implicit behavior,
- composable builders over ad-hoc constructors,
- clear tracing visibility for debugging failures.

## Scope and Non-Goals

### In Scope
- A minimal PipeWire server implementation usable by integration tests.
- Scripted, pre-declared action sequences.
- Single client node support.
- Builder-first configuration API.
- Test fixtures and integration coverage for deterministic scenarios.

### Out of Scope
- Multi-client orchestration.
- Runtime dynamic scripting language.
- Feature parity with real PipeWire behavior.
- General-purpose server APIs or long-term daemon concerns.

## Functional Requirements

1. The crate MUST run a scripted sequence of server actions with deterministic ordering.
2. The crate MUST support exactly one connected client node.
3. If a second client attempts to connect, behavior MUST be explicit and testable (reject/fail with a clear error path).
4. Scripted behavior MUST be pre-configured before run start (no hidden runtime mutation model).
5. The crate MUST provide a simple mechanism to observe progression through the scripted steps.

## API and Design Requirements (Rust)

- Use `bon` to provide a builder-heavy API for server setup and scenario definition.
- Prefer typed configuration and explicit data models over stringly-typed inputs.
- Use `tracing` with meaningful spans/events around lifecycle and scripted step execution.
- Follow Let-It-Fail principles where appropriate; do not add error-handling layers that only restate errors without adding context.
- Keep public API intentionally narrow and test-oriented.

## Testing Expectations

Implementation MUST include tests that validate:
- deterministic execution of scripted action order,
- single-client acceptance behavior,
- explicit rejection/failure path for additional clients,
- reproducible behavior across repeated test runs,
- observable tracing points for debugging scenario failures.

Favor integration tests that exercise real crate boundaries over mock-heavy internal tests.

## Deliverables

1. New Rust crate for minimal scripted PipeWire test server.
2. Builder-based public API using `bon`.
3. Script/scenario model for deterministic action sequencing.
4. Integration tests demonstrating required behavior and constraints.
5. Documentation covering:
   - crate purpose and non-goals,
   - scenario definition flow,
   - single-client constraint,
   - how integration tests invoke and validate scenarios.

## Acceptance Criteria

All of the following must be true:
- Deterministic scripted scenario executes consistently across multiple runs.
- Only one client node can be active at a time.
- Additional client connection attempt follows documented and tested failure behavior.
- Public API is builder-oriented and uses `bon` in meaningful, non-trivial ways.
- Tracing output provides enough context to diagnose step-level failures.
- Documentation reflects actual behavior and test usage.

## Commit and Documentation Workflow Requirements

- Start by creating or updating the implementation-facing markdown/docs that define scenario behavior and crate intent.
- Create a `jj commit` after this initial documentation milestone.
- Continue implementation in small, reviewable increments and `jj commit` as meaningful units of work are completed.
- Commit messages must describe the work performed (not process phases).
- Do not push changes; local commits only.

## Execution Guidance for the Subagent

- Keep design minimal. Add only what is needed to satisfy deterministic integration testing.
- Prefer explicitness over abstraction.
- If a feature does not serve deterministic scripted testing, defer it.
- Maintain clear separation between test-support infrastructure and production-facing code paths.
