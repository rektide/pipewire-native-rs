// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: Copyright (c) 2026 Asymptotic Inc.

#![warn(missing_docs)]

//! Deterministic scripted PipeWire native protocol test server.
//!
//! This crate targets integration testing of `pipewire-native` and related crates.
//! It intentionally starts with a small subset and a script engine that can be
//! extended over time.

/// Public re-exports for generated builder APIs.
pub mod builders;
/// Native protocol framing and message helpers.
pub mod protocol;
/// Server runtime and execution entry points.
pub mod runtime;
/// Script model and matching/action semantics.
pub mod script;
/// Mutable server state captured while executing a scenario.
pub mod state;
/// Helpers for test setup and deterministic socket paths.
pub mod testkit;
