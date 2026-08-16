// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: Copyright (c) 2026 Asymptotic Inc.

#![warn(missing_docs)]

//! Data-plane primitives for hosting PipeWire node transport in Rust.
//!
//! This crate focuses on memfd-backed shared memory and eventfd signaling.

/// Control-plane event descriptors and bridge state.
pub mod control;
/// Runtime adapters that serialize semantic commands and process wake hints.
pub mod runtime;
/// ClientNode session-domain primitives.
pub mod session;
/// Shared memory helpers for memfd import and mapping.
pub mod shm;
/// Eventfd signal wrappers.
pub mod signal;
/// Transport descriptors and binding.
pub mod transport;
