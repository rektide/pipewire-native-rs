// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: Copyright (c) 2026 Asymptotic Inc.

#![warn(missing_docs)]

//! Data-plane primitives for hosting PipeWire node transport in Rust.
//!
//! This crate focuses on memfd-backed shared memory and eventfd signaling.

/// Control-plane event descriptors and bridge state.
pub mod control;
/// Runtime worker that drives process callbacks from transport signals.
pub mod runtime;
/// Shared memory helpers for memfd import and mapping.
pub mod shm;
/// Eventfd signal wrappers.
pub mod signal;
/// Transport descriptors and binding.
pub mod transport;
