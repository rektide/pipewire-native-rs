// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: Copyright (c) 2026 Asymptotic Inc.

//! ClientNode session-domain primitives.

/// PipeWire node activation ABI access and v6 transitions.
pub mod activation;
/// Semantic first-slice configuration and canonical wire conversion.
pub mod config;
/// Cycle-scoped output buffer selection and publication.
pub mod cycle;
/// Session error categories.
pub mod error;
/// Connection-scoped imported-memory ownership and generational mapping.
pub mod memory;
/// Transactional output format, buffer, and IO generations.
pub mod output;
/// Runtime-independent per-node ClientNode owner.
pub mod owner;
/// Downstream activation generations.
pub mod peer;
/// SPA port IO, chunk, and media-plane views.
pub mod port;
/// Own transport generation and hidden completion descriptor.
pub mod transport;
