// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: Copyright (c) 2026 Asymptotic Inc.

//! ClientNode session-domain primitives.

/// PipeWire node activation ABI access and v6 transitions.
pub mod activation;
/// Cycle-scoped output buffer selection and publication.
pub mod cycle;
/// Connection-scoped imported-memory ownership and generational mapping.
pub mod memory;
/// SPA port IO, chunk, and media-plane views.
pub mod port;
