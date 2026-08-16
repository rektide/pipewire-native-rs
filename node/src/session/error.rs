// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: Copyright (c) 2026 Asymptotic Inc.

//! Stable error vocabulary for ClientNode session configuration and cycles.

use std::fmt;

use super::{
    activation::ActivationError,
    config::{NodeId, UnsupportedFeature},
    cycle::CycleError,
    memory::MemoryError,
    port::PortError,
};

/// Runtime-independent ClientNode session failure.
#[derive(Debug)]
pub enum SessionError {
    /// A deliberately unsupported protocol feature was requested.
    Unsupported(UnsupportedFeature),
    /// Descriptor arithmetic overflowed.
    Overflow(&'static str),
    /// A repeated descriptor exceeded the session's own bound.
    TooManyDescriptors(&'static str),
    /// A Format POD was malformed or incomplete.
    InvalidFormat(&'static str),
    /// Imported memory failed to resolve or map.
    Memory(MemoryError),
    /// Activation mapping or transition failed.
    Activation(ActivationError),
    /// Port mapping or publication failed.
    Port(PortError),
    /// Cycle selection or commit failed.
    Cycle(CycleError),
    /// Eventfd setup or signaling failed.
    Signal(std::io::Error),
    /// Wake refers to a retired transport generation.
    StaleGeneration {
        /// Current transport generation.
        expected: u64,
        /// Generation attached to the wake.
        received: u64,
    },
    /// Peer propagation failed after own completion.
    PeerTrigger {
        /// Downstream node whose activation failed.
        peer: NodeId,
        /// Fatal activation-state failure.
        source: ActivationError,
    },
    /// A peer became TRIGGERED but its eventfd write failed.
    PeerSignal {
        /// Downstream node left in the triggered state.
        peer: NodeId,
        /// Fatal eventfd write failure.
        source: std::io::Error,
    },
    /// Current configuration does not meet the command's readiness barrier.
    NotReady(&'static str),
    /// Command is not valid in the current state.
    InvalidTransition(&'static str),
    /// Callback returned an application error.
    Callback(String),
    /// Callback panicked after claiming a cycle.
    CallbackPanicked,
    /// Session has disconnected terminally.
    Disconnected,
}

impl fmt::Display for SessionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

impl std::error::Error for SessionError {}

impl From<MemoryError> for SessionError {
    fn from(value: MemoryError) -> Self {
        Self::Memory(value)
    }
}
impl From<ActivationError> for SessionError {
    fn from(value: ActivationError) -> Self {
        Self::Activation(value)
    }
}
impl From<PortError> for SessionError {
    fn from(value: PortError) -> Self {
        Self::Port(value)
    }
}
impl From<CycleError> for SessionError {
    fn from(value: CycleError) -> Self {
        Self::Cycle(value)
    }
}
impl From<std::io::Error> for SessionError {
    fn from(value: std::io::Error) -> Self {
        Self::Signal(value)
    }
}
