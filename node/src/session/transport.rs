// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: Copyright (c) 2026 Asymptotic Inc.

//! Transactional per-node transport generations.

use std::panic::{catch_unwind, AssertUnwindSafe};

use super::{
    activation::{ActivationError, ActivationStatus, ActivationView},
    config::TransportDescriptor,
    error::SessionError,
    memory::{MemoryInterval, MemoryKey, MemoryMapping, MemoryResolver},
};
use crate::signal::EventFd;

/// Result of using one transport wake as a claim hint.
#[derive(Debug)]
pub(crate) enum ClaimCompletion<T> {
    /// Activation was not TRIGGERED, so no callback ran.
    NotClaimed,
    /// Activation was claimed and finished; callback result is retained.
    Finished(Result<T, SessionError>),
}

/// Owns one exact activation mapping and both transport eventfds.
#[derive(Debug)]
pub struct TransportGeneration {
    id: u64,
    trigger: EventFd,
    _completion: EventFd,
    activation_key: MemoryKey,
    activation: MemoryMapping,
}

impl TransportGeneration {
    /// Builds and initializes a candidate before it can replace a live transport.
    pub fn bind(
        id: u64,
        descriptor: TransportDescriptor,
        memory: &impl MemoryResolver,
    ) -> Result<Self, SessionError> {
        let key = memory.resolve(descriptor.activation.memory)?;
        let mut mapping = memory.map(
            key,
            descriptor.activation.offset,
            descriptor.activation.len,
            true,
        )?;
        let activation =
            unsafe { ActivationView::from_raw_parts(mapping.as_mut_ptr(), mapping.len())? };
        activation.initialize_client_v1();
        let trigger = EventFd::from_owned_fd(descriptor.trigger_fd)?;
        let completion = EventFd::from_owned_fd(descriptor.completion_fd)?;
        Ok(Self {
            id,
            trigger,
            _completion: completion,
            activation_key: key,
            activation: mapping,
        })
    }

    /// Stable generation attached to runtime readiness registrations.
    pub const fn id(&self) -> u64 {
        self.id
    }

    /// Exact imported generation retained by the activation mapping.
    pub const fn activation_key(&self) -> MemoryKey {
        self.activation_key
    }

    pub(crate) fn interval(&self) -> MemoryInterval {
        self.activation.interval()
    }

    /// Non-blockingly drains the coalesced wake counter.
    pub fn drain(&self) -> Result<u64, SessionError> {
        self.trigger.drain().map_err(SessionError::Signal)
    }

    /// Duplicates the trigger handle for a runtime readiness adapter.
    pub fn try_clone_trigger(&self) -> Result<EventFd, SessionError> {
        self.trigger.try_clone().map_err(SessionError::Signal)
    }

    /// Reads own activation status.
    pub fn status(&mut self) -> Result<ActivationStatus, SessionError> {
        Ok(self.activation_view()?.status()?)
    }

    /// Makes the v6 activation schedulable.
    pub fn mark_ready(&mut self) -> Result<(), SessionError> {
        Ok(self.activation_view()?.mark_ready()?)
    }

    /// Leaves graph scheduling between callbacks.
    pub fn deactivate(&mut self) -> Result<ActivationStatus, SessionError> {
        Ok(self.activation_view()?.deactivate()?)
    }

    pub(crate) fn claim_and_finish<T>(
        &mut self,
        awake_ns: u64,
        finish_time: impl FnOnce() -> u64,
        process: impl FnOnce() -> Result<(T, i32), SessionError>,
    ) -> Result<ClaimCompletion<T>, SessionError> {
        let activation = self.activation_view()?;
        let claim = match activation.claim_cycle(awake_ns) {
            Ok(claim) => claim,
            Err(ActivationError::InvalidTransition {
                expected: ActivationStatus::Triggered,
                actual,
            }) => {
                let _ = actual;
                return Ok(ClaimCompletion::NotClaimed);
            }
            Err(error) => return Err(error.into()),
        };
        let processed = catch_unwind(AssertUnwindSafe(process));
        let (result, status) = match processed {
            Ok(Ok((value, status))) => (Ok(value), status),
            Ok(Err(error)) => (Err(error), -libc::EIO),
            Err(_) => (Err(SessionError::CallbackPanicked), -libc::EIO),
        };
        claim.publish_result_and_finish(status, finish_time())?;
        Ok(ClaimCompletion::Finished(result))
    }

    fn activation_view(&mut self) -> Result<ActivationView<'_>, SessionError> {
        Ok(unsafe {
            ActivationView::from_raw_parts(self.activation.as_mut_ptr(), self.activation.len())?
        })
    }
}
