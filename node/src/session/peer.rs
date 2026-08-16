// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: Copyright (c) 2026 Asymptotic Inc.

//! Deterministic downstream activation generations.

use std::collections::BTreeMap;

use super::{
    activation::{ActivationView, TriggerOutcome},
    config::{NodeId, PeerActivationDescriptor},
    error::SessionError,
    memory::{MemoryKey, MemoryMapping, MemoryResolver},
};
use crate::signal::EventFd;

/// One exact downstream activation generation.
#[derive(Debug)]
pub struct PeerActivation {
    node: NodeId,
    generation: u64,
    signal: EventFd,
    key: MemoryKey,
    offset: usize,
    mapping: MemoryMapping,
}

impl PeerActivation {
    /// Validates and maps a candidate peer before installation.
    pub fn bind(
        generation: u64,
        descriptor: PeerActivationDescriptor,
        memory: &impl MemoryResolver,
    ) -> Result<Self, SessionError> {
        let key = memory.resolve(descriptor.activation.memory)?;
        let mut mapping = memory.map(
            key,
            descriptor.activation.offset,
            descriptor.activation.len,
            true,
        )?;
        unsafe { ActivationView::from_raw_parts(mapping.as_mut_ptr(), mapping.len())? };
        Ok(Self {
            node: descriptor.node,
            generation,
            signal: EventFd::from_owned_fd(descriptor.signal_fd)?,
            key,
            offset: descriptor.activation.offset,
            mapping,
        })
    }

    /// Downstream node identity.
    pub const fn node(&self) -> NodeId {
        self.node
    }

    /// Peer generation assigned by the owning session.
    pub const fn generation(&self) -> u64 {
        self.generation
    }

    /// Exact imported activation generation.
    pub const fn memory_key(&self) -> MemoryKey {
        self.key
    }

    pub(crate) fn interval(&self) -> (MemoryKey, usize, usize) {
        (self.key, self.offset, self.offset + self.mapping.len())
    }

    pub(crate) fn trigger(&mut self, now_ns: u64) -> Result<TriggerOutcome, SessionError> {
        let activation = unsafe {
            ActivationView::from_raw_parts(self.mapping.as_mut_ptr(), self.mapping.len())?
        };
        let outcome = activation.decrement_pending_and_trigger(now_ns)?;
        if outcome == TriggerOutcome::Triggered {
            self.signal.signal(1)?;
        }
        Ok(outcome)
    }
}

/// Peer activations visited in deterministic node-ID order.
#[derive(Debug, Default)]
pub struct PeerSet {
    by_node: BTreeMap<NodeId, PeerActivation>,
}

impl PeerSet {
    /// Transactionally installs a validated generation.
    pub fn insert(&mut self, peer: PeerActivation) -> Option<PeerActivation> {
        self.by_node.insert(peer.node, peer)
    }

    /// Idempotently removes one downstream node.
    pub fn remove(&mut self, node: NodeId) -> Option<PeerActivation> {
        self.by_node.remove(&node)
    }

    /// Returns peer count.
    pub fn len(&self) -> usize {
        self.by_node.len()
    }

    /// Returns whether no peers are configured.
    pub fn is_empty(&self) -> bool {
        self.by_node.is_empty()
    }

    pub(crate) fn trigger_all(&mut self, now_ns: u64) -> Result<(), SessionError> {
        for (node, peer) in &mut self.by_node {
            peer.trigger(now_ns).map_err(|error| match error {
                SessionError::Activation(source) => SessionError::PeerTrigger {
                    peer: *node,
                    source,
                },
                SessionError::Signal(source) => SessionError::PeerSignal {
                    peer: *node,
                    source,
                },
                other => other,
            })?;
        }
        Ok(())
    }

    pub(crate) fn remove_memory(&mut self, key_id: super::memory::MemoryId) {
        self.by_node.retain(|_, peer| peer.key.id != key_id);
    }

    pub(crate) fn intervals(&self) -> impl Iterator<Item = (MemoryKey, usize, usize)> + '_ {
        self.by_node.values().map(PeerActivation::interval)
    }

    pub(crate) fn intervals_except(
        &self,
        node: NodeId,
    ) -> impl Iterator<Item = (MemoryKey, usize, usize)> + '_ {
        self.by_node
            .iter()
            .filter(move |(candidate, _)| **candidate != node)
            .map(|(_, peer)| peer.interval())
    }

    pub(crate) fn contains(&self, node: NodeId) -> bool {
        self.by_node.contains_key(&node)
    }
}
