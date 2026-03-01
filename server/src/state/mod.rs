// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: Copyright (c) 2026 Asymptotic Inc.

/// Last `Core::Sync` values observed from inbound client messages.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SyncState {
    /// Sync `id` field.
    pub id: u32,
    /// Sync `seq` field.
    pub seq: u32,
}

/// Mutable state tracked while executing a scripted scenario.
#[derive(Debug, Default)]
pub struct ExecutionState {
    /// Most recent sync request observed from client.
    pub last_sync: Option<SyncState>,
    /// Most recent registry proxy id requested through `Core::GetRegistry`.
    pub last_registry_proxy_id: Option<u32>,
    /// Number of scripted steps that were successfully matched.
    pub completed_steps: usize,
    /// Number of accepted client connections.
    pub accepted_clients: usize,
    /// Number of rejected additional client connections.
    pub rejected_clients: usize,
}
