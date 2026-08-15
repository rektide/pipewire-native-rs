// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: Copyright (c) 2026 Asymptotic Inc.

use bon::Builder;

use crate::protocol::InboundMessage;

/// A deterministic scripted scenario.
#[derive(Debug, Clone, Builder)]
pub struct Scenario {
    /// Ordered steps expected from the client.
    pub steps: Vec<ScriptStep>,
    /// Optional human-readable scenario label.
    pub name: Option<String>,
}

/// One expected inbound message and its resulting outbound actions.
#[derive(Debug, Clone, Builder)]
pub struct ScriptStep {
    /// Expected inbound message kind.
    pub expect: Expectation,
    /// Actions executed immediately after the expected message is observed.
    pub actions: Vec<Action>,
    /// Optional human-readable step label.
    pub name: Option<String>,
}

/// Inbound message expectation for a script step.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum Expectation {
    /// Match any inbound message.
    Any,
    /// Match `Core::Hello`.
    CoreHello,
    /// Match `Client::UpdateProperties`.
    ClientUpdateProperties,
    /// Match `Core::GetRegistry`.
    CoreGetRegistry,
    /// Match `Core::Sync`.
    CoreSync,
    /// Match `Registry::Bind`.
    RegistryBind,
    /// Match `Registry::Destroy`.
    RegistryDestroy,
    /// Match exact object id + opcode regardless of decoded message type.
    Exact {
        /// Expected object id.
        object_id: u32,
        /// Expected opcode.
        opcode: u8,
    },
}

impl Expectation {
    /// Returns true if an inbound message satisfies this expectation.
    pub fn matches(&self, object_id: u32, opcode: u8, inbound: &InboundMessage) -> bool {
        match self {
            Self::Any => true,
            Self::CoreHello => matches!(inbound, InboundMessage::CoreHello { .. }),
            Self::ClientUpdateProperties => {
                matches!(inbound, InboundMessage::ClientUpdateProperties)
            }
            Self::CoreGetRegistry => matches!(inbound, InboundMessage::CoreGetRegistry { .. }),
            Self::CoreSync => matches!(inbound, InboundMessage::CoreSync { .. }),
            Self::RegistryBind => matches!(inbound, InboundMessage::RegistryBind { .. }),
            Self::RegistryDestroy => matches!(inbound, InboundMessage::RegistryDestroy { .. }),
            Self::Exact {
                object_id: expected_object_id,
                opcode: expected_opcode,
            } => object_id == *expected_object_id && opcode == *expected_opcode,
        }
    }
}

/// Payload for emitting a `Core::Info` event.
#[derive(Debug, Clone, Builder)]
pub struct CoreInfoAction {
    /// Core cookie.
    pub cookie: u32,
    /// Server user name.
    pub user_name: String,
    /// Server host name.
    pub host_name: String,
    /// Server version string.
    pub version: String,
    /// Server display name.
    pub name: String,
    /// Additional key/value properties.
    pub props: Vec<(String, String)>,
}

/// Payload for emitting a `Registry::Global` event.
#[derive(Debug, Clone, Builder)]
pub struct RegistryGlobalAction {
    /// Global id.
    pub id: u32,
    /// Permission bit mask.
    pub permissions: u32,
    /// Global interface type string.
    pub type_: String,
    /// Global interface version.
    pub version: u32,
    /// Additional key/value properties.
    pub props: Vec<(String, String)>,
}

/// Payload for emitting `Core::Error`.
#[derive(Debug, Clone, Builder)]
pub struct CoreErrorAction {
    /// Resource id associated with the error.
    pub id: u32,
    /// Failing sequence number.
    pub seq: u32,
    /// Errno-style error value.
    pub res: i32,
    /// Error message.
    pub message: String,
}

/// Payload for emitting `Core::AddMem`.
#[derive(Debug, Clone, Builder)]
pub struct CoreAddMemAction {
    /// Server memory id announced to the client.
    pub id: u32,
    /// Memory type from `spa_data_type`.
    pub memory_type: u32,
    /// Index of the descriptor in the outbound frame's FD table.
    pub fd_index: i32,
    /// Extra memory flags.
    pub flags: u32,
    /// Allocated memfd size in bytes.
    pub size: usize,
}

/// Outbound actions performed when a step expectation is satisfied.
#[derive(Debug, Clone)]
pub enum Action {
    /// Emit a `Core::Info` event.
    SendCoreInfo(CoreInfoAction),
    /// Emit `Core::Done` using last captured sync values.
    SendCoreDoneFromLastSync,
    /// Emit `Core::Done` with explicit values.
    SendCoreDone {
        /// Done id.
        id: u32,
        /// Done sequence.
        seq: u32,
    },
    /// Emit `Core::Error`.
    SendCoreError(CoreErrorAction),
    /// Emit `Core::AddMem` with a memfd fd attached via SCM_RIGHTS.
    SendCoreAddMem(CoreAddMemAction),
    /// Emit `Registry::Global` on last known registry proxy id.
    SendRegistryGlobalOnLastRegistry(RegistryGlobalAction),
    /// Emit `Registry::GlobalRemove` on last known registry proxy id.
    SendRegistryGlobalRemoveOnLastRegistry {
        /// Global id to remove.
        id: u32,
    },
    /// Close current client connection.
    CloseConnection,
}
