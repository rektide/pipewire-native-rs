// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: Copyright (c) 2026 Asymptotic Inc.

use std::io;

use pipewire_native_spa as spa;

/// Core object id in the native protocol.
pub const CORE_ID: u32 = 0;
/// Client object id for the connected client.
pub const CLIENT_ID: u32 = 1;

/// Core method opcodes.
pub mod core_method {
    /// Core::Hello.
    pub const HELLO: u8 = 1;
    /// Core::Sync.
    pub const SYNC: u8 = 2;
    /// Core::GetRegistry.
    pub const GET_REGISTRY: u8 = 5;
}

/// Core event opcodes.
pub mod core_event {
    /// Core::Info.
    pub const INFO: u8 = 0;
    /// Core::Done.
    pub const DONE: u8 = 1;
    /// Core::Error.
    pub const ERROR: u8 = 3;
    /// Core::AddMem.
    pub const ADD_MEM: u8 = 6;
    /// Core::RemoveMem.
    pub const REMOVE_MEM: u8 = 7;
}

pub use spa::buffer::data_type as spa_data_type;

/// Client method opcodes.
pub mod client_method {
    /// Client::UpdateProperties.
    pub const UPDATE_PROPERTIES: u8 = 2;
}

/// Registry method opcodes.
pub mod registry_method {
    /// Registry::Bind.
    pub const BIND: u8 = 1;
    /// Registry::Destroy.
    pub const DESTROY: u8 = 2;
}

/// Registry event opcodes.
pub mod registry_event {
    /// Registry::Global.
    pub const GLOBAL: u8 = 0;
    /// Registry::GlobalRemove.
    pub const GLOBAL_REMOVE: u8 = 1;
}

/// Decoded subset of inbound methods relevant for scripted scenarios.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum InboundMessage {
    /// Core hello.
    CoreHello {
        /// Client-reported protocol version.
        version: u32,
    },
    /// Core sync.
    CoreSync {
        /// Id echoed in Core::Done.
        id: u32,
        /// Sequence echoed in Core::Done.
        seq: u32,
    },
    /// Core get registry.
    CoreGetRegistry {
        /// Registry interface version requested by client.
        version: u32,
        /// Proxy id allocated by client for registry.
        new_id: u32,
    },
    /// Client update properties.
    ClientUpdateProperties,
    /// Registry bind.
    RegistryBind {
        /// Global id to bind.
        id: u32,
        /// Interface type string.
        type_: String,
        /// Interface version.
        version: u32,
        /// Client proxy id for the bound object.
        new_id: u32,
    },
    /// Registry destroy.
    RegistryDestroy {
        /// Global id to destroy.
        id: u32,
    },
    /// Any unrecognized object/opcode pair.
    Unknown {
        /// Object id from packet header.
        object_id: u32,
        /// Opcode from packet header.
        opcode: u8,
    },
}

/// Decodes a known subset of methods from a packet header + payload.
pub fn decode_inbound_message(
    object_id: u32,
    opcode: u8,
    payload: &[u8],
) -> io::Result<InboundMessage> {
    match (object_id, opcode) {
        (CORE_ID, core_method::HELLO) => {
            let version = parse_struct(payload, |sp| {
                let version = sp.pop_int()?;
                Ok(version as u32)
            })?;

            Ok(InboundMessage::CoreHello { version })
        }
        (CORE_ID, core_method::SYNC) => {
            let (id, seq) = parse_struct(payload, |sp| {
                let id = sp.pop_int()?;
                let seq = sp.pop_int()?;
                Ok((id as u32, seq as u32))
            })?;

            Ok(InboundMessage::CoreSync { id, seq })
        }
        (CORE_ID, core_method::GET_REGISTRY) => {
            let (version, new_id) = parse_struct(payload, |sp| {
                let version = sp.pop_int()?;
                let new_id = sp.pop_int()?;
                Ok((version as u32, new_id as u32))
            })?;

            Ok(InboundMessage::CoreGetRegistry { version, new_id })
        }
        (CLIENT_ID, client_method::UPDATE_PROPERTIES) => {
            // Shape check only: Struct(Struct(PairList))
            parse_struct(payload, |sp| {
                sp.pop_struct(|sp| {
                    let n_items = sp.pop_int()?;
                    for _ in 0..n_items {
                        let _ = sp.pop_string()?;
                        let _ = sp.pop_string()?;
                    }
                    Ok(())
                })?;

                Ok(())
            })?;

            Ok(InboundMessage::ClientUpdateProperties)
        }
        (_, registry_method::BIND) => {
            let (id, type_, version, new_id) = parse_struct(payload, |sp| {
                let id = sp.pop_int()?;
                let type_ = sp.pop_string()?;
                let version = sp.pop_int()?;
                let new_id = sp.pop_int()?;
                Ok((id as u32, type_, version as u32, new_id as u32))
            })?;

            Ok(InboundMessage::RegistryBind {
                id,
                type_,
                version,
                new_id,
            })
        }
        (_, registry_method::DESTROY) => {
            let id = parse_struct(payload, |sp| {
                let id = sp.pop_int()?;
                Ok(id as u32)
            })?;

            Ok(InboundMessage::RegistryDestroy { id })
        }
        _ => Ok(InboundMessage::Unknown { object_id, opcode }),
    }
}

/// Encodes a `Core::Hello` payload.
pub fn encode_core_hello_payload(version: u32) -> io::Result<Vec<u8>> {
    encode_struct_payload(|sb| sb.push_int(version as i32))
}

/// Encodes a `Core::GetRegistry` payload.
pub fn encode_core_get_registry_payload(version: u32, new_id: u32) -> io::Result<Vec<u8>> {
    encode_struct_payload(|sb| sb.push_int(version as i32).push_int(new_id as i32))
}

/// Encodes a `Core::Sync` payload.
pub fn encode_core_sync_payload(id: u32, seq: u32) -> io::Result<Vec<u8>> {
    encode_struct_payload(|sb| sb.push_int(id as i32).push_int(seq as i32))
}

/// Encodes a `Client::UpdateProperties` payload with no properties.
pub fn encode_client_update_properties_empty_payload() -> io::Result<Vec<u8>> {
    encode_struct_payload(|sb| sb.push_struct(|sb| sb.push_int(0)))
}

/// Encodes a `Core::Done` payload.
pub fn encode_core_done_payload(id: u32, seq: u32) -> io::Result<Vec<u8>> {
    encode_struct_payload(|sb| sb.push_int(id as i32).push_int(seq as i32))
}

/// Encodes a `Core::Error` payload.
pub fn encode_core_error_payload(
    id: u32,
    seq: u32,
    res: i32,
    message: &str,
) -> io::Result<Vec<u8>> {
    encode_struct_payload(|sb| {
        sb.push_int(id as i32)
            .push_int(seq as i32)
            .push_int(res)
            .push_string(message)
    })
}

/// Decoded `Core::AddMem` payload fields.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct CoreAddMemPayload {
    /// Server memory id.
    pub id: u32,
    /// Memory type from `spa_data_type`.
    pub memory_type: u32,
    /// Index of the descriptor in the frame's FD table.
    pub fd_index: i32,
    /// Extra memory flags.
    pub flags: u32,
}

/// Encodes a `Core::AddMem` payload. The indexed fd is sent out-of-band.
pub fn encode_core_add_mem_payload(
    id: u32,
    memory_type: u32,
    fd_index: i32,
    flags: u32,
) -> io::Result<Vec<u8>> {
    encode_struct_payload(|sb| {
        sb.push_int(id as i32)
            .push_id(spa::pod::types::Id(memory_type))
            .push_fd(fd_index)
            .push_int(flags as i32)
    })
}

/// Decodes `Core::AddMem` payload fields.
pub fn decode_core_add_mem_payload(payload: &[u8]) -> io::Result<CoreAddMemPayload> {
    parse_struct(payload, |sp| {
        let id = sp.pop_int()?;
        let memory_type = sp.pop_id::<u32>()?;
        let fd_index = sp.pop_fd()?;
        let flags = sp.pop_int()?;

        Ok(CoreAddMemPayload {
            id: id as u32,
            memory_type: memory_type.0,
            fd_index: fd_index.0,
            flags: flags as u32,
        })
    })
}

/// Encodes a `Core::RemoveMem` payload.
pub fn encode_core_remove_mem_payload(id: u32) -> io::Result<Vec<u8>> {
    encode_struct_payload(|sb| sb.push_int(id as i32))
}

/// Encodes a minimal `Core::Info` payload.
pub fn encode_core_info_payload(
    cookie: u32,
    user_name: &str,
    host_name: &str,
    version: &str,
    name: &str,
    props: &[(String, String)],
) -> io::Result<Vec<u8>> {
    encode_struct_payload(|sb| {
        sb.push_int(CORE_ID as i32)
            .push_int(cookie as i32)
            .push_string(user_name)
            .push_string(host_name)
            .push_string(version)
            .push_string(name)
            .push_long(1) // PROPS change mask
            .push_struct(|sb| push_string_pair_list(sb, props))
    })
}

/// Encodes a `Registry::Global` payload.
pub fn encode_registry_global_payload(
    id: u32,
    permissions: u32,
    type_: &str,
    version: u32,
    props: &[(String, String)],
) -> io::Result<Vec<u8>> {
    encode_struct_payload(|sb| {
        sb.push_int(id as i32)
            .push_int(permissions as i32)
            .push_string(type_)
            .push_int(version as i32)
            .push_struct(|sb| push_string_pair_list(sb, props))
    })
}

/// Encodes a `Registry::GlobalRemove` payload.
pub fn encode_registry_global_remove_payload(id: u32) -> io::Result<Vec<u8>> {
    encode_struct_payload(|sb| sb.push_int(id as i32))
}

fn parse_struct<T>(
    payload: &[u8],
    parse: impl FnOnce(&mut spa::pod::parser::Parser<'_>) -> Result<T, spa::pod::Error>,
) -> io::Result<T> {
    let mut parser = spa::pod::parser::Parser::new(payload);
    parser
        .pop_struct(parse)
        .map(|(out, _)| out)
        .map_err(pod_error)
}

fn encode_struct_payload(
    build: impl FnOnce(spa::pod::builder::StructBuilder<'_>) -> spa::pod::builder::StructBuilder<'_>,
) -> io::Result<Vec<u8>> {
    let mut data = vec![0u8; 8192];
    let out = spa::pod::builder::Builder::new(data.as_mut_slice())
        .push_struct(build)
        .build()
        .map_err(pod_error)?;

    Ok(out.to_vec())
}

fn push_string_pair_list<'a>(
    mut sb: spa::pod::builder::StructBuilder<'a>,
    props: &[(String, String)],
) -> spa::pod::builder::StructBuilder<'a> {
    sb = sb.push_int(props.len() as i32);
    for (key, value) in props {
        sb = sb.push_string(key.as_str()).push_string(value.as_str());
    }
    sb
}

fn pod_error(err: spa::pod::Error) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!("pod codec error: {err:?}"),
    )
}

#[cfg(test)]
mod tests {
    use super::{
        decode_core_add_mem_payload, decode_inbound_message, encode_core_add_mem_payload,
        encode_core_sync_payload, spa_data_type, InboundMessage, CORE_ID,
    };

    #[test]
    fn decodes_core_sync_payload() {
        let payload = encode_core_sync_payload(11, 22).unwrap();
        let msg = decode_inbound_message(CORE_ID, super::core_method::SYNC, &payload).unwrap();

        assert_eq!(msg, InboundMessage::CoreSync { id: 11, seq: 22 });
    }

    #[test]
    fn roundtrip_core_add_mem_payload() {
        let payload = encode_core_add_mem_payload(9, spa_data_type::MEM_FD, 2, 5).unwrap();
        let decoded = decode_core_add_mem_payload(payload.as_slice()).unwrap();

        assert_eq!(decoded.id, 9);
        assert_eq!(decoded.memory_type, spa_data_type::MEM_FD);
        assert_eq!(decoded.fd_index, 2);
        assert_eq!(decoded.flags, 5);
    }
}
