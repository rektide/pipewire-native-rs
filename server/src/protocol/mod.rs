// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: Copyright (c) 2026 Asymptotic Inc.

mod frame;
mod messages;

pub use frame::{
    write_packet, write_packet_with_fds, NativeHeader, NativePacket, NativePacketReader, HEADER_LEN,
};
pub use messages::{
    client_method, core_event, core_method, decode_core_add_mem_payload, decode_inbound_message,
    encode_client_update_properties_empty_payload, encode_core_add_mem_payload,
    encode_core_done_payload, encode_core_error_payload, encode_core_get_registry_payload,
    encode_core_hello_payload, encode_core_info_payload, encode_core_remove_mem_payload,
    encode_core_sync_payload, encode_registry_global_payload,
    encode_registry_global_remove_payload, registry_event, registry_method, spa_data_type,
    CoreAddMemPayload, InboundMessage, CLIENT_ID, CORE_ID,
};
