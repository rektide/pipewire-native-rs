// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: Copyright (c) 2026 Asymptotic Inc.

mod frame;
mod messages;

pub use frame::{read_packet, write_packet, NativeHeader, NativePacket, HEADER_LEN};
pub use messages::{
    client_method, core_event, core_method, decode_inbound_message,
    encode_client_update_properties_empty_payload, encode_core_done_payload,
    encode_core_error_payload, encode_core_get_registry_payload, encode_core_hello_payload,
    encode_core_info_payload, encode_core_sync_payload, encode_registry_global_payload,
    encode_registry_global_remove_payload, registry_event, registry_method, InboundMessage,
    CLIENT_ID, CORE_ID,
};
