// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: Copyright (c) 2026 Asymptotic Inc.

use pipewire_native_protocol::wire::client_node::{self as wire, Method};
use pipewire_native_spa as spa;

use crate::{
    protocol::connection::Connection,
    proxy::{client_node::ClientNode, HasProxy},
};

use super::{message::InboundMessage, Marshallable};

pub(crate) struct Outgoing(pub(crate) Method);

impl std::fmt::Debug for Outgoing {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ClientNodeMethod")
            .field("opcode", &self.0.opcode())
            .finish_non_exhaustive()
    }
}

impl Marshallable for Outgoing {
    fn opcode(&self) -> u8 {
        self.0.opcode()
    }

    fn encode(&self, data: &mut [u8]) -> Result<usize, spa::pod::Error> {
        let payload = wire::encode_method(&self.0)
            .map_err(|error| spa::pod::Error::Invalid(error.to_string()))?;
        if data.len() < payload.len() {
            return Err(spa::pod::Error::NoSpace);
        }
        data[..payload.len()].copy_from_slice(&payload);
        Ok(payload.len())
    }

    fn decode(_opcode: u8, _data: &[u8]) -> Result<(Self, usize), spa::pod::Error> {
        Err(spa::pod::Error::Invalid(
            "ClientNode outgoing methods are encode-only".into(),
        ))
    }
}

pub(crate) fn marshal(connection: Connection) -> crate::proxy::client_node::ClientNodeMethods {
    crate::proxy::client_node::ClientNodeMethods {
        send: Box::new(move |node, method| connection.push(node.proxy().id(), Outgoing(method))),
    }
}

pub(crate) fn demarshal(message: &mut InboundMessage<'_>, node: ClientNode) -> std::io::Result<()> {
    let event = message.decode_client_node_event()?;
    node.dispatch(event);
    Ok(())
}
