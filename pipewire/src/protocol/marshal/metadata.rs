// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: Copyright (c) 2025 Asymptotic Inc.
// SPDX-FileCopyrightText: Copyright (c) 2025 Arun Raghavan

use pipewire_native_macros as macros;
use pipewire_native_spa::{self as spa, pod::Pod};

use crate::{
    closure, default_topic, log, object_notify,
    protocol::connection::Connection,
    proxy::{
        metadata::{Metadata, MetadataMethods},
        HasProxy,
    },
    trace, Id,
};

default_topic!(log::topic::PROTOCOL);

#[repr(u8)]
#[derive(Debug, macros::Marshallable)]
pub(crate) enum Methods {
    SetProperty(SetProperty) = 1,
    Clear(Clear),
}

#[derive(Debug, macros::PodStruct)]
pub(crate) struct SetProperty {
    subject: i32,
    key: Option<String>,
    type_: Option<String>,
    value: Option<String>,
}

#[derive(Debug, macros::PodStruct)]
pub(crate) struct Clear {
    empty: (),
}

impl Methods {
    pub(crate) fn marshal(connection: Connection) -> MetadataMethods<Metadata> {
        MetadataMethods {
            set_property: closure!([connection] metadata, subject, key, type_, value, {
                connection.push(
                    metadata.proxy().id(),
                    Methods::SetProperty(SetProperty {
                        subject: subject as i32,
                        key: key.map(|s| s.to_string()),
                        type_: type_.map(|s| s.to_string()),
                        value: value.map(|s| s.to_string()),
                    })
                )
            }),
            clear: closure!([connection] metadata, {
                connection.push(
                    metadata.proxy().id(),
                    Methods::Clear(Clear{
                        empty: ()
                    }),
                )
            }),
        }
    }
}

#[derive(Debug, macros::Marshallable)]
pub(crate) enum Events {
    Property(Property),
}

#[derive(Debug, macros::PodStruct)]
pub(crate) struct Property {
    subject: i32,
    key: Option<String>,
    type_: Option<String>,
    value: Option<String>,
}

impl Events {
    pub(crate) fn demarshal(
        message: &mut super::message::InboundMessage<'_>,
        metadata: Metadata,
    ) -> std::io::Result<()> {
        let event = message.decode::<Events>()?;

        trace!("got event: {event:?}");

        match event {
            Events::Property(prop) => {
                let subject = prop.subject as Id;
                let key = prop.key.as_deref();
                let type_ = prop.type_.as_deref();
                let value = prop.value.as_deref();

                object_notify!(metadata, property, subject, key, type_, value)
            }
        }

        Ok(())
    }
}
