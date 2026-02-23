// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: Copyright (c) 2025 Asymptotic Inc.
// SPDX-FileCopyrightText: Copyright (c) 2025 Arun Raghavan

use pipewire_native_macros as macros;
use pipewire_native_spa::{self as spa, pod::Pod};

use crate::{
    default_topic, log, object_notify,
    properties::Properties,
    protocol::connection::Connection,
    proxy::factory::{Factory, FactoryChangeMask, FactoryInfo},
    trace, Id,
};

use super::PairList;

default_topic!(log::topic::PROTOCOL);

// No methods

#[derive(Debug, macros::Marshallable)]
pub(crate) enum Events {
    Info(Info),
}

#[derive(Debug, macros::PodStruct)]
pub(crate) struct Info {
    id: i32,
    name: String,
    type_: String,
    version: i32,
    change_mask: i64,
    props: PairList<String, String>,
}

impl Events {
    pub(crate) fn demarshal(
        connection: &Connection,
        header: &super::message::Header,
        factory: Factory,
    ) -> std::io::Result<()> {
        let event = connection.decode_core_message::<Events>(header)?;

        trace!("got event: {event:?}");

        match event {
            Events::Info(info) => {
                let props = Properties::new_vec(info.props.data);

                let factory_info = FactoryInfo {
                    id: info.id as Id,
                    name: info.name.as_str(),
                    type_: info.type_.as_str(),
                    version: info.version as u32,
                    mask: FactoryChangeMask::from_bits_truncate(info.change_mask as u32),
                    props: &props,
                };

                object_notify!(factory, info, &factory_info);
            }
        }

        Ok(())
    }
}
