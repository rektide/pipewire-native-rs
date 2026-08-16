// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: Copyright (c) 2025 Asymptotic Inc.
// SPDX-FileCopyrightText: Copyright (c) 2025 Arun Raghavan

use pipewire_native_macros as macros;
use pipewire_native_protocol::native::frame::FrameFds;
use pipewire_native_spa::{self as spa, pod::Pod};

use super::Marshallable;

pub(crate) struct InboundMessage<'a> {
    opcode: u8,
    payload: &'a [u8],
    fds: &'a mut FrameFds,
    footer: Option<CoreFooter>,
    footer_handler: Option<&'a dyn Fn(&CoreFooter)>,
}

impl<'a> InboundMessage<'a> {
    #[cfg(test)]
    pub(crate) fn new(opcode: u8, payload: &'a [u8], fds: &'a mut FrameFds) -> Self {
        Self {
            opcode,
            payload,
            fds,
            footer: None,
            footer_handler: None,
        }
    }

    pub(crate) fn with_footer_handler(
        opcode: u8,
        payload: &'a [u8],
        fds: &'a mut FrameFds,
        footer_handler: &'a dyn Fn(&CoreFooter),
    ) -> Self {
        Self {
            opcode,
            payload,
            fds,
            footer: None,
            footer_handler: Some(footer_handler),
        }
    }

    pub(crate) fn decode<T: Marshallable>(&mut self) -> std::io::Result<T> {
        let (body, body_size) = T::decode(self.opcode, self.payload).map_err(|error| {
            let kind = match &error {
                spa::pod::Error::Invalid(message)
                    if message == &format!("Could not decode opcode {}", self.opcode) =>
                {
                    std::io::ErrorKind::Unsupported
                }
                _ => std::io::ErrorKind::InvalidData,
            };
            std::io::Error::new(kind, format!("could not decode message body: {error:?}"))
        })?;
        let (footer, footer_size) = if body_size < self.payload.len() {
            let (footer, size) =
                CoreFooter::decode(&self.payload[body_size..]).map_err(|error| {
                    std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("could not decode message footer: {error:?}"),
                    )
                })?;
            (Some(footer), size)
        } else {
            (None, 0)
        };
        if body_size + footer_size != self.payload.len() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "message payload length {} differs from body {body_size} plus footer {footer_size}",
                    self.payload.len()
                ),
            ));
        }
        self.footer = footer;
        if let (Some(handler), Some(footer)) = (self.footer_handler, self.footer.as_ref()) {
            handler(footer);
        }
        Ok(body)
    }

    pub(crate) fn take_fd(&mut self, index: u32) -> std::io::Result<std::os::fd::OwnedFd> {
        self.fds
            .take(index)
            .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))
    }

    pub(crate) fn decode_client_node_event(
        &mut self,
    ) -> std::io::Result<pipewire_native_protocol::wire::client_node::Event> {
        let body_size = pipewire_native_spa::pod::RawPod::wrap(self.payload)
            .map_err(|error| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("could not locate ClientNode message body: {error:?}"),
                )
            })?
            .total_size();
        let event = pipewire_native_protocol::wire::client_node::decode_event(
            self.opcode,
            &self.payload[..body_size],
            self.fds,
            pipewire_native_protocol::wire::client_node::Limits::default(),
        )?;
        let (footer, footer_size) = if body_size < self.payload.len() {
            let (footer, size) =
                CoreFooter::decode(&self.payload[body_size..]).map_err(|error| {
                    std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("could not decode message footer: {error:?}"),
                    )
                })?;
            (Some(footer), size)
        } else {
            (None, 0)
        };
        if body_size + footer_size != self.payload.len() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "ClientNode payload has trailing bytes after its footer",
            ));
        }
        self.footer = footer;
        if let (Some(handler), Some(footer)) = (self.footer_handler, self.footer.as_ref()) {
            handler(footer);
        }
        Ok(event)
    }
}

pub(crate) struct CoreFooter {
    pub(crate) payloads: Vec<CoreFooterPayload>,
}

impl CoreFooter {
    pub(crate) fn new() -> Self {
        CoreFooter { payloads: vec![] }
    }

    #[allow(unused)]
    pub(crate) fn push(&mut self, payload: CoreFooterPayload) {
        self.payloads.push(payload);
    }
}

pub(crate) struct ClientFooter {
    pub(crate) payloads: Vec<ClientFooterPayload>,
}

impl ClientFooter {
    pub(crate) fn new() -> Self {
        ClientFooter { payloads: vec![] }
    }

    pub(crate) fn push(&mut self, payload: ClientFooterPayload) {
        self.payloads.push(payload);
    }
}
pub(crate) enum CoreFooterPayload {
    Generation(CoreGeneration),
}

pub(crate) enum ClientFooterPayload {
    Generation(ClientGeneration),
}

#[derive(macros::PodStruct)]
pub(crate) struct CoreGeneration {
    pub(crate) registry_generation: i64,
}

#[derive(macros::PodStruct)]
pub(crate) struct ClientGeneration {
    pub(crate) client_generation: i64,
}

impl spa::pod::Pod for CoreFooter {
    type DecodesTo = Self;

    fn encode(&self, data: &mut [u8]) -> Result<usize, spa::pod::Error> {
        let mut builder = spa::pod::builder::Builder::new(data);

        builder = builder.push_struct(|mut sb| {
            for p in &self.payloads {
                sb = match p {
                    CoreFooterPayload::Generation(g) => {
                        sb.push_id(spa::pod::types::Id(0u32)).push_pod(g)
                    }
                };
            }

            sb
        });

        let out = builder.build()?;

        Ok(out.len())
    }

    fn decode(data: &[u8]) -> Result<(Self::DecodesTo, usize), spa::pod::Error> {
        let mut parser = spa::pod::parser::Parser::new(data);

        parser.pop_struct(|sp| {
            let mut footer = CoreFooter::new();

            while sp.available() > 0 {
                let opcode = sp.pop_id::<u32>()?;
                let payload = match opcode.0 {
                    0 => {
                        let g = sp.pop_pod::<CoreGeneration>()?;
                        CoreFooterPayload::Generation(g)
                    }
                    opcode => {
                        return Err(spa::pod::Error::Invalid(format!(
                            "Invalid footer opcode {opcode}"
                        )))
                    }
                };

                footer.payloads.push(payload);
            }

            Ok(footer)
        })
    }
}

impl spa::pod::Pod for ClientFooter {
    type DecodesTo = Self;

    fn encode(&self, data: &mut [u8]) -> Result<usize, spa::pod::Error> {
        let mut builder = spa::pod::builder::Builder::new(data);

        builder = builder.push_struct(|mut sb| {
            for p in &self.payloads {
                sb = match p {
                    ClientFooterPayload::Generation(g) => {
                        sb.push_id(spa::pod::types::Id(0u32)).push_pod(g)
                    }
                };
            }

            sb
        });

        let out = builder.build()?;

        Ok(out.len())
    }

    fn decode(data: &[u8]) -> Result<(Self::DecodesTo, usize), spa::pod::Error> {
        let mut parser = spa::pod::parser::Parser::new(data);

        parser.pop_struct(|sp| {
            let mut footer = ClientFooter::new();

            while sp.available() > 0 {
                let opcode = sp.pop_id::<u32>()?;
                let payload = match opcode.0 {
                    0 => {
                        let g = sp.pop_pod::<ClientGeneration>()?;
                        ClientFooterPayload::Generation(g)
                    }
                    opcode => {
                        return Err(spa::pod::Error::Invalid(format!(
                            "Invalid footer opcode {opcode}"
                        )))
                    }
                };

                footer.payloads.push(payload);
            }

            Ok(footer)
        })
    }
}

#[cfg(test)]
mod tests {
    use std::os::{fd::AsFd, unix::net::UnixStream};

    use pipewire_native_protocol::native::frame::{
        FrameLimits, FrameReceiver, FrameSender, OutboundFrame, ReceiveOutcome,
    };

    use super::InboundMessage;

    fn frame_with_fd() -> pipewire_native_protocol::native::frame::ReceivedFrame {
        let (tx, rx) = UnixStream::pair().unwrap();
        let file = tempfile::tempfile().unwrap();
        let limits = FrameLimits::default();
        let frame =
            OutboundFrame::duplicate_fds(0, 0, 0, Vec::new(), &[file.as_fd()], limits).unwrap();
        let mut sender = FrameSender::new(limits);
        sender.enqueue(frame).unwrap();
        sender.flush(tx.as_fd()).unwrap();
        let mut receiver = FrameReceiver::new(limits);
        match receiver.receive(rx.as_fd()).unwrap() {
            ReceiveOutcome::Frame(frame) => frame,
            outcome => panic!("expected frame, got {outcome:?}"),
        }
    }

    #[test]
    fn fd_index_is_bounds_checked_without_consuming_valid_entry() {
        let frame = frame_with_fd();
        let (_, payload, mut fds) = frame.into_parts();
        let mut message = InboundMessage::new(0, &payload, &mut fds);

        assert_eq!(
            message.take_fd(1).unwrap_err().kind(),
            std::io::ErrorKind::InvalidData
        );
        message.take_fd(0).unwrap();
    }

    #[test]
    fn fd_index_can_transfer_ownership_only_once() {
        let frame = frame_with_fd();
        let (_, payload, mut fds) = frame.into_parts();
        let mut message = InboundMessage::new(0, &payload, &mut fds);

        let fd = message.take_fd(0).unwrap();
        assert_eq!(
            message.take_fd(0).unwrap_err().kind(),
            std::io::ErrorKind::InvalidData
        );
        drop(fd);
    }
}
