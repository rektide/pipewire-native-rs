// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: Copyright (c) 2026 Asymptotic Inc.

//! Semantic configuration accepted by the first ClientNode output session.

use std::num::{NonZeroU32, NonZeroUsize};

use pipewire_native_protocol::wire::client_node as wire;
use pipewire_native_spa::{
    buffer::DataType,
    param::{
        format::{Format, MediaSubtype, MediaType},
        ParamType,
    },
    pod::{parser::Parser, types::Id},
};

use super::{
    error::SessionError,
    memory::{MemoryId, RegionRef},
};

/// Only output port supported by the first session slice.
pub const OUTPUT_PORT: PortId = PortId(0);
/// Maximum retained buffers per output generation.
pub const MAX_SESSION_BUFFERS: usize = 64;
/// Maximum retained metadata descriptors per buffer.
pub const MAX_SESSION_METAS: usize = 64;
/// Maximum retained data descriptors per buffer.
pub const MAX_SESSION_DATAS: usize = 1;
const SPA_AUDIO_FORMAT_S16_LE: u32 = 0x103;

/// PipeWire node identifier.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct NodeId(pub u32);

/// PipeWire port identifier.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct PortId(pub u32);

/// PipeWire mix identifier. The first output slice accepts no mix.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct MixId(pub u32);

/// Raw SPA audio-channel identifier retained from the negotiated format.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AudioChannel(pub u32);

/// Supported audio sample encoding.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AudioSampleFormat {
    /// Signed 16-bit little-endian interleaved PCM.
    S16Le,
}

/// Fixed audio format used to validate and size output cycles.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NegotiatedAudioFormat {
    /// Sample encoding.
    pub sample_format: AudioSampleFormat,
    /// Samples per second.
    pub rate: NonZeroU32,
    /// Ordered channel positions, or unknown positions when omitted by the peer.
    pub channels: Box<[AudioChannel]>,
    /// Bytes in one sample.
    pub bytes_per_sample: NonZeroUsize,
    /// Bytes in one interleaved frame.
    pub frame_stride: NonZeroUsize,
}

impl NegotiatedAudioFormat {
    /// Creates the exact supported S16LE format.
    pub fn pcm_s16le(rate: u32, channels: u32) -> Result<Self, SessionError> {
        let rate = NonZeroU32::new(rate).ok_or(SessionError::InvalidFormat("zero sample rate"))?;
        let channel_count =
            NonZeroU32::new(channels).ok_or(SessionError::InvalidFormat("zero channel count"))?;
        let frame_stride = usize::try_from(channel_count.get())
            .ok()
            .and_then(|count| count.checked_mul(2))
            .and_then(NonZeroUsize::new)
            .ok_or(SessionError::Overflow("PCM frame stride"))?;
        Ok(Self {
            sample_format: AudioSampleFormat::S16Le,
            rate,
            channels: vec![AudioChannel(0); channel_count.get() as usize].into_boxed_slice(),
            bytes_per_sample: NonZeroUsize::new(2).unwrap(),
            frame_stride,
        })
    }

    /// Converts a canonical wire Format POD to the exact supported PCM value.
    pub fn from_wire(value: &wire::PortSetParam) -> Result<Self, SessionError> {
        validate_port(value.direction, value.port_id, None)?;
        if value.param_id != wire::SPA_PARAM_FORMAT {
            return Err(SessionError::Unsupported(UnsupportedFeature::Parameter(
                value.param_id,
            )));
        }
        let pod = value
            .param
            .as_ref()
            .ok_or(SessionError::InvalidFormat("format was cleared"))?;
        let mut parser = Parser::new(pod.data());
        let ((media_type, media_subtype, sample_format, rate, channels, positions), _) = parser
            .pop_object::<Format, ParamType, _>(|properties, id| {
                let mut values = (None, None, None, None, None, None);
                if id != ParamType::Format {
                    return Err(pipewire_native_spa::pod::Error::Invalid(
                        "not SPA_PARAM_Format".into(),
                    ));
                }
                while let Some((key, _, value)) = properties.pop_property()? {
                    match key {
                        Format::MediaType => values.0 = Some(value.decode::<Id<MediaType>>()?.0),
                        Format::MediaSubtype => {
                            values.1 = Some(value.decode::<Id<MediaSubtype>>()?.0)
                        }
                        Format::AudioFormat => values.2 = Some(value.decode::<Id<u32>>()?.0),
                        Format::AudioRate => values.3 = Some(value.decode::<i32>()?),
                        Format::AudioChannels => values.4 = Some(value.decode::<i32>()?),
                        Format::AudioPosition => values.5 = Some(value.decode::<&[Id<u32>]>()?),
                        _ => {}
                    }
                }
                Ok(values)
            })
            .map_err(|_| SessionError::InvalidFormat("malformed Format POD"))?;
        if media_type != Some(MediaType::Audio) || media_subtype != Some(MediaSubtype::Raw) {
            return Err(SessionError::Unsupported(UnsupportedFeature::MediaFormat));
        }
        if sample_format != Some(SPA_AUDIO_FORMAT_S16_LE) {
            return Err(SessionError::Unsupported(UnsupportedFeature::SampleFormat(
                sample_format.unwrap_or(0),
            )));
        }
        let rate = u32::try_from(rate.ok_or(SessionError::InvalidFormat("missing sample rate"))?)
            .map_err(|_| SessionError::InvalidFormat("invalid sample rate"))?;
        let count =
            u32::try_from(channels.ok_or(SessionError::InvalidFormat("missing channel count"))?)
                .map_err(|_| SessionError::InvalidFormat("invalid channel count"))?;
        let mut format = Self::pcm_s16le(rate, count)?;
        if let Some(positions) = positions {
            if positions.len() != format.channels.len() {
                return Err(SessionError::InvalidFormat(
                    "channel position count mismatch",
                ));
            }
            format.channels = positions.into_iter().map(|id| AudioChannel(id.0)).collect();
        }
        Ok(format)
    }
}

/// Transport resources after wire FD indices have been resolved.
#[derive(Debug)]
pub struct TransportDescriptor {
    /// Process-wake eventfd.
    pub trigger_fd: std::os::fd::OwnedFd,
    /// Hidden v6 completion eventfd retained for generation lifetime only.
    pub completion_fd: std::os::fd::OwnedFd,
    /// Own activation region.
    pub activation: RegionRef,
}

/// Downstream activation resources.
#[derive(Debug)]
pub struct PeerActivationDescriptor {
    /// Downstream node identity.
    pub node: NodeId,
    /// Downstream wake eventfd.
    pub signal_fd: std::os::fd::OwnedFd,
    /// Downstream activation region.
    pub activation: RegionRef,
}

/// Synchronous port-IO descriptor.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PortIoDescriptor {
    /// Output port.
    pub port: PortId,
    /// IO mapping.
    pub region: RegionRef,
}

/// Set or clear operation for synchronous output IO.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PortIoUpdate {
    /// Install a checked descriptor.
    Set(PortIoDescriptor),
    /// Clear the current mapping.
    Clear,
}

/// Install or remove a downstream activation.
#[derive(Debug)]
pub enum PeerActivationUpdate {
    /// Install a new generation.
    Set(PeerActivationDescriptor),
    /// Idempotently remove one node's target.
    Remove(NodeId),
}

/// One output buffer set.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BufferSetDescriptor {
    /// Output port.
    pub port: PortId,
    /// Checked one-plane buffers.
    pub buffers: Box<[BufferDescriptor]>,
}

/// Metadata/chunk and media description for one output buffer.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BufferDescriptor {
    /// Metadata and trailing chunk mapping.
    pub metadata: RegionRef,
    /// Metadata payloads preceding the data chunk.
    pub metas: Box<[MetaDescriptor]>,
    /// Imported media memory.
    pub media_memory: MemoryId,
    /// Media plane offset in the imported memory.
    pub map_offset: usize,
    /// Writable plane capacity.
    pub max_size: usize,
}

/// Checked metadata payload shape.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MetaDescriptor {
    /// SPA metadata type.
    pub type_id: u32,
    /// Payload size before 8-byte alignment.
    pub size: usize,
}

/// Features intentionally outside the first session slice.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum UnsupportedFeature {
    /// Input/capture ports.
    InputDirection,
    /// Non-zero or explicit mix IDs.
    Mix(MixId),
    /// Port other than output port zero.
    Port(PortId),
    /// Double-buffered asynchronous IO.
    AsyncIo,
    /// Client-allocated buffer reversal.
    ClientAllocatedBuffers,
    /// Multiple planes or a non-MemId plane.
    DataPlane,
    /// Data-plane flags outside the first writable mapping policy.
    DataFlags(u32),
    /// Non-audio/non-raw media.
    MediaFormat,
    /// Non-S16LE sample encoding.
    SampleFormat(u32),
    /// Parameter outside Format.
    Parameter(u32),
    /// IO kind outside synchronous Buffers.
    IoType(u32),
}

pub(crate) fn validate_port(
    direction: wire::Direction,
    port_id: u32,
    mix_id: Option<u32>,
) -> Result<PortId, SessionError> {
    if direction != wire::Direction::Output {
        return Err(SessionError::Unsupported(
            UnsupportedFeature::InputDirection,
        ));
    }
    let port = PortId(port_id);
    if port != OUTPUT_PORT {
        return Err(SessionError::Unsupported(UnsupportedFeature::Port(port)));
    }
    if let Some(mix) = mix_id {
        return Err(SessionError::Unsupported(UnsupportedFeature::Mix(MixId(
            mix,
        ))));
    }
    Ok(port)
}

impl TryFrom<wire::Transport> for TransportDescriptor {
    type Error = SessionError;

    fn try_from(value: wire::Transport) -> Result<Self, Self::Error> {
        Ok(Self {
            trigger_fd: value.trigger_fd,
            completion_fd: value.completion_fd,
            activation: value.activation.try_into()?,
        })
    }
}

impl TryFrom<wire::PortUseBuffers> for BufferSetDescriptor {
    type Error = SessionError;

    fn try_from(value: wire::PortUseBuffers) -> Result<Self, Self::Error> {
        let port = validate_port(value.direction, value.port_id, value.mix_id)?;
        if value.flags & wire::SPA_NODE_BUFFERS_FLAG_ALLOC != 0 {
            return Err(SessionError::Unsupported(
                UnsupportedFeature::ClientAllocatedBuffers,
            ));
        }
        if value.buffers.len() > MAX_SESSION_BUFFERS {
            return Err(SessionError::TooManyDescriptors("buffers"));
        }
        let mut buffers = Vec::with_capacity(value.buffers.len());
        for buffer in value.buffers {
            if buffer.metas.len() > MAX_SESSION_METAS {
                return Err(SessionError::TooManyDescriptors("metadata"));
            }
            if buffer.datas.len() != MAX_SESSION_DATAS {
                return Err(SessionError::Unsupported(UnsupportedFeature::DataPlane));
            }
            let data = buffer.datas[0];
            if DataType::try_from(data.type_id) != Ok(DataType::MemId) {
                return Err(SessionError::Unsupported(UnsupportedFeature::DataPlane));
            }
            if data.flags != 0 {
                return Err(SessionError::Unsupported(UnsupportedFeature::DataFlags(
                    data.flags,
                )));
            }
            buffers.push(BufferDescriptor {
                metadata: buffer.metadata.try_into()?,
                metas: buffer
                    .metas
                    .into_iter()
                    .map(|meta| MetaDescriptor {
                        type_id: meta.type_id,
                        size: meta.size as usize,
                    })
                    .collect(),
                media_memory: MemoryId(data.data_id),
                map_offset: data.map_offset as usize,
                max_size: data.max_size as usize,
            });
        }
        Ok(Self {
            port,
            buffers: buffers.into_boxed_slice(),
        })
    }
}

impl TryFrom<wire::PortSetIo> for PortIoUpdate {
    type Error = SessionError;

    fn try_from(value: wire::PortSetIo) -> Result<Self, Self::Error> {
        match value {
            wire::PortSetIo::Set {
                direction,
                port_id,
                mix_id,
                io_id,
                region,
            } => {
                let port = validate_port(direction, port_id, mix_id)?;
                match io_id {
                    wire::SPA_IO_BUFFERS => Ok(Self::Set(PortIoDescriptor {
                        port,
                        region: region.try_into()?,
                    })),
                    wire::SPA_IO_ASYNC_BUFFERS => {
                        Err(SessionError::Unsupported(UnsupportedFeature::AsyncIo))
                    }
                    other => Err(SessionError::Unsupported(UnsupportedFeature::IoType(other))),
                }
            }
            wire::PortSetIo::Clear {
                direction,
                port_id,
                mix_id,
                io_id,
            } => {
                validate_port(direction, port_id, mix_id)?;
                if io_id != wire::SPA_IO_BUFFERS {
                    return Err(SessionError::Unsupported(UnsupportedFeature::IoType(io_id)));
                }
                Ok(Self::Clear)
            }
        }
    }
}

impl TryFrom<wire::SetActivation> for PeerActivationUpdate {
    type Error = SessionError;

    fn try_from(value: wire::SetActivation) -> Result<Self, Self::Error> {
        match value {
            wire::SetActivation::Set {
                node_id,
                signal_fd,
                activation,
            } => Ok(Self::Set(PeerActivationDescriptor {
                node: NodeId(node_id),
                signal_fd,
                activation: activation.try_into()?,
            })),
            wire::SetActivation::Remove { node_id } => Ok(Self::Remove(NodeId(node_id))),
        }
    }
}

impl TryFrom<wire::RegionRef> for RegionRef {
    type Error = SessionError;

    fn try_from(value: wire::RegionRef) -> Result<Self, Self::Error> {
        let len = value.size as usize;
        if len == 0 || (value.offset as usize).checked_add(len).is_none() {
            return Err(SessionError::Overflow("memory region"));
        }
        Ok(Self {
            memory: MemoryId(value.memory_id),
            offset: value.offset as usize,
            len,
        })
    }
}

#[cfg(test)]
mod tests {
    use pipewire_native_protocol::wire::client_node::{
        self as wire, BufferDescriptor as WireBuffer, DataDescriptor as WireData, Direction,
        PortSetParam, PortUseBuffers, RegionRef as WireRegion,
    };
    use pipewire_native_spa::{
        buffer::data_type,
        param::{
            format::{Format, MediaSubtype, MediaType},
            ParamType,
        },
        pod::{
            builder::Builder,
            types::{Id, ObjectType, PropertyFlags},
            RawPodOwned,
        },
    };

    use super::*;

    fn format_pod(rate: i32, channels: i32, sample: u32) -> RawPodOwned {
        let mut bytes = [0; 512];
        let encoded = Builder::new(&mut bytes)
            .push_object(ObjectType::Format, ParamType::Format, |object| {
                object
                    .push_property(
                        Format::MediaType,
                        PropertyFlags::empty(),
                        Id(MediaType::Audio),
                    )
                    .push_property(
                        Format::MediaSubtype,
                        PropertyFlags::empty(),
                        Id(MediaSubtype::Raw),
                    )
                    .push_property(Format::AudioFormat, PropertyFlags::empty(), Id(sample))
                    .push_property(Format::AudioRate, PropertyFlags::empty(), rate)
                    .push_property(Format::AudioChannels, PropertyFlags::empty(), channels)
            })
            .build()
            .unwrap();
        RawPodOwned::wrap(encoded.to_vec()).unwrap()
    }

    #[test]
    fn converts_exact_s16le_format_and_rejects_invalid_values() {
        let event = PortSetParam {
            direction: Direction::Output,
            port_id: 0,
            param_id: wire::SPA_PARAM_FORMAT,
            flags: 0,
            param: Some(format_pod(48_000, 2, SPA_AUDIO_FORMAT_S16_LE)),
        };
        let format = NegotiatedAudioFormat::from_wire(&event).unwrap();
        assert_eq!(format.rate.get(), 48_000);
        assert_eq!(format.channels.len(), 2);
        assert_eq!(format.frame_stride.get(), 4);

        for (rate, channels, sample) in [
            (0, 2, SPA_AUDIO_FORMAT_S16_LE),
            (48_000, 0, SPA_AUDIO_FORMAT_S16_LE),
            (48_000, 2, 0x104),
        ] {
            let event = PortSetParam {
                param: Some(format_pod(rate, channels, sample)),
                ..event.clone()
            };
            assert!(NegotiatedAudioFormat::from_wire(&event).is_err());
        }
    }

    #[test]
    fn rejects_input_mix_alloc_and_non_memid_planes() {
        let base = PortUseBuffers {
            direction: Direction::Output,
            port_id: 0,
            mix_id: None,
            flags: 0,
            buffers: vec![WireBuffer {
                metadata: WireRegion {
                    memory_id: 1,
                    offset: 0,
                    size: 16,
                },
                metas: vec![],
                datas: vec![WireData {
                    type_id: data_type::MEM_ID,
                    data_id: 2,
                    flags: 0,
                    map_offset: 0,
                    max_size: 16,
                }],
            }],
        };
        assert!(BufferSetDescriptor::try_from(base.clone()).is_ok());
        assert!(matches!(
            BufferSetDescriptor::try_from(PortUseBuffers {
                direction: Direction::Input,
                ..base.clone()
            }),
            Err(SessionError::Unsupported(
                UnsupportedFeature::InputDirection
            ))
        ));
        assert!(matches!(
            BufferSetDescriptor::try_from(PortUseBuffers {
                mix_id: Some(1),
                ..base.clone()
            }),
            Err(SessionError::Unsupported(UnsupportedFeature::Mix(MixId(1))))
        ));
        assert!(matches!(
            BufferSetDescriptor::try_from(PortUseBuffers {
                flags: wire::SPA_NODE_BUFFERS_FLAG_ALLOC,
                ..base.clone()
            }),
            Err(SessionError::Unsupported(
                UnsupportedFeature::ClientAllocatedBuffers
            ))
        ));
        let mut wrong_plane = base;
        wrong_plane.buffers[0].datas[0].type_id = data_type::MEM_PTR;
        assert!(matches!(
            BufferSetDescriptor::try_from(wrong_plane),
            Err(SessionError::Unsupported(UnsupportedFeature::DataPlane))
        ));
    }
}
