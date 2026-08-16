//! Minimum canonical ClientNode v6 wire surface for one synchronous output cycle.
//!
//! This module owns POD conversion, wire constants, bounded repeated fields, and
//! frame-local descriptor selection. It intentionally does not own mmap, session
//! ordering, activation transitions, or process callbacks.

use std::{io, os::fd::OwnedFd};

use pipewire_native_spa::{
    self as spa,
    pod::{types::Type, RawPodOwned},
};

use crate::native::frame::FrameFds;

/// Factory used to create a ClientNode object.
pub const FACTORY_NAME: &str = "client-node";
/// PipeWire ClientNode interface name.
pub const INTERFACE: &str = "PipeWire:Interface:ClientNode";
/// Interface version implemented by this wire surface.
pub const INTERFACE_VERSION: u32 = 6;
/// ClientNode methods ABI version.
pub const METHODS_VERSION: u32 = 0;
/// ClientNode events ABI version.
pub const EVENTS_VERSION: u32 = 1;
/// Activation shared-memory ABI version selected by ClientNode v6.
pub const ACTIVATION_VERSION: u32 = 1;
/// PipeWire invalid-ID sentinel.
pub const INVALID_ID: u32 = u32::MAX;

/// Synchronous port IO used by the first output cycle.
pub const SPA_IO_BUFFERS: u32 = 1;
/// Double-buffered port IO, decoded on the wire but not processed by the first cycle.
pub const SPA_IO_ASYNC_BUFFERS: u32 = 10;

/// Exact `pw_node_activation.status` values used by ClientNode v6.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u32)]
pub enum ActivationStatus {
    /// Prepared but not yet triggered.
    NotTriggered = 0,
    /// Triggered and waiting for the client to claim the cycle.
    Triggered = 1,
    /// Claimed by the processing client.
    Awake = 2,
    /// Processing is complete.
    Finished = 3,
    /// The node is not schedulable.
    Inactive = 4,
}

/// Exact synchronous `spa_io_buffers.status` values.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(i32)]
pub enum BufferStatus {
    /// The IO area should be ignored.
    Ok = 0,
    /// The host requests output data.
    NeedData = 1,
    /// The node has published output data.
    HaveData = 2,
    /// Processing stopped because of an error.
    Stopped = 4,
    /// Previously published data was drained.
    Drained = 8,
}

/// Client-to-server method opcodes.
pub mod method {
    /// Advertise node information and parameters.
    pub const UPDATE: u8 = 2;
    /// Advertise port information and parameters.
    pub const PORT_UPDATE: u8 = 3;
    /// Join or leave graph scheduling.
    pub const SET_ACTIVE: u8 = 4;
}

/// Server-to-client event opcodes.
pub mod event {
    /// Install transport eventfds and own activation region.
    pub const TRANSPORT: u8 = 0;
    /// Deliver a node command.
    pub const COMMAND: u8 = 4;
    /// Set or clear a port parameter.
    pub const PORT_SET_PARAM: u8 = 7;
    /// Install or clear a port buffer set.
    pub const PORT_USE_BUFFERS: u8 = 8;
    /// Install or clear port IO.
    pub const PORT_SET_IO: u8 = 9;
    /// Install or remove a downstream activation target.
    pub const SET_ACTIVATION: u8 = 10;
}

/// Bounds for peer-controlled repeated ClientNode fields.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Limits {
    /// Maximum parameter PODs in one advertisement.
    pub max_params: usize,
    /// Maximum properties in one info structure.
    pub max_properties: usize,
    /// Maximum parameter-info entries in one info structure.
    pub max_param_info: usize,
    /// Maximum buffers in one configuration.
    pub max_buffers: usize,
    /// Maximum metadata descriptors per buffer.
    pub max_metas: usize,
    /// Maximum data descriptors per buffer.
    pub max_datas: usize,
    /// Maximum size of one opaque parameter POD.
    pub max_pod_bytes: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_params: 64,
            max_properties: 256,
            max_param_info: 64,
            max_buffers: 64,
            max_metas: 64,
            max_datas: 256,
            max_pod_bytes: 1024 * 1024,
        }
    }
}

/// SPA port direction.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u32)]
pub enum Direction {
    /// Input port.
    Input = 0,
    /// Output port.
    Output = 1,
}

/// A checked reference to a region of an imported Core memory object.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RegionRef {
    /// Core memory ID.
    pub memory_id: u32,
    /// Byte offset in the memory object.
    pub offset: u32,
    /// Region length in bytes.
    pub size: u32,
}

/// Parameter availability advertised in node or port info.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ParamInfo {
    /// SPA parameter ID.
    pub id: u32,
    /// SPA parameter-info flags.
    pub flags: u32,
}

/// Node-info portion of `Update`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NodeInfo {
    /// Maximum input ports.
    pub max_input_ports: u32,
    /// Maximum output ports.
    pub max_output_ports: u32,
    /// SPA node-info change mask.
    pub change_mask: u64,
    /// SPA node-info flags.
    pub flags: u64,
    /// Node properties.
    pub properties: Vec<(String, String)>,
    /// Advertised parameter information.
    pub params: Vec<ParamInfo>,
}

/// Port-info portion of `PortUpdate`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PortInfo {
    /// SPA port-info change mask.
    pub change_mask: u64,
    /// SPA port-info flags.
    pub flags: u64,
    /// Port rate numerator.
    pub rate_num: u32,
    /// Port rate denominator.
    pub rate_denom: u32,
    /// Port properties.
    pub properties: Vec<(String, String)>,
    /// Advertised parameter information.
    pub params: Vec<ParamInfo>,
}

/// ClientNode `Update` method.
#[derive(Clone)]
pub struct Update {
    /// Changed node fields.
    pub change_mask: u32,
    /// Parameter PODs advertised with the update.
    pub params: Vec<RawPodOwned>,
    /// Optional node information; `None` is the canonical clear/absent form.
    pub info: Option<NodeInfo>,
}

/// ClientNode `PortUpdate` method.
#[derive(Clone)]
pub struct PortUpdate {
    /// Port direction.
    pub direction: Direction,
    /// Port ID.
    pub port_id: u32,
    /// Changed port fields.
    pub change_mask: u32,
    /// Parameter PODs advertised with the update.
    pub params: Vec<RawPodOwned>,
    /// Optional port information; `None` is the canonical clear/absent form.
    pub info: Option<PortInfo>,
}

/// ClientNode `SetActive` method.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SetActive {
    /// Whether graph scheduling is requested.
    pub active: bool,
}

/// Selected outgoing ClientNode method.
pub enum Method {
    /// Node advertisement.
    Update(Update),
    /// Port advertisement.
    PortUpdate(PortUpdate),
    /// Scheduling request.
    SetActive(SetActive),
}

impl Method {
    /// Native-protocol opcode for this method.
    #[must_use]
    pub fn opcode(&self) -> u8 {
        match self {
            Self::Update(_) => method::UPDATE,
            Self::PortUpdate(_) => method::PORT_UPDATE,
            Self::SetActive(_) => method::SET_ACTIVE,
        }
    }
}

/// Metadata descriptor in `PortUseBuffers`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MetaDescriptor {
    /// SPA metadata type.
    pub type_id: u32,
    /// Metadata payload size.
    pub size: u32,
}

/// Data-plane descriptor in `PortUseBuffers`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DataDescriptor {
    /// SPA data type, such as MemId or MemPtr.
    pub type_id: u32,
    /// Memory ID or metadata-relative pointer bit pattern.
    pub data_id: u32,
    /// SPA data flags.
    pub flags: u32,
    /// Mapping offset.
    pub map_offset: u32,
    /// Maximum plane size.
    pub max_size: u32,
}

/// One buffer descriptor in `PortUseBuffers`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BufferDescriptor {
    /// Metadata and chunk backing region.
    pub metadata: RegionRef,
    /// Metadata payload descriptors.
    pub metas: Vec<MetaDescriptor>,
    /// Data-plane descriptors.
    pub datas: Vec<DataDescriptor>,
}

/// Transport installation with frame-owned eventfds.
#[derive(Debug)]
pub struct Transport {
    /// Eventfd used as a process wake hint.
    pub trigger_fd: OwnedFd,
    /// Retained transport completion eventfd.
    pub completion_fd: OwnedFd,
    /// Own activation shared-memory region.
    pub activation: RegionRef,
}

/// Port parameter update, including the canonical `None` clear form.
#[derive(Clone, Debug)]
pub struct PortSetParam {
    /// Port direction.
    pub direction: Direction,
    /// Port ID.
    pub port_id: u32,
    /// SPA parameter ID.
    pub param_id: u32,
    /// Parameter flags.
    pub flags: u32,
    /// Parameter object, or `None` to clear it.
    pub param: Option<RawPodOwned>,
}

/// Port buffer update, including an empty vector as the canonical clear form.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PortUseBuffers {
    /// Port direction.
    pub direction: Direction,
    /// Port ID.
    pub port_id: u32,
    /// Mix ID, with `None` representing `SPA_ID_INVALID`.
    pub mix_id: Option<u32>,
    /// Buffer configuration flags.
    pub flags: u32,
    /// Bounded buffer descriptors; empty clears the set.
    pub buffers: Vec<BufferDescriptor>,
}

/// Port IO update with an explicit clear form.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PortSetIo {
    /// Install an IO mapping.
    Set {
        /// Port direction.
        direction: Direction,
        /// Port ID.
        port_id: u32,
        /// Mix ID, with `None` representing `SPA_ID_INVALID`.
        mix_id: Option<u32>,
        /// SPA IO ID.
        io_id: u32,
        /// IO backing region.
        region: RegionRef,
    },
    /// Clear an IO mapping using `SPA_ID_INVALID, 0, 0`.
    Clear {
        /// Port direction.
        direction: Direction,
        /// Port ID.
        port_id: u32,
        /// Mix ID, with `None` representing `SPA_ID_INVALID`.
        mix_id: Option<u32>,
        /// SPA IO ID.
        io_id: u32,
    },
}

/// Downstream activation update with frame-owned signaling FD.
#[derive(Debug)]
pub enum SetActivation {
    /// Install or replace a downstream activation target.
    Set {
        /// Downstream node ID.
        node_id: u32,
        /// Eventfd used to signal the downstream node.
        signal_fd: OwnedFd,
        /// Downstream activation region.
        activation: RegionRef,
    },
    /// Remove a target using `Fd(-1), SPA_ID_INVALID, 0, 0`.
    Remove {
        /// Downstream node ID.
        node_id: u32,
    },
}

/// Selected node commands needed by the first output cycle and teardown.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u32)]
pub enum Command {
    /// Suspend and discard configuration.
    Suspend = 0,
    /// Pause scheduling.
    Pause = 1,
    /// Start scheduling.
    Start = 2,
}

/// Selected incoming ClientNode event with all descriptor indices resolved.
#[derive(Debug)]
pub enum Event {
    /// Transport installation.
    Transport(Transport),
    /// Port parameter update.
    PortSetParam(PortSetParam),
    /// Port buffer update.
    PortUseBuffers(PortUseBuffers),
    /// Port IO update.
    PortSetIo(PortSetIo),
    /// Downstream activation update.
    SetActivation(SetActivation),
    /// Node command.
    Command(Command),
}

/// Encode one selected client-to-server method payload.
pub fn encode_method(value: &Method) -> io::Result<Vec<u8>> {
    match value {
        Method::Update(value) => encode_payload(|sb| {
            let mut sb = sb
                .push_int(value.change_mask as i32)
                .push_int(value.params.len() as i32);
            for param in &value.params {
                sb = sb.push_pod(param);
            }
            if let Some(info) = &value.info {
                sb.push_struct(|sb| push_node_info(sb, info))
            } else {
                sb.push_none()
            }
        }),
        Method::PortUpdate(value) => encode_payload(|sb| {
            let mut sb = sb
                .push_int(value.direction as i32)
                .push_int(value.port_id as i32)
                .push_int(value.change_mask as i32)
                .push_int(value.params.len() as i32);
            for param in &value.params {
                sb = sb.push_pod(param);
            }
            if let Some(info) = &value.info {
                sb.push_struct(|sb| push_port_info(sb, info))
            } else {
                sb.push_none()
            }
        }),
        Method::SetActive(value) => encode_payload(|sb| sb.push_bool(value.active)),
    }
}

/// Decode one selected client-to-server method payload.
pub fn decode_method(opcode: u8, payload: &[u8], limits: Limits) -> io::Result<Method> {
    match opcode {
        method::UPDATE => parse_payload(payload, |sp| {
            let change_mask = sp.pop_int()? as u32;
            let count = count(sp.pop_int()?, limits.max_params, "params")?;
            let params = pop_pods(sp, count, limits.max_pod_bytes)?;
            let info = pop_optional_node_info(sp, limits)?;
            Ok(Method::Update(Update { change_mask, params, info }))
        }),
        method::PORT_UPDATE => parse_payload(payload, |sp| {
            let direction = direction(sp.pop_int()?)?;
            let port_id = sp.pop_int()? as u32;
            let change_mask = sp.pop_int()? as u32;
            let count = count(sp.pop_int()?, limits.max_params, "params")?;
            let params = pop_pods(sp, count, limits.max_pod_bytes)?;
            let info = pop_optional_port_info(sp, limits)?;
            Ok(Method::PortUpdate(PortUpdate {
                direction,
                port_id,
                change_mask,
                params,
                info,
            }))
        }),
        method::SET_ACTIVE => parse_payload(payload, |sp| {
            Ok(Method::SetActive(SetActive {
                active: sp.pop_bool()?,
            }))
        }),
        _ => Err(io::Error::new(
            io::ErrorKind::Unsupported,
            format!("unknown ClientNode method opcode {opcode}"),
        )),
    }
}

/// Decode one selected server-to-client event and transfer exactly its indexed FDs.
///
/// Unknown opcodes are reported as `Unsupported`; dropping the enclosing frame then
/// closes all of that frame's descriptors without affecting a later frame.
pub fn decode_event(
    opcode: u8,
    payload: &[u8],
    fds: &mut FrameFds,
    limits: Limits,
) -> io::Result<Event> {
    match opcode {
        event::TRANSPORT => {
            let (trigger, completion, activation) = parse_payload(payload, |sp| {
                let trigger = pod_fd_index(sp.pop_fd()?.0)?;
                let completion = pod_fd_index(sp.pop_fd()?.0)?;
                let activation = pop_region(sp)?;
                reject_partial_clear(activation)?;
                Ok((trigger, completion, activation))
            })?;
            require_fds(fds, &[trigger, completion])?;
            let trigger_fd = take_fd(fds, trigger)?;
            let completion_fd = take_fd(fds, completion)?;
            Ok(Event::Transport(Transport {
                trigger_fd,
                completion_fd,
                activation,
            }))
        }
        event::PORT_SET_PARAM => parse_payload(payload, |sp| {
            let direction = direction(sp.pop_int()?)?;
            let port_id = sp.pop_int()? as u32;
            let param_id = sp.pop_id::<u32>()?.0;
            if param_id != 4 {
                return Err(spa::pod::Error::Invalid(format!(
                    "unsupported first-cycle port parameter {param_id}"
                )));
            }
            let flags = sp.pop_int()? as u32;
            let pod = sp.pop_raw_pod()?;
            let param = match pod.type_() {
                Type::None => None,
                Type::Object => Some(RawPodOwned::wrap(pod.data().to_vec())?),
                type_ => return Err(spa::pod::Error::Invalid(format!("port parameter must be Object or None, got {type_:?}"))),
            };
            if param.as_ref().is_some_and(|pod| pod.total_size() > limits.max_pod_bytes) {
                return Err(spa::pod::Error::Invalid("port parameter exceeds configured byte limit".into()));
            }
            Ok(Event::PortSetParam(PortSetParam { direction, port_id, param_id, flags, param }))
        }),
        event::PORT_USE_BUFFERS => parse_payload(payload, |sp| {
            let direction = direction(sp.pop_int()?)?;
            let port_id = sp.pop_int()? as u32;
            let mix_id = optional_id(sp.pop_int()? as u32);
            let flags = sp.pop_int()? as u32;
            let n_buffers = count(sp.pop_int()?, limits.max_buffers, "buffers")?;
            let mut buffers = Vec::with_capacity(n_buffers);
            for _ in 0..n_buffers {
                let metadata = pop_region(sp)?;
                reject_partial_clear(metadata)?;
                let n_metas = count(sp.pop_int()?, limits.max_metas, "metas")?;
                let mut metas = Vec::with_capacity(n_metas);
                for _ in 0..n_metas {
                    metas.push(MetaDescriptor {
                        type_id: sp.pop_id::<u32>()?.0,
                        size: sp.pop_int()? as u32,
                    });
                }
                let n_datas = count(sp.pop_int()?, limits.max_datas, "datas")?;
                let mut datas = Vec::with_capacity(n_datas);
                for _ in 0..n_datas {
                    let data = DataDescriptor {
                        type_id: sp.pop_id::<u32>()?.0,
                        data_id: sp.pop_int()? as u32,
                        flags: sp.pop_int()? as u32,
                        map_offset: sp.pop_int()? as u32,
                        max_size: sp.pop_int()? as u32,
                    };
                    if data.max_size == 0 || data.map_offset.checked_add(data.max_size).is_none() {
                        return Err(spa::pod::Error::Invalid("invalid data-plane range".into()));
                    }
                    datas.push(data);
                }
                buffers.push(BufferDescriptor { metadata, metas, datas });
            }
            Ok(Event::PortUseBuffers(PortUseBuffers { direction, port_id, mix_id, flags, buffers }))
        }),
        event::PORT_SET_IO => parse_payload(payload, |sp| {
            let direction = direction(sp.pop_int()?)?;
            let port_id = sp.pop_int()? as u32;
            let mix_id = optional_id(sp.pop_int()? as u32);
            let io_id = sp.pop_id::<u32>()?.0;
            if io_id != 1 {
                return Err(spa::pod::Error::Invalid(format!(
                    "unsupported first-cycle port IO {io_id}"
                )));
            }
            let region = pop_region(sp)?;
            let update = if is_clear_region(region) {
                PortSetIo::Clear { direction, port_id, mix_id, io_id }
            } else {
                reject_partial_clear(region)?;
                PortSetIo::Set { direction, port_id, mix_id, io_id, region }
            };
            Ok(Event::PortSetIo(update))
        }),
        event::SET_ACTIVATION => {
            let (node_id, fd, activation) = parse_payload(payload, |sp| {
                Ok((sp.pop_int()? as u32, sp.pop_fd()?.0, pop_region(sp)?))
            })?;
            if fd == -1 && is_clear_region(activation) {
                require_fds(fds, &[])?;
                return Ok(Event::SetActivation(SetActivation::Remove { node_id }));
            }
            reject_partial_clear(activation).map_err(pod_error)?;
            let index = fd_index(fd)?;
            require_fds(fds, &[index])?;
            let signal_fd = take_fd(fds, index)?;
            Ok(Event::SetActivation(SetActivation::Set { node_id, signal_fd, activation }))
        }
        event::COMMAND => parse_payload(payload, |sp| {
            let pod = sp.pop_raw_pod()?;
            Ok(Event::Command(decode_command(pod.data())?))
        }),
        _ => Err(io::Error::new(
            io::ErrorKind::Unsupported,
            format!("unknown ClientNode event opcode {opcode}"),
        )),
    }
}

/// Encode a Transport event payload using frame-local FD indices.
pub fn encode_transport(trigger_index: u32, completion_index: u32, activation: RegionRef) -> io::Result<Vec<u8>> {
    encode_payload(|sb| sb.push_fd(trigger_index as i32).push_fd(completion_index as i32).push_int(activation.memory_id as i32).push_int(activation.offset as i32).push_int(activation.size as i32))
}

/// Encode a PortSetParam event payload.
pub fn encode_port_set_param(value: &PortSetParam) -> io::Result<Vec<u8>> {
    encode_payload(|sb| {
        let sb = sb.push_int(value.direction as i32).push_int(value.port_id as i32).push_id(spa::pod::types::Id(value.param_id)).push_int(value.flags as i32);
        if let Some(param) = &value.param { sb.push_pod(param) } else { sb.push_none() }
    })
}

/// Encode a PortUseBuffers event payload.
pub fn encode_port_use_buffers(value: &PortUseBuffers) -> io::Result<Vec<u8>> {
    encode_payload(|sb| {
        let mut sb = sb.push_int(value.direction as i32).push_int(value.port_id as i32).push_int(wire_id(value.mix_id) as i32).push_int(value.flags as i32).push_int(value.buffers.len() as i32);
        for buffer in &value.buffers {
            sb = sb.push_int(buffer.metadata.memory_id as i32).push_int(buffer.metadata.offset as i32).push_int(buffer.metadata.size as i32).push_int(buffer.metas.len() as i32);
            for meta in &buffer.metas { sb = sb.push_id(spa::pod::types::Id(meta.type_id)).push_int(meta.size as i32); }
            sb = sb.push_int(buffer.datas.len() as i32);
            for data in &buffer.datas { sb = sb.push_id(spa::pod::types::Id(data.type_id)).push_int(data.data_id as i32).push_int(data.flags as i32).push_int(data.map_offset as i32).push_int(data.max_size as i32); }
        }
        sb
    })
}

/// Encode a PortSetIo event payload, preserving the canonical clear sentinel.
pub fn encode_port_set_io(value: PortSetIo) -> io::Result<Vec<u8>> {
    let (direction, port_id, mix_id, io_id, region) = match value {
        PortSetIo::Set { direction, port_id, mix_id, io_id, region } => (direction, port_id, mix_id, io_id, region),
        PortSetIo::Clear { direction, port_id, mix_id, io_id } => (direction, port_id, mix_id, io_id, RegionRef { memory_id: INVALID_ID, offset: 0, size: 0 }),
    };
    encode_payload(|sb| sb.push_int(direction as i32).push_int(port_id as i32).push_int(wire_id(mix_id) as i32).push_id(spa::pod::types::Id(io_id)).push_int(region.memory_id as i32).push_int(region.offset as i32).push_int(region.size as i32))
}

/// Encode a SetActivation event payload using an optional frame-local FD index.
pub fn encode_set_activation(node_id: u32, value: Option<(u32, RegionRef)>) -> io::Result<Vec<u8>> {
    let (fd, region) = value.unwrap_or((-1_i32 as u32, RegionRef { memory_id: INVALID_ID, offset: 0, size: 0 }));
    encode_payload(|sb| sb.push_int(node_id as i32).push_fd(fd as i32).push_int(region.memory_id as i32).push_int(region.offset as i32).push_int(region.size as i32))
}

/// Encode one selected command event payload.
pub fn encode_command(command: Command) -> io::Result<Vec<u8>> {
    let pod = command_pod(command)?;
    encode_payload(|sb| sb.push_pod(&pod))
}

fn command_pod(command: Command) -> io::Result<RawPodOwned> {
    let mut bytes = vec![0_u8; 16];
    bytes[0..4].copy_from_slice(&8_u32.to_ne_bytes());
    bytes[4..8].copy_from_slice(&(Type::Object as u32).to_ne_bytes());
    bytes[8..12].copy_from_slice(&0x30002_u32.to_ne_bytes());
    bytes[12..16].copy_from_slice(&(command as u32).to_ne_bytes());
    RawPodOwned::wrap(bytes).map_err(pod_error)
}

fn decode_command(bytes: &[u8]) -> Result<Command, spa::pod::Error> {
    if bytes.len() != 16 || u32::from_ne_bytes(bytes[4..8].try_into().unwrap()) != Type::Object as u32 || u32::from_ne_bytes(bytes[8..12].try_into().unwrap()) != 0x30002 {
        return Err(spa::pod::Error::Invalid("command is not an exact SPA Node command object".into()));
    }
    match u32::from_ne_bytes(bytes[12..16].try_into().unwrap()) {
        0 => Ok(Command::Suspend),
        1 => Ok(Command::Pause),
        2 => Ok(Command::Start),
        id => Err(spa::pod::Error::Invalid(format!("unsupported node command {id}"))),
    }
}

fn encode_payload(build: impl FnOnce(spa::pod::builder::StructBuilder<'_>) -> spa::pod::builder::StructBuilder<'_>) -> io::Result<Vec<u8>> {
    let mut data = vec![0_u8; 16 * 1024 * 1024];
    let size = spa::pod::builder::Builder::new(&mut data).push_struct(build).build().map_err(pod_error)?.len();
    data.truncate(size);
    Ok(data)
}

fn parse_payload<T>(payload: &[u8], parse: impl FnOnce(&mut spa::pod::parser::Parser<'_>) -> Result<T, spa::pod::Error>) -> io::Result<T> {
    let mut outer = spa::pod::parser::Parser::new(payload);
    let (value, size) = outer.pop_struct(|sp| {
        let value = parse(sp)?;
        if sp.available() != 0 { return Err(spa::pod::Error::Invalid(format!("{} trailing bytes in ClientNode struct", sp.available()))); }
        Ok(value)
    }).map_err(pod_error)?;
    if size != payload.len() { return Err(io::Error::new(io::ErrorKind::InvalidData, "trailing POD after ClientNode message struct")); }
    Ok(value)
}

fn push_node_info<'a>(sb: spa::pod::builder::StructBuilder<'a>, info: &NodeInfo) -> spa::pod::builder::StructBuilder<'a> {
    push_info_tail(sb.push_int(info.max_input_ports as i32).push_int(info.max_output_ports as i32).push_long(info.change_mask as i64).push_long(info.flags as i64), &info.properties, &info.params)
}

fn push_port_info<'a>(sb: spa::pod::builder::StructBuilder<'a>, info: &PortInfo) -> spa::pod::builder::StructBuilder<'a> {
    push_info_tail(sb.push_long(info.change_mask as i64).push_long(info.flags as i64).push_int(info.rate_num as i32).push_int(info.rate_denom as i32), &info.properties, &info.params)
}

fn push_info_tail<'a>(mut sb: spa::pod::builder::StructBuilder<'a>, properties: &[(String, String)], params: &[ParamInfo]) -> spa::pod::builder::StructBuilder<'a> {
    sb = sb.push_int(properties.len() as i32);
    for (key, value) in properties { sb = sb.push_string(key).push_string(value); }
    sb = sb.push_int(params.len() as i32);
    for param in params { sb = sb.push_id(spa::pod::types::Id(param.id)).push_int(param.flags as i32); }
    sb
}

fn pop_optional_node_info(sp: &mut spa::pod::parser::Parser<'_>, limits: Limits) -> Result<Option<NodeInfo>, spa::pod::Error> {
    let pod = sp.pop_raw_pod()?;
    match pod.type_() {
        Type::None => Ok(None),
        Type::Struct => {
            let mut parser = spa::pod::parser::Parser::new(pod.data());
            parser.pop_struct(|sp| {
                let max_input_ports = sp.pop_int()? as u32;
                let max_output_ports = sp.pop_int()? as u32;
                let change_mask = sp.pop_long()? as u64;
                let flags = sp.pop_long()? as u64;
                let (properties, params) = pop_info_tail(sp, limits)?;
                if sp.available() != 0 { return Err(spa::pod::Error::Invalid("trailing node info fields".into())); }
                Ok(NodeInfo { max_input_ports, max_output_ports, change_mask, flags, properties, params })
            }).map(|v| Some(v.0))
        }
        type_ => Err(spa::pod::Error::Invalid(format!("node info must be Struct or None, got {type_:?}"))),
    }
}

fn pop_optional_port_info(sp: &mut spa::pod::parser::Parser<'_>, limits: Limits) -> Result<Option<PortInfo>, spa::pod::Error> {
    let pod = sp.pop_raw_pod()?;
    match pod.type_() {
        Type::None => Ok(None),
        Type::Struct => {
            let mut parser = spa::pod::parser::Parser::new(pod.data());
            parser.pop_struct(|sp| {
                let change_mask = sp.pop_long()? as u64;
                let flags = sp.pop_long()? as u64;
                let rate_num = sp.pop_int()? as u32;
                let rate_denom = sp.pop_int()? as u32;
                let (properties, params) = pop_info_tail(sp, limits)?;
                if sp.available() != 0 { return Err(spa::pod::Error::Invalid("trailing port info fields".into())); }
                Ok(PortInfo { change_mask, flags, rate_num, rate_denom, properties, params })
            }).map(|v| Some(v.0))
        }
        type_ => Err(spa::pod::Error::Invalid(format!("port info must be Struct or None, got {type_:?}"))),
    }
}

fn pop_info_tail(sp: &mut spa::pod::parser::Parser<'_>, limits: Limits) -> Result<(Vec<(String, String)>, Vec<ParamInfo>), spa::pod::Error> {
    let n_properties = count(sp.pop_int()?, limits.max_properties, "properties")?;
    let mut properties = Vec::with_capacity(n_properties);
    for _ in 0..n_properties { properties.push((sp.pop_string()?, sp.pop_string()?)); }
    let n_params = count(sp.pop_int()?, limits.max_param_info, "param info")?;
    let mut params = Vec::with_capacity(n_params);
    for _ in 0..n_params { params.push(ParamInfo { id: sp.pop_id::<u32>()?.0, flags: sp.pop_int()? as u32 }); }
    Ok((properties, params))
}

fn pop_pods(sp: &mut spa::pod::parser::Parser<'_>, count: usize, max_bytes: usize) -> Result<Vec<RawPodOwned>, spa::pod::Error> {
    let mut pods = Vec::with_capacity(count);
    for _ in 0..count {
        let pod = sp.pop_raw_pod()?;
        if pod.total_size() > max_bytes { return Err(spa::pod::Error::Invalid("parameter POD exceeds configured byte limit".into())); }
        pods.push(RawPodOwned::wrap(pod.data().to_vec())?);
    }
    Ok(pods)
}

fn pop_region(sp: &mut spa::pod::parser::Parser<'_>) -> Result<RegionRef, spa::pod::Error> {
    Ok(RegionRef { memory_id: sp.pop_int()? as u32, offset: sp.pop_int()? as u32, size: sp.pop_int()? as u32 })
}

fn direction(value: i32) -> Result<Direction, spa::pod::Error> {
    match value { 0 => Ok(Direction::Input), 1 => Ok(Direction::Output), _ => Err(spa::pod::Error::Invalid(format!("invalid SPA direction {value}"))) }
}

fn count(value: i32, max: usize, name: &str) -> Result<usize, spa::pod::Error> {
    let value = usize::try_from(value).map_err(|_| spa::pod::Error::Invalid(format!("negative {name} count")))?;
    if value > max { return Err(spa::pod::Error::Invalid(format!("{name} count {value} exceeds limit {max}"))); }
    Ok(value)
}

fn fd_index(value: i32) -> io::Result<u32> {
    u32::try_from(value).map_err(|_| io::Error::new(io::ErrorKind::InvalidData, format!("negative frame FD index {value}")))
}

fn pod_fd_index(value: i32) -> Result<u32, spa::pod::Error> {
    u32::try_from(value)
        .map_err(|_| spa::pod::Error::Invalid(format!("negative frame FD index {value}")))
}

fn require_fds(fds: &FrameFds, indices: &[u32]) -> io::Result<()> {
    if fds.len() != indices.len() { return Err(io::Error::new(io::ErrorKind::InvalidData, format!("ClientNode event references {} descriptors but frame carries {}", indices.len(), fds.len()))); }
    for (position, index) in indices.iter().enumerate() {
        if indices[..position].contains(index) { return Err(io::Error::new(io::ErrorKind::InvalidData, format!("descriptor index {index} is referenced more than once"))); }
        fds.get(*index).map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    }
    Ok(())
}

fn take_fd(fds: &mut FrameFds, index: u32) -> io::Result<OwnedFd> {
    fds.take(index).map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
}

fn optional_id(value: u32) -> Option<u32> { (value != INVALID_ID).then_some(value) }
fn wire_id(value: Option<u32>) -> u32 { value.unwrap_or(INVALID_ID) }
fn is_clear_region(region: RegionRef) -> bool { region.memory_id == INVALID_ID && region.offset == 0 && region.size == 0 }

fn reject_partial_clear(region: RegionRef) -> Result<(), spa::pod::Error> {
    if region.memory_id == INVALID_ID || region.size == 0 { return Err(spa::pod::Error::Invalid("invalid partial region-clear sentinel".into())); }
    region.offset.checked_add(region.size).ok_or_else(|| spa::pod::Error::Invalid("region range overflows u32".into()))?;
    Ok(())
}

fn pod_error(error: spa::pod::Error) -> io::Error { io::Error::new(io::ErrorKind::InvalidData, format!("ClientNode POD codec error: {error:?}")) }
