// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: Copyright (c) 2026 Asymptotic Inc.

//! Transactional format, buffer, and synchronous IO generations.

use super::{
    config::{BufferSetDescriptor, NegotiatedAudioFormat, PortIoDescriptor},
    cycle::{CommittedOutput, OutputBufferPublisher, OutputCycle, PublishedOutput},
    error::SessionError,
    memory::{MemoryInterval, MemoryKey, MemoryMapping, MemoryResolver},
    port::{BufferStatus, BuffersIoView, OutputBuffer, PortIoType},
};

const CHUNK_SIZE: usize = 16;
const IO_SIZE: usize = 8;

#[derive(Debug)]
struct BoundBuffer {
    metadata_key: MemoryKey,
    metadata: MemoryMapping,
    chunk_offset: usize,
    media_key: MemoryKey,
    media: MemoryMapping,
}

/// Fully bound output format, buffers, and synchronous IO.
#[derive(Debug)]
pub struct OutputGeneration {
    id: u64,
    format: NegotiatedAudioFormat,
    io_key: MemoryKey,
    io: MemoryMapping,
    buffers: Box<[BoundBuffer]>,
}

impl OutputGeneration {
    /// Binds a complete candidate and rejects aliasing writable intervals.
    pub fn bind(
        id: u64,
        format: NegotiatedAudioFormat,
        buffers: &BufferSetDescriptor,
        io: PortIoDescriptor,
        memory: &impl MemoryResolver,
    ) -> Result<Self, SessionError> {
        if buffers.buffers.is_empty() {
            return Err(SessionError::NotReady("empty output buffer set"));
        }
        let io_key = memory.resolve(io.region.memory)?;
        let mut io_mapping = memory.map(io_key, io.region.offset, io.region.len, true)?;
        unsafe {
            BuffersIoView::from_raw_parts(
                PortIoType::Buffers,
                io_mapping.as_mut_ptr(),
                io_mapping.len(),
            )?
        };
        if io.region.len != IO_SIZE {
            return Err(SessionError::NotReady("invalid Buffers IO size"));
        }

        let mut intervals = vec![io_mapping.interval()];
        let mut bound = Vec::with_capacity(buffers.buffers.len());
        for descriptor in &buffers.buffers {
            let chunk_offset = descriptor.metas.iter().try_fold(0usize, |offset, meta| {
                offset
                    .checked_add(meta.size)
                    .and_then(|end| end.checked_add(7))
                    .map(|end| end & !7)
                    .ok_or(SessionError::Overflow("metadata layout"))
            })?;
            let chunk_end = chunk_offset
                .checked_add(CHUNK_SIZE)
                .ok_or(SessionError::Overflow("chunk layout"))?;
            if chunk_end > descriptor.metadata.len {
                return Err(SessionError::NotReady(
                    "metadata region does not contain chunk",
                ));
            }
            if descriptor.max_size == 0 {
                return Err(SessionError::NotReady("zero media capacity"));
            }
            let metadata_key = memory.resolve(descriptor.metadata.memory)?;
            let media_key = memory.resolve(descriptor.media_memory)?;
            let mut metadata = memory.map(
                metadata_key,
                descriptor.metadata.offset,
                descriptor.metadata.len,
                true,
            )?;
            let mut media =
                memory.map(media_key, descriptor.map_offset, descriptor.max_size, true)?;
            intervals.push(metadata.interval());
            intervals.push(media.interval());
            reject_overlaps(&intervals)?;
            unsafe {
                OutputBuffer::from_raw_parts(
                    metadata.as_mut_ptr().add(chunk_offset),
                    CHUNK_SIZE,
                    media.as_mut_ptr(),
                    media.len(),
                    0,
                    descriptor.max_size,
                )?;
            }
            bound.push(BoundBuffer {
                metadata_key,
                metadata,
                chunk_offset,
                media_key,
                media,
            });
        }
        Ok(Self {
            id,
            format,
            io_key,
            io: io_mapping,
            buffers: bound.into_boxed_slice(),
        })
    }

    /// Output configuration generation.
    pub const fn id(&self) -> u64 {
        self.id
    }

    /// Negotiated format.
    pub const fn format(&self) -> &NegotiatedAudioFormat {
        &self.format
    }

    /// Returns whether this generation pins a numeric memory ID.
    pub fn depends_on(&self, key: MemoryKey) -> bool {
        self.io_key == key
            || self
                .buffers
                .iter()
                .any(|buffer| buffer.metadata_key == key || buffer.media_key == key)
    }

    pub(crate) fn process(
        &mut self,
        callback: &mut dyn OutputProcess,
    ) -> Result<(Option<PublishedOutput>, i32), SessionError> {
        let io = unsafe {
            BuffersIoView::from_raw_parts(PortIoType::Buffers, self.io.as_mut_ptr(), self.io.len())?
        };
        let mut output_buffers = Vec::with_capacity(self.buffers.len());
        for buffer in &mut self.buffers {
            output_buffers.push(unsafe {
                OutputBuffer::from_raw_parts(
                    buffer.metadata.as_mut_ptr().add(buffer.chunk_offset),
                    CHUNK_SIZE,
                    buffer.media.as_mut_ptr(),
                    buffer.media.len(),
                    0,
                    buffer.media.len(),
                )?
            });
        }
        let mut publisher = OutputBufferPublisher::new(io, &mut output_buffers, &self.format);
        let Some(cycle) = publisher.select()? else {
            return Ok((None, BufferStatus::Ok as i32));
        };
        let committed = callback
            .process(cycle)
            .map_err(|error| SessionError::Callback(error.message))?;
        let published = publisher.publish_committed(committed)?;
        Ok((Some(published), BufferStatus::HaveData as i32))
    }

    pub(crate) fn intervals(&self) -> impl Iterator<Item = MemoryInterval> + '_ {
        std::iter::once(self.io.interval()).chain(
            self.buffers.iter().flat_map(|buffer| {
                [buffer.metadata.interval(), buffer.media.interval()]
            }),
        )
    }
}

/// Application callback failure.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProcessError {
    /// Diagnostic message retained by the session error.
    pub message: String,
}

/// Runtime-independent output callback invoked only after an activation claim.
pub trait OutputProcess {
    /// Fill and commit the server-selected output cycle.
    fn process(&mut self, cycle: OutputCycle<'_>) -> Result<CommittedOutput, ProcessError>;
}

fn reject_overlaps(intervals: &[MemoryInterval]) -> Result<(), SessionError> {
    for (index, left) in intervals.iter().enumerate() {
        for right in &intervals[index + 1..] {
            if left.overlaps(*right) {
                return Err(SessionError::InvalidTransition(
                    "overlapping writable memory regions",
                ));
            }
        }
    }
    Ok(())
}
