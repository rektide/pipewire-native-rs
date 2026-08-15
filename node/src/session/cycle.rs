// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: Copyright (c) 2026 Asymptotic Inc.

//! Cycle-scoped selection and publication for synchronous output buffers.

use std::fmt;

use super::port::{
    BufferStatus, BuffersIoState, BuffersIoView, ChunkFlags, OutputBuffer, PortError,
};

/// Failure to select or publish one output buffer cycle.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CycleError {
    /// PipeWire selected a buffer index outside the configured set.
    InvalidBufferId {
        /// Selected index.
        buffer_id: u32,
        /// Number of configured buffers.
        buffer_count: usize,
    },
    /// The committed byte count exceeds the contiguous checked capacity.
    CapacityExceeded {
        /// Requested committed bytes.
        requested: usize,
        /// Available contiguous bytes.
        capacity: usize,
    },
    /// A byte count cannot be represented by public `spa_chunk.size`.
    ChunkSizeOverflow(usize),
    /// Port/chunk validation failed.
    Port(PortError),
}

impl fmt::Display for CycleError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

impl std::error::Error for CycleError {}

impl From<PortError> for CycleError {
    fn from(value: PortError) -> Self {
        Self::Port(value)
    }
}

/// Minimum synchronous output publication owner for one configured buffer set.
#[derive(Debug)]
pub struct OutputBufferPublisher<'io, 'buffers, 'region> {
    io: BuffersIoView<'io>,
    buffers: &'buffers mut [OutputBuffer<'region>],
}

impl<'io, 'buffers, 'region> OutputBufferPublisher<'io, 'buffers, 'region> {
    /// Associates synchronous IO with its configured buffers.
    pub fn new(io: BuffersIoView<'io>, buffers: &'buffers mut [OutputBuffer<'region>]) -> Self {
        Self { io, buffers }
    }

    /// Returns a volatile snapshot of the current IO selection.
    pub fn io_state(&self) -> BuffersIoState {
        self.io.state()
    }

    /// Selects exactly the buffer requested by synchronous `NEED_DATA` IO.
    ///
    /// A status other than `NEED_DATA` yields `Ok(None)` and exposes no media.
    pub fn select(&mut self) -> Result<Option<OutputCycle<'_, 'io, 'region>>, CycleError> {
        let state = self.io.state();
        if state.status != BufferStatus::NeedData as i32 {
            return Ok(None);
        }
        let index = state.buffer_id as usize;
        let buffer_count = self.buffers.len();
        let buffer = self
            .buffers
            .get_mut(index)
            .ok_or(CycleError::InvalidBufferId {
                buffer_id: state.buffer_id,
                buffer_count,
            })?;
        Ok(Some(OutputCycle {
            io: &self.io,
            buffer,
            buffer_id: state.buffer_id,
        }))
    }
}

/// Exclusive, callback-scoped access to one server-selected output media plane.
#[derive(Debug)]
pub struct OutputCycle<'cycle, 'io, 'region> {
    io: &'cycle BuffersIoView<'io>,
    buffer: &'cycle mut OutputBuffer<'region>,
    buffer_id: u32,
}

impl OutputCycle<'_, '_, '_> {
    /// Returns the selected configured buffer index.
    pub const fn buffer_id(&self) -> u32 {
        self.buffer_id
    }

    /// Returns the exact contiguous capacity available at this chunk's ring offset.
    pub const fn capacity(&self) -> usize {
        self.buffer.capacity()
    }

    /// Borrows only the checked media capacity for this cycle.
    pub fn media(&mut self) -> &mut [u8] {
        self.buffer.media_capacity()
    }

    /// Publishes the chunk first, then buffer ID, then `HAVE_DATA` status.
    ///
    /// The caller must complete the owning activation only after this returns. That
    /// activation transition is the synchronization edge that lets PipeWire consume
    /// the volatile chunk and IO writes.
    pub fn publish(
        self,
        bytes_used: usize,
        stride: usize,
        flags: ChunkFlags,
    ) -> Result<PublishedOutput, CycleError> {
        let capacity = self.buffer.capacity();
        if bytes_used > capacity {
            return Err(CycleError::CapacityExceeded {
                requested: bytes_used,
                capacity,
            });
        }
        u32::try_from(bytes_used).map_err(|_| CycleError::ChunkSizeOverflow(bytes_used))?;
        self.buffer.publish(bytes_used, stride, flags)?;
        std::sync::atomic::fence(std::sync::atomic::Ordering::Release);
        self.io.publish_have_data(self.buffer_id);
        Ok(PublishedOutput {
            buffer_id: self.buffer_id,
            bytes_used,
        })
    }
}

/// Proof that chunk and synchronous IO publication completed successfully.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PublishedOutput {
    buffer_id: u32,
    bytes_used: usize,
}

impl PublishedOutput {
    /// Returns the published configured buffer index.
    pub const fn buffer_id(self) -> u32 {
        self.buffer_id
    }

    /// Returns the published media byte count.
    pub const fn bytes_used(self) -> usize {
        self.bytes_used
    }
}

#[cfg(test)]
mod tests {
    use super::{CycleError, OutputBufferPublisher};
    use crate::session::port::{
        BufferStatus, BuffersIoView, ChunkFlags, ChunkState, OutputBuffer, PortIoType,
    };

    #[repr(C, align(4))]
    struct Aligned<const N: usize>([u8; N]);

    fn write_i32(bytes: &mut [u8], offset: usize, value: i32) {
        unsafe { bytes.as_mut_ptr().add(offset).cast::<i32>().write(value) };
    }

    fn write_u32(bytes: &mut [u8], offset: usize, value: u32) {
        unsafe { bytes.as_mut_ptr().add(offset).cast::<u32>().write(value) };
    }

    #[test]
    fn selects_one_buffer_and_publishes_chunk_then_io() {
        let mut io = Aligned([0; 8]);
        write_i32(&mut io.0, 0, BufferStatus::NeedData as i32);
        write_u32(&mut io.0, 4, 1);

        let mut chunks = [Aligned([0; 16]), Aligned([0; 16])];
        write_u32(&mut chunks[1].0, 0, 4);
        let mut media0 = [0_u8; 16];
        let mut media1 = [0_u8; 24];
        let io_view = unsafe {
            BuffersIoView::from_raw_parts(PortIoType::Buffers, io.0.as_mut_ptr(), 8).unwrap()
        };
        let buffer0 = unsafe {
            OutputBuffer::from_raw_parts(
                chunks[0].0.as_mut_ptr(),
                16,
                media0.as_mut_ptr(),
                media0.len(),
                0,
                16,
            )
            .unwrap()
        };
        let buffer1 = unsafe {
            OutputBuffer::from_raw_parts(
                chunks[1].0.as_mut_ptr(),
                16,
                media1.as_mut_ptr(),
                media1.len(),
                8,
                16,
            )
            .unwrap()
        };
        let mut buffers = [buffer0, buffer1];
        {
            let mut publisher = OutputBufferPublisher::new(io_view, &mut buffers);
            let mut cycle = publisher.select().unwrap().unwrap();
            assert_eq!(cycle.buffer_id(), 1);
            assert_eq!(cycle.capacity(), 12);
            cycle.media()[..8].copy_from_slice(&[1, 2, 3, 4, 5, 6, 7, 8]);
            let published = cycle.publish(8, 4, ChunkFlags::NONE).unwrap();
            assert_eq!(published.buffer_id(), 1);
            assert_eq!(published.bytes_used(), 8);
            assert_eq!(publisher.io_state().status, BufferStatus::HaveData as i32);
            assert_eq!(publisher.io_state().buffer_id, 1);
        }
        assert_eq!(
            buffers[1].chunk_state(),
            ChunkState {
                offset: 4,
                size: 8,
                stride: 4,
                flags: 0,
            }
        );
        assert_eq!(&media1[12..20], &[1, 2, 3, 4, 5, 6, 7, 8]);
        assert_eq!(media0, [0; 16]);
    }

    #[test]
    fn does_not_expose_media_without_need_data_and_rejects_bad_selection() {
        let mut io = Aligned([0; 8]);
        let io_view = unsafe {
            BuffersIoView::from_raw_parts(PortIoType::Buffers, io.0.as_mut_ptr(), 8).unwrap()
        };
        let mut buffers = [];
        {
            let mut publisher = OutputBufferPublisher::new(io_view, &mut buffers);
            assert!(publisher.select().unwrap().is_none());
        }
        write_i32(&mut io.0, 0, BufferStatus::NeedData as i32);
        write_u32(&mut io.0, 4, 7);
        let io_view = unsafe {
            BuffersIoView::from_raw_parts(PortIoType::Buffers, io.0.as_mut_ptr(), 8).unwrap()
        };
        let mut publisher = OutputBufferPublisher::new(io_view, &mut buffers);
        assert_eq!(
            publisher.select().unwrap_err(),
            CycleError::InvalidBufferId {
                buffer_id: 7,
                buffer_count: 0,
            }
        );
    }

    #[test]
    fn rejects_publication_beyond_checked_capacity() {
        let mut io = Aligned([0; 8]);
        write_i32(&mut io.0, 0, BufferStatus::NeedData as i32);
        let mut chunk = Aligned([0; 16]);
        write_u32(&mut chunk.0, 0, 6);
        let mut media = [0_u8; 8];
        let io_view = unsafe {
            BuffersIoView::from_raw_parts(PortIoType::Buffers, io.0.as_mut_ptr(), 8).unwrap()
        };
        let buffer = unsafe {
            OutputBuffer::from_raw_parts(
                chunk.0.as_mut_ptr(),
                16,
                media.as_mut_ptr(),
                media.len(),
                0,
                8,
            )
            .unwrap()
        };
        let mut buffers = [buffer];
        let mut publisher = OutputBufferPublisher::new(io_view, &mut buffers);
        let cycle = publisher.select().unwrap().unwrap();
        assert_eq!(
            cycle.publish(3, 1, ChunkFlags::NONE).unwrap_err(),
            CycleError::CapacityExceeded {
                requested: 3,
                capacity: 2,
            }
        );
        assert_eq!(publisher.io_state().status, BufferStatus::NeedData as i32);
    }
}
