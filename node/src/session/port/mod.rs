// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: Copyright (c) 2026 Asymptotic Inc.

//! Field-specific views over public SPA output-buffer shared-memory records.

use std::{cell::UnsafeCell, fmt, marker::PhantomData, ptr::NonNull};

pub use pipewire_native_protocol::wire::client_node::{BufferStatus, PortIoType};

#[cfg(not(all(target_arch = "x86_64", target_os = "linux", target_env = "gnu")))]
compile_error!(
    "pipewire-native-node SPA buffer ABI is currently supported only on x86_64-unknown-linux-gnu; add and differentially verify a target-specific ABI table before enabling another target"
);

#[path = "abi/x86_64_unknown_linux_gnu.rs"]
mod abi;

/// One volatile snapshot of synchronous buffer IO.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BuffersIoState {
    /// Raw SPA status, including possible negative errno values.
    pub status: i32,
    /// Selected configured buffer index.
    pub buffer_id: u32,
}

/// One volatile snapshot of `spa_chunk`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ChunkState {
    /// Ring offset of valid data.
    pub offset: u32,
    /// Number of valid bytes.
    pub size: u32,
    /// Media stride in bytes.
    pub stride: i32,
    /// Raw `SPA_CHUNK_FLAG_*` bits.
    pub flags: u32,
}

/// Checked chunk flags for output publication.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ChunkFlags(u32);

impl ChunkFlags {
    /// No chunk flags.
    pub const NONE: Self = Self(0);
    /// The chunk contents are corrupted.
    pub const CORRUPTED: Self = Self(1 << 0);
    /// The chunk contains media-specific neutral data.
    pub const EMPTY: Self = Self(1 << 1);

    /// Creates flags while preserving any public SPA extension bits.
    pub const fn from_bits(bits: u32) -> Self {
        Self(bits)
    }

    /// Returns the underlying SPA flag bits.
    pub const fn bits(self) -> u32 {
        self.0
    }
}

/// Failure to construct or use SPA buffer shared-memory views.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PortError {
    /// A raw region pointer was null.
    Null(&'static str),
    /// A shared record did not have its exact public ABI size.
    InvalidSize {
        /// Record being validated.
        record: &'static str,
        /// Supplied size.
        actual: usize,
        /// Required size.
        required: usize,
    },
    /// A shared record did not meet its public ABI alignment.
    Misaligned {
        /// Record being validated.
        record: &'static str,
        /// Supplied address.
        address: usize,
        /// Required alignment.
        required: usize,
    },
    /// Async buffer IO requires separate cycle-slot semantics.
    AsyncBuffersUnsupported,
    /// The media mapping is too large for Rust pointer arithmetic.
    MediaRegionTooLarge,
    /// A data plane declared an empty capacity.
    ZeroMaxSize,
    /// `maxsize` cannot be represented by public `spa_data.maxsize`/`spa_chunk` fields.
    MaxSizeOutOfRange(usize),
    /// `mapoffset + maxsize` overflowed.
    MediaRangeOverflow,
    /// The declared data plane lies outside its mapping.
    MediaRangeOutOfBounds {
        /// End of the requested plane.
        end: usize,
        /// Available mapping size.
        available: usize,
    },
    /// The current chunk wraps past the end of the one-plane first slice.
    WrappedChunk {
        /// Offset modulo maxsize.
        offset: usize,
        /// Current chunk byte count.
        size: usize,
        /// Declared plane capacity.
        max_size: usize,
    },
    /// The chunk stride cannot describe this plane.
    InvalidStride {
        /// Shared stride value.
        stride: i32,
        /// Declared plane capacity.
        max_size: usize,
    },
}

impl fmt::Display for PortError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

impl std::error::Error for PortError {}

/// ABI-checked view over one synchronous `spa_io_buffers` record.
#[derive(Debug)]
pub struct BuffersIoView<'a> {
    base: NonNull<u8>,
    _region: PhantomData<&'a UnsafeCell<[u8]>>,
}

// SAFETY: moving the raw field view does not access memory. Unsafe construction
// requires the caller to transfer protocol ownership with the view.
unsafe impl Send for BuffersIoView<'_> {}

impl<'a> BuffersIoView<'a> {
    /// Constructs a synchronous buffer IO view.
    ///
    /// # Safety
    ///
    /// `base..base + len` must remain mapped, readable, and writable for `'a` and
    /// contain a live IO record of `kind`. The caller must enforce SPA's process-cycle
    /// synchronization and must not form ordinary references over these bytes while
    /// a foreign participant may access them.
    pub unsafe fn from_raw_parts(
        kind: PortIoType,
        base: *mut u8,
        len: usize,
    ) -> Result<Self, PortError> {
        if kind == PortIoType::AsyncBuffers {
            return Err(PortError::AsyncBuffersUnsupported);
        }
        let base = checked_record(
            base,
            len,
            abi::IO_BUFFERS_SIZE,
            abi::IO_BUFFERS_ALIGN,
            "spa_io_buffers",
        )?;
        Ok(Self {
            base,
            _region: PhantomData,
        })
    }

    /// Volatile-loads both externally mutable fields.
    pub fn state(&self) -> BuffersIoState {
        BuffersIoState {
            status: unsafe { self.read_i32(abi::IO_STATUS) },
            buffer_id: unsafe { self.read_u32(abi::IO_BUFFER_ID) },
        }
    }

    pub(crate) fn publish_have_data(&self, buffer_id: u32) {
        // The id must become visible before HAVE_DATA. The activation finish CAS
        // performed by the session supplies the inter-process release edge.
        unsafe { self.write_u32(abi::IO_BUFFER_ID, buffer_id) };
        std::sync::atomic::fence(std::sync::atomic::Ordering::Release);
        unsafe { self.write_i32(abi::IO_STATUS, BufferStatus::HaveData as i32) };
    }

    unsafe fn read_i32(&self, offset: usize) -> i32 {
        unsafe { self.base.as_ptr().add(offset).cast::<i32>().read_volatile() }
    }

    unsafe fn read_u32(&self, offset: usize) -> u32 {
        unsafe { self.base.as_ptr().add(offset).cast::<u32>().read_volatile() }
    }

    unsafe fn write_i32(&self, offset: usize, value: i32) {
        unsafe {
            self.base
                .as_ptr()
                .add(offset)
                .cast::<i32>()
                .write_volatile(value)
        };
    }

    unsafe fn write_u32(&self, offset: usize, value: u32) {
        unsafe {
            self.base
                .as_ptr()
                .add(offset)
                .cast::<u32>()
                .write_volatile(value)
        };
    }
}

/// ABI-checked view over one `spa_chunk` record.
#[derive(Debug)]
pub struct ChunkView<'a> {
    base: NonNull<u8>,
    _region: PhantomData<&'a UnsafeCell<[u8]>>,
}

unsafe impl Send for ChunkView<'_> {}

impl<'a> ChunkView<'a> {
    /// Constructs a chunk field view over externally shared memory.
    ///
    /// # Safety
    ///
    /// `base..base + len` must remain mapped, readable, and writable for `'a`, must
    /// contain one live `spa_chunk`, and must be governed by exclusive SPA cycle
    /// ownership. No broad Rust reference may be formed over concurrently shared
    /// chunk bytes.
    pub unsafe fn from_raw_parts(base: *mut u8, len: usize) -> Result<Self, PortError> {
        let base = checked_record(base, len, abi::CHUNK_SIZE, abi::CHUNK_ALIGN, "spa_chunk")?;
        Ok(Self {
            base,
            _region: PhantomData,
        })
    }

    /// Volatile-loads all externally mutable chunk fields.
    pub fn state(&self) -> ChunkState {
        ChunkState {
            offset: unsafe { self.read_u32(abi::CHUNK_OFFSET) },
            size: unsafe { self.read_u32(abi::CHUNK_DATA_SIZE) },
            stride: unsafe { self.read_i32(abi::CHUNK_STRIDE) },
            flags: unsafe { self.read_u32(abi::CHUNK_FLAGS) },
        }
    }

    pub(crate) fn publish(&self, state: ChunkState) {
        unsafe {
            self.write_u32(abi::CHUNK_OFFSET, state.offset);
            self.write_u32(abi::CHUNK_DATA_SIZE, state.size);
            self.write_i32(abi::CHUNK_STRIDE, state.stride);
            self.write_u32(abi::CHUNK_FLAGS, state.flags);
        }
    }

    unsafe fn read_i32(&self, offset: usize) -> i32 {
        unsafe { self.base.as_ptr().add(offset).cast::<i32>().read_volatile() }
    }

    unsafe fn read_u32(&self, offset: usize) -> u32 {
        unsafe { self.base.as_ptr().add(offset).cast::<u32>().read_volatile() }
    }

    unsafe fn write_i32(&self, offset: usize, value: i32) {
        unsafe {
            self.base
                .as_ptr()
                .add(offset)
                .cast::<i32>()
                .write_volatile(value)
        };
    }

    unsafe fn write_u32(&self, offset: usize, value: u32) {
        unsafe {
            self.base
                .as_ptr()
                .add(offset)
                .cast::<u32>()
                .write_volatile(value)
        };
    }
}

/// One checked writable output data plane and its separately shared chunk.
#[derive(Debug)]
pub struct OutputBuffer<'a> {
    chunk: ChunkView<'a>,
    media: NonNull<u8>,
    max_size: usize,
    data_offset: usize,
    _media: PhantomData<&'a mut [u8]>,
}

unsafe impl Send for OutputBuffer<'_> {}

impl<'a> OutputBuffer<'a> {
    /// Binds a chunk to one writable media plane.
    ///
    /// # Safety
    ///
    /// Both regions must remain mapped for `'a` and their backing objects must not
    /// shrink. The media plane must be ordinary byte-addressable memory. The caller
    /// must prevent another `OutputBuffer` from representing the same media bytes and
    /// must use this value only during an already-claimed SPA process cycle in which
    /// the `NEED_DATA` selection grants exclusive writable access. The chunk region
    /// must satisfy [`ChunkView::from_raw_parts`]'s contract and not overlap the media
    /// plane.
    pub unsafe fn from_raw_parts(
        chunk_base: *mut u8,
        chunk_len: usize,
        media_base: *mut u8,
        media_len: usize,
        map_offset: usize,
        max_size: usize,
    ) -> Result<Self, PortError> {
        let chunk = unsafe { ChunkView::from_raw_parts(chunk_base, chunk_len)? };
        let media_base = NonNull::new(media_base).ok_or(PortError::Null("media plane"))?;
        if media_len > isize::MAX as usize {
            return Err(PortError::MediaRegionTooLarge);
        }
        if max_size == 0 {
            return Err(PortError::ZeroMaxSize);
        }
        if max_size > u32::MAX as usize {
            return Err(PortError::MaxSizeOutOfRange(max_size));
        }
        let end = map_offset
            .checked_add(max_size)
            .ok_or(PortError::MediaRangeOverflow)?;
        if end > media_len {
            return Err(PortError::MediaRangeOutOfBounds {
                end,
                available: media_len,
            });
        }

        let state = chunk.state();
        let data_offset = state.offset as usize % max_size;
        let size = state.size as usize;
        if size > max_size - data_offset {
            return Err(PortError::WrappedChunk {
                offset: data_offset,
                size,
                max_size,
            });
        }
        if state.stride < 0 || state.stride as usize > max_size {
            return Err(PortError::InvalidStride {
                stride: state.stride,
                max_size,
            });
        }

        let media = unsafe { NonNull::new_unchecked(media_base.as_ptr().add(map_offset)) };
        Ok(Self {
            chunk,
            media,
            max_size,
            data_offset,
            _media: PhantomData,
        })
    }

    /// Returns writable bytes from the current ring offset to the plane end.
    pub(crate) fn media_capacity(&mut self) -> &mut [u8] {
        unsafe {
            std::slice::from_raw_parts_mut(
                self.media.as_ptr().add(self.data_offset),
                self.max_size - self.data_offset,
            )
        }
    }

    /// Returns the number of contiguous writable bytes exposed this cycle.
    pub const fn capacity(&self) -> usize {
        self.max_size - self.data_offset
    }

    pub(crate) fn publish(
        &self,
        bytes_used: usize,
        stride: usize,
        flags: ChunkFlags,
    ) -> Result<(), PortError> {
        if stride > self.max_size || stride > i32::MAX as usize {
            return Err(PortError::InvalidStride {
                stride: i32::try_from(stride).unwrap_or(i32::MAX),
                max_size: self.max_size,
            });
        }
        self.chunk.publish(ChunkState {
            offset: self.data_offset as u32,
            size: bytes_used as u32,
            stride: stride as i32,
            flags: flags.bits(),
        });
        Ok(())
    }

    /// Returns a volatile snapshot of this buffer's shared chunk.
    pub fn chunk_state(&self) -> ChunkState {
        self.chunk.state()
    }
}

fn checked_record(
    base: *mut u8,
    len: usize,
    required: usize,
    align: usize,
    record: &'static str,
) -> Result<NonNull<u8>, PortError> {
    let base = NonNull::new(base).ok_or(PortError::Null(record))?;
    if len != required {
        return Err(PortError::InvalidSize {
            record,
            actual: len,
            required,
        });
    }
    if !(base.as_ptr() as usize).is_multiple_of(align) {
        return Err(PortError::Misaligned {
            record,
            address: base.as_ptr() as usize,
            required: align,
        });
    }
    Ok(base)
}

#[cfg(test)]
mod tests {
    use static_assertions::{assert_impl_all, assert_not_impl_any};

    use super::{BufferStatus, BuffersIoView, ChunkState, OutputBuffer, PortError, PortIoType};

    assert_impl_all!(BuffersIoView<'static>: Send);
    assert_not_impl_any!(BuffersIoView<'static>: Sync);
    assert_impl_all!(OutputBuffer<'static>: Send);
    assert_not_impl_any!(OutputBuffer<'static>: Sync);

    #[repr(C, align(4))]
    struct Aligned<const N: usize>([u8; N]);

    fn write_chunk(bytes: &mut Aligned<16>, state: ChunkState) {
        unsafe {
            let base = bytes.0.as_mut_ptr();
            base.cast::<u32>().write(state.offset);
            base.add(4).cast::<u32>().write(state.size);
            base.add(8).cast::<i32>().write(state.stride);
            base.add(12).cast::<u32>().write(state.flags);
        }
    }

    #[test]
    fn rejects_async_wrong_size_and_misalignment() {
        let mut io = Aligned([0; 8]);
        assert_eq!(
            unsafe {
                BuffersIoView::from_raw_parts(PortIoType::AsyncBuffers, io.0.as_mut_ptr(), 8)
            }
            .unwrap_err(),
            PortError::AsyncBuffersUnsupported
        );
        assert!(matches!(
            unsafe { BuffersIoView::from_raw_parts(PortIoType::Buffers, io.0.as_mut_ptr(), 7) },
            Err(PortError::InvalidSize { .. })
        ));
        let mut unaligned = [0_u8; 9];
        assert!(matches!(
            unsafe {
                BuffersIoView::from_raw_parts(PortIoType::Buffers, unaligned.as_mut_ptr().add(1), 8)
            },
            Err(PortError::Misaligned { .. })
        ));
    }

    #[test]
    fn validates_plane_range_chunk_wrap_and_stride() {
        let mut chunk = Aligned([0; 16]);
        let mut media = [0_u8; 32];
        write_chunk(
            &mut chunk,
            ChunkState {
                offset: 6,
                size: 2,
                stride: 2,
                flags: 0,
            },
        );
        let buffer = unsafe {
            OutputBuffer::from_raw_parts(
                chunk.0.as_mut_ptr(),
                16,
                media.as_mut_ptr(),
                media.len(),
                8,
                8,
            )
        }
        .unwrap();
        assert_eq!(buffer.capacity(), 2);

        write_chunk(
            &mut chunk,
            ChunkState {
                offset: 6,
                size: 3,
                stride: 2,
                flags: 0,
            },
        );
        assert!(matches!(
            unsafe {
                OutputBuffer::from_raw_parts(
                    chunk.0.as_mut_ptr(),
                    16,
                    media.as_mut_ptr(),
                    media.len(),
                    30,
                    8,
                )
            },
            Err(PortError::MediaRangeOutOfBounds { .. })
        ));
        assert!(matches!(
            unsafe {
                OutputBuffer::from_raw_parts(
                    chunk.0.as_mut_ptr(),
                    16,
                    media.as_mut_ptr(),
                    media.len(),
                    0,
                    8,
                )
            },
            Err(PortError::WrappedChunk { .. })
        ));

        write_chunk(
            &mut chunk,
            ChunkState {
                offset: 0,
                size: 0,
                stride: -1,
                flags: 0,
            },
        );
        assert!(matches!(
            unsafe {
                OutputBuffer::from_raw_parts(
                    chunk.0.as_mut_ptr(),
                    16,
                    media.as_mut_ptr(),
                    media.len(),
                    0,
                    8,
                )
            },
            Err(PortError::InvalidStride { .. })
        ));

        let _ = BufferStatus::Ok;
    }
}
