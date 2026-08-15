// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: Copyright (c) 2026 Asymptotic Inc.

// Public SPA ABI, verified against PipeWire commit
// 69c1b4c8b6a1cfa95982e5ed740a3995d94c1308. See node/README.md.
pub const IO_BUFFERS_SIZE: usize = 8;
pub const IO_BUFFERS_ALIGN: usize = 4;
pub const IO_STATUS: usize = 0;
pub const IO_BUFFER_ID: usize = 4;
pub const CHUNK_SIZE: usize = 16;
pub const CHUNK_ALIGN: usize = 4;
pub const CHUNK_OFFSET: usize = 0;
pub const CHUNK_DATA_SIZE: usize = 4;
pub const CHUNK_STRIDE: usize = 8;
pub const CHUNK_FLAGS: usize = 12;
