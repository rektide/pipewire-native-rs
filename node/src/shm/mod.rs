// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: Copyright (c) 2026 Asymptotic Inc.

mod memfd;
mod registry;

pub use memfd::{create_memfd, MappedRegion, SealStatus, ShrinkPolicy};
pub use registry::MemoryRegistry;
