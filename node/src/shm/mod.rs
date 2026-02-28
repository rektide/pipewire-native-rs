// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: Copyright (c) 2026 Asymptotic Inc.

mod memfd;
mod registry;

pub use memfd::{MappedRegion, create_memfd};
pub use registry::MemoryRegistry;
