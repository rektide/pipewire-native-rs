// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: Copyright (c) 2026 Asymptotic Inc.

// PipeWire pw_node_activation v1 ABI, verified against upstream commit
// 69c1b4c8b6a1cfa95982e5ed740a3995d94c1308. See node/README.md.
pub const SIZE: usize = 2312;
pub const ALIGN: usize = 8;
pub const STATUS: usize = 0;
pub const STATE0_STATUS: usize = 8;
pub const STATE0_REQUIRED: usize = 12;
pub const STATE0_PENDING: usize = 16;
pub const SIGNAL_TIME: usize = 32;
pub const AWAKE_TIME: usize = 40;
pub const FINISH_TIME: usize = 48;
pub const CLIENT_VERSION: usize = 540;
pub const SERVER_VERSION: usize = 544;
