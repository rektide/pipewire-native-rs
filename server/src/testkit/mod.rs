// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: Copyright (c) 2026 Asymptotic Inc.

use std::{
    io,
    path::PathBuf,
    thread::{self, JoinHandle},
    time::{SystemTime, UNIX_EPOCH},
};

use crate::runtime::{RunReport, ScriptedServer};

/// Builds a unique Unix socket path in the system temp directory.
pub fn unique_socket_path(prefix: &str) -> PathBuf {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock should be after unix epoch");
    std::env::temp_dir().join(format!(
        "{prefix}-{}-{}.sock",
        std::process::id(),
        now.as_nanos()
    ))
}

/// Spawns a scripted server on a dedicated thread.
pub fn spawn(server: ScriptedServer) -> JoinHandle<io::Result<RunReport>> {
    thread::spawn(move || server.run())
}
