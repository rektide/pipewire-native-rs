// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: Copyright (c) 2026 Asymptotic Inc.

use std::{
    io,
    os::unix::net::UnixStream,
    path::Path,
    path::PathBuf,
    sync::mpsc::{self, Receiver},
    thread::{self, JoinHandle},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
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

/// Deadline shared by setup and completion waits in integration tests.
#[derive(Clone, Copy, Debug)]
pub struct TestDeadline {
    expires_at: Instant,
}

impl TestDeadline {
    /// Creates a deadline that expires after `timeout`.
    pub fn after(timeout: Duration) -> Self {
        Self {
            expires_at: Instant::now() + timeout,
        }
    }

    /// Returns the remaining duration or a diagnostic timeout error.
    pub fn remaining(self, operation: &str) -> io::Result<Duration> {
        let remaining = self.expires_at.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            Err(io::Error::new(
                io::ErrorKind::TimedOut,
                format!("test deadline elapsed while {operation}"),
            ))
        } else {
            Ok(remaining)
        }
    }

    /// Connects before the deadline and installs bounded socket reads and writes.
    pub fn connect(self, socket_path: &Path) -> io::Result<UnixStream> {
        loop {
            match UnixStream::connect(socket_path) {
                Ok(stream) => {
                    let remaining = self.remaining("configuring client socket")?;
                    stream.set_read_timeout(Some(remaining))?;
                    stream.set_write_timeout(Some(remaining))?;
                    return Ok(stream);
                }
                Err(err)
                    if matches!(
                        err.kind(),
                        io::ErrorKind::NotFound | io::ErrorKind::ConnectionRefused
                    ) =>
                {
                    let remaining = self.remaining(&format!(
                        "connecting to {} (last error: {err})",
                        socket_path.display()
                    ))?;
                    thread::park_timeout(remaining.min(Duration::from_millis(10)));
                }
                Err(err) => return Err(err),
            }
        }
    }
}

/// Completion handle for a scripted server thread.
pub struct ServerHandle {
    result: Receiver<io::Result<RunReport>>,
    thread: JoinHandle<()>,
}

impl ServerHandle {
    /// Waits for server completion without allowing an unbounded join.
    pub fn wait(self, deadline: TestDeadline) -> io::Result<RunReport> {
        let remaining = deadline.remaining("waiting for scripted server completion")?;
        let result = self.result.recv_timeout(remaining).map_err(|err| {
            io::Error::new(
                io::ErrorKind::TimedOut,
                format!("scripted server did not complete before test deadline: {err}"),
            )
        })?;
        self.thread.join().map_err(|_| {
            io::Error::other("scripted server thread panicked after reporting completion")
        })?;
        result
    }
}

/// Spawns a scripted server on a dedicated thread.
pub fn spawn(server: ScriptedServer) -> ServerHandle {
    let (sender, result) = mpsc::sync_channel(1);
    let thread = thread::spawn(move || {
        let _ = sender.send(server.run());
    });
    ServerHandle { result, thread }
}
