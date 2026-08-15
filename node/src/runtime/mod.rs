// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: Copyright (c) 2026 Asymptotic Inc.

use std::io;

use tokio::{
    sync::watch,
    task::{JoinError, JoinHandle},
};

use crate::{shm::MappedRegion, transport::BoundTransport};

/// Per-cycle process context.
pub struct ProcessCycle<'a> {
    /// Counter value drained from trigger eventfd.
    pub trigger_count: u64,
    /// Raw activation mapping owned by this callback-scoped cycle.
    ///
    /// Byte-reference construction remains unsafe: this borrow serializes local
    /// callbacks but does not prove synchronization with PipeWire or other mappings.
    pub activation: &'a mut MappedRegion,
}

/// Process callback invoked for each cycle trigger.
pub type ProcessCallback = Box<dyn FnMut(&mut ProcessCycle<'_>) -> io::Result<()> + Send + 'static>;

/// Runtime worker that drives node processing from eventfd signals.
pub struct NodeRuntime {
    transport: BoundTransport,
    process: ProcessCallback,
}

impl NodeRuntime {
    /// Creates a runtime worker.
    pub fn new(transport: BoundTransport, process: ProcessCallback) -> Self {
        Self { transport, process }
    }

    /// Runs the worker until shutdown is requested or an error occurs.
    pub async fn run(mut self, mut stop_rx: watch::Receiver<bool>) -> io::Result<()> {
        loop {
            tokio::select! {
                changed = stop_rx.changed() => {
                    if changed.is_err() || *stop_rx.borrow() {
                        break;
                    }
                }
                trigger = self.transport.wait_cycle() => {
                    let trigger_count = trigger?;
                    let mut cycle = ProcessCycle {
                        trigger_count,
                        activation: self.transport.activation_mut(),
                    };

                    (self.process)(&mut cycle)?;
                    self.transport.signal_complete(1)?;
                }
            }
        }

        Ok(())
    }
}

/// Handle to a spawned node runtime worker.
pub struct NodeRuntimeHandle {
    stop_tx: watch::Sender<bool>,
    join: JoinHandle<io::Result<()>>,
}

impl NodeRuntimeHandle {
    /// Requests shutdown for the worker task.
    pub fn request_shutdown(&self) -> io::Result<()> {
        self.stop_tx.send(true).map_err(|_| {
            io::Error::new(
                io::ErrorKind::BrokenPipe,
                "runtime task is not accepting shutdown requests",
            )
        })
    }

    /// Waits for task completion.
    pub async fn wait(self) -> io::Result<()> {
        join_result_to_io(self.join.await)
    }

    /// Requests shutdown and waits for task completion.
    pub async fn shutdown(self) -> io::Result<()> {
        let _ = self.stop_tx.send(true);
        self.wait().await
    }
}

/// Spawns a runtime worker onto the current Tokio runtime.
pub fn spawn(runtime: NodeRuntime) -> NodeRuntimeHandle {
    let (stop_tx, stop_rx) = watch::channel(false);
    let join = tokio::spawn(runtime.run(stop_rx));

    NodeRuntimeHandle { stop_tx, join }
}

fn join_result_to_io(join: Result<io::Result<()>, JoinError>) -> io::Result<()> {
    match join {
        Ok(result) => result,
        Err(err) => Err(io::Error::other(format!("node runtime join error: {err}"))),
    }
}
