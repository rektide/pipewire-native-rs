// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: Copyright (c) 2026 Asymptotic Inc.

//! Typed access to PipeWire's native `pw_node_activation` shared-memory ABI.

use std::{
    cell::UnsafeCell,
    fmt,
    marker::PhantomData,
    ptr::NonNull,
    sync::atomic::{AtomicI32, AtomicU32, Ordering},
};

mod abi {
    include!(concat!(env!("OUT_DIR"), "/activation_abi.rs"));
}

/// The activation shared-memory ABI version used by ClientNode v6.
pub const ACTIVATION_VERSION: u32 = 1;

/// Exact values of `PW_NODE_ACTIVATION_*`.
#[repr(u32)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ActivationStatus {
    /// Prepared but not yet triggered.
    NotTriggered = 0,
    /// Triggered and waiting for the client to claim the cycle.
    Triggered = 1,
    /// Claimed by the processing client.
    Awake = 2,
    /// Processing is complete and the node can be prepared again.
    Finished = 3,
    /// The node is not schedulable.
    Inactive = 4,
}

impl TryFrom<u32> for ActivationStatus {
    type Error = ActivationError;

    fn try_from(value: u32) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::NotTriggered),
            1 => Ok(Self::Triggered),
            2 => Ok(Self::Awake),
            3 => Ok(Self::Finished),
            4 => Ok(Self::Inactive),
            value => Err(ActivationError::UnknownStatus(value)),
        }
    }
}

/// Failure to construct or operate on an activation view.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ActivationError {
    /// The supplied pointer was null.
    Null,
    /// The region is smaller than the target C ABI record.
    Undersized {
        /// Supplied byte length.
        actual: usize,
        /// Required byte length.
        required: usize,
    },
    /// The region base does not satisfy the target C ABI alignment.
    Misaligned {
        /// Supplied address.
        address: usize,
        /// Required alignment.
        required: usize,
    },
    /// The server activation ABI is not version 1.
    UnsupportedVersion(u32),
    /// Shared memory contained an unknown activation status.
    UnknownStatus(u32),
    /// A compare-and-swap observed a state other than the required predecessor.
    InvalidTransition {
        /// Required predecessor.
        expected: ActivationStatus,
        /// Status observed by the failed compare-and-swap.
        actual: ActivationStatus,
    },
    /// A peer dependency count was already zero or negative.
    InvalidPending(i32),
}

impl fmt::Display for ActivationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

impl std::error::Error for ActivationError {}

/// Result of decrementing a peer's pending dependency count.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TriggerOutcome {
    /// Other dependencies remain; no status transition occurred.
    Waiting(i32),
    /// Pending reached zero and `NOT_TRIGGERED -> TRIGGERED` succeeded.
    Triggered,
}

/// ABI-checked view over a `pw_node_activation` shared-memory region.
///
/// This type never creates a Rust reference to the complete C record. Atomic fields
/// are addressed individually and payload fields are accessed with volatile loads or
/// stores under the activation protocol's ownership transitions.
#[derive(Debug)]
pub struct ActivationView<'a> {
    base: NonNull<u8>,
    _region: PhantomData<&'a UnsafeCell<[u8]>>,
}

// SAFETY: moving a view does not access its memory. The marker deliberately keeps
// the view !Sync so a session must retain one process-domain owner.
unsafe impl Send for ActivationView<'_> {}

impl<'a> ActivationView<'a> {
    /// Constructs a view over externally shared activation memory.
    ///
    /// # Safety
    ///
    /// `base..base + len` must remain mapped, readable, and writable for `'a`. It
    /// must contain a live `pw_node_activation` initialized by PipeWire. All foreign
    /// accesses must obey PipeWire's activation atomic and ownership protocol, and no
    /// Rust reference may be formed over concurrently accessed bytes.
    pub unsafe fn from_raw_parts(base: *mut u8, len: usize) -> Result<Self, ActivationError> {
        let base = NonNull::new(base).ok_or(ActivationError::Null)?;
        if len < abi::SIZE {
            return Err(ActivationError::Undersized {
                actual: len,
                required: abi::SIZE,
            });
        }
        if !(base.as_ptr() as usize).is_multiple_of(abi::ALIGN) {
            return Err(ActivationError::Misaligned {
                address: base.as_ptr() as usize,
                required: abi::ALIGN,
            });
        }

        let view = Self {
            base,
            _region: PhantomData,
        };
        let version = unsafe { view.read_u32(abi::SERVER_VERSION) };
        if version != ACTIVATION_VERSION {
            return Err(ActivationError::UnsupportedVersion(version));
        }
        Ok(view)
    }

    /// Returns the target-native size required for an activation mapping.
    pub const fn required_size() -> usize {
        abi::SIZE
    }

    /// Returns the target-native alignment required for an activation mapping.
    pub const fn required_alignment() -> usize {
        abi::ALIGN
    }

    /// Publishes activation ABI v1 support without changing server-owned bytes.
    pub fn initialize_client_v1(&self) {
        unsafe { self.write_u32(abi::CLIENT_VERSION, ACTIVATION_VERSION) };
    }

    /// Atomically reads the current activation status.
    pub fn status(&self) -> Result<ActivationStatus, ActivationError> {
        self.status_atomic().load(Ordering::SeqCst).try_into()
    }

    /// Makes a ClientNode v6 activation schedulable (`INACTIVE -> FINISHED`).
    pub fn mark_ready(&self) -> Result<(), ActivationError> {
        self.transition(ActivationStatus::Inactive, ActivationStatus::Finished)
    }

    /// Removes scheduling authorization regardless of the prior status.
    pub fn deactivate(&self) -> Result<ActivationStatus, ActivationError> {
        self.status_atomic()
            .swap(ActivationStatus::Inactive as u32, Ordering::SeqCst)
            .try_into()
    }

    /// Claims one triggered process cycle and records its awake time.
    pub fn claim_cycle(&self, now_ns: u64) -> Result<CycleClaim<'_, 'a>, ActivationError> {
        self.transition(ActivationStatus::Triggered, ActivationStatus::Awake)?;
        unsafe { self.write_u64(abi::AWAKE_TIME, now_ns) };
        Ok(CycleClaim { activation: self })
    }

    /// Atomically reads the current process result (`state[0].status`).
    pub fn process_result(&self) -> i32 {
        self.atomic_i32(abi::STATE0_STATUS).load(Ordering::SeqCst)
    }

    /// Atomically reads `state[0].required`.
    pub fn required(&self) -> i32 {
        self.atomic_i32(abi::STATE0_REQUIRED).load(Ordering::SeqCst)
    }

    /// Atomically writes `state[0].required`.
    pub fn set_required(&self, required: i32) {
        self.atomic_i32(abi::STATE0_REQUIRED)
            .store(required, Ordering::SeqCst);
    }

    /// Atomically reads `state[0].pending`.
    pub fn pending(&self) -> i32 {
        self.atomic_i32(abi::STATE0_PENDING).load(Ordering::SeqCst)
    }

    /// Resets pending dependencies from the atomically observed required count.
    pub fn reset_pending(&self) -> Result<i32, ActivationError> {
        let required = self.required();
        if required < 0 {
            return Err(ActivationError::InvalidPending(required));
        }
        self.atomic_i32(abi::STATE0_PENDING)
            .store(required, Ordering::SeqCst);
        Ok(required)
    }

    /// Decrements peer pending and triggers it when the count reaches zero.
    ///
    /// A successful trigger records `signal_time` before returning. The caller may
    /// signal the peer eventfd only after receiving [`TriggerOutcome::Triggered`].
    pub fn decrement_pending_and_trigger(
        &self,
        now_ns: u64,
    ) -> Result<TriggerOutcome, ActivationError> {
        let pending = self.atomic_i32(abi::STATE0_PENDING);
        let mut current = pending.load(Ordering::SeqCst);
        loop {
            if current <= 0 {
                return Err(ActivationError::InvalidPending(current));
            }
            match pending.compare_exchange(current, current - 1, Ordering::SeqCst, Ordering::SeqCst)
            {
                Ok(_) if current > 1 => return Ok(TriggerOutcome::Waiting(current - 1)),
                Ok(_) => break,
                Err(actual) => current = actual,
            }
        }

        self.transition(ActivationStatus::NotTriggered, ActivationStatus::Triggered)?;
        unsafe { self.write_u64(abi::SIGNAL_TIME, now_ns) };
        Ok(TriggerOutcome::Triggered)
    }

    /// Volatile-loads `signal_time`.
    ///
    /// # Safety
    ///
    /// The caller must establish from the activation state machine that no foreign
    /// participant can concurrently write this field.
    pub unsafe fn signal_time(&self) -> u64 {
        unsafe { self.read_u64(abi::SIGNAL_TIME) }
    }

    /// Volatile-loads `awake_time`.
    ///
    /// # Safety
    ///
    /// The caller must establish from the activation state machine that no foreign
    /// participant can concurrently write this field.
    pub unsafe fn awake_time(&self) -> u64 {
        unsafe { self.read_u64(abi::AWAKE_TIME) }
    }

    /// Volatile-loads `finish_time`.
    ///
    /// # Safety
    ///
    /// The caller must establish from the activation state machine that no foreign
    /// participant can concurrently write this field.
    pub unsafe fn finish_time(&self) -> u64 {
        unsafe { self.read_u64(abi::FINISH_TIME) }
    }

    fn transition(
        &self,
        from: ActivationStatus,
        to: ActivationStatus,
    ) -> Result<(), ActivationError> {
        match self.status_atomic().compare_exchange(
            from as u32,
            to as u32,
            Ordering::SeqCst,
            Ordering::SeqCst,
        ) {
            Ok(_) => Ok(()),
            Err(actual) => Err(ActivationError::InvalidTransition {
                expected: from,
                actual: actual.try_into()?,
            }),
        }
    }

    fn status_atomic(&self) -> &AtomicU32 {
        unsafe { &*self.base.as_ptr().add(abi::STATUS).cast::<AtomicU32>() }
    }

    fn atomic_i32(&self, offset: usize) -> &AtomicI32 {
        unsafe { &*self.base.as_ptr().add(offset).cast::<AtomicI32>() }
    }

    unsafe fn read_u32(&self, offset: usize) -> u32 {
        unsafe { self.base.as_ptr().add(offset).cast::<u32>().read_volatile() }
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

    unsafe fn read_u64(&self, offset: usize) -> u64 {
        unsafe { self.base.as_ptr().add(offset).cast::<u64>().read_volatile() }
    }

    unsafe fn write_u64(&self, offset: usize, value: u64) {
        unsafe {
            self.base
                .as_ptr()
                .add(offset)
                .cast::<u64>()
                .write_volatile(value)
        };
    }
}

/// Exclusive protocol authorization to publish one process-cycle result.
#[derive(Debug)]
pub struct CycleClaim<'view, 'region> {
    activation: &'view ActivationView<'region>,
}

impl CycleClaim<'_, '_> {
    /// Publishes the process result and finish timestamp, then completes the cycle.
    pub fn publish_result_and_finish(
        self,
        process_status: i32,
        now_ns: u64,
    ) -> Result<(), ActivationError> {
        self.activation
            .atomic_i32(abi::STATE0_STATUS)
            .store(process_status, Ordering::SeqCst);
        unsafe { self.activation.write_u64(abi::FINISH_TIME, now_ns) };
        self.activation
            .transition(ActivationStatus::Awake, ActivationStatus::Finished)
    }
}

#[cfg(test)]
mod tests {
    use std::{mem::size_of, sync::atomic::AtomicU32};

    use static_assertions::{assert_impl_all, assert_not_impl_any};

    use super::{
        abi, ActivationError, ActivationStatus, ActivationView, TriggerOutcome, ACTIVATION_VERSION,
    };

    assert_impl_all!(ActivationView<'static>: Send);
    assert_not_impl_any!(ActivationView<'static>: Sync);

    unsafe extern "C" {
        static pw_activation_abi_size: usize;
        static pw_activation_abi_align: usize;
        static pw_activation_abi_status: usize;
        static pw_activation_abi_state0_status: usize;
        static pw_activation_abi_state0_required: usize;
        static pw_activation_abi_state0_pending: usize;
        static pw_activation_abi_signal_time: usize;
        static pw_activation_abi_awake_time: usize;
        static pw_activation_abi_finish_time: usize;
        static pw_activation_abi_client_version: usize;
        static pw_activation_abi_server_version: usize;
    }

    struct Fixture {
        words: Vec<u64>,
    }

    impl Fixture {
        fn new(status: ActivationStatus) -> Self {
            let mut words = vec![0; abi::SIZE.div_ceil(size_of::<u64>())];
            let base = words.as_mut_ptr().cast::<u8>();
            unsafe {
                base.add(abi::STATUS)
                    .cast::<AtomicU32>()
                    .write(AtomicU32::new(status as u32));
                base.add(abi::SERVER_VERSION)
                    .cast::<u32>()
                    .write(ACTIVATION_VERSION);
            }
            Self { words }
        }

        fn view(&mut self) -> ActivationView<'_> {
            unsafe {
                ActivationView::from_raw_parts(self.words.as_mut_ptr().cast(), abi::SIZE).unwrap()
            }
        }
    }

    #[test]
    fn c_abi_matches_generated_rust_layout() {
        unsafe {
            assert_eq!(pw_activation_abi_size, abi::SIZE);
            assert_eq!(pw_activation_abi_align, abi::ALIGN);
            assert_eq!(pw_activation_abi_status, abi::STATUS);
            assert_eq!(pw_activation_abi_state0_status, abi::STATE0_STATUS);
            assert_eq!(pw_activation_abi_state0_required, abi::STATE0_REQUIRED);
            assert_eq!(pw_activation_abi_state0_pending, abi::STATE0_PENDING);
            assert_eq!(pw_activation_abi_signal_time, abi::SIGNAL_TIME);
            assert_eq!(pw_activation_abi_awake_time, abi::AWAKE_TIME);
            assert_eq!(pw_activation_abi_finish_time, abi::FINISH_TIME);
            assert_eq!(pw_activation_abi_client_version, abi::CLIENT_VERSION);
            assert_eq!(pw_activation_abi_server_version, abi::SERVER_VERSION);
        }
    }

    #[test]
    fn performs_client_node_v6_cycle_transitions() {
        let mut fixture = Fixture::new(ActivationStatus::Inactive);
        let activation = fixture.view();
        activation.initialize_client_v1();
        activation.mark_ready().unwrap();
        assert_eq!(activation.status().unwrap(), ActivationStatus::Finished);

        activation.status_atomic().store(
            ActivationStatus::Triggered as u32,
            std::sync::atomic::Ordering::SeqCst,
        );
        let claim = activation.claim_cycle(100).unwrap();
        claim.publish_result_and_finish(2, 140).unwrap();

        assert_eq!(activation.status().unwrap(), ActivationStatus::Finished);
        assert_eq!(activation.process_result(), 2);
        assert_eq!(unsafe { activation.awake_time() }, 100);
        assert_eq!(unsafe { activation.finish_time() }, 140);
    }

    #[test]
    fn decrements_pending_before_triggering_peer() {
        let mut fixture = Fixture::new(ActivationStatus::NotTriggered);
        let activation = fixture.view();
        activation.set_required(2);
        assert_eq!(activation.reset_pending().unwrap(), 2);
        assert_eq!(
            activation.decrement_pending_and_trigger(10).unwrap(),
            TriggerOutcome::Waiting(1)
        );
        assert_eq!(activation.status().unwrap(), ActivationStatus::NotTriggered);
        assert_eq!(
            activation.decrement_pending_and_trigger(20).unwrap(),
            TriggerOutcome::Triggered
        );
        assert_eq!(activation.status().unwrap(), ActivationStatus::Triggered);
        assert_eq!(unsafe { activation.signal_time() }, 20);
    }

    #[test]
    fn rejects_invalid_transitions_and_pending_underflow() {
        let mut fixture = Fixture::new(ActivationStatus::Finished);
        let activation = fixture.view();
        assert_eq!(
            activation.claim_cycle(1).unwrap_err(),
            ActivationError::InvalidTransition {
                expected: ActivationStatus::Triggered,
                actual: ActivationStatus::Finished,
            }
        );
        assert_eq!(
            activation.decrement_pending_and_trigger(1).unwrap_err(),
            ActivationError::InvalidPending(0)
        );
        assert_eq!(activation.pending(), 0);
    }

    #[test]
    fn rejects_undersized_misaligned_and_wrong_version_regions() {
        let mut fixture = Fixture::new(ActivationStatus::Inactive);
        let base = fixture.words.as_mut_ptr().cast::<u8>();
        assert!(matches!(
            unsafe { ActivationView::from_raw_parts(base, abi::SIZE - 1) },
            Err(ActivationError::Undersized { .. })
        ));
        assert!(matches!(
            unsafe { ActivationView::from_raw_parts(base.wrapping_add(1), abi::SIZE) },
            Err(ActivationError::Misaligned { .. })
        ));
        unsafe {
            base.add(abi::SERVER_VERSION).cast::<u32>().write(0);
        }
        assert_eq!(
            unsafe { ActivationView::from_raw_parts(base, abi::SIZE) }.unwrap_err(),
            ActivationError::UnsupportedVersion(0)
        );
    }
}
