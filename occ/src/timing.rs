//! Optional wall-clock diagnostics. Counters stay local to each attempt or the
//! coordinator; no shared atomic counters are updated on the read/commit path.
use std::ops::{Deref, DerefMut};
use std::time::Instant;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct LockTimings {
    pub acquisitions: u64,
    pub wait_ns: u64,
    pub hold_ns: u64,
}

impl LockTimings {
    pub(crate) fn merge(&mut self, other: Self) {
        self.acquisitions += other.acquisitions;
        self.wait_ns += other.wait_ns;
        self.hold_ns += other.hold_ns;
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct OccTimings {
    pub block_ns: u64,
    pub setup_ns: u64,
    pub scheduler_ns: u64,
    pub finalize_ns: u64,
    pub compaction_ns: u64,
    pub coordinator_result_wait_ns: u64,
    pub coordinator_submit_ns: u64,
    pub coordinator_retry_ns: u64,
    pub coordinator_callback_ns: u64,
    pub coordinator_other_ns: u64,
    pub coordinator_validation: LockTimings,
    pub coordinator_publication: LockTimings,
    /// Summed across workers, including snapshots and rejected attempts.
    pub worker_reads: LockTimings,
    /// Read locks taken during coordinator retries, including snapshot capture.
    pub retry_reads: LockTimings,
    /// Summed worker attempt wall time; overlaps coordinator and other workers.
    pub worker_attempt_ns: u64,
}

impl OccTimings {
    /// Additive measurements; `_ns` values are nanoseconds, others are counts.
    pub fn measurements(&self) -> [(&'static str, u64); 23] {
        [
            ("block_ns", self.block_ns),
            ("setup_ns", self.setup_ns),
            ("scheduler_ns", self.scheduler_ns),
            ("finalize_ns", self.finalize_ns),
            ("compaction_ns", self.compaction_ns),
            (
                "coordinator_result_wait_ns",
                self.coordinator_result_wait_ns,
            ),
            ("coordinator_submit_ns", self.coordinator_submit_ns),
            ("coordinator_retry_ns", self.coordinator_retry_ns),
            ("coordinator_callback_ns", self.coordinator_callback_ns),
            ("coordinator_other_ns", self.coordinator_other_ns),
            (
                "coordinator_validation_lock_wait_ns",
                self.coordinator_validation.wait_ns,
            ),
            (
                "coordinator_validation_work_ns",
                self.coordinator_validation.hold_ns,
            ),
            (
                "coordinator_validation_count",
                self.coordinator_validation.acquisitions,
            ),
            (
                "coordinator_publish_lock_wait_ns",
                self.coordinator_publication.wait_ns,
            ),
            (
                "coordinator_publish_work_ns",
                self.coordinator_publication.hold_ns,
            ),
            (
                "coordinator_publish_count",
                self.coordinator_publication.acquisitions,
            ),
            ("worker_read_lock_wait_ns", self.worker_reads.wait_ns),
            ("worker_read_lock_hold_ns", self.worker_reads.hold_ns),
            ("worker_read_lock_count", self.worker_reads.acquisitions),
            ("retry_read_lock_wait_ns", self.retry_reads.wait_ns),
            ("retry_read_lock_hold_ns", self.retry_reads.hold_ns),
            ("retry_read_lock_count", self.retry_reads.acquisitions),
            ("worker_attempt_ns", self.worker_attempt_ns),
        ]
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct OccFlushTimings {
    pub total_ns: u64,
    pub qmdb_ns: u64,
    pub code_ns: u64,
    pub overlay_clear_ns: u64,
}

impl OccFlushTimings {
    pub fn measurements(&self) -> [(&'static str, u64); 4] {
        [
            ("final_flush_ns", self.total_ns),
            ("final_qmdb_flush_ns", self.qmdb_ns),
            ("final_code_flush_ns", self.code_ns),
            ("final_overlay_clear_ns", self.overlay_clear_ns),
        ]
    }
}

pub(crate) struct Clock(Option<Instant>);

impl Clock {
    pub fn start(enabled: bool) -> Self {
        Self(enabled.then(Instant::now))
    }

    pub fn elapsed_ns(&self) -> u64 {
        self.0.map_or(0, |start| start.elapsed().as_nanos() as u64)
    }
}

/// Records acquisition separately from the time the guard is held. Update the
/// local counters after releasing the actual lock, including on early returns.
pub(crate) struct TimedGuard<'a, G> {
    guard: Option<G>,
    timing: Option<&'a mut LockTimings>,
    started: Option<Instant>,
    acquired: Option<Instant>,
}

impl<'a, G> TimedGuard<'a, G> {
    pub fn new(acquire: impl FnOnce() -> G, timing: Option<&'a mut LockTimings>) -> Self {
        let started = timing.as_ref().map(|_| Instant::now());
        let guard = acquire();
        let acquired = started.map(|_| Instant::now());
        Self {
            guard: Some(guard),
            timing,
            started,
            acquired,
        }
    }
}

impl<G: Deref> Deref for TimedGuard<'_, G> {
    type Target = G::Target;
    fn deref(&self) -> &Self::Target {
        self.guard.as_ref().unwrap().deref()
    }
}

impl<G: DerefMut> DerefMut for TimedGuard<'_, G> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.guard.as_mut().unwrap().deref_mut()
    }
}

impl<G> Drop for TimedGuard<'_, G> {
    fn drop(&mut self) {
        let ended = self.acquired.map(|_| Instant::now());
        drop(self.guard.take());
        if let Some(timing) = self.timing.as_mut() {
            timing.acquisitions += 1;
            timing.wait_ns += self
                .acquired
                .unwrap()
                .duration_since(self.started.unwrap())
                .as_nanos() as u64;
            timing.hold_ns += ended
                .unwrap()
                .duration_since(self.acquired.unwrap())
                .as_nanos() as u64;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use parking_lot::RwLock;
    use std::{sync::mpsc, thread, time::Duration};

    #[test]
    fn acquisition_wait_and_hold_are_separate_and_unlock_on_drop() {
        let lock = RwLock::new(0);
        let held = lock.write();
        let (started_tx, started_rx) = mpsc::sync_channel(1);
        thread::scope(|scope| {
            let reader = scope.spawn(|| {
                let mut timing = LockTimings::default();
                {
                    let guard = TimedGuard::new(
                        || {
                            started_tx.send(()).unwrap();
                            lock.read()
                        },
                        Some(&mut timing),
                    );
                    assert_eq!(*guard, 0);
                    thread::sleep(Duration::from_millis(2));
                }
                timing
            });
            started_rx.recv().unwrap();
            thread::sleep(Duration::from_millis(10));
            drop(held);
            let timing = reader.join().unwrap();
            assert_eq!(timing.acquisitions, 1);
            assert!(timing.wait_ns >= 10_000_000);
            assert!(timing.hold_ns >= 2_000_000);
        });
        assert!(lock.try_write().is_some());
    }
}
