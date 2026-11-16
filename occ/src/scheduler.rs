use super::attempt::{execute, Attempt};
use super::timing::{Clock, OccTimings};
use super::tx_context::Runtime;
use super::OccStats;
use crate::exetask::Tx;
use anyhow::{anyhow, Result};
use crossbeam::channel::{bounded, Sender};
use qmdb::ADS;
use rayon::{Scope, ThreadPool};
use std::panic::{catch_unwind, AssertUnwindSafe};

fn submit<'scope, T: ADS>(
    scope: &Scope<'scope>,
    runtime: &'scope Runtime<T>,
    tx: &'scope Tx,
    sender: &'scope Sender<Result<Attempt>>,
) {
    scope.spawn(move |_| {
        // Begin the read phase when a worker actually starts, rather than
        // making queued work stale before it has had CPU time to execute.
        let result = catch_unwind(AssertUnwindSafe(|| {
            let clock = Clock::start(runtime.timings_enabled);
            let snapshot = runtime.state.version();
            let mut attempt = execute(runtime, tx, snapshot);
            attempt.worker_ns = clock.elapsed_ns();
            attempt
        }))
        .map_err(|_| anyhow!("OCC speculative worker panicked"));
        let _ = sender.send(result);
    });
}

/// Parallel execution with one ordered validator/publisher. A stale head is
/// retried synchronously while later commits remain blocked, ensuring progress.
/// Scoped workers also finish before an operational error is returned.
pub(crate) fn run<T: ADS, F>(
    runtime: &Runtime<T>,
    transactions: &[Box<Tx>],
    pool: &ThreadPool,
    window: usize,
    mut committed: F,
) -> Result<OccStats>
where
    F: FnMut(usize, Attempt, Option<Attempt>) -> Result<()> + Send,
{
    // Keep the blocking coordinator on the calling thread, leaving every pool
    // worker available (including configurations with a single worker).
    let scheduler_clock = Clock::start(runtime.timings_enabled);
    // One mailbox per outstanding slot, reused after its result is consumed.
    // This bounds allocations by window size instead of transaction count.
    let slots: Vec<_> = (0..transactions.len().min(window))
        .map(|_| bounded(1))
        .collect();
    let mut stats = pool.in_place_scope(|scope| {
        let mut stats = OccStats {
            timings: runtime.timings_enabled.then(OccTimings::default),
            ..OccStats::default()
        };
        let mut next = 0;
        let submit_clock = Clock::start(runtime.timings_enabled);
        while next < transactions.len().min(window) {
            submit(
                scope,
                runtime,
                &transactions[next],
                &slots[next % slots.len()].0,
            );
            next += 1;
        }
        if let Some(timing) = stats.timings.as_mut() {
            timing.coordinator_submit_ns += submit_clock.elapsed_ns();
        }
        for (index, tx) in transactions.iter().enumerate() {
            let wait_clock = Clock::start(runtime.timings_enabled);
            let mut attempt = slots[index % slots.len()]
                .1
                .recv()
                .map_err(|_| anyhow!("OCC worker disconnected"))??;
            if let Some(timing) = stats.timings.as_mut() {
                timing.coordinator_result_wait_ns += wait_clock.elapsed_ns();
                timing.worker_attempt_ns += attempt.worker_ns;
                timing
                    .worker_reads
                    .merge(attempt.read_timing.take().unwrap());
            }
            stats.attempts += 1;
            let mut rejected = None;
            if !runtime.state.validate_timed(
                &attempt.reads,
                stats
                    .timings
                    .as_mut()
                    .map(|t| &mut t.coordinator_validation),
            ) {
                stats.validation_failures += 1;
                let retry_clock = Clock::start(runtime.timings_enabled);
                let snapshot = runtime.state.version();
                rejected = Some(std::mem::replace(
                    &mut attempt,
                    execute(runtime, tx, snapshot),
                ));
                if let Some(timing) = stats.timings.as_mut() {
                    timing.coordinator_retry_ns += retry_clock.elapsed_ns();
                    timing
                        .retry_reads
                        .merge(attempt.read_timing.take().unwrap());
                }
                stats.attempts += 1;
                assert!(runtime.state.validate_timed(
                    &attempt.reads,
                    stats
                        .timings
                        .as_mut()
                        .map(|t| &mut t.coordinator_validation)
                ));
            }
            if let Some((changes, _, _, codes, _, _)) = &attempt.output.1 {
                runtime.state.publish_timed(
                    changes,
                    codes,
                    stats
                        .timings
                        .as_mut()
                        .map(|t| &mut t.coordinator_publication),
                );
                stats.applied += 1;
            }
            stats.finalized += 1;
            if attempt.output.0.is_err() {
                stats.errors += 1;
            }
            let callback_clock = Clock::start(runtime.timings_enabled);
            committed(index, attempt, rejected)?;
            if let Some(timing) = stats.timings.as_mut() {
                timing.coordinator_callback_ns += callback_clock.elapsed_ns();
            }
            if next < transactions.len() {
                let submit_clock = Clock::start(runtime.timings_enabled);
                submit(
                    scope,
                    runtime,
                    &transactions[next],
                    &slots[next % slots.len()].0,
                );
                next += 1;
                if let Some(timing) = stats.timings.as_mut() {
                    timing.coordinator_submit_ns += submit_clock.elapsed_ns();
                }
            }
        }
        Ok::<_, anyhow::Error>(stats)
    })?;
    if let Some(timing) = stats.timings.as_mut() {
        timing.scheduler_ns = scheduler_clock.elapsed_ns();
        let accounted = timing.coordinator_result_wait_ns
            + timing.coordinator_submit_ns
            + timing.coordinator_retry_ns
            + timing.coordinator_callback_ns
            + timing.coordinator_validation.wait_ns
            + timing.coordinator_validation.hold_ns
            + timing.coordinator_publication.wait_ns
            + timing.coordinator_publication.hold_ns;
        // Includes bookkeeping, timing overhead and scoped-worker teardown.
        timing.coordinator_other_ns = timing.scheduler_ns.saturating_sub(accounted);
    }
    Ok(stats)
}
