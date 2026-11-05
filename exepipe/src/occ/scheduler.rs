use super::attempt::{execute, Attempt};
use super::tx_context::Runtime;
use super::OccStats;
use crate::exetask::Tx;
use anyhow::{anyhow, Result};
use crossbeam::channel::{bounded, Receiver};
use qmdb::ADS;
use rayon::{Scope, ThreadPool};
use std::collections::VecDeque;
use std::panic::{catch_unwind, AssertUnwindSafe};

fn submit<'scope, T: ADS>(
    scope: &Scope<'scope>,
    runtime: &'scope Runtime<T>,
    tx: &'scope Tx,
) -> Receiver<Result<Attempt>> {
    let (sender, receiver) = bounded(1);
    scope.spawn(move |_| {
        // Begin the read phase when a worker actually starts, rather than
        // making queued work stale before it has had CPU time to execute.
        let snapshot = runtime.state.version();
        let result = catch_unwind(AssertUnwindSafe(|| execute(runtime, tx, snapshot)))
            .map_err(|_| anyhow!("OCC speculative worker panicked"));
        let _ = sender.send(result);
    });
    receiver
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
    F: FnMut(usize, Attempt) -> Result<()> + Send,
{
    // Keep the blocking coordinator on the calling thread, leaving every pool
    // worker available (including configurations with a single worker).
    pool.in_place_scope(|scope| {
        let mut pending = VecDeque::new();
        let mut next = 0;
        while next < transactions.len().min(window) {
            pending.push_back(submit(scope, runtime, &transactions[next]));
            next += 1;
        }
        let mut stats = OccStats::default();
        for (index, tx) in transactions.iter().enumerate() {
            let mut attempt = pending
                .pop_front()
                .unwrap()
                .recv()
                .map_err(|_| anyhow!("OCC worker disconnected"))??;
            stats.attempts += 1;
            if !runtime.state.validate(&attempt.reads) {
                stats.validation_failures += 1;
                attempt = execute(runtime, tx, runtime.state.version());
                stats.attempts += 1;
                assert!(runtime.state.validate(&attempt.reads));
            }
            if let Some((changes, _, _, codes, _, _)) = &attempt.output.1 {
                runtime.state.publish(changes, codes);
                stats.applied += 1;
            }
            stats.finalized += 1;
            if attempt.output.0.is_err() {
                stats.errors += 1;
            }
            committed(index, attempt)?;
            if next < transactions.len() {
                pending.push_back(submit(scope, runtime, &transactions[next]));
                next += 1;
            }
        }
        Ok(stats)
    })
}
