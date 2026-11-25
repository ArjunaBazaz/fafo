//! Benchmark boundary only. FAFO keeps its original execution loop; OCC
//! consumes the same prepared blocks and waits for persistence before completion.
use super::cli::Executor;
use exepipe::{
    exetask::{ExeTask, Tx},
    Block, ExePipe,
};
use fafo_occ::{OccConfig, OccExecutor};
use indicatif::ProgressBar;
use qmdb::AdsWrap;
use revm::{primitives::Address, state::Bytecode};
use std::{
    collections::HashMap,
    sync::mpsc::{Receiver, SyncSender},
};

pub enum SelectedExecutor {
    Fafo(ExePipe),
    Occ(OccExecutor),
    Serial(OccExecutor),
}

impl SelectedExecutor {
    pub fn new(selection: Executor, ads: AdsWrap<ExeTask>, dir: &str) -> Self {
        match selection {
            Executor::Fafo => Self::Fafo(ExePipe::new(ads, dir)),
            Executor::Serial => Self::Serial(super::serial_adapter::new(ads, dir)),
            Executor::Occ => {
                let mut executor =
                    OccExecutor::new(ads, dir, OccConfig::default()).expect("initialize OCC");
                executor.set_timings_enabled(std::env::var("OCC_TIMINGS").as_deref() == Ok("1"));
                Self::Occ(executor)
            }
        }
    }

    pub fn insert_code(&mut self, address: &Address, code: Bytecode) {
        match self {
            Self::Fafo(pipe) => pipe.insert_code(address, code),
            Self::Serial(pipe) => pipe
                .insert_code(address, code)
                .expect("initialize serial bytecode"),
            Self::Occ(pipe) => pipe
                .insert_code(address, code)
                .expect("initialize OCC bytecode"),
        }
    }
}

pub fn executor_loop(
    task_receiver: Receiver<(Vec<Vec<Box<Tx>>>, Block)>,
    done_sender: SyncSender<bool>,
    pipe: &mut OccExecutor,
    pb: &ProgressBar,
) {
    for (task_in, block) in task_receiver {
        pb.inc(1);
        let output = pipe.run_block(task_in, block).expect("execute OCC block");
        let stats = output.stats;
        metrics::counter!("occ.txns_processed").increment(stats.finalized as u64);
        metrics::counter!("occ.attempts").increment(stats.attempts as u64);
        metrics::counter!("occ.validation_failures").increment(stats.validation_failures as u64);
        metrics::counter!("occ.applied").increment(stats.applied as u64);
        metrics::counter!("occ.errors").increment(stats.errors as u64);
        metrics::counter!("occ.persistence_tasks").increment(stats.persistence_tasks as u64);
        assert_eq!(
            stats.errors, 0,
            "OCC benchmark encountered final transaction errors"
        );
        if let Some(timing) = stats.timings {
            record_timings(timing.measurements());
            metrics::counter!("occ.timing.blocks").increment(1);
            log::info!(
                "OCC timings: block={:.3}s scheduler={:.3}s result_wait={:.3}s validation_wait={:.3}s validation_work={:.3}s publish_wait={:.3}s publish_work={:.3}s retry={:.3}s callback={:.3}s compaction={:.3}s",
                seconds(timing.block_ns), seconds(timing.scheduler_ns),
                seconds(timing.coordinator_result_wait_ns),
                seconds(timing.coordinator_validation.wait_ns), seconds(timing.coordinator_validation.hold_ns),
                seconds(timing.coordinator_publication.wait_ns), seconds(timing.coordinator_publication.hold_ns),
                seconds(timing.coordinator_retry_ns), seconds(timing.coordinator_callback_ns),
                seconds(timing.compaction_ns),
            );
            let start = std::time::Instant::now();
            drop(output);
            metrics::counter!("occ.timing.result_drop_ns")
                .increment(start.elapsed().as_nanos() as u64);
        }
    }
    pb.finish_with_message("Finished executor_loop");
    if let Some(timing) = pipe.flush_with_timings().expect("flush OCC persistence") {
        record_timings(timing.measurements());
        log::info!(
            "OCC final drain: total={:.3}s QMDB={:.3}s code={:.3}s overlay_clear={:.3}s",
            seconds(timing.total_ns),
            seconds(timing.qmdb_ns),
            seconds(timing.code_ns),
            seconds(timing.overlay_clear_ns)
        );
    }
    done_sender.send(true).unwrap();
}

fn seconds(nanoseconds: u64) -> f64 {
    nanoseconds as f64 / 1e9
}

fn record_timings(measurements: impl IntoIterator<Item = (&'static str, u64)>) {
    for (name, value) in measurements {
        metrics::counter!(format!("occ.timing.{name}")).increment(value);
    }
}

pub fn report(values: &HashMap<String, f64>, expected: u64) {
    let finalized = values.get("occ.txns_processed").copied().unwrap_or(0.0);
    assert_eq!(finalized, expected as f64);
    log::info!(
        "OCC: {} transactions, {} attempts, {} validation failures",
        finalized,
        values.get("occ.attempts").unwrap_or(&0.0),
        values.get("occ.validation_failures").unwrap_or(&0.0)
    );
}
