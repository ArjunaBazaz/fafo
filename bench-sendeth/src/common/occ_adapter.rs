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
}

impl SelectedExecutor {
    pub fn new(selection: Executor, ads: AdsWrap<ExeTask>, dir: &str) -> Self {
        match selection {
            Executor::Fafo => Self::Fafo(ExePipe::new(ads, dir)),
            Executor::Occ => {
                Self::Occ(OccExecutor::new(ads, dir, OccConfig::default()).expect("initialize OCC"))
            }
        }
    }

    pub fn insert_code(&mut self, address: &Address, code: Bytecode) {
        match self {
            Self::Fafo(pipe) => pipe.insert_code(address, code),
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
    }
    pb.finish_with_message("Finished executor_loop");
    pipe.flush().expect("flush OCC persistence");
    done_sender.send(true).unwrap();
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
