//! Serial reference using ordered OCC with one worker and one outstanding tx.
//! Retains OCC validation/overlay overhead; QMDB configuration is unchanged.
use exepipe::{
    exetask::{ExeTask, Tx},
    Block,
};
use fafo_occ::{OccConfig, OccExecutor};
use indicatif::ProgressBar;
use qmdb::AdsWrap;
use std::{
    collections::HashMap,
    sync::mpsc::{Receiver, SyncSender},
};

fn config() -> OccConfig {
    OccConfig {
        workers: 1,
        window: 1,
        ..OccConfig::default()
    }
}

pub fn new(ads: AdsWrap<ExeTask>, dir: &str) -> OccExecutor {
    // No FAFO/OCC defaults, storage settings or process affinity are changed.
    OccExecutor::new(ads, dir, config()).expect("initialize serial executor")
}

pub fn executor_loop(
    task_receiver: Receiver<(Vec<Vec<Box<Tx>>>, Block)>,
    done_sender: SyncSender<bool>,
    pipe: &mut OccExecutor,
    pb: &ProgressBar,
) {
    for (task_in, block) in task_receiver {
        pb.inc(1);
        let output = pipe
            .run_block(task_in, block)
            .expect("execute serial block");
        let stats = output.stats;
        assert_eq!(
            stats.errors, 0,
            "Serial benchmark encountered final transaction errors"
        );
        assert_eq!(
            stats.validation_failures, 0,
            "Serial execution unexpectedly retried a transaction"
        );
        assert_eq!(stats.attempts, stats.finalized);
        metrics::counter!("serial.txns_processed").increment(stats.finalized as u64);
        metrics::counter!("serial.attempts").increment(stats.attempts as u64);
        metrics::counter!("serial.validation_failures").increment(stats.validation_failures as u64);
        metrics::counter!("serial.applied").increment(stats.applied as u64);
        metrics::counter!("serial.errors").increment(stats.errors as u64);
        metrics::counter!("serial.persistence_tasks").increment(stats.persistence_tasks as u64);
    }
    pb.finish_with_message("Finished serial executor_loop");
    pipe.flush().expect("flush serial persistence");
    done_sender.send(true).unwrap();
}

pub fn report(values: &HashMap<String, f64>, expected: u64) {
    let finalized = values.get("serial.txns_processed").copied().unwrap_or(0.0);
    assert_eq!(finalized, expected as f64);
    log::info!("Serial: {} transactions, one execution worker, window=1; QMDB background processing unchanged", finalized);
}

#[cfg(test)]
mod tests {
    #[test]
    fn one_at_a_time_without_changing_persistence_batching() {
        let config = super::config();
        assert_eq!(config.execution_workers(), 1);
        assert_eq!(config.persistence_workers(), 0);
        assert_eq!(config.window, 1);
        assert_eq!(
            config.persistence_batch_size,
            fafo_occ::OccConfig::default().persistence_batch_size
        );
    }
}
