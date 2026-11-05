//! The harness owns preparation and timing; each executor owns coordination.
use super::cli::{BenchmarkCli, ExecutorKind};
use exepipe::exetask::{ExeTask, Tx};
use exepipe::occ::{OccConfig, OccExecutor};
use exepipe::resources::ExecutionResources;
use exepipe::{Block, ExePipe};
use exepipe_common::statecache::StateCache;
use qmdb::AdsWrap;
use revm::{primitives::Address, state::Bytecode};
use std::sync::Arc;

pub enum BenchmarkExecutor {
    Fafo {
        pipe: Box<ExePipe>,
        cache: Arc<StateCache>,
    },
    Occ(Box<OccExecutor>),
}

impl BenchmarkExecutor {
    pub fn new(ads: AdsWrap<ExeTask>, dir: &str, args: &BenchmarkCli) -> Self {
        let budget =
            ExecutionResources::new(args.scheduler_threads).expect("invalid scheduler budget");
        match args.executor {
            ExecutorKind::Fafo => Self::Fafo {
                pipe: Box::new(ExePipe::with_resources(ads, dir, budget)),
                cache: Arc::new(StateCache::new()),
            },
            ExecutorKind::Occ => Self::Occ(Box::new(
                OccExecutor::new(
                    ads,
                    dir,
                    OccConfig {
                        workers: budget.occ_execution_workers(),
                        window: args.occ_window,
                        persistence_batch_size: exepipe::dispatcher::def::TX_IN_TASK,
                    },
                )
                .expect("cannot initialize OCC"),
            )),
        }
    }

    pub fn insert_code(&mut self, address: &Address, code: Bytecode) {
        match self {
            Self::Fafo { pipe, .. } => pipe.insert_code(address, code),
            Self::Occ(pipe) => pipe.insert_code(address, code).expect("cannot insert code"),
        }
    }

    pub fn run_block(&mut self, transactions: Vec<Vec<Box<Tx>>>, block: Block) {
        match self {
            Self::Fafo { pipe, cache } => {
                *cache = pipe.run_block_with_sim(transactions, block, cache.clone())
            }
            Self::Occ(pipe) => {
                let output = pipe
                    .run_block(transactions, block)
                    .expect("OCC block failed");
                metrics::counter!("occ.attempts").increment(output.stats.attempts as u64);
                metrics::counter!("occ.validation_failures")
                    .increment(output.stats.validation_failures as u64);
                metrics::counter!("occ.persistence_tasks")
                    .increment(output.stats.persistence_tasks as u64);
            }
        }
    }

    pub fn flush(&mut self) {
        match self {
            Self::Fafo { pipe, .. } => pipe.flush(),
            Self::Occ(pipe) => pipe.flush().expect("OCC flush failed"),
        }
    }
}
