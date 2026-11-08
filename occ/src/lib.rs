//! Ordered optimistic concurrency control, independent of FAFO's simulator,
//! framer and dispatcher. Transactions execute privately against versioned
//! snapshots; one coordinator validates and publishes them in input order.
//!
//! `run_block` finishes logical execution and queues QMDB writes. Call `flush`
//! before measuring completed persistence or reading final roots. The executor
//! owns its inter-block overlay; callers do not pass FAFO's state cache.
mod attempt;
mod scheduler;
mod state;
mod tx_context;

use anyhow::{ensure, Result};
use codedb::CodeDB;
pub use exepipe::exetask;
pub use exepipe::exetask::{ExeTask, Tx};
pub use exepipe::Block;
use exepipe_common::executor::error::ExecutionError;
use qmdb::tasks::taskid::join_task_id;
use qmdb::tasks::TasksManager;
use qmdb::{AdsWrap, ADS};
use rayon::{ThreadPool, ThreadPoolBuilder};
use revm::context::result::ResultAndState;
use revm::primitives::{Address, B256};
use revm::state::Bytecode;
use state::VersionedState;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use tx_context::Runtime;

#[derive(Clone, Copy, Debug)]
pub struct OccConfig {
    pub workers: usize,
    /// Maximum number of transactions submitted but not yet committed.
    pub window: usize,
    /// Persistence grouping only; validation/commit remain per transaction.
    pub persistence_batch_size: usize,
}

impl Default for OccConfig {
    fn default() -> Self {
        Self {
            workers: 101,
            window: 128,
            persistence_batch_size: exepipe::dispatcher::def::TX_IN_TASK,
        }
    }
}

impl OccConfig {
    /// Includes the calling coordinator; shared storage threads are additional.
    /// The default 102 matches the original FAFO scheduler's peak allocation.
    pub fn with_thread_budget(total: usize) -> Result<Self> {
        ensure!(
            total >= 2,
            "OCC needs a coordinator and at least one worker"
        );
        Ok(Self {
            workers: total - 1,
            ..Self::default()
        })
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct OccStats {
    pub finalized: usize,
    pub attempts: usize,
    pub validation_failures: usize,
    /// Transactions producing a change set, including EVM reverts with fees.
    pub applied: usize,
    /// Final executor errors; EVM reverts are represented in execution results.
    pub errors: usize,
    /// QMDB tasks containing finalized transactions (excluding end-block).
    pub persistence_tasks: usize,
}

pub struct OccBlockResult {
    pub results: Vec<Result<ResultAndState, ExecutionError>>,
    pub stats: OccStats,
}

pub struct OccExecutor {
    ads: AdsWrap<ExeTask>,
    code_db: Arc<CodeDB>,
    pool: ThreadPool,
    config: OccConfig,
    state: Arc<VersionedState>,
    block_hashes: HashMap<u64, B256>,
    last_height: i64,
    poisoned: bool,
    pending_blocks: VecDeque<(i64, u64)>,
}

impl OccExecutor {
    /// Takes exclusive ownership of the database execution path. Do not run a
    /// FAFO executor or another writer concurrently against the same database.
    pub fn new(ads: AdsWrap<ExeTask>, dir: &str, config: OccConfig) -> Result<Self> {
        ensure!(config.workers > 0, "OCC workers must be positive");
        ensure!(config.window > 0, "OCC window must be positive");
        ensure!(
            config.persistence_batch_size > 0,
            "OCC persistence batch size must be positive"
        );
        let meta = ads.get_metadb();
        let meta = meta.read();
        let last_height = meta.get_curr_height();
        let extra = meta.get_extra_data();
        let code_size = extra
            .split(':')
            .next()
            .and_then(|s| s.parse().ok())
            .unwrap_or(-1);
        drop(meta);
        Ok(Self {
            ads,
            code_db: Arc::new(CodeDB::new(
                dir,
                code_size,
                8 * 1024 * 1024,
                256 * 1024 * 1024,
            )),
            pool: ThreadPoolBuilder::new()
                .num_threads(config.workers)
                .build()?,
            config,
            state: Arc::new(VersionedState::default()),
            block_hashes: HashMap::new(),
            last_height,
            poisoned: false,
            pending_blocks: VecDeque::new(),
        })
    }

    pub fn insert_code(&mut self, address: &Address, code: Bytecode) -> Result<()> {
        ensure!(!self.poisoned, "OCC executor has an incomplete block");
        // Setup only: drain and discard cached snapshots before external writes.
        self.flush()?;
        self.code_db.append(address, code, 0)?;
        Ok(())
    }

    pub fn set_block_hash(&mut self, height: u64, hash: B256) {
        self.block_hashes.insert(height, hash);
    }

    /// Flatten the input groups and commit in their original transaction order.
    /// Predicted access sets are ignored. No speculative changes reach QMDB.
    /// Per-transaction failures are returned in `results`; operational failures
    /// poison the executor because already committed work cannot be rolled back.
    /// `block.env.number` is the authoritative height: the existing `Block`
    /// header fields are private to exepipe. The benchmark already sets both.
    pub fn run_block(
        &mut self,
        task_in: Vec<Vec<Box<Tx>>>,
        block: Block,
    ) -> Result<OccBlockResult> {
        ensure!(!self.poisoned, "OCC executor has an incomplete block");
        let height = i64::try_from(block.env.number)?;
        ensure!(
            height == self.last_height + 1,
            "OCC blocks must have consecutive heights"
        );
        let transactions: Vec<_> = task_in.into_iter().flatten().collect();
        ensure!(
            transactions.len() < (1 << 24),
            "too many OCC persistence tasks in block"
        );
        let batch_size = self.config.persistence_batch_size;
        let persistence_tasks = transactions.len().div_ceil(batch_size);
        let manager = Arc::new(TasksManager::with_size(persistence_tasks + 1));
        manager.set_valid_count(persistence_tasks + 1);
        let (started, durable) = self.ads.start_block(height, manager.clone());
        ensure!(started, "QMDB refused to start OCC block");
        if let Some(durable) = durable {
            let mut through_version = None;
            while self
                .pending_blocks
                .front()
                .is_some_and(|(height, _)| *height <= durable.curr_height)
            {
                through_version = self.pending_blocks.pop_front().map(|(_, version)| version);
            }
            if let Some(version) = through_version {
                self.state.prune_durable(version);
            }
        }
        self.poisoned = true;
        let runtime = Runtime {
            ads: self.ads.get_shared(),
            code_db: self.code_db.clone(),
            state: self.state.clone(),
            height,
            env: block.env,
            block_hashes: self.block_hashes.clone(),
        };
        let mut results = Vec::with_capacity(transactions.len());
        let mut pending_changes = Vec::with_capacity(batch_size);
        let mut stats = scheduler::run(
            &runtime,
            &transactions,
            &self.pool,
            self.config.window,
            |index, attempt| {
                if let Some((change_set, _, _, codes, _, _)) = attempt.output.1 {
                    for (address, code) in codes {
                        self.code_db.append(
                            &address,
                            code,
                            join_task_id(height, index / batch_size, false),
                        )?;
                    }
                    pending_changes.push(change_set);
                }
                if (index + 1) % batch_size == 0 || index + 1 == transactions.len() {
                    Self::send_task(
                        &runtime.ads,
                        &manager,
                        height,
                        index / batch_size,
                        false,
                        std::mem::replace(&mut pending_changes, Vec::with_capacity(batch_size)),
                    );
                }
                results.push(attempt.output.0);
                Ok(())
            },
        )?;
        stats.persistence_tasks = persistence_tasks;
        // Match the current FAFO path: beneficiary rewards are disabled in the
        // shared EVM handler and its block gas-fee accumulator is not populated.
        self.code_db.flush()?;
        runtime.ads.insert_extra_data(height, String::new());
        Self::send_task(
            &runtime.ads,
            &manager,
            height,
            persistence_tasks,
            true,
            Vec::new(),
        );
        self.state.compact();
        self.pending_blocks
            .push_back((height, self.state.version()));
        self.last_height = height;
        self.poisoned = false;
        Ok(OccBlockResult { results, stats })
    }

    fn send_task<T: ADS>(
        ads: &T,
        manager: &TasksManager<ExeTask>,
        height: i64,
        index: usize,
        end_block: bool,
        changes: Vec<qmdb::utils::changeset::ChangeSet>,
    ) {
        // QMDB only needs Task::get_change_sets; transaction results stay above.
        let mut task = ExeTask::new(Vec::new());
        task.set_change_sets(Arc::new(changes));
        manager.set_task(index, task);
        let task_id = join_task_id(height, index, end_block);
        ads.fetch_prev_entries(task_id);
        ads.add_task_to_updater(task_id);
    }

    /// Drain QMDB and code writes, then release the inter-block state overlay.
    pub fn flush(&mut self) -> Result<()> {
        ensure!(!self.poisoned, "OCC executor has an incomplete block");
        self.ads.flush();
        self.code_db.flush()?;
        self.state.clear();
        self.pending_blocks.clear();
        Ok(())
    }
}

#[cfg(test)]
mod tests;
