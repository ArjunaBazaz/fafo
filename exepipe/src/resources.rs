//! Scheduler thread budgets, excluding shared database and benchmark threads.
use crate::dispatcher::def::SCAN_COUNT;
use crate::simulator::simulator::SIM_SHARD_COUNT;
use anyhow::{ensure, Result};

pub const FAFO_WARMUP_THREADS: usize = 12;
pub const FAFO_SIMULATION_THREADS: usize = 16;
pub const COORDINATOR_THREADS: usize = 1;
pub const FRAMER_THREADS: usize = 1;
pub const FAFO_AUXILIARY_THREADS: usize = FAFO_WARMUP_THREADS
    + FAFO_SIMULATION_THREADS
    + SCAN_COUNT
    + SIM_SHARD_COUNT
    + FRAMER_THREADS
    + COORDINATOR_THREADS;
pub const DEFAULT_SCHEDULER_THREADS: usize = 64 + FAFO_AUXILIARY_THREADS;

#[derive(Clone, Copy, Debug)]
pub struct ExecutionResources {
    scheduler_threads: usize,
}

impl Default for ExecutionResources {
    fn default() -> Self {
        Self {
            scheduler_threads: DEFAULT_SCHEDULER_THREADS,
        }
    }
}

impl ExecutionResources {
    pub fn new(scheduler_threads: usize) -> Result<Self> {
        ensure!(scheduler_threads > FAFO_AUXILIARY_THREADS,
            "scheduler thread budget must be at least {} for FAFO's auxiliary threads and one execution worker",
            FAFO_AUXILIARY_THREADS + 1);
        Ok(Self { scheduler_threads })
    }
    pub fn scheduler_threads(self) -> usize {
        self.scheduler_threads
    }
    pub fn fafo_execution_workers(self) -> usize {
        self.scheduler_threads - FAFO_AUXILIARY_THREADS
    }
    pub fn occ_execution_workers(self) -> usize {
        self.scheduler_threads - COORDINATOR_THREADS
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn both_executors_have_the_same_total_budget() {
        for total in [FAFO_AUXILIARY_THREADS + 1, DEFAULT_SCHEDULER_THREADS, 128] {
            let resources = ExecutionResources::new(total).unwrap();
            assert_eq!(
                resources.fafo_execution_workers() + FAFO_AUXILIARY_THREADS,
                total
            );
            assert_eq!(
                resources.occ_execution_workers() + COORDINATOR_THREADS,
                total
            );
        }
        assert_eq!(ExecutionResources::default().fafo_execution_workers(), 64);
        assert!(ExecutionResources::new(FAFO_AUXILIARY_THREADS).is_err());
    }
}
