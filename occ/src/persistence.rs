//! Ordered persistence behind a bounded queue. This worker is reserved from the
//! executor's existing thread budget, rather than added on top of it.
use super::attempt::Attempt;
use super::state::ReadSet;
use super::timing::{Clock, PersistenceTimings};
use super::{ExeTask, OccExecutor};
use anyhow::{anyhow, Result};
use codedb::CodeDB;
use crossbeam::channel::bounded;
use qmdb::tasks::{taskid::join_task_id, TasksManager};
use qmdb::utils::changeset::ChangeSet;
use qmdb::ADS;
use revm::{primitives::Address, state::Bytecode};

pub(crate) struct Batch {
    pub index: usize,
    pub changes: Vec<ChangeSet>,
    pub codes: Vec<(Address, Bytecode)>,
    // Reclaim accepted read metadata and rejected speculative results away
    // from the ordered validator. The bounded queue also bounds this garbage.
    pub retired: Vec<(ReadSet, Option<Attempt>)>,
}

impl Batch {
    fn persist<T: ADS>(
        self,
        ads: &T,
        code_db: &CodeDB,
        manager: &TasksManager<ExeTask>,
        height: i64,
        enabled: bool,
        timing: &mut PersistenceTimings,
    ) -> Result<()> {
        let total = Clock::start(enabled);
        let code_clock = Clock::start(enabled);
        for (address, code) in self.codes {
            code_db.append(&address, code, join_task_id(height, self.index, false))?;
        }
        timing.code_ns += code_clock.elapsed_ns();
        let task_clock = Clock::start(enabled);
        OccExecutor::send_task_with_timings(
            ads,
            manager,
            height,
            self.index,
            false,
            self.changes,
            enabled.then_some(&mut *timing),
        );
        timing.task_ns += task_clock.elapsed_ns();
        let cleanup_clock = Clock::start(enabled);
        drop(self.retired);
        timing.cleanup_ns += cleanup_clock.elapsed_ns();
        timing.worker_ns += total.elapsed_ns();
        Ok(())
    }
}

/// Always join the persister, including on scheduler errors. Registration,
/// prefetch and updater submission retain their original FIFO task order.
#[allow(clippy::too_many_arguments)]
pub(crate) fn run<T: ADS, R>(
    ads: &T,
    code_db: &CodeDB,
    manager: &TasksManager<ExeTask>,
    height: i64,
    background: bool,
    capacity: usize,
    enabled: bool,
    schedule: impl FnOnce(&mut (dyn FnMut(Batch) -> Result<()> + Send)) -> Result<R>,
) -> Result<(R, PersistenceTimings)> {
    if !background {
        let mut timing = PersistenceTimings::default();
        let result = schedule(&mut |batch| {
            batch.persist(ads, code_db, manager, height, enabled, &mut timing)
        })?;
        return Ok((result, timing));
    }
    std::thread::scope(|scope| {
        let (sender, receiver) = bounded::<Batch>(capacity.max(1));
        let worker = std::thread::Builder::new()
            .name("occ-persistence".into())
            .spawn_scoped(scope, move || {
                let mut timing = PersistenceTimings::default();
                for batch in receiver {
                    batch.persist(ads, code_db, manager, height, enabled, &mut timing)?;
                }
                Ok::<_, anyhow::Error>(timing)
            })?;
        let mut enqueue_ns = 0;
        let result = schedule(&mut |batch| {
            let clock = Clock::start(enabled);
            let result = sender.send(batch);
            enqueue_ns += clock.elapsed_ns();
            result.map_err(|_| anyhow!("OCC persistence worker disconnected"))
        });
        let drain_clock = Clock::start(enabled);
        drop(sender);
        let mut timing = worker
            .join()
            .map_err(|_| anyhow!("OCC persistence worker panicked"))??;
        timing.enqueue_ns = enqueue_ns;
        timing.drain_ns = drain_clock.elapsed_ns();
        Ok((result?, timing))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use exepipe::test_helper::MockADS;

    fn empty_batch(index: usize) -> Batch {
        Batch {
            index,
            changes: Vec::new(),
            codes: Vec::new(),
            retired: Vec::new(),
        }
    }

    #[test]
    fn scheduling_errors_still_drain_already_accepted_batches() {
        let temp = tempfile::tempdir().unwrap();
        let code_db = CodeDB::new(temp.path().to_str().unwrap(), -1, 4096, 1024 * 1024);
        for background in [false, true] {
            let manager = TasksManager::with_size(9);
            let result: Result<((), _)> = run(
                &MockADS::new(),
                &code_db,
                &manager,
                1,
                background,
                1,
                true,
                |send| {
                    for index in 0..9 {
                        let mut batch = empty_batch(index);
                        if index == 8 {
                            batch.codes.push((
                                Address::ZERO,
                                Bytecode::new_raw(vec![0x60, 1, 0x50, 0].into()),
                            ));
                        }
                        send(batch)?;
                    }
                    anyhow::bail!("intentional scheduler error")
                },
            );
            assert!(result
                .unwrap_err()
                .to_string()
                .contains("intentional scheduler error"));
            for index in 0..9 {
                assert!(manager.task_for_read(index).is_some());
            }
            assert_eq!(
                code_db.get_byte_code(&Address::ZERO).original_byte_slice(),
                &[0x60, 1, 0x50, 0]
            );
        }
    }

    #[test]
    #[cfg(panic = "unwind")]
    fn worker_panics_disconnect_a_full_queue_and_return_an_error() {
        let temp = tempfile::tempdir().unwrap();
        let code_db = CodeDB::new(temp.path().to_str().unwrap(), -1, 4096, 1024 * 1024);
        // Every registration fails: this manager deliberately has no slots.
        let manager = TasksManager::with_size(0);
        let result = run(
            &MockADS::new(),
            &code_db,
            &manager,
            1,
            true,
            1,
            false,
            |send| {
                for index in 0..9 {
                    send(empty_batch(index))?;
                }
                Ok(())
            },
        );
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("persistence worker panicked"));
    }
}
