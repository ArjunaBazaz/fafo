//! Versioned committed state. Only the coordinator writes; workers read snapshots.
use super::timing::{LockTimings, TimedGuard};
use codedb::CodeDB;
use exepipe_common::acc_data::decode_account_info_from_bz;
use parking_lot::{Mutex, RwLock};
use qmdb::def::{BYTES_CACHE_SHARD_COUNT, OP_CREATE, OP_DELETE, OP_WRITE};
use qmdb::entryfile::readbuf::ReadBuf;
use qmdb::utils::{changeset::ChangeSet, hasher::Hash32};
use qmdb::ADS;
use revm::primitives::{Address, B256};
use revm::state::{AccountInfo, Bytecode};
use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

pub(crate) type ReadSet = HashMap<Hash32, u64>;
type Value = Option<Arc<[u8]>>;

struct VersionedValue {
    version: u64,
    value: Value,
}

type Values = HashMap<Hash32, Vec<VersionedValue>>;

pub(crate) struct VersionedState {
    // Only the coordinator publishes. The watermark advances after all of a
    // transaction's writes are installed, so snapshots never see half a commit.
    version: AtomicU64,
    shards: [RwLock<Values>; BYTES_CACHE_SHARD_COUNT],
    codes: RwLock<HashMap<B256, Bytecode>>,
    base_codes: Mutex<HashMap<B256, Bytecode>>,
}

impl Default for VersionedState {
    fn default() -> Self {
        Self {
            version: AtomicU64::new(0),
            shards: std::array::from_fn(|_| RwLock::new(HashMap::new())),
            codes: RwLock::new(HashMap::new()),
            base_codes: Mutex::new(HashMap::new()),
        }
    }
}

impl VersionedState {
    fn shard(&self, hash: &Hash32) -> &RwLock<Values> {
        // Same partitioning as FAFO's byte cache.
        let index = u64::from_be_bytes(hash[..8].try_into().unwrap()) as usize;
        &self.shards[index % BYTES_CACHE_SHARD_COUNT]
    }

    pub fn version(&self) -> u64 {
        self.version.load(Ordering::Acquire)
    }

    fn lookup<T: ADS>(
        values: &Values,
        ads: &T,
        height: i64,
        snapshot: u64,
        hash: &Hash32,
        key: &[u8],
    ) -> (u64, Value) {
        if let Some(history) = values.get(hash) {
            let index = history.partition_point(|entry| entry.version <= snapshot);
            let entry = &history[index.checked_sub(1).expect("snapshot was reclaimed")];
            return (entry.version, entry.value.clone());
        }
        // Keep the read guard held across the base read. A commit installs its
        // old value in the overlay before QMDB can update that key underneath us.
        let mut buf = ReadBuf::new();
        ads.read_entry(height, hash, key, &mut buf);
        if buf.is_empty() {
            (0, None)
        } else {
            (0, Some(Arc::from(buf.as_entry_bz().value())))
        }
    }

    #[cfg(test)]
    pub fn read<T: ADS>(
        &self,
        ads: &T,
        height: i64,
        snapshot: u64,
        hash: &Hash32,
        key: &[u8],
    ) -> (u64, Value) {
        self.read_timed(ads, height, snapshot, hash, key, None)
    }

    pub fn read_timed<T: ADS>(
        &self,
        ads: &T,
        height: i64,
        snapshot: u64,
        hash: &Hash32,
        key: &[u8],
        timing: Option<&mut LockTimings>,
    ) -> (u64, Value) {
        Self::lookup(
            &TimedGuard::new(|| self.shard(hash).read(), timing),
            ads,
            height,
            snapshot,
            hash,
            key,
        )
    }

    #[cfg(test)]
    pub fn account<T: ADS>(
        &self,
        ads: &T,
        code_db: &CodeDB,
        height: i64,
        snapshot: u64,
        address: &Address,
        hash: &Hash32,
    ) -> (u64, Option<AccountInfo>) {
        self.account_timed(ads, code_db, height, snapshot, address, hash, None)
    }

    // Preserve the account lookup arguments and add only a local timing sink.
    #[allow(clippy::too_many_arguments)]
    pub fn account_timed<T: ADS>(
        &self,
        ads: &T,
        code_db: &CodeDB,
        height: i64,
        snapshot: u64,
        address: &Address,
        hash: &Hash32,
        timing: Option<&mut LockTimings>,
    ) -> (u64, Option<AccountInfo>) {
        let (version, value) =
            self.read_timed(ads, height, snapshot, hash, address.as_slice(), timing);
        // Immutable bytes and bytecode can be decoded outside the shard lock.
        let Some(value) = value else {
            return (version, None);
        };
        let mut info = decode_account_info_from_bz(&value);
        if info.is_empty_code_hash() {
            info.code = Some(Bytecode::default());
        } else if info.code.is_none() {
            let code = if let Some(code) = self.codes.read().get(&info.code_hash) {
                code.clone()
            } else {
                let mut cache = self.base_codes.lock();
                cache
                    .entry(info.code_hash)
                    .or_insert_with(|| {
                        let code = code_db.get_byte_code(address);
                        assert_eq!(code.hash_slow(), info.code_hash);
                        code
                    })
                    .clone()
            };
            info.code = Some(code);
        }
        (version, Some(info))
    }

    #[cfg(test)]
    pub fn validate(&self, reads: &ReadSet) -> bool {
        self.validate_timed(reads, None)
    }

    pub fn validate_timed(&self, reads: &ReadSet, mut timing: Option<&mut LockTimings>) -> bool {
        // The sole coordinator is also the sole publisher: no commit can race
        // this validation, even though each key has its own shard guard.
        reads.iter().all(|(key, version)| {
            let values = TimedGuard::new(|| self.shard(key).read(), timing.as_deref_mut());
            values
                .get(key)
                .map_or(0, |history| history.last().unwrap().version)
                == *version
        })
    }

    /// Publish all writes together. Even deletions retain a version and an
    /// explicit absence value, so reads cannot resurrect a value from QMDB.
    #[cfg(test)]
    pub fn publish(&self, changes: &ChangeSet, codes: &HashMap<Address, Bytecode>) {
        self.publish_timed(changes, codes, None)
    }

    pub fn publish_timed(
        &self,
        changes: &ChangeSet,
        codes: &HashMap<Address, Bytecode>,
        mut timing: Option<&mut LockTimings>,
    ) {
        let version = self.version() + 1;
        if !codes.is_empty() {
            let mut committed_codes = self.codes.write();
            for code in codes.values() {
                committed_codes.insert(code.hash_slow(), code.clone());
            }
        }
        changes.apply_op_in_range(|op, hash, _, value, old_value, _| {
            if !matches!(op, OP_CREATE | OP_WRITE | OP_DELETE) {
                return;
            }
            let mut values = TimedGuard::new(|| self.shard(hash).write(), timing.as_deref_mut());
            let history = values.entry(*hash).or_insert_with(|| {
                vec![VersionedValue {
                    version: 0,
                    value: if op == OP_CREATE {
                        None
                    } else {
                        Some(Arc::from(old_value))
                    },
                }]
            });
            history.push(VersionedValue {
                version,
                value: if op == OP_DELETE {
                    None
                } else {
                    Some(Arc::from(value))
                },
            });
        });
        self.version.store(version, Ordering::Release);
    }

    /// Called only after the scoped workers have all stopped. Keep the latest
    /// overlay across blocks until QMDB has drained; discard older snapshots.
    pub fn compact(&self) {
        for shard in &self.shards {
            for history in shard.write().values_mut() {
                if history.len() > 1 {
                    let latest = history.pop().unwrap();
                    history.clear();
                    history.push(latest); // Reuse the allocation across blocks.
                }
            }
        }
    }

    /// Between blocks, forget keys whose latest write is already durable.
    /// No snapshot is active here; future reads can use the QMDB base again.
    pub fn prune_durable(&self, version: u64) {
        for shard in &self.shards {
            shard
                .write()
                .retain(|_, history| history.last().unwrap().version > version);
        }
    }

    /// Safe only after persistence and all speculative work have completed.
    pub fn clear(&self) {
        for shard in &self.shards {
            *shard.write() = HashMap::new();
        }
        self.codes.write().clear();
        self.base_codes.lock().clear();
        self.version.store(0, Ordering::Release);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use exepipe::test_helper::MockADS;
    use std::time::{Duration, Instant};

    #[test]
    fn snapshot_watermark_hides_partial_cross_shard_publication() {
        let state = VersionedState::default();
        let ads = MockADS::new();
        let mut first = [0; 32];
        first[7] = 1;
        let mut second = [0; 32];
        second[0] = 1;
        second[7] = 2;
        let mut changes = ChangeSet::new();
        changes.add_op_with_old_value(OP_CREATE, 0, &first, b"first", b"new", b"", None);
        changes.add_op_with_old_value(OP_CREATE, 1, &second, b"second", b"new", b"", None);
        changes.sort();
        // Pause publication after its first shard, before its second shard.
        let held = state.shard(&second).write();
        std::thread::scope(|scope| {
            let writer = scope.spawn(|| state.publish(&changes, &HashMap::new()));
            let deadline = Instant::now() + Duration::from_secs(5);
            let mut partially_installed = false;
            while Instant::now() < deadline {
                if state.shard(&first).read().contains_key(&first) {
                    partially_installed = true;
                    break;
                }
                std::thread::yield_now();
            }
            let snapshot = state.version();
            let old_first = state.read(&ads, 1, snapshot, &first, b"first").1;
            // Release before asserting, so failed assertions cannot strand the
            // scoped publisher behind a guard held by this thread.
            drop(held);
            writer.join().unwrap();
            assert!(partially_installed);
            assert_eq!(snapshot, 0);
            assert!(old_first.is_none());
            assert!(state
                .read(&ads, 1, snapshot, &second, b"second")
                .1
                .is_none());
            let snapshot = state.version();
            assert_eq!(snapshot, 1);
            for (hash, key) in [(first, b"first".as_slice()), (second, b"second".as_slice())] {
                assert_eq!(
                    state
                        .read(&ads, 1, snapshot, &hash, key)
                        .1
                        .unwrap()
                        .as_ref(),
                    b"new"
                );
            }
        });
    }
}
