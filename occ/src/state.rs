//! Versioned committed state. Only the coordinator writes; workers read snapshots.
use codedb::CodeDB;
use exepipe_common::acc_data::decode_account_info_from_bz;
use parking_lot::{Mutex, RwLock};
use qmdb::def::{OP_CREATE, OP_DELETE, OP_WRITE};
use qmdb::entryfile::readbuf::ReadBuf;
use qmdb::utils::{changeset::ChangeSet, hasher::Hash32};
use qmdb::ADS;
use revm::primitives::{Address, B256};
use revm::state::{AccountInfo, Bytecode};
use std::collections::HashMap;
use std::sync::Arc;

pub(crate) type ReadSet = HashMap<Hash32, u64>;
type Value = Option<Arc<[u8]>>;

struct VersionedValue {
    version: u64,
    value: Value,
}

#[derive(Default)]
struct Inner {
    version: u64,
    values: HashMap<Hash32, Vec<VersionedValue>>,
    codes: HashMap<B256, Bytecode>,
}

#[derive(Default)]
pub(crate) struct VersionedState {
    inner: RwLock<Inner>,
    base_codes: Mutex<HashMap<B256, Bytecode>>,
}

impl VersionedState {
    pub fn version(&self) -> u64 {
        self.inner.read().version
    }

    fn lookup<T: ADS>(
        inner: &Inner,
        ads: &T,
        height: i64,
        snapshot: u64,
        hash: &Hash32,
        key: &[u8],
    ) -> (u64, Value) {
        if let Some(history) = inner.values.get(hash) {
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

    pub fn read<T: ADS>(
        &self,
        ads: &T,
        height: i64,
        snapshot: u64,
        hash: &Hash32,
        key: &[u8],
    ) -> (u64, Value) {
        Self::lookup(&self.inner.read(), ads, height, snapshot, hash, key)
    }

    pub fn account<T: ADS>(
        &self,
        ads: &T,
        code_db: &CodeDB,
        height: i64,
        snapshot: u64,
        address: &Address,
        hash: &Hash32,
    ) -> (u64, Option<AccountInfo>) {
        let inner = self.inner.read();
        let (version, value) =
            Self::lookup(&inner, ads, height, snapshot, hash, address.as_slice());
        let Some(value) = value else {
            return (version, None);
        };
        let mut info = decode_account_info_from_bz(&value);
        if info.is_empty_code_hash() {
            info.code = Some(Bytecode::default());
        } else if info.code.is_none() {
            let code = if let Some(code) = inner.codes.get(&info.code_hash) {
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

    pub fn validate(&self, reads: &ReadSet) -> bool {
        let inner = self.inner.read();
        reads.iter().all(|(key, version)| {
            inner
                .values
                .get(key)
                .map_or(0, |history| history.last().unwrap().version)
                == *version
        })
    }

    /// Publish all writes together. Even deletions retain a version and an
    /// explicit absence value, so reads cannot resurrect a value from QMDB.
    pub fn publish(&self, changes: &ChangeSet, codes: &HashMap<Address, Bytecode>) {
        let mut inner = self.inner.write();
        inner.version += 1;
        let version = inner.version;
        for code in codes.values() {
            inner.codes.insert(code.hash_slow(), code.clone());
        }
        changes.apply_op_in_range(|op, hash, _, value, old_value, _| {
            if !matches!(op, OP_CREATE | OP_WRITE | OP_DELETE) {
                return;
            }
            let history = inner.values.entry(*hash).or_insert_with(|| {
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
    }

    /// Called only after the scoped workers have all stopped. Keep the latest
    /// overlay across blocks until QMDB has drained; discard older snapshots.
    pub fn compact(&self) {
        let mut inner = self.inner.write();
        for history in inner.values.values_mut() {
            let latest = history.pop().unwrap();
            *history = vec![latest];
        }
    }

    /// Between blocks, forget keys whose latest write is already durable.
    /// No snapshot is active here; future reads can use the QMDB base again.
    pub fn prune_durable(&self, version: u64) {
        self.inner
            .write()
            .values
            .retain(|_, history| history.last().unwrap().version > version);
    }

    /// Safe only after persistence and all speculative work have completed.
    pub fn clear(&self) {
        *self.inner.write() = Inner::default();
        self.base_codes.lock().clear();
    }
}
