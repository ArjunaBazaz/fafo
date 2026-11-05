use super::state::{ReadSet, VersionedState};
use codedb::CodeDB;
use exepipe_common::executor::exector_database::ExectorDatabase;
use exepipe_common::utils::join_address_index;
use qmdb::utils::hasher;
use qmdb::ADS;
use revm::context::BlockEnv;
use revm::primitives::{Address, B256, U256};
use revm::state::AccountInfo;
use std::collections::HashMap;
use std::sync::Arc;

pub(crate) struct Runtime<T: ADS> {
    pub ads: T,
    pub code_db: Arc<CodeDB>,
    pub state: Arc<VersionedState>,
    pub height: i64,
    pub env: BlockEnv,
    pub block_hashes: HashMap<u64, B256>,
}

pub(crate) struct TxContext<'a, T: ADS> {
    runtime: &'a Runtime<T>,
    snapshot: u64,
    reads: &'a mut ReadSet,
    slot: Option<Arc<[u8]>>,
}

impl<'a, T: ADS> TxContext<'a, T> {
    pub fn new(runtime: &'a Runtime<T>, snapshot: u64, reads: &'a mut ReadSet) -> Self {
        Self {
            runtime,
            snapshot,
            reads,
            slot: None,
        }
    }
}

impl<T: ADS> ExectorDatabase for TxContext<'_, T> {
    fn basic(&mut self, address: &Address) -> Option<AccountInfo> {
        let hash = hasher::hash(address);
        let r = self.runtime;
        let (version, info) =
            r.state
                .account(&r.ads, &r.code_db, r.height, self.snapshot, address, &hash);
        self.reads.insert(hash, version);
        info
    }

    fn storage_value(&mut self, address: Address, index: &U256) -> Option<&[u8]> {
        let key = join_address_index(&address, index);
        let hash = hasher::hash(key);
        let r = self.runtime;
        let (version, value) = r.state.read(&r.ads, r.height, self.snapshot, &hash, &key);
        self.reads.insert(hash, version);
        self.slot = value;
        self.slot.as_deref()
    }

    fn block_hash(&mut self, number: u64) -> Option<B256> {
        // Block environment/history are immutable for the entire block.
        self.runtime.block_hashes.get(&number).copied()
    }
}
