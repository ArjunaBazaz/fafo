//! Actual reads are recorded here, independently of FAFO's prediction proxy.
use super::state::{ReadSet, VersionedState};
use codedb::CodeDB;
use exepipe_common::executor::error::DBError;
use exepipe_common::slot_entry_value::SlotEntryValue;
use exepipe_common::utils::join_address_index;
use qmdb::utils::hasher::{self, Hash32};
use qmdb::ADS;
use revm::context::BlockEnv;
use revm::primitives::{Address, B256, U256};
use revm::state::{AccountInfo, Bytecode};
use revm::Database;
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
    pub reads: ReadSet,
    pub orig_acc_map: HashMap<Address, Option<AccountInfo>>,
    pub orig_slot_dust_map: HashMap<Hash32, Option<u128>>,
}

impl<'a, T: ADS> TxContext<'a, T> {
    pub fn new(runtime: &'a Runtime<T>, snapshot: u64) -> Self {
        Self {
            runtime,
            snapshot,
            reads: ReadSet::new(),
            orig_acc_map: HashMap::new(),
            orig_slot_dust_map: HashMap::new(),
        }
    }
}

impl<T: ADS> Database for TxContext<'_, T> {
    type Error = DBError;

    fn basic(&mut self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
        if let Some(info) = self.orig_acc_map.get(&address) {
            return Ok(info.clone());
        }
        let hash = hasher::hash(address);
        let r = self.runtime;
        let (version, info) =
            r.state
                .account(&r.ads, &r.code_db, r.height, self.snapshot, &address, &hash);
        self.reads.insert(hash, version);
        self.orig_acc_map.insert(address, info.clone());
        Ok(info)
    }

    fn storage(&mut self, address: Address, index: U256) -> Result<U256, Self::Error> {
        let key = join_address_index(&address, &index);
        let hash = hasher::hash(key);
        let r = self.runtime;
        let (version, value) = r.state.read(&r.ads, r.height, self.snapshot, &hash, &key);
        self.reads.insert(hash, version);
        match value {
            None => {
                self.orig_slot_dust_map.insert(hash, None);
                Ok(U256::ZERO)
            }
            Some(value) => {
                let entry = SlotEntryValue::from_bz(&value);
                self.orig_slot_dust_map.insert(hash, Some(entry.get_dust()));
                Ok(U256::from_be_slice(entry.get_value()))
            }
        }
    }

    fn code_by_hash(&mut self, hash: B256) -> Result<Bytecode, Self::Error> {
        // basic() supplies bytecode by the versioned account's code identity,
        // matching the shared REVM path's embedded-code convention.
        Err(DBError::Other(anyhow::anyhow!(
            "code_by_hash {hash} is unsupported; load the account first"
        )))
    }

    fn block_hash(&mut self, number: u64) -> Result<B256, Self::Error> {
        self.runtime
            .block_hashes
            .get(&number)
            .copied()
            .ok_or_else(|| {
                DBError::Other(anyhow::anyhow!("missing historical block hash {number}"))
            })
    }
}
