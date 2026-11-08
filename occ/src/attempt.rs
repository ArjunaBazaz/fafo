//! Execute once without predictions; reuse the unchanged state encoder only
//! after actual writes are known. No FAFO scheduling/access-check code is edited.
use super::state::ReadSet;
use super::tx_context::{Runtime, TxContext};
use crate::Tx;
use exepipe_common::access_set::{
    addr_to_short_hash, extract_short_hash, get_to_addr_access, CrwSets,
};
use exepipe_common::executor::error::ExecutionError;
use exepipe_common::executor::get_change_set::{get_change_set, ChangeSetConfig};
use exepipe_common::revm::run_tx;
use exepipe_common::utils::join_address_index;
use qmdb::utils::{
    changeset::ChangeSet,
    hasher::{self, Hash32},
};
use qmdb::ADS;
use revm::context::{result::ResultAndState, ContextTr, Transaction};
use revm::primitives::{Address, U256};
use revm::state::Bytecode;
use revm::Database;
use std::collections::HashMap;

type Effects = (
    ChangeSet,
    u128,
    u64,
    HashMap<Address, Bytecode>,
    Vec<Hash32>,
    Vec<Hash32>,
);
type ExecutionOutput = (Result<ResultAndState, ExecutionError>, Option<Effects>);

pub(crate) struct Attempt {
    pub reads: ReadSet,
    pub output: ExecutionOutput,
}

fn failed(reads: ReadSet, error: ExecutionError) -> Attempt {
    Attempt {
        reads,
        output: (Err(error), None),
    }
}

pub(crate) fn execute<T: ADS>(runtime: &Runtime<T>, tx: &Tx, snapshot: u64) -> Attempt {
    let mut db = TxContext::new(runtime, snapshot);
    let caller = match db.basic(tx.env.caller) {
        Ok(Some(info)) => info,
        Ok(None) => return failed(db.reads, ExecutionError::CallerNotFound(tx.env.caller)),
        Err(error) => return failed(db.reads, ExecutionError::Other(anyhow::anyhow!(error))),
    };
    let gas_price = tx.env.effective_gas_price(runtime.env.basefee as u128);
    let required = U256::from(gas_price)
        .checked_mul(U256::from(tx.env.gas_limit))
        .and_then(|gas| gas.checked_add(tx.env.value));
    let Some(required) = required else {
        return failed(
            db.reads,
            ExecutionError::Other(anyhow::anyhow!("transaction upfront cost overflow")),
        );
    };
    if caller.balance < required {
        return failed(
            db.reads,
            ExecutionError::InsufficientBalanceForCall {
                balance: caller.balance,
                required,
            },
        );
    }

    let (mut evm, result) = run_tx(&runtime.env, &tx.env, db);
    let db = evm.db();
    let reads = std::mem::take(&mut db.reads);
    let result = match result {
        Ok(result) => result,
        Err(error) => return failed(reads, ExecutionError::EVMError(error)),
    };

    // This is post-execution permission metadata for the shared serializer,
    // NOT a prediction and NOT an OCC dependency set. Validation uses full-key
    // read versions collected above, including reads producing no state change.
    let mut actual_writes = CrwSets::default();
    for (address, account) in &result.state {
        if !account.is_touched() {
            continue;
        }
        actual_writes.write_hashes.push(addr_to_short_hash(address));
        for (index, _) in account.changed_storage_slots() {
            actual_writes
                .write_hashes
                .push(extract_short_hash(&hasher::hash(join_address_index(
                    address, index,
                ))));
        }
    }
    let gas_used = result.result.gas_used();
    let effects = get_change_set(ChangeSetConfig {
        caller: &tx.env.caller,
        to_addr_access: get_to_addr_access(&tx.env),
        gas_price,
        remained_gas_limit: tx.env.gas_limit - gas_used,
        state: &result.state,
        orig_acc_map: &db.orig_acc_map,
        orig_slot_dust_map: &db.orig_slot_dust_map,
        crw_sets: &actual_writes,
    });
    match effects {
        Ok((changes, reward, creation_gas, codes)) => Attempt {
            reads,
            output: (
                Ok(result),
                Some((
                    changes,
                    reward,
                    creation_gas + gas_used,
                    codes,
                    Vec::new(),
                    Vec::new(),
                )),
            ),
        },
        Err(error) => failed(reads, error),
    }
}
