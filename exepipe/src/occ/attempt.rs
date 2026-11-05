use super::state::ReadSet;
use super::tx_context::{Runtime, TxContext};
use crate::exetask::Tx;
use exepipe_common::access_set::CrwSets;
use exepipe_common::executor::execute_tx::{execute_tx_with_policy, ExecutionOutput};
use exepipe_common::executor::AccessPolicy;
use qmdb::ADS;

pub(crate) struct Attempt {
    pub reads: ReadSet,
    pub output: ExecutionOutput,
}

pub(crate) fn execute<T: ADS>(runtime: &Runtime<T>, tx: &Tx, snapshot: u64) -> Attempt {
    let mut reads = ReadSet::new();
    let db = TxContext::new(runtime, snapshot, &mut reads);
    let output = execute_tx_with_policy(
        db,
        &runtime.env,
        &tx.env,
        &CrwSets::default(),
        AccessPolicy::Unrestricted,
    );
    Attempt { reads, output }
}
