# Ordered OCC executor

`exepipe::occ::OccExecutor` is independent of FAFO's simulator, framer, and
dispatcher. Select it in `bench-sendeth` with `--executor occ`.

```rust,ignore
use exepipe::occ::{OccConfig, OccExecutor};

let mut executor = OccExecutor::new(ads, database_dir, OccConfig {
    workers: 64,
    window: 128,
    ..OccConfig::default()
})?;
// Uses the existing Vec<Vec<Box<Tx>>> and Block input types.
let output = executor.run_block(transactions, block)?;
executor.flush()?; // includes QMDB persistence and Merkleization
```

## Execution

1. Flatten incoming groups and retain input transaction order.
2. Submit at most `window` transactions to the execution pool. Each attempt reads
   a fixed committed version and records full key hashes and read versions,
   including absent values. REVM owns transaction-local writes.
3. Validate the next transaction's reads against the committed state. If stale,
   reexecute it synchronously against the latest state while later commits wait.
4. Publish the complete accepted write set and its versions under one write lock.
   Only then append created bytecode and enqueue its change set to QMDB.
5. End the block after every transaction has a final outcome. Reclaim historical
   versions after workers stop, retaining the latest overlay for following blocks.
   As QMDB completes old blocks, discard their durable overlay keys between blocks.
6. `flush` waits for QMDB and code storage, then discards the overlay.

The calling thread coordinates commits; the configured pool is reserved for
speculative execution. `workers = 1, window = 1` provides a sequential reference
using the same execution and persistence semantics. Retry work executes on the
coordinator, so account for that thread when comparing total CPU budgets.

## Shared code and semantics

The executor shares REVM, account/storage encoding, dust accounting, change-set
construction, transaction types, CodeDB, and QMDB with FAFO. The shared executor
has an explicit access policy: existing FAFO calls still enforce predictions;
OCC ignores predictions and records actual reads in its database adapter.

This retains the repository's current EVM semantics, including disabled nonce
checking and beneficiary rewards. REVM errors in OCC return an error and no
change set rather than invoking FAFO's unimplemented execution-error handler.
EVM reverts are normal validated execution results, not concurrency aborts.
This is not a replacement implementation of Ethereum transaction validity rules.

## Lifecycle and limitations

- Give the executor exclusive ownership of the database execution path. External
  writers would invalidate the version-tracking assumptions.
- Submit consecutive heights starting after QMDB's current height. `run_block`
  completes logical execution, but database work can remain pending until `flush`.
  It owns cross-block state; no FAFO `StateCache` argument is needed.
- Supply historical block hashes with `set_block_hash` when transactions need them.
  `insert_code` is for setup and drains pending persistence before changing code.
- Per-transaction errors are returned in input order. An operational failure
  after block registration poisons the executor: partially committed blocks
  cannot be retried on that instance. Recover the database before resuming.
- Persistence groups four finalized transactions per QMDB task by default, like
  FAFO's target task size. Set `persistence_batch_size` to change storage batching;
  this never groups validation or retries into a single transaction.
- Historical versions are retained for one block. Latest writes remain in the
  overlay until QMDB completes their blocks or `flush` drains it. Committed code
  is cached until `flush`. Memory depends on the in-flight write working set
  and block size. This baseline prioritizes correctness;
  it does not implement dependency prediction, speculative-write forwarding,
  parallel validation, or automatic abort backoff.

The benchmark gives both schedulers the same total thread budget (102 by default)
and inherited CPU affinity, with `--cpu-cores N` available to restrict both to the
same logical CPUs. OCC receives 101 workers plus the calling coordinator; FAFO
distributes its budget among execution, simulation, warmup and coordination.
Shared QMDB/CodeDB workers are additional and identical. See the benchmark README
for the allocation breakdown and resource metrics.

Run the focused tests in the repository's Linux development environment:

```bash
cargo test -p exepipe --lib occ::tests
```
