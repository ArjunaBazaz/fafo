# Isolated ordered OCC executor

All OCC code lives in this directory. This is a **standalone Cargo workspace**,
not a member of the repository's original workspace. The benchmark now consumes
it through a path dependency and a thin adapter in
`bench-sendeth/src/common/occ_adapter.rs`. The original FAFO cache handoff,
scheduler, thread allocation, metrics, and workload reader are left untouched.

## Build and test

Use the repository's Linux/WSL development environment and Rust toolchain:

```bash
cargo check --manifest-path occ/Cargo.toml
cargo test --manifest-path occ/Cargo.toml
cargo clippy --manifest-path occ/Cargo.toml --lib
```

Benchmark builds include OCC; standalone OCC builds use its own `Cargo.lock`.
Its release profile matches the original workspace's release settings.

## Benchmark adapter

The benchmark selects `--executor fafo|occ` (default `fafo`) and uses this dependency:

```toml
fafo-occ = { path = "../occ" }
```

The adapter reuses the existing `AdsWrap<ExeTask>`, `Vec<Vec<Box<Tx>>>`, `Block`,
and optional bytecode setup. It selects exactly one executor at construction:

```rust,ignore
use fafo_occ::{OccConfig, OccExecutor};

let mut executor = OccExecutor::new(ads, database_dir, OccConfig::default())?;
// If applicable: executor.insert_code(&contract_address, bytecode)?;
for (task_in, block) in blocks {
    let output = executor.run_block(task_in, block)?;
    // Results are in input order. Record output.stats only for this mode.
}
executor.flush()?; // wait for QMDB persistence/Merkleization before stopping timing
```

The FAFO branch continues calling the original `ExePipe::run_block_with_sim`
and passing its original cache argument. The OCC branch calls
`run_block` without a cache argument, because OCC owns its inter-block overlay.
It does not instantiate an unused FAFO executor or call its simulator/framer.
`Block.env.number` supplies OCC's block height; the current benchmark already
sets it to the same height as the private block header. Heights must be consecutive.

The adapter shares input preparation and timing boundaries, waits for the
selected executor's final `flush`, and keeps FAFO-only framer metrics conditional.
OCC emits no global metrics and does not change the existing metric recorder.
Its returned statistics count finalized transactions, attempts, validation
failures, applied change sets, final errors, and QMDB persistence tasks.

For the usual helper script, run the same command with either executor:

```bash
bash bench-sendeth/run.sh --size fast --token eth --executor fafo
bash bench-sendeth/run.sh --size fast --token eth --executor occ
```

The direct Cargo command also accepts the flag before or after `replay-workload`:

```bash
cargo run --release -p bench-sendeth --bin bench-sendeth -- \
  --workload eth --executor occ replay-workload \
  --blocks-workload-filename /path/to/blocks.dat
```

Supply the same database directory and sizing flags used to initialize and
prepare that workload. Both modes use the same reference database and workload
file. FAFO retains its `_run` directory and default metric filenames; OCC uses
`_run_occ` and an `occ_` metrics filename prefix to avoid overwriting FAFO runs.
An explicit `--output` still selects the requested metrics file in either mode.
OCC statistics use `occ.*`; common throughput/duration metrics keep their names.
Final OCC executor errors fail the benchmark; EVM reverts remain normal results.

The default allocation is 101 speculative workers plus the calling coordinator:
102 scheduler threads, matching the original FAFO scheduler's peak total
(64 execution, 16 simulation, 12 warmup, four scanners, four simulator shards,
one framer, and one coordinator). Shared QMDB/CodeDB workers are additional.
`OccConfig::with_thread_budget(total)` changes that total without altering FAFO.
Use the same process affinity/container limits for later comparisons. This
crate does not alter CPU affinity or impose a memory quota on either executor.

## Execution and state

Workers execute private attempts against a versioned committed snapshot and
record actual account/storage reads, including absence reads and storage dust
metadata. A snapshot is acquired when the worker starts, not when work is queued.
The coordinator validates and commits in input transaction order. A stale head
is discarded and reexecuted against the current committed state while later
commits wait, ensuring progress. Failed attempts are validated too: a previous
transfer may make an initially missing or underfunded caller valid.

Accepted writes publish atomically to the versioned overlay; created bytecode
and QMDB changes are submitted only after validation. Storage deletions retain
versioned tombstones. Old snapshot versions are reclaimed after workers finish.
Latest values remain available across blocks while QMDB catches up, then their
overlay entries are removed when QMDB reports those blocks complete. `flush`
drains storage and releases the remaining overlay. Give OCC exclusive ownership
of the database writing path; external writers bypass its validation metadata.

Validation/retry granularity is one transaction. Persistence batches up to four
finalized transactions per task by default, using the original `ExeTask` as a
change-set container. `persistence_batch_size` changes only storage batching.
`workers = 1, window = 1` is a sequential reference using the same semantics.

## What is reused without modification

- Original transaction/block types and QMDB task interfaces.
- REVM's existing `run_tx` handler/configuration.
- Account/storage encoding and `get_change_set`, including dust accounting.
- CodeDB and QMDB persistence/Merkleization.

FAFO's `ExecutorProxy` enforces predicted access sets, so OCC does not call it.
Instead its own REVM database adapter records actual reads. After execution,
OCC derives write permissions from the actual REVM result solely to invoke the
unchanged change-set encoder. Those short hashes are not used for validation;
OCC validates full key hashes and versions. Supplied `Tx.crw_sets` predictions
are ignored. There is no simulation pass or speculative-write forwarding.

This retains the repository's current nonce-check and beneficiary-reward
settings, and the supported state-conversion semantics of its existing encoder.
OCC returns REVM errors without invoking FAFO's unimplemented error handler;
EVM reverts are normal results with their appropriate accepted state changes.
It is a baseline for this repository, not a claim of complete Ethereum protocol
validation or support for every account-lifecycle behavior beyond that encoder.

An operational failure after block registration poisons the executor because
already accepted writes cannot be rolled back on that instance. Recover the
database before resuming. In release builds, the original abort-on-panic profile
also applies; worker panic recovery is only possible in unwind builds.
