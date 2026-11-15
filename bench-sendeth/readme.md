# bench-sendeth

`bench-sendeth` is a synthetic workload generator and replay harness used to
benchmark **FAFO** (the default) or **OCC**.  It can:

1. **Initialise** a large key-value database with billions of accounts.
2. **Generate** transfer workloads with tunable contention or derived from a real
   transaction trace.
3. **Replay** those workloads in parallel, collecting rich metrics along the
   way.

The tool is written in Rust and includes a helper script [`run.sh`](./run.sh) that
handles the full `cargo run …` invocation for you.

## Quick-start (recommended)

The one-liner below runs a *small* benchmark suitable for laptops, taking only a couple of minutes:

```bash
./run.sh --size fast
```

Select OCC with one additional flag, keeping the same database, workload, and
timing boundaries:

```bash
./run.sh --size fast --executor occ
./run.sh --size fast --executor fafo
```

The Rust CLI also accepts `--executor fafo|occ` with `replay-workload`.
FAFO retains its original execution loop and metrics. A thin
[`occ_adapter.rs`](src/common/occ_adapter.rs) calls the isolated OCC executor;
it uses 101 workers plus the coordinator, matching FAFO's peak scheduler thread
budget. Both use the same QMDB/CodeDB settings and process CPU limits.
OCC uses a separate `_run_occ` database copy and `occ_` metrics filename prefix;
its scheduler statistics are under `occ.*`. Common duration and throughput
metrics have the same names. An explicit `--output` overrides either filename.

The script will:

1. Create or reuse a database under `/tmp/QMDB`.
2. Create a low-contention ETH workload if it does not already exist.
3. Replay the workload and print timing information.

Use `--help` to see all options exposed by `run.sh`:

```bash
./run.sh --help
```

Common flags:

* `--db-dir DIR` – where to store QMDB files (default `/tmp/QMDB`).
* `--size fast|medium|full|BITS` – workload size shortcut (18, 26, 30 bits) or
  an explicit power-of-two.
* `--token eth|token` – transfer asset type: native ETH or ERC-20.
* `--generator simple|static|hotn|none|replay` – contention model.

See the *Generators* section below for details.


## Reproduce paper results

```bash
# No contention
./bench-sendeth/run.sh --size full --generator none
# Contention
./bench-sendeth/run.sh --size full --generator hotn --contention-degree 0.1 --skew 100
# Replay
./workloads/get_workloads.sh
./bench-sendeth/run.sh --size full --generator replay
```


## Anatomy of a benchmark run

Internally a run consists of three distinct phases.  You can execute them
individually via the `bench-sendeth` binary, but `run.sh` already orchestrates
this flow for you. **We strongly recommend using `run.sh` to run benchmarks.**

### 1. `init-db`
Populates the database with \(2^{entry\_count\_bits}\) accounts.  This phase is
slow – expect tens of minutes to multiple hours depending on the size and
hardware.  You normally do this once per `(db-dir, size, token)` triple.

Example (30-bit ETH workload on a fast machine):

```bash
cargo run --package bench-sendeth --bin bench-sendeth --release -- \
  --db-dir /tmp/QMDB --workload eth --entry-count-bits 30 \
  init-db
```

### 2. `prepare-workload`
Creates `blocks_*.dat`, a compact description of the transfers that will be
replayed later.  Generation is fast (seconds–minutes).  The command supports
several workload *generators*:

| Generator | Description |
|-----------|-------------|
| `none`    | Zero contention, purely random senders & receivers. |
| `hotn`    | A set of N hot accounts; higher `--skew` ($\alpha$ in paper) means more churn. |
| `replay`  | Extracts transfers from a real transaction dump (`--txns-json`). |

Example (10 % hotn contention, 100 hot keys, eth workload):

```bash
cargo run --package bench-sendeth --bin bench-sendeth --release -- \
  --db-dir /tmp/QMDB --workload eth --entry-count-bits 30 \
  prepare-workload --generator hotn --contention-degree 0.1 --skew 100
```

### 3. `replay-workload`
Restores the database into a fresh directory, replays the specified workload in
parallel and writes a JSON metrics file next to the database.

```bash
cargo run --package bench-sendeth --bin bench-sendeth --release -- \
  --db-dir /tmp/QMDB --workload eth \
  replay-workload --blocks-workload-filename /tmp/QMDB/exepipe_bencheth/eth_21_10/blocks_hotn_0.1_100.dat
```

---

## Full CLI reference

Below is a consolidated list of flags across all subcommands.  Flags marked with
*️⃣ are normally set for you by `run.sh`.

### Global flags

* `--db-dir DIR` – database root directory *️⃣
* `--workload eth|token` – asset type *️⃣
* `--entry-count-bits N` – \(log_2\) of the number of accounts (default 31)
* `--create-txn-per-block-bits N` – overrides default TXs per create block
* `--transfer-txn-count N` – total TXs to send during replay (0 = all)
* `--txn-per-transfer-block N` – TXs per replay block
* `--output FILE` – custom metrics JSON path

### Subcommands

#### `init-db`
No additional flags.

#### `prepare-workload`

* `--generator ...` – see table above
* `--contention-degree FLOAT` – fraction of hot accounts (default 0.1)
* `--skew FLOAT` – Zipf skew (`simple`, `hotn`) (default 5e-7)
* `--txns-json PATH` – ND-JSON.gz dump for `replay` mode
* `--txn-filter eth|erc20` – which transfers to extract (default `erc20`)
* `--replay-cycle BOOL` – loop over dump when EOF reached (default true)

#### `replay-workload`

* `--blocks-workload-filename FILE` – path to the *.dat workload (default
  `blocks.dat`)

---

## Metrics output

To repeat a configuration and collect throughput, OCC attempt abort rates and
CPU utilization, use the standalone Python runner from the repository root:

```bash
export CARGO_TARGET_DIR=/tmp/fafo-bench-target
python3 bench-sendeth/run_benchmarks.py \
  --db-dir /mnt/ssd/fafo-benchmark-data \
  --size medium --token eth --generator hotn \
  --contention-degree 0.2 --skew 10 --executor occ \
  -n 5 --filename results/occ-medium-0.2-10.log
```

It accepts the same benchmark flags as `run.sh`, plus `-n`/`--n`/`--runs` and
`--filename` (`--output`/`-o` are aliases). Use `--executor fafo` for FAFO.
It builds the release executable once, initializes the reference database if
missing, and generates the selected workload once per invocation. Generation
replaces that configuration's workload file. Each measured replay uses the
existing benchmark execution path. `--binary /path/to/bench-sendeth` skips the
build when an executable is already available.

All commands and combined stdout/stderr logs, individual measurements, and the
final summary are written to `--filename` and displayed in the terminal. The
runner also preserves each replay's JSON metrics and a machine-readable
`summary.json` under `<filename>.runs/`. Choose a new filename for another batch;
existing reports are preserved. A failed run stops the batch, returns a nonzero
exit status, and identifies any summary as covering only completed runs.

- Average throughput is the arithmetic mean of the replay `txns_per_sec` values.
- OCC attempt abort rate is `100 * validation_failures / attempts`, including
  attempts discarded before successful retries. Both the mean per-run rate and
  the pooled rate across all attempts are reported. FAFO's abort rate is N/A.
- Total CPU utilization is `100 * sum(user + system CPU seconds) / sum(wall
  seconds)` for the measured replay commands and their waited-for descendants.
  This has the same scope as `/usr/bin/time` around the binary: it includes
  database copying, ADS initialization, replay and the final flush. Build,
  reference initialization and workload generation are excluded. A result of
  1600% means an average of 16 logical CPUs busy; it is not normalized to the
  machine's CPU count. Total CPU seconds and average logical CPUs busy are also
  reported. No benchmark instrumentation is added by this runner.

The runner uses only Python's standard library and requires Linux/WSL. Its tests
can be run with `python3 -m unittest discover -s bench-sendeth/tests -v`.

To diagnose OCC coordinator versus global-overlay-lock costs, enable optional
timing counters when running the benchmark. Rebuild first to include them:

```bash
export CARGO_TARGET_DIR=/tmp/fafo-bench-target
export RUSTFLAGS="-Awarnings"
cargo build --locked --release -p bench-sendeth --bin bench-sendeth

OCC_TIMINGS=1 python3 bench-sendeth/run_benchmarks.py \
  --binary "$CARGO_TARGET_DIR/release/bench-sendeth" \
  --db-dir /mnt/ssd/fafo-benchmark-data \
  --size medium --token eth --generator hotn \
  --contention-degree 0.5 --skew 10 --executor occ \
  --n 3 --filename results/occ-medium-0.5-10-timed.log

python3 bench-sendeth/report_occ_timings.py \
  results/occ-medium-0.5-10-timed.log.runs
```

The report aggregates the per-run metric files. Pass one `run-001.metrics.json`
file instead to examine a single trial. `OCC_TIMINGS=1` also works with `run.sh`
or the direct binary. It is read only when constructing an OCC executor; FAFO
does not collect these diagnostics. Omit the variable or set `OCC_TIMINGS=0` for
baseline measurements.

Interpret the scheduler breakdown as follows:

- High publication **lock acquisition** time means the coordinator is delayed
  acquiring the global write lock. Examine worker read-lock hold times too:
  the existing read guard includes underlying database reads.
- High validation/publication **work while holding the lock**, callback or job
  submission time indicates serialized coordinator work. Work under the write
  lock also delays workers, so both effects can occur together.
- High ordered-result wait time points to slow initial attempts or scheduling
  delays at the front of the commit order. Worker read-lock acquisition times
  help distinguish lock stalls from other worker costs.
- Retry time includes retry read locks, execution and change-set conversion.
  Compaction, result destruction and final flush are reported separately.

These are wall-clock timings, including possible descheduling. Acquisition
times include uncontended overhead; worker sums overlap and must not be added
to block duration. Compaction is included in finalize time, and retry read-lock
times are included in retry duration. Diagnostic clocks perturb performance;
compare throughput using an additional run with `OCC_TIMINGS` disabled. The
external CPU utilization retains its whole-command scope.

Metrics are recorded with [`metrics`](https://docs.rs/metrics) and written as a
pretty-printed JSON.  When using `run.sh` the file ends up in
`$DB_DIR/exepipe_bencheth/<…>/metrics/*.json` and includes:

* Timing for each phase of the run.
* Internal contention statistics.
* Parallelism observed by the framer.

You can load the file into Python, R, or your favourite Jupyter notebook for
visualisation.

---

## Fetching traces based on historical real-world transactions

The `replay` generator needs a compressed ND-JSON dump of Ethereum transactions.
Convenience scripts under [`workloads/`](../workloads/) can fetch prepared
samples:

```bash
workloads/get_workloads.sh
```

By default `run.sh` will look for `eth.ndjson.gz` (ETH transfers) or
`token_erc20.ndjson.gz` (ERC-20 transfers) under that directory.

---
