# bench-sendeth

The serial reference is selected with `--executor serial` in the Rust CLI,
`run.sh`, `run_benchmarks.py`, or `run_contention_sweep.py`. It uses the existing
ordered OCC engine with `workers = 1, window = 1`: only one EVM transaction is
executed at a time, in input order, with no validation retries. QMDB/CodeDB
configuration, background processing, four-transaction persistence grouping,
and final drain timing remain unchanged. The calling coordinator and database
threads still exist, so process CPU utilization may exceed 100%.

This is a sequential OCC reference, retaining read-version tracking, validation,
the versioned overlay, and worker handoff overhead; it is not an optimized
direct-loop serial executor. Persistence submission runs inline on the
coordinator in this one-worker configuration, while QMDB itself stays parallel.
FAFO and normal OCC settings are unchanged. Serial replay uses `_run_serial`,
default replay metric files use a `serial_` prefix, and raw counters use
`serial.*`. Final errors or unexpected retries fail the benchmark. Serial CSV
reports include throughput/CPU means and sample deviations, without OCC abort
columns.

After rebuilding the binary, run matching serial medium sweeps on SSD and RAM:

```bash
STAMP=$(date -u +%Y%m%dT%H%M%SZ)

python3 bench-sendeth/run_contention_sweep.py \
  --binary "$CARGO_TARGET_DIR/release/bench-sendeth" \
  --executor serial --size medium --skew 10 -n 5 \
  --db-dir /mnt/ssd/fafo-benchmark-data \
  --filename "/mnt/ssd/abazaz/results/ssd-serial-medium-skew10-n5-$STAMP.csv"

python3 bench-sendeth/run_ram_contention_sweeps.py \
  --binary "$CARGO_TARGET_DIR/release/bench-sendeth" \
  --executor serial --size medium --skew 10 -n 5 \
  --db-dir /mnt/ssd/fafo-benchmark-data --ram-dir /dev/shm \
  --filename "/mnt/ssd/abazaz/results/ram-serial-medium-skew10-n5-$STAMP.csv"
```

Each command covers contention 0.0 through 0.9 and performs 50 measured replays.
The RAM wrapper now accepts `--executor serial`, `occ`, or `fafo` for a single
sweep; omitting it retains the original OCC-then-FAFO behavior.

For a metrics-only contention sweep (ETH, hotn), run:

```bash
python3 bench-sendeth/run_contention_sweep.py \
  --executor occ --size medium --skew 50 -n 5 \
  --db-dir /mnt/ssd/fafo-benchmark-data \
  --filename results/occ-medium-sweep.csv
```

Select `fafo`, `occ`, or `serial`, and `fast`, `medium`, or `full`. The script builds release
once unless `--binary /path/to/bench-sendeth` is supplied. It reuses the existing
runner to initialize a missing reference database, prepare each workload, and
replay contention levels 0.0 through 0.9 in steps of 0.1. `-n`, `--n`, or `--runs` sets the number
of runs per level (default: 2, minimum: 1). Diagnostics are disabled.
`--skew` sets the number of hot accounts and defaults to 10. It must be a positive
integer no larger than the selected database's account count. Use the same skew
for FAFO and OCC comparisons. For the full sweep through 0.9, skew 50 provides funding
headroom with the existing initial balances; skew 10 or 30 can exhaust hot accounts.

The CSV contains ten rows with executor, size, contention, run count, skew, mean CPU
utilization percentage, mean throughput in transactions/second, and (for OCC)
mean attempt abort percentage. Each metric also has a `stddev_*` column reporting
the sample standard deviation across runs, using the `n-1` denominator. For one
run these columns are blank because sample deviation is undefined. Deviations
use the metric's units: transactions/second for throughput and percentage points
for CPU utilization and abort rate. Each mean is the arithmetic average of the
per-run measurements. CPU utilization includes replay-command database copying,
ADS initialization, execution and final flush; build, reference initialization
and workload generation are excluded. CPU utilization can exceed 100% on
multiple cores. OCC abort percentage is validation failures divided by attempts.

Raw command logs and individual metrics are temporary; only the requested CSV
is retained. Existing output files are refused. If a run fails, completed
contention levels remain in the CSV and recent logs are printed to stderr;
incomplete sets of the requested number of runs are not averaged.

To run OCC then FAFO with RAM-backed replay databases using the same settings:

```bash
python3 bench-sendeth/run_ram_contention_sweeps.py \
  --binary "$CARGO_TARGET_DIR/release/bench-sendeth" \
  --size medium --skew 10 -n 5 \
  --db-dir /mnt/ssd/fafo-benchmark-data \
  --ram-dir /dev/shm \
  --filename /mnt/ssd/abazaz/results/ram-occ-fafo-medium.csv
```

This separate wrapper reuses `run_contention_sweep.py` unchanged. It defaults to
medium, skew 10, and five runs per level. Both sweeps cover 0.0 through 0.9, so
the defaults perform 100 measured replays. The combined CSV has 20 rows, OCC
first and FAFO second, retaining all means and sample deviations; FAFO's abort
columns are blank. Existing output files are refused.

`--db-dir` identifies the SSD reference database, initialized if missing. A
temporary reference symlink points there from each executor's fresh RAM root.
Writable QMDB/CodeDB replay files reside in RAM. The OCC copy is removed before
FAFO starts, and all temporary RAM copies are cleaned up afterward. The SSD
reference and the combined CSV remain. Without `--binary`, release is built
once before either sweep. Scratch logs/metrics are temporary files beside the
output report, avoiding a potentially full `/tmp`.

The wrapper checks that `--ram-dir` is on tmpfs or ramfs, and for tmpfs refuses
to start if even one reference copy will not fit. Allow additional physical
memory and filesystem space for execution and database growth; this minimum
capacity check does not guarantee adequate RAM. Monitor `free -h` and `vmstat 1`
to ensure swapping does not distort the comparison. Completed CSV rows are
retained on failure, and FAFO is not started if OCC fails. Use skew 50 for full
sweeps to avoid exhausting hot-account balances at high contention.

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

The Rust CLI also accepts `--executor fafo|occ|serial` with `replay-workload`.
FAFO retains its original execution loop and metrics. A thin
[`occ_adapter.rs`](src/common/occ_adapter.rs) calls the isolated OCC executor;
it uses 100 execution workers, one persistence worker, and the coordinator,
matching FAFO's peak scheduler thread
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
  acquiring a write lock for a touched overlay shard. Examine worker read-lock
  hold times too: a shard read guard includes underlying database reads.
  Snapshot capture itself is lock-free; lock counts now count shard acquisitions.
- High validation/publication **work while holding the lock**, callback or job
  submission time indicates serialized coordinator work. Work under the write
  lock also delays workers, so both effects can occur together.
- High ordered-result wait time points to slow initial attempts or scheduling
  delays at the front of the commit order. Worker read-lock acquisition times
  help distinguish lock stalls from other worker costs.
- Retry time includes retry read locks, execution and change-set conversion.
  Rejected-attempt cleanup now runs on the ordered persistence worker.
- Persistence stages show queue-send/backpressure (included in callbacks), final
  worker drain (included in finalize), CodeDB appends, task registration, prefetch,
  updater submission, and metadata cleanup. Worker work overlaps the scheduler;
  these rows are not additional block phases. High queue-send time suggests
  persistence cannot keep up with accepted batches.
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
