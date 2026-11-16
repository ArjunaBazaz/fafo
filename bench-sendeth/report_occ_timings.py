#!/usr/bin/env python3
"""Print OCC diagnostic timings from benchmark JSON files or runner .runs directories."""
import argparse
import json
import math
from pathlib import Path
import sys


def report(paths):
    totals = {}
    layouts = set()
    for path in paths:
        with path.open(encoding="utf-8") as source:
            metrics = json.load(source)["metrics"]
        if "occ.timing.block_ns" not in metrics:
            raise ValueError(f"{path}: no OCC timings; rebuild and run with OCC_TIMINGS=1")
        layouts.add("occ.timing.persistence_worker_ns" in metrics)
        if len(layouts) > 1:
            raise ValueError("cannot mix timings from before and after the OCC pipeline change")
        for name, item in metrics.items():
            if name.startswith("occ.timing."):
                value = float(item["value"])
                if not math.isfinite(value) or value < 0:
                    raise ValueError(f"{path}: invalid timing counter {name}")
                key = name.removeprefix("occ.timing.")
                totals[key] = totals.get(key, 0) + value

    def value(name):
        return totals[name]

    def row(label, name, denominator=None):
        seconds = value(name) / 1e9
        percentage = ""
        if denominator and value(denominator):
            percentage = f"{100 * value(name) / value(denominator):9.2f}%"
        print(f"{label:43} {seconds:12.6f} {percentage:>10}")

    print(f"Files: {len(paths)}; measured blocks: {int(value('blocks'))}")
    print("Times are wall-clock seconds, aggregated over these files.\n")
    print(f"{'Block phases':43} {'Seconds':>12} {'% block':>10}")
    row("Block total", "block_ns", "block_ns")
    row("  Setup / start_block / durability pruning", "setup_ns", "block_ns")
    row("  Scheduler", "scheduler_ns", "block_ns")
    row("  Finalize (includes compaction)", "finalize_ns", "block_ns")
    row("    Compaction", "compaction_ns", "block_ns")
    print(f"\n{'Scheduler breakdown':43} {'Seconds':>12} {'% sched':>10}")
    for label, key in (
        ("Wait for next ordered result", "coordinator_result_wait_ns"),
        ("Submit speculative jobs", "coordinator_submit_ns"),
        ("Validation: acquire read lock", "coordinator_validation_lock_wait_ns"),
        ("Validation: work holding read lock", "coordinator_validation_work_ns"),
        ("Publication: acquire write lock", "coordinator_publish_lock_wait_ns"),
        ("Publication: work holding write lock", "coordinator_publish_work_ns"),
        ("Synchronous retry (includes its reads)", "coordinator_retry_ns"),
        ("Commit callback / persistence submission", "coordinator_callback_ns"),
        ("Other / bookkeeping / scoped-worker drain", "coordinator_other_ns"),
    ):
        row(label, key, "scheduler_ns")
    print("\nWorker sums overlap each other and the coordinator; do not add them to block time.")
    row("Worker attempt wall time", "worker_attempt_ns")
    row("Worker read-lock acquisition time", "worker_read_lock_wait_ns")
    row("Worker read-lock hold time", "worker_read_lock_hold_ns")
    print(f"Worker read-lock acquisitions: {int(value('worker_read_lock_count'))}")
    print("\nRetry read-lock times are already included in synchronous retry time.")
    row("Retry read-lock acquisition time", "retry_read_lock_wait_ns")
    row("Retry read-lock hold time", "retry_read_lock_hold_ns")
    print(f"Retry read-lock acquisitions: {int(value('retry_read_lock_count'))}")
    print(f"Validation acquisitions: {int(value('coordinator_validation_count'))}; "
          f"publication acquisitions: {int(value('coordinator_publish_count'))}")
    if "persistence_worker_ns" in totals:
        print("\nPersistence stages overlap the scheduler when the background worker is enabled.")
        print("Queue send is included in callback time; final drain is included in finalize time.")
        for label, key in (
            ("Queue send (includes backpressure)", "persistence_enqueue_ns"),
            ("Final persistence-worker drain", "persistence_drain_ns"),
            ("Persistence work total", "persistence_worker_ns"),
            ("  CodeDB append", "persistence_code_ns"),
            ("  QMDB task submission total", "persistence_task_ns"),
            ("    Build / register task", "persistence_task_build_ns"),
            ("    Fetch previous entries", "persistence_prefetch_ns"),
            ("    Submit to updater", "persistence_updater_ns"),
            ("  Retired-attempt cleanup", "persistence_cleanup_ns"),
        ):
            row(label, key)
        print("Lock counts now count shard acquisitions; snapshot capture is lock-free.")
    print("\nOutside run_block (final flush includes QMDB, code and overlay cleanup):")
    for label, key in (
        ("Result destruction", "result_drop_ns"),
        ("Final flush total", "final_flush_ns"),
        ("  QMDB drain", "final_qmdb_flush_ns"),
        ("  CodeDB flush", "final_code_flush_ns"),
        ("  Overlay cleanup", "final_overlay_clear_ns"),
    ):
        row(label, key)
    print("\nLock acquisition time includes uncontended overhead and scheduling delays.")
    print("These measurements are not CPU time; diagnostic clocks add overhead.")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("paths", nargs="+", type=Path,
                        help="benchmark JSON files or directories containing run-*.metrics.json")
    args = parser.parse_args()
    files = []
    for path in args.paths:
        matches = sorted(path.glob("run-*.metrics.json")) if path.is_dir() else [path]
        if not matches:
            parser.error(f"no run metrics found in {path}")
        files.extend(matches)
    # The same file passed twice should not double the measurements.
    files = list(dict.fromkeys(path.resolve() for path in files))
    try:
        report(files)
    except (OSError, ValueError, TypeError, KeyError) as error:
        print(f"Cannot report OCC timings: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
