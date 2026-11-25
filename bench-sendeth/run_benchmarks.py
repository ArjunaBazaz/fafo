#!/usr/bin/env python3
"""Repeat the existing Linux benchmark and collect logs, metrics and CPU usage."""

import argparse
from datetime import datetime, timezone
import json
import math
import os
from pathlib import Path
import shlex
import signal
import statistics
import subprocess
import sys
import time

try:
    import resource
except ImportError:
    resource = None


ROOT = Path(__file__).resolve().parent.parent
WORKLOAD_MARKER = "Initialized blocks workload file: "


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db-dir", default="/tmp/QMDB")
    parser.add_argument("--size", default="medium", help="fast, medium, full, or account-count bits")
    parser.add_argument("--token", choices=("eth", "token"), default="eth")
    parser.add_argument("--executor", choices=("fafo", "occ", "serial"), default="fafo")
    parser.add_argument("--generator", choices=("none", "simple", "static", "hotn", "replay"), default="none")
    parser.add_argument("--contention-degree", type=float, default=0.1)
    parser.add_argument("--skew", type=float, help="hotn: hot-account count (default 100); simple: hot fraction (default 0.00001)")
    parser.add_argument("--txns-json", default="")
    parser.add_argument("--txn-filter", choices=("eth", "erc20", "token"), default="")
    parser.add_argument("-n", "--n", "--runs", dest="runs", type=int, required=True)
    parser.add_argument("--filename", "--output", "-o", dest="filename", type=Path, required=True,
                        help="new file for combined logs and summary; individual JSON files go in FILE.runs/")
    parser.add_argument("--binary", type=Path, help="use an already-built executable and skip Cargo build")
    args = parser.parse_args(argv)
    if args.runs < 1:
        parser.error("n must be at least 1")
    presets = {"fast": 18, "medium": 26, "full": 30}
    try:
        args.entry_bits = presets[args.size] if args.size in presets else int(args.size)
    except ValueError:
        parser.error("size must be fast, medium, full, or an integer")
    if not 11 <= args.entry_bits <= 63:
        parser.error("size must be between 11 and 63 bits")
    args.create_bits = min(21 if args.token == "eth" else 18, args.entry_bits - 5)
    if not math.isfinite(args.contention_degree) or not 0 <= args.contention_degree <= 1:
        parser.error("contention-degree must be between 0 and 1")
    if args.skew is None:
        args.skew = 100.0 if args.generator == "hotn" else 0.00001
    if not math.isfinite(args.skew):
        parser.error("skew must be finite")
    if args.generator == "hotn" and not (
        args.skew.is_integer() and 1 <= args.skew <= 2 ** args.entry_bits
    ):
        parser.error("hotn skew must be a positive integer no larger than the account count")
    if args.generator == "simple" and not 0 <= args.skew <= 1:
        parser.error("simple skew must be between 0 and 1")
    return args


def run_command(command, log, env):
    """CPU time includes the child and descendants it waits for, like GNU time."""
    log("\n$ " + shlex.join([str(part) for part in command]) + "\n")
    before = resource.getrusage(resource.RUSAGE_CHILDREN)
    start = time.perf_counter()
    lines = []
    with subprocess.Popen(
        [str(part) for part in command], cwd=ROOT, env=env,
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        text=True, encoding="utf-8", errors="replace", start_new_session=True,
    ) as child:
        try:
            for line in child.stdout:
                log(line)
                lines.append(line)
            returncode = child.wait()
        except KeyboardInterrupt:
            os.killpg(child.pid, signal.SIGINT)
            try:
                child.wait(timeout=10)
            except subprocess.TimeoutExpired:
                os.killpg(child.pid, signal.SIGKILL)
                child.wait()
            raise
    elapsed = time.perf_counter() - start
    after = resource.getrusage(resource.RUSAGE_CHILDREN)
    if returncode:
        raise RuntimeError(f"command failed with exit status {returncode}; see command output above")
    return {
        "wall_seconds": elapsed,
        "user_cpu_seconds": after.ru_utime - before.ru_utime,
        "system_cpu_seconds": after.ru_stime - before.ru_stime,
    }, lines


def read_metrics(filename, executor):
    with filename.open(encoding="utf-8") as source:
        data = json.load(source)

    def metric(name):
        value = float(data["metrics"][name]["value"])
        if not math.isfinite(value) or value < 0:
            raise ValueError(f"invalid metric {name}: {value}")
        return value

    result = {"throughput_tx_per_sec": metric("bench-sendeth.replay.txns_per_sec")}
    if executor == "occ":
        attempts = metric("occ.attempts")
        aborted = metric("occ.validation_failures")
        finalized = metric("occ.txns_processed")
        errors = metric("occ.errors")
        if (attempts <= 0 or aborted > attempts or errors != 0
                or attempts != finalized + aborted
                or finalized != data["transfer_txn_count"]):
            raise ValueError("inconsistent OCC counters or final executor errors")
        result.update(attempts=attempts, aborted=aborted,
                      abort_pct=100 * aborted / attempts)
    if executor == "serial":
        finalized = metric("serial.txns_processed")
        attempts = metric("serial.attempts")
        if (metric("serial.errors") != 0 or metric("serial.validation_failures") != 0
                or attempts != finalized or finalized != data["transfer_txn_count"]):
            raise ValueError("inconsistent serial counters or final executor errors")
    return result


def summarize(records, executor):
    wall = sum(run["wall_seconds"] for run in records)
    cpu = sum(run["user_cpu_seconds"] + run["system_cpu_seconds"] for run in records)
    summary = {
        "completed_runs": len(records),
        "average_throughput_tx_per_sec": statistics.mean(run["throughput_tx_per_sec"] for run in records),
        "total_wall_seconds": wall,
        "total_cpu_seconds": cpu,
        "total_cpu_utilization_pct": 100 * cpu / wall,
        "average_logical_cpus_busy": cpu / wall,
    }
    if executor == "occ":
        summary["average_abort_pct"] = statistics.mean(run["abort_pct"] for run in records)
        summary["pooled_abort_pct"] = 100 * sum(run["aborted"] for run in records) / sum(run["attempts"] for run in records)
    return summary


def execute(args, log, artifacts):
    env = os.environ.copy()
    env["RUSTFLAGS"] = "-Awarnings"  # Match run.sh's build flags.
    env["RUST_LOG"] = "info"  # Includes the generated workload's actual path.
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    desired = 65535 if hard == resource.RLIM_INFINITY else min(65535, hard)
    if soft < desired:
        resource.setrlimit(resource.RLIMIT_NOFILE, (desired, hard))
    records = []
    report = {"executor": args.executor, "requested_runs": args.runs, "status": "failed", "runs": records}
    try:
        if args.binary:
            binary = args.binary.expanduser().resolve()
        else:
            run_command(["cargo", "build", "--locked", "--release", "-p", "bench-sendeth", "--bin", "bench-sendeth"], log, env)
            metadata = subprocess.run(
                ["cargo", "metadata", "--no-deps", "--format-version", "1"],
                cwd=ROOT, env=env, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            )
            if metadata.stderr:
                log(metadata.stderr)
            if metadata.returncode:
                raise RuntimeError(f"cargo metadata failed with exit status {metadata.returncode}")
            binary = Path(json.loads(metadata.stdout)["target_directory"]) / "release" / "bench-sendeth"
        db_dir = Path(args.db_dir).expanduser().resolve()
        data_dir = db_dir / "exepipe_bencheth" / f"{args.token}_{args.create_bits}_{args.entry_bits - args.create_bits}"
        common = [str(binary), "--db-dir", str(db_dir), "--workload", args.token,
                  "--entry-count-bits", str(args.entry_bits),
                  "--create-txn-per-block-bits", str(args.create_bits)]
        if not (data_dir / "data").is_dir():
            run_command(common + ["init-db"], log, env)

        # Generate once per invocation, outside measurements. Read the real path
        # from the existing benchmark, avoiding duplicate float/filename rules.
        prepare = common + ["prepare-workload", "--generator", args.generator,
                            "--contention-degree", str(args.contention_degree), "--skew", str(args.skew)]
        if args.generator == "replay":
            trace = Path(args.txns_json).expanduser().resolve() if args.txns_json else ROOT / "workloads" / ("eth.ndjson" if args.token == "eth" else "token_erc20.ndjson")
            if not trace.is_file():
                raise ValueError(f"trace file does not exist: {trace}")
            prepare += ["--txns-json", str(trace), "--txn-filter", args.txn_filter or args.token]
        _, lines = run_command(prepare, log, env)
        paths = [line.split(WORKLOAD_MARKER, 1)[1].strip() for line in lines if WORKLOAD_MARKER in line]
        if len(paths) != 1 or not Path(paths[0]).is_file():
            raise RuntimeError("prepare-workload did not report a valid workload file")
        for index in range(1, args.runs + 1):
            log(f"\n=== Measured run {index}/{args.runs}: {args.executor} ===\n")
            metrics_file = artifacts / f"run-{index:03d}.metrics.json"
            command = common + ["--executor", args.executor, "--output", str(metrics_file),
                                "replay-workload", "--blocks-workload-filename", paths[0]]
            timing, _ = run_command(command, log, env)
            result = read_metrics(metrics_file, args.executor)
            result.update(timing, run=index, metrics_file=str(metrics_file))
            result["cpu_utilization_pct"] = 100 * (timing["user_cpu_seconds"] + timing["system_cpu_seconds"]) / timing["wall_seconds"]
            records.append(result)
            log("Run measurements: " + json.dumps(result, sort_keys=True) + "\n")
        report["status"] = "complete"
    except (OSError, ValueError, TypeError, KeyError, RuntimeError) as error:
        report["error"] = str(error)
        log(f"\nERROR: {error}\n")
    except KeyboardInterrupt:
        report["status"] = "interrupted"
        log("\nInterrupted; reporting completed runs only.\n")
    finally:
        if records:
            report["summary"] = summarize(records, args.executor)
        summary_file = artifacts / "summary.json"
        summary_file.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
        log(f"\n=== Summary: {len(records)}/{args.runs} runs completed; status={report['status']} ===\n")
        if records:
            summary = report["summary"]
            log(f"Average throughput: {summary['average_throughput_tx_per_sec']:.2f} tx/s\n")
            if args.executor == "occ":
                log(f"Average attempt abort rate: {summary['average_abort_pct']:.4f}%\n")
                log(f"Pooled attempt abort rate: {summary['pooled_abort_pct']:.4f}%\n")
            else:
                log(f"Average attempt abort rate: N/A ({args.executor.upper()})\n")
            log(f"Total CPU utilization: {summary['total_cpu_utilization_pct']:.2f}%\n")
            log(f"Average logical CPUs busy: {summary['average_logical_cpus_busy']:.2f}\n")
            log(f"Total CPU time: {summary['total_cpu_seconds']:.3f} seconds\n")
            log(f"Total measured wall time: {summary['total_wall_seconds']:.3f} seconds\n")
        log(f"Saved summary: {summary_file}\n")
    return 0 if report["status"] == "complete" else 1


def main(argv=None):
    args = parse_args(argv)
    if resource is None or sys.platform != "linux":
        raise SystemExit("Run this script on Linux or inside WSL.")
    filename = args.filename.expanduser().resolve()
    artifacts = Path(str(filename) + ".runs")
    if filename.exists() or artifacts.exists():
        raise SystemExit("Output already exists; choose a new --filename to preserve earlier measurements.")
    filename.parent.mkdir(parents=True, exist_ok=True)
    artifacts.mkdir()
    with filename.open("x", encoding="utf-8", buffering=1) as destination:
        def log(message):
            sys.stdout.write(message)
            sys.stdout.flush()
            destination.write(message)

        log(f"Started: {datetime.now(timezone.utc).isoformat()}\n")
        log("Arguments: " + json.dumps(vars(args), default=str, sort_keys=True) + "\n")
        log(f"Host: {os.uname().nodename}; allowed logical CPU IDs: {sorted(os.sched_getaffinity(0))}\n")
        log("CPU scope: each replay command, including copying, ADS initialization, execution and final flush.\n")
        log("Build, reference initialization and workload generation are excluded from CPU measurements.\n")
        return execute(args, log, artifacts)


if __name__ == "__main__":
    sys.exit(main())
