#!/usr/bin/env python3
"""Report ETH/hotn run means and sample deviations at contention 0.0 through 0.9.

Writes CSV metrics only. Reuses run_benchmarks.py's preparation, replay and CPU
measurement code; build/init/preparation are outside measured replay commands.
"""
import argparse
from collections import deque
import csv
import json
import os
from pathlib import Path
import statistics
import subprocess
import sys
import tempfile

import run_benchmarks as runner


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--executor", choices=("fafo", "occ", "serial"), required=True)
    parser.add_argument("--size", choices=("fast", "medium", "full"), required=True)
    parser.add_argument("--skew", type=int, default=10,
                        help="number of hot accounts (positive integer; default: 10)")
    parser.add_argument("-n", "--n", "--runs", dest="runs", type=int, default=2,
                        help="runs per contention level (positive integer; default: 2)")
    parser.add_argument("--filename", "--output", "-o", type=Path, required=True,
                        help="new CSV file containing metric means and sample standard deviations")
    parser.add_argument("--db-dir", type=Path, default=Path("/mnt/ssd/fafo-benchmark-data"))
    parser.add_argument("--binary", type=Path,
                        help="use an existing executable; otherwise build release once")
    args = parser.parse_args(argv)
    if args.runs < 1:
        parser.error("n must be at least 1")
    account_count = 1 << {"fast": 18, "medium": 26, "full": 30}[args.size]
    if not 1 <= args.skew <= account_count:
        parser.error(f"--skew must be between 1 and {account_count} for size {args.size}")
    return args


def build_binary(log, env):
    runner.run_command(
        ["cargo", "build", "--locked", "--release", "-p", "bench-sendeth", "--bin", "bench-sendeth"],
        log, env,
    )
    metadata = subprocess.run(
        ["cargo", "metadata", "--no-deps", "--format-version", "1"],
        cwd=runner.ROOT, env=env, text=True, capture_output=True,
    )
    if metadata.returncode:
        raise RuntimeError(metadata.stderr.strip() or "cargo metadata failed")
    return Path(json.loads(metadata.stdout)["target_directory"]) / "release" / "bench-sendeth"


def averaged_row(report, executor, size, contention, skew, expected_runs):
    runs = report["runs"]
    if report["status"] != "complete" or len(runs) != expected_runs:
        raise ValueError(f"each contention level requires exactly {expected_runs} completed runs")

    def deviation(key):
        # Sample standard deviation uses n-1; one observation cannot estimate it.
        return statistics.stdev(r[key] for r in runs) if len(runs) > 1 else None

    row = {
        "executor": executor,
        "size": size,
        "contention": contention,
        "runs": len(runs),
        "skew": skew,
        "average_cpu_utilization_pct": statistics.mean(r["cpu_utilization_pct"] for r in runs),
        "stddev_cpu_utilization_pct": deviation("cpu_utilization_pct"),
        "average_throughput_tx_per_sec": statistics.mean(r["throughput_tx_per_sec"] for r in runs),
        "stddev_throughput_tx_per_sec": deviation("throughput_tx_per_sec"),
    }
    if executor == "occ":
        row["average_attempt_abort_rate_pct"] = statistics.mean(r["abort_pct"] for r in runs)
        row["stddev_attempt_abort_rate_pct"] = deviation("abort_pct")
    return row


def main(argv=None):
    args = parse_args(argv)
    if runner.resource is None or sys.platform != "linux":
        raise SystemExit("Run this script on Linux or inside WSL.")
    output = args.filename.expanduser().resolve()
    if output.exists():
        raise SystemExit("Output already exists; choose a new --filename.")
    output.parent.mkdir(parents=True, exist_ok=True)
    env = dict(os.environ, OCC_TIMINGS="0", RUSTFLAGS="-Awarnings", RUST_LOG="info")
    recent = deque(maxlen=40)
    fields = ["executor", "size", "contention", "runs", "skew",
              "average_cpu_utilization_pct", "stddev_cpu_utilization_pct",
              "average_throughput_tx_per_sec", "stddev_throughput_tx_per_sec"]
    if args.executor == "occ":
        fields.extend(["average_attempt_abort_rate_pct", "stddev_attempt_abort_rate_pct"])
    try:
        # Raw logs and JSON are scratch data, never mixed into the requested CSV.
        with tempfile.TemporaryDirectory(prefix="fafo-contention-sweep-") as scratch:
            scratch = Path(scratch)
            with (scratch / "commands.log").open("w", encoding="utf-8") as raw:
                def log(message):
                    raw.write(message)
                    recent.extend(message.splitlines())

                if args.binary:
                    binary = args.binary.expanduser().resolve()
                    if not binary.is_file() or not os.access(binary, os.X_OK):
                        raise ValueError(f"benchmark binary is missing or not executable: {binary}")
                else:
                    print("Building release benchmark...", flush=True)
                    binary = build_binary(log, env)

                # execute() copies the process environment for child commands.
                # Disable diagnostics even if the launching shell enabled them.
                previous_timings = os.environ.get("OCC_TIMINGS")
                os.environ["OCC_TIMINGS"] = "0"
                try:
                    with output.open("x", newline="", encoding="utf-8") as destination:
                        writer = csv.DictWriter(destination, fieldnames=fields)
                        writer.writeheader()
                        destination.flush()
                        for step in range(10):
                            contention = f"0.{step}"
                            print(f"{args.executor} / {args.size}: contention={contention}, skew={args.skew}, {args.runs} runs "
                                  "(initializing database if needed)...", flush=True)
                            artifacts = scratch / f"contention-{contention}"
                            artifacts.mkdir()
                            repeat_args = runner.parse_args([
                                "--executor", args.executor, "--size", args.size,
                                "--db-dir", str(args.db_dir.expanduser().resolve()),
                                "--binary", str(binary), "--token", "eth",
                                "--generator", "hotn", "--skew", str(args.skew),
                                "--contention-degree", contention, "-n", str(args.runs),
                                "--filename", str(scratch / "unused.log"),
                            ])
                            if runner.execute(repeat_args, log, artifacts):
                                raise RuntimeError(f"contention={contention} failed")
                            report = json.loads((artifacts / "summary.json").read_text(encoding="utf-8"))
                            writer.writerow(averaged_row(report, args.executor, args.size, contention,
                                                         args.skew, args.runs))
                            destination.flush()
                finally:
                    if previous_timings is None:
                        os.environ.pop("OCC_TIMINGS", None)
                    else:
                        os.environ["OCC_TIMINGS"] = previous_timings
    except (OSError, ValueError, TypeError, KeyError, RuntimeError) as error:
        print(f"Sweep failed: {error}", file=sys.stderr)
        if recent:
            print("Recent benchmark output:\n" + "\n".join(recent), file=sys.stderr)
        print("Any completed contention levels remain in the CSV.", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        print("Sweep interrupted; any completed contention levels remain in the CSV.", file=sys.stderr)
        return 130
    print(f"Saved averaged metrics and standard deviations: {output}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
