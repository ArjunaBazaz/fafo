#!/usr/bin/env python3
"""Run matching OCC then FAFO contention sweeps with RAM-backed replay databases.

The reference database stays on SSD. Each executor gets a fresh temporary RAM
directory with a reference symlink; only one executor's replay copy is retained
at a time. The requested CSV combines both existing sweep reports.
"""
import argparse
import csv
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile

import run_contention_sweep as sweep


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--size", choices=("fast", "medium", "full"), default="medium")
    parser.add_argument("--skew", type=int, default=10)
    parser.add_argument("-n", "--n", "--runs", dest="runs", type=int, default=5)
    parser.add_argument("--db-dir", type=Path, default=Path("/mnt/ssd/fafo-benchmark-data"),
                        help="SSD reference database root; initialized if missing")
    parser.add_argument("--ram-dir", type=Path, default=Path("/dev/shm"),
                        help="existing tmpfs or ramfs parent for temporary replay directories")
    parser.add_argument("--binary", type=Path, help="existing executable; otherwise build release once")
    parser.add_argument("--filename", "--output", "-o", type=Path, required=True,
                        help="new combined CSV, preferably on SSD")
    args = parser.parse_args(argv)
    # Keep size, hot-account and run-count validation identical to the sweep.
    sweep.parse_args(["--executor", "occ", "--size", args.size, "--skew", str(args.skew),
                      "-n", str(args.runs), "--filename", str(args.filename)])
    return args


def console(message):
    print(message, end="", flush=True)


def main(argv=None):
    args = parse_args(argv)
    runner = sweep.runner
    if runner.resource is None or sys.platform != "linux":
        raise SystemExit("Run this script on Linux or inside WSL.")
    output = args.filename.expanduser().resolve()
    if output.exists():
        raise SystemExit("Output already exists; choose a new --filename.")
    ram_parent = args.ram_dir.expanduser().resolve()
    reference_root = args.db_dir.expanduser().resolve()
    try:
        if not ram_parent.is_dir():
            raise ValueError(f"RAM directory does not exist: {ram_parent}")
        mount = subprocess.run(
            ["findmnt", "--noheadings", "--output", "FSTYPE", "--target", str(ram_parent)],
            text=True, capture_output=True, check=True,
        )
        if mount.stdout.strip() not in ("tmpfs", "ramfs"):
            raise ValueError(f"{ram_parent} must be on tmpfs or ramfs for a RAM test")
        # Verify permission before starting a potentially long database setup.
        with tempfile.TemporaryDirectory(prefix="fafo-ram-check-", dir=ram_parent):
            pass
        output.parent.mkdir(parents=True, exist_ok=True)
        # Scratch logs/metrics remain beside the permanent report, not on a
        # potentially full /tmp filesystem or in the measured RAM directory.
        with tempfile.TemporaryDirectory(prefix=".fafo-ram-sweep-", dir=output.parent) as scratch:
            scratch = Path(scratch)
            env = dict(os.environ, OCC_TIMINGS="0", RUSTFLAGS="-Awarnings", RUST_LOG="info",
                       TMPDIR=str(scratch), TMP=str(scratch), TEMP=str(scratch))
            with output.open("x", newline="", encoding="utf-8") as destination:
                if args.binary:
                    binary = args.binary.expanduser().resolve()
                    if not binary.is_file() or not os.access(binary, os.X_OK):
                        raise ValueError(f"benchmark binary is missing or not executable: {binary}")
                else:
                    binary = sweep.build_binary(console, env)
                entry_bits = {"fast": 18, "medium": 26, "full": 30}[args.size]
                create_bits = min(21, entry_bits - 5)
                relative_reference = Path("exepipe_bencheth") / f"eth_{create_bits}_{entry_bits-create_bits}"
                reference = reference_root / relative_reference
                if not (reference / "data").is_dir():
                    runner.run_command([
                        str(binary), "--db-dir", str(reference_root), "--workload", "eth",
                        "--entry-count-bits", str(entry_bits),
                        "--create-txn-per-block-bits", str(create_bits), "init-db",
                    ], console, env)
                if not (reference / "data").is_dir():
                    raise ValueError(f"reference initialization did not create {reference / 'data'}")
                available = shutil.disk_usage(ram_parent).free
                reference_bytes = sum(path.stat().st_size for path in reference.rglob("*") if path.is_file())
                if mount.stdout.strip() == "tmpfs" and reference_bytes >= available:
                    raise ValueError("RAM filesystem cannot fit even one reference copy; "
                                     "use a smaller size or free RAM capacity")
                print(f"Reference: {reference}\nRAM filesystem free: {available / (1024 ** 3):.2f} GiB; "
                      f"reference file sizes: {reference_bytes / (1024 ** 3):.2f} GiB. "
                      "Allow additional memory for execution and database growth.", flush=True)
                writer = None
                for executor in ("occ", "fafo"):
                    print(f"\nStarting {executor.upper()} RAM sweep: size={args.size}, "
                          f"skew={args.skew}, n={args.runs}", flush=True)
                    # Exiting this context reclaims the previous executor's RAM
                    # copy before the next executor begins. rmtree does not
                    # follow the reference symlink into the SSD database.
                    with tempfile.TemporaryDirectory(prefix=f"fafo-ram-{executor}-", dir=ram_parent) as ram:
                        ram = Path(ram)
                        link = ram / relative_reference
                        link.parent.mkdir(parents=True)
                        link.symlink_to(reference, target_is_directory=True)
                        stage_csv = scratch / f"{executor}.csv"
                        error = None
                        try:
                            runner.run_command([
                                sys.executable, "-B", str(Path(sweep.__file__).resolve()),
                                "--binary", str(binary), "--executor", executor,
                                "--size", args.size, "--skew", str(args.skew), "-n", str(args.runs),
                                "--db-dir", str(ram), "--filename", str(stage_csv),
                            ], console, env)
                        except (RuntimeError, KeyboardInterrupt) as failed:
                            error = failed
                        # Preserve completed contention rows even if a later
                        # level fails. FAFO's OCC-only columns remain blank.
                        if stage_csv.is_file():
                            with stage_csv.open(newline="", encoding="utf-8") as source:
                                reader = csv.DictReader(source)
                                if reader.fieldnames:
                                    if writer is None:
                                        writer = csv.DictWriter(destination, fieldnames=reader.fieldnames)
                                        writer.writeheader()
                                    writer.writerows(reader)
                                    destination.flush()
                        if error is not None:
                            raise error
        print(f"Saved combined RAM sweep metrics: {output}", flush=True)
        return 0
    except (OSError, ValueError, RuntimeError, subprocess.CalledProcessError) as error:
        print(f"RAM sweeps failed: {error}\nCompleted rows, if any, remain in {output}", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        print(f"Interrupted; completed rows, if any, remain in {output}", file=sys.stderr)
        return 130


if __name__ == "__main__":
    sys.exit(main())
