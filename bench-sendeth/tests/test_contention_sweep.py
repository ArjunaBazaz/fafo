"""Exercise the metrics-only sweep through the real repeated-run pipeline."""
import csv
import json
import math
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

from test_run_benchmarks import FAKE_BENCHMARK


SCRIPT = Path(__file__).resolve().parents[1] / "run_contention_sweep.py"


@unittest.skipUnless(sys.platform == "linux", "CPU accounting requires Linux/WSL")
class ContentionSweepTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="fafo sweep ")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.binary = self.root / "fake benchmark"
        source = FAKE_BENCHMARK.replace(
            'args = sys.argv[1:]',
            'args = sys.argv[1:]\n'
            'with open(os.environ["SWEEP_TEST_CALLS"], "a") as calls:\n'
            '    calls.write(json.dumps({"args": args, "timings": os.environ.get("OCC_TIMINGS")}) + "\\n")',
        )
        source = source.replace(
            'data = Path(value("--db-dir")) / "exepipe_bencheth" / "eth_13_5"',
            'bits = int(value("--entry-count-bits"))\n'
            'create = int(value("--create-txn-per-block-bits"))\n'
            'data = Path(value("--db-dir")) / "exepipe_bencheth" / f"eth_{create}_{bits-create}"',
        )
        self.binary.write_text(source, encoding="utf-8")
        self.binary.chmod(0o755)
        self.output = self.root / "averages.csv"
        self.calls = self.root / "calls.jsonl"

    def invoke(self, executor="occ", size="fast", skew=None, runs=None, runs_flag="-n", **extra_env):
        command = [
            sys.executable, "-B", str(SCRIPT), "--executor", executor, "--size", size,
            "--filename", str(self.output), "--db-dir", str(self.root / "database"),
            "--binary", str(self.binary),
        ]
        if skew is not None:
            command += ["--skew", str(skew)]
        if runs is not None:
            command += [runs_flag, str(runs)]
        return subprocess.run(command, env=dict(os.environ, SWEEP_TEST_CALLS=str(self.calls), OCC_TIMINGS="1", **extra_env),
            text=True, capture_output=True)

    def rows(self):
        with self.output.open(newline="", encoding="utf-8") as source:
            return list(csv.DictReader(source))

    def test_ten_levels_two_runs_each_and_no_diagnostic_or_log_output_in_csv(self):
        result = self.invoke()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        rows = self.rows()
        self.assertEqual([row["contention"] for row in rows], [f"0.{n}" for n in range(10)])
        for row in rows:
            self.assertEqual(row["runs"], "2")
            self.assertEqual(row["skew"], "10")
            self.assertEqual(float(row["average_throughput_tx_per_sec"]), 150)
            self.assertEqual(float(row["average_attempt_abort_rate_pct"]), 35)
            self.assertAlmostEqual(float(row["stddev_throughput_tx_per_sec"]), math.sqrt(5000))
            self.assertAlmostEqual(float(row["stddev_attempt_abort_rate_pct"]), math.sqrt(450))
            self.assertGreater(float(row["average_cpu_utilization_pct"]), 0)
            self.assertGreaterEqual(float(row["stddev_cpu_utilization_pct"]), 0)
        calls = [json.loads(line) for line in self.calls.read_text().splitlines()]
        self.assertTrue(all(call["timings"] == "0" for call in calls))
        self.assertEqual(sum("init-db" in call["args"] for call in calls), 1)
        prepared = [call["args"] for call in calls if "prepare-workload" in call["args"]]
        self.assertEqual(len(prepared), 10)
        for index, args in enumerate(prepared):
            for key, expected in [("--generator", "hotn"), ("--skew", "10.0"),
                                  ("--contention-degree", f"0.{index}"), ("--workload", "eth")]:
                self.assertEqual(args[args.index(key) + 1], expected)
        self.assertEqual(sum("replay-workload" in call["args"] for call in calls), 20)
        self.assertNotIn("stdout from replay", self.output.read_text())
        self.assertFalse(Path(str(self.output) + ".runs").exists())

    def test_fafo_and_other_sizes(self):
        for size, bits in [("medium", "26"), ("full", "30")]:
            with self.subTest(size=size):
                self.output = self.root / f"{size}.csv"
                result = self.invoke("fafo", size)
                self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
                rows = self.rows()
                self.assertEqual(len(rows), 10)
                self.assertEqual(rows[0]["executor"], "fafo")
                self.assertEqual(rows[0]["size"], size)
                self.assertNotIn("average_attempt_abort_rate_pct", rows[0])
                self.assertNotIn("stddev_attempt_abort_rate_pct", rows[0])
                calls = [json.loads(line)["args"] for line in self.calls.read_text().splitlines()]
                self.assertEqual(calls[-1][calls[-1].index("--entry-count-bits") + 1], bits)

    def test_failed_pair_is_not_reported_as_a_two_run_average(self):
        result = self.invoke(FAFO_REPEAT_TEST_FAIL="2")
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(self.rows(), [])
        self.assertIn("exit status 7", result.stderr)
        self.assertNotIn("Saved averaged metrics", result.stdout)

    def test_serial_sweep_has_ten_levels_and_standard_deviations(self):
        result = self.invoke(executor="serial", size="medium", skew=10, runs=3)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        rows = self.rows()
        self.assertEqual([r["contention"] for r in rows], [f"0.{n}" for n in range(10)])
        for row in rows:
            self.assertEqual(row["executor"], "serial")
            self.assertEqual(row["size"], "medium")
            self.assertEqual(row["skew"], "10")
            self.assertEqual(row["runs"], "3")
            self.assertEqual(float(row["average_throughput_tx_per_sec"]), 200)
            self.assertEqual(float(row["stddev_throughput_tx_per_sec"]), 100)
            self.assertNotIn("average_attempt_abort_rate_pct", row)
        calls = [json.loads(line)["args"] for line in self.calls.read_text().splitlines()]
        replays = [args for args in calls if "replay-workload" in args]
        self.assertEqual(len(replays), 30)
        self.assertTrue(all(args[args.index("--executor") + 1] == "serial" for args in replays))

    def test_custom_skew_is_forwarded_to_every_workload_and_recorded(self):
        for executor, skew in [("occ", 30), ("fafo", 50)]:
            with self.subTest(executor=executor, skew=skew):
                self.output = self.root / f"{executor}-{skew}.csv"
                self.calls.unlink(missing_ok=True)
                result = self.invoke(executor=executor, skew=skew)
                self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
                self.assertEqual([row["skew"] for row in self.rows()], [str(skew)] * 10)
                calls = [json.loads(line)["args"] for line in self.calls.read_text().splitlines()]
                prepared = [args for args in calls if "prepare-workload" in args]
                self.assertEqual(len(prepared), 10)
                for args in prepared:
                    self.assertEqual(float(args[args.index("--skew") + 1]), skew)

    def test_invalid_skew_is_rejected_before_build_or_database_access(self):
        for skew in [0, -1, "1.5", 262145]:
            with self.subTest(skew=skew):
                result = self.invoke(skew=skew)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("--skew", result.stderr)
                self.assertFalse(self.calls.exists())
                self.assertFalse(self.output.exists())

    def test_custom_run_count_and_sample_deviation_for_both_executors(self):
        for executor, flag in [("occ", "--n"), ("fafo", "--runs")]:
            with self.subTest(executor=executor):
                self.output = self.root / f"{executor}-three-runs.csv"
                self.calls.unlink(missing_ok=True)
                result = self.invoke(executor=executor, runs=3, runs_flag=flag)
                self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
                rows = self.rows()
                self.assertEqual(len(rows), 10)
                for row in rows:
                    self.assertEqual(row["runs"], "3")
                    self.assertEqual(float(row["average_throughput_tx_per_sec"]), 200)
                    # [100, 200, 300]: sample deviation=100, population deviation~81.65.
                    self.assertEqual(float(row["stddev_throughput_tx_per_sec"]), 100)
                    if executor == "occ":
                        self.assertEqual(float(row["average_attempt_abort_rate_pct"]), 40)
                        self.assertAlmostEqual(float(row["stddev_attempt_abort_rate_pct"]), math.sqrt(300))
                calls = [json.loads(line)["args"] for line in self.calls.read_text().splitlines()]
                self.assertEqual(sum("replay-workload" in args for args in calls), 30)

    def test_single_run_has_blank_sample_deviations(self):
        result = self.invoke(runs=1)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        for row in self.rows():
            self.assertEqual(row["runs"], "1")
            self.assertEqual(float(row["average_throughput_tx_per_sec"]), 100)
            for key in ("stddev_cpu_utilization_pct", "stddev_throughput_tx_per_sec",
                        "stddev_attempt_abort_rate_pct"):
                self.assertEqual(row[key], "")
        calls = [json.loads(line)["args"] for line in self.calls.read_text().splitlines()]
        self.assertEqual(sum("replay-workload" in args for args in calls), 10)

    def test_invalid_run_count_is_rejected_before_database_access(self):
        for runs in [0, -1, "1.5"]:
            with self.subTest(runs=runs):
                result = self.invoke(runs=runs)
                self.assertNotEqual(result.returncode, 0)
                self.assertFalse(self.calls.exists())
                self.assertFalse(self.output.exists())

    def test_existing_output_is_preserved(self):
        self.output.write_text("previous results")
        result = self.invoke()
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(self.output.read_text(), "previous results")
        self.assertFalse(self.calls.exists())


if __name__ == "__main__":
    unittest.main()
