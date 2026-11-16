"""Exercise the metrics-only sweep through the real repeated-run pipeline."""
import csv
import json
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

    def invoke(self, executor="occ", size="fast", **extra_env):
        return subprocess.run([
            sys.executable, "-B", str(SCRIPT), "--executor", executor, "--size", size,
            "--filename", str(self.output), "--db-dir", str(self.root / "database"),
            "--binary", str(self.binary),
        ], env=dict(os.environ, SWEEP_TEST_CALLS=str(self.calls), OCC_TIMINGS="1", **extra_env),
            text=True, capture_output=True)

    def rows(self):
        with self.output.open(newline="", encoding="utf-8") as source:
            return list(csv.DictReader(source))

    def test_six_levels_two_runs_each_and_no_diagnostic_or_log_output_in_csv(self):
        result = self.invoke()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        rows = self.rows()
        self.assertEqual([row["contention"] for row in rows], [f"0.{n}" for n in range(1, 7)])
        for row in rows:
            self.assertEqual(row["runs"], "2")
            self.assertEqual(float(row["average_throughput_tx_per_sec"]), 150)
            self.assertEqual(float(row["average_attempt_abort_rate_pct"]), 35)
            self.assertGreater(float(row["average_cpu_utilization_pct"]), 0)
        calls = [json.loads(line) for line in self.calls.read_text().splitlines()]
        self.assertTrue(all(call["timings"] == "0" for call in calls))
        self.assertEqual(sum("init-db" in call["args"] for call in calls), 1)
        prepared = [call["args"] for call in calls if "prepare-workload" in call["args"]]
        self.assertEqual(len(prepared), 6)
        for index, args in enumerate(prepared, 1):
            for key, expected in [("--generator", "hotn"), ("--skew", "10.0"),
                                  ("--contention-degree", f"0.{index}"), ("--workload", "eth")]:
                self.assertEqual(args[args.index(key) + 1], expected)
        self.assertEqual(sum("replay-workload" in call["args"] for call in calls), 12)
        self.assertNotIn("stdout from replay", self.output.read_text())
        self.assertFalse(Path(str(self.output) + ".runs").exists())

    def test_fafo_and_other_sizes(self):
        for size, bits in [("medium", "26"), ("full", "30")]:
            with self.subTest(size=size):
                self.output = self.root / f"{size}.csv"
                result = self.invoke("fafo", size)
                self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
                rows = self.rows()
                self.assertEqual(len(rows), 6)
                self.assertEqual(rows[0]["executor"], "fafo")
                self.assertEqual(rows[0]["size"], size)
                self.assertNotIn("average_attempt_abort_rate_pct", rows[0])
                calls = [json.loads(line)["args"] for line in self.calls.read_text().splitlines()]
                self.assertEqual(calls[-1][calls[-1].index("--entry-count-bits") + 1], bits)

    def test_failed_pair_is_not_reported_as_a_two_run_average(self):
        result = self.invoke(FAFO_REPEAT_TEST_FAIL="2")
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(self.rows(), [])
        self.assertIn("exit status 7", result.stderr)
        self.assertNotIn("Saved averaged metrics", result.stdout)

    def test_existing_output_is_preserved(self):
        self.output.write_text("previous results")
        result = self.invoke()
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(self.output.read_text(), "previous results")
        self.assertFalse(self.calls.exists())


if __name__ == "__main__":
    unittest.main()
