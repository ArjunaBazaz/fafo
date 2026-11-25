"""Test sequential sweeps, shared reference state, and isolated RAM cleanup."""
import csv
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

from test_run_benchmarks import FAKE_BENCHMARK


SCRIPT = Path(__file__).resolve().parents[1] / "run_ram_contention_sweeps.py"


@unittest.skipUnless(sys.platform == "linux" and Path("/dev/shm").is_dir(), "RAM test requires Linux tmpfs")
class RamContentionSweepTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="fafo ram test ")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.ram = tempfile.TemporaryDirectory(prefix="fafo-ram-test-", dir="/dev/shm")
        self.addCleanup(self.ram.cleanup)
        self.ram_parent = Path(self.ram.name)
        self.output = self.root / "combined.csv"
        self.calls = self.root / "calls.jsonl"
        self.binary = self.root / "fake benchmark"
        source = FAKE_BENCHMARK.replace(
            'args = sys.argv[1:]',
            'args = sys.argv[1:]\n'
            'with open(os.environ["RAM_SWEEP_CALLS"], "a") as log:\n'
            '    log.write(json.dumps({"args": args, "timings": os.environ.get("OCC_TIMINGS")}) + "\\n")',
        ).replace(
            'data = Path(value("--db-dir")) / "exepipe_bencheth" / "eth_13_5"',
            'bits = int(value("--entry-count-bits"))\n'
            'create = int(value("--create-txn-per-block-bits"))\n'
            'data = Path(value("--db-dir")) / "exepipe_bencheth" / f"eth_{create}_{bits-create}"',
        ).replace(
            '    output = Path(value("--output"))',
            '    if value("--executor") == "fafo":\n'
            '        assert not list(Path(value("--db-dir")).parent.glob("fafo-ram-occ-*"))\n'
            '    suffix = {"occ": "_run_occ", "serial": "_run_serial", "fafo": "_run"}[value("--executor")]\n'
            '    replay = data.with_name(data.name + suffix)\n'
            '    replay.mkdir(exist_ok=True)\n'
            '    (replay / "marker").write_text("RAM working copy")\n'
            '    output = Path(value("--output"))',
        ).replace(
            'if os.environ.get("FAFO_REPEAT_TEST_FAIL") == str(index):',
            'if os.environ.get("FAFO_REPEAT_TEST_FAIL") == str(index) and '
            'value("--executor") == os.environ.get("FAIL_EXECUTOR", "occ"):',
        )
        self.binary.write_text(source, encoding="utf-8")
        self.binary.chmod(0o755)

    def invoke(self, executor=None, **extra_env):
        command = [
            sys.executable, "-B", str(SCRIPT), "--binary", str(self.binary),
            "--size", "fast", "--skew", "30", "-n", "2",
            "--db-dir", str(self.root / "ssd"), "--ram-dir", str(self.ram_parent),
            "--filename", str(self.output),
        ]
        if executor is not None:
            command += ["--executor", executor]
        return subprocess.run(command, env=dict(os.environ, RAM_SWEEP_CALLS=str(self.calls), OCC_TIMINGS="1", **extra_env),
            text=True, capture_output=True)

    def rows(self):
        with self.output.open(newline="", encoding="utf-8") as source:
            return list(csv.DictReader(source))

    def test_occ_then_fafo_have_matching_parameters_and_combined_statistics(self):
        result = self.invoke()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        rows = self.rows()
        self.assertEqual([row["executor"] for row in rows], ["occ"] * 10 + ["fafo"] * 10)
        for offset in (0, 10):
            self.assertEqual([row["contention"] for row in rows[offset:offset+10]],
                             [f"0.{index}" for index in range(10)])
        for row in rows:
            self.assertEqual(row["skew"], "30")
            self.assertEqual(row["runs"], "2")
            self.assertEqual(float(row["average_throughput_tx_per_sec"]), 150)
            self.assertGreater(float(row["stddev_throughput_tx_per_sec"]), 0)
            self.assertEqual(row["average_attempt_abort_rate_pct"], "35.0" if row["executor"] == "occ" else "")
        calls = [json.loads(line) for line in self.calls.read_text().splitlines()]
        self.assertTrue(all(call["timings"] == "0" for call in calls))
        self.assertEqual(sum("init-db" in call["args"] for call in calls), 1)
        replays = [call["args"] for call in calls if "replay-workload" in call["args"]]
        self.assertEqual(len(replays), 40)
        self.assertEqual([args[args.index("--executor") + 1] for args in replays],
                         ["occ"] * 20 + ["fafo"] * 20)
        self.assertTrue(all(Path(args[args.index("--db-dir") + 1]).is_relative_to(self.ram_parent)
                            for args in replays))
        self.assertEqual(list(self.ram_parent.iterdir()), [])
        self.assertTrue((self.root / "ssd/exepipe_bencheth/eth_13_5/data").is_dir())
        self.assertFalse(list(self.output.parent.glob(".fafo-ram-sweep-*")))

    def test_fafo_failure_preserves_occ_rows_and_cleans_ram(self):
        result = self.invoke(FAFO_REPEAT_TEST_FAIL="2", FAIL_EXECUTOR="fafo")
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual([row["executor"] for row in self.rows()], ["occ"] * 10)
        self.assertEqual(list(self.ram_parent.iterdir()), [])
        self.assertTrue((self.root / "ssd/exepipe_bencheth/eth_13_5/data").is_dir())

    def test_serial_only_uses_ram_and_does_not_run_occ_or_fafo(self):
        result = self.invoke(executor="serial")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        rows = self.rows()
        self.assertEqual(len(rows), 10)
        self.assertTrue(all(row["executor"] == "serial" for row in rows))
        self.assertNotIn("average_attempt_abort_rate_pct", rows[0])
        calls = [json.loads(line)["args"] for line in self.calls.read_text().splitlines()]
        replays = [args for args in calls if "replay-workload" in args]
        self.assertEqual(len(replays), 20)
        for args in replays:
            self.assertEqual(args[args.index("--executor") + 1], "serial")
            self.assertTrue(Path(args[args.index("--db-dir") + 1]).is_relative_to(self.ram_parent))
        self.assertEqual(list(self.ram_parent.iterdir()), [])
        self.assertTrue((self.root / "ssd/exepipe_bencheth/eth_13_5/data").is_dir())

    def test_existing_report_is_preserved_before_any_benchmark_runs(self):
        self.output.write_text("previous results")
        result = self.invoke()
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(self.output.read_text(), "previous results")
        self.assertFalse(self.calls.exists())


if __name__ == "__main__":
    unittest.main()
