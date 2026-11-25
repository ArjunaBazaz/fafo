"""Exercise repeated runs, metric aggregation, CPU accounting and failed runs."""

import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


SCRIPT = Path(__file__).resolve().parents[1] / "run_benchmarks.py"
SPEC = importlib.util.spec_from_file_location("run_benchmarks", SCRIPT)
RUNNER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(RUNNER)

FAKE_BENCHMARK = r'''#!/usr/bin/env python3
import json
import os
from pathlib import Path
import sys
import time

args = sys.argv[1:]
def value(flag):
    return args[args.index(flag) + 1]

data = Path(value("--db-dir")) / "exepipe_bencheth" / "eth_13_5"
if "init-db" in args:
    (data / "data").mkdir(parents=True)
    print("initialized reference database")
elif "prepare-workload" in args:
    blocks = data / "blocks.dat"
    blocks.write_bytes(b"prepared transactions")
    print("Initialized blocks workload file: " + str(blocks))
else:
    output = Path(value("--output"))
    index = int(output.name.split("-")[1].split(".")[0])
    print(f"stdout from replay {index}")
    print(f"stderr from replay {index}", file=sys.stderr)
    if os.environ.get("FAFO_REPEAT_TEST_FAIL") == str(index):
        sys.exit(7)
    end = time.process_time() + 0.01
    while time.process_time() < end:
        pass
    metrics = {"bench-sendeth.replay.txns_per_sec": {"value": str(100 * index)}}
    if value("--executor") == "occ":
        aborted = 25 if index == 1 else 100
        metrics.update({
            "occ.txns_processed": {"value": "100"},
            "occ.errors": {"value": "0"},
            "occ.attempts": {"value": str(100 + aborted)},
            "occ.validation_failures": {"value": str(aborted)},
        })
    if value("--executor") == "serial":
        metrics.update({
            "serial.txns_processed": {"value": "100"},
            "serial.attempts": {"value": "100"},
            "serial.validation_failures": {"value": "0"},
            "serial.errors": {"value": "1" if os.environ.get("FAFO_SERIAL_TEST_ERROR") else "0"},
        })
    if os.environ.get("FAFO_REPEAT_TEST_MISSING_METRIC"):
        del metrics["bench-sendeth.replay.txns_per_sec"]
    output.write_text(json.dumps({"metrics": metrics, "transfer_txn_count": 100}))
'''

FAKE_CARGO = r'''#!/usr/bin/env python3
import json
import os
from pathlib import Path
import shutil
import sys

target = Path(os.environ["FAFO_REPEAT_TEST_TARGET"])
if sys.argv[1] == "build":
    executable = target / "release" / "bench-sendeth"
    executable.parent.mkdir(parents=True)
    shutil.copy2(os.environ["FAFO_REPEAT_TEST_BINARY"], executable)
    print("built benchmark once")
else:
    print(json.dumps({"target_directory": str(target)}))
'''


@unittest.skipUnless(sys.platform == "linux", "CPU accounting requires Linux/WSL")
class RepeatedBenchmarkTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="fafo repeat ")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.binary = self.root / "fake benchmark"
        self.binary.write_text(FAKE_BENCHMARK, encoding="utf-8")
        self.binary.chmod(0o755)
        self.output = self.root / "results log.txt"
        self.artifacts = Path(str(self.output) + ".runs")

    def invoke(self, executor="occ", env=None, use_prebuilt=True):
        command = [sys.executable, str(SCRIPT)]
        if use_prebuilt:
            command += ["--binary", str(self.binary)]
        command += [
            "--db-dir", str(self.root / "database"), "--size", "fast",
            "--generator", "hotn", "--contention-degree", "0.2", "--skew", "10",
            "--executor", executor, "-n", "2", "--filename", str(self.output),
        ]
        return subprocess.run(command, env=env, text=True,
                              stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    def summary(self):
        return json.loads((self.artifacts / "summary.json").read_text())

    def test_occ_runs_preserve_logs_and_use_correct_abort_denominator(self):
        result = self.invoke()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        report = self.summary()
        summary = report["summary"]
        self.assertEqual(summary["completed_runs"], 2)
        self.assertEqual(summary["average_throughput_tx_per_sec"], 150)
        self.assertAlmostEqual(summary["average_abort_pct"], 35)
        self.assertAlmostEqual(summary["pooled_abort_pct"], 100 * 125 / 325)
        self.assertGreater(summary["total_cpu_seconds"], 0)
        self.assertAlmostEqual(summary["total_cpu_utilization_pct"],
                               100 * summary["total_cpu_seconds"] / summary["total_wall_seconds"])
        self.assertTrue((self.artifacts / "run-001.metrics.json").is_file())
        self.assertTrue((self.artifacts / "run-002.metrics.json").is_file())
        text = self.output.read_text()
        self.assertIn("stdout from replay 1", text)
        self.assertIn("stderr from replay 2", text)
        self.assertIn("Average attempt abort rate: 35.0000%", text)
        self.assertEqual(text.count("initialized reference database"), 1)
        self.assertEqual(text.count("Initialized blocks workload file:"), 1)

    def test_fafo_does_not_require_occ_metrics(self):
        result = self.invoke("fafo")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertNotIn("average_abort_pct", self.summary()["summary"])
        self.assertIn("Average attempt abort rate: N/A (FAFO)", self.output.read_text())

    def test_serial_runs_validate_counts_without_occ_abort_metrics(self):
        result = self.invoke("serial")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(self.summary()["executor"], "serial")
        self.assertEqual(self.summary()["summary"]["average_throughput_tx_per_sec"], 150)
        self.assertNotIn("average_abort_pct", self.summary()["summary"])
        self.assertIn("Average attempt abort rate: N/A (SERIAL)", self.output.read_text())

    def test_serial_final_errors_are_not_reported_as_success(self):
        result = self.invoke("serial", env=dict(os.environ, FAFO_SERIAL_TEST_ERROR="1"))
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(self.summary()["runs"], [])
        self.assertIn("inconsistent serial counters", result.stdout)

    def test_default_build_resolves_cargo_target_and_builds_once(self):
        cargo = self.root / "cargo"
        cargo.write_text(FAKE_CARGO, encoding="utf-8")
        cargo.chmod(0o755)
        env = dict(os.environ,
                   PATH=str(self.root) + os.pathsep + os.environ["PATH"],
                   FAFO_REPEAT_TEST_TARGET=str(self.root / "custom target"),
                   FAFO_REPEAT_TEST_BINARY=str(self.binary))
        result = self.invoke(env=env, use_prebuilt=False)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(self.output.read_text().count("built benchmark once"), 1)
        self.assertEqual(self.summary()["summary"]["completed_runs"], 2)

    def test_failed_run_stops_and_reports_partial_results(self):
        env = dict(os.environ, FAFO_REPEAT_TEST_FAIL="2")
        result = self.invoke(env=env)
        self.assertNotEqual(result.returncode, 0)
        report = self.summary()
        self.assertEqual(report["status"], "failed")
        self.assertEqual(report["summary"]["completed_runs"], 1)
        self.assertEqual(report["summary"]["average_throughput_tx_per_sec"], 100)
        self.assertIn("exit status 7", report["error"])

    def test_missing_metrics_fail_instead_of_counting_as_zero(self):
        env = dict(os.environ, FAFO_REPEAT_TEST_MISSING_METRIC="1")
        result = self.invoke(env=env)
        self.assertNotEqual(result.returncode, 0)
        report = self.summary()
        self.assertEqual(report["runs"], [])
        self.assertNotIn("summary", report)

    def test_existing_report_is_preserved(self):
        self.output.write_text("previous measurement")
        result = self.invoke()
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(self.output.read_text(), "previous measurement")
        self.assertFalse(self.artifacts.exists())


class AggregationTests(unittest.TestCase):
    def test_cpu_percent_is_weighted_by_wall_time(self):
        records = [
            {"wall_seconds": 1, "user_cpu_seconds": 1, "system_cpu_seconds": 1,
             "throughput_tx_per_sec": 100},
            {"wall_seconds": 9, "user_cpu_seconds": 3, "system_cpu_seconds": 0,
             "throughput_tx_per_sec": 200},
        ]
        summary = RUNNER.summarize(records, "fafo")
        self.assertEqual(summary["total_cpu_utilization_pct"], 50)
        self.assertEqual(summary["average_logical_cpus_busy"], 0.5)


if __name__ == "__main__":
    unittest.main()
