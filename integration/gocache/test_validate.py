"""Protect baseline side effects and result accounting without a Redis server."""

import argparse
import contextlib
import copy
import importlib.util
import io
import json
import subprocess
import sys
import tempfile
import unittest
import urllib.error
from pathlib import Path
from unittest.mock import Mock, patch

SPEC = importlib.util.spec_from_file_location(
    "gocache_validation", Path(__file__).with_name("validate.py")
)
validation = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(validation)


@contextlib.contextmanager
def fixture(args, folder):
    yield "http://fixture", {"write": {}}


def baseline_probe(folder, fault=None):
    """Exercise the real HTTP observer with only transport/process I/O replaced."""
    state = {"value": "V1", "diagnostics": 0}
    calls = []
    server = Mock(server_port=12345)

    def http(base, path, method="GET", value=None):
        calls.append([method, path])
        if fault == "programming":
            raise AttributeError("observer programming error")
        if fault == "transport":
            raise urllib.error.URLError("fixture unavailable")
        if path == "/diagnostics":
            state["diagnostics"] += 1
            events = [{"seq": 1, "event": "loader_completed"}]
            if state["diagnostics"] > 1:
                events.append(
                    {
                        "seq": 2,
                        "event": "redis_completed",
                        "command": "get",
                        "result": "ok",
                    }
                )
            return events
        if method == "PUT":
            state["value"] = value
        if fault == "repeat" and path == "/item" and method == "GET":
            return {"value": "V2"}
        if fault == "write" and method == "PUT":
            return {"value": "V1"}
        if fault == "authoritative" and path == "/authoritative/item":
            return {"value": "V1"}
        return {"value": state["value"]}

    def redis_get(upstream):
        if state["value"] == "V1" or fault == "delete":
            return "V1"
        return None

    def cli(args, folder, cfg, command):
        observer = object.__new__(server_factory.call_args.args[1])
        observer.headers = {"Content-Length": "0"}
        observer.rfile = io.BytesIO()
        observer.wfile = io.BytesIO()
        statuses = []
        observer.send_response = statuses.append
        observer.end_headers = lambda: None
        observer.do_PUT()
        passed = statuses == [200] and state["value"] == "V2"
        return {"outcome": "PASS" if passed else "INFRASTRUCTURE_ERROR"}

    args = argparse.Namespace(upstream="127.0.0.1:12345")
    with (
        patch.object(validation, "fixture", fixture),
        patch.object(
            validation, "ThreadingHTTPServer", return_value=server
        ) as server_factory,
        patch.object(validation, "http", http),
        patch.object(validation, "redis_get", redis_get),
        patch.object(validation, "cli", cli),
    ):
        evidence = validation.baseline(args, folder)
    return {"calls": calls, "value": state["value"], "evidence": evidence}


class BaselineTest(unittest.TestCase):
    def test_required_operations_also_execute_in_optimized_python(self):
        for optimized in (False, True):
            with self.subTest(optimized=optimized):
                command = [sys.executable, "-B"]
                if optimized:
                    command.append("-O")
                run = subprocess.run(
                    command + [str(Path(__file__).resolve()), "--baseline-probe"],
                    capture_output=True,
                    text=True,
                    timeout=10,
                    check=False,
                )
                self.assertEqual(run.returncode, 0, run.stderr)
                result = json.loads(run.stdout)
                self.assertEqual(result["value"], "V2")
                self.assertIn(["GET", "/item"], result["calls"])
                self.assertIn(["PUT", "/item"], result["calls"])
                self.assertIn(["GET", "/authoritative/item"], result["calls"])
                self.assertTrue(
                    result["evidence"][
                        "PUT_updated_authoritative_and_Delete_removed_entry"
                    ]
                )

    def test_failed_checks_stop_baseline_without_claiming_mutation_success(self):
        for fault in ("repeat", "write", "authoritative", "delete", "transport"):
            with self.subTest(fault=fault), tempfile.TemporaryDirectory() as tmp:
                folder = Path(tmp)
                with self.assertRaisesRegex(
                    RuntimeError, "baseline failed; no race executed"
                ):
                    baseline_probe(folder, fault)
                evidence = json.loads((folder / "evidence.json").read_text())
                self.assertEqual(evidence["doctor_outcome"], "INFRASTRUCTURE_ERROR")
                self.assertIn("error", evidence)
                self.assertNotIn(
                    "PUT_updated_authoritative_and_Delete_removed_entry", evidence
                )

    def test_programming_errors_keep_their_original_exception(self):
        with (
            tempfile.TemporaryDirectory() as tmp,
            self.assertRaisesRegex(AttributeError, "observer programming error"),
        ):
            baseline_probe(Path(tmp), "programming")


class PositiveControlTest(unittest.TestCase):
    def setUp(self):
        self.report = json.loads(
            (Path(__file__).resolve().parents[2]
             / "docs/cases/gocache-v4.4.0-v0.3/redis-fail.json").read_text()
        )
        self.summary = {
            "baseline": {"doctor_outcome": "PASS"},
            "requested_runs": 1,
            "independent_fixture_processes": 1,
            "counts": {name: int(name == "FAIL") for name in validation.CODES},
            "runs": [{"run": 1}],
        }

    def check(self, report=None, summary=None):
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp)
            (output / "run-01").mkdir()
            (output / "run-01/report.json").write_text(json.dumps(report or self.report))
            validation.require_positive_control(
                argparse.Namespace(output=output, runs=1), summary or self.summary
            )

    def test_completed_detached_schedule_is_a_successful_positive_control(self):
        self.check()

    def test_every_other_outcome_and_partial_batch_fails_validation(self):
        for outcome in ("PASS", "UNRESOLVED", "INFRASTRUCTURE_ERROR"):
            with self.subTest(outcome=outcome):
                summary = copy.deepcopy(self.summary)
                summary["counts"] = {
                    name: int(name == outcome) for name in validation.CODES
                }
                with self.assertRaises(RuntimeError):
                    self.check(summary=summary)
        summary = copy.deepcopy(self.summary)
        summary["independent_fixture_processes"] = 0
        with self.assertRaises(RuntimeError):
            self.check(summary=summary)

    def test_sf001_alone_cannot_replace_schedule_or_attribution(self):
        for fault in ("missing", "order", "episode", "finding", "outcome", "reader"):
            with self.subTest(fault=fault):
                report = copy.deepcopy(self.report)
                events = report["events"]
                if fault == "missing":
                    report["events"] = [
                        e for e in events if e["event"] != "stale_set_completed"
                    ]
                elif fault == "order":
                    events[12], events[13] = events[13], events[12]
                elif fault == "episode":
                    events[12]["miss_episode"] = 999
                elif fault == "finding":
                    report["findings"] = []
                elif fault == "outcome":
                    report["outcome"] = "UNRESOLVED"
                else:
                    report["events"] = [
                        e for e in events if e["event"] != "read_completed"
                    ]
                with self.assertRaises(RuntimeError):
                    self.check(report=report)


class ResultAccountingTest(unittest.TestCase):
    def run_matrix(self, output, outcomes, diagnostic):
        args = argparse.Namespace(
            output=output,
            runs=len(outcomes),
            server="Redis test",
            resp=2,
            binary=Path("unused-binary"),
        )
        completed = []

        def cli(args, folder, cfg, command):
            report = {
                "outcome": outcomes[len(completed)],
                "events": [{"state": "READ_STARTED", "event": "read_started"}],
                "findings": [],
                "duration_ms": 1,
            }
            (folder / "report.json").write_text(json.dumps(report))
            completed.append(report)
            return report

        with (
            patch.object(validation, "fixture", fixture),
            patch.object(
                validation, "baseline", return_value={"doctor_outcome": "PASS"}
            ),
            patch.object(validation, "cli", cli),
            patch.object(validation, "http", side_effect=diagnostic),
            patch.object(validation.subprocess, "check_output", return_value="0.2.0\n"),
            contextlib.redirect_stdout(io.StringIO()),
        ):
            validation.validate(args)

    def test_diagnostic_transport_failures_preserve_all_cli_outcomes(self):
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp) / "results"
            outcomes = list(validation.CODES)
            self.run_matrix(output, outcomes, urllib.error.URLError("fixture stopped"))
            summary = json.loads((output / "summary.json").read_text())
            self.assertEqual(summary["counts"], dict.fromkeys(outcomes, 1))
            self.assertEqual([row["outcome"] for row in summary["runs"]], outcomes)
            for row in summary["runs"]:
                self.assertEqual(row["stage"], "READ_STARTED")
                self.assertEqual(row["diagnosis"], "diagnostics_unavailable")
                self.assertIn("diagnostics_error", row)
                self.assertTrue(
                    (output / f"run-{row['run']:02d}" / "report.json").exists()
                )

    def test_programming_error_propagates_but_preserves_partial_summary(self):
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp) / "results"
            with self.assertRaisesRegex(AttributeError, "diagnostic bug"):
                self.run_matrix(
                    output,
                    ["FAIL", "UNRESOLVED", "PASS"],
                    [[], AttributeError("diagnostic bug")],
                )
            summary = json.loads((output / "summary.json").read_text())
            self.assertEqual(
                [row["outcome"] for row in summary["runs"]], ["FAIL", "UNRESOLVED"]
            )
            self.assertEqual(summary["requested_runs"], 3)
            self.assertEqual(summary["independent_fixture_processes"], 2)
            self.assertEqual(
                summary["counts"],
                {"PASS": 0, "FAIL": 1, "UNRESOLVED": 1, "INFRASTRUCTURE_ERROR": 0},
            )

    def test_invalid_diagnostic_json_does_not_replace_a_fail_verdict(self):
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp) / "results"
            error = json.JSONDecodeError("invalid diagnostics", "invalid", 0)
            self.run_matrix(output, ["FAIL"], error)
            summary = json.loads((output / "summary.json").read_text())
            self.assertEqual(summary["counts"]["FAIL"], 1)
            self.assertEqual(summary["runs"][0]["diagnostics_error"], "JSONDecodeError")


if __name__ == "__main__":
    if sys.argv[1:] == ["--baseline-probe"]:
        with tempfile.TemporaryDirectory() as tmp:
            print(json.dumps(baseline_probe(Path(tmp))))
    else:
        unittest.main()
