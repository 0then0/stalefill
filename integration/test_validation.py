"""Retain evidence when compatibility validation rejects a CLI result."""

import argparse
import contextlib
import copy
import importlib.util
import io
import json
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

spec = importlib.util.spec_from_file_location(
    "client_validation", Path(__file__).with_name("validate.py")
)
validation = importlib.util.module_from_spec(spec)
spec.loader.exec_module(validation)


class ValidationEvidenceTest(unittest.TestCase):
    def run_validation(self, output, reports):
        args = argparse.Namespace(
            artifact_dir=output,
            clients=["redis-py"],
            resp=[2],
            fill="transaction",
            mode="broken",
            python="python",
            go_fixture="unused",
            node="node",
            upstream="127.0.0.1:6379",
            binary="unused",
            repetitions=len(reports),
            trace_dir=None,
            output=None,
        )
        runs = [
            subprocess.CompletedProcess([], 1, json.dumps(report), "diagnostic")
            for report in reports
        ]
        app = Mock()
        app.poll.return_value = None
        with (
            patch.object(validation.subprocess, "Popen", return_value=app),
            patch.object(validation.subprocess, "run", side_effect=runs),
            patch.object(validation.urllib.request, "urlopen"),
            contextlib.redirect_stdout(io.StringIO()),
        ):
            validation.validate(args)

    def report(self):
        return json.loads(
            (
                Path(__file__).resolve().parents[1]
                / "docs/traces/transactional-broken.json"
            ).read_text()
        )

    def test_changed_trace_is_rejected_and_both_reports_are_retained(self):
        first = self.report()
        changed = copy.deepcopy(first)
        changed["events"].append(
            {"event": "upstream connection failed", "seq": len(changed["events"]) + 1}
        )
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp) / "reports"
            with self.assertRaisesRegex(RuntimeError, "first=.*current="):
                self.run_validation(output, [first, changed])
            case = output / "redis-py/resp2/transaction-broken"
            for number, report in enumerate((first, changed), 1):
                self.assertEqual(
                    json.loads((case / f"run-{number:02d}.json").read_text()), report
                )
                self.assertEqual(
                    (case / f"run-{number:02d}.stderr").read_text(), "diagnostic"
                )
            self.assertTrue((case / "config.json").is_file())
            self.assertTrue((case / "fixture.log").is_file())

    def test_unexpected_outcome_is_rejected_after_retaining_report(self):
        report = self.report()
        report["outcome"] = "UNRESOLVED"
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp) / "reports"
            with self.assertRaisesRegex(RuntimeError, "iteration 0"):
                self.run_validation(output, [report])
            self.assertEqual(
                json.loads(
                    (
                        output / "redis-py/resp2/transaction-broken/run-01.json"
                    ).read_text()
                ),
                report,
            )


if __name__ == "__main__":
    unittest.main()
