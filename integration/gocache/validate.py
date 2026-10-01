"""Run a StaleFill CLI against an unmodified gocache dependency.

Only the separate doctor preflight polls for async publication to verify the
sequential baseline. Race runs use the ordinary direct HTTP probes without
polling, sleeps, wrappers, retries of an outcome, or setter synchronization.
"""

import argparse
import json
import socket
import subprocess
import threading
import time
import urllib.request
from collections import Counter
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

KEY = "stalefill:gocache:item:1"
CODES = {"PASS": 0, "FAIL": 1, "UNRESOLVED": 2, "INFRASTRUCTURE_ERROR": 3}


class BaselineError(RuntimeError):
    """An expected failure of a sequential baseline observation."""


def address():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return f"127.0.0.1:{sock.getsockname()[1]}"


def http(base, path, method="GET", value=None):
    body = None if value is None else json.dumps({"value": value}).encode()
    req = urllib.request.Request(base + path, data=body, method=method)
    if body is not None:
        req.add_header("Content-Type", "application/json")
    with urllib.request.urlopen(req, timeout=5) as response:
        return json.load(response)


def redis_get(upstream):
    # Independent observer, only used by sequential baseline verification.
    host, port = upstream.rsplit(":", 1)
    with socket.create_connection((host, int(port)), timeout=2) as sock:
        sock.sendall(f"*2\r\n$3\r\nGET\r\n${len(KEY)}\r\n{KEY}\r\n".encode())
        with sock.makefile("rb") as stream:
            header = stream.readline()
            if header == b"$-1\r\n":
                return None
            if not header.startswith(b"$"):
                raise BaselineError("unexpected observer Redis response")
            length = int(header[1:])
            value = stream.read(length)
            if stream.read(2) != b"\r\n":
                raise BaselineError("incomplete observer Redis response")
            return value.decode()


def config(proxy, upstream, base):
    def probe(path, method="GET", value=None, expected=None):
        p = {"method": method, "url": base + path, "status": 200}
        if value is not None:
            p["json"] = {"value": value}
        if expected is not None:
            p["assert"] = {"json_path": "$.value", "equals": expected}
        return p

    return {
        "version": 1,
        "redis": {"listen": proxy, "upstream": upstream, "allow_remote": False},
        "scenario": {
            "type": "stale_fill_after_invalidation",
            "key": KEY,
            "timeout": "10s",
        },
        "prepare": probe("/item", "PUT", "V1"),
        "read": probe("/item", expected="V1"),
        "write": probe("/item", "PUT", "V2"),
        "authoritative": probe("/authoritative/item", expected="V2"),
        "verify": probe("/item", expected="V2"),
    }


@contextmanager
def fixture(args, folder):
    proxy, listen = address(), address()
    base = "http://" + listen
    with (
        (folder / "app.json").open("w") as log,
        (folder / "app.stderr").open("w") as errors,
    ):
        app = subprocess.Popen(
            [
                str(args.fixture),
                "--redis",
                proxy,
                "--listen",
                listen,
                "--resp",
                str(args.resp),
            ],
            stdout=log,
            stderr=errors,
        )
        try:
            deadline = time.monotonic() + 10
            while True:
                if app.poll() is not None:
                    raise RuntimeError("fixture exited before readiness")
                try:
                    http(base, "/health")
                    break
                except OSError:
                    if time.monotonic() >= deadline:
                        raise RuntimeError("fixture readiness timed out")
                    # Startup readiness only; never called during a race.
                    time.sleep(0.02)
            yield base, config(proxy, args.upstream, base)
        finally:
            app.terminate()
            try:
                app.wait(timeout=15)
            except subprocess.TimeoutExpired:
                app.kill()
                app.wait()
                raise RuntimeError("fixture Close/drain exceeded cleanup guard")


def cli(args, folder, cfg, command):
    path = folder / "config.json"
    path.write_text(json.dumps(cfg, indent=2) + "\n")
    run = subprocess.run(
        [
            str(args.binary),
            command,
            "--config",
            str(path),
            "--report",
            str(folder / "report.json"),
            "--json",
        ],
        capture_output=True,
        text=True,
        timeout=15,
        check=False,
    )
    (folder / "cli.stderr").write_text(run.stderr)
    report = json.loads(run.stdout)
    if run.returncode != CODES[report["outcome"]]:
        raise RuntimeError("CLI outcome and exit code disagree")
    return report


def observation(report, snapshot):
    """Distinguish missing schedule events using passive fixture metadata."""
    reads = [e for e in snapshot if e["event"] == "http_get_started"]
    if len(reads) < 3:
        return {"diagnosis": "baseline_or_prepare_incomplete"}
    reader = reads[2]["request"]  # Two baseline GET probes precede it.
    metadata = {"reader_request": reader}
    reader_events = [e for e in snapshot if e.get("request") == reader]
    miss = any(
        e["event"] == "redis_completed" and e.get("result") == "miss"
        for e in reader_events
    )
    hit = any(
        e["event"] == "redis_completed"
        and e.get("command") == "get"
        and e.get("result") == "ok"
        for e in reader_events
    )
    metadata.update(reader_Redis_miss=miss, reader_Redis_hit=hit)
    returned = next(
        (e for e in reader_events if e["event"] == "http_get_handler_returned"), None
    )
    loads = [e for e in snapshot if e["event"] == "loader_completed"]
    sets = [
        e
        for e in snapshot
        if e["event"] == "redis_started" and e.get("command") == "set"
    ]
    loader_index = next(
        (i for i, e in enumerate(loads) if e.get("request") == reader), None
    )
    # There is one FIFO setter and one key. This is observation, not association
    # supplied to StaleFill; no library internals are accessed.
    if loader_index is not None and loader_index < len(sets):
        load, fill = loads[loader_index], sets[loader_index]
        metadata["load_to_SET_attempt_us"] = fill["elapsed_us"] - load["elapsed_us"]
        if returned:
            metadata["handler_return_to_SET_attempt_us"] = (
                fill["elapsed_us"] - returned["elapsed_us"]
            )
        completed = next(
            (e for e in snapshot if e.get("operation") == fill["seq"]), None
        )
        if completed:
            metadata["SET_attempt_to_completion_us"] = (
                completed["elapsed_us"] - fill["elapsed_us"]
            )
    names = {e["event"] for e in report["events"]}
    if report["outcome"] == "FAIL":
        diagnosis = "complete_schedule_observed_stale_verification"
    elif hit and "cache_miss_observed" not in names:
        diagnosis = "baseline_async_publication_survived_prepare_no_race_miss"
    elif "stale_set_held" in names and "write_started" not in names:
        diagnosis = "reader_done_selected_despite_held_async_fill"
    elif miss and "stale_set_held" not in names:
        diagnosis = "reader_done_before_fill_barrier"
    else:
        diagnosis = "inspect_trace"
    metadata["diagnosis"] = diagnosis
    return metadata


def baseline(args, folder):
    evidence = {}
    with fixture(args, folder) as (base, cfg):
        # The ordinary doctor write probe delegates to an external sequential
        # observer. It checks publication/cache hit, then calls the same PUT.
        # The application and the race configuration have no such delegation.
        class Observer(BaseHTTPRequestHandler):
            def log_message(self, *_):
                pass

            def do_PUT(self):
                try:
                    self.rfile.read(int(self.headers.get("Content-Length", "0")))
                    deadline = time.monotonic() + 5
                    while redis_get(args.upstream) != "V1":
                        if time.monotonic() >= deadline:
                            raise BaselineError(
                                "baseline old async fill did not reach Redis"
                            )
                    evidence["redis_received_V1"] = True
                    before = http(base, "/diagnostics")
                    loads = sum(e["event"] == "loader_completed" for e in before)
                    # A Redis entry can precede setCache cleanup. Repeat normal
                    # external reads until an actual Redis cache hit is observed.
                    while True:
                        if http(base, "/item")["value"] != "V1":
                            raise BaselineError(
                                "baseline repeated GET did not return V1"
                            )
                        after = http(base, "/diagnostics")
                        if (
                            sum(e["event"] == "loader_completed" for e in after)
                            != loads
                        ):
                            raise BaselineError("baseline repeated GET reloaded")
                        if any(
                            e["seq"] > len(before)
                            and e["event"] == "redis_completed"
                            and e.get("command") == "get"
                            and e.get("result") == "ok"
                            for e in after
                        ):
                            break
                        if time.monotonic() >= deadline:
                            raise BaselineError(
                                "baseline Redis cache hit was not observed"
                            )
                    evidence["repeated_GET_hit_Redis_without_loader"] = True
                    if http(base, "/item", "PUT", "V2")["value"] != "V2":
                        raise BaselineError("baseline PUT did not return V2")
                    if http(base, "/authoritative/item")["value"] != "V2":
                        raise BaselineError(
                            "baseline authoritative read did not return V2"
                        )
                    if redis_get(args.upstream) is not None:
                        raise BaselineError(
                            "baseline Delete did not remove the Redis entry"
                        )
                    evidence["PUT_updated_authoritative_and_Delete_removed_entry"] = (
                        True
                    )
                    self.send_response(200)
                    self.end_headers()
                    self.wfile.write(b"{}")
                except (BaselineError, OSError, json.JSONDecodeError) as error:
                    evidence["error"] = str(error)
                    self.send_response(500)
                    self.end_headers()

        server = ThreadingHTTPServer(("127.0.0.1", 0), Observer)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            cfg["write"]["url"] = f"http://127.0.0.1:{server.server_port}/write"
            report = cli(args, folder, cfg, "doctor")
            evidence["doctor_outcome"] = report["outcome"]
            evidence["following_GET_returned_V2"] = report["outcome"] == "PASS"
        finally:
            server.shutdown()
            server.server_close()
            thread.join()
    (folder / "evidence.json").write_text(json.dumps(evidence, indent=2) + "\n")
    if report["outcome"] != "PASS":
        raise RuntimeError(f"baseline failed; no race executed: {evidence}")
    return evidence


def validate(args):
    args.output.mkdir(parents=True, exist_ok=False)
    version = subprocess.check_output([str(args.binary), "version"], text=True).strip()
    doctor = args.output / "doctor"
    doctor.mkdir()
    evidence = baseline(args, doctor)
    rows = []
    processes = 0
    try:
        for number in range(1, args.runs + 1):
            folder = args.output / f"run-{number:02d}"
            folder.mkdir()
            with fixture(args, folder) as (base, cfg):
                processes += 1
                report = cli(args, folder, cfg, "test")
                events = report["events"]
                row = {
                    "run": number,
                    "outcome": report["outcome"],
                    "findings": [f["id"] for f in report["findings"]],
                    "stage": events[-1]["state"] if events else "START",
                    "last_event": events[-1]["event"] if events else None,
                    "race_started": any(e["event"] == "read_started" for e in events),
                    "fill_held": any(e["event"] == "stale_set_held" for e in events),
                    "write_started": any(e["event"] == "write_started" for e in events),
                    "duration_ms": report["duration_ms"],
                    "diagnosis": "diagnostics_not_collected",
                }
                # Keep the CLI verdict even if optional observation or cleanup
                # fails. Unexpected errors still propagate with their traceback.
                rows.append(row)
                try:
                    snapshot = http(base, "/diagnostics")
                except (OSError, json.JSONDecodeError) as error:
                    row["diagnosis"] = "diagnostics_unavailable"
                    row["diagnostics_error"] = type(error).__name__
                else:
                    (folder / "observed-at-cli-return.json").write_text(
                        json.dumps(snapshot, indent=2) + "\n"
                    )
                    row.update(observation(report, snapshot))
            print(json.dumps(row), flush=True)
    finally:
        summary = {
            "upstream": "eko/gocache",
            "tag": "lib/v4.4.0",
            "commit": "515e65d2cd170b9ba807c64f9b86ed71f6842754",
            "server": args.server,
            "resp": args.resp,
            "stalefill": version,
            "baseline": evidence,
            "requested_runs": args.runs,
            "independent_fixture_processes": processes,
            "counts": {
                name: sum(row["outcome"] == name for row in rows) for name in CODES
            },
            "stages": dict(
                Counter(
                    (row["outcome"] + "/" + row["stage"] + "/" + str(row["last_event"]))
                    for row in rows
                )
            ),
            "diagnoses": dict(Counter(row["diagnosis"] for row in rows)),
            "runs": rows,
        }
        (args.output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    print(
        json.dumps({"counts": summary["counts"], "stages": summary["stages"]}),
        flush=True,
    )
    return summary


def require_positive_control(args, summary):
    """Accept only a complete SF001 schedule on the pinned vulnerable target.

    This is validation of the detector, not a different library verdict. Read
    every retained report; counts alone cannot establish schedule completion.
    """
    if (
        summary["baseline"]["doctor_outcome"] != "PASS"
        or summary["requested_runs"] != args.runs
        or summary["independent_fixture_processes"] != args.runs
        or len(summary["runs"]) != args.runs
        or summary["counts"] != {
            name: args.runs if name == "FAIL" else 0 for name in CODES
        }
    ):
        raise RuntimeError("positive control requires every requested run to FAIL SF001")
    required = (
        "baseline_completed",
        "prepared",
        "read_started",
        "cache_miss_observed",
        "stale_set_held",
        "write_started",
        "invalidation_applied",
        "write_completed",
        "authoritative_confirmed",
        "stale_set_released",
        "stale_set_completed",
        "verify_started",
        "verification_stale",
    )
    for row in summary["runs"]:
        report = json.loads(
            (args.output / f"run-{row['run']:02d}" / "report.json").read_text()
        )
        events = report["events"]
        names = [event["event"] for event in events]
        if (
            report["outcome"] != "FAIL"
            or [f["id"] for f in report["findings"]] != ["SF001"]
        ):
            raise RuntimeError(f"run {row['run']}: expected FAIL SF001")
        if any(names.count(name) != 1 for name in required + ("read_completed",)):
            raise RuntimeError(f"run {row['run']}: incomplete or ambiguous schedule")
        positions = [names.index(name) for name in required]
        if positions != sorted(positions) or not (
            names.index("cache_miss_observed")
            < names.index("read_completed")
            < names.index("verify_started")
        ):
            raise RuntimeError(f"run {row['run']}: schedule order is incomplete")
        miss = events[names.index("cache_miss_observed")]
        held = events[names.index("stale_set_held")]
        if (
            not all(miss.get(field, 0) > 0 for field in ("miss_episode", "miss_connection"))
            or held.get("fill_connection", 0) < 1
        ):
            raise RuntimeError(f"run {row['run']}: missing publication attribution")
        for name in required[4:]:
            event = events[names.index(name)]
            if any(
                event.get(field) != value
                for field, value in (
                    ("miss_episode", miss["miss_episode"]),
                    ("miss_connection", miss["miss_connection"]),
                    ("fill_connection", held["fill_connection"]),
                )
            ):
                raise RuntimeError(f"run {row['run']}: publication attribution changed")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--upstream", required=True)
    parser.add_argument(
        "--server", required=True, help="exact server version, verified independently"
    )
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--fixture", type=Path, required=True)
    parser.add_argument(
        "--output", type=Path, required=True, help="new artifact directory"
    )
    parser.add_argument("--runs", type=int, default=24)
    parser.add_argument("--resp", type=int, choices=[2, 3], default=2)
    parser.add_argument(
        "--expect-sf001",
        action="store_true",
        help="fail validation unless every pinned-target run completes the SF001 schedule",
    )
    args = parser.parse_args()
    if args.runs < 1:
        parser.error("--runs must be positive")
    args.binary = args.binary.resolve()
    args.fixture = args.fixture.resolve()
    summary = validate(args)
    if args.expect_sf001:
        require_positive_control(args, summary)
