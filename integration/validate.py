"""Run real-client HTTP fixtures through the CLI on a disposable real server.

Dependencies are installed separately. All scheduling happens inside StaleFill;
health polling only waits for the fixture HTTP listener to start.
"""

import argparse
import json
import os
import shutil
import socket
import subprocess
import tempfile
import time
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def address():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return f"127.0.0.1:{s.getsockname()[1]}"


def validate(args):
    results = []
    artifact_dir = getattr(args, "artifact_dir", None)
    if artifact_dir:
        artifact_dir = Path(artifact_dir)
        artifact_dir.mkdir(parents=True, exist_ok=False)
    commands = {
        "redis-py": [args.python, str(ROOT / "integration/redis-py/app.py")],
        "go-redis": [str(Path(args.go_fixture).resolve())],
        "node-redis": [args.node, str(ROOT / "integration/node-redis/app.mjs")],
    }
    with tempfile.TemporaryDirectory(prefix="stalefill-clients-") as tmp:
        tmp = Path(tmp)
        for name in args.clients:
            for protocol in args.resp:
                for mode in ("broken", "fixed"):
                    proxy, http = address(), address()
                    base = f"http://{http}"
                    config = json.loads((ROOT / "examples/stalefill.json").read_text())
                    config["redis"].update(listen=proxy, upstream=args.upstream)
                    config["scenario"]["timeout"] = "10s"
                    for step in ("prepare", "read", "write", "verify"):
                        config[step]["url"] = base + "/items/42"
                    config["authoritative"]["url"] = base + "/authoritative/42"
                    retained = None
                    if artifact_dir:
                        retained = (
                            artifact_dir
                            / name
                            / f"resp{protocol}"
                            / f"{args.fill}-{mode}"
                        )
                        retained.mkdir(parents=True)
                        (retained / "config.json").write_text(
                            json.dumps(config, indent=2) + "\n"
                        )
                    config_path = tmp / "config.json"
                    config_path.write_text(json.dumps(config))
                    with (tmp / "fixture.log").open("w+") as log:
                        command = commands[name] + [
                            "--redis",
                            proxy,
                            "--listen",
                            http,
                            "--mode",
                            mode,
                            "--resp",
                            str(protocol),
                        ]
                        if name == "redis-py":
                            command += ["--fill", args.fill]
                        app = subprocess.Popen(command, stdout=log, stderr=log)
                        try:
                            deadline = time.monotonic() + 15
                            while True:
                                if app.poll() is not None:
                                    log.seek(0)
                                    raise RuntimeError(
                                        f"{name} fixture exited: {log.read()}"
                                    )
                                try:
                                    with urllib.request.urlopen(
                                        base + "/health", timeout=1
                                    ):
                                        break
                                except OSError:
                                    if time.monotonic() >= deadline:
                                        raise RuntimeError(
                                            f"{name} fixture startup timed out"
                                        )
                                    time.sleep(0.02)
                            first = None
                            for repetition in range(args.repetitions):
                                run = subprocess.run(
                                    [
                                        str(Path(args.binary).resolve()),
                                        "test",
                                        "--config",
                                        str(config_path),
                                        "--report",
                                        str(tmp / "report.json"),
                                        "--json",
                                    ],
                                    capture_output=True,
                                    text=True,
                                    timeout=15,
                                    check=False,  # Broken fixtures must exit 1 (FAIL).
                                )
                                if retained:
                                    (
                                        retained / f"run-{repetition + 1:02d}.json"
                                    ).write_text(run.stdout)
                                    (
                                        retained / f"run-{repetition + 1:02d}.stderr"
                                    ).write_text(run.stderr)
                                report = json.loads(run.stdout)
                                expected, code = (
                                    ("FAIL", 1) if mode == "broken" else ("PASS", 0)
                                )
                                fill_mode = (
                                    "hash"
                                    if args.fill == "single"
                                    else "transactional_hash"
                                )
                                if (
                                    report["outcome"] != expected
                                    or run.returncode != code
                                    or report.get("fill_mode") != fill_mode
                                ):
                                    raise RuntimeError(
                                        f"{name} RESP{protocol} {mode} iteration {repetition}: {report}"
                                    )
                                if mode == "broken" and [
                                    f["id"] for f in report["findings"]
                                ] != ["SF001"]:
                                    raise RuntimeError("wrong stale finding")
                                events = [e["event"] for e in report["events"]]
                                required = [
                                    "cache_miss_observed",
                                    "fill_transaction_identified",
                                    "fill_exec_held",
                                    "write_started",
                                    "invalidation_applied",
                                    "write_completed",
                                    "authoritative_confirmed",
                                    "fill_exec_released",
                                    "fill_exec_completed",
                                    "read_completed",
                                    "verify_started",
                                ]
                                if args.fill == "single":
                                    required = [
                                        "cache_miss_observed",
                                        "stale_set_held",
                                        "write_started",
                                        "invalidation_applied",
                                        "write_completed",
                                        "authoritative_confirmed",
                                        "stale_set_released",
                                        "stale_set_completed",
                                        "read_completed",
                                        "verify_started",
                                    ]
                                positions = [events.index(e) for e in required]
                                if positions != sorted(positions) or any(
                                    e["seq"] != i + 1
                                    for i, e in enumerate(report["events"])
                                ):
                                    raise RuntimeError("publication ordering changed")
                                if first is None:
                                    first = events
                                    if (
                                        args.trace_dir
                                        and args.fill == "transaction"
                                        and name == "redis-py"
                                        and protocol == 3
                                    ):
                                        trace = Path(args.trace_dir)
                                        trace.mkdir(parents=True, exist_ok=True)
                                        (
                                            trace / f"transactional-{mode}.json"
                                        ).write_text(
                                            json.dumps(report, indent=2) + "\n"
                                        )
                                elif first != events:
                                    raise RuntimeError(
                                        f"{name} RESP{protocol} {args.fill} {mode} logical trace changed "
                                        f"on iteration {repetition}: first={first}, current={events}"
                                    )
                            result = {
                                "client": name,
                                "resp": protocol,
                                "fill": args.fill,
                                "mode": mode,
                                "outcome": expected,
                                "repetitions": args.repetitions,
                                "schedule_misses": 0,
                                "logical_order_stable": True,
                            }
                            results.append(result)
                            print(json.dumps(result), flush=True)
                        finally:
                            app.terminate()
                            try:
                                app.wait(timeout=5)
                            except subprocess.TimeoutExpired:
                                app.kill()
                                app.wait()
                            if retained:
                                log.flush()
                                shutil.copyfile(
                                    tmp / "fixture.log", retained / "fixture.log"
                                )
    if args.output:
        Path(args.output).write_text(
            json.dumps({"server": args.server, "clients": results}, indent=2) + "\n"
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--upstream", required=True)
    parser.add_argument(
        "--server", required=True, help="exact tested server version label"
    )
    parser.add_argument("--binary", default=str(ROOT / "bin/stalefill"))
    parser.add_argument("--go-fixture", default=str(ROOT / "bin/go-redis-fixture"))
    parser.add_argument("--python", default=os.environ.get("PYTHON", "python3"))
    parser.add_argument("--node", default="node")
    parser.add_argument(
        "--clients",
        nargs="+",
        choices=["redis-py", "go-redis", "node-redis"],
        default=["redis-py", "go-redis", "node-redis"],
    )
    parser.add_argument("--resp", type=int, nargs="+", choices=[2, 3], default=[2, 3])
    parser.add_argument("--repetitions", type=int, default=24)
    parser.add_argument(
        "--fill", choices=["single", "transaction"], default="transaction"
    )
    parser.add_argument("--output")
    parser.add_argument("--trace-dir")
    parser.add_argument(
        "--artifact-dir",
        help="new directory retaining every CLI report, config and fixture log, including failed runs",
    )
    args = parser.parse_args()
    if args.repetitions < 1:
        parser.error("repetitions must be positive")
    if args.fill == "single" and args.clients != ["redis-py"]:
        parser.error("single-command fixture validation uses --clients redis-py")
    validate(args)
