"""Exercise real RA nodes and an independent observer; always remove this run."""

import concurrent.futures
import json
import os
from pathlib import Path
import subprocess
import time
import urllib.error
import urllib.request


ROOT = Path(__file__).resolve().parents[1]
REPETITIONS = 10


def request(url, payload=None):
    body = None if payload is None else json.dumps(payload).encode()
    req = urllib.request.Request(
        url, data=body, headers={"Content-Type": "application/json"}
    )
    with urllib.request.urlopen(req, timeout=5) as response:
        return json.load(response)


def main():
    run_id = f"ra-smoke-{time.time_ns()}"
    env = dict(os.environ, RUN_ID=run_id, OBSERVER_URL="http://observer:8090",
               MEMBERSHIP_CONFIG="./configs/compose.json")
    command = ["docker", "compose", "-p", f"mc714-ra-smoke-{os.getpid()}", "--profile", "verify"]

    def compose(*args, capture=False):
        return subprocess.run(
            command + list(args), cwd=ROOT, env=env, check=True,
            stdout=subprocess.PIPE if capture else None, text=True, timeout=300,
        )

    def report():
        return request("http://127.0.0.1:18089/report")

    def verify_report(expected):
        evidence = report()
        assert evidence["run_id"] == run_id, evidence
        assert evidence["entries"] == evidence["exits"] == expected, evidence
        assert evidence["active"] == evidence["violations"] == evidence["invalid_events"] == 0, evidence
        return evidence

    def exercise(node):
        for _ in range(REPETITIONS):
            result = request(
                f"http://127.0.0.1:{18080 + node}/ra/execute",
                {"hold_ms": 10, "wait_ms": 3000},
            )
            assert result["status"] == "completed" and result["node_id"] == f"node{node}", result
            assert result["run_id"] == run_id, result

    def expect_blocked(node):
        try:
            request(
                f"http://127.0.0.1:{18080 + node}/ra/execute",
                {"hold_ms": 0, "wait_ms": 250},
            )
        except urllib.error.HTTPError as error:
            assert error.code == 504, (node, error.code, error.read())
        else:
            raise AssertionError(f"node{node} entered without all participants")
        state = request(f"http://127.0.0.1:{18080 + node}/ra/state")
        assert state["state"] == "RELEASED" and state["deferred"] == 0, state

    try:
        compose("up", "--build", "--detach", "--wait", "--wait-timeout", "60")
        deadline = time.monotonic() + 5
        while True:
            try:
                initial = report()
                break
            except urllib.error.URLError:
                if time.monotonic() >= deadline:
                    raise
                time.sleep(0.05)
        assert initial["entries"] == initial["exits"] == initial["active"] == 0, initial
        result = json.loads(compose(
            "exec", "-T", "node1", "/lab", "-action", "ra", "-url", "http://node1:8080",
            "-hold-ms", "10", "-wait-ms", "3000", "-timeout", "5s", capture=True,
        ).stdout)
        assert result["status"] == "completed" and result["run_id"] == run_id, result
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
            list(pool.map(exercise, (1, 2, 3)))
        completed = 1 + 3 * REPETITIONS
        evidence = verify_report(completed)
        logical_messages = sum(
            request(f"http://127.0.0.1:{port}/ra/state")["logical_messages"]
            for port in (18081, 18082, 18083)
        )
        assert logical_messages == 2 * (3 - 1) * completed, logical_messages
        print(json.dumps({"scenario": "normal", "completed": completed,
                          "logical_messages": logical_messages, "observer": evidence}), flush=True)
        compose("kill", "node3")
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            list(pool.map(expect_blocked, (1, 2)))
        verify_report(completed)
        print("PASS: concurrent critical sections do not overlap; crash blocks entry; deadlines clean local state", flush=True)
    except BaseException:
        compose("logs", "--no-color")
        raise
    finally:
        compose("down", "--volumes", "--remove-orphans")


if __name__ == "__main__":
    main()
