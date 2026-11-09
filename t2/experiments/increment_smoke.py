"""Verify protected increments with real nodes, an observer and recorded history."""

import concurrent.futures
import itertools
import json
import os
from pathlib import Path
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request


ROOT = Path(__file__).resolve().parents[1]


def main():
    run_id = f"increment-smoke-{time.time_ns()}"
    env = dict(os.environ, RUN_ID=run_id, OBSERVER_URL="http://observer:8090",
               MEMBERSHIP_CONFIG="./configs/integrated.json")
    command = ["docker", "compose", "-p", f"mc714-increment-smoke-{os.getpid()}", "--profile", "verify"]
    output = ROOT / "data" / "integrated" / run_id
    output.mkdir(parents=True)
    history = {"run_id": run_id, "initial_value": 0, "operations": []}
    history_lock = threading.Lock()
    identifiers = itertools.count(1)

    def compose(*args, capture=False):
        return subprocess.run(command + list(args), cwd=ROOT, env=env, check=True,
                              stdout=subprocess.PIPE if capture else None, text=True, timeout=300)

    def get(url):
        with urllib.request.urlopen(url, timeout=5) as response:
            return json.load(response)

    def call(node, operation, timeout_ms=3000):
        path = "/increment" if operation == "increment" else f"/abd/{operation}"
        payload = {"timeout_ms": timeout_ms}
        if operation == "write":
            payload["value"] = 123
        request = urllib.request.Request(
            f"http://127.0.0.1:{18080 + node}{path}", data=json.dumps(payload).encode(),
            headers={"Content-Type": "application/json"},
        )
        try:
            with urllib.request.urlopen(request, timeout=5) as response:
                return response.status, json.load(response)
        except urllib.error.HTTPError as error:
            return error.code, json.load(error)

    def perform(node, operation, timeout_ms=3000, cli=False):
        with history_lock:
            identifier = f"op-{next(identifiers)}"
        entry = {"id": identifier, "operation": operation, "node_id": f"node{node}",
                 "start_ns": time.monotonic_ns()}
        if operation == "write":
            entry["value"] = 123
        try:
            if cli:
                result = compose("exec", "-T", f"node{node}", "/lab", "-action", "increment",
                                 "-url", f"http://node{node}:8080", capture=True)
                status, body = 200, json.loads(result.stdout)
            else:
                status, body = call(node, operation, timeout_ms)
            entry["end_ns"] = time.monotonic_ns()
            entry["http_status"], entry["response"] = status, body
            if status == 200:
                assert body["status"] == "completed" and body["run_id"] == run_id, body
                assert body["node_id"] == f"node{node}" and body["cluster_id"] == history["cluster_id"], body
                entry["outcome"] = "completed"
                entry["value"] = body["register"]["value"]
                if operation == "increment":
                    entry["before"] = body["before"]
                    assert entry["value"] == entry["before"] + 1, body
            else:
                assert body["status"] == "error" and body["outcome"] in {"not_applied", "indeterminate", "failed"}, body
                entry["outcome"] = body["outcome"]
            return entry
        except BaseException as error:
            entry["end_ns"] = time.monotonic_ns()
            entry["outcome"] = "failed" if operation == "read" else "indeterminate"
            entry["client_error"] = str(error)
            raise
        finally:
            with history_lock:
                history["operations"].append(entry)

    def report():
        evidence = get("http://127.0.0.1:18089/report")
        assert evidence["run_id"] == run_id and evidence["cluster_id"] == history["cluster_id"], evidence
        assert evidence["entries"] == evidence["exits"] == 10, evidence
        assert evidence["active"] == evidence["violations"] == evidence["invalid_events"] == 0, evidence
        return evidence

    def save_history():
        path = output / "history.json"
        path.write_text(json.dumps(history, indent=2) + "\n")
        return path

    def worker(node):
        results = [perform(node, "increment") for _ in range(3)]
        assert all(entry["outcome"] == "completed" for entry in results), results
        return results

    try:
        compose("up", "--build", "--detach", "--wait", "--wait-timeout", "60")
        cluster = get("http://127.0.0.1:18081/cluster")
        assert cluster["mode"] == "integrated", cluster
        history["cluster_id"] = cluster["cluster_id"]
        forbidden = perform(2, "write")
        assert forbidden["http_status"] == 403 and forbidden["outcome"] == "not_applied", forbidden
        assert perform(1, "read")["value"] == 0
        first = perform(1, "increment", cli=True)
        assert first["before"] == 0 and first["value"] == 1
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
            results = [entry for group in pool.map(worker, (1, 2, 3)) for entry in group]
        assert sorted([first["value"]] + [entry["value"] for entry in results]) == list(range(1, 11))
        for node in (1, 2, 3):
            assert perform(node, "read")["value"] == 10
        report()
        compose("kill", "node3")
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            failures = list(pool.map(lambda node: perform(node, "increment", timeout_ms=250), (1, 2)))
        assert all(entry["http_status"] == 504 and entry["outcome"] == "not_applied" for entry in failures), failures
        assert perform(2, "read")["value"] == 10  # ABD still has its majority.
        for node in (1, 2):
            state = get(f"http://127.0.0.1:{18080 + node}/ra/state")
            assert state["state"] == "RELEASED" and state["deferred"] == 0, state
        evidence = report()
        (output / "observer.json").write_text(json.dumps(evidence, indent=2) + "\n")
        path = save_history()
        subprocess.run([sys.executable, "analysis/linearizability.py", str(path)], cwd=ROOT, check=True, timeout=30)
        print(json.dumps({"scenario": "integrated", "completed_increments": 10, "final_value": 10,
                          "violations": evidence["violations"], "after_crash": "increments refused, ABD read succeeds",
                          "history": str(path.relative_to(ROOT))}), flush=True)
    except BaseException:
        compose("logs", "--no-color")
        raise
    finally:
        save_history()
        compose("down", "--volumes", "--remove-orphans")


if __name__ == "__main__":
    main()
