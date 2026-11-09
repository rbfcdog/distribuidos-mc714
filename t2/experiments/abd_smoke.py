"""Record real ABD histories, check linearizability, and verify crash durability."""

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
    run_id = f"abd-smoke-{time.time_ns()}"
    env = dict(os.environ, RUN_ID=run_id, OBSERVER_URL="", MEMBERSHIP_CONFIG="./configs/compose.json")
    command = ["docker", "compose", "-p", f"mc714-abd-smoke-{os.getpid()}"]
    output = ROOT / "data" / "abd" / run_id
    output.mkdir(parents=True)
    history = {"run_id": run_id, "initial_value": 0, "operations": []}
    history_lock = threading.Lock()
    sequence = itertools.count(1)

    def compose(*args, capture=False):
        return subprocess.run(
            command + list(args), cwd=ROOT, env=env, check=True,
            stdout=subprocess.PIPE if capture else None, text=True, timeout=300,
        )

    def get(url):
        with urllib.request.urlopen(url, timeout=5) as response:
            return json.load(response)

    def rpc(node, operation, value, timeout_ms):
        payload = {"timeout_ms": timeout_ms}
        if operation == "write":
            payload["value"] = value
        request = urllib.request.Request(
            f"http://127.0.0.1:{18080 + node}/abd/{operation}",
            data=json.dumps(payload).encode(), headers={"Content-Type": "application/json"},
        )
        try:
            with urllib.request.urlopen(request, timeout=5) as response:
                return response.status, json.load(response)
        except urllib.error.HTTPError as error:
            return error.code, json.load(error)

    def perform(node, operation, value=None, timeout_ms=3000, cli=False):
        with history_lock:
            identifier = f"op-{next(sequence)}"
        entry = {"id": identifier, "node_id": f"node{node}", "operation": operation,
                 "start_ns": time.monotonic_ns()}
        if operation == "write":
            entry["value"] = value
        try:
            if cli:
                result = compose(
                    "exec", "-T", f"node{node}", "/lab", "-action", operation,
                    "-url", f"http://node{node}:8080", "-value", str(value), capture=True,
                )
                status, body = 200, json.loads(result.stdout)
            else:
                status, body = rpc(node, operation, value, timeout_ms)
            entry["end_ns"] = time.monotonic_ns()
            entry["http_status"], entry["response"] = status, body
            if status == 200:
                assert body["status"] == "completed" and body["operation"] == operation, body
                assert body["node_id"] == f"node{node}" and body["run_id"] == run_id, body
                assert body["cluster_id"] == history["cluster_id"], body
                entry["outcome"] = "completed"
                if operation == "read":
                    entry["value"] = body["register"]["value"]
                else:
                    assert body["register"]["value"] == value, body
            else:
                assert body["status"] == "error", body
                allowed = {"not_applied", "indeterminate"} if operation == "write" else {"failed"}
                assert body["outcome"] in allowed, body
                entry["outcome"] = body["outcome"]
            return entry
        except BaseException as error:
            entry["end_ns"] = time.monotonic_ns()
            entry["outcome"] = "indeterminate" if operation == "write" else "failed"
            entry["client_error"] = str(error)
            raise
        finally:
            with history_lock:
                history["operations"].append(entry)

    def save_history():
        path = output / "history.json"
        path.write_text(json.dumps(history, indent=2) + "\n")
        return path

    try:
        compose("up", "--build", "--detach", "--wait", "--wait-timeout", "60")
        cluster = get("http://127.0.0.1:18081/cluster")
        history["cluster_id"] = cluster["cluster_id"]
        assert perform(1, "read")["value"] == 0
        perform(1, "write", 0, cli=True)
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
            writes = list(pool.map(lambda node: perform(node, "write", 100 + node), (1, 2, 3)))
        assert all(entry["outcome"] == "completed" for entry in writes), writes
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
            reads = list(pool.map(lambda node: perform(node, "read"), (1, 2, 3)))
        assert all(entry["outcome"] == "completed" for entry in reads), reads
        assert len({entry["value"] for entry in reads}) == 1, reads
        compose("kill", "node3")
        assert perform(1, "write", 777)["outcome"] == "completed"
        assert perform(2, "read")["value"] == 777
        compose("kill", "node1")
        snapshot = output / "node1-register.json"
        compose("cp", f"node1:/data/{run_id}/node1/register.json", str(snapshot))
        persisted = json.loads(snapshot.read_text())
        assert persisted["node_id"] == "node1" and persisted["run_id"] == run_id, persisted
        assert persisted["cluster_id"] == history["cluster_id"], persisted
        assert persisted["register"]["value"] == 777, persisted
        compose("start", "node1")
        container = compose("ps", "--all", "--quiet", "node1", capture=True).stdout.strip()
        stopped = subprocess.run(
            ["docker", "wait", container], check=True, text=True, capture_output=True, timeout=10,
        )
        assert int(stopped.stdout.strip()) != 0, "same-run restart unexpectedly succeeded"
        failed_write = perform(2, "write", 999, timeout_ms=250)
        failed_read = perform(2, "read", timeout_ms=250)
        assert failed_write["http_status"] == 504 and failed_write["outcome"] == "not_applied", failed_write
        assert failed_read["http_status"] == 504 and failed_read["outcome"] == "failed", failed_read
        path = save_history()
        subprocess.run([sys.executable, "analysis/linearizability.py", str(path)], cwd=ROOT, check=True, timeout=30)
        print(json.dumps({"scenario": "abd", "operations": len(history["operations"]),
                          "history": str(path.relative_to(ROOT)), "durable_value": 777,
                          "one_crash": "available", "no_majority": "refused",
                          "same_run_restart": "refused"}), flush=True)
    except BaseException:
        compose("logs", "--no-color")
        raise
    finally:
        save_history()
        compose("down", "--volumes", "--remove-orphans")


if __name__ == "__main__":
    main()
