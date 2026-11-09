#!/usr/bin/env python3
"""Sequential, immutable RA/ABD campaigns. Parent builds bin/load before pilot."""
from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import queue
import random
import signal
import subprocess
import sys
import tarfile
import tempfile
import threading
import time
import urllib.request
import uuid

ROOT = Path(__file__).resolve().parents[1]
NS = 1_000_000_000
SHUFFLE_SEED = 7142026
BINDINGS = ("source_digest", "source_archive_sha256", "workload_sha256", "runner_sha256", "image")
PROTOCOL = {
    "nodes": [3, 5, 7], "repetitions": 10, "warmup_s": 10,
    "normal_duration_s": 60, "fault_duration_s": 35, "inject_s": 5,
    "heal_s": 20, "hold_ms": 5, "write_percent": 50, "wait_ms": 1000,
    "client_timeout_s": 2, "workers": 32, "queue": 128,
    "topology": "client-bridge+internal-protocol-peer-aliases",
    "pilot_rates": [2, 5, 10, 20], "pilot_warmup_s": 2,
    "pilot_duration_s": 8, "pilot_fault_duration_s": 12,
    "pilot_inject_s": 3, "pilot_heal_s": 7, "guard_s": 1,
}


class CampaignError(RuntimeError):
    pass


def require(condition, message):
    if not condition:
        raise CampaignError(message)


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def read_json(path: Path):
    with path.open(encoding="utf-8") as stream:
        return json.load(stream)


def save_json(path: Path, value):
    """Create an evidence file once, never replace an existing artifact."""
    with path.open("x", encoding="utf-8") as stream:
        json.dump(value, stream, indent=2, sort_keys=True, allow_nan=False)
        stream.write("\n")
        stream.flush()
        os.fsync(stream.fileno())


def command(args, timeout=60, *, check=True):
    args = [str(arg) for arg in args]
    start_wall, start_mono = time.time_ns(), time.monotonic_ns()
    try:
        result = subprocess.run(args, cwd=ROOT, text=True, capture_output=True,
                                timeout=timeout, start_new_session=True)
        code, stdout, stderr = result.returncode, result.stdout, result.stderr
    except subprocess.TimeoutExpired as exc:
        code = 124
        stdout = exc.stdout or ""
        stderr = exc.stderr or ""
        if isinstance(stdout, bytes):
            stdout = stdout.decode(errors="replace")
        if isinstance(stderr, bytes):
            stderr = stderr.decode(errors="replace")
        stderr += f"\ncommand timed out after {timeout}s"
    except OSError as exc:
        code, stdout, stderr = 127, "", str(exc)
    record = {"command": args, "start_unix_ns": start_wall,
              "end_unix_ns": time.time_ns(),
              "monotonic_duration_ns": time.monotonic_ns() - start_mono,
              "returncode": code, "stdout": stdout, "stderr": stderr}
    if check and code:
        raise CampaignError(f"command failed: {args}: {stderr or stdout}")
    return record


def host_state():
    mem = {}
    for line in Path("/proc/meminfo").read_text().splitlines():
        key, value = line.split(":", 1)
        if key in {"MemTotal", "MemFree", "MemAvailable", "SwapTotal", "SwapFree"}:
            mem[key + "_bytes"] = int(value.split()[0]) * 1024
    cpu = next((line.split(":", 1)[1].strip()
                for line in Path("/proc/cpuinfo").read_text().splitlines()
                if line.startswith("model name")), platform.processor())
    return {"unix_ns": time.time_ns(), "os": platform.platform(),
            "cpu_model": cpu, "logical_cpus": os.cpu_count(),
            "affinity": sorted(os.sched_getaffinity(0)), "memory": mem,
            "loadavg": list(os.getloadavg()), "dedicated_or_idle_host": False}


def source_files(root=ROOT):
    files = [root / "go.mod", root / "Dockerfile"]
    for directory in ("cmd", "internal"):
        files.extend(path for path in (root / directory).rglob("*") if path.is_file())
    return sorted(files, key=lambda path: path.relative_to(root).as_posix())


def source_digest(root=ROOT):
    digest = hashlib.sha256()
    for path in source_files(root):
        digest.update(path.relative_to(root).as_posix().encode() + b"\0")
        digest.update(path.read_bytes())
        digest.update(b"\0")
    return digest.hexdigest()


def membership(nodes):
    require(nodes in (3, 5, 7), "only 3/5/7 nodes are supported")
    return {"mode": "standalone", "members": [
        {"id": f"node{i}", "url": f"http://peer-node{i}:8080"}
        for i in range(1, nodes + 1)]}


def topology(nodes, image, run_id, membership_path):
    membership(nodes)
    require(image.startswith("sha256:") and len(image) == 71, "immutable Docker image ID required")
    services, volumes = {}, {}
    for i in range(1, nodes + 1):
        node = f"node{i}"
        volume = f"{node}-data"
        volumes[volume] = {}
        services[node] = {
            "image": image, "pull_policy": "never", "restart": "no",
            "read_only": True, "cap_drop": ["ALL"],
            "security_opt": ["no-new-privileges:true"], "user": "65532:65532",
            "pids_limit": 64, "cpus": 1.0, "mem_limit": "256m",
            "stop_grace_period": "8s",
            "command": ["-id", node, "-run-id", run_id, "-config",
                        "/etc/mc714/cluster.json", "-listen", ":8080",
                        "-observer", "", "-data-dir", "/data"],
            "ports": [f"127.0.0.1:{18080 + i}:8080"],
            "networks": {"client": {}, "protocol": {"aliases": [f"peer-{node}"]}},
            "volumes": [
                {"type": "bind", "source": str(Path(membership_path).resolve()),
                 "target": "/etc/mc714/cluster.json", "read_only": True},
                {"type": "volume", "source": volume, "target": "/data"}],
        }
    return {"services": services, "volumes": volumes,
            "networks": {"client": {}, "protocol": {"internal": True}}}


def percentile(values, probability):
    if not values:
        return None
    ordered = sorted(values)
    index = (len(ordered) - 1) * probability
    lower = math.floor(index)
    upper = math.ceil(index)
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (index - lower)


def load_operations(path):
    rows = []
    with path.open(encoding="utf-8") as stream:
        for number, line in enumerate(stream, 1):
            row = json.loads(line)
            require(isinstance(row, dict) and "type" in row,
                    f"invalid raw record at line {number}")
            rows.append(row)
    return rows


def validate_raw(rows, manifest):
    metadata = [row for row in rows if row["type"] == "metadata"]
    require(len(metadata) == 1, "missing/duplicate workload metadata")
    expected = {key: manifest[key] for key in
                ("algorithm", "nodes", "run_id", "seed", "rate", "workers",
                 "queue", "hold_ms", "write_percent", "wait_ms")}
    expected.update(warmup_ns=int(manifest["warmup_s"] * NS),
                    duration_ns=int(manifest["duration_s"] * NS), client_timeout_ns=2 * NS)
    require(all(metadata[0].get(key) == value for key, value in expected.items()),
            "workload metadata differs from manifest")
    for phase in ("warmup", "measurement"):
        markers = [row for row in rows if row["type"] == "phase" and row.get("phase") == phase]
        starts = [row for row in markers if row.get("event") == "start"]
        ends = [row for row in markers if row.get("event") == "end"]
        require(len(starts) == len(ends) == 1, f"missing phase boundaries: {phase}")
        elapsed = ends[0].get("elapsed_ns")
        wall = ends[0].get("wall_elapsed_ns")
        require(isinstance(elapsed, int) and isinstance(wall, int) and
                elapsed > 0 and abs(elapsed - wall) <= NS // 10,
                "phase clock step or missing elapsed evidence")
        limit = manifest["warmup_s" if phase == "warmup" else "duration_s"] * NS
        for row in rows:
            if row["type"] != "operation" or row.get("phase") != phase:
                continue
            scheduled, start, end = row.get("scheduled_ns"), row.get("start_ns"), row.get("end_ns")
            require(isinstance(scheduled, int) and 0 <= scheduled < limit and
                    isinstance(end, int) and scheduled <= end <= elapsed,
                    "invalid operation timing")
            require(start is None or isinstance(start, int) and scheduled <= start <= end,
                    "invalid invocation timing")
            require(isinstance(row.get("generator_lag_ns"), int) and row["generator_lag_ns"] >= 0,
                    "missing generator lag evidence")
            require(row.get("node_id") in {f"node{i}" for i in range(1, manifest["nodes"] + 1)},
                    "unexpected operation node")
    for stage in ("baseline", "final"):
        snapshots = [row for row in rows if row["type"] == "snapshot" and row.get("stage") == stage]
        require(len(snapshots) == 1 and len(snapshots[0].get("nodes", [])) == manifest["nodes"],
                f"missing {stage} snapshot evidence")
        if stage == "baseline" or manifest["scenario"] != "crash":
            require(all("state" in node for node in snapshots[0]["nodes"]),
                    f"unexpected missing {stage} snapshot")
    measurement_start = next(row["unix_ns"] for row in rows if row["type"] == "phase"
                             and row.get("phase") == "measurement" and row.get("event") == "start")
    return measurement_start


def pilot_criteria(rows):
    operations = [row for row in rows if row["type"] == "operation" and row.get("phase") == "measurement"]
    completed = sum(row.get("outcome") == "completed" for row in operations)
    lags = [row["generator_lag_ns"] for row in operations]
    forbidden = {name: sum(row.get("outcome") == name for row in operations)
                 for name in ("invalid_response", "queue_full", "deadline")}
    p99 = percentile(lags, .99)
    fraction = completed / len(operations) if operations else 0
    return {"offered": len(operations), "completed": completed,
            "success_fraction": fraction, "generator_lag_p99_ns": p99,
            "forbidden_outcomes": forbidden,
            "qualified": len(operations) >= 10 and fraction >= .95 and
            not any(forbidden.values()) and p99 is not None and p99 <= 10_000_000}


def fault_invariants(rows, events, algorithm, scenario, duration_s):
    if scenario == "normal":
        return {"validated": True, "scenario": "normal"}
    records = events["events"]
    faults = [event for event in records if event["event"] == scenario]
    heals = [event for event in records if event["event"] == "heal"]
    require(len(faults) == 1 and len(heals) == (1 if scenario == "partition" else 0),
            "missing/duplicate fault or healing evidence")
    require(events.get("clock_valid") is True and all(event["returncode"] == 0 for event in records),
            "failed injection/heal or clock step")
    lower = faults[0]["end_elapsed_ns"] + NS
    upper = heals[0]["start_elapsed_ns"] - NS if heals else duration_s * NS
    require(0 <= lower < upper <= duration_s * NS, "empty/invalid guarded fault window")
    operations = [row for row in rows if row["type"] == "operation" and row.get("phase") == "measurement"]
    selected = [row for row in operations if row.get("start_ns") is not None
                and lower <= row["start_ns"] < upper]
    majority = [row for row in selected if row["node_id"] in ("node1", "node2")]
    minority = [row for row in selected if row["node_id"] == "node3"]
    # Calls returning after healing may have gained permissions only after rejoin.
    # A permanent crash has no such safe completion boundary, including drain.
    completion_limit = heals[0]["start_elapsed_ns"] if heals else math.inf
    success = lambda cohort: sum(row["outcome"] == "completed" and row["end_ns"] < completion_limit for row in cohort)
    require(majority and minority, "inconclusive fault: no actual invocations on each side")
    require(success(minority) == 0, "isolated/killed node confirmed a new request")
    if algorithm == "ra":
        require(success(majority) == 0, "RA confirmed an invocation without all peers")
    else:
        require(success(majority) > 0, "ABD majority has no observed progress")
    result = {"validated": True, "guard_start_ns": lower, "guard_end_ns": upper,
              "majority_sent": len(majority), "minority_sent": len(minority),
              "majority_completed": success(majority), "minority_completed": success(minority),
              "healed_completed": None, "recovery_ns": None}
    if scenario == "partition":
        refusals = [row for row in minority if row.get("http_status") == 504
                    and row["end_ns"] < upper]
        require(refusals, "inconclusive partition: no HTTP504 minority refusal")
        require(all(row.get("write_outcome") == "not_applied" for row in minority
                    if row.get("operation") == "write" and row["end_ns"] < upper),
                "isolated ABD write did not report not_applied")
        probes = events.get("minority_probes", [])
        require(any(probe.get("path") == "/cluster" and probe.get("http_status") == 200
                    for probe in probes), "partition lost client access to node3")
        result["minority_http504"] = len(refusals)
    if heals:
        heal_end = heals[0]["end_elapsed_ns"]
        resumed = [row for row in operations if row.get("start_ns") is not None
                   and heal_end + NS <= row["start_ns"] < duration_s * NS
                   and row["outcome"] == "completed" and row["end_ns"] <= duration_s * NS]
        majority_resumed = [row for row in resumed if row["node_id"] in ("node1", "node2")]
        minority_resumed = [row for row in resumed if row["node_id"] == "node3"]
        require(majority_resumed and minority_resumed,
                "inconclusive healing: new successful invocations required on both sides")
        recovered = [row for row in operations if row.get("start_ns") is not None
                     and heal_end <= row["start_ns"] < duration_s * NS
                     and row["outcome"] == "completed" and row["end_ns"] <= duration_s * NS]
        result.update(healed_completed=len(resumed),
                      healed_majority_completed=len(majority_resumed),
                      healed_minority_completed=len(minority_resumed),
                      recovery_ns=min(row["end_ns"] for row in recovered) - heal_end)
    return result


def trial_checksums(directory):
    return {path.relative_to(directory).as_posix(): sha256(path)
            for path in sorted(directory.rglob("*"))
            if path.is_file() and path != directory / "checksums.json"}


def verify_complete(directory, expected):
    require((directory / "checksums.json").is_file(), f"partial trial cannot resume: {directory}")
    checksums = read_json(directory / "checksums.json")
    require(isinstance(checksums, dict) and checksums == trial_checksums(directory),
            f"trial hash/file-set mismatch: {directory}")
    manifest = read_json(directory / "manifest.json")
    require(manifest.get("status") == "complete", f"failed trial cannot resume: {directory}")
    require(all(manifest.get(key) == value for key, value in expected.items()),
            f"trial configuration/source mismatch: {directory}")
    required = {"manifest.json", "operations.jsonl", "events.json", "compose.json",
                "membership.json", "logs.txt", "containers.json", "stats.json", "commands.json"}
    require(required <= checksums.keys(), f"missing evidence files: {directory}")
    return manifest


def trial_specs(rates):
    trials = []
    for algorithm in ("ra", "abd"):
        for repetition in range(1, 11):
            for scenario, sizes in (("normal", (3, 5, 7)), ("crash", (3,)), ("partition", (3,))):
                for nodes in sizes:
                    trials.append({"trial_id": f"{algorithm}-n{nodes}-{scenario}-r{repetition:02d}",
                                   "algorithm": algorithm, "nodes": nodes, "scenario": scenario,
                                   "repetition": repetition, "seed": 714000 + repetition,
                                   "rate": rates[algorithm], "warmup_s": 10,
                                   "duration_s": 60 if scenario == "normal" else 35,
                                   "inject_s": None if scenario == "normal" else 5,
                                   "heal_s": 20 if scenario == "partition" else None})
    random.Random(SHUFFLE_SEED).shuffle(trials)
    return trials


def compose_command(directory, project, *args):
    return ["docker", "compose", "--project-name", project,
            "--file", str(directory / "compose.json"), *args]


def ready(nodes, run_id):
    deadline = time.monotonic() + 45
    pending = set(range(1, nodes + 1))
    while pending and time.monotonic() < deadline:
        for i in list(pending):
            try:
                with urllib.request.urlopen(f"http://127.0.0.1:{18080 + i}/healthz", timeout=.5) as response:
                    health = json.loads(response.read(65536))
                if health.get("status") == "ok" and health.get("node_id") == f"node{i}" and health.get("run_id") == run_id:
                    pending.remove(i)
            except (OSError, ValueError):
                pass
        if pending:
            time.sleep(.1)
    require(not pending, f"cluster readiness failed: {sorted(pending)}")


def execute_workload(directory, manifest, container3, commands, event_document):
    args = [str(ROOT / "bin/load"), "-algorithm", manifest["algorithm"],
            "-targets", ",".join(f"http://127.0.0.1:{18080 + i}" for i in range(1, manifest["nodes"] + 1)),
            "-run-id", manifest["run_id"], "-rate", str(manifest["rate"]),
            "-warmup", f'{manifest["warmup_s"]}s', "-duration", f'{manifest["duration_s"]}s',
            "-seed", str(manifest["seed"]), "-workers", "32", "-queue", "128",
            "-wait-ms", "1000", "-client-timeout", "2s", "-hold-ms", "5",
            "-write-percent", "50", "-output", str(directory / "operations.jsonl")]
    save_json(directory / "workload-command.json", args)
    messages = queue.Queue()
    with (directory / "workload.stderr").open("x") as stderr, (directory / "lifecycle.jsonl").open("x") as stdout:
        process = subprocess.Popen(args, cwd=ROOT, stdout=subprocess.PIPE, stderr=stderr,
                                   text=True, start_new_session=True)
        def reader():
            try:
                for line in process.stdout:
                    stdout.write(line)
                    stdout.flush()
                    messages.put((line, time.time_ns(), time.monotonic_ns()))
            finally:
                messages.put((None, time.time_ns(), time.monotonic_ns()))
        thread = threading.Thread(target=reader, daemon=True)
        thread.start()
        deadline = time.monotonic() + manifest["warmup_s"] + manifest["duration_s"] + 45
        reference = None
        start_wall = None
        schedule = []
        finished = False
        try:
            while True:
                require(time.monotonic() < deadline, "bounded workload deadline exceeded")
                now_mono = time.monotonic_ns()
                if reference is not None:
                    clock_valid = abs((time.time_ns() - start_wall) - (now_mono - reference)) <= NS // 10
                    event_document["clock_valid"] &= clock_valid
                    require(clock_valid, "host wall clock stepped >100ms during measurement")
                    while schedule and now_mono >= reference + int(schedule[0][0] * NS):
                        _, kind = schedule.pop(0)
                        if kind == "crash":
                            args = compose_command(directory, manifest["project"], "kill", "-s", "SIGKILL", "node3")
                        elif kind == "partition":
                            args = ["docker", "network", "disconnect", manifest["project"] + "_protocol", container3]
                        else:
                            args = ["docker", "network", "connect", "--alias", "peer-node3",
                                    manifest["project"] + "_protocol", container3]
                        event = command(args, timeout=10, check=False)
                        commands.append(event)
                        event.update(event=kind,
                                     start_elapsed_ns=event["start_unix_ns"] - start_wall,
                                     end_elapsed_ns=event["end_unix_ns"] - start_wall)
                        event_document["events"].append(event)
                        valid = abs(event["end_unix_ns"] - event["start_unix_ns"] -
                                    event["monotonic_duration_ns"]) <= NS // 10
                        event_document["clock_valid"] &= valid
                        require(valid and event["returncode"] == 0,
                                f"fault command failed/timing invalid: {kind}: {event['stderr']}")
                        if kind == "partition":
                            for path in ("/cluster", "/healthz"):
                                probe = {"path": path, "start_unix_ns": time.time_ns()}
                                event_document.setdefault("minority_probes", []).append(probe)
                                with urllib.request.urlopen("http://127.0.0.1:18083" + path, timeout=2) as response:
                                    probe.update(http_status=response.status,
                                                 body=json.loads(response.read(65536)))
                                probe["end_unix_ns"] = time.time_ns()
                                require(probe["http_status"] == 200 and
                                        probe["body"].get("node_id") == "node3" and
                                        probe["body"].get("run_id") == manifest["run_id"],
                                        "isolated node3 client identity/reachability failed")
                        now_mono = time.monotonic_ns()
                try:
                    line, received_wall, received_mono = messages.get(timeout=.05)
                except queue.Empty:
                    continue
                if line is None:
                    break
                message = json.loads(line)
                if message.get("event") == "measurement_start":
                    require(reference is None, "duplicate measurement_start lifecycle")
                    start_wall = message["unix_ns"]
                    delay = received_wall - start_wall
                    require(0 <= delay <= NS // 10, "late lifecycle/clock step prevents reliable injection")
                    reference = received_mono - delay
                    event_document["measurement_start_unix_ns"] = start_wall
                    if manifest["scenario"] != "normal":
                        schedule.append((manifest["inject_s"], manifest["scenario"]))
                    if manifest["scenario"] == "partition":
                        schedule.append((manifest["heal_s"], "heal"))
                elif message.get("event") == "finished":
                    finished = True
                else:
                    raise CampaignError(f"unexpected lifecycle message: {message}")
            code = process.wait(timeout=5)
            require(code == 0 and reference is not None and finished and not schedule,
                    f"workload incomplete: exit={code}, pending faults={schedule}")
        finally:
            if process.poll() is None:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait(timeout=5)
            thread.join(timeout=5)
            require(not thread.is_alive(), "lifecycle reader did not finish")


def run_trial(output, campaign, spec, *, pilot=False, frozen_sha256=None):
    directory = output / ("pilot" if pilot else "trials") / spec["trial_id"]
    expected = {**spec, **{key: campaign[key] for key in BINDINGS},
                "schema": 1, "campaign_id": campaign["campaign_id"],
                "hold_ms": 5, "write_percent": 50, "wait_ms": 1000,
                "client_timeout_s": 2, "workers": 32, "queue": 128,
                "frozen_sha256": frozen_sha256}
    if directory.exists():
        return verify_complete(directory, expected)
    directory.mkdir(parents=True)
    run_id = "c" + uuid.uuid4().hex
    project = "mc714-" + uuid.uuid4().hex[:20]
    manifest = {**expected, "run_id": run_id, "project": project,
                "started_unix_ns": time.time_ns(), "campaign_path": "../../campaign.json",
                "source_archive_path": "../../source.tar.gz", "versions": campaign["versions"],
                "host_before": host_state()}
    save_json(directory / "membership.json", membership(spec["nodes"]))
    save_json(directory / "compose.json", topology(spec["nodes"], campaign["image"],
                                                    run_id, directory / "membership.json"))
    commands = []
    events = {"schema": 1, "measurement_start_unix_ns": None, "events": [], "clock_valid": True}
    failure = None
    container_ids = []
    def invoke(args, timeout=60, check=True):
        record = command(args, timeout, check=False)
        commands.append(record)
        if check:
            require(record["returncode"] == 0, f"command failed: {record['command']}: {record['stderr']}")
        return record
    try:
        invoke(compose_command(directory, project, "up", "-d", "--no-build"), timeout=90)
        ready(spec["nodes"], run_id)
        result = invoke(compose_command(directory, project, "ps", "--all", "--quiet"))
        container_ids = result["stdout"].split()
        require(len(container_ids) == spec["nodes"], "wrong number of owned containers")
        node3 = invoke(compose_command(directory, project, "ps", "--all", "--quiet", "node3"))["stdout"].strip()
        require(node3 and "\n" not in node3, "node3 container identity missing")
        execute_workload(directory, manifest, node3, commands, events)
        rows = load_operations(directory / "operations.jsonl")
        raw_start = validate_raw(rows, manifest)
        require(raw_start == events["measurement_start_unix_ns"], "lifecycle/raw measurement_start mismatch")
        manifest["invariants"] = fault_invariants(rows, events, spec["algorithm"],
                                                   spec["scenario"], spec["duration_s"])
        if pilot and spec["scenario"] == "normal":
            manifest["pilot_criteria"] = pilot_criteria(rows)
    except BaseException as exc:
        failure = exc
    finally:
        # Diagnostics must never prevent project-scoped teardown.
        if not container_ids:
            try:
                discovered = invoke(compose_command(directory, project, "ps", "--all", "--quiet"), check=False)
                container_ids = discovered["stdout"].split()
            except BaseException as exc:
                failure = failure or exc
        for filename, args in (
            ("logs.txt", compose_command(directory, project, "logs", "--no-color", "--timestamps")),
            ("containers.json", ["docker", "inspect", *container_ids] if container_ids else
             compose_command(directory, project, "ps", "--all", "--format", "json")),
            ("stats.json", ["docker", "stats", "--no-stream", "--format", "{{json .}}", *container_ids]
             if container_ids else compose_command(directory, project, "ps", "--all", "--format", "json")),
        ):
            try:
                record = invoke(args, timeout=20, check=False)
                (directory / filename).write_text(record["stdout"] + ("\n" + record["stderr"] if record["stderr"] else ""))
                require(record["returncode"] == 0,
                        f"required diagnostics failed: {filename}: {record['stderr']}")
            except BaseException as exc:
                failure = failure or exc
        try:
            teardown = invoke(compose_command(directory, project, "down", "--volumes", "--remove-orphans", "--timeout", "8"),
                              timeout=60, check=False)
            require(teardown["returncode"] == 0, f"project teardown failed: {teardown['stderr']}")
            remaining = invoke(["docker", "ps", "-aq", "--filter", f"label=com.docker.compose.project={project}"], check=False)
            volumes = invoke(["docker", "volume", "ls", "-q", "--filter", f"label=com.docker.compose.project={project}"], check=False)
            networks = invoke(["docker", "network", "ls", "-q", "--filter", f"label=com.docker.compose.project={project}"], check=False)
            require(all(record["returncode"] == 0 and not record["stdout"].strip()
                        for record in (remaining, volumes, networks)), "owned resources remain after teardown")
        except BaseException as exc:
            failure = failure or exc
        manifest.update(ended_unix_ns=time.time_ns(), host_after=host_state(),
                        status="failed" if failure else "complete")
        if failure:
            manifest["error"] = f"{type(failure).__name__}: {failure}"
        save_json(directory / "events.json", events)
        save_json(directory / "commands.json", commands)
        save_json(directory / "manifest.json", manifest)
        save_json(directory / "checksums.json", trial_checksums(directory))
    if failure:
        raise CampaignError(f"trial {spec['trial_id']} failed; raw artifacts preserved: {failure}") from failure
    print(json.dumps({"trial_id": spec["trial_id"], "status": "complete",
                      "algorithm": spec["algorithm"], "nodes": spec["nodes"],
                      "scenario": spec["scenario"], "rate": spec["rate"]}), flush=True)
    return manifest


def initialize(output):
    existing = output / "campaign.json"
    if existing.exists():
        campaign = read_json(existing)
        validate_campaign(output, campaign)
        return campaign
    require(not output.exists() or not any(output.iterdir()),
            "partial campaign initialization exists; preserve it and choose a new output")
    output.mkdir(parents=True, exist_ok=True)
    require((ROOT / "bin/load").is_file(), "parent must build host bin/load before pilot")
    versions = {"python": sys.version}
    for key, args in (("go", ["go", "version"]), ("docker", ["docker", "version"]),
                      ("compose", ["docker", "compose", "version"])):
        versions[key] = command(args)["stdout"].strip()
    digest = source_digest()
    workload_hash = sha256(ROOT / "bin/load")
    runner_hash = sha256(Path(__file__).resolve())
    with tarfile.open(output / "source.tar.gz", "x:gz") as archive:
        for path in source_files() + [ROOT / "bin/load", Path(__file__).resolve()]:
            archive.add(path, arcname=path.relative_to(ROOT).as_posix(), recursive=False)
    image_tag = "mc714-campaign:" + uuid.uuid4().hex
    build = command(["docker", "build", "--tag", image_tag, "."], timeout=900, check=False)
    save_json(output / "image-build.json", build)
    require(build["returncode"] == 0, f"image build failed: {build['stderr']}")
    inspected = command(["docker", "image", "inspect", image_tag])
    save_json(output / "image-inspect.json", json.loads(inspected["stdout"]))
    image = json.loads(inspected["stdout"])[0]["Id"]
    require(source_digest() == digest and sha256(ROOT / "bin/load") == workload_hash,
            "source/workload changed during campaign initialization")
    campaign = {"schema": 1, "campaign_id": output.name, "created_unix_ns": time.time_ns(),
                "host": host_state(), "versions": versions,
                "git_revision": command(["git", "rev-parse", "HEAD"])["stdout"].strip(),
                "git_status": command(["git", "status", "--porcelain"])["stdout"],
                "source_digest": digest, "source_archive_sha256": sha256(output / "source.tar.gz"),
                "workload_sha256": workload_hash, "runner_sha256": runner_hash,
                "image": image, "image_build_tag": image_tag, "protocol": PROTOCOL}
    save_json(output / "campaign.json", campaign)
    return campaign


def validate_campaign(output, campaign):
    require(campaign.get("schema") == 1 and campaign.get("campaign_id") == output.name,
            "campaign identity/schema mismatch")
    require(campaign.get("protocol") == PROTOCOL, "campaign protocol changed")
    require(campaign.get("source_digest") == source_digest(), "Go/Docker source changed since pilot")
    require(campaign.get("workload_sha256") == sha256(ROOT / "bin/load"), "workload binary changed since pilot")
    require(campaign.get("runner_sha256") == sha256(Path(__file__).resolve()), "runner changed since pilot")
    require(campaign.get("source_archive_sha256") == sha256(output / "source.tar.gz"), "source archive changed")
    image = command(["docker", "image", "inspect", campaign["image"]])
    require(json.loads(image["stdout"])[0]["Id"] == campaign["image"], "frozen image unavailable or changed")


def validate_frozen(output, campaign):
    frozen = read_json(output / "frozen.json")
    require(frozen.get("schema") == 1 and frozen.get("campaign_id") == campaign["campaign_id"],
            "frozen campaign identity mismatch")
    require(all(frozen.get(key) == campaign[key] for key in BINDINGS), "frozen source/image mismatch")
    rates = frozen.get("rates", {})
    require(set(rates) == {"ra", "abd"} and all(rate in PROTOCOL["pilot_rates"] for rate in rates.values()),
            "invalid frozen rates")
    require(frozen.get("protocol") == PROTOCOL and frozen.get("shuffle_seed") == SHUFFLE_SEED and
            frozen.get("trials") == trial_specs(rates), "frozen protocol/trial list changed")
    require(frozen.get("pilot_report_sha256") == sha256(output / "pilot-report.json"), "pilot report changed")
    report = read_json(output / "pilot-report.json")
    require(report.get("rates") == rates and len(report.get("candidates", [])) == 8
            and len(report.get("faults", [])) == 4, "pilot report incomplete")
    seen_candidates = set()
    qualifying = {"ra": [], "abd": []}
    for candidate in report["candidates"]:
        algorithm, rate = candidate["algorithm"], candidate["rate"]
        require(algorithm in qualifying and rate in PROTOCOL["pilot_rates"] and
                (algorithm, rate) not in seen_candidates, "duplicate/invalid pilot candidate")
        seen_candidates.add((algorithm, rate))
        require(sorted(endpoint["nodes"] for endpoint in candidate["endpoints"]) == [3, 7],
                "missing pilot endpoint")
        for endpoint in candidate["endpoints"]:
            nodes = endpoint["nodes"]
            trial_id = f"{algorithm}-n{nodes}-rate{rate}"
            require(endpoint["trial_id"] == trial_id, "pilot endpoint identity mismatch")
            directory = output / "pilot" / trial_id
            expected = {**{key: campaign[key] for key in BINDINGS},
                        "trial_id": trial_id, "algorithm": algorithm, "nodes": nodes,
                        "scenario": "normal", "repetition": 0, "seed": 714100 + rate,
                        "rate": rate, "warmup_s": 2, "duration_s": 8,
                        "inject_s": None, "heal_s": None, "frozen_sha256": None}
            manifest = verify_complete(directory, expected)
            criteria = pilot_criteria(load_operations(directory / "operations.jsonl"))
            require(manifest.get("pilot_criteria") == criteria and
                    endpoint == {"trial_id": trial_id, "nodes": nodes, **criteria},
                    "pilot qualification differs from raw evidence")
        qualified = all(endpoint["qualified"] for endpoint in candidate["endpoints"])
        require(candidate["qualified"] == qualified, "pilot qualification mismatch")
        if qualified:
            qualifying[algorithm].append(rate)
    require(all(qualifying[algorithm] and rates[algorithm] == max(qualifying[algorithm])
                for algorithm in qualifying), "frozen rate is not largest qualified candidate")
    seen_faults = set()
    for fault in report["faults"]:
        algorithm, scenario = fault["algorithm"], fault["scenario"]
        require(algorithm in rates and scenario in ("crash", "partition") and
                (algorithm, scenario) not in seen_faults, "invalid/duplicate pilot fault")
        seen_faults.add((algorithm, scenario))
        trial_id = f"{algorithm}-{scenario}"
        require(fault["trial_id"] == trial_id, "pilot fault identity mismatch")
        expected = {**{key: campaign[key] for key in BINDINGS},
                    "trial_id": trial_id, "algorithm": algorithm, "nodes": 3,
                    "scenario": scenario, "repetition": 0, "seed": 714999,
                    "rate": rates[algorithm], "warmup_s": 2, "duration_s": 12,
                    "inject_s": 3, "heal_s": 7 if scenario == "partition" else None,
                    "frozen_sha256": None}
        directory = output / "pilot" / trial_id
        manifest = verify_complete(directory, expected)
        invariants = fault_invariants(load_operations(directory / "operations.jsonl"),
                                      read_json(directory / "events.json"), algorithm, scenario, 12)
        require(fault["invariants"] == manifest.get("invariants") == invariants,
                "pilot fault validation differs from raw evidence")
    return frozen


def pilot(output, campaign):
    if (output / "frozen.json").exists():
        validate_frozen(output, campaign)
        return
    report = {"schema": 1, "candidates": [], "faults": [], "rates": {}}
    for algorithm in ("ra", "abd"):
        qualified_rates = []
        for rate in PROTOCOL["pilot_rates"]:
            endpoints = []
            for nodes in (3, 7):
                spec = {"trial_id": f"{algorithm}-n{nodes}-rate{rate}", "algorithm": algorithm,
                        "nodes": nodes, "scenario": "normal", "repetition": 0,
                        "seed": 714100 + rate, "rate": rate, "warmup_s": 2,
                        "duration_s": 8, "inject_s": None, "heal_s": None}
                manifest = run_trial(output, campaign, spec, pilot=True)
                endpoints.append({"trial_id": spec["trial_id"], "nodes": nodes,
                                  **manifest["pilot_criteria"]})
            qualified = all(endpoint["qualified"] for endpoint in endpoints)
            report["candidates"].append({"algorithm": algorithm, "rate": rate,
                                         "endpoints": endpoints, "qualified": qualified})
            if qualified:
                qualified_rates.append(rate)
        if not qualified_rates:
            # A rejected pilot is evidence too; never invent an operating rate.
            report["error"] = f"no qualifying candidate for {algorithm}"
            if not (output / "pilot-report.json").exists():
                save_json(output / "pilot-report.json", report)
            raise CampaignError(report["error"])
        report["rates"][algorithm] = max(qualified_rates)
    for algorithm in ("ra", "abd"):
        for scenario in ("crash", "partition"):
            spec = {"trial_id": f"{algorithm}-{scenario}", "algorithm": algorithm,
                    "nodes": 3, "scenario": scenario, "repetition": 0,
                    "seed": 714999, "rate": report["rates"][algorithm],
                    "warmup_s": 2, "duration_s": 12, "inject_s": 3,
                    "heal_s": 7 if scenario == "partition" else None}
            manifest = run_trial(output, campaign, spec, pilot=True)
            report["faults"].append({"trial_id": spec["trial_id"], "algorithm": algorithm,
                                     "scenario": scenario, "invariants": manifest["invariants"]})
    if (output / "pilot-report.json").exists():
        require(read_json(output / "pilot-report.json") == report, "existing pilot report differs")
    else:
        save_json(output / "pilot-report.json", report)
    frozen = {"schema": 1, "campaign_id": campaign["campaign_id"],
              **{key: campaign[key] for key in BINDINGS}, "rates": report["rates"],
              "protocol": PROTOCOL, "shuffle_seed": SHUFFLE_SEED,
              "pilot_report_sha256": sha256(output / "pilot-report.json"),
              "trials": trial_specs(report["rates"])}
    save_json(output / "frozen.json", frozen)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("pilot", "run"))
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    output = args.output.resolve()
    def interrupted(signum, frame):
        raise CampaignError(f"interrupted by signal {signum}")
    signal.signal(signal.SIGTERM, interrupted)
    # One lock across every output/project: published host ports are intentionally fixed.
    lock_path = Path(tempfile.gettempdir()) / f"mc714-campaign-{os.getuid()}.lock"
    with lock_path.open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise CampaignError("another campaign already owns the host ports") from exc
        if args.action == "pilot":
            campaign = initialize(output)
            pilot(output, campaign)
        else:
            require((output / "campaign.json").is_file(), "run requires completed pilot")
            campaign = read_json(output / "campaign.json")
            validate_campaign(output, campaign)
            frozen = validate_frozen(output, campaign)
            frozen_hash = sha256(output / "frozen.json")
            expected_ids = {spec["trial_id"] for spec in frozen["trials"]}
            if (output / "trials").exists():
                require(all(path.is_dir() and path.name in expected_ids for path in (output / "trials").iterdir()),
                        "unexpected main trial directory/file")
            for spec in frozen["trials"]:
                run_trial(output, campaign, spec, frozen_sha256=frozen_hash)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (CampaignError, OSError, ValueError, KeyError) as exc:
        print(f"campaign: {exc}", file=sys.stderr)
        raise SystemExit(1)
