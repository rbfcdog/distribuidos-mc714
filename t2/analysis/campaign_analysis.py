#!/usr/bin/env python3
"""Analyze immutable RA/ABD trials; see REPORT.md for statistical definitions."""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
import hashlib
import json
import math
from pathlib import Path
import statistics
import sys
import tarfile

NS = 1_000_000_000
GUARD = NS
CLOCK_TOLERANCE = 100_000_000
OUTCOMES = {"completed", "busy", "deadline", "unavailable", "transport_error",
            "invalid_response", "queue_full", "error"}
BINDINGS = ("campaign_id", "source_digest", "source_archive_sha256", "workload_sha256", "runner_sha256", "image")
# Exact two-sided 95% Student-t critical values; campaigns have at most ten repetitions.
T975 = (None, 12.7062047364, 4.30265272975, 3.18244630528, 2.77644510520,
        2.57058183564, 2.44691184879, 2.36462425159, 2.30600413520, 2.26215716285)


def require(condition, message):
    if not condition:
        raise ValueError(message)


def integer(value, name, minimum=0):
    require(type(value) is int and value >= minimum, f"{name}: expected integer >= {minimum}")
    return value


def number(value, name, positive=False):
    require(type(value) in (int, float) and math.isfinite(value)
            and (not positive or value > 0), f"{name}: invalid finite number")
    return value


def unique_object(pairs):
    obj = {}
    for key, value in pairs:
        require(key not in obj, f"duplicate JSON key: {key}")
        obj[key] = value
    return obj


def loads(text):
    def invalid(value):
        raise ValueError(f"nonfinite JSON value: {value}")
    return json.loads(text, object_pairs_hook=unique_object, parse_constant=invalid)


def read_json(path):
    value = loads(path.read_text(encoding="utf-8"))
    require(isinstance(value, dict), f"{path}: expected object")
    return value


def digest(path):
    with path.open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def quantile(values, probability):
    """Hyndman-Fan type 7: interpolate at (n-1)*p, including endpoints."""
    require(0 <= probability <= 1, "quantile probability outside [0,1]")
    if not values:
        return None
    ordered = sorted(values)
    index = (len(ordered) - 1) * probability
    low = math.floor(index)
    high = math.ceil(index)
    return ordered[low] + (ordered[high] - ordered[low]) * (index - low)


def percentiles(values):
    return {"n": len(values), **{f"p{p}": quantile(values, p / 100) for p in (50, 95, 99)}}


def estimate(values):
    observed = [value for value in values if value is not None]
    n = len(observed)
    require(n <= 10, "more than ten independent repetitions in an aggregate")
    mean = statistics.mean(observed) if n else None
    sd = statistics.stdev(observed) if n > 1 else None
    half = T975[n - 1] * sd / math.sqrt(n) if n > 1 else None
    return {"n": n, "missing": len(values) - n, "values": values, "mean": mean,
            "sample_sd": sd, "ci95": [mean - half, mean + half] if half is not None else None}


def validate_hashes(directory):
    checksums = read_json(directory / "checksums.json")
    files = {path.relative_to(directory).as_posix() for path in directory.rglob("*") if path.is_file()}
    require(not any(path.is_symlink() for path in directory.rglob("*")), f"{directory}: symlink evidence")
    require(set(checksums) == files - {"checksums.json"}, f"{directory}: checksum coverage mismatch")
    required = {"manifest.json", "operations.jsonl", "events.json", "compose.json", "membership.json", "logs.txt"}
    require(required <= set(checksums), f"{directory}: missing evidence files")
    for filename, expected in checksums.items():
        require(type(expected) is str and len(expected) == 64, f"invalid SHA256: {filename}")
        require(digest(directory / filename) == expected, f"{directory / filename}: SHA256 mismatch")


def validate_source(root, campaign):
    require(digest(root / "source.tar.gz") == campaign["source_archive_sha256"], "source archive SHA256 mismatch")
    with tarfile.open(root / "source.tar.gz", "r:gz") as archive:
        members = {}
        for member in archive.getmembers():
            require(not member.issym() and not member.islnk(), "source archive contains links")
            if not member.isfile():
                continue
            name = member.name.removeprefix("./")
            require(not name.startswith("/") and ".." not in Path(name).parts and name not in members,
                    "unsafe or duplicate source archive path")
            members[name] = member
        source_names = sorted(name for name in members if name in ("go.mod", "Dockerfile")
                              or name.startswith(("cmd/", "internal/")))
        require({"go.mod", "Dockerfile", "bin/load", "experiments/campaign.py"} <= set(members),
                "source archive missing bound sources/workload/runner")
        hashed = hashlib.sha256()
        for name in source_names:
            hashed.update(name.encode() + b"\0")
            hashed.update(archive.extractfile(members[name]).read())
            hashed.update(b"\0")
        require(hashed.hexdigest() == campaign["source_digest"], "archived source digest mismatch")
        workload = archive.extractfile(members["bin/load"]).read()
        require(hashlib.sha256(workload).hexdigest() == campaign["workload_sha256"], "archived workload digest mismatch")
        runner = archive.extractfile(members["experiments/campaign.py"]).read()
        require(hashlib.sha256(runner).hexdigest() == campaign["runner_sha256"], "archived runner digest mismatch")


def validate_topology(directory, manifest, metadata):
    membership = read_json(directory / "membership.json")
    members = [{"id": f"node{i}", "url": f"http://peer-node{i}:8080"}
               for i in range(1, manifest["nodes"] + 1)]
    require(membership == {"mode": "standalone", "members": members}, "noncanonical membership")
    identity = {"run_id": manifest["run_id"], "mode": "standalone", "members": members}
    identity_bytes = json.dumps(identity, separators=(",", ":")).encode()
    require(hashlib.sha256(identity_bytes).hexdigest() == metadata["cluster_id"], "membership cluster hash mismatch")
    compose = read_json(directory / "compose.json")
    require(compose["networks"] == {"client": {}, "protocol": {"internal": True}},
            "client and isolated internal protocol networks are required")
    require(set(compose["services"]) == {member["id"] for member in members}, "compose node count mismatch")
    for i, member in enumerate(members, 1):
        service = compose["services"][member["id"]]
        require(service["image"] == manifest["image"], "compose image mismatch")
        require(service["networks"] == {"client": {}, "protocol": {"aliases": [f"peer-node{i}"]}},
                "protocol aliases must exist only on protocol network")
        require(service["ports"] == [f"127.0.0.1:{18080 + i}:8080"], "unexpected client ingress")
        require(service["cpus"] == 1.0 and service["mem_limit"] == "256m" and service["pids_limit"] == 64,
                "compose resource limits differ from protocol")
        require(service["read_only"] is True and service["cap_drop"] == ["ALL"]
                and service["user"] == "65532:65532", "compose isolation differs from protocol")
        command = service["command"]
        require(command[command.index("-run-id") + 1] == manifest["run_id"]
                and command[command.index("-id") + 1] == member["id"]
                and command[command.index("-observer") + 1] == "", "compose identity or observer mismatch")


def validate_raw(path, manifest):
    metadata = None
    markers = {}
    snapshots = {}
    operations = []
    ids = set()
    active = None
    sequence = []
    expected_nodes = {f"node{i}" for i in range(1, manifest["nodes"] + 1)}
    with path.open(encoding="utf-8") as stream:
        for line_no, line in enumerate(stream, 1):
            require(line.strip(), f"{path}:{line_no}: empty JSONL line")
            row = loads(line)
            require(isinstance(row, dict), f"{path}:{line_no}: expected object")
            kind = row.get("type")
            if kind == "metadata":
                require(metadata is None and not sequence, "metadata must be unique first row")
                require(type(row["schema"]) is int and row["schema"] == 1, "incompatible raw schema")
                for key in ("algorithm", "nodes", "run_id", "seed", "rate", "workers", "queue", "wait_ms", "hold_ms", "write_percent"):
                    require(row[key] == manifest[key], f"raw metadata mismatch: {key}")
                for key in ("nodes", "workers", "queue", "wait_ms", "hold_ms", "write_percent",
                            "warmup_ns", "duration_ns", "client_timeout_ns"):
                    integer(row[key], f"metadata {key}")
                integer(row["seed"], "metadata seed", -(2**63))
                for key, field in (("warmup_s", "warmup_ns"), ("duration_s", "duration_ns"), ("client_timeout_s", "client_timeout_ns")):
                    require(row[field] == round(manifest[key] * NS), f"raw metadata mismatch: {field}")
                require(type(row["cluster_id"]) is str and row["cluster_id"], "missing cluster identity")
                require(row["targets"] == [f"http://127.0.0.1:{18080+i}" for i in range(1, manifest["nodes"] + 1)],
                        "noncanonical client targets")
                metadata = row
            elif kind == "phase":
                require(metadata is not None, "phase before metadata")
                phase, event = row["phase"], row["event"]
                require(phase in ("warmup", "measurement") and event in ("start", "end"), "unknown phase marker")
                require((phase, event) not in markers, "duplicate phase marker")
                integer(row["unix_ns"], "phase unix_ns", 1)
                if event == "start":
                    require(active is None, "overlapping phases")
                    active = phase
                else:
                    require(active == phase, "phase end without matching start")
                    active = None
                    elapsed = integer(row["elapsed_ns"], "phase elapsed_ns")
                    wall = integer(row["wall_elapsed_ns"], "phase wall_elapsed_ns")
                    require(elapsed >= metadata[f"{phase if phase == 'warmup' else 'duration'}_ns"], "phase ended before offered window")
                    require(wall == row["unix_ns"] - markers[(phase, "start")]["unix_ns"], "inconsistent wall phase time")
                    require(abs(wall - elapsed) <= CLOCK_TOLERANCE, "wall-clock step detected")
                markers[(phase, event)] = row
                sequence.append(f"{phase}:{event}")
            elif kind == "operation":
                require(metadata is not None and active == row["phase"], "operation outside active phase")
                integer(row["id"], "operation id", 1)
                require(row["id"] not in ids, "duplicate operation id")
                ids.add(row["id"])
                require(row["node_id"] in expected_nodes, "unknown operation node")
                require(row["operation"] in ({"execute"} if manifest["algorithm"] == "ra" else {"read", "write"}), "algorithm/operation mismatch")
                require(row["outcome"] in OUTCOMES, "unknown outcome")
                scheduled = integer(row["scheduled_ns"], "scheduled_ns")
                end = integer(row["end_ns"], "end_ns")
                lag = integer(row["generator_lag_ns"], "generator_lag_ns")
                require(scheduled < metadata["warmup_ns" if active == "warmup" else "duration_ns"], "arrival outside offered window")
                require(end >= scheduled + lag, "response before arrival")
                status = integer(row["http_status"], "http_status")
                require(status == 0 or 100 <= status <= 599, "invalid HTTP status")
                if row["outcome"] == "queue_full":
                    require(row["start_ns"] is None and status == 0, "queue rejection sent to server")
                else:
                    start = integer(row["start_ns"], "start_ns")
                    require(scheduled + lag <= start <= end, "invalid request time ordering")
                if row["outcome"] == "completed":
                    require(200 <= status < 300, "completion without successful HTTP status")
                    for field in (("wait_ns", "held_ns") if manifest["algorithm"] == "ra" else ("duration_ns",)):
                        integer(row[field], field)
                    if manifest["algorithm"] == "abd":
                        integer(row["value"], "register value", -(2**63))
                if row["operation"] == "write":
                    require(type(row.get("value")) is int and row["value"] == row["id"],
                            "attempted write must preserve its unique operation id value")
                for field in ("wait_ns", "held_ns", "duration_ns"):
                    if field in row:
                        integer(row[field], field)
                if "write_outcome" in row:
                    require(row["operation"] == "write" and row["outcome"] != "completed",
                            "write uncertainty attached to incompatible operation")
                if row["operation"] == "write" and row["outcome"] != "completed":
                    require(row.get("write_outcome") in ("not_applied", "indeterminate"), "failed write missing application uncertainty")
                operations.append(row)
            elif kind == "snapshot":
                require(metadata is not None and active is None, "snapshot overlaps active phase")
                stage = row["stage"]
                require(stage in ("baseline", "final") and stage not in snapshots, "invalid snapshot stage")
                require(isinstance(row["nodes"], list), "invalid snapshot nodes")
                by_node = {}
                for node in row["nodes"]:
                    node_id = node["node_id"]
                    require(node_id in expected_nodes and node_id not in by_node, "duplicate/unknown snapshot node")
                    require(("state" in node) != ("error" in node), "snapshot needs exactly state or error")
                    if "state" in node:
                        state = node["state"]
                        require(isinstance(state, dict), "invalid snapshot state")
                        for field in ("logical_messages", "transport_attempts"):
                            integer(state[field], field)
                        for field, expected in (("node_id", node_id), ("run_id", metadata["run_id"]), ("cluster_id", metadata["cluster_id"])):
                            require(field not in state or state[field] == expected, f"snapshot identity mismatch: {field}")
                    else:
                        require(type(node["error"]) is str and node["error"], "empty snapshot error")
                    by_node[node_id] = node
                require(set(by_node) == expected_nodes, "snapshot must account for every node")
                snapshots[stage] = by_node
                sequence.append(stage)
            else:
                raise ValueError(f"{path}:{line_no}: unknown record type {kind!r}")
    require(sequence == ["warmup:start", "warmup:end", "baseline", "measurement:start", "measurement:end", "final"], "missing/out-of-order phases or snapshots")
    require(ids == set(range(1, len(ids) + 1)), "operation id gap: missing raw arrivals")
    require(markers[("measurement", "start")]["unix_ns"] >= markers[("warmup", "end")]["unix_ns"], "measurement overlaps warmup")
    for row in operations:
        require(row["end_ns"] <= markers[(row["phase"], "end")]["elapsed_ns"], "operation after phase drain")
    require(all("state" in node for node in snapshots["baseline"].values()), "incomplete counter baseline")
    coverage = {"baseline": manifest["nodes"], "final": 0, "expected": manifest["nodes"], "errors": {}}
    deltas = {"logical_messages": 0, "transport_attempts": 0}
    partial_deltas = {}
    for node_id, final in snapshots["final"].items():
        if "state" not in final:
            coverage["errors"][node_id] = final["error"]
            continue
        coverage["final"] += 1
        partial_deltas[node_id] = {}
        for field in deltas:
            delta = final["state"][field] - snapshots["baseline"][node_id]["state"][field]
            require(delta >= 0, f"counter decreased: {node_id}/{field}")
            deltas[field] += delta
            partial_deltas[node_id][field] = delta
    if coverage["final"] != manifest["nodes"]:
        deltas = dict.fromkeys(deltas)
    return {"metadata": metadata, "markers": markers, "operations": operations,
            "counters": {"coverage": coverage, "cluster_delta": deltas, "observed_node_deltas": partial_deltas}}


def operation_metrics(rows, duration_ns, lower_ns=0):
    """Rows form an invocation cohort; throughput counts responses within this interval."""
    successes = [row for row in rows if row["outcome"] == "completed"]
    sent = [row for row in rows if row["start_ns"] is not None]
    outcomes = Counter(row["outcome"] for row in rows)
    window_success = sum(lower_ns <= row["end_ns"] < duration_ns for row in successes)
    span = (duration_ns - lower_ns) / NS
    require(span > 0, "empty metric interval")
    latency = lambda field: percentiles([row[field] / 1e6 for row in successes if field in row])
    return {"counts": {"offered": len(rows), "sent": len(sent), "completed": len(successes),
                       "completed_in_window": window_success,
                       "unsuccessful": len(rows) - len(successes),
                       "rejected": outcomes["busy"] + outcomes["queue_full"],
                       "deadline": outcomes["deadline"], "transport_error": outcomes["transport_error"],
                       "queue_full": outcomes["queue_full"],
                       "unknown_writes": sum(row.get("write_outcome") == "indeterminate" for row in rows),
                       "censored_at_window": sum(row["end_ns"] > duration_ns for row in rows),
                       **{f"outcome_{outcome}": outcomes[outcome] for outcome in sorted(OUTCOMES)}},
            "interval_seconds": span, "offered_rate": len(rows) / span,
            "throughput": window_success / span,
            "success_fraction": len(successes) / len(rows) if rows else None,
            "client_latency_ms": percentiles([(row["end_ns"] - row["start_ns"]) / 1e6 for row in successes]),
            "failed_client_latency_ms": percentiles([(row["end_ns"] - row["start_ns"]) / 1e6 for row in sent if row["outcome"] != "completed"]),
            "deadline_client_latency_ms": percentiles([(row["end_ns"] - row["start_ns"]) / 1e6 for row in sent if row["outcome"] == "deadline"]),
            "scheduled_response_ms": percentiles([(row["end_ns"] - row["scheduled_ns"]) / 1e6 for row in successes]),
            "successful_start_lag_ms": percentiles([(row["start_ns"] - row["scheduled_ns"]) / 1e6 for row in successes]),
            "sent_start_lag_ms": percentiles([(row["start_ns"] - row["scheduled_ns"]) / 1e6 for row in sent]),
            "generator_lag_ms": percentiles([row["generator_lag_ns"] / 1e6 for row in rows]),
            "ra_wait_ms": latency("wait_ns"), "server_duration_ms": latency("duration_ns")}


def validate_events(events, raw, manifest):
    require(type(events["schema"]) is int and events["schema"] == 1
            and events["clock_valid"] is True, "incompatible events or invalid clock")
    start = raw["markers"][("measurement", "start")]["unix_ns"]
    require(events["measurement_start_unix_ns"] == start, "fault measurement epoch mismatch")
    expected = [] if manifest["scenario"] == "normal" else [manifest["scenario"]] + (["heal"] if manifest["scenario"] == "partition" else [])
    require([event["event"] for event in events["events"]] == expected, "missing/unexpected fault events")
    previous = 0
    duration = raw["metadata"]["duration_ns"]
    for event in events["events"]:
        require(event["returncode"] == 0 and isinstance(event["command"], list) and event["command"], "failed fault command")
        a, b = integer(event["start_elapsed_ns"], "event start"), integer(event["end_elapsed_ns"], "event end")
        require(previous <= a <= b < duration, "fault event outside ordered measurement")
        require(event["start_unix_ns"] - start == a and event["end_unix_ns"] - start == b, "fault elapsed timestamp mismatch")
        monotonic = integer(event["monotonic_duration_ns"], "fault monotonic duration")
        require(abs(b - a - monotonic) <= CLOCK_TOLERANCE, "fault command wall-clock step")
        previous = b
    if manifest["scenario"] == "partition":
        probes = events["minority_probes"]
        require({probe["path"] for probe in probes} == {"/cluster", "/healthz"}, "missing minority access probes")
        fault, heal = events["events"]
        for probe in probes:
            require(probe["http_status"] == 200, "minority client access unavailable")
            require(fault["end_unix_ns"] <= probe["start_unix_ns"] <= probe["end_unix_ns"] <= heal["start_unix_ns"],
                    "minority access probe outside partition")
            body = probe["body"]
            require(body["node_id"] == "node3" and body["run_id"] == manifest["run_id"], "minority probe identity mismatch")
            if probe["path"] == "/cluster":
                require(body["cluster_id"] == raw["metadata"]["cluster_id"] and body["mode"] == "standalone",
                        "minority cluster probe mismatch")
    return events["events"]


def fault_analysis(rows, duration, events, algorithm, scenario):
    if not events:
        return None
    fault = events[0]
    heal = events[1] if len(events) > 1 else None
    intervals = [("pre", 0, fault["start_elapsed_ns"] - GUARD),
                 ("fault", fault["end_elapsed_ns"] + GUARD,
                  heal["start_elapsed_ns"] - GUARD if heal else duration)]
    if heal:
        intervals.append(("healed", heal["end_elapsed_ns"] + GUARD, duration))
    result = {"intervals": {}, "guards_seconds": 1, "observed_post_heal_completion_s": None,
              "recovery_censored": True, "recovery_definition": "first completion of a new invocation starting at or after heal end, minus heal end; includes offered-load interarrival, not intrinsic detection"}
    for name, lo, hi in intervals:
        require(0 <= lo < hi <= duration, "fault leaves no guarded observation interval")
        invocation = lambda row: row["start_ns"] if row["start_ns"] is not None else row["scheduled_ns"]
        cohort = [row for row in rows if lo <= invocation(row) < hi]
        groups = {}
        for group, nodes in (("all", {"node1", "node2", "node3"}), ("majority", {"node1", "node2"}), ("minority", {"node3"})):
            subset = [row for row in cohort if row["node_id"] in nodes]
            groups[group] = {kind: operation_metrics([row for row in subset if kind == "all" or row["operation"] == kind], hi, lo)
                             for kind in (("all", "execute") if algorithm == "ra" else ("all", "read", "write"))}
        result["intervals"][name] = {"start_ns": lo, "end_ns": hi, "groups": groups}
        if name == "fault":
            majority = groups["majority"]["all"]["counts"]
            minority = groups["minority"]["all"]["counts"]
            require(majority["sent"] > 0, "inconclusive fault: no majority invocation")
            require(minority["sent"] > 0, "inconclusive fault: no minority invocation")
            completion_limit = heal["start_elapsed_ns"] if heal else math.inf
            confirmed = lambda nodes: sum(row["outcome"] == "completed" and row["end_ns"] < completion_limit
                                           for row in cohort if row["node_id"] in nodes)
            majority_confirmed = confirmed({"node1", "node2"})
            minority_confirmed = confirmed({"node3"})
            result["fault_confirmation_counts"] = {"majority": majority_confirmed, "minority": minority_confirmed,
                                                   "completion_limit_ns": heal["start_elapsed_ns"] if heal else None}
            if algorithm == "ra":
                require(majority_confirmed == 0, "RA completed while peer unavailable")
                require(minority_confirmed == 0, "isolated RA node completed")
            else:
                require(majority_confirmed > 0, "ABD majority made no observed progress")
                require(minority_confirmed == 0, "ABD minority confirmed isolated request")
            if scenario == "partition":
                refusals = [row for row in cohort if row["node_id"] == "node3"
                            and row["http_status"] == 504 and row["outcome"] == "deadline"
                            and row["end_ns"] < hi]
                require(refusals, "partition lacks minority HTTP504 refusal evidence")
                require(all(row.get("write_outcome") == "not_applied" for row in cohort
                            if row["node_id"] == "node3" and row["operation"] == "write" and row["end_ns"] < hi),
                        "isolated failed write not confirmed not_applied")
        if name == "healed":
            require(groups["majority"]["all"]["counts"]["completed_in_window"] > 0
                    and groups["minority"]["all"]["counts"]["completed_in_window"] > 0,
                    "healing did not restore both majority and minority")
    if heal:
        newly_completed = [row for row in rows if row["outcome"] == "completed"
                           and row["start_ns"] >= heal["end_elapsed_ns"] and row["end_ns"] < duration]
        require(newly_completed, "no new post-heal completion")
        result["observed_post_heal_completion_s"] = (min(row["end_ns"] for row in newly_completed)
                                                      - heal["end_elapsed_ns"]) / NS
        result["recovery_censored"] = False
    result["operation_intervals"] = []
    for row in rows:
        instant = row["start_ns"] if row["start_ns"] is not None else row["scheduled_ns"]
        phase = next((name for name, lo, hi in intervals if lo <= instant < hi), "transition_guard")
        result["operation_intervals"].append({"id": row["id"], "interval": phase,
                                               "group": "minority" if row["node_id"] == "node3" else "majority"})
    if algorithm == "ra" and scenario == "crash":
        result["recovery_definition"] = "permanent crash: no finite RA recovery; observation right-censored"
    # Completion-time bins are calculated separately per trial, never pooled as independent repetitions.
    result["timeline"] = []
    for second in range(math.ceil(duration / NS)):
        lo, hi = second * NS, min((second + 1) * NS, duration)
        by_group = {}
        for group, nodes in (("majority", {"node1", "node2"}), ("minority", {"node3"})):
            subset = [row for row in rows if row["node_id"] in nodes and lo <= row["end_ns"] < hi]
            by_group[group] = {"completed_rate": sum(row["outcome"] == "completed" for row in subset) / ((hi - lo) / NS),
                               "failed_rate": sum(row["outcome"] != "completed" for row in subset) / ((hi - lo) / NS),
                               "deadline_rate": sum(row["outcome"] == "deadline" for row in subset) / ((hi - lo) / NS),
                               "transport_error_rate": sum(row["outcome"] == "transport_error" for row in subset) / ((hi - lo) / NS)}
        result["timeline"].append({"second": second, **by_group})
    return result


def flatten_metrics(value, prefix=""):
    result = {}
    for key, item in value.items():
        name = f"{prefix}.{key}" if prefix else key
        if isinstance(item, dict):
            result.update(flatten_metrics(item, name))
        elif item is None or type(item) in (int, float):
            result[name] = item
    return result


def trial_key(manifest):
    return (manifest["algorithm"], manifest["nodes"], manifest["scenario"], manifest["repetition"])


def expected_keys():
    return {(algorithm, nodes, scenario, repetition)
            for algorithm in ("ra", "abd") for scenario in ("normal", "crash", "partition")
            for nodes in ((3, 5, 7) if scenario == "normal" else (3,)) for repetition in range(1, 11)}


def validate_manifest(manifest, campaign, frozen, frozen_digest, partial):
    require(type(manifest["schema"]) is int and manifest["schema"] == 1
            and manifest["status"] == "complete", "incomplete/incompatible trial manifest")
    require(manifest["algorithm"] in ("ra", "abd") and manifest["nodes"] in (3, 5, 7)
            and manifest["scenario"] in ("normal", "crash", "partition"), "invalid trial configuration")
    integer(manifest["repetition"], "repetition", 1 if frozen is not None else 0)
    for key in BINDINGS:
        require(manifest[key] == campaign[key], f"trial/campaign binding mismatch: {key}")
    require(type(manifest["run_id"]) is str and manifest["run_id"], "missing run identity")
    number(manifest["rate"], "rate", True)
    for field in ("warmup_s", "duration_s", "client_timeout_s"):
        number(manifest[field], field, True)
    if frozen is not None:
        require(manifest["frozen_sha256"] == frozen_digest, "trial frozen protocol hash mismatch")
        require(trial_key(manifest) in expected_keys(), "trial outside frozen experiment design")
        require(manifest["rate"] == frozen["rates"][manifest["algorithm"]], "rate differs from frozen pilot selection")
        constants = {"warmup_s": 10, "duration_s": 60 if manifest["scenario"] == "normal" else 35,
                     "hold_ms": 5, "write_percent": 50, "wait_ms": 1000,
                     "client_timeout_s": 2, "workers": 32, "queue": 128}
        for field, value in constants.items():
            require(manifest[field] == value, f"not frozen primary protocol: {field}")
        require(manifest.get("inject_s") == (None if manifest["scenario"] == "normal" else 5)
                and manifest.get("heal_s") == (20 if manifest["scenario"] == "partition" else None),
                "fault schedule differs from frozen protocol")
        specs = [spec for spec in frozen["trials"] if trial_key(spec) == trial_key(manifest)]
        require(len(specs) == 1, "frozen trial specification absent/duplicated")
        for field in ("trial_id", "algorithm", "nodes", "scenario", "repetition", "seed", "rate", "warmup_s", "duration_s", "inject_s", "heal_s"):
            require(manifest.get(field) == specs[0].get(field), f"manifest/frozen specification mismatch: {field}")
    else:
        require(partial, "frozen.json is required")


def analyze_campaign(root, allow_partial=False):
    campaign = read_json(root / "campaign.json")
    require(type(campaign["schema"]) is int and campaign["schema"] == 1, "incompatible campaign schema")
    for field in BINDINGS:
        require(type(campaign[field]) is str and campaign[field], f"missing campaign binding: {field}")
    require(campaign["image"].startswith("sha256:") and len(campaign["image"]) == 71
            and all(character in "0123456789abcdef" for character in campaign["image"][7:]),
            "campaign image is not immutable Docker image ID")
    require(isinstance(campaign["host"], dict) and isinstance(campaign["versions"], dict), "missing host/version metadata")
    validate_source(root, campaign)
    frozen = read_json(root / "frozen.json") if (root / "frozen.json").exists() else None
    frozen_digest = digest(root / "frozen.json") if frozen is not None else None
    if frozen is not None:
        require(type(frozen["schema"]) is int and frozen["schema"] == 1, "incompatible frozen schema")
        for key in BINDINGS:
            require(frozen[key] == campaign[key], f"frozen/campaign binding mismatch: {key}")
        keys = [trial_key(spec) for spec in frozen["trials"]]
        require(len(keys) == 100 and set(keys) == expected_keys(), "frozen protocol must enumerate exactly 100 unique trials")
        require(len({spec["trial_id"] for spec in frozen["trials"]}) == 100, "duplicate frozen trial ids")
        require(all(type(spec["repetition"]) is int and spec["seed"] == 714000 + spec["repetition"]
                    for spec in frozen["trials"]), "incompatible frozen repetition seeds")
        protocol = frozen["protocol"]
        constants = {"nodes": [3, 5, 7], "repetitions": 10, "warmup_s": 10,
                     "normal_duration_s": 60, "fault_duration_s": 35, "inject_s": 5,
                     "heal_s": 20, "hold_ms": 5, "write_percent": 50, "wait_ms": 1000,
                     "client_timeout_s": 2, "workers": 32, "queue": 128, "guard_s": 1,
                     "topology": "client-bridge+internal-protocol-peer-aliases"}
        require(all(protocol.get(key) == value for key, value in constants.items()), "incompatible frozen protocol")
        require(set(frozen["rates"]) == {"ra", "abd"}
                and all(value in (2, 5, 10, 20) for value in frozen["rates"].values()), "invalid frozen rates")
        require(frozen["pilot_report_sha256"] == digest(root / "pilot-report.json"), "pilot report hash mismatch")
        require(read_json(root / "pilot-report.json")["rates"] == frozen["rates"], "pilot/frozen rates mismatch")
    require(frozen is not None or allow_partial, "missing frozen protocol; partial pilot requires --allow-partial")
    main_root = root / "trials"
    pilot_mode = frozen is None or (allow_partial and (not main_root.is_dir() or not any(main_root.iterdir())))
    trial_root = root / "pilot" if pilot_mode else main_root
    require(trial_root.is_dir(), f"missing trial directory: {trial_root}")
    require(all(path.is_dir() and not path.is_symlink() for path in trial_root.iterdir()), "unexpected trial root entry")
    directories = sorted(trial_root.iterdir())
    require(directories, "no trials found")
    runs, seen, run_ids = [], set(), set()
    for directory in directories:
        validate_hashes(directory)
        manifest = read_json(directory / "manifest.json")
        validate_manifest(manifest, campaign, None if pilot_mode else frozen, frozen_digest, allow_partial)
        key = trial_key(manifest)
        # Pilot may repeat a configuration at distinct candidate rates.
        identity = (key, manifest["rate"]) if pilot_mode else key
        require(identity not in seen, "duplicate trial tuple")
        seen.add(identity)
        require(manifest["run_id"] not in run_ids, "duplicate run_id")
        run_ids.add(manifest["run_id"])
        raw = validate_raw(directory / "operations.jsonl", manifest)
        validate_topology(directory, manifest, raw["metadata"])
        events = validate_events(read_json(directory / "events.json"), raw, manifest)
        rows = [row for row in raw["operations"] if row["phase"] == "measurement"]
        duration = raw["metadata"]["duration_ns"]
        metrics = {kind: operation_metrics([row for row in rows if kind == "all" or row["operation"] == kind], duration)
                   for kind in (("all", "execute") if manifest["algorithm"] == "ra" else ("all", "read", "write"))}
        completed = metrics["all"]["counts"]["completed"]
        counters = raw["counters"]
        counters["per_completed_operation"] = {field: value / completed if value is not None and completed else None
                                               for field, value in counters["cluster_delta"].items()}
        runs.append({"trial_id": manifest.get("trial_id", directory.name), "manifest": manifest,
                     "metrics": metrics, "counters": counters,
                     "fault": fault_analysis(rows, duration, events, manifest["algorithm"], manifest["scenario"]),
                     "warmup_operations_excluded": len(raw["operations"]) - len(rows),
                     "measurement_elapsed_seconds": raw["markers"][("measurement", "end")]["elapsed_ns"] / NS,
                     "measurement_drain_seconds": (raw["markers"][("measurement", "end")]["elapsed_ns"] - duration) / NS,
                     "fault_events": events})
    complete = frozen is not None and seen == expected_keys()
    require(complete or allow_partial, f"INCOMPLETE: expected 100 frozen trials; found {len(runs)}; missing {sorted(expected_keys() - seen)}")
    aggregates = []
    grouped = defaultdict(list)
    for run in runs:
        manifest = run["manifest"]
        # Pilot candidates must not masquerade as independent repetitions of one rate.
        grouped[(manifest["algorithm"], manifest["nodes"], manifest["scenario"], manifest["rate"])].append(run)
    for (algorithm, nodes, scenario, rate), group in sorted(grouped.items()):
        flattened = [flatten_metrics({"metrics": run["metrics"], "counters": {"cluster_delta": run["counters"]["cluster_delta"], "per_completed_operation": run["counters"]["per_completed_operation"]},
                                      "fault": {"intervals": run["fault"]["intervals"], "observed_post_heal_completion_s": run["fault"]["observed_post_heal_completion_s"]} if run["fault"] else {}}) for run in group]
        names = set().union(*(row.keys() for row in flattened))
        aggregate = {"algorithm": algorithm, "nodes": nodes, "scenario": scenario, "rate": rate,
                     "repetitions": len(group), "trial_ids": [run["trial_id"] for run in group],
                     "statistics": {name: estimate([row.get(name) for row in flattened]) for name in sorted(names)}}
        if scenario != "normal":
            aggregate["timeline"] = []
            lengths = {len(run["fault"]["timeline"]) for run in group}
            require(len(lengths) == 1, "incompatible fault timeline lengths")
            for second in range(next(iter(lengths))):
                aggregate["timeline"].append({"second": second, **{f"{side}_{metric}": estimate([run["fault"]["timeline"][second][side][metric] for run in group])
                    for side in ("majority", "minority") for metric in ("completed_rate", "failed_rate", "deadline_rate", "transport_error_rate")}})
        aggregates.append(aggregate)
    return {"schema": 1, "complete": complete, "status": "COMPLETE" if complete else "INCOMPLETE — PILOT/SMOKE ONLY",
            "expected_trials": 100, "observed_trials": len(runs), "campaign": campaign,
            "frozen_sha256": frozen_digest, "percentile_definition": "Hyndman-Fan type 7; completion timings conditional on success; failed/deadline timings use separate sent cohorts; generator/all-sent lag retains all applicable arrivals",
            "confidence_definition": "95% Student-t across per-repetition metrics; sample standard deviation; no request pooling; n<2 => undefined CI",
            "analysis": {"script_sha256": digest(Path(__file__)), "python": sys.version,
                         "dependency_lock_sha256": digest(Path(__file__).with_name("uv.lock"))},
            "runs": runs, "aggregates": aggregates}


def display(value):
    return "—" if value is None else f"{value:.3f}"


def report(summary):
    lines = ["# Campanha experimental RA/ABD", "", f"**{summary['status']}** — {summary['observed_trials']}/100 repetições.", "",
             "## Método e integridade", "",
             "Somente ensaios completos com SHA-256 de todos os arquivos de evidência e identidade/fonte/imagem compatíveis. A campanha final exige exatamente os 100 ensaios do protocolo congelado; piloto é excluído do resultado final e só pode ser analisado explicitamente como incompleto. O arquivo-fonte e o binário arquivados são revalidados. Aquecimento é drenado e excluído. Falhas de integridade, relógio (>100 ms), contadores decrescentes ou intervalos inconclusivos abortam a análise.", "",
             "A topologia separa rede client (bridge, portas locais preservadas) e protocol (interna); aliases peer-nodeN existem apenas na rede de protocolo e são os únicos endereços canônicos na associação. A partição desconecta somente protocol, mantendo node3 acessível ao gerador: recusa minoritária exige resposta HTTP504, não apenas falha de transporte. Limite: os pares são confiáveis e seguem os endereços configurados; não modela adversário usando caminhos alternativos.", "",
             "Percentis interpolam a posição (n−1)p na amostra ordenada (tipo 7). Latências e espera RA são **condicionais ao sucesso**, inclusive respostas após a janela; n está no JSON. A vazão conta apenas respostas bem-sucedidas dentro da janela de oferta. Censura de fronteira (fim após a janela) é distinta de erro, e todas as respostas são preservadas após drenagem. Rejeições contam somente busy e fila local cheia; deadlines, transporte e demais falhas são desfechos separados. Escritas indeterminadas não são consideradas não aplicadas.", "",
             "Cada ponto é a média de métricas calculadas por repetição independente; desvio-padrão amostral e IC95% Student-t (9 gl: 2,262157) usam repetições, não pedidos agrupados. O IC t pressupõe métricas aproximadamente normais entre repetições; dez repetições não garantem essa aproximação para caudas pesadas. IC ausente com n<2; métricas sem amostra = null/—, nunca zero artificial. A semente pareada entre N não cria repetições extras. Lag do gerador usa todas as ofertas; start−scheduled inclui também fila de clientes.", "",
             f"Host efetivamente registrado: `{json.dumps(summary['campaign']['host'], ensure_ascii=False, sort_keys=True)}`.", "",
             "## Ensaios normais", "",
             "Médias entre repetições; IC95% de p99 exibido na tabela, demais ICs e desvios-padrão amostrais no JSON. Espera e latências em ms.", "",
             "| Algoritmo | N | Operação | n rep. | Ofertas/s | Vazão/s | Sucesso | p50 cliente | p95 cliente | p99 cliente [IC95%] | p95 espera RA |", "|---|---:|---|---:|---:|---:|---:|---:|---:|---|---:|"]
    def stat(group, name):
        return group["statistics"].get(name, {"mean": None, "ci95": None, "n": 0})
    for group in summary["aggregates"]:
        if group["scenario"] != "normal":
            continue
        for kind in (("execute",) if group["algorithm"] == "ra" else ("all", "read", "write")):
            prefix = f"metrics.{kind}."
            p99 = stat(group, prefix + "client_latency_ms.p99")
            ci = p99["ci95"]
            p99_text = display(p99["mean"]) + (f" [{display(ci[0])}; {display(ci[1])}]" if ci else " [—]")
            fields = [display(stat(group, prefix + name)["mean"]) for name in ("offered_rate", "throughput", "success_fraction", "client_latency_ms.p50", "client_latency_ms.p95")]
            lines.append(f"| {group['algorithm'].upper()} | {group['nodes']} | {kind} | {group['repetitions']} | " + " | ".join(fields) + f" | {p99_text} | {display(stat(group, prefix + 'ra_wait_ms.p95')['mean'])} |")
    lines += ["", "### Populações, falhas e geração de carga", "",
              "Contagens médias por repetição (não somas de pedidos independentes). Lag p99 em ms inclui todas as ofertas.", "",
              "| Algoritmo | N | Cenário | Ofertadas | Enviadas | Concluídas | Rejeitadas | Deadlines | Transporte | Escritas incertas | Censuradas | p99 lag gerador |",
              "|---|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|"]
    for group in summary["aggregates"]:
        fields = [display(stat(group, "metrics.all." + name)["mean"]) for name in
                  ("counts.offered", "counts.sent", "counts.completed", "counts.rejected", "counts.deadline",
                   "counts.transport_error", "counts.unknown_writes", "counts.censored_at_window", "generator_lag_ms.p99")]
        lines.append(f"| {group['algorithm'].upper()} | {group['nodes']} | {group['scenario']} | " + " | ".join(fields) + " |")
    lines += ["", "## Contadores de comunicação", "", "| Algoritmo | N | Cenário | Frames lógicos/conclusão | Tentativas HTTP/conclusão | n cobertura completa |", "|---|---:|---|---:|---:|---:|"]
    for group in summary["aggregates"]:
        logical = stat(group, "counters.per_completed_operation.logical_messages")
        transport = stat(group, "counters.per_completed_operation.transport_attempts")
        lines.append(f"| {group['algorithm'].upper()} | {group['nodes']} | {group['scenario']} | {display(logical['mean'])} | {display(transport['mean'])} | {logical['n']} |")
    lines += ["", "Frames são envelopes gerados, não pacotes de rede; tentativas contam RPCs HTTP de saída. Deltas incluem trabalho malsucedido e fanout cancelado/repetido. Divisor = conclusões de toda a coorte medida, inclusive drenagem. Sem snapshot final de qualquer nó, total do cluster e razão são indisponíveis; deltas parciais por nó permanecem no JSON. RA ideal sem falhas gera 2(N−1) frames por aquisição; ABD utiliza duas fases de quórum, mas envio/repetição/cancelamento impede identificar frames com apenas respostas necessárias.", "", "## Falhas e recuperação observada", "", "Guardas de 1 s excluem transições: pré termina antes do início da injeção−1 s; falha começa após aplicação+1 s e termina antes da cura−1 s; curado começa após fim da cura+1 s. Coortes usam instante de invocação (oferta em rejeições locais); vazão requer também conclusão dentro do intervalo. Pendências além da fronteira são censuradas. Maioria=node1/2; minoria=node3. Separadamente, recuperação observada é a primeira conclusão de invocação nova com início a partir do fim da cura, menos fim da cura, **sem** acrescentar a guarda de 1 s usada na prova de progresso. Inclui espera pela próxima chegada e conclusão sob carga, **não** tempo intrínseco de detecção/reintegração.", "", "| Algoritmo | Cenário | Fase | Grupo | Ofertas/s | Vazão/s | Deadlines | Censuradas | p99 cliente ms |", "|---|---|---|---|---:|---:|---:|---:|---:|"]
    recovery_lines = []
    for group in summary["aggregates"]:
        if group["scenario"] == "normal":
            continue
        for phase in ("pre", "fault", "healed"):
            for side in ("majority", "minority"):
                prefix = f"fault.intervals.{phase}.groups.{side}.all."
                if prefix + "throughput" not in group["statistics"]:
                    continue
                fields = [display(stat(group, prefix + name)["mean"]) for name in ("offered_rate", "throughput", "counts.deadline", "counts.censored_at_window", "client_latency_ms.p99")]
                lines.append(f"| {group['algorithm'].upper()} | {group['scenario']} | {phase} | {side} | " + " | ".join(fields) + " |")
        recovery = stat(group, "fault.observed_post_heal_completion_s")
        if group["scenario"] == "partition":
            ci = recovery["ci95"]
            uncertainty = f" IC95% [{display(ci[0])}; {display(ci[1])}] s" if ci else ""
            recovery_lines.append(f"- {group['algorithm'].upper()} / partição: conclusão pós-cura média {display(recovery['mean'])} s;{uncertainty} (n={recovery['n']}).")
        elif group["algorithm"] == "ra":
            recovery_lines.append("- RA / crash permanente: novas aquisições permanecem bloqueadas; recuperação finita censurada.")
        else:
            recovery_lines.append("- ABD / crash permanente: a maioria permaneceu disponível; não houve reintegração do nó morto nem tempo de cura a estimar.")
    lines += ["", "### Conclusões pós-cura e prova de disponibilidade", ""] + recovery_lines + [
        "", "A prova de recusa usa invocações iniciadas no intervalo guardado: qualquer confirmação antes do início real da cura é relevante, inclusive na margem final de transição. No crash permanente, uma conclusão tardia durante a drenagem também violaria a recusa, pois não existe cura. Respostas após início da cura não provam sucesso durante a partição. Os resumos de fase mantêm limites guardados e censura separados dessa verificação. Retomada exige novas conclusões em maioria e minoria após a guarda de cura.", ""]
    lines += ["## Interpretação limitada aos dados", ""]
    for group in summary["aggregates"]:
        if group["scenario"] == "normal":
            rate = stat(group, "metrics.all.throughput")["mean"]
            success = stat(group, "metrics.all.success_fraction")["mean"]
            lines.append(f"- {group['algorithm'].upper()}, N={group['nodes']}: vazão média observada {display(rate)}/s, fração de sucesso {display(success)}, sob oferta configurada {group['rate']}/s. Não é medição de capacidade máxima.")
        else:
            majority = stat(group, "fault.intervals.fault.groups.majority.all.throughput")["mean"]
            minority = stat(group, "fault.intervals.fault.groups.minority.all.throughput")["mean"]
            lines.append(f"- {group['algorithm'].upper()} / {group['scenario']}: vazão guardada maioria {display(majority)}/s e minoria {display(minority)}/s; os valores, não causalidade adicional, fundamentam a comparação de disponibilidade.")
    if summary["complete"]:
        normals = {(group["algorithm"], group["nodes"]): group for group in summary["aggregates"] if group["scenario"] == "normal"}
        def series(algorithm, metric):
            return " → ".join(display(stat(normals[(algorithm, nodes)], metric)["mean"]) for nodes in (3, 5, 7))
        lines += [
            "", "### Escala e complexidade de mensagens", "",
            f"- RA gerou {series('ra', 'counters.per_completed_operation.logical_messages')} frames por conclusão em N=3/5/7, comparáveis a 2(N−1)=4/8/12 sem falhas. A espera p95 foi {series('ra', 'metrics.execute.ra_wait_ms.p95')} ms; a seção crítica tem duração configurada de 5 ms. Aumentar participantes acrescenta permissões necessárias e fanout O(N).",
            f"- ABD apresentou {series('abd', 'counters.per_completed_operation.transport_attempts')} tentativas HTTP e {series('abd', 'counters.per_completed_operation.logical_messages')} frames por conclusão. Duas fases enviadas a N−1 pares explicam a escala de cerca de 2(N−1) RPCs; respostas geradas e fanout cancelado distinguem RPCs de frames e do mínimo necessário para confirmar o quórum.",
            f"- Medianas cliente ABD: leitura {series('abd', 'metrics.read.client_latency_ms.p50')} ms e escrita {series('abd', 'metrics.write.client_latency_ms.p50')} ms. Não há controle experimental que atribua a diferença exclusivamente a fsync.",
            "- Diferenças nas médias das caudas ABD precisam ser lidas junto aos ICs p99 e à variabilidade entre repetições. A ordem das médias isolada não estabelece efeito estatisticamente significativo entre tamanhos; estes ICs não são um teste pareado de diferenças.",
            "- A carga é agregada e fixa por algoritmo: aumentar N reduz pedidos oferecidos por coordenador e pode reduzir colisões no gate local. A melhora de fração concluída não demonstra aumento de capacidade máxima. RA e ABD usam taxas e trabalhos distintos; não interpretar a vazão maior de ABD como comparação direta de velocidade.",
            "", "### Consequências das falhas", "",
            "- A exigência de todos os pares de RA explica o bloqueio observado nos dois lados da partição e após crash. ABD confirma na maioria intersectante e recusa a minoria; a cura devolve progresso aos três processos sem restart.",
            "- Os gráficos de falhas mostram todos os desfechos malsucedidos (busy, deadline, transporte e demais erros), não só timeouts. Um nó morto pode recusar conexão imediatamente; zero deadlines não significa zero falhas. Contagens e latências de deadlines/erros enviados ficam separadas no JSON.",
            "- Tempos pós-cura incluem próxima chegada e execução sob taxas diferentes por algoritmo. Não são comparação isolada da velocidade intrínseca de recuperação. Escritas sem resposta permanecem indeterminadas, sem afirmar aplicação ou rollback.",
        ]
    versions = summary["campaign"]["versions"]
    lines += ["", "## Limitações", "", "- Mesmo host físico: CPU, disco, escalonador e rede Docker compartilham recursos/domínio de falha. Repetições e ICs não capturam todos esses vieses sistemáticos; isolamento em contêiner não equivale a máquinas independentes.",
              "- Não há observador na carga de desempenho; estes traços longos não estabelecem linearizabilidade. Ensaios limitados de correção são separados.",
              f"- Versões efetivamente registradas (sem presumir Docker Compose v2): `{json.dumps(versions, ensure_ascii=False, sort_keys=True)}`.",
              "- Custos de fsync não podem ser causalmente isolados neste desenho; não atribuir diferenças exclusivamente à persistência. Gate busy do cliente/servidor e carga oferecida afetam os resultados.",
              "- p99 condicional ao sucesso não descreve pedidos que falharam; consultar denominadores, deadlines, rejeições, escritas indeterminadas e censura. Contadores incluem tentativas e frames não entregues; não são bytes/pacotes físicos.",
              "- Linhas temporais são binadas por repetição e depois agregadas; bandas são IC95% entre repetições, nunca entre pedidos. Sem amostra, lacunas não devem ser interpretadas como latência zero.", "", "## Artefatos", "", "`summary.json` contém valores por repetição/operação, contagens, n de percentis, cobertura dos contadores, estatísticas e linhas temporais. PNGs são gerados salvo `--no-plots`.", ""]
    lines += [f"Proveniência da análise: `{json.dumps(summary['analysis'], ensure_ascii=False, sort_keys=True)}`.", ""]
    if not summary["complete"]:
        lines += ["**INCOMPLETO: somente piloto/smoke; tabelas não representam os 100 ensaios congelados e não sustentam conclusões finais.**", ""]
    return "\n".join(lines)


def plot_results(summary, output):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    colors = {"ra": "#b54747", "read": "#2878b5", "write": "#2b9348", "abd": "#2878b5"}
    normals = [group for group in summary["aggregates"] if group["scenario"] == "normal"]
    def draw(axis, groups, metric, label, color):
        rates = sorted({group["rate"] for group in groups})
        if len(rates) > 1:
            for rate in rates:
                draw(axis, [group for group in groups if group["rate"] == rate],
                     metric, f"{label} (λ={rate})", color)
            return
        points = [(group["nodes"], group["statistics"].get(metric)) for group in groups]
        points = sorted(((node, value) for node, value in points if value and value["mean"] is not None),
                        key=lambda point: point[0])
        if not points:
            return
        x = [node for node, _ in points]
        y = [value["mean"] for _, value in points]
        axis.plot(x, y, marker="o", linewidth=1.2, color=color, label=label)
        for node, value in points:
            if value["ci95"] is not None:
                axis.errorbar([node], [value["mean"]], yerr=[[value["mean"] - value["ci95"][0]], [value["ci95"][1] - value["mean"]]], fmt="none", capsize=3, color=color)
    def finish(fig, axes, filename):
        for axis in axes:
            axis.grid(axis="y", alpha=0.25)
            if axis.get_legend_handles_labels()[0]:
                axis.legend(fontsize=8)
            else:
                axis.text(.5, .5, "Sem amostras válidas", ha="center", transform=axis.transAxes)
        fig.savefig(output / filename, dpi=220)
        plt.close(fig)
    figure, axes = plt.subplots(1, 3, figsize=(12, 3.8), constrained_layout=True)
    for axis, kind, algorithm, metric, title in zip(axes, ("execute", "read", "write"), ("ra", "abd", "abd"), ("ra_wait_ms", "client_latency_ms", "client_latency_ms"), ("RA — espera de aquisição", "ABD — leitura", "ABD — escrita")):
        groups = [group for group in normals if group["algorithm"] == algorithm]
        for percentile, style_color in ((50, "#2878b5"), (95, "#2b9348"), (99, "#b54747")):
            draw(axis, groups, f"metrics.{kind}.{metric}.p{percentile}", f"p{percentile}", style_color)
        axis.set(title=title, xlabel="Nós", ylabel="ms (sucessos; média e IC95%)", xticks=[3, 5, 7])
    finish(figure, axes, "normal_latency.png")
    figure, axes = plt.subplots(1, 3, figsize=(12, 3.8), constrained_layout=True)
    for axis, metric, title, units in zip(axes, ("metrics.all.throughput", "counters.per_completed_operation.logical_messages", "counters.per_completed_operation.transport_attempts"), ("Vazão na janela", "Frames lógicos gerados", "Tentativas HTTP de saída"), ("Conclusões/s", "Frames/conclusão", "Tentativas/conclusão")):
        for algorithm in ("ra", "abd"):
            selected = [group for group in normals if group["algorithm"] == algorithm]
            offered = sorted({group["rate"] for group in selected})
            label = f"{algorithm.upper()} (λ={offered[0]}/s)" if len(offered) == 1 else algorithm.upper()
            draw(axis, selected, metric, label, colors[algorithm])
        axis.set(title=title, xlabel="Nós", ylabel=units + " (média e IC95%)", xticks=[3, 5, 7])
    finish(figure, axes, "normal_throughput_messages.png")
    for group in summary["aggregates"]:
        if "timeline" not in group:
            continue
        figure, axes = plt.subplots(2, 1, figsize=(9, 6), constrained_layout=True, sharex=True)
        timeline = group["timeline"]
        for axis, metric, title in zip(axes, ("completed_rate", "failed_rate"), ("Conclusões/s", "Falhas/s (todos os desfechos)")):
            for side, color, label in (("majority", "#2878b5", "Maioria (nós 1/2)"), ("minority", "#b54747", "Minoria (nó 3)")):
                values = [(row["second"] + .5, row[f"{side}_{metric}"]) for row in timeline]
                axis.plot([x for x, _ in values], [value["mean"] for _, value in values], color=color, label=label)
                if all(value["ci95"] is not None for _, value in values):
                    axis.fill_between([x for x, _ in values], [max(0, value["ci95"][0]) for _, value in values], [value["ci95"][1] for _, value in values], color=color, alpha=.18)
            runs = [run for run in summary["runs"] if run["trial_id"] in group["trial_ids"]]
            for event_index in range(len(runs[0]["fault_events"])):
                times = [run["fault_events"][event_index]["end_elapsed_ns"] / NS for run in runs]
                axis.axvspan(min(times), max(times) + 1, alpha=.12, color="#777777")
                axis.axvline(statistics.mean(times), color="#222222", linestyle="--", linewidth=1)
            axis.set_ylabel(title + " (média; IC95%)")
        axes[0].set_title(f"{group['algorithm'].upper()} — {group['scenario']} — bins de 1 s por repetição")
        axes[1].set_xlabel("Segundos desde início da medição; faixas = transição + guarda")
        finish(figure, axes, f"fault_{group['algorithm']}_{group['scenario']}.png")


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--no-plots", action="store_true")
    parser.add_argument("--allow-partial", action="store_true", help="explicit incomplete pilot/smoke analysis only")
    args = parser.parse_args(argv)
    try:
        summary = analyze_campaign(args.root, args.allow_partial)
        output = args.output or args.root / "analysis"
        output.mkdir(parents=True, exist_ok=True)
        if not args.no_plots:
            plot_results(summary, output)
        (output / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2, allow_nan=False) + "\n", encoding="utf-8")
        (output / "REPORT.md").write_text(report(summary), encoding="utf-8")
        print(f"{summary['status']}: {summary['observed_trials']}/100 trials; {output}")
        return 0
    except (ValueError, KeyError, TypeError, IndexError, AttributeError, OSError, tarfile.TarError, ImportError) as exc:
        print(f"campaign analysis failed: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
