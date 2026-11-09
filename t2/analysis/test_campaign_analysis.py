"""Run: python3 -m unittest discover -s analysis -p 'test_campaign_analysis.py'.

Synthetic evidence below exercises parsing only; it is never campaign data.
"""
import copy
import hashlib
import io
import json
from pathlib import Path
import tarfile
import tempfile
import unittest

from campaign_analysis import (NS, analyze_campaign, digest, estimate, expected_keys,
                               fault_analysis, loads, operation_metrics, quantile,
                               validate_hashes, validate_raw)


def save(path, value):
    path.write_text(json.dumps(value) + "\n", encoding="utf-8")


def operation(identifier=1, *, phase="measurement", outcome="completed", end=2*NS,
              scheduled=NS, start=NS, node="node1"):
    row = {"type": "operation", "phase": phase, "id": identifier, "node_id": node,
           "operation": "execute", "scheduled_ns": scheduled, "start_ns": start,
           "end_ns": end, "http_status": 200, "outcome": outcome, "generator_lag_ns": 0}
    if outcome == "completed":
        row.update(wait_ns=50, held_ns=100)
    elif outcome == "queue_full":
        row.update(start_ns=None, http_status=0)
    elif outcome == "deadline":
        row["http_status"] = 504
    else:
        row["http_status"] = 0
    return row


def evidence_rows(manifest):
    metadata = {"type": "metadata", "schema": 1, "cluster_id": "cluster",
                "targets": [f"http://127.0.0.1:{18080+i}" for i in range(1, 4)],
                **{key: manifest[key] for key in ("algorithm", "run_id", "nodes", "rate", "seed",
                                                  "workers", "queue", "wait_ms", "hold_ms", "write_percent")},
                "warmup_ns": manifest["warmup_s"]*NS, "duration_ns": manifest["duration_s"]*NS,
                "client_timeout_ns": 2*NS}
    baseline = {"type": "snapshot", "stage": "baseline", "nodes": [
        {"node_id": f"node{i}", "state": {"logical_messages": 10, "transport_attempts": 5}}
        for i in range(1, 4)]}
    final = copy.deepcopy(baseline)
    final["stage"] = "final"
    for node in final["nodes"]:
        node["state"].update(logical_messages=14, transport_attempts=7)
    warmup_end = 100*NS + manifest["warmup_s"]*NS
    measurement_start = warmup_end + NS
    return [metadata,
            {"type": "phase", "phase": "warmup", "event": "start", "unix_ns": 100*NS},
            operation(1, phase="warmup", scheduled=0, start=0, end=1),
            {"type": "phase", "phase": "warmup", "event": "end", "unix_ns": warmup_end,
             "elapsed_ns": manifest["warmup_s"]*NS, "wall_elapsed_ns": manifest["warmup_s"]*NS},
            baseline,
            {"type": "phase", "phase": "measurement", "event": "start", "unix_ns": measurement_start},
            operation(2),
            {"type": "phase", "phase": "measurement", "event": "end",
             "unix_ns": measurement_start + manifest["duration_s"]*NS,
             "elapsed_ns": manifest["duration_s"]*NS, "wall_elapsed_ns": manifest["duration_s"]*NS},
            final]


def manifest():
    return {"schema": 1, "status": "complete", "trial_id": "ra-n3-normal-r01",
            "algorithm": "ra", "nodes": 3, "scenario": "normal", "repetition": 1,
            "seed": 714001, "rate": 2, "warmup_s": 10, "duration_s": 60,
            "inject_s": None, "heal_s": None, "hold_ms": 5, "write_percent": 50,
            "wait_ms": 1000, "workers": 32, "queue": 128, "client_timeout_s": 2,
            "run_id": "test-run", "project": "test-project"}


def write_rows(path, rows):
    path.write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")


def fixture(root):
    """One main trial with a real checksum/archive structure but synthetic rows."""
    files = {"go.mod": b"module example\n", "Dockerfile": b"FROM scratch\n",
             "cmd/main.go": b"package main\n", "internal/core.go": b"package core\n",
             "bin/load": b"synthetic-test-binary", "experiments/campaign.py": b"# test runner\n"}
    source = hashlib.sha256()
    for name in sorted(name for name in files if name not in ("bin/load", "experiments/campaign.py")):
        source.update(name.encode() + b"\0" + files[name] + b"\0")
    with tarfile.open(root / "source.tar.gz", "w:gz") as archive:
        for name, content in files.items():
            member = tarfile.TarInfo(name)
            member.size = len(content)
            archive.addfile(member, io.BytesIO(content))
    campaign = {"schema": 1, "campaign_id": "test", "source_digest": source.hexdigest(),
                "source_archive_sha256": digest(root / "source.tar.gz"),
                "workload_sha256": hashlib.sha256(files["bin/load"]).hexdigest(),
                "runner_sha256": hashlib.sha256(files["experiments/campaign.py"]).hexdigest(),
                "image": "sha256:" + "a"*64, "host": {}, "versions": {"Compose": "test"}}
    save(root / "campaign.json", campaign)
    protocol = {"nodes": [3, 5, 7], "repetitions": 10, "warmup_s": 10,
                "normal_duration_s": 60, "fault_duration_s": 35, "inject_s": 5, "heal_s": 20,
                "hold_ms": 5, "write_percent": 50, "wait_ms": 1000, "client_timeout_s": 2,
                "workers": 32, "queue": 128, "guard_s": 1,
                "topology": "client-bridge+internal-protocol-peer-aliases"}
    specs = []
    for algorithm, nodes, scenario, repetition in sorted(expected_keys()):
        specs.append({"trial_id": f"{algorithm}-n{nodes}-{scenario}-r{repetition:02d}",
                      "algorithm": algorithm, "nodes": nodes, "scenario": scenario,
                      "repetition": repetition, "seed": 714000+repetition, "rate": 2,
                      "warmup_s": 10, "duration_s": 60 if scenario == "normal" else 35,
                      "inject_s": None if scenario == "normal" else 5,
                      "heal_s": 20 if scenario == "partition" else None})
    save(root / "pilot-report.json", {"rates": {"ra": 2, "abd": 2}})
    save(root / "frozen.json", {**campaign, "protocol": protocol, "rates": {"ra": 2, "abd": 2},
                                "pilot_report_sha256": digest(root / "pilot-report.json"), "trials": specs})
    trial = root / "trials" / "ra-n3-normal-r01"
    trial.mkdir(parents=True)
    m = {**campaign, **manifest(), "frozen_sha256": digest(root / "frozen.json")}
    save(trial / "manifest.json", m)
    members = [{"id": f"node{i}", "url": f"http://peer-node{i}:8080"} for i in range(1, 4)]
    membership = {"mode": "standalone", "members": members}
    save(trial / "membership.json", membership)
    rows = evidence_rows(m)
    identity = {"run_id": m["run_id"], **membership}
    rows[0]["cluster_id"] = hashlib.sha256(json.dumps(identity, separators=(",", ":")).encode()).hexdigest()
    write_rows(trial / "operations.jsonl", rows)
    save(trial / "events.json", {"schema": 1, "clock_valid": True, "events": [],
                                 "measurement_start_unix_ns": rows[5]["unix_ns"]})
    services = {}
    for i in range(1, 4):
        services[f"node{i}"] = {"image": campaign["image"], "networks": {"client": {}, "protocol": {"aliases": [f"peer-node{i}"]}},
                                "ports": [f"127.0.0.1:{18080+i}:8080"], "cpus": 1.0, "mem_limit": "256m", "pids_limit": 64,
                                "read_only": True, "cap_drop": ["ALL"], "user": "65532:65532",
                                "command": ["-run-id", m["run_id"], "-id", f"node{i}", "-observer", ""]}
    save(trial / "compose.json", {"services": services, "networks": {"client": {}, "protocol": {"internal": True}}})
    (trial / "logs.txt").write_text("synthetic test evidence\n")
    save(trial / "checksums.json", {path.name: digest(path) for path in trial.iterdir() if path.is_file()})
    return trial, m


class CampaignStatisticsTests(unittest.TestCase):
    def test_interpolated_quantile_and_repetition_t_interval(self):
        self.assertIsNone(quantile([], .99))
        self.assertEqual(2.5, quantile([4, 1, 3, 2], .5))
        self.assertAlmostEqual(3.97, quantile([1, 2, 3, 4], .99))
        stats = estimate(list(range(1, 11)))
        self.assertEqual(10, stats["n"])
        self.assertAlmostEqual(5.5, stats["mean"])
        self.assertAlmostEqual(3.0276503541, stats["sample_sd"])
        self.assertAlmostEqual(2.16585059, stats["ci95"][1] - stats["mean"], places=6)
        self.assertIsNone(estimate([None, 5])["ci95"])
        self.assertEqual(1, estimate([None, 5])["missing"])

    def test_censoring_is_not_failure_and_error_only_percentiles_are_missing(self):
        rows = [operation(1, end=NS+1, scheduled=0, start=0),
                operation(2, outcome="deadline", scheduled=0, start=0, end=NS//2),
                operation(3, outcome="queue_full", scheduled=0, end=5)]
        metric = operation_metrics(rows, NS)
        self.assertEqual(1, metric["counts"]["completed"])
        self.assertEqual(1, metric["counts"]["censored_at_window"])
        self.assertEqual(0, metric["throughput"])
        self.assertEqual(1, metric["counts"]["rejected"])
        self.assertEqual(2, metric["counts"]["unsuccessful"])
        self.assertEqual(1, metric["counts"]["deadline"])
        self.assertEqual(3, metric["generator_lag_ms"]["n"])
        self.assertEqual(1, metric["failed_client_latency_ms"]["n"])
        self.assertEqual(500, metric["deadline_client_latency_ms"]["p99"])
        metric = operation_metrics(rows[1:], NS)
        self.assertEqual(0, metric["client_latency_ms"]["n"])
        self.assertIsNone(metric["client_latency_ms"]["p99"])
        self.assertIsNone(operation_metrics([], NS)["success_fraction"])

    def test_raw_warmup_counter_coverage_and_impossible_timing(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "raw.jsonl"
            m = manifest()
            rows = evidence_rows(m)
            rows[-1]["nodes"][2] = {"node_id": "node3", "error": "killed"}
            write_rows(path, rows)
            raw = validate_raw(path, m)
            self.assertEqual(1, len([row for row in raw["operations"] if row["phase"] == "measurement"]))
            self.assertEqual(2, raw["counters"]["coverage"]["final"])
            self.assertIsNone(raw["counters"]["cluster_delta"]["logical_messages"])
            rows[6]["end_ns"] = rows[6]["start_ns"]-1
            write_rows(path, rows)
            with self.assertRaisesRegex(ValueError, "response before arrival|ordering"):
                validate_raw(path, m)

    def test_clock_step_counter_decrease_missing_marker_duplicate_key(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "raw.jsonl"
            m = manifest()
            for mutation in ("clock", "counter", "marker"):
                rows = evidence_rows(m)
                if mutation == "clock":
                    rows[7]["elapsed_ns"] += NS
                elif mutation == "counter":
                    rows[-1]["nodes"][0]["state"]["logical_messages"] = 0
                else:
                    del rows[3]
                write_rows(path, rows)
                with self.assertRaises(ValueError, msg=mutation):
                    validate_raw(path, m)
        with self.assertRaisesRegex(ValueError, "duplicate"):
            loads('{"schema":1,"schema":2}')

    def test_hash_integrity_and_default_completeness_fail_closed(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            trial, _ = fixture(root)
            with self.assertRaisesRegex(ValueError, "INCOMPLETE"):
                analyze_campaign(root)
            partial = analyze_campaign(root, allow_partial=True)
            self.assertFalse(partial["complete"])
            self.assertEqual(1, partial["observed_trials"])
            self.assertEqual(1, partial["runs"][0]["warmup_operations_excluded"])
            (trial / "logs.txt").write_text("tampered")
            with self.assertRaisesRegex(ValueError, "SHA256"):
                validate_hashes(trial)
            with self.assertRaises(ValueError):
                analyze_campaign(root, allow_partial=True)

    def test_pilot_opt_in_and_archived_source_integrity(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            trial, m = fixture(root)
            (root / "frozen.json").unlink()
            (root / "trials").rename(root / "pilot")
            trial = root / "pilot" / trial.name
            m["repetition"] = 0
            save(trial / "manifest.json", m)
            hashes = json.loads((trial / "checksums.json").read_text())
            hashes["manifest.json"] = digest(trial / "manifest.json")
            save(trial / "checksums.json", hashes)
            with self.assertRaisesRegex(ValueError, "frozen"):
                analyze_campaign(root)
            self.assertFalse(analyze_campaign(root, allow_partial=True)["complete"])
            (root / "source.tar.gz").write_bytes(b"corrupt archive")
            with self.assertRaisesRegex(ValueError, "source archive SHA256"):
                analyze_campaign(root, allow_partial=True)

    def test_partition_requires_real_refusal_and_healed_minority_progress(self):
        fault = {"start_elapsed_ns": 2*NS, "end_elapsed_ns": 2*NS}
        heal = {"start_elapsed_ns": 8*NS, "end_elapsed_ns": 8*NS}
        rows = [operation(1, scheduled=4*NS, start=4*NS, end=5*NS),
                operation(2, outcome="deadline", scheduled=4*NS, start=4*NS, end=5*NS, node="node3"),
                operation(5, scheduled=8*NS+100_000_000, start=8*NS+100_000_000, end=8*NS+200_000_000),
                operation(3, scheduled=10*NS, start=10*NS, end=11*NS),
                operation(4, scheduled=10*NS, start=10*NS, end=12*NS, node="node3")]
        for row in rows:
            row["operation"] = "read"
        result = fault_analysis(rows, 15*NS, [fault, heal], "abd", "partition")
        self.assertAlmostEqual(.2, result["observed_post_heal_completion_s"])
        self.assertFalse(result["recovery_censored"])
        with self.assertRaisesRegex(ValueError, "restore both"):
            fault_analysis(rows[:-1], 15*NS, [fault, heal], "abd", "partition")
        rows[1].update(http_status=0, outcome="transport_error")
        with self.assertRaisesRegex(ValueError, "HTTP504"):
            fault_analysis(rows, 15*NS, [fault, heal], "abd", "partition")

    def test_fault_guards_http_refusal_and_permanent_ra_crash(self):
        event = {"start_elapsed_ns": 2*NS, "end_elapsed_ns": 2*NS}
        rows = [operation(1, outcome="deadline", scheduled=4*NS, start=4*NS, end=5*NS),
                operation(2, outcome="transport_error", scheduled=4*NS, start=4*NS, end=5*NS, node="node3")]
        result = fault_analysis(rows, 10*NS, [event], "ra", "crash")
        self.assertIsNone(result["observed_post_heal_completion_s"])
        self.assertTrue(result["recovery_censored"])
        self.assertEqual("fault", result["operation_intervals"][0]["interval"])
        self.assertEqual(1, result["timeline"][5]["minority"]["transport_error_rate"])
        self.assertEqual(1, result["timeline"][5]["minority"]["failed_rate"])
        self.assertEqual(0, result["timeline"][5]["minority"]["deadline_rate"])
        rows[0] = operation(1, scheduled=4*NS, start=4*NS, end=5*NS)
        with self.assertRaisesRegex(ValueError, "RA completed"):
            fault_analysis(rows, 10*NS, [event], "ra", "crash")
        rows[0]["end_ns"] = 11*NS
        with self.assertRaisesRegex(ValueError, "RA completed"):
            fault_analysis(rows, 10*NS, [event], "ra", "crash")


if __name__ == "__main__":
    unittest.main()
