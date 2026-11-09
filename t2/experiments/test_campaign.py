"""Pure stdlib checks; no Docker, workload execution or generated measurements."""
import importlib.util
from pathlib import Path
import tempfile
import unittest

SPEC = importlib.util.spec_from_file_location("campaign", Path(__file__).with_name("campaign.py"))
campaign = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(campaign)
NS = campaign.NS


class CampaignTests(unittest.TestCase):
    def test_topologies_keep_client_access_separate_from_protocol(self):
        for size in (3, 5, 7):
            with self.subTest(size=size):
                config = campaign.topology(size, "sha256:" + "a" * 64, "run", Path("membership.json"))
                self.assertEqual(len(config["services"]), size)
                self.assertEqual(len(config["volumes"]), size)
                self.assertEqual(config["networks"], {"client": {}, "protocol": {"internal": True}})
                members = campaign.membership(size)["members"]
                for index, member in enumerate(members, 1):
                    node = config["services"][f"node{index}"]
                    self.assertEqual(member["url"], f"http://peer-node{index}:8080")
                    self.assertEqual(node["networks"]["protocol"]["aliases"], [f"peer-node{index}"])
                    self.assertNotIn("aliases", node["networks"]["client"])
                    self.assertEqual(node["ports"], [f"127.0.0.1:{18080 + index}:8080"])
                    self.assertTrue(node["read_only"])
                    self.assertEqual(node["user"], "65532:65532")
                    self.assertEqual(node["pids_limit"], 64)
                    self.assertEqual(node["cpus"], 1.0)
                    self.assertEqual(node["mem_limit"], "256m")
                    self.assertTrue(node["volumes"][0]["read_only"])
                    self.assertTrue(Path(node["volumes"][0]["source"]).is_absolute())
                self.assertNotIn("observer", config["services"])
        with self.assertRaises(campaign.CampaignError):
            campaign.topology(4, "sha256:" + "a" * 64, "run", Path("m"))
        with self.assertRaises(campaign.CampaignError):
            campaign.topology(3, "mc714-t2:local", "run", Path("m"))

    def test_main_replications_and_paired_schedule(self):
        trials = campaign.trial_specs({"ra": 10, "abd": 20})
        self.assertEqual(len(trials), 100)
        self.assertEqual(len({row["trial_id"] for row in trials}), 100)
        self.assertEqual(sum(row["scenario"] == "normal" for row in trials), 60)
        self.assertEqual(trials, campaign.trial_specs({"ra": 10, "abd": 20}))
        for repetition in range(1, 11):
            cohort = [row for row in trials if row["repetition"] == repetition]
            self.assertEqual({row["seed"] for row in cohort}, {714000 + repetition})
            self.assertEqual(len(cohort), 10)
            for row in cohort:
                self.assertEqual(row["warmup_s"], 10)
                self.assertEqual(row["duration_s"], 60 if row["scenario"] == "normal" else 35)
                if row["scenario"] != "normal":
                    self.assertEqual(row["nodes"], 3)
                    self.assertEqual(row["inject_s"], 5)

    def test_pilot_sampling_uses_generator_lag_not_queue_wait(self):
        rows = [{"type": "operation", "phase": "measurement", "outcome": "completed",
                 "generator_lag_ns": 1_000_000, "scheduled_ns": 0, "start_ns": NS}
                for _ in range(20)]
        self.assertTrue(campaign.pilot_criteria(rows)["qualified"])
        rows[0]["outcome"] = "unavailable"
        self.assertTrue(campaign.pilot_criteria(rows)["qualified"])
        rows[1]["outcome"] = "unavailable"
        self.assertFalse(campaign.pilot_criteria(rows)["qualified"])
        rows[1]["outcome"] = "completed"
        rows[0]["outcome"] = "deadline"
        self.assertFalse(campaign.pilot_criteria(rows)["qualified"])
        rows[0]["outcome"] = "completed"
        rows[-1]["generator_lag_ns"] = 50_000_000
        self.assertFalse(campaign.pilot_criteria(rows)["qualified"])
        self.assertFalse(campaign.pilot_criteria(rows[:9])["qualified"])
        self.assertFalse(campaign.pilot_criteria([])["qualified"])
        self.assertEqual(campaign.percentile([0, 10], .95), 9.5)

    @staticmethod
    def fault_events():
        return {"clock_valid": True, "minority_probes": [{"path": "/cluster", "http_status": 200}],
                "events": [{"event": "partition", "returncode": 0,
                            "start_elapsed_ns": 3 * NS, "end_elapsed_ns": 3 * NS},
                           {"event": "heal", "returncode": 0,
                            "start_elapsed_ns": 7 * NS, "end_elapsed_ns": 7 * NS}]}

    @staticmethod
    def operation(node, outcome, start, end, **extras):
        return {"type": "operation", "phase": "measurement", "node_id": node,
                "outcome": outcome, "start_ns": int(start * NS), "end_ns": int(end * NS),
                "operation": "read", **extras}

    def test_fault_invariants_are_nonvacuous_and_require_http_refusal(self):
        rows = [self.operation("node1", "completed", 4.2, 4.3),
                self.operation("node3", "deadline", 4.2, 5.3, http_status=504),
                self.operation("node2", "completed", 8.2, 8.3),
                self.operation("node3", "completed", 8.4, 8.5)]
        result = campaign.fault_invariants(rows, self.fault_events(), "abd", "partition", 12)
        self.assertEqual(result["minority_http504"], 1)
        self.assertEqual(result["majority_completed"], 1)
        with self.assertRaises(campaign.CampaignError):
            campaign.fault_invariants(rows, self.fault_events(), "ra", "partition", 12)
        rows[0]["outcome"] = "deadline"
        self.assertTrue(campaign.fault_invariants(rows, self.fault_events(), "ra", "partition", 12)["validated"])
        for malformed in ([], rows[:1], rows[:2], rows[:3]):
            with self.assertRaises(campaign.CampaignError):
                campaign.fault_invariants(malformed, self.fault_events(), "ra", "partition", 12)
        rows[1]["http_status"] = 0
        with self.assertRaises(campaign.CampaignError):
            campaign.fault_invariants(rows, self.fault_events(), "ra", "partition", 12)

    def test_fault_guard_excludes_transition_and_late_heal_completion(self):
        rows = [self.operation("node1", "completed", 3.9, 4),
                self.operation("node1", "completed", 4.5, 7.1),
                self.operation("node3", "deadline", 4.3, 5.4, http_status=504),
                self.operation("node1", "completed", 8.2, 8.4),
                self.operation("node3", "completed", 8.4, 8.5)]
        self.assertTrue(campaign.fault_invariants(rows, self.fault_events(), "ra", "partition", 12)["validated"])
        # A completion before healing is a violation even in its transition guard.
        rows[1]["end_ns"] = int(6.5 * NS)
        with self.assertRaises(campaign.CampaignError):
            campaign.fault_invariants(rows, self.fault_events(), "ra", "partition", 12)
        rows[1]["end_ns"] = int(7.1 * NS)
        events = self.fault_events()
        events["clock_valid"] = False
        with self.assertRaises(campaign.CampaignError):
            campaign.fault_invariants(rows, events, "ra", "partition", 12)
        events = self.fault_events()
        events["events"][0]["returncode"] = 1
        with self.assertRaises(campaign.CampaignError):
            campaign.fault_invariants(rows, events, "ra", "partition", 12)

    def test_observed_recovery_has_no_artificial_guard_delay(self):
        rows = [self.operation("node1", "deadline", 4.2, 5.3),
                self.operation("node3", "deadline", 4.2, 5.3, http_status=504),
                self.operation("node1", "completed", 7.1, 7.2),
                self.operation("node2", "completed", 8.2, 8.3),
                self.operation("node3", "completed", 8.4, 8.5)]
        result = campaign.fault_invariants(rows, self.fault_events(), "ra", "partition", 12)
        self.assertEqual(result["recovery_ns"], int(.2 * NS))

    def test_crash_never_fabricates_ra_recovery(self):
        events = {"clock_valid": True, "events": [{"event": "crash", "returncode": 0,
                  "start_elapsed_ns": 3 * NS, "end_elapsed_ns": 3 * NS}]}
        rows = [self.operation("node1", "deadline", 5, 6),
                self.operation("node3", "transport_error", 5, 5.1)]
        result = campaign.fault_invariants(rows, events, "ra", "crash", 12)
        self.assertIsNone(result["recovery_ns"])
        self.assertIsNone(result["healed_completed"])

    def test_hash_resume_rejects_partial_changed_config_and_extra_files(self):
        with tempfile.TemporaryDirectory() as temp:
            directory = Path(temp)
            expected = {"source_digest": "source", "image": "sha256:image", "rate": 10}
            with self.assertRaises(campaign.CampaignError):
                campaign.verify_complete(directory, expected)
            for name in ("operations.jsonl", "events.json", "compose.json", "membership.json",
                         "logs.txt", "containers.json", "stats.json", "commands.json"):
                (directory / name).write_text("evidence")
            campaign.save_json(directory / "manifest.json", {**expected, "status": "complete"})
            campaign.save_json(directory / "checksums.json", campaign.trial_checksums(directory))
            self.assertEqual(campaign.verify_complete(directory, expected)["status"], "complete")
            with self.assertRaises(campaign.CampaignError):
                campaign.verify_complete(directory, {**expected, "rate": 20})
            (directory / "operations.jsonl").write_text("tampered")
            with self.assertRaises(campaign.CampaignError):
                campaign.verify_complete(directory, expected)
            (directory / "operations.jsonl").write_text("evidence")
            (directory / "unexpected.txt").write_text("extra")
            with self.assertRaises(campaign.CampaignError):
                campaign.verify_complete(directory, expected)
            with self.assertRaises(FileExistsError):
                campaign.save_json(directory / "manifest.json", {})

    def test_source_digest_binds_actual_dirty_go_and_docker_bytes(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            (root / "cmd").mkdir()
            (root / "internal").mkdir()
            for name, content in (("go.mod", "module example"), ("Dockerfile", "FROM scratch"),
                                  ("cmd/main.go", "package main"), ("internal/state.go", "package state")):
                (root / name).write_text(content)
            first = campaign.source_digest(root)
            (root / "README.md").write_text("documentation is not algorithm source")
            self.assertEqual(first, campaign.source_digest(root))
            (root / "internal/state.go").write_text("package state\n// dirty change")
            self.assertNotEqual(first, campaign.source_digest(root))


if __name__ == "__main__":
    unittest.main()
