"""Run with: python3 -m unittest discover -s analysis -p 'test_linearizability.py'."""

import contextlib
import io
import json
import tempfile
import unittest
from pathlib import Path

from linearizability import MAX_OPERATIONS, MAX_STATES, check_history, main


def operation(identifier, kind, value, start, end, outcome="completed"):
    result = {"id": identifier, "operation": kind, "start_ns": start,
              "end_ns": end, "outcome": outcome}
    if kind == "write" or outcome == "completed":
        result["value"] = value
    return result


def increment(identifier, before, value, start, end, outcome="completed"):
    result = operation(identifier, "increment", value, start, end, outcome)
    if outcome == "completed":
        result["before"] = before
    return result


def history(*operations, initial=0):
    return {"initial_value": initial, "operations": list(operations)}


class LinearizabilityTests(unittest.TestCase):
    def assert_verdict(self, expected, source, **kwargs):
        result = check_history(source, **kwargs)
        self.assertEqual(expected, result["verdict"], result)
        self.assertIsInstance(result["states"], int)
        if expected != "linearizable":
            self.assertEqual([], result["order"])
            self.assertTrue(result["reason"])
        return result

    def test_empty_and_sequential_histories(self):
        self.assertEqual([], self.assert_verdict("linearizable", history())["order"])
        source = history(operation("r0", "read", 0, 0, 1),
                         operation("w", "write", 7, 2, 3),
                         operation("r7", "read", 7, 4, 5))
        self.assertEqual(["r0", "w", "r7"],
                         self.assert_verdict("linearizable", source)["order"])

    def test_concurrent_writers_require_backtracking_and_value_in_memo_key(self):
        source = history(operation("a", "write", 1, 0, 10),
                         operation("b", "write", 2, 0, 10),
                         operation("r", "read", 1, 11, 12))
        self.assertEqual(["b", "a", "r"],
                         self.assert_verdict("linearizable", source)["order"])

    def test_stale_read_after_completed_write_is_invalid(self):
        self.assert_verdict("nonlinearizable", history(
            operation("w", "write", 1, 0, 1),
            operation("stale", "read", 0, 2, 3)))

    def test_read_without_a_source_write_is_invalid(self):
        self.assert_verdict("nonlinearizable", history(operation("r", "read", 9, 0, 1)))

    def test_equal_timestamps_are_concurrent_but_strictly_earlier_is_not(self):
        self.assertEqual(["r", "w"], self.assert_verdict("linearizable", history(
            operation("w", "write", 1, 0, 1),
            operation("r", "read", 0, 1, 2)))["order"])
        self.assert_verdict("nonlinearizable", history(
            operation("w", "write", 1, 0, 0),
            operation("r", "read", 0, 1, 1)))

    def test_pending_write_can_be_omitted(self):
        source = history(operation("pending", "write", 5, 0, 1, "indeterminate"),
                         operation("r", "read", 0, 2, 3))
        self.assert_verdict("linearizable", source)
        self.assert_verdict("linearizable", history(
            operation("pending", "write", 5, 0, 1, "indeterminate")))

    def test_pending_write_can_explain_visibility(self):
        source = history(operation("pending", "write", 5, 0, 1, "indeterminate"),
                         operation("r", "read", 5, 2, 3))
        self.assertEqual(["pending", "r"],
                         self.assert_verdict("linearizable", source)["order"])

    def test_timeout_does_not_force_write_before_later_old_read(self):
        source = history(operation("pending", "write", 5, 0, 1, "indeterminate"),
                         operation("old", "read", 0, 2, 3),
                         operation("new", "read", 5, 4, 5))
        self.assertEqual(["old", "pending", "new"],
                         self.assert_verdict("linearizable", source)["order"])

    def test_pending_write_must_still_respect_its_invocation(self):
        self.assert_verdict("nonlinearizable", history(
            operation("r", "read", 5, 0, 1),
            operation("pending", "write", 5, 2, 3, "indeterminate")))

    def test_visible_pending_write_cannot_later_be_undone_by_omission(self):
        self.assert_verdict("nonlinearizable", history(
            operation("pending", "write", 5, 0, 1, "indeterminate"),
            operation("new", "read", 5, 2, 3),
            operation("old", "read", 0, 4, 5)))

    def test_not_applied_writes_cannot_explain_reads(self):
        rejected = operation("rejected", "write", 5, 0, 1, "not_applied")
        self.assert_verdict("nonlinearizable", history(
            rejected, operation("r", "read", 5, 2, 3)))
        self.assertEqual(["r"], self.assert_verdict("linearizable", history(
            rejected, operation("r", "read", 0, 2, 3)))["order"])

    def test_unsuccessful_reads_are_omitted(self):
        source = history(operation("failed", "read", None, 0, 1, "failed"),
                         operation("unknown", "read", None, 2, 3, "indeterminate"),
                         operation("r", "read", 0, 4, 5))
        self.assertEqual(["r"], self.assert_verdict("linearizable", source)["order"])

    def test_int64_boundaries_and_metadata(self):
        for value in (-(1 << 63), (1 << 63) - 1):
            with self.subTest(value=value):
                source = history(operation("w", "write", value, 0, 1),
                                 operation("r", "read", value, 2, 3), initial=value)
                source["run_id"] = "provenance"
                source["operations"][0]["raw_response"] = {"status": "completed"}
                self.assert_verdict("linearizable", source)

    def test_concurrent_increments_return_distinct_values(self):
        source = history(increment("second", 1, 2, 0, 10),
                         increment("first", 0, 1, 0, 10),
                         operation("r", "read", 2, 11, 12))
        self.assert_verdict("linearizable", source)

    def test_concurrent_increments_cannot_lose_an_update(self):
        self.assert_verdict("nonlinearizable", history(
            increment("a", 0, 1, 0, 10),
            increment("b", 0, 1, 0, 10),
            operation("r", "read", 1, 11, 12)))

    def test_increment_requires_observed_before_and_exact_successor(self):
        for before, value in ((1, 2), (1, 1), (0, 2), (0, 0)):
            with self.subTest(before=before, value=value):
                self.assert_verdict("nonlinearizable", history(
                    increment("i", before, value, 0, 1)))

    def test_increments_interoperate_with_writes_and_reads(self):
        source = history(operation("w", "write", -2, 0, 1),
                         increment("i", -2, -1, 2, 3),
                         operation("r", "read", -1, 4, 5))
        source["operations"][1]["raw_response"] = {"status": "completed"}
        self.assert_verdict("linearizable", source)

    def test_increment_int64_edges_and_overflow(self):
        minimum, maximum = -(1 << 63), (1 << 63) - 1
        for before in (minimum, maximum - 1):
            with self.subTest(before=before):
                self.assert_verdict("linearizable", history(
                    increment("i", before, before + 1, 0, 1), initial=before))
        for value in (minimum, maximum):
            with self.subTest(overflow_value=value):
                self.assert_verdict("nonlinearizable", history(
                    increment("i", maximum, value, 0, 1), initial=maximum))
        pending = increment("pending", None, None, 0, 1, "indeterminate")
        self.assert_verdict("linearizable", history(
            pending, operation("r", "read", maximum, 2, 3), initial=maximum))
        self.assert_verdict("nonlinearizable", history(
            pending, operation("r", "read", minimum, 2, 3), initial=maximum))

    def test_pending_increment_can_be_omitted_or_explain_visibility(self):
        pending = increment("pending", None, None, 0, 1, "indeterminate")
        self.assert_verdict("linearizable", history(pending))
        for value in (0, 1):
            with self.subTest(read=value):
                self.assert_verdict("linearizable", history(
                    pending, operation("r", "read", value, 2, 3)))
        self.assert_verdict("nonlinearizable", history(
            pending, operation("r", "read", 2, 2, 3)))
        self.assert_verdict("nonlinearizable", history(
            pending, operation("new", "read", 1, 2, 3),
            operation("old", "read", 0, 4, 5)))

    def test_pending_increment_can_apply_after_timeout(self):
        pending = increment("pending", None, None, 0, 1, "indeterminate")
        self.assert_verdict("linearizable", history(
            pending, operation("old", "read", 0, 2, 3),
            operation("new", "read", 1, 4, 5)))
        self.assert_verdict("linearizable", history(
            pending, increment("completed", 0, 1, 2, 3),
            operation("r", "read", 2, 4, 5)))

    def test_pending_increment_cannot_apply_before_invocation(self):
        self.assert_verdict("nonlinearizable", history(
            operation("r", "read", 1, 0, 1),
            increment("pending", None, None, 2, 3, "indeterminate")))

    def test_not_applied_increment_cannot_explain_new_value(self):
        rejected = increment("rejected", None, None, 0, 1, "not_applied")
        self.assert_verdict("linearizable", history(
            rejected, operation("r", "read", 0, 2, 3)))
        self.assert_verdict("nonlinearizable", history(
            rejected, operation("r", "read", 1, 2, 3)))

    def test_increment_result_fields_require_int64(self):
        for field in ("before", "value"):
            for value in (None, True, False, 0.0, "0", -(1 << 63) - 1, 1 << 63):
                with self.subTest(field=field, value=value):
                    op = increment("i", 0, 1, 0, 1)
                    op[field] = value
                    self.assert_verdict("invalid", history(op))
            with self.subTest(missing=field):
                op = increment("i", 0, 1, 0, 1)
                del op[field]
                self.assert_verdict("invalid", history(op))

    def test_unsuccessful_increments_have_no_result_fields(self):
        self.assert_verdict("invalid", history(
            increment("failed", None, None, 0, 1, "failed")))
        for outcome in ("not_applied", "indeterminate"):
            for field in ("before", "value"):
                for value in (None, 0):
                    with self.subTest(outcome=outcome, field=field, value=value):
                        op = increment("i", None, None, 0, 1, outcome)
                        op[field] = value
                        self.assert_verdict("invalid", history(op))

    def test_increment_bounds_count_pending_but_not_not_applied(self):
        for outcome in ("completed", "indeterminate", "not_applied"):
            with self.subTest(outcome=outcome):
                operations = [increment(str(i), i, i + 1, 0, 1, outcome)
                              for i in range(MAX_OPERATIONS + 1)]
                verdict = "linearizable" if outcome == "not_applied" else "inconclusive"
                self.assert_verdict(verdict, history(*operations))

    def test_invalid_history_shapes_and_initial_value_types(self):
        for source in (None, [], True, {}, {"initial_value": 0},
                       {"initial_value": 0, "operations": {}},
                       {"initial_value": 0, "operations": [None]}):
            with self.subTest(source=source):
                self.assert_verdict("invalid", source)
        for value in (True, False, None, 0.0, "0", -(1 << 63) - 1, 1 << 63):
            with self.subTest(initial=value):
                self.assert_verdict("invalid", history(initial=value))

    def test_invalid_operation_fields_and_types(self):
        invalid_fields = {
            "id": [None, 1, True, []],
            "operation": [None, "update", True, []],
            "start_ns": [None, -1, True, 0.0, "0"],
            "end_ns": [None, -1, True, 0.0, "0"],
            "outcome": [None, "success", True, []],
            "value": [None, True, False, 0.0, "0", -(1 << 63) - 1, 1 << 63],
        }
        for field, values in invalid_fields.items():
            for value in values:
                with self.subTest(field=field, value=value):
                    op = operation("r", "read", 0, 0, 1)
                    op[field] = value
                    self.assert_verdict("invalid", history(op))
        for field in invalid_fields:
            with self.subTest(missing=field):
                op = operation("r", "read", 0, 0, 1)
                del op[field]
                self.assert_verdict("invalid", history(op))
        self.assert_verdict("invalid", history(operation("r", "read", 0, 3, 2)))

    def test_duplicate_ids_and_incompatible_outcomes(self):
        self.assert_verdict("invalid", history(
            operation("same", "read", 0, 0, 1), operation("same", "read", 0, 2, 3)))
        self.assert_verdict("invalid", history(operation("w", "write", 0, 0, 1, "failed")))
        self.assert_verdict("invalid", history(operation("r", "read", None, 0, 1, "not_applied")))
        for outcome in ("failed", "indeterminate"):
            with self.subTest(outcome=outcome):
                op = operation("r", "read", None, 0, 1, outcome)
                op["value"] = None
                self.assert_verdict("invalid", history(op))
        for outcome in ("not_applied", "indeterminate"):
            with self.subTest(write_outcome=outcome):
                op = operation("w", "write", 0, 0, 1, outcome)
                del op["value"]
                self.assert_verdict("invalid", history(op))

    def test_operation_bound_never_accepts_a_subset(self):
        operations = [operation(str(i), "read", 0, i * 2, i * 2 + 1)
                      for i in range(MAX_OPERATIONS + 1)]
        self.assert_verdict("inconclusive", history(*operations))
        self.assert_verdict("linearizable", history(*operations[:-1]))
        operations[-1]["value"] = True
        self.assert_verdict("invalid", history(*operations))

    def test_bound_counts_pending_but_not_omitted_operations(self):
        self.assert_verdict("inconclusive", history(*[
            operation(str(i), "write", i, 0, 1, "indeterminate")
            for i in range(MAX_OPERATIONS + 1)]))
        self.assert_verdict("linearizable", history(*[
            operation(str(i), "write", i, 0, 1, "not_applied")
            for i in range(MAX_OPERATIONS + 1)]))

    def test_state_bound_and_invalid_search_limits(self):
        source = history(operation("r", "read", 0, 0, 1))
        result = self.assert_verdict("inconclusive", source, max_states=1)
        self.assertEqual(1, result["states"])
        self.assert_verdict("linearizable", source, max_states=2)
        for limit in (None, True, 0, -1, 1.0, "10", MAX_STATES + 1):
            with self.subTest(limit=limit):
                self.assert_verdict("invalid", source, max_states=limit)

    def test_cli_json_and_exit_codes(self):
        cases = [
            (history(operation("r", "read", 0, 0, 1)), 0, "linearizable"),
            (history(operation("r", "read", 9, 0, 1)), 1, "nonlinearizable"),
            (history(increment("i", 0, 1, 0, 1),
                     operation("r", "read", 1, 2, 3)), 0, "linearizable"),
            (history(increment("a", 0, 1, 0, 10),
                     increment("b", 0, 1, 0, 10)), 1, "nonlinearizable"),
            ({}, 2, "invalid"),
            (history(*[operation(str(i), "read", 0, 0, 1)
                       for i in range(MAX_OPERATIONS + 1)]), 2, "inconclusive"),
        ]
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "history.json"
            for source, expected_code, verdict in cases:
                with self.subTest(verdict=verdict):
                    path.write_text(json.dumps(source), encoding="utf-8")
                    output = io.StringIO()
                    with contextlib.redirect_stdout(output):
                        code = main([str(path)])
                    self.assertEqual(expected_code, code)
                    self.assertEqual(verdict, json.loads(output.getvalue())["verdict"])
            path.write_text("{broken", encoding="utf-8")
            for arguments in ([], [str(path)], [str(path) + ".missing"], ["a", "b"]):
                with self.subTest(arguments=arguments):
                    output = io.StringIO()
                    with contextlib.redirect_stdout(output):
                        code = main(arguments)
                    self.assertEqual(2, code)
                    self.assertEqual("invalid", json.loads(output.getvalue())["verdict"])


if __name__ == "__main__":
    unittest.main()
