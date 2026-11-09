#!/usr/bin/env python3
"""Independent, bounded linearizability checker for a single int64 register."""

import json
import sys
from pathlib import Path


MAX_OPERATIONS = 18
MAX_STATES = 200_000
INT64_MIN = -(1 << 63)
INT64_MAX = (1 << 63) - 1


def _result(verdict: str, states: int = 0, order: list[str] | None = None,
            reason: str | None = None) -> dict:
    result = {"verdict": verdict, "order": order if order is not None else [],
              "states": states}
    if reason is not None:
        result["reason"] = reason
    return result


def _int64(value: object) -> bool:
    return type(value) is int and INT64_MIN <= value <= INT64_MAX


def check_history(history: object, max_states: int = MAX_STATES) -> dict:
    """Return a verdict, expanded-state count, and a witness order if one exists.

    Indeterminate writes and increments can be omitted or apply after client timeout.
    Completed operations are never dropped, even when the search limit is reached.
    """
    if type(max_states) is not int or not 1 <= max_states <= MAX_STATES:
        return _result("invalid", reason=f"max_states must be an integer in 1..{MAX_STATES}")
    if not isinstance(history, dict):
        return _result("invalid", reason="history must be an object")
    if not _int64(history.get("initial_value")):
        return _result("invalid", reason="initial_value must be an int64")
    operations = history.get("operations")
    if not isinstance(operations, list):
        return _result("invalid", reason="operations must be an array")

    modeled = []
    ids = set()
    for index, operation in enumerate(operations):
        prefix = f"operations[{index}]"
        if not isinstance(operation, dict):
            return _result("invalid", reason=f"{prefix} must be an object")
        identifier = operation.get("id")
        if not isinstance(identifier, str):
            return _result("invalid", reason=f"{prefix}.id must be a string")
        if identifier in ids:
            return _result("invalid", reason=f"duplicate operation id: {identifier!r}")
        ids.add(identifier)
        kind = operation.get("operation")
        if kind not in ("read", "write", "increment"):
            return _result("invalid", reason=f"{prefix}.operation must be read, write or increment")
        start, end = operation.get("start_ns"), operation.get("end_ns")
        if type(start) is not int or start < 0:
            return _result("invalid", reason=f"{prefix}.start_ns must be a nonnegative integer")
        if type(end) is not int or end < start:
            return _result("invalid", reason=f"{prefix}.end_ns must be an integer >= start_ns")
        outcome = operation.get("outcome")
        if outcome not in ("completed", "not_applied", "indeterminate", "failed"):
            return _result("invalid", reason=f"{prefix}.outcome is not recognized")
        if outcome == "not_applied" and kind not in ("write", "increment"):
            return _result("invalid", reason=f"{prefix}: not_applied is only valid for writes or increments")
        if outcome == "failed" and kind != "read":
            return _result("invalid", reason=f"{prefix}: failed is only valid for reads")
        if kind == "increment":
            if outcome == "completed":
                if not _int64(operation.get("before")):
                    return _result("invalid", reason=f"{prefix}.before must be an int64")
            elif "before" in operation or "value" in operation:
                return _result("invalid", reason=f"{prefix}: unsuccessful increments must not have before or value")
        if kind == "write" or outcome == "completed":
            if not _int64(operation.get("value")):
                return _result("invalid", reason=f"{prefix}.value must be an int64")
        elif "value" in operation:
            return _result("invalid", reason=f"{prefix}: unsuccessful reads must not have a value")
        if outcome == "completed" or (kind in ("write", "increment") and outcome == "indeterminate"):
            modeled.append(operation)

    if len(modeled) > MAX_OPERATIONS:
        return _result("inconclusive", reason=f"more than {MAX_OPERATIONS} modeled operations")

    mandatory = 0
    predecessors = []
    for index, operation in enumerate(modeled):
        if operation["outcome"] == "completed":
            mandatory |= 1 << index
        # Pending mutations have no response event: client end_ns imposes no edge.
        predecessors.append(sum(
            1 << other_index
            for other_index, other in enumerate(modeled)
            if other_index != index and other["outcome"] == "completed"
            and other["end_ns"] < operation["start_ns"]
        ))

    seen = set()
    order = []

    class StateLimit(Exception):
        pass

    def search(remaining: int, value: int) -> bool:
        state = (remaining, value)
        if state in seen:
            return False
        if len(seen) >= max_states:
            raise StateLimit
        seen.add(state)
        # Remaining optional mutations can all be omitted; they block no successors.
        if not remaining & mandatory:
            return True
        for index, operation in enumerate(modeled):
            bit = 1 << index
            if not remaining & bit or predecessors[index] & remaining:
                continue
            kind = operation["operation"]
            if kind == "read":
                if operation["value"] != value:
                    continue
                next_value = value
            elif kind == "increment":
                if value == INT64_MAX:
                    continue
                next_value = value + 1
                if operation["outcome"] == "completed" and (
                    operation["before"] != value or operation["value"] != next_value
                ):
                    continue
            else:
                next_value = operation["value"]
            order.append(operation["id"])
            if search(remaining ^ bit, next_value):
                return True
            order.pop()
        return False

    try:
        found = search((1 << len(modeled)) - 1, history["initial_value"])
    except StateLimit:
        return _result("inconclusive", len(seen), reason=f"search exhausted max_states={max_states}")
    if found:
        return _result("linearizable", len(seen), order)
    return _result("nonlinearizable", len(seen), reason="no legal register order respects real-time precedence")


def main(argv: list[str] | None = None) -> int:
    arguments = sys.argv[1:] if argv is None else argv
    if len(arguments) != 1:
        result = _result("invalid", reason="usage: linearizability.py HISTORY.json")
    else:
        try:
            with Path(arguments[0]).open(encoding="utf-8") as source:
                history = json.load(source)
        except (OSError, ValueError, RecursionError) as error:
            result = _result("invalid", reason=f"cannot read history: {error}")
        else:
            result = check_history(history)
    print(json.dumps(result, separators=(",", ":")))
    return {"linearizable": 0, "nonlinearizable": 1}.get(result["verdict"], 2)


if __name__ == "__main__":
    raise SystemExit(main())
