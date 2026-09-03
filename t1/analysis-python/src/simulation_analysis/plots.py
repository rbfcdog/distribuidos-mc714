"""Render publication-ready figures from the Go simulator CSV exports."""

from __future__ import annotations

import argparse
from pathlib import Path

import matplotlib
import pandas as pd

matplotlib.use("Agg")
import matplotlib.pyplot as plt

RESULT_COLUMNS = {
    "policy",
    "burst_size",
    "trials",
    "mean_throughput",
    "theoretical_throughput",
    "mean_response_time",
    "theoretical_response_time",
    "mean_completed",
    "mean_unfinished",
    "throughput_absolute_error",
    "response_absolute_error",
}
TRACE_COLUMNS = {"policy", "burst_size", "time", "server_id", "active", "queue_length", "completed"}
POLICY_LABELS = {
    "random": "Random",
    "round_robin": "Round robin",
    "shortest_queue": "Shortest queue",
}
COLORS = {"random": "#b54747", "round_robin": "#2878b5", "shortest_queue": "#2b9348"}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--results", type=Path, default=Path("data/results.csv"))
    parser.add_argument("--trace", type=Path, default=Path("data/server_trace.csv"))
    parser.add_argument("--output", type=Path, default=Path("figures"))
    args = parser.parse_args()

    results = read_csv(args.results, RESULT_COLUMNS)
    trace = read_csv(args.trace, TRACE_COLUMNS)
    args.output.mkdir(parents=True, exist_ok=True)

    plot_metrics(results, args.output / "metrics_by_burst.png")
    plot_model_error(results, args.output / "model_comparison_error.png")
    plot_queue_dynamics(trace, args.output / "queue_dynamics_burst_120.png")
    plot_server_distribution(trace, args.output / "server_distribution_burst_120.png")
    plot_relative_performance(results, args.output / "relative_policy_performance.png")


def read_csv(path: Path, required_columns: set[str]) -> pd.DataFrame:
    if not path.is_file():
        raise FileNotFoundError(f"missing simulator export: {path}")
    frame = pd.read_csv(path)
    missing = required_columns.difference(frame.columns)
    if missing:
        raise ValueError(f"{path} is missing columns: {', '.join(sorted(missing))}")
    return frame


def plot_metrics(results: pd.DataFrame, output: Path) -> None:
    figure, axes = plt.subplots(2, 1, figsize=(4.35, 5.0), constrained_layout=True)
    _plot_metric(
        axes[0],
        results,
        simulation="mean_throughput",
        theoretical="theoretical_throughput",
        ylabel="Throughput (requests / time unit)",
    )
    _plot_metric(
        axes[1],
        results,
        simulation="mean_response_time",
        theoretical="theoretical_response_time",
        ylabel="Average response time (time units)",
    )
    axes[0].set_title("Throughput")
    axes[1].set_title("Response time")
    figure.savefig(output, dpi=220)
    plt.close(figure)


def _plot_metric(axis: plt.Axes, results: pd.DataFrame, simulation: str, theoretical: str, ylabel: str) -> None:
    for policy, group in results.groupby("policy", sort=False):
        ordered = group.sort_values("burst_size")
        axis.plot(
            ordered["burst_size"],
            ordered[simulation],
            marker="o",
            linewidth=2,
            color=COLORS[policy],
            label=POLICY_LABELS[policy],
        )
    theoretical_values = results.sort_values("burst_size").drop_duplicates("burst_size")
    axis.plot(
        theoretical_values["burst_size"],
        theoretical_values[theoretical],
        linestyle="--",
        linewidth=1.8,
        color="#292929",
        label="Fair-routing model",
    )
    axis.set_xlabel("Burst size (requests)")
    axis.set_ylabel(ylabel)
    axis.grid(alpha=0.25)
    axis.legend(fontsize=8)


def plot_model_error(results: pd.DataFrame, output: Path) -> None:
    figure, axes = plt.subplots(1, 2, figsize=(11, 4.25), constrained_layout=True)
    for policy, group in results.groupby("policy", sort=False):
        ordered = group.sort_values("burst_size")
        for axis, column, label in (
            (axes[0], "throughput_absolute_error", "Absolute throughput error"),
            (axes[1], "response_absolute_error", "Absolute response-time error"),
        ):
            axis.plot(
                ordered["burst_size"],
                ordered[column],
                marker="o",
                linewidth=2,
                color=COLORS[policy],
                label=POLICY_LABELS[policy],
            )
            axis.set_xlabel("Burst size (requests)")
            axis.set_ylabel(label)
            axis.grid(alpha=0.25)
            axis.legend(fontsize=8)
    axes[0].set_title("Simulation-model throughput gap")
    axes[1].set_title("Simulation-model response-time gap")
    figure.savefig(output, dpi=220)
    plt.close(figure)


def plot_queue_dynamics(trace: pd.DataFrame, output: Path) -> None:
    selected = trace.loc[trace["burst_size"] == 120]
    if selected.empty:
        raise ValueError("trace contains no burst-size 120 scenario")
    totals = (
        selected.groupby(["policy", "time"], as_index=False)[["active", "queue_length"]]
        .sum()
        .sort_values(["policy", "time"])
    )

    figure, axes = plt.subplots(2, 1, figsize=(4.35, 4.6), sharex=True, constrained_layout=True)
    for policy, group in totals.groupby("policy", sort=False):
        label = POLICY_LABELS[policy]
        axes[0].step(group["time"], group["active"], where="post", color=COLORS[policy], label=label)
        axes[1].step(group["time"], group["queue_length"], where="post", color=COLORS[policy], label=label)
    axes[0].set_title("Aggregate server state during a representative 120-request burst")
    axes[0].set_ylabel("Active requests")
    axes[1].set_ylabel("Queued requests")
    axes[1].set_xlabel("Simulation time")
    for axis in axes:
        axis.grid(alpha=0.25)
        axis.legend(fontsize=8)
    figure.savefig(output, dpi=220)
    plt.close(figure)

def plot_server_distribution(trace: pd.DataFrame, output: Path) -> None:
    selected = trace.loc[trace["burst_size"] == 120].sort_values("time")
    if selected.empty:
        raise ValueError("trace contains no burst-size 120 scenario")
    completed = selected.groupby(["policy", "server_id"], as_index=False).tail(1)
    max_queues = (
        selected.groupby(["policy", "server_id"], as_index=False)["queue_length"]
        .max()
        .sort_values(["policy", "server_id"])
    )

    figure, axes = plt.subplots(2, 1, figsize=(4.35, 4.6), constrained_layout=True)
    server_ids = sorted(selected["server_id"].unique())
    policies = list(POLICY_LABELS)
    width = 0.23
    for index, policy in enumerate(policies):
        offset = (index - 1) * width
        server_completed = completed.loc[completed["policy"] == policy].sort_values("server_id")
        server_queues = max_queues.loc[max_queues["policy"] == policy].sort_values("server_id")
        axes[0].bar(
            [server + offset for server in server_ids],
            server_completed["completed"],
            width=width,
            color=COLORS[policy],
            label=POLICY_LABELS[policy],
        )
        axes[1].bar(
            [server + offset for server in server_ids],
            server_queues["queue_length"],
            width=width,
            color=COLORS[policy],
            label=POLICY_LABELS[policy],
        )
    for axis in axes:
        axis.set_xticks(server_ids, [f"Server {server + 1}" for server in server_ids])
        axis.grid(axis="y", alpha=0.25)
        axis.legend(fontsize=8)
    axes[0].set_title("Completed requests per server")
    axes[0].set_ylabel("Requests")
    axes[1].set_title("Maximum queue length per server")
    axes[1].set_ylabel("Queued requests")
    figure.savefig(output, dpi=220)
    plt.close(figure)


def plot_relative_performance(results: pd.DataFrame, output: Path) -> None:
    baseline = results.loc[results["policy"] == "random", ["burst_size", "mean_throughput", "mean_response_time"]]
    baseline = baseline.rename(
        columns={
            "mean_throughput": "random_throughput",
            "mean_response_time": "random_response_time",
        }
    )
    comparison = results.merge(baseline, on="burst_size", validate="many_to_one")
    comparison["throughput_gain"] = 100 * (
        comparison["mean_throughput"] / comparison["random_throughput"] - 1
    )
    comparison["response_reduction"] = 100 * (
        1 - comparison["mean_response_time"] / comparison["random_response_time"]
    )

    figure, axes = plt.subplots(1, 2, figsize=(10.5, 4.2), constrained_layout=True)
    for policy, group in comparison.groupby("policy", sort=False):
        ordered = group.sort_values("burst_size")
        axes[0].plot(
            ordered["burst_size"],
            ordered["throughput_gain"],
            marker="o",
            linewidth=2,
            color=COLORS[policy],
            label=POLICY_LABELS[policy],
        )
        axes[1].plot(
            ordered["burst_size"],
            ordered["response_reduction"],
            marker="o",
            linewidth=2,
            color=COLORS[policy],
            label=POLICY_LABELS[policy],
        )
    for axis in axes:
        axis.axhline(0, color="#292929", linewidth=1)
        axis.set_xlabel("Burst size (requests)")
        axis.set_ylabel("Percent relative to random routing")
        axis.grid(alpha=0.25)
        axis.legend(fontsize=8)
    axes[0].set_title("Throughput gain")
    axes[1].set_title("Response-time reduction")
    figure.savefig(output, dpi=220)
    plt.close(figure)


if __name__ == "__main__":
    main()
