"""Render publication-ready figures from the Go simulator CSV exports."""

from __future__ import annotations

import argparse
from pathlib import Path

import matplotlib
import numpy as np
import pandas as pd

matplotlib.use("Agg")
import matplotlib.pyplot as plt

RESULT_COLUMNS = {
    "policy",
    "burst_size",
    "trials",
    "mean_throughput",
    "std_throughput",
    "finite_horizon_throughput",
    "stationary_throughput",
    "mean_response_time",
    "std_response_time",
    "finite_horizon_response_time",
    "stationary_response_time",
    "mean_completed",
    "mean_unfinished",
    "utilization",
    "throughput_absolute_error",
    "response_absolute_error",
}
TRIAL_COLUMNS = {
    "policy",
    "burst_size",
    "trial",
    "throughput",
    "response_time",
    "duration",
    "completed",
    "assigned_0",
    "assigned_1",
    "assigned_2",
}
TRACE_COLUMNS = {"policy", "burst_size", "time", "server_id", "active", "queue_length", "completed"}
TRAFFIC_COLUMNS = {"burst_size", "index", "interarrival"}
POLICY_LABELS = {
    "random": "Random",
    "round_robin": "Round robin",
    "shortest_queue": "Shortest queue",
}
COLORS = {"random": "#b54747", "round_robin": "#2878b5", "shortest_queue": "#2b9348"}
PARETO_L = 0.0004
PARETO_H = 0.04
PARETO_ALPHA = 1.4
SERVER_CAPACITY = 15
SERVICE_TIME = 0.05


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--results", type=Path, default=Path("data/results.csv"))
    parser.add_argument("--trials", type=Path, default=Path("data/trials.csv"))
    parser.add_argument("--trace", type=Path, default=Path("data/server_trace.csv"))
    parser.add_argument("--traffic", type=Path, default=Path("data/traffic.csv"))
    parser.add_argument("--output", type=Path, default=Path("figures"))
    args = parser.parse_args()

    results = read_csv(args.results, RESULT_COLUMNS)
    trials = read_csv(args.trials, TRIAL_COLUMNS)
    trace = read_csv(args.trace, TRACE_COLUMNS)
    traffic = read_csv(args.traffic, TRAFFIC_COLUMNS)
    args.output.mkdir(parents=True, exist_ok=True)

    plot_traffic(traffic, args.output / "traffic_bounded_pareto.png")
    plot_metrics(results, args.output / "metrics_by_burst.png")
    plot_model_comparison(results, args.output / "model_comparison.png")
    plot_fairness(trials, args.output / "fairness_vs_equal_share.png")
    plot_occupancy(trace, args.output / "occupancy_and_queues.png")
    plot_server_distribution(trace, args.output / "server_distribution_burst_120.png")
    plot_queue_dynamics(trace, args.output / "queue_dynamics_burst_120.png")
    plot_relative_performance(results, args.output / "relative_policy_performance.png")
    plot_model_error(results, args.output / "model_comparison_error.png")


def read_csv(path: Path, required_columns: set[str]) -> pd.DataFrame:
    if not path.is_file():
        raise FileNotFoundError(f"missing simulator export: {path}")
    frame = pd.read_csv(path)
    missing = required_columns.difference(frame.columns)
    if missing:
        raise ValueError(f"{path} is missing columns: {', '.join(sorted(missing))}")
    return frame


def bounded_pareto_pdf(x: np.ndarray) -> np.ndarray:
    term = 1.0 - (PARETO_L / PARETO_H) ** PARETO_ALPHA
    density = PARETO_ALPHA * (PARETO_L**PARETO_ALPHA) * x ** (-(PARETO_ALPHA + 1)) / term
    return np.where((x >= PARETO_L) & (x <= PARETO_H), density, 0.0)


def plot_traffic(traffic: pd.DataFrame, output: Path) -> None:
    samples = traffic.loc[traffic["burst_size"] == 120, "interarrival"].to_numpy()
    if samples.size == 0:
        samples = traffic["interarrival"].to_numpy()
    grid = np.linspace(PARETO_L, PARETO_H, 400)

    figure, axis = plt.subplots(figsize=(4.35, 2.9), constrained_layout=True)
    axis.hist(samples, bins=24, density=True, color="#6b6b6b", alpha=0.45, label="Simulated arrivals")
    axis.plot(grid, bounded_pareto_pdf(grid), color="#1d3557", linewidth=2, label=r"Bounded Pareto PDF ($\alpha=1.4$)")
    axis.set_xlim(0, 0.012)
    axis.set_xlabel("Inter-arrival time")
    axis.set_ylabel("Density")
    axis.set_title("Bounded Pareto traffic (H=0.8)")
    axis.grid(alpha=0.25)
    axis.legend(fontsize=7)
    figure.savefig(output, dpi=220)
    plt.close(figure)


def plot_metrics(results: pd.DataFrame, output: Path) -> None:
    figure, axes = plt.subplots(2, 1, figsize=(4.35, 4.2), constrained_layout=True)
    for policy, group in results.groupby("policy", sort=False):
        ordered = group.sort_values("burst_size")
        axes[0].errorbar(
            ordered["burst_size"],
            ordered["mean_throughput"],
            yerr=ordered["std_throughput"],
            marker="o",
            linewidth=2,
            capsize=3,
            color=COLORS[policy],
            label=POLICY_LABELS[policy],
        )
        axes[1].errorbar(
            ordered["burst_size"],
            ordered["mean_response_time"],
            yerr=ordered["std_response_time"],
            marker="o",
            linewidth=2,
            capsize=3,
            color=COLORS[policy],
            label=POLICY_LABELS[policy],
        )
    finite = results.sort_values("burst_size").drop_duplicates("burst_size")
    axes[0].plot(
        finite["burst_size"],
        finite["finite_horizon_throughput"],
        linestyle="--",
        linewidth=1.8,
        color="#292929",
        label="Finite-horizon model",
    )
    axes[1].axhline(SERVICE_TIME, linestyle="--", linewidth=1.8, color="#292929", label="Service time / finite-horizon")
    axes[0].set_ylabel("Throughput (req / time)")
    axes[1].set_ylabel("Mean response time")
    axes[1].set_xlabel("Burst size (requests)")
    axes[0].set_title("Throughput, 10-run mean ± s")
    axes[1].set_title("Response time, 10-run mean ± s")
    for axis in axes:
        axis.grid(alpha=0.25)
        axis.legend(fontsize=7)
    figure.savefig(output, dpi=220)
    plt.close(figure)


def plot_model_comparison(results: pd.DataFrame, output: Path) -> None:
    figure, axes = plt.subplots(2, 1, figsize=(4.35, 4.1), constrained_layout=True)
    selected = results.loc[results["policy"] == "round_robin"].sort_values("burst_size")
    bursts = selected["burst_size"].to_numpy()
    width = 8
    axes[0].bar(bursts - width, selected["mean_throughput"], width=width, color="#2878b5", label="Simulation")
    axes[0].bar(bursts, selected["finite_horizon_throughput"], width=width, color="#2b9348", label="Finite-horizon")
    axes[0].bar(bursts + width, selected["stationary_throughput"], width=width, color="#6b6b6b", label="Stationary 1/3")
    axes[1].bar(bursts - width, selected["mean_response_time"], width=width, color="#2878b5", label="Simulation")
    axes[1].bar(bursts, selected["finite_horizon_response_time"], width=width, color="#2b9348", label="Finite-horizon")
    axes[1].bar(bursts + width, selected["stationary_response_time"], width=width, color="#6b6b6b", label="Stationary 1/3")
    axes[0].set_title("Throughput vs analytical models")
    axes[1].set_title("Response time vs analytical models")
    axes[0].set_ylabel("Throughput")
    axes[1].set_ylabel("Response time")
    axes[1].set_xlabel("Burst size (requests)")
    for axis in axes:
        axis.set_xticks(bursts)
        axis.grid(axis="y", alpha=0.25)
        axis.legend(fontsize=7)
    figure.savefig(output, dpi=220)
    plt.close(figure)


def plot_fairness(trials: pd.DataFrame, output: Path) -> None:
    shares = trials.copy()
    total = shares["assigned_0"] + shares["assigned_1"] + shares["assigned_2"]
    for server in range(3):
        shares[f"share_{server}"] = shares[f"assigned_{server}"] / total
    means = shares.groupby(["policy", "burst_size"], as_index=False)[["share_0", "share_1", "share_2"]].mean()

    figure, axes = plt.subplots(3, 1, figsize=(4.35, 4.4), sharex=True, constrained_layout=True)
    for axis, policy in zip(axes, POLICY_LABELS):
        group = means.loc[means["policy"] == policy].sort_values("burst_size")
        for server, color in enumerate(("#1d3557", "#457b9d", "#a8dadc")):
            axis.plot(
                group["burst_size"],
                100 * group[f"share_{server}"],
                marker="o",
                linewidth=1.8,
                color=color,
                label=f"Server {server + 1}",
            )
        axis.axhline(100 / 3, linestyle="--", color="#292929", linewidth=1.2, label="Fair 1/3")
        axis.set_ylabel("Share (%)")
        axis.set_title(POLICY_LABELS[policy])
        axis.set_ylim(20, 50)
        axis.grid(alpha=0.25)
        axis.legend(fontsize=6, loc="upper right")
    axes[-1].set_xlabel("Burst size (requests)")
    figure.savefig(output, dpi=220)
    plt.close(figure)


def plot_occupancy(trace: pd.DataFrame, output: Path) -> None:
    selected = trace.loc[trace["burst_size"] == 120].copy()
    if selected.empty:
        raise ValueError("trace contains no burst-size 120 scenario")
    totals = (
        selected.groupby(["policy", "time"], as_index=False)[["active", "queue_length"]]
        .sum()
        .sort_values(["policy", "time"])
    )

    figure, axes = plt.subplots(2, 1, figsize=(4.35, 5.0), sharex=True, constrained_layout=True)
    for policy, group in totals.groupby("policy", sort=False):
        label = POLICY_LABELS[policy]
        axes[0].step(group["time"], group["active"], where="post", color=COLORS[policy], label=label)
        axes[1].step(group["time"], group["queue_length"], where="post", color=COLORS[policy], label=label)
    axes[0].axhline(3 * SERVER_CAPACITY, linestyle="--", color="#292929", linewidth=1.2, label="Capacity 3×15")
    axes[0].set_ylabel("Active requests")
    axes[0].set_title("Monitored occupancy, burst 120")
    axes[1].set_ylabel("Queued requests")
    axes[1].set_xlabel("Simulation time")
    axes[1].set_title("Monitored queue length, burst 120")
    for axis in axes:
        axis.grid(alpha=0.25)
        axis.legend(fontsize=7)
    figure.savefig(output, dpi=220)
    plt.close(figure)


def plot_model_error(results: pd.DataFrame, output: Path) -> None:
    figure, axes = plt.subplots(1, 2, figsize=(11, 4.25), constrained_layout=True)
    for policy, group in results.groupby("policy", sort=False):
        ordered = group.sort_values("burst_size")
        for axis, sim, theory, label in (
            (axes[0], "mean_throughput", "stationary_throughput", "Absolute throughput error vs stationary"),
            (axes[1], "mean_response_time", "stationary_response_time", "Absolute response-time error vs stationary"),
        ):
            axis.plot(
                ordered["burst_size"],
                (ordered[sim] - ordered[theory]).abs(),
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
    comparison["throughput_gain"] = 100 * (comparison["mean_throughput"] / comparison["random_throughput"] - 1)
    comparison["response_reduction"] = 100 * (1 - comparison["mean_response_time"] / comparison["random_response_time"])

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
