from __future__ import annotations

import argparse
import shutil
from pathlib import Path

import matplotlib
import pandas as pd

matplotlib.use("Agg")
import matplotlib.pyplot as plt

RESULT_COLUMNS = {
    "policy",
    "lambda",
    "stable",
    "analytical_response",
    "mean_response",
    "ci95_response",
    "mean_throughput",
    "ci95_throughput",
    "mean_jobs",
    "little_right",
    "ci95_jobs",
}
TRACE_COLUMNS = {"policy", "lambda", "time", "jobs"}
ARCHITECTURE_COLUMNS = {
    "scenario",
    "mean_throughput",
    "ci95_throughput",
    "mean_rejected_full",
    "mean_backup_activations",
}
ARCHITECTURE_LABELS = {
    "private_queues": "Filas privadas",
    "shared_queue": "Fila compartilhada",
    "bounded_buffers": "Buffers limitados",
    "backup_overflow": "Backup de overflow",
    "multi_pool": "Pools hierárquicos",
}
POLICIES = ("random", "round_robin", "shortest_queue")
LABELS = {"random": "Aleatória", "round_robin": "Round Robin", "shortest_queue": "Fila Mais Curta"}
COLORS = {"random": "#b54747", "round_robin": "#2878b5", "shortest_queue": "#2b9348"}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--results", type=Path, default=Path("data/results.csv"))
    parser.add_argument("--trace", type=Path, default=Path("data/server_trace.csv"))
    parser.add_argument("--bounded-pareto-architectures", type=Path, default=Path("data/bounded_pareto_architectures.csv"))
    parser.add_argument("--output", type=Path, default=Path("figures"))
    parser.add_argument("--report-output", type=Path, default=Path("../report/figures"))
    args = parser.parse_args()
    results = read_csv(args.results, RESULT_COLUMNS)
    trace = read_csv(args.trace, TRACE_COLUMNS)
    architectures = read_csv(args.bounded_pareto_architectures, ARCHITECTURE_COLUMNS)
    args.output.mkdir(parents=True, exist_ok=True)
    plot_response(results, args.output / "response_comparison.png")
    plot_unstable(trace, args.output / "unstable_queues.png")
    plot_jobs(results, args.output / "jobs_comparison.png")
    plot_bounded_pareto_architectures(architectures, args.output / "bounded_pareto_architectures.png")
    args.report_output.mkdir(parents=True, exist_ok=True)
    for figure in args.output.glob("*.png"):
        shutil.copy2(figure, args.report_output / figure.name)


def read_csv(path: Path, required: set[str]) -> pd.DataFrame:
    if not path.is_file():
        raise FileNotFoundError(path)
    frame = pd.read_csv(path)
    missing = required.difference(frame.columns)
    if missing:
        raise ValueError(f"{path} missing columns: {sorted(missing)}")
    return frame


def plot_response(results: pd.DataFrame, output: Path) -> None:
    stable = results.loc[results["stable"]].copy()
    random_rows = stable.loc[stable["policy"] == "random"].sort_values("lambda")
    figure, axis = plt.subplots(figsize=(4.35, 3.1), constrained_layout=True)
    axis.plot(random_rows["lambda"], random_rows["analytical_response"], color="#222222", linewidth=1.6, label="M/M/1 analítico")
    for policy in POLICIES:
        rows = stable.loc[stable["policy"] == policy].sort_values("lambda")
        axis.errorbar(rows["lambda"], rows["mean_response"], yerr=rows["ci95_response"], marker="o", capsize=3, linewidth=1.2, color=COLORS[policy], label=LABELS[policy])
    axis.set_xlabel("Taxa de chegada λ")
    axis.set_ylabel("Tempo médio de resposta E[R]")
    axis.set_xticks(sorted(stable["lambda"].unique()))
    axis.grid(axis="y", alpha=0.25)
    axis.legend(fontsize=7)
    figure.savefig(output, dpi=220)
    plt.close(figure)

def plot_jobs(results: pd.DataFrame, output: Path) -> None:
    stable = results.loc[results["stable"]].copy()
    figure, axis = plt.subplots(figsize=(4.35, 1.7), constrained_layout=True)
    for policy in POLICIES:
        rows = stable.loc[stable["policy"] == policy].sort_values("lambda")
        axis.errorbar(rows["lambda"], rows["mean_jobs"], yerr=rows["ci95_jobs"], marker="o", capsize=3, linewidth=1.2, color=COLORS[policy], label=LABELS[policy])
    axis.set_xlabel("Taxa de chegada λ")
    axis.set_ylabel("População média E[N]")
    axis.set_xticks(sorted(stable["lambda"].unique()))
    axis.grid(axis="y", alpha=0.25)
    axis.legend(fontsize=7, ncol=3, loc="upper left")
    figure.savefig(output, dpi=220)
    plt.close(figure)

def plot_bounded_pareto_architectures(architectures: pd.DataFrame, output: Path) -> None:
    rows = architectures.copy()
    positions = list(range(len(rows)))
    labels = [ARCHITECTURE_LABELS[scenario] for scenario in rows["scenario"]]
    figure, axes = plt.subplots(1, 2, figsize=(4.35, 1.5), constrained_layout=True, sharey=True)
    axes[0].errorbar(rows["mean_throughput"], positions, xerr=rows["ci95_throughput"], fmt="o", capsize=2, color="#2878b5")
    axes[0].set_xlabel("Vazão X")
    axes[0].set_yticks(positions, labels, fontsize=6.5)
    axes[0].grid(axis="x", alpha=0.25)
    axes[1].barh([position - 0.18 for position in positions], rows["mean_rejected_full"], height=0.34, color="#b54747", label="Rejeitadas")
    axes[1].barh([position + 0.18 for position in positions], rows["mean_backup_activations"], height=0.34, color="#2b9348", label="Backup")
    axes[1].set_xlabel("Eventos")
    axes[1].grid(axis="x", alpha=0.25)
    axes[1].legend(fontsize=6.5, loc="lower right")
    axes[0].invert_yaxis()
    figure.savefig(output, dpi=220)
    plt.close(figure)


def plot_unstable(trace: pd.DataFrame, output: Path) -> None:
    rows = trace.loc[trace["lambda"] == 3.3]
    figure, axis = plt.subplots(figsize=(4.35, 2.5), constrained_layout=True)
    for policy in POLICIES:
        policy_rows = rows.loc[rows["policy"] == policy]
        axis.plot(policy_rows["time"], policy_rows["jobs"], linewidth=1.0, color=COLORS[policy], label=LABELS[policy])
    axis.plot([0, 5000], [0, 1500], color="#222222", linestyle="--", linewidth=1.2, label="0,3t")
    axis.set_xlabel("Tempo")
    axis.set_ylabel("Requisições no sistema N(t)")
    axis.grid(alpha=0.25)
    axis.legend(fontsize=7)
    figure.savefig(output, dpi=220)
    plt.close(figure)


if __name__ == "__main__":
    main()
