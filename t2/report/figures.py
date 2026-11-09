"""Render the frozen campaign statistics at IEEE column sizes without recollection."""

import json
import math
import statistics
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

ROOT = Path(__file__).resolve().parent
SUMMARY = ROOT.parent / "data/campaigns/reference-bounded-20261021/analysis/summary.json"
COLORS = {"ra": "#b54747", "abd": "#2878b5"}


def estimates(rows):
    # Missing repetitions must not become fabricated zeros in the paper.
    assert all(row["n"] == 10 and row["missing"] == 0 for row in rows)
    assert all(row["ci95"][0] <= row["mean"] <= row["ci95"][1] for row in rows)
    assert all(math.isfinite(v) for row in rows for v in (row["mean"], *row["ci95"]))
    return ([row["mean"] for row in rows], [row["ci95"][0] for row in rows],
            [row["ci95"][1] for row in rows])


def curve(axis, groups, metric, label, color, marker="o", linestyle="-"):
    ordered = sorted(groups, key=lambda group: group["nodes"])
    means, lower, upper = estimates([group["statistics"][metric] for group in ordered])
    axis.errorbar([group["nodes"] for group in ordered], means,
                  yerr=([m - lo for m, lo in zip(means, lower)],
                        [hi - m for m, hi in zip(means, upper)]),
                  label=label, color=color, marker=marker, linestyle=linestyle,
                  linewidth=1.15, markersize=3.5, capsize=2.5)


def finish(figure, axes, name):
    for axis in axes:
        axis.grid(axis="y", color="0.88", linewidth=.5)
        axis.set_axisbelow(True)
        axis.set_ylim(bottom=0)
        axis.spines[["top", "right"]].set_visible(False)
        axis.legend(frameon=False, fontsize=7)
    target = ROOT / "figures" / f"{name}.pdf"
    figure.savefig(target, metadata={"Title": name.replace("_", " ")})
    plt.close(figure)
    print(target.relative_to(ROOT))


def main():
    summary = json.loads(SUMMARY.read_text(encoding="utf-8"))
    assert summary["complete"] and summary["observed_trials"] == 100
    normal = {algorithm: [group for group in summary["aggregates"]
                          if group["scenario"] == "normal" and group["algorithm"] == algorithm]
              for algorithm in COLORS}
    assert all(sorted(group["nodes"] for group in groups) == [3, 5, 7]
               for groups in normal.values())
    (ROOT / "figures").mkdir(exist_ok=True)
    plt.rcParams.update({"font.family": "serif", "font.serif": ["STIXGeneral"],
                         "mathtext.fontset": "stix", "font.size": 8.5,
                         "axes.labelsize": 8.5, "axes.titlesize": 9,
                         "xtick.labelsize": 8, "ytick.labelsize": 8,
                         "axes.linewidth": .6, "pdf.fonttype": 42})

    figure, axis = plt.subplots(figsize=(3.4, 2.15), layout="constrained")
    for percentile, color, marker in ((50, "#2878b5", "o"), (95, "#2b9348", "s"),
                                      (99, "#b54747", "^")):
        curve(axis, normal["ra"], f"metrics.execute.ra_wait_ms.p{percentile}",
              f"p{percentile}", color, marker)
    axis.set(xlabel="Número de nós", ylabel="Espera de aquisição (ms)", xticks=[3, 5, 7])
    finish(figure, [axis], "ra_latency")

    figure, axis = plt.subplots(figsize=(3.4, 2.15), layout="constrained")
    for operation, label, color, marker in (("read", "Leitura", "#2878b5", "o"),
                                            ("write", "Escrita", "#2b9348", "s")):
        for percentile, linestyle in ((95, "-"), (99, "--")):
            curve(axis, normal["abd"], f"metrics.{operation}.client_latency_ms.p{percentile}",
                  f"{label} p{percentile}", color, marker, linestyle)
    axis.set(xlabel="Número de nós", ylabel="Latência no cliente (ms)", xticks=[3, 5, 7])
    finish(figure, [axis], "abd_latency")

    figure, axis = plt.subplots(figsize=(3.4, 2.15), layout="constrained")
    for algorithm, marker in (("ra", "o"), ("abd", "s")):
        curve(axis, normal[algorithm], "metrics.all.throughput",
              f"{algorithm.upper()} com oferta de {normal[algorithm][0]['rate']}/s",
              COLORS[algorithm], marker)
    axis.set(xlabel="Número de nós", ylabel="Conclusões por segundo", xticks=[3, 5, 7])
    finish(figure, [axis], "throughput")

    figure, axis = plt.subplots(figsize=(3.4, 2.15), layout="constrained")
    for algorithm, marker in (("ra", "o"), ("abd", "s")):
        curve(axis, normal[algorithm], "counters.per_completed_operation.logical_messages",
              f"{algorithm.upper()} mensagens lógicas", COLORS[algorithm], marker)
    curve(axis, normal["abd"], "counters.per_completed_operation.transport_attempts",
          "ABD tentativas HTTP", "#2b9348", "^", "--")
    axis.set(xlabel="Número de nós", ylabel="Mensagens por conclusão", xticks=[3, 5, 7])
    finish(figure, [axis], "communication")

    for scenario in ("partition", "crash"):
        figure, axes = plt.subplots(2, 2, figsize=(7.0, 3.45), sharex=True,
                                    layout="constrained")
        for column, algorithm in enumerate(("ra", "abd")):
            group, = [group for group in summary["aggregates"]
                      if group["scenario"] == scenario and group["algorithm"] == algorithm]
            runs = [run for run in summary["runs"] if run["trial_id"] in group["trial_ids"]]
            seconds = [row["second"] + .5 for row in group["timeline"]]
            for row_index, metric in enumerate(("completed_rate", "failed_rate")):
                axis = axes[row_index, column]
                for side, color, linestyle, label in (
                        ("majority", "#2878b5", "-", "Nós 1 e 2"),
                        ("minority", "#b54747", "--", "Nó 3")):
                    means, lower, upper = estimates([row[f"{side}_{metric}"]
                                                     for row in group["timeline"]])
                    axis.plot(seconds, means, color=color, linestyle=linestyle,
                              linewidth=1.1, label=label)
                    axis.fill_between(seconds, lower, upper, color=color, alpha=.14)
                for event_index in range(len(runs[0]["fault_events"])):
                    events = [run["fault_events"][event_index] for run in runs]
                    starts = [event["start_elapsed_ns"] / 1e9 for event in events]
                    ends = [event["end_elapsed_ns"] / 1e9 for event in events]
                    axis.axvspan(min(starts), max(ends) + 1, color="0.4", alpha=.12)
                    axis.axvline(statistics.mean(ends), color="0.3", linestyle=":", linewidth=.8)
                axis.set(ylabel="Conclusões/s" if row_index == 0 else "Falhas/s",
                         xlim=(0, 35), xticks=[0, 5, 10, 15, 20, 25, 30, 35])
            axes[0, column].set_title(algorithm.upper())
            axes[1, column].set_xlabel("Tempo de medição (s)")
        finish(figure, list(axes.flat), scenario)


if __name__ == "__main__":
    main()
