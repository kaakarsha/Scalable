import csv
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

RES = Path(__file__).resolve().parent / "results"
OUT = Path(__file__).resolve().parent.parent.parent / "screenshots" / "benchmarks_measured"
OUT.mkdir(parents=True, exist_ok=True)

BLUE, RED, GREY = "#1f6fb2", "#c0392b", "#7f8c8d"


def rows(name):
    p = RES / name
    if not p.exists():
        return []
    with p.open() as f:
        return list(csv.DictReader(f))


def fig_speedup():
    r = rows("speedup_measured.csv")
    if not r:
        return
    x = [int(v["task_slots"]) for v in r]
    sp = [float(v["speedup"]) for v in r]
    secs = [float(v["seconds"]) for v in r]

    fig, ax = plt.subplots(figsize=(6, 4))
    ax.plot(x, sp, marker="o", lw=2, color=BLUE, label="measured")
    ax.plot(x, x, ls="--", color=GREY, label="ideal (linear)")
    for xi, yi in zip(x, sp):
        ax.annotate(f"{yi:.2f}x", (xi, yi), textcoords="offset points",
                    xytext=(6, -12), fontsize=9, color=BLUE)
    ax.set_xlabel("Spark task slots (executors x cores)")
    ax.set_ylabel("Speedup  $S_p = T_1 / T_p$")
    ax.set_title("Batch layer speedup on EMR — 2.03 GB, measured")
    ax.set_xticks(x)
    ax.grid(alpha=0.3)
    ax.legend()
    fig.tight_layout()
    fig.savefig(OUT / "fig1_speedup.png", dpi=200)
    plt.close(fig)

    fig, ax = plt.subplots(figsize=(6, 4))
    bars = ax.bar([str(v) for v in x], secs, color=BLUE, width=0.6)
    for b, s in zip(bars, secs):
        ax.text(b.get_x() + b.get_width() / 2, s + 8, f"{s:.0f}s",
                ha="center", fontsize=9)
    ax.set_xlabel("Spark task slots")
    ax.set_ylabel("Job wall-clock (s)")
    ax.set_title("Batch job runtime vs parallelism — sequential vs parallel")
    ax.grid(alpha=0.3, axis="y")
    fig.tight_layout()
    fig.savefig(OUT / "fig2_batch_runtime.png", dpi=200)
    plt.close(fig)

    fig, ax = plt.subplots(figsize=(6, 4))
    eff = [float(v["efficiency"]) for v in r]
    ax.plot(x, eff, marker="s", lw=2, color=RED)
    ax.axhline(1.0, ls="--", color=GREY, lw=1)
    ax.set_xlabel("Spark task slots")
    ax.set_ylabel("Parallel efficiency  $S_p / p$")
    ax.set_title("Parallel efficiency — diminishing returns")
    ax.set_xticks(x)
    ax.set_ylim(0, 1.15)
    ax.grid(alpha=0.3)
    fig.tight_layout()
    fig.savefig(OUT / "fig3_efficiency.png", dpi=200)
    plt.close(fig)


def fig_throughput():
    r = [v for v in rows("loadgen_runs.csv") if float(v["rate"]) > 0]
    if not r:
        return
    agg = {}
    for v in r:
        k = int(float(v["rate"]))
        agg.setdefault(k, []).append(v)
    offered = sorted(agg)
    achieved = [max(float(v["throughput_eps"]) for v in agg[k]) for k in offered]

    fig, ax = plt.subplots(figsize=(6, 4))
    ax.plot(offered, achieved, marker="o", lw=2, color=BLUE, label="achieved")
    ax.plot(offered, offered, ls="--", color=GREY, label="offered (ideal)")
    ax.set_xlabel("Offered ingestion rate (events/s)")
    ax.set_ylabel("Achieved throughput (events/s)")
    ax.set_title("Ingestion throughput: offered vs achieved (2 shards)")
    ax.grid(alpha=0.3)
    ax.legend()
    fig.tight_layout()
    fig.savefig(OUT / "fig4_throughput_offered_vs_achieved.png", dpi=200)
    plt.close(fig)


def fig_shards():
    labels = ["laptop\n2 shards", "EC2 in-region\n4 shards"]
    thr = [1276.3, 3063.5]
    lat = [1563.3, 48.3]

    fig, (a1, a2) = plt.subplots(1, 2, figsize=(9, 4))
    b = a1.bar(labels, thr, color=[GREY, BLUE], width=0.55)
    for bb, v in zip(b, thr):
        a1.text(bb.get_x() + bb.get_width() / 2, v + 60, f"{v:,.0f}", ha="center", fontsize=10)
    a1.set_ylabel("Throughput (events/s)")
    a1.set_title("Ingest throughput")
    a1.grid(alpha=0.3, axis="y")

    b = a2.bar(labels, lat, color=[GREY, BLUE], width=0.55)
    for bb, v in zip(b, lat):
        a2.text(bb.get_x() + bb.get_width() / 2, v + 30, f"{v:,.0f} ms", ha="center", fontsize=10)
    a2.set_ylabel("put_records latency p50 (ms)")
    a2.set_title("Write latency (log scale)")
    a2.set_yscale("log")
    a2.grid(alpha=0.3, axis="y")

    fig.suptitle("Effect of resharding and in-region placement (measured)")
    fig.tight_layout()
    fig.savefig(OUT / "fig5_shards_and_placement.png", dpi=200)
    plt.close(fig)


if __name__ == "__main__":
    fig_speedup()
    fig_throughput()
    fig_shards()
    for p in sorted(OUT.glob("*.png")):
        print(f"  {p.name}")
    print(f"\nfigures in {OUT}")
