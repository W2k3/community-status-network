"""Stage 6c: charts from results/*.json.   .venv-charts/bin/python bench/charts.py

matplotlib is the ONLY non-stdlib dependency in the project and is used here only, to draw
charts from the recorded results (the system itself never imports it).

Colours: categorical slots 1-2 of the validated reference palette (blue, orange), light surface.
Failures are shown with a different SHAPE (x) as well as a different colour, never colour alone.
"""
import json
import os
import sys

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

RES = sys.argv[1] if len(sys.argv) > 1 else "results"
BLUE, ORANGE, AQUA, RED = "#2a78d6", "#eb6834", "#1baf7a", "#e34948"
SURFACE, INK, MUTED, GRID = "#fcfcfb", "#0b0b0b", "#52514e", "#e6e5e1"

plt.rcParams.update({
    "figure.facecolor": SURFACE, "axes.facecolor": SURFACE, "savefig.facecolor": SURFACE,
    "text.color": INK, "axes.labelcolor": MUTED, "xtick.color": MUTED, "ytick.color": MUTED,
    "axes.edgecolor": GRID, "axes.spines.top": False, "axes.spines.right": False,
    "font.size": 11, "axes.titlesize": 13, "axes.titleweight": "bold", "axes.titlelocation": "left",
    "axes.grid": True, "grid.color": GRID, "grid.linewidth": 0.8, "axes.axisbelow": True,
    "legend.frameon": False,
})


def label_bars(ax, bars, fmt):
    for b in bars:
        ax.annotate(fmt(b.get_height()), (b.get_x() + b.get_width() / 2, b.get_height()),
                    ha="center", va="bottom", xytext=(0, 3), textcoords="offset points",
                    fontsize=10, color=INK)


def save(fig, name):
    path = os.path.join(RES, name)
    fig.savefig(path, dpi=160, bbox_inches="tight")
    plt.close(fig)
    print("wrote", path)


def load_charts(data):
    runs = data["runs"]
    xs = [str(r["clients"]) for r in runs]
    n = len(runs)
    first, last = runs[0], runs[-1]

    # 1. latency: average and p95 per client count
    fig, ax = plt.subplots(figsize=(7, 4.2))
    w = 0.38
    pos = range(n)
    b1 = ax.bar([p - w / 2 for p in pos], [r["avg_ms"] for r in runs], w - 0.04, color=BLUE, label="average", zorder=3)
    b2 = ax.bar([p + w / 2 for p in pos], [r["p95_ms"] for r in runs], w - 0.04, color=ORANGE, label="95th percentile", zorder=3)
    label_bars(ax, b1, lambda v: f"{v:.1f}"); label_bars(ax, b2, lambda v: f"{v:.0f}")
    ax.set_xticks(list(pos)); ax.set_xticklabels(xs)
    ax.set_xlabel("concurrent clients"); ax.set_ylabel("response time (ms)")
    ax.set_title(f"p95 latency goes from {first['p95_ms']:.0f} ms to {last['p95_ms']:.0f} ms "
                 f"as clients rise {first['clients']} to {last['clients']}")
    ax.legend(loc="upper left")
    save(fig, "load_latency.png")

    # 2. throughput
    fig, ax = plt.subplots(figsize=(7, 4.2))
    b = ax.bar(xs, [r["throughput_rps"] for r in runs], 0.5, color=BLUE, zorder=3)
    label_bars(ax, b, lambda v: f"{v:,.0f}")
    ax.set_xlabel("concurrent clients"); ax.set_ylabel("completed requests per second")
    ax.set_title("Throughput (all operations, 3 nodes on one laptop)")
    save(fig, "load_throughput.png")

    # 3. success rate: correct answers vs conflicts vs errors
    fig, ax = plt.subplots(figsize=(7, 4.2))
    ok = [r["ok_pct"] for r in runs]; conf = [r["conflict_409_pct"] for r in runs]
    err = [round(100 - r["success_rate_pct"], 2) for r in runs]
    ax.bar(xs, ok, 0.5, color=BLUE, label="2xx success", zorder=3)
    ax.bar(xs, conf, 0.5, bottom=[o + 0.15 for o in ok], color=AQUA, label="409 conflict (stale edit rejected: correct)", zorder=3)
    if any(err):
        ax.bar(xs, err, 0.5, bottom=[o + c + 0.3 for o, c in zip(ok, conf)], color=RED, label="errors", zorder=3)
    for i, r in enumerate(runs):
        ax.annotate(f"{r['success_rate_pct']:.1f}% correct", (i, 100), ha="center", va="bottom",
                    xytext=(0, 3), textcoords="offset points", fontsize=10)
    ax.set_ylim(0, 112); ax.set_xlabel("concurrent clients"); ax.set_ylabel("% of requests")
    ax.set_title("Every request got a correct answer; 409s are intended")
    ax.legend(loc="lower center", bbox_to_anchor=(0.5, -0.38), ncol=1)
    save(fig, "load_success.png")


def failure_charts(data):
    trials = data["trials"]
    # 4. timeline of probe requests around the kill (trial 1)
    t = trials[0]["timeline"]
    fig, ax = plt.subplots(figsize=(8, 3.6))
    for row, key, name in ((1, "write", "write via follower N2"), (0, "read", "read via follower N3")):
        oks = [x for x, ok in t[key] if ok]; bad = [x for x, ok in t[key] if not ok]
        ax.scatter(oks, [row] * len(oks), s=14, color=BLUE, zorder=3, label="request succeeded" if row == 1 else None)
        ax.scatter(bad, [row] * len(bad), s=34, marker="x", color=RED, linewidths=1.6, zorder=4,
                   label="request failed" if row == 1 else None)
    ax.axvline(0, color=MUTED, ls="--", lw=1)
    ax.annotate("leader N1 killed", (0, 1.55), ha="right", xytext=(-4, 0), textcoords="offset points", fontsize=10, color=MUTED)
    if t["elected"] is not None:
        ax.axvline(t["elected"], color=ORANGE, ls="--", lw=1.4)
        ax.annotate("N2 elected", (t["elected"], 1.55), ha="left", xytext=(4, 0), textcoords="offset points", fontsize=10, color=INK)
    ax.set_yticks([0, 1]); ax.set_yticklabels(["read via N3", "write via N2"]); ax.set_ylim(-0.5, 1.8)
    ax.set_xlabel("seconds relative to the kill")
    out = trials[0]["write_unavailable_s"]
    ax.set_title(f"Writes were unavailable for {out:.1f} s after the leader died; reads never failed")
    ax.grid(axis="y", visible=False)
    ax.legend(loc="upper center", bbox_to_anchor=(0.5, -0.2), ncol=2)
    save(fig, "failover_timeline.png")

    # 5. per-trial failover and recovery times
    fig, (a1, a2) = plt.subplots(1, 2, figsize=(10, 4.2), gridspec_kw={"width_ratios": [3, 2]})
    names = [str(i + 1) for i in range(len(trials))]
    pos = range(len(trials)); w = 0.26
    series = [("election_s", "election complete", BLUE), ("write_unavailable_s", "writes available again", ORANGE),
              ("user_write_latency_s", "client write completes (retries)", AQUA)]
    for k, (key, lab, col) in enumerate(series):
        bars = a1.bar([p + (k - 1) * w for p in pos], [tr[key] for tr in trials], w - 0.03, color=col, label=lab, zorder=3)
        label_bars(a1, bars, lambda v: f"{v:.1f}")
    a1.set_xticks(list(pos)); a1.set_xticklabels(names)
    a1.set_xlabel("trial"); a1.set_ylabel("seconds after leader kill")
    a1.set_title("Failover time")
    a1.legend(loc="upper center", bbox_to_anchor=(0.5, -0.18), ncol=1)
    w2 = 0.34
    for k, (key, lab, col) in enumerate((("restart_to_serving_s", "serving requests", BLUE),
                                         ("restart_to_caught_up_s", "caught up with leader", ORANGE))):
        bars = a2.bar([p + (k - 0.5) * w2 for p in pos], [tr[key] for tr in trials], w2 - 0.03, color=col, label=lab, zorder=3)
        label_bars(a2, bars, lambda v: f"{v:.2f}")
    lo = min(tr["entries_behind_on_restart"] for tr in trials)
    hi = max(tr["entries_behind_on_restart"] for tr in trials)
    a2.set_xticks(list(pos)); a2.set_xticklabels(names)
    a2.set_xlabel("trial"); a2.set_ylabel("seconds after restart")
    a2.set_title(f"Restarted node ({lo:,} to {hi:,} entries behind)")
    a2.legend(loc="upper center", bbox_to_anchor=(0.5, -0.18), ncol=1)
    fig.tight_layout()
    save(fig, "failover_trials.png")


if __name__ == "__main__":
    with open(os.path.join(RES, "load_results.json")) as f:
        load_charts(json.load(f))
    fp = os.path.join(RES, "failure_results.json")
    if os.path.exists(fp):
        with open(fp) as f:
            failure_charts(json.load(f))
