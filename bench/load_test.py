"""Stage 6a: load test.  python3 bench/load_test.py --clients 10,50,100 --duration 12

For each client count: start a FRESH 3-node cluster (real processes), then run N concurrent
client threads for `duration` seconds. Each client loops over a realistic mix of operations:
    60% read one report   10% filtered list   15% submit a report   15% update (GET then PATCH)
Updates target 5 shared "hot" reports, so clients really do collide: HTTP 409 is the system
CORRECTLY rejecting a stale edit, so it counts as a successful, correct response.
Clients are spread over all 3 nodes (client i starts on node i % 3), so follower->leader
forwarding is part of what is measured.

Metrics: per-request latency (avg/p50/p95/p99), throughput (completed HTTP requests/s),
success rate (correct answers / all requests), plus an integrity check at the end
(all 3 nodes hold identical data and the WAL length equals the number of acknowledged writes).
"""
import argparse
import csv
import json
import math
import os
import platform
import random
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from bench.proc_cluster import ProcessCluster, http_json          # noqa: E402
from csn.client import Client, ApiError                             # noqa: E402

LOCATIONS = [f"Zone-{k}" for k in range(20)]
HOT = 5


def pct(sorted_vals, p):
    if not sorted_vals:
        return float("nan")
    return sorted_vals[max(0, math.ceil(p / 100 * len(sorted_vals)) - 1)]


def lat_stats(vals):
    v = sorted(vals)
    return {"avg_ms": round(sum(v) / len(v), 2) if v else None, "p50_ms": round(pct(v, 50), 2),
            "p95_ms": round(pct(v, 95), 2), "p99_ms": round(pct(v, 99), 2),
            "max_ms": round(v[-1], 2) if v else None}


def worker(i, urls, stop_at, hot_ids, samples, acked, retries):
    """One simulated user. samples: (kind, start_time, latency_ms, outcome)."""
    rng = random.Random(i)
    c = Client(urls[i % 3:] + urls[:i % 3], timeout=10)       # rotate so clients start on different nodes
    mine = []

    def timed(kind, fn):
        t0 = time.time()
        try:
            r = fn(); outcome = "ok"
        except ApiError as e:
            r = e.body; outcome = "conflict" if e.status == 409 else "error"
        except Exception:
            r = None; outcome = "error"
        mine.append((kind, t0, (time.time() - t0) * 1000, outcome))
        return r, outcome

    while time.time() < stop_at:
        x = rng.random(); rid = rng.choice(hot_ids)
        if x < 0.60:
            timed("read", lambda: c.get(rid))
        elif x < 0.70:
            timed("list", lambda: c.list(location="Hotspot"))
        elif x < 0.85:
            r, o = timed("submit", lambda: c.submit(rng.choice(["power", "water", "road"]),
                                                    rng.choice(LOCATIONS), "load test", f"user{i}"))
            if o == "ok":
                acked.append(1)
        else:
            cur, o = timed("read", lambda: c.get(rid))
            if o == "ok":
                r, o = timed("update", lambda: c.update(rid, f"user{i}", cur["version"],
                                                        description=f"edit {rng.random():.6f}"))
                if o == "ok":
                    acked.append(1)
    samples.extend(mine)
    retries.append((c.stats["retries"], c.retry_reasons))   # how often / why the client library retried


def run_one(n_clients, duration, logs_dir=None):
    cl = ProcessCluster()
    try:
        cl.start_all()
        cl.wait_ready(leader="N1")
        seed = Client(cl.urls())
        hot_ids = [seed.submit("power", "Seed", f"hot {k}", "seed")["id"] for k in range(HOT)]
        for k in range(3):
            seed.submit("water", "Hotspot", f"filter target {k}", "seed")
        seeded = HOT + 3
        samples, acked, retries = [], [], []
        stop_at = time.time() + duration
        t0 = time.time()
        ts = [threading.Thread(target=worker, args=(i, cl.urls(), stop_at, hot_ids, samples, acked, retries))
              for i in range(n_clients)]
        [t.start() for t in ts]; [t.join() for t in ts]
        elapsed = time.time() - t0

        # ---- integrity check: give background replication a moment, then compare nodes ----
        time.sleep(1.5)
        healths = [cl.health(n, 2) for n in cl.nodes]
        seqs = [h["last_seq"] for h in healths]
        views = [http_json(cl.url(n) + "/reports", 10) for n in cl.nodes]
        consistent = views[0] == views[1] == views[2] and len(set(seqs)) == 1
        wal_matches_acks = seqs[0] == seeded + len(acked)

        done = [s for s in samples if s[3] in ("ok", "conflict")]
        res = {
            "clients": n_clients, "duration_s": round(elapsed, 1),
            "requests": len(samples), "throughput_rps": round(len(done) / elapsed, 1),
            "success_rate_pct": round(100 * len(done) / len(samples), 2),
            "ok_pct": round(100 * sum(1 for s in samples if s[3] == "ok") / len(samples), 2),
            "conflict_409_pct": round(100 * sum(1 for s in samples if s[3] == "conflict") / len(samples), 2),
            "errors": sum(1 for s in samples if s[3] == "error"),
            **lat_stats([s[2] for s in done]),
            "by_kind": {k: {"count": sum(1 for s in done if s[0] == k),
                            **lat_stats([s[2] for s in done if s[0] == k])}
                        for k in ("read", "list", "submit", "update")},
            "client_retries": sum(r[0] for r in retries),
            "retry_reasons": {k: sum(r[1].get(k, 0) for r in retries) for k in {k for r in retries for k in r[1]}},
            "acked_writes": len(acked), "wal_seq_after": seqs[0], "wal_extra_entries": seqs[0] - seeded - len(acked),
            "nodes_consistent": consistent, "wal_equals_acked_writes": wal_matches_acks,
        }
        return res
    finally:
        if logs_dir:
            cl.save_logs(logs_dir)       # node logs = evidence (elections, catch-ups)
        cl.stop()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--clients", default="10,50,100")
    ap.add_argument("--duration", type=float, default=12)
    ap.add_argument("--out", default="results")
    a = ap.parse_args()
    os.makedirs(a.out, exist_ok=True)
    runs = []
    for n in [int(x) for x in a.clients.split(",")]:
        print(f"--- {n} concurrent clients for {a.duration:g}s ...", flush=True)
        r = run_one(n, a.duration, f"{a.out}/logs/load_{n}clients")
        runs.append(r)
        print(f"    {r['requests']} requests, {r['throughput_rps']} req/s, avg {r['avg_ms']} ms, "
              f"p95 {r['p95_ms']} ms, success {r['success_rate_pct']}% "
              f"(409s {r['conflict_409_pct']}%), errors {r['errors']}, "
              f"consistent={r['nodes_consistent']}, wal==acks={r['wal_equals_acked_writes']}", flush=True)
    meta = {"date": time.strftime("%Y-%m-%d %H:%M"), "python": platform.python_version(),
            "platform": platform.platform(), "cpus": os.cpu_count(),
            "note": "load generator and all 3 nodes share one machine; use numbers comparatively"}
    with open(f"{a.out}/load_results.json", "w") as f:
        json.dump({"meta": meta, "runs": runs}, f, indent=2)
    cols = ["clients", "requests", "throughput_rps", "avg_ms", "p50_ms", "p95_ms", "p99_ms", "max_ms",
            "success_rate_pct", "conflict_409_pct", "errors", "nodes_consistent", "wal_equals_acked_writes"]
    with open(f"{a.out}/load_results.csv", "w", newline="") as f:
        w = csv.writer(f); w.writerow(cols)
        for r in runs: w.writerow([r[c] for c in cols])
    lines = ["| Clients | Requests | Throughput (req/s) | Avg (ms) | p95 (ms) | p99 (ms) | Success rate | 409 conflicts | Errors | Nodes identical |",
             "|---:|---:|---:|---:|---:|---:|---:|---:|---:|:---:|"]
    for r in runs:
        lines.append(f"| {r['clients']} | {r['requests']} | {r['throughput_rps']} | {r['avg_ms']} | {r['p95_ms']} | "
                     f"{r['p99_ms']} | {r['success_rate_pct']}% | {r['conflict_409_pct']}% | {r['errors']} | "
                     f"{'yes' if r['nodes_consistent'] and r['wal_equals_acked_writes'] else 'NO'} |")
    table = "\n".join(lines)
    with open(f"{a.out}/load_results.md", "w") as f:
        f.write(f"Load test, {meta['date']}, {meta['cpus']} CPUs, Python {meta['python']}\n\n{table}\n")
    print("\n" + table)


if __name__ == "__main__":
    main()
