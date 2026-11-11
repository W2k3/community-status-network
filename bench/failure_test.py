"""Stage 6b: failure test.   python3 bench/failure_test.py --trials 3

Each trial uses a fresh 3-node cluster of real processes and does this:
  1. Light background load (5 client threads) so the cluster is busy when the leader dies.
  2. SIGKILL the leader N1 (a hard crash: no shutdown code runs).
  3. Measure, from the moment of the kill:
       - election time          : until N2 reports itself leader
       - write unavailability   : until a write sent to follower N2 succeeds again
         (probe sends 1 write / 50 ms with NO retries, so it sees the raw outage)
       - client-visible latency : one write by a normal client (retry + backoff + failover)
                                  that starts 0.1 s after the kill
       - read availability      : failed reads on follower N3 during the outage (expect 0)
       - lost writes            : every write acknowledged to ANY client must still exist
  4. Keep N1 dead, write a backlog of 300 reports through the new leader.
  5. Restart N1 and measure recovery: time until it answers requests (WAL replayed), and until it
     has caught up with the leader (catch-up), and check all 3 WALs are identical at the end.
"""
import argparse
import json
import os
import platform
import statistics
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from bench.proc_cluster import ProcessCluster, http_json          # noqa: E402
from csn.client import Client, ApiError                             # noqa: E402

BACKLOG = 300


def trial(n, background=5, logs_dir=None):
    cl = ProcessCluster()
    try:
        cl.start_all()
        cl.wait_ready(leader="N1")
        seed = Client(cl.urls())
        seed.submit("power", "Seed", "seed", "seed")
        stop = threading.Event()
        acked = []                       # (report id, version) for every acknowledged write
        bg_stats = {"requests": 0, "failed": 0}
        lock = threading.Lock()

        def background_client(i):
            c = Client(cl.urls()[i % 3:] + cl.urls()[:i % 3], timeout=8)
            k = 0
            while not stop.is_set():
                k += 1
                try:
                    r = c.submit("water", f"BG{i}", f"bg {k}", f"bg{i}")
                    with lock:
                        acked.append((r["id"], r["version"]))
                    cur = c.get(r["id"])
                    r2 = c.update(r["id"], f"bg{i}", cur["version"], description="bg edit")
                    with lock:
                        acked.append((r2["id"], r2["version"]))
                except ApiError:
                    pass                  # 409 etc. is a correct answer, not an outage
                except Exception:
                    with lock:
                        bg_stats["failed"] += 1
                with lock:
                    bg_stats["requests"] += 1
                time.sleep(0.05)

        write_probe, read_probe = [], []   # (absolute time, ok)

        def probe(kind, client_fn, out):
            while not stop.is_set():
                t = time.time()
                try:
                    client_fn(); ok = True
                except Exception:
                    ok = False
                out.append((t, ok))
                time.sleep(max(0, 0.05 - (time.time() - t)))

        w_client = Client([cl.url("N2")], timeout=1.0, max_attempts=1)    # raw, no retries
        r_client = Client([cl.url("N3")], timeout=1.0, max_attempts=1)
        threads = [threading.Thread(target=background_client, args=(i,)) for i in range(background)]
        threads += [threading.Thread(target=probe, args=("w", lambda: w_client.submit("road", "Probe", "p", "probe"), write_probe)),
                    threading.Thread(target=probe, args=("r", lambda: r_client.get("R1"), read_probe))]
        [t.start() for t in threads]
        time.sleep(3.0)                    # steady state

        # ---- watch for the election result ----
        marks = {}

        def watch():
            while "agreed" not in marks and not stop.is_set():
                h2, h3 = cl.health("N2", 0.3), cl.health("N3", 0.3)
                now = time.time()
                if h2 and h2["leader"] == "N2" and h2["role"] == "leader":
                    marks.setdefault("elected", now)
                if h3 and h3["leader"] == "N2" and "elected" in marks:
                    marks["agreed"] = now
                time.sleep(0.02)

        wt = threading.Thread(target=watch); wt.start()
        user = {}

        def user_client():                 # a normal user: starts on dead N1, uses retry/backoff/failover
            time.sleep(0.1)
            c = Client(cl.urls(), timeout=5)
            t0 = time.time()
            try:
                c.submit("power", "User", "written during failover", "user")
                user["ok"] = True
            except Exception as e:
                user["ok"] = False; user["err"] = str(e)
            user["latency_s"] = time.time() - t0
            user["retries"], user["failovers"] = c.stats["retries"], c.stats["failovers"]

        ut = threading.Thread(target=user_client)
        t_kill = time.time()
        cl.kill("N1")                      # <-- SIGKILL the leader
        ut.start()
        time.sleep(9.0)                    # let the failover complete and run a while
        stop.set()
        [t.join() for t in threads]; wt.join(); ut.join()

        after = [t for t, ok in write_probe if t > t_kill]
        first_ok_after = next((t for t, ok in write_probe if t > t_kill and ok), None)
        first_fail_after = next((t for t, ok in write_probe if t > t_kill and not ok), None)
        read_failures = sum(1 for t, ok in read_probe if t > t_kill and not ok)
        probe_writes_failed = sum(1 for t, ok in write_probe if t > t_kill and not ok)

        # ---- no acknowledged write may be lost ----
        time.sleep(1.0)
        have = {r["id"]: r["version"] for r in http_json(cl.url("N3") + "/reports", 10)}
        lost = [(i, v) for i, v in acked if have.get(i, 0) < v]

        # ---- backlog while N1 is down, then restart N1 ----
        c2 = Client([cl.url("N2"), cl.url("N3")])
        for k in range(BACKLOG):
            c2.submit("road", "Backlog", f"b{k}", "backlog")
        leader_seq = cl.health("N2")["last_seq"]
        t_spawn = cl.start("N1")
        t_serving = t_caught = None
        n1_seq_at_first_response = None
        while time.time() - t_spawn < 30:
            h = cl.health("N1", 0.3)
            if h and t_serving is None:
                t_serving = time.time(); n1_seq_at_first_response = h["last_seq"]
            if h and h["last_seq"] >= leader_seq and h["leader"] == "N2":
                t_caught = time.time(); break
            time.sleep(0.02)
        h1 = cl.health("N1", 1)
        time.sleep(1.0)
        wals = [cl.wal_entries(x) for x in ("N1", "N2", "N3")]
        res = {
            "election_s": round(marks["elected"] - t_kill, 2) if "elected" in marks else None,
            "followers_agree_s": round(marks["agreed"] - t_kill, 2) if "agreed" in marks else None,
            "write_unavailable_s": round(first_ok_after - t_kill, 2) if first_ok_after else None,
            "write_probes_failed": probe_writes_failed,
            "user_write_latency_s": round(user["latency_s"], 2), "user_write_ok": user["ok"],
            "user_retries": user["retries"], "user_failovers": user["failovers"],
            "read_probe_failures": read_failures,
            "background_requests": bg_stats["requests"], "background_failed": bg_stats["failed"],
            "acknowledged_writes": len(acked), "lost_acknowledged_writes": len(lost),
            "restart_to_serving_s": round(t_serving - t_spawn, 2) if t_serving else None,
            "restart_to_caught_up_s": round(t_caught - t_spawn, 2) if t_caught else None,
            "entries_behind_on_restart": leader_seq - (n1_seq_at_first_response or 0),
            "n1_role_after_restart": ("leader" if h1 and h1["role"] == "leader" else "follower"),
            "wals_identical": wals[0] == wals[1] == wals[2],
            "final_wal_length": len(wals[0]),
            "timeline": {  # seconds relative to the kill, for the chart
                "write": [[round(t - t_kill, 3), ok] for t, ok in write_probe if -1.5 < t - t_kill < 7],
                "read": [[round(t - t_kill, 3), ok] for t, ok in read_probe if -1.5 < t - t_kill < 7],
                "elected": round(marks["elected"] - t_kill, 3) if "elected" in marks else None,
                "first_ok": round(first_ok_after - t_kill, 3) if first_ok_after else None},
        }
        if logs_dir:
            cl.save_logs(logs_dir)
        return res
    finally:
        cl.stop()


def mean(vals):
    vals = [v for v in vals if v is not None]
    return round(statistics.mean(vals), 2) if vals else None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--trials", type=int, default=3)
    ap.add_argument("--out", default="results")
    a = ap.parse_args()
    os.makedirs(a.out, exist_ok=True)
    trials = []
    for i in range(1, a.trials + 1):
        print(f"--- trial {i}/{a.trials}: kill the leader under load ...", flush=True)
        r = trial(i, logs_dir=f"{a.out}/logs/failure_trial{i}")
        trials.append(r)
        print(f"    election {r['election_s']}s | write outage {r['write_unavailable_s']}s | "
              f"user write took {r['user_write_latency_s']}s (retries {r['user_retries']}) | "
              f"read failures {r['read_probe_failures']} | lost acked writes {r['lost_acknowledged_writes']}/"
              f"{r['acknowledged_writes']} | restart: serving {r['restart_to_serving_s']}s, caught up "
              f"{r['restart_to_caught_up_s']}s ({r['entries_behind_on_restart']} entries) | "
              f"N1 rejoined as {r['n1_role_after_restart']} | WALs identical {r['wals_identical']}", flush=True)
    keys = ["election_s", "write_unavailable_s", "user_write_latency_s", "restart_to_serving_s", "restart_to_caught_up_s"]
    summary = {k: {"mean": mean([t[k] for t in trials]),
                   "min": min(t[k] for t in trials if t[k] is not None),
                   "max": max(t[k] for t in trials if t[k] is not None)} for k in keys}
    meta = {"date": time.strftime("%Y-%m-%d %H:%M"), "python": platform.python_version(),
            "platform": platform.platform(), "heartbeat_s": 1.0, "dead_after_s": 3.0, "backlog_entries": BACKLOG}
    with open(f"{a.out}/failure_results.json", "w") as f:
        json.dump({"meta": meta, "summary": summary, "trials": trials}, f, indent=2)
    L = ["| Trial | Election (s) | Write outage (s) | User write latency (s) | Read failures | Lost acked writes | Restart: serving (s) | Restart: caught up (s) | Entries behind | WALs identical |",
         "|---:|---:|---:|---:|---:|---:|---:|---:|---:|:---:|"]
    for i, t in enumerate(trials, 1):
        L.append(f"| {i} | {t['election_s']} | {t['write_unavailable_s']} | {t['user_write_latency_s']} | "
                 f"{t['read_probe_failures']} | {t['lost_acknowledged_writes']}/{t['acknowledged_writes']} | "
                 f"{t['restart_to_serving_s']} | {t['restart_to_caught_up_s']} | {t['entries_behind_on_restart']} | "
                 f"{'yes' if t['wals_identical'] else 'NO'} |")
    L.append(f"| **mean** | {summary['election_s']['mean']} | {summary['write_unavailable_s']['mean']} | "
             f"{summary['user_write_latency_s']['mean']} | | | {summary['restart_to_serving_s']['mean']} | "
             f"{summary['restart_to_caught_up_s']['mean']} | | |")
    table = "\n".join(L)
    with open(f"{a.out}/failure_results.md", "w") as f:
        f.write(f"Failure test (leader SIGKILL under load), {meta['date']}, heartbeat 1s, dead after 3s\n\n{table}\n")
    print("\n" + table)


if __name__ == "__main__":
    main()
