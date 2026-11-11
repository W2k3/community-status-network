#!/usr/bin/env python3
"""One-command demo of the whole system.      python3 demo.py

It starts the 3 nodes (ports from config.json, real OS processes), then walks through every
requirement with narration:
   1 submit via a follower (forwarded to the leader)      4 live feed viewer
   2 update + conflicting edit -> HTTP 409                5 kill the leader -> election + client failover
   3 read the same data from every node                   6 restart the old leader -> WAL replay + catch-up

Options:
   --pause      wait for Enter between steps (handy while recording the video)
   --no-keep    exit when the walkthrough ends (default: leave the cluster running so you can
                open http://127.0.0.1:8002/ in a browser or use the CLI; Ctrl-C stops it)
   --start-only just start the cluster and wait (no walkthrough)

Demo data lives in data/demo/ and is wiped at the start of each run.
"""
import argparse
import atexit
import json
import os
import shutil
import socket
import subprocess
import sys
import threading
import time
import urllib.request

ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, ROOT)
from csn.client import Client, ApiError, LiveFeed          # noqa: E402

CONFIG = os.path.join(ROOT, "config.json")
DATA = os.path.join(ROOT, "data", "demo")
with open(CONFIG) as f:
    NODES = json.load(f)["nodes"]
URLS = {n: "http://" + a for n, a in NODES.items()}
procs = {}
USE_COLOR = sys.stdout.isatty()


def c(code, text):
    return f"\033[{code}m{text}\033[0m" if USE_COLOR else text


def say(text):
    print(c("1;36", f"\n=== {text}"), flush=True)


def note(text):
    print(c("2", f"    {text}"), flush=True)


def health(nid, timeout=0.5):
    try:
        with urllib.request.urlopen(URLS[nid] + "/health", timeout=timeout) as r:
            return json.loads(r.read())
    except OSError:
        return None


def start_node(nid):
    out = open(os.path.join(DATA, f"{nid}.out"), "a")
    procs[nid] = subprocess.Popen([sys.executable, "-m", "csn.server", "--id", nid, "--config", CONFIG,
                                   "--data-dir", DATA], cwd=ROOT, stdout=out, stderr=subprocess.STDOUT)


def stop_all():
    for p in procs.values():
        if p.poll() is None:
            p.terminate()
    for p in procs.values():
        try:
            p.wait(timeout=3)
        except subprocess.TimeoutExpired:
            p.kill()


def wait_until(cond, what, timeout=20):
    end = time.time() + timeout
    while time.time() < end:
        v = cond()
        if v:
            return v
        time.sleep(0.05)
    sys.exit(f"timed out waiting for {what}")


def wait_agreed(nodes, leader=None):
    def ok():
        hs = [health(n) for n in nodes]
        return all(hs) and len({h["leader"] for h in hs}) == 1 and hs[0]["leader"] and \
            (leader is None or hs[0]["leader"] == leader) and hs[0]["leader"]
    return wait_until(ok, f"nodes {nodes} to agree on a leader")


def fmt(r):
    return (f"{r['id']} {r['type']:<5} {r['location']:<12} status={r['status']:<8} "
            f"v{r['version']} by {r['updated_by']}")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--pause", action="store_true")
    ap.add_argument("--no-keep", action="store_true")
    ap.add_argument("--start-only", action="store_true")
    a = ap.parse_args()

    def pause():
        if a.pause:
            input(c("2", "    [Enter to continue] "))

    for nid, addr in NODES.items():                      # ports free?
        host, port = addr.rsplit(":", 1)
        with socket.socket() as s:
            if s.connect_ex((host, int(port))) == 0:
                sys.exit(f"port {port} ({nid}) is already in use - stop the other process first")
    shutil.rmtree(DATA, ignore_errors=True)
    os.makedirs(DATA)
    atexit.register(stop_all)

    say("Starting 3 nodes: N1 (initial leader), N2, N3")
    for nid in NODES:
        start_node(nid)
        time.sleep(0.15)                                 # N1 first, so it is the initial leader
    leader = wait_agreed(list(NODES), "N1")
    for nid in NODES:
        h = health(nid)
        note(f"{nid} {URLS[nid]}  role={h['role']}  leader={h['leader']}  term={h['term']}")
    if a.start_only:
        print("\nCluster is up. Ctrl-C to stop.", flush=True)
        return keep_running()

    cli = Client(list(URLS.values()))                    # tries N1, N2, N3 in order with failover
    via_n3 = Client([URLS["N3"]])
    via_n2 = Client([URLS["N2"]])
    via_n1 = Client([URLS["N1"]])
    pause()

    # ---- live feed viewer, attached to the LEADER so we can watch it fail over later ----
    feed = LiveFeed(list(URLS.values()))

    def viewer():
        last_node = None
        for ev in feed.events():
            if feed.current_node != last_node:
                last_node = feed.current_node
                print(c("35", f"    [viewer] connected to {last_node}"), flush=True)
            print(c("35", f"    [viewer] seq {ev['seq']} {ev['op']:<7} {fmt(ev['report'])}"), flush=True)
    threading.Thread(target=viewer, daemon=True).start()
    time.sleep(0.5)

    say("1. A resident submits a report to FOLLOWER N3 (it forwards the write to the leader N1)")
    r = via_n3.submit("water", "Kira", "Borehole is dry", "alice")
    note(f"reply: {fmt(r)}")
    note("the viewer above (attached to N1) saw it appear in real time")
    pause()
    time.sleep(0.3)

    say("2. A moderator verifies it via follower N2")
    r2 = via_n2.update(r["id"], "mod1", r["version"], role="moderator", status="verified")
    note(f"reply: {fmt(r2)}")
    pause()
    time.sleep(0.3)

    say("3. Conflict: bob still holds version 1 and tries to edit -> the leader rejects it with 409")
    try:
        via_n3.update(r["id"], "bob", 1, description="Borehole fixed")
    except ApiError as e:
        note(f"HTTP {e.status}: {e.body['error']}; current version is {e.body['current_version']}")
        note("bob re-reads and retries with the current version:")
        r3 = via_n3.update(r["id"], "bob", e.body["current_version"], description="Borehole fixed")
        note(f"reply: {fmt(r3)}")
    pause()
    time.sleep(0.3)

    say("4. Reads work on any node (each answers from its own copy)")
    for nid in NODES:
        rep = Client([URLS[nid]]).get(r["id"])
        note(f"{nid}: {fmt(rep)}")
    pause()

    say("5. KILL the leader N1 (SIGKILL = a crash). Then a client writes while the cluster recovers")
    t0 = time.time()
    procs["N1"].kill()
    procs["N1"].wait()
    note("N1 is dead. The client library starts on N1, gets 'connection refused', backs off and fails over...")
    r4 = cli.submit("power", "Ntinda", "Power cut since 8am", "carol")
    dt = time.time() - t0
    note(f"write succeeded after {dt:.1f}s: {fmt(r4)}")
    note(f"client stats: {cli.stats['retries']} retries, {cli.stats['failovers']} failovers, "
         f"now using {cli.current_node}")
    for nid in ("N2", "N3"):
        h = health(nid)
        note(f"{nid}: role={h['role']} leader={h['leader']} term={h['term']}")
    note("(3s of silence = leader declared dead; lowest live ID, N2, wins; term 1 -> 2)")
    time.sleep(0.5)
    pause()

    say("6. Restart N1: it replays its WAL, learns N2 is leader, and catches up on what it missed")
    r5 = via_n2.submit("road", "Jinja Rd", "Blocked by a truck", "dave")
    note(f"(written while N1 was down: {fmt(r5)})")
    log_offset = os.path.getsize(os.path.join(DATA, "N1.out"))     # only show lines from the restart
    start_node("N1")
    wait_until(lambda: (health("N1") or {}).get("last_seq") == health("N2")["last_seq"], "N1 catch-up")
    h = health("N1")
    note(f"N1: role={h['role']} leader={h['leader']} last_seq={h['last_seq']}  (rejoined as a FOLLOWER)")
    with open(os.path.join(DATA, "N1.out")) as f:
        f.seek(log_offset)
        for line in f:
            if "caught up" in line or "leader is now" in line or "listening" in line:
                note("N1 log: " + line.strip())
    pause()

    say("7. Proof: all three write-ahead logs are identical")
    wals = []
    for nid in NODES:
        with open(os.path.join(DATA, f"{nid}.wal")) as f:
            wals.append([json.loads(l) for l in f if l.strip()])
    for nid, w in zip(NODES, wals):
        note(f"{nid}.wal: {len(w)} entries, last seq {w[-1]['seq']}")
    note("identical on all nodes: " + c("1;32", str(wals[0] == wals[1] == wals[2])))
    feed.close()

    print(c("1;32", "\nDemo complete."), flush=True)
    if a.no_keep:
        return
    print("Cluster is still running:")
    print(f"  browser viewer : {URLS['N2']}/")
    print(f"  CLI            : python3 -m csn.cli list     (or: submit / update / watch)")
    print("  Ctrl-C to stop everything.", flush=True)
    keep_running()


def keep_running():
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nstopping nodes...", flush=True)


if __name__ == "__main__":
    main()
