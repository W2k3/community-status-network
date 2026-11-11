"""Starts the 3 nodes as REAL separate OS processes (like the demo), so benchmarks include
real HTTP, real fsync and real process death (SIGKILL)."""
import json
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import urllib.request

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def free_port():
    s = socket.socket(); s.bind(("127.0.0.1", 0)); p = s.getsockname()[1]; s.close(); return p


def http_json(url, timeout=1.0):
    with urllib.request.urlopen(url, timeout=timeout) as r:
        return json.loads(r.read())


class ProcessCluster:
    def __init__(self, hb_interval=1.0, dead_after=3.0):
        self.root = tempfile.mkdtemp(prefix="csn-bench-")
        self.nodes = {i: f"127.0.0.1:{free_port()}" for i in ("N1", "N2", "N3")}
        self.config = os.path.join(self.root, "config.json")
        with open(self.config, "w") as f:
            json.dump({"nodes": self.nodes, "initial_leader": "N1",
                       "heartbeat_interval": hb_interval, "dead_after": dead_after}, f)
        self.procs = {}
        os.makedirs(f"{self.root}/logs")

    def url(self, nid): return "http://" + self.nodes[nid]
    def urls(self): return [self.url(i) for i in self.nodes]

    def start(self, nid):
        log = open(f"{self.root}/logs/{nid}.log", "a")
        self.procs[nid] = subprocess.Popen(
            [sys.executable, "-m", "csn.server", "--id", nid, "--config", self.config,
             "--data-dir", f"{self.root}/data", "--no-access-log"],
            cwd=REPO, stdout=log, stderr=subprocess.STDOUT)
        return time.time()

    def start_all(self):
        for nid in self.nodes:
            self.start(nid)
            time.sleep(0.15)     # N1 first, so it is the initial leader

    def kill(self, nid):
        """SIGKILL: no shutdown handlers, no flush -- a real crash."""
        p = self.procs.pop(nid)
        p.kill(); p.wait()

    def health(self, nid, timeout=0.5):
        try:
            return http_json(self.url(nid) + "/health", timeout)
        except (OSError, ValueError):
            return None

    def wait_ready(self, nodes=None, leader=None, timeout=20):
        """Wait until every listed node is up and they all agree on one leader."""
        nodes = nodes or list(self.procs)
        end = time.time() + timeout
        while time.time() < end:
            hs = [self.health(n) for n in nodes]
            if all(hs) and len({h["leader"] for h in hs}) == 1 and hs[0]["leader"] is not None \
                    and (leader is None or hs[0]["leader"] == leader):
                return hs[0]["leader"]
            time.sleep(0.1)
        raise RuntimeError(f"cluster not ready: {[self.health(n) for n in nodes]}")

    def wal_entries(self, nid):
        path = f"{self.root}/data/{nid}.wal"
        with open(path) as f:
            return [json.loads(l) for l in f if l.strip()]

    def save_logs(self, dest):
        os.makedirs(dest, exist_ok=True)
        for fn in os.listdir(f"{self.root}/logs"):
            shutil.copy(f"{self.root}/logs/{fn}", dest)

    def stop(self):
        for nid in list(self.procs):
            self.kill(nid)
        shutil.rmtree(self.root, ignore_errors=True)
