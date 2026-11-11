"""Test helper: a 3-node cluster in one process with FAST timings (heartbeat 0.2s, dead after 0.6s),
where nodes can be killed and restarted (a restart re-reads the same data dir = WAL replay)."""
import shutil
import socket
import tempfile
import threading
import time

from csn.client import Client
from csn.server import make_server

HB, DEAD = 0.2, 0.6


def free_port():
    s = socket.socket(); s.bind(("127.0.0.1", 0)); p = s.getsockname()[1]; s.close(); return p


class FastCluster:
    def __init__(self, start=True):
        self.root = tempfile.mkdtemp()
        self.nodes = {i: f"127.0.0.1:{free_port()}" for i in ("N1", "N2", "N3")}
        self.srvs = {}
        if start:
            for i in self.nodes:
                self.start(i)

    def start(self, nid):
        port = int(self.nodes[nid].rsplit(":", 1)[1])
        srv = make_server(nid, port, f"{self.root}/{nid}", self.nodes, "N1",
                          hb_interval=HB, dead_after=DEAD, quiet=True)
        threading.Thread(target=srv.serve_forever, daemon=True).start()
        srv.node.start()
        self.srvs[nid] = srv
        return srv

    def kill(self, nid):
        srv = self.srvs.pop(nid)
        srv.node.stop(); srv.shutdown(); srv.server_close(); srv.node.store.wal.close()

    def client(self, nid=None, **kw):
        urls = ["http://" + a for i, a in self.nodes.items() if nid in (None, i)]
        return Client(urls, **kw)

    def node(self, nid): return self.srvs[nid].node

    def wait(self, cond, timeout=6.0, what="condition"):
        end = time.time() + timeout
        while time.time() < end:
            if cond():
                return True
            time.sleep(0.05)
        raise AssertionError(f"timed out waiting for {what}")

    def wait_leader(self, expected, nodes=None, timeout=6.0):
        nodes = nodes or list(self.srvs)
        self.wait(lambda: all(self.node(n).leader_id == expected for n in nodes),
                  timeout, f"{nodes} to agree leader={expected}")

    def stop(self):
        for nid in list(self.srvs):
            self.kill(nid)
        shutil.rmtree(self.root, ignore_errors=True)
