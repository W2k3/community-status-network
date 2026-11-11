"""Stage 3 tests: 3 nodes in one process. Run:  python3 -m unittest tests.test_stage3 -v"""
import shutil
import tempfile
import threading
import time
import unittest

from csn.client import Client, ApiError
from csn.server import make_server


class Cluster:
    """Starts N1 (leader), N2, N3 on free ports, each with its own data dir."""
    def __init__(self):
        self.root = tempfile.mkdtemp()
        self.srvs = {i: make_server(i, 0, f"{self.root}/{i}") for i in ("N1", "N2", "N3")}
        nodes = {i: f"127.0.0.1:{s.server_address[1]}" for i, s in self.srvs.items()}
        for i, s in self.srvs.items():            # now that ports are known, wire the cluster
            s.node.nodes = dict(nodes)
            s.node.set_leader("N1", 1)      # fixed leader; heartbeats are not started here
            threading.Thread(target=s.serve_forever, daemon=True).start()
        self.clients = {i: Client("http://" + a, max_attempts=1) for i, a in nodes.items()}

    def kill(self, node_id):
        s = self.srvs[node_id]; s.shutdown(); s.server_close()

    def stop(self):
        for i, s in self.srvs.items():
            try: s.shutdown(); s.server_close()
            except Exception: pass
            s.node.store.wal.close()
        shutil.rmtree(self.root, ignore_errors=True)

    def wait_synced(self, ids, seq, timeout=3):
        """Replication to the 3rd node is background work: poll until it catches up."""
        end = time.time() + timeout
        while time.time() < end:
            if all(self.srvs[i].node.store.wal.last_seq >= seq for i in ids):
                return True
            time.sleep(0.02)
        return False


class Stage3Test(unittest.TestCase):
    def setUp(self): self.cl = Cluster()
    def tearDown(self): self.cl.stop()

    def test_write_on_leader_replicates_everywhere(self):
        r = self.cl.clients["N1"].submit("power", "Ntinda", "no power", "alice")
        self.assertTrue(self.cl.wait_synced(["N2", "N3"], 1))
        for i in ("N2", "N3"):
            self.assertEqual(self.cl.clients[i].get(r["id"])["description"], "no power")

    def test_write_on_follower_is_forwarded(self):
        r = self.cl.clients["N3"].submit("water", "Kira", "dry tap", "bob")   # ask a follower
        self.assertEqual(r["id"], "R1")
        self.assertTrue(self.cl.wait_synced(["N1", "N2", "N3"], 1))
        self.assertEqual(self.cl.srvs["N1"].node.store.wal.last_seq, 1)       # it lives on the leader's log
        self.assertEqual(self.cl.clients["N2"].get("R1")["location"], "Kira")

    def test_409_is_relayed_through_follower(self):
        self.cl.clients["N1"].submit("road", "Jinja Rd", "blocked", "a")
        self.cl.clients["N2"].update("R1", "bob", 1, description="b")
        with self.assertRaises(ApiError) as cm:
            self.cl.clients["N3"].update("R1", "carol", 1, description="stale")
        self.assertEqual(cm.exception.status, 409)
        self.assertEqual(cm.exception.body["current_version"], 2)

    def test_identical_logs_under_concurrent_writes(self):
        """Many concurrent writes via all 3 nodes -> every node's WAL is the same sequence."""
        def worker(i, nid):
            c = self.cl.clients[nid]
            for k in range(5):
                c.submit("power", f"loc{i}", f"d{k}", f"u{i}")
        ts = [threading.Thread(target=worker, args=(i, f"N{1 + i % 3}")) for i in range(6)]
        [t.start() for t in ts]; [t.join() for t in ts]
        self.assertTrue(self.cl.wait_synced(["N1", "N2", "N3"], 30, timeout=5))
        logs = [self.cl.srvs[i].node.store.wal.read_all() for i in ("N1", "N2", "N3")]
        self.assertEqual(len(logs[0]), 30)
        self.assertEqual(logs[0], logs[1]); self.assertEqual(logs[0], logs[2])
        self.assertEqual([e["seq"] for e in logs[0]], list(range(1, 31)))

    def test_one_follower_down_writes_still_succeed(self):
        self.cl.kill("N3")
        r = self.cl.clients["N2"].submit("power", "A", "a", "u")   # majority = leader + N2
        self.assertEqual(r["version"], 1)
        self.assertEqual(self.cl.clients["N2"].get(r["id"])["description"], "a")

    def test_no_majority_returns_503(self):
        self.cl.kill("N2"); self.cl.kill("N3")
        with self.assertRaises(ApiError) as cm:
            self.cl.clients["N1"].submit("power", "A", "a", "u")
        self.assertEqual(cm.exception.status, 503)

    def test_leader_down_follower_write_gives_503(self):
        self.cl.kill("N1")
        with self.assertRaises(ApiError) as cm:
            self.cl.clients["N2"].submit("power", "A", "a", "u")
        self.assertEqual(cm.exception.status, 503)      # Stage 4 adds election to fix this
        self.assertEqual(self.cl.clients["N2"].list(), [])   # reads still work


if __name__ == "__main__":
    unittest.main()
