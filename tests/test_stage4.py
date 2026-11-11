"""Stage 4 tests: heartbeats, election, client failover, WAL replay + catch-up.
Run:  python3 -m unittest tests.test_stage4 -v      (uses 0.2s heartbeats / 0.6s timeout)"""
import time
import unittest

from csn.client import Client, ApiError
from tests.cluster_util import FastCluster


class Stage4Test(unittest.TestCase):
    def setUp(self):
        self.cl = FastCluster()
        self.cl.wait_leader("N1")                       # cold start: N1 is the initial leader
    def tearDown(self): self.cl.stop()

    def seqs(self, *ids):
        return [self.cl.node(i).store.wal.last_seq for i in ids]

    def test_leader_failure_elects_lowest_live_node(self):
        self.cl.kill("N1")
        self.cl.wait_leader("N2", ["N2", "N3"])         # lowest live ID wins
        self.assertTrue(self.cl.node("N2").is_leader)
        self.assertEqual(self.cl.node("N2").term, 2)

    def test_client_fails_over_and_writes_succeed(self):
        c = self.cl.client()                            # node list [N1, N2, N3], starts with N1
        c.submit("power", "A", "before", "u")
        self.cl.kill("N1")                              # leader dies; client doesn't know yet
        r = c.submit("power", "B", "after", "u")        # retries + backoff until N2 is elected
        self.assertEqual(r["id"], "R2")
        self.assertGreaterEqual(c.stats["failovers"], 1)
        self.assertNotEqual(c.current_node, self.cl.client().nodes[0])   # moved off N1
        self.assertEqual({x["id"] for x in c.list()}, {"R1", "R2"})      # nothing lost

    def test_acknowledged_write_survives_leader_crash(self):
        c = self.cl.client("N1")
        for k in range(5):
            c.submit("water", f"L{k}", "d", "u")
        self.cl.kill("N1")
        self.cl.wait_leader("N2", ["N2", "N3"])
        self.assertEqual(len(self.cl.client("N3").list()), 5)

    def test_follower_restart_replays_wal_then_catches_up(self):
        c = self.cl.client("N1")
        c.submit("power", "A", "a", "u"); c.submit("power", "B", "b", "u")
        self.cl.wait(lambda: self.seqs("N3") == [2], what="N3 has 2 entries")
        self.cl.kill("N3")
        for k in range(4):                              # written while N3 is down
            c.submit("road", f"C{k}", "c", "u")
        n3 = self.cl.start("N3").node                   # restart: replay WAL (2), then catch up (+4)
        self.assertEqual(n3.store.wal.last_seq >= 2, True)
        self.cl.wait(lambda: self.seqs("N3") == [6], what="N3 caught up to seq 6")
        self.assertEqual(self.cl.node("N3").store.wal.entries, self.cl.node("N1").store.wal.entries)
        self.assertEqual(len(self.cl.client("N3").list()), 6)

    def test_old_leader_rejoins_as_follower_and_catches_up(self):
        c = self.cl.client("N2")
        c.submit("power", "A", "a", "u")
        self.cl.kill("N1")
        self.cl.wait_leader("N2", ["N2", "N3"])
        c.submit("power", "B", "b", "u"); c.submit("power", "C", "c", "u")   # N1 misses these
        n1 = self.cl.start("N1").node
        self.assertEqual(n1.leader_id, "N2")            # did NOT reclaim leadership
        self.assertFalse(n1.is_leader)
        self.cl.wait(lambda: self.seqs("N1") == [3], what="N1 caught up")
        r = self.cl.client("N1").submit("power", "D", "d", "u")   # follower forwards to N2
        self.assertEqual(r["id"], "R4")

    def test_diverged_old_leader_is_resynced(self):
        """N1 holds a write that was never acknowledged (no majority). After the others move on,
        N1's log disagrees with theirs at the same seq number; on rejoin it must resync."""
        c = self.cl.client("N1", max_attempts=1)
        c.submit("power", "A", "a", "u")
        self.cl.wait(lambda: self.seqs("N2", "N3") == [1, 1], what="replicated")
        self.cl.kill("N2"); self.cl.kill("N3")
        with self.assertRaises(ApiError) as cm:
            c.submit("power", "LOST", "never acked", "u")   # leader alone: 503, but in N1's own WAL
        self.assertEqual(cm.exception.status, 503)
        self.assertEqual(self.seqs("N1"), [2])
        self.cl.kill("N1")
        self.cl.start("N2"); self.cl.start("N3")
        self.cl.wait_leader("N2", ["N2", "N3"])
        self.cl.client("N2").submit("water", "REAL", "acked write", "u")     # their seq 2 != N1's seq 2
        self.cl.start("N1")
        self.cl.wait(lambda: self.cl.node("N1").store.wal.entries == self.cl.node("N2").store.wal.entries,
                     what="N1 resynced to N2's history")
        locs = {r["location"] for r in self.cl.client("N1").list()}
        self.assertEqual(locs, {"A", "REAL"})           # the unacknowledged "LOST" write is gone

    def test_stale_term_replication_is_refused(self):
        self.cl.kill("N1")
        self.cl.wait_leader("N2", ["N2", "N3"])         # term 2
        n3 = self.cl.node("N3")
        stale = {"entry": {"seq": 99, "op": "create_report", "payload": {}, "node_id": "N1"},
                 "term": 1, "leader": "N1"}
        self.assertFalse(n3.accept_replication(stale))

    def test_client_gives_up_when_everything_is_down(self):
        c = self.cl.client(max_attempts=3, backoff=0.05)
        for n in ("N1", "N2", "N3"):
            self.cl.kill(n)
        with self.assertRaises(ConnectionError):
            c.list()


if __name__ == "__main__":
    unittest.main()
