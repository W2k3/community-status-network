"""Stage 5 tests: SSE live feed.  Run:  python3 -m unittest tests.test_stage5 -v"""
import json
import threading
import time
import unittest
import urllib.request

from csn.client import LiveFeed
from csn.feed import EventBus
from tests.cluster_util import FastCluster


class Collector:
    """Runs a LiveFeed in a background thread and stores what it receives."""
    def __init__(self, feed):
        self.feed, self.events = feed, []
        threading.Thread(target=self._run, daemon=True).start()
    def _run(self):
        for ev in self.feed.events():
            self.events.append(ev)
    def seqs(self): return [e["seq"] for e in self.events]


class Stage5Test(unittest.TestCase):
    def setUp(self):
        self.cl = FastCluster()
        self.cl.wait_leader("N1")
        self.feeds = []
    def tearDown(self):
        for f in self.feeds: f.close()
        self.cl.stop()

    def watch(self, nid=None, **filters):
        urls = ["http://" + a for i, a in self.cl.nodes.items() if nid in (None, i)]
        f = LiveFeed(urls, backoff=0.05, **filters)
        self.feeds.append(f)
        c = Collector(f)
        self.cl.wait(lambda: f.current_node is not None, what="viewer connected")
        return c

    def test_viewer_on_follower_sees_writes_made_on_leader(self):
        v = self.watch("N3")                                    # viewer attached to a FOLLOWER
        w = self.cl.client("N1")
        w.submit("power", "Ntinda", "no power", "alice")
        w.update("R1", "mod", 1, role="moderator", status="verified")
        self.cl.wait(lambda: len(v.events) == 2, what="2 events")
        self.assertEqual([(e["op"], e["report"]["version"], e["served_by"]) for e in v.events],
                         [("created", 1, "N3"), ("updated", 2, "N3")])
        self.assertEqual(v.events[1]["report"]["status"], "verified")

    def test_write_via_follower_reaches_viewer_on_another_follower(self):
        v = self.watch("N2")
        self.cl.client("N3").submit("water", "Kira", "dry", "bob")   # N3 forwards to N1
        self.cl.wait(lambda: len(v.events) == 1, what="event")
        self.assertEqual(v.events[0]["report"]["location"], "Kira")

    def test_filters(self):
        v = self.watch("N1", type="water")
        w = self.cl.client("N1")
        w.submit("power", "A", "a", "u"); w.submit("water", "B", "b", "u"); w.submit("road", "C", "c", "u")
        self.cl.wait(lambda: len(v.events) >= 1, what="water event")
        time.sleep(0.3)
        self.assertEqual([e["report"]["type"] for e in v.events], ["water"])

    def test_reconnect_replays_from_last_event_id(self):
        w = self.cl.client("N1")
        for k in range(4): w.submit("road", f"L{k}", "d", "u")
        self.cl.wait(lambda: self.cl.node("N3").store.wal.last_seq == 4, what="N3 synced")
        req = urllib.request.Request("http://" + self.cl.nodes["N3"] + "/events",
                                     headers={"Last-Event-ID": "2"})
        got = []
        with urllib.request.urlopen(req, timeout=3) as r:
            for raw in r:
                if raw.startswith(b"id:"): got.append(int(raw[3:]))
                if len(got) == 2: break
        self.assertEqual(got, [3, 4])                           # exactly what it missed

    def test_viewer_fails_over_when_its_node_dies_and_misses_nothing(self):
        v = self.watch()                                        # nodes [N1,N2,N3]: attaches to N1
        self.assertEqual(v.feed.current_node, "http://" + self.cl.nodes["N1"])
        w = self.cl.client()
        w.submit("power", "A", "first", "u")
        self.cl.wait(lambda: v.seqs() == [1], what="first event")
        self.cl.kill("N1")                                      # viewer's node (and the leader) dies
        self.cl.wait_leader("N2", ["N2", "N3"])
        w.submit("power", "B", "second", "u"); w.submit("power", "C", "third", "u")
        self.cl.wait(lambda: v.seqs() == [1, 2, 3], what="viewer resumed on another node")
        self.assertNotEqual(v.feed.current_node, "http://" + self.cl.nodes["N1"])
        self.assertGreaterEqual(v.feed.stats["reconnects"], 1)

    def test_events_arrive_in_seq_order_under_concurrent_writes(self):
        v = self.watch("N2")
        def worker(i):
            c = self.cl.client("N1")
            for k in range(5): c.submit("power", f"L{i}", f"d{k}", "u")
        ts = [threading.Thread(target=worker, args=(i,)) for i in range(6)]
        [t.start() for t in ts]; [t.join() for t in ts]
        self.cl.wait(lambda: len(v.events) == 30, what="30 events")
        self.assertEqual(v.seqs(), list(range(1, 31)))

    def test_html_page_and_nodes_endpoint(self):
        base = "http://" + self.cl.nodes["N2"]
        page = urllib.request.urlopen(base + "/").read().decode()
        self.assertIn("EventSource", page)
        nodes = json.loads(urllib.request.urlopen(base + "/nodes").read())
        self.assertEqual(set(nodes["nodes"]), {"N1", "N2", "N3"})


class EventBusTest(unittest.TestCase):
    def entry(self, seq): return {"seq": seq, "op": "create_report", "payload": {}, "node_id": "N1"}

    def test_orders_out_of_order_publishes(self):
        bus = EventBus(); sub = bus.subscribe()
        for s in (2, 3, 1): bus.publish(self.entry(s))
        self.assertEqual([sub.q.get_nowait()["seq"] for _ in range(3)], [1, 2, 3])

    def test_slow_subscriber_is_dropped_not_blocking(self):
        bus = EventBus(queue_size=2); slow = bus.subscribe(); ok = bus.subscribe()
        for s in range(1, 6):
            bus.publish(self.entry(s))
            ok.q.get_nowait()                    # this viewer keeps up
        self.assertTrue(slow.dropped); self.assertFalse(ok.dropped)


if __name__ == "__main__":
    unittest.main()
