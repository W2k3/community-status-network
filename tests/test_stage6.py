"""Stage 6 tests: what the web client needs (history, idempotent submit, limits, CORS, fonts).
Run:  python3 -m unittest tests.test_stage6 -v"""
import http.client
import json
import shutil
import tempfile
import threading
import unittest

from csn.client import Client, ApiError
from csn.server import make_server, MAX_BODY
from csn.store import MAX_DESCRIPTION
from tests.cluster_util import FastCluster


class SingleNodeTest(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()
        self.srv = make_server("N1", 0, self.dir)
        threading.Thread(target=self.srv.serve_forever, daemon=True).start()
        self.port = self.srv.server_address[1]
        self.c = Client(f"http://127.0.0.1:{self.port}", max_attempts=1)

    def tearDown(self):
        self.srv.shutdown(); self.srv.server_close()
        shutil.rmtree(self.dir, ignore_errors=True)

    def raw(self, method, path, body=None, headers=None):
        conn = http.client.HTTPConnection("127.0.0.1", self.port, timeout=5)
        conn.request(method, path, body, headers or {})
        res = conn.getresponse()
        data = res.read()
        conn.close()
        return res, data

    def test_history_lists_every_version(self):
        self.c.submit("water", "Kira", "Borehole dry", "alice")
        self.c.update("R1", "alice", 1, description="Borehole dry since morning")
        self.c.update("R1", "grace", 2, role="moderator", status="verified")
        self.c.submit("road", "Ntinda", "other report", "bob")
        h = self.c.history("R1")
        self.assertEqual([e["report"]["version"] for e in h], [1, 2, 3])
        self.assertEqual([e["op"] for e in h], ["created", "updated", "updated"])
        self.assertEqual(h[0]["report"]["description"], "Borehole dry")
        self.assertEqual(h[2]["report"]["status"], "verified")

    def test_history_of_unknown_report_is_404(self):
        with self.assertRaises(ApiError) as cm:
            self.c.history("R99")
        self.assertEqual(cm.exception.status, 404)

    def test_same_request_id_creates_one_report(self):
        a = self.c.submit("power", "Ntinda", "No power", "alice", request_id="abc-1")
        b = self.c.submit("power", "Ntinda", "No power", "alice", request_id="abc-1")
        c = self.c.submit("power", "Ntinda", "No power", "alice", request_id="abc-2")
        self.assertEqual((a["id"], b["id"], c["id"]), ("R1", "R1", "R2"))
        self.assertEqual(len(self.c.list()), 2)

    def test_request_id_survives_restart(self):
        self.c.submit("power", "Ntinda", "No power", "alice", request_id="abc-1")
        self.srv.shutdown(); self.srv.server_close(); self.srv.node.store.wal.close()
        self.srv = make_server("N1", 0, self.dir)             # replays the WAL
        threading.Thread(target=self.srv.serve_forever, daemon=True).start()
        c = Client(f"http://127.0.0.1:{self.srv.server_address[1]}", max_attempts=1)
        self.assertEqual(c.submit("power", "Ntinda", "No power", "alice", request_id="abc-1")["id"], "R1")
        self.assertEqual(len(c.list()), 1)

    def test_long_input_is_rejected(self):
        with self.assertRaises(ApiError) as cm:
            self.c.submit("power", "Ntinda", "x" * (MAX_DESCRIPTION + 1), "alice")
        self.assertEqual(cm.exception.status, 400)
        self.c.submit("power", "Ntinda", "ok", "alice")
        with self.assertRaises(ApiError) as cm:
            self.c.update("R1", "alice", 1, description="x" * (MAX_DESCRIPTION + 1))
        self.assertEqual(cm.exception.status, 400)
        with self.assertRaises(ApiError) as cm:
            self.c.update("R1", "alice", 1, description="   ")
        self.assertEqual(cm.exception.status, 400)
        self.assertEqual(self.c.get("R1")["version"], 1)

    def test_oversized_body_is_413(self):
        res, _ = self.raw("POST", "/reports", b"x" * (MAX_BODY + 1), {"Content-Type": "application/json"})
        self.assertEqual(res.status, 413)

    def test_cors_preflight(self):
        res, _ = self.raw("OPTIONS", "/reports/R1", headers={"Origin": "http://127.0.0.1:1",
                          "Access-Control-Request-Method": "PATCH"})
        self.assertEqual(res.status, 204)
        self.assertIn("PATCH", res.getheader("Access-Control-Allow-Methods"))
        self.assertIn("Content-Type", res.getheader("Access-Control-Allow-Headers"))
        self.assertEqual(res.getheader("Access-Control-Allow-Origin"), "*")

    def test_fonts_are_served_and_nothing_else(self):
        res, data = self.raw("GET", "/fonts/inter.woff2")
        self.assertEqual((res.status, res.getheader("Content-Type"), data[:4]), (200, "font/woff2", b"wOF2"))
        for path in ("/fonts/../live.html", "/fonts/..%2F..%2Fconfig.json", "/fonts/nope.woff2"):
            res, _ = self.raw("GET", path)
            self.assertEqual(res.status, 404, path)


class ClusterTest(unittest.TestCase):
    def setUp(self): self.cl = FastCluster()
    def tearDown(self): self.cl.stop()

    def test_request_id_dedups_through_any_node_and_after_failover(self):
        a = self.cl.client("N2").submit("water", "Kira", "dry", "alice", request_id="k1")
        b = self.cl.client("N3").submit("water", "Kira", "dry", "alice", request_id="k1")
        self.assertEqual(a["id"], b["id"])
        self.cl.kill("N1")
        self.cl.wait_leader("N2", ["N2", "N3"])
        c = self.cl.client("N3").submit("water", "Kira", "dry", "alice", request_id="k1")
        self.assertEqual(c["id"], a["id"])
        self.assertEqual(len(self.cl.client("N2").list()), 1)

    def test_history_is_the_same_on_every_node(self):
        c = self.cl.client("N2")
        c.submit("water", "Kira", "dry", "alice")
        c.update("R1", "grace", 1, role="moderator", status="verified")
        self.cl.wait(lambda: all(self.cl.node(n).store.wal.last_seq == 2 for n in ("N1", "N2", "N3")))
        hs = [json.dumps(self.cl.client(n).history("R1"), sort_keys=True) for n in ("N1", "N2", "N3")]
        self.assertEqual(len(set(hs)), 1)


if __name__ == "__main__":
    unittest.main()
