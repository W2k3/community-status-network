"""Stage 1 tests.  Run:  python3 -m unittest tests.test_stage1 -v"""
import shutil
import tempfile
import threading
import unittest

from csn.client import Client, ApiError
from csn.server import make_server
from csn.store import Store


class Stage1Test(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()
        self.srv = make_server("N1", 0, self.dir)   # port 0 = pick a free port
        threading.Thread(target=self.srv.serve_forever, daemon=True).start()
        self.c = Client(f"http://127.0.0.1:{self.srv.server_address[1]}", max_attempts=1)

    def tearDown(self):
        self.srv.shutdown(); self.srv.server_close()
        shutil.rmtree(self.dir, ignore_errors=True)

    def test_submit_and_get(self):
        r = self.c.submit("power", "Ntinda", "No power since 8am", "alice")
        self.assertEqual((r["id"], r["version"], r["status"]), ("R1", 1, "open"))
        self.assertEqual(self.c.get("R1")["location"], "Ntinda")

    def test_filter(self):
        self.c.submit("power", "Ntinda", "x", "a")
        self.c.submit("water", "Kira", "y", "a")
        self.assertEqual(len(self.c.list(type="water")), 1)
        self.assertEqual(len(self.c.list(location="ntin")), 1)
        self.assertEqual(len(self.c.list()), 2)

    def test_update_bumps_version(self):
        self.c.submit("road", "Jinja Rd", "blocked", "a")
        r = self.c.update("R1", "a", 1, description="blocked by truck")
        self.assertEqual(r["version"], 2)

    def test_only_moderator_resolves(self):
        self.c.submit("road", "Jinja Rd", "blocked", "a")
        with self.assertRaises(ApiError) as cm:
            self.c.update("R1", "a", 1, status="resolved")
        self.assertEqual(cm.exception.status, 400)
        self.assertEqual(self.c.update("R1", "mod", 1, role="moderator",
                                       status="resolved")["status"], "resolved")

    def test_errors(self):
        with self.assertRaises(ApiError) as cm:
            self.c.get("R99")
        self.assertEqual(cm.exception.status, 404)
        with self.assertRaises(ApiError) as cm:
            self.c.submit("alien", "x", "y", "a")
        self.assertEqual(cm.exception.status, 400)

    def test_wal_replay_after_restart(self):
        self.c.submit("water", "Kira", "dry tap", "a")
        self.c.update("R1", "a", 1, description="still dry")
        before = Store("N1", self.dir).reports   # fresh Store = simulated restart
        self.assertEqual(before["R1"]["version"], 2)
        self.assertEqual(before["R1"]["description"], "still dry")
        s2 = Store("N1", self.dir)
        self.assertEqual(s2.create_report({"type": "power", "location": "L",
                         "description": "d", "updated_by": "a"})["id"], "R2")


if __name__ == "__main__":
    unittest.main()
