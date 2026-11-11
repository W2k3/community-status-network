"""Stage 2 tests: per-record locking + version checks.
Run:  python3 -m unittest tests.test_stage2 -v"""
import shutil
import tempfile
import threading
import time
import unittest

from csn.client import Client, ApiError
from csn.server import make_server


class Stage2Test(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()
        self.srv = make_server("N1", 0, self.dir)
        self.store = self.srv.node.store
        threading.Thread(target=self.srv.serve_forever, daemon=True).start()
        self.c = Client(f"http://127.0.0.1:{self.srv.server_address[1]}", max_attempts=1)

    def tearDown(self):
        self.srv.shutdown(); self.srv.server_close(); self.store.wal.close()
        shutil.rmtree(self.dir, ignore_errors=True)

    def _race(self, n, report_id="R1"):
        """n threads all edit the report from the SAME base version at the same instant."""
        barrier, results = threading.Barrier(n), []
        def worker(i):
            barrier.wait()
            try:
                r = self.c.update(report_id, f"user{i}", 1, description=f"edit by user{i}")
                results.append((200, r))
            except ApiError as e:
                results.append((e.status, e.body))
        ts = [threading.Thread(target=worker, args=(i,)) for i in range(n)]
        [t.start() for t in ts]; [t.join() for t in ts]
        return results

    def test_two_threads_one_gets_409(self):
        self.c.submit("power", "Ntinda", "no power", "alice")
        results = self._race(2)
        codes = sorted(code for code, _ in results)
        self.assertEqual(codes, [200, 409])
        loser = next(b for code, b in results if code == 409)
        self.assertEqual(loser["current_version"], 2)       # tells the client what to rebase on
        final = self.c.get("R1")
        self.assertEqual(final["version"], 2)               # exactly one write applied
        winner = next(b for code, b in results if code == 200)
        self.assertEqual(final["description"], winner["description"])

    def test_many_threads_exactly_one_wins(self):
        self.c.submit("power", "Ntinda", "no power", "alice")
        results = self._race(10)
        self.assertEqual(sum(1 for c, _ in results if c == 200), 1)
        self.assertEqual(sum(1 for c, _ in results if c == 409), 9)
        self.assertEqual(self.c.get("R1")["version"], 2)

    def test_retry_after_409_succeeds(self):
        self.c.submit("water", "Kira", "dry", "alice")
        self.c.update("R1", "bob", 1, description="bob's edit")
        with self.assertRaises(ApiError) as cm:
            self.c.update("R1", "alice", 1, description="alice's stale edit")
        cur = cm.exception.body["current_version"]
        self.assertEqual(self.c.update("R1", "alice", cur, description="merged")["version"], 3)

    def test_missing_base_version_is_400(self):
        self.c.submit("water", "Kira", "dry", "alice")
        with self.assertRaises(ApiError) as cm:
            self.c._call("PATCH", "/reports/R1", {"updated_by": "a", "description": "x"})
        self.assertEqual(cm.exception.status, 400)

    def test_locks_are_per_record(self):
        """A slow edit on R1 must not block an edit on R2."""
        self.c.submit("power", "A", "a", "u"); self.c.submit("power", "B", "b", "u")
        self.store.critical_section_delay = 0.8
        slow = threading.Thread(target=lambda: self.c.update("R1", "u", 1, description="slow"))
        slow.start(); time.sleep(0.2)                       # R1's lock is now held
        t0 = time.time()
        self.store.critical_section_delay = 0               # R2 edit is not delayed
        self.c.update("R2", "u", 1, description="fast")
        self.assertLess(time.time() - t0, 0.5)              # did not wait for R1
        slow.join()
        self.assertEqual(self.c.get("R1")["version"], 2)

    def test_wal_has_no_duplicate_versions(self):
        self.c.submit("power", "A", "a", "u")
        self._race(8)
        versions = [e["payload"]["version"] for e in self.store.wal.read_all()
                    if e["payload"]["id"] == "R1"]
        self.assertEqual(versions, [1, 2])


if __name__ == "__main__":
    unittest.main()
