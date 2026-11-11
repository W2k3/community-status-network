#!/usr/bin/env python3
"""Run every test and save the evidence:   python3 run_tests.py
Writes results/test_results.txt (one PASS/FAIL line per test + a per-stage summary) for the report."""
import io
import os
import platform
import sys
import time
import unittest

ROOT = os.path.dirname(os.path.abspath(__file__))
os.chdir(ROOT)
sys.path.insert(0, ROOT)


def flatten(suite):
    for t in suite:
        if isinstance(t, unittest.TestSuite):
            yield from flatten(t)
        else:
            yield t


suite = unittest.defaultTestLoader.discover("tests")
all_tests = [t.id() for t in flatten(suite)]
t0 = time.time()
# The unittest report goes to a buffer; node request logs still print to the terminal.
result = unittest.TextTestRunner(stream=io.StringIO(), verbosity=0).run(suite)
elapsed = time.time() - t0

bad = {t.id(): err for t, err in result.failures + result.errors}
skipped = {t.id() for t, _ in result.skipped}
per_stage = {}
os.makedirs("results", exist_ok=True)
with open("results/test_results.txt", "w") as f:
    f.write(f"Test run {time.strftime('%Y-%m-%d %H:%M:%S')}  |  Python {platform.python_version()}  |  {platform.platform()}\n")
    f.write("=" * 78 + "\n")
    for tid in sorted(all_tests):
        status = "FAIL" if tid in bad else ("SKIP" if tid in skipped else "PASS")
        f.write(f"{status}  {tid}\n")
        stage = tid.split(".")[-3] if tid.count(".") >= 2 else tid
        per_stage.setdefault(stage, [0, 0])
        per_stage[stage][0] += 1
        per_stage[stage][1] += status == "PASS"
    for tid, err in bad.items():
        f.write("\n" + "-" * 78 + f"\nFAILURE: {tid}\n{err}\n")
    f.write("=" * 78 + "\n")
    for stage, (n, ok) in sorted(per_stage.items()):
        f.write(f"{stage:<14} {ok}/{n} passed\n")
    passed = sum(v[1] for v in per_stage.values())
    f.write(f"TOTAL          {passed}/{len(all_tests)} passed in {elapsed:.1f}s\n")
print(open("results/test_results.txt").read().split("=" * 78)[-1].strip())
print("full log: results/test_results.txt")
sys.exit(0 if result.wasSuccessful() else 1)
