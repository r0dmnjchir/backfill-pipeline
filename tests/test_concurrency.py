from __future__ import annotations

import tempfile
import threading
import time
import unittest
from datetime import date
from pathlib import Path

from backfill_pipeline import Ledger, plan, run
from backfill_pipeline.concurrency import run_parallel


class ParallelRunTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.ledger = Ledger(Path(self._tmp.name) / "ledger.db")
        self.addCleanup(self.ledger.close)
        self.chunks = plan(date(2026, 3, 1), date(2026, 3, 8))

    def test_every_partition_loads_exactly_once(self):
        seen: list[str] = []
        lock = threading.Lock()

        def loader(partition: str) -> int:
            time.sleep(0.01)
            with lock:
                seen.append(partition)
            return 3

        result = run_parallel("j", loader, self.chunks, self.ledger.path, workers=4)
        self.assertEqual(result.loaded, 8)
        self.assertEqual(result.rows, 24)
        self.assertEqual(sorted(seen), [c.label for c in self.chunks])
        self.assertEqual(self.ledger.counts("j")["done"], 8)

    def test_rerun_after_parallel_pass_skips_everything(self):
        def loader(partition: str) -> int:
            return 1

        run_parallel("j", loader, self.chunks, self.ledger.path, workers=3)
        again = run_parallel("j", loader, self.chunks, self.ledger.path, workers=3)
        self.assertEqual((again.loaded, again.skipped), (0, 8))

    def test_one_failure_does_not_cancel_other_chunks(self):
        def loader(partition: str) -> int:
            if partition == "2026-03-04":
                raise RuntimeError("bad partition")
            return 2

        result = run_parallel("j", loader, self.chunks, self.ledger.path, workers=4)
        self.assertEqual((result.loaded, result.failed), (7, 1))
        self.assertFalse(result.ok)
        self.assertEqual(self.ledger.state("j", "2026-03-04"), "failed")

    def test_run_routes_to_parallel_when_workers_above_one(self):
        def loader(partition: str) -> int:
            return 1

        result = run("j", loader, "2026-03-01", "2026-03-03", self.ledger, workers=2)
        self.assertEqual(result.loaded, 3)
        self.assertEqual(self.ledger.counts("j")["done"], 3)

    def test_workers_must_be_positive(self):
        with self.assertRaises(ValueError):
            run_parallel("j", lambda p: 1, self.chunks, self.ledger.path, workers=0)


if __name__ == "__main__":
    unittest.main()
