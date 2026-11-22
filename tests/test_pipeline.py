from __future__ import annotations

import tempfile
import unittest
from datetime import date, timedelta
from pathlib import Path

from backfill_pipeline import Ledger, plan, run
from backfill_pipeline.ledger import LedgerError


class PlanTests(unittest.TestCase):
    def test_plan_is_inclusive_and_steps_by_granularity(self):
        chunks = plan(date(2026, 3, 1), date(2026, 3, 5), granularity_days=2)
        self.assertEqual([c.label for c in chunks], ["2026-03-01", "2026-03-03", "2026-03-05"])
        self.assertEqual(chunks[1].last_day(), date(2026, 3, 4))

    def test_plan_rejects_inverted_and_wide_ranges(self):
        with self.assertRaises(ValueError):
            plan(date(2026, 3, 5), date(2026, 3, 1))
        with self.assertRaises(ValueError):
            plan(date(2026, 1, 1), date(2026, 12, 31), max_chunks=10)
        with self.assertRaises(ValueError):
            plan(date(2026, 3, 1), date(2026, 3, 2), granularity_days=0)


class LedgerTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.ledger = Ledger(Path(self._tmp.name) / "ledger.db")
        self.addCleanup(self.ledger.close)

    def test_second_claim_loses_until_finish_or_fail(self):
        self.assertTrue(self.ledger.claim("j", "2026-03-01"))
        self.assertFalse(self.ledger.claim("j", "2026-03-01"))
        self.ledger.fail("j", "2026-03-01", "boom")
        self.assertTrue(self.ledger.claim("j", "2026-03-01"))

    def test_done_partition_is_skipped_on_rerun(self):
        self.ledger.claim("j", "2026-03-01")
        self.ledger.finish("j", "2026-03-01", 4)
        self.assertFalse(self.ledger.claim("j", "2026-03-01"))
        self.assertEqual(self.ledger.state("j", "2026-03-01"), "done")

    def test_lease_expiry_allows_takeover(self):
        self.assertTrue(self.ledger.claim("j", "2026-03-01", lease_seconds=1))
        self.ledger._db.execute(
            "UPDATE chunk_runs SET lease_until = ? WHERE job = 'j'",
            ((date(2020, 1, 1).isoformat()),),
        )
        self.assertTrue(self.ledger.claim("j", "2026-03-01"))

    def test_reset_returns_failed_chunks_only(self):
        for partition in ("2026-03-01", "2026-03-02"):
            self.ledger.claim("j", partition)
            self.ledger.fail("j", partition, "boom")
        self.assertEqual(self.ledger.reset("j"), 2)
        self.assertEqual(self.ledger.state("j", "2026-03-01"), "pending")

    def test_finish_without_claim_raises(self):
        with self.assertRaises(LedgerError):
            self.ledger.finish("j", "2026-03-09", 1)


class RunTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.ledger = Ledger(Path(self._tmp.name) / "ledger.db")
        self.addCleanup(self.ledger.close)

    def test_rerun_loads_nothing_twice(self):
        calls: list[str] = []

        def loader(partition: str) -> int:
            calls.append(partition)
            return 2

        first = run("j", loader, "2026-03-01", "2026-03-02", self.ledger)
        second = run("j", loader, "2026-03-01", "2026-03-02", self.ledger)
        self.assertEqual((first.loaded, first.rows), (2, 4))
        self.assertEqual((second.loaded, second.skipped), (0, 2))
        self.assertEqual(calls, ["2026-03-01", "2026-03-02"])

    def test_failure_is_recorded_and_retried(self):
        state = {"fail": True}

        def loader(partition: str) -> int:
            if state["fail"]:
                raise RuntimeError("warehouse timeout")
            return 1

        first = run("j", loader, "2026-03-01", "2026-03-01", self.ledger)
        self.assertEqual((first.failed, first.ok), (1, False))
        self.assertIn("warehouse timeout", self.ledger.status("j")[0]["error"])
        state["fail"] = False
        second = run("j", loader, "2026-03-01", "2026-03-01", self.ledger)
        self.assertEqual(second.loaded, 1)


if __name__ == "__main__":
    unittest.main()
