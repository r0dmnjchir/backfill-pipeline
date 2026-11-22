"""Attempt ledger.

One row per (job, partition). A chunk may be claimed only while it is pending,
failed, or running with an expired lease; the claim is a single conditional
UPDATE so two workers cannot both win. `done` is terminal until an explicit
reset, which is what makes a re-run converge instead of duplicating rows.
"""

from __future__ import annotations

import sqlite3
from contextlib import closing
from datetime import datetime, timedelta, timezone
from pathlib import Path

__all__ = ["Ledger", "LedgerError", "STATES"]

STATES = ("pending", "running", "done", "failed")
_SCHEMA = """
CREATE TABLE IF NOT EXISTS chunk_runs (
    job         TEXT NOT NULL,
    partition   TEXT NOT NULL,
    state       TEXT NOT NULL CHECK (state IN ('pending', 'running', 'done', 'failed')),
    attempts    INTEGER NOT NULL DEFAULT 0,
    rows_loaded INTEGER NOT NULL DEFAULT 0,
    lease_until TEXT,
    error       TEXT,
    updated_at  TEXT NOT NULL,
    PRIMARY KEY (job, partition)
);
CREATE INDEX IF NOT EXISTS chunk_runs_state ON chunk_runs (job, state);
"""


class LedgerError(RuntimeError):
    """A write assumed a state that no longer holds."""


def _now() -> datetime:
    return datetime.now(timezone.utc).replace(microsecond=0)


class Ledger:
    """SQLite-backed record of which partitions a job has loaded."""

    def __init__(self, path: str | Path, *, busy_timeout: float = 10.0) -> None:
        self.path = Path(path)
        if self.path.parent != Path(""):
            self.path.parent.mkdir(parents=True, exist_ok=True)
        self._db = sqlite3.connect(self.path, isolation_level=None, timeout=busy_timeout)
        self._db.row_factory = sqlite3.Row
        self._db.execute("PRAGMA journal_mode=WAL")
        self._db.execute("PRAGMA busy_timeout=%d" % int(busy_timeout * 1000))
        self._db.executescript(_SCHEMA)

    def __enter__(self) -> "Ledger":
        return self

    def __exit__(self, *exc_info) -> None:
        self.close()

    def close(self) -> None:
        self._db.close()

    def register(self, job: str, partition: str) -> None:
        """Ensure the row exists; a known partition keeps its state."""
        self._db.execute(
            "INSERT OR IGNORE INTO chunk_runs (job, partition, state, updated_at) "
            "VALUES (?, ?, 'pending', ?)",
            (job, partition, _now().isoformat()),
        )

    def claim(self, job: str, partition: str, *, lease_seconds: int = 1800) -> bool:
        """Try to take the chunk. True if this caller owns it now."""
        if lease_seconds < 1:
            raise ValueError("lease_seconds must be >= 1")
        self.register(job, partition)
        now = _now()
        cursor = self._db.execute(
            """
            UPDATE chunk_runs
               SET state = 'running',
                   attempts = attempts + 1,
                   lease_until = ?,
                   error = NULL,
                   updated_at = ?
             WHERE job = ? AND partition = ?
               AND (state IN ('pending', 'failed')
                    OR (state = 'running' AND lease_until < ?))
            """,
            ((now + timedelta(seconds=lease_seconds)).isoformat(), now.isoformat(),
             job, partition, now.isoformat()),
        )
        return cursor.rowcount == 1

    def finish(self, job: str, partition: str, rows_loaded: int = 0) -> None:
        """Mark a claimed chunk done; releases the lease."""
        cursor = self._db.execute(
            "UPDATE chunk_runs SET state = 'done', rows_loaded = ?, lease_until = NULL, "
            "error = NULL, updated_at = ? "
            "WHERE job = ? AND partition = ? AND state = 'running'",
            (int(rows_loaded), _now().isoformat(), job, partition),
        )
        if cursor.rowcount != 1:
            raise LedgerError(f"{job}/{partition} was not claimed by this worker")

    def fail(self, job: str, partition: str, error: str) -> None:
        """Mark a claimed chunk failed so the next run retries it."""
        cursor = self._db.execute(
            "UPDATE chunk_runs SET state = 'failed', lease_until = NULL, error = ?, "
            "updated_at = ? WHERE job = ? AND partition = ? AND state = 'running'",
            (error.strip()[:2000], _now().isoformat(), job, partition),
        )
        if cursor.rowcount != 1:
            raise LedgerError(f"{job}/{partition} was not claimed by this worker")

    def reset(self, job: str, states: tuple[str, ...] = ("failed",)) -> int:
        """Return chunks in `states` to pending. Reports rows changed."""
        bad = set(states) - set(STATES)
        if bad:
            raise ValueError(f"unknown states: {sorted(bad)}")
        if not states:
            return 0
        marks = ",".join("?" * len(states))
        cursor = self._db.execute(
            f"UPDATE chunk_runs SET state = 'pending', lease_until = NULL, error = NULL, "
            f"updated_at = ? WHERE job = ? AND state IN ({marks})",
            (_now().isoformat(), job, *states),
        )
        return cursor.rowcount

    def state(self, job: str, partition: str) -> str | None:
        row = self._db.execute(
            "SELECT state FROM chunk_runs WHERE job = ? AND partition = ?",
            (job, partition),
        ).fetchone()
        return None if row is None else row["state"]

    def status(self, job: str) -> list[dict[str, object]]:
        with closing(self._db.execute(
            "SELECT partition, state, attempts, rows_loaded, lease_until, error, updated_at "
            "FROM chunk_runs WHERE job = ? ORDER BY partition",
            (job,),
        )) as rows:
            return [dict(row) for row in rows]

    def counts(self, job: str) -> dict[str, int]:
        result = dict.fromkeys(STATES, 0)
        with closing(self._db.execute(
            "SELECT state, COUNT(*) AS n FROM chunk_runs WHERE job = ? GROUP BY state",
            (job,),
        )) as rows:
            for row in rows:
                result[row["state"]] = row["n"]
        return result
