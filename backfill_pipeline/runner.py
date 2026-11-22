"""Execution loop over a plan."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Callable

from .ledger import Ledger
from .planner import Chunk, plan, parse_date

__all__ = ["Result", "run", "run_chunks", "Loader"]

Loader = Callable[[str], int]


@dataclass(slots=True)
class Result:
    planned: int = 0
    loaded: int = 0
    skipped: int = 0
    failed: int = 0
    rows: int = 0

    @property
    def ok(self) -> bool:
        return self.failed == 0

    def as_dict(self) -> dict[str, int | bool]:
        return {**asdict(self), "ok": self.ok}


def run_chunks(
    job: str,
    loader: Loader,
    chunks: tuple[Chunk, ...] | list[Chunk],
    ledger: Ledger,
    *,
    lease_seconds: int = 1800,
) -> Result:
    """Load every chunk not already done. One failure does not stop the run."""
    result = Result(planned=len(chunks))
    for chunk in chunks:
        partition = chunk.label
        if not ledger.claim(job, partition, lease_seconds=lease_seconds):
            result.skipped += 1
            continue
        try:
            rows = int(loader(partition) or 0)
        except Exception as exc:  # noqa: BLE001 - recorded, then retried next run
            ledger.fail(job, partition, f"{type(exc).__name__}: {exc}")
            result.failed += 1
            continue
        ledger.finish(job, partition, rows)
        result.loaded += 1
        result.rows += rows
    return result


def run(
    job: str,
    loader: Loader,
    start: str,
    end: str,
    ledger: Ledger,
    *,
    granularity_days: int = 1,
    max_chunks: int = 512,
    lease_seconds: int = 1800,
) -> Result:
    """Plan `start..end` and load it. Dates are inclusive `YYYY-MM-DD`."""
    chunks = plan(
        parse_date(start),
        parse_date(end),
        granularity_days=granularity_days,
        max_chunks=max_chunks,
    )
    return run_chunks(job, loader, chunks, ledger, lease_seconds=lease_seconds)
