"""Bounded parallel loading.

Each worker owns its own SQLite connection to the ledger, so claims stay atomic
without a process-wide lock; WAL plus the busy timeout absorb the contention.
The loader must therefore be safe to run concurrently for *different*
partitions, which the loader contract already requires.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Callable

from .ledger import Ledger
from .planner import Chunk
from .runner import Loader, Result

__all__ = ["run_parallel"]


def _load_one(
    job: str,
    loader: Loader,
    partition: str,
    ledger_path: str,
    lease_seconds: int,
) -> tuple[str, int]:
    with Ledger(ledger_path) as ledger:
        if not ledger.claim(job, partition, lease_seconds=lease_seconds):
            return "skipped", 0
        try:
            rows = int(loader(partition) or 0)
        except Exception as exc:  # noqa: BLE001 - recorded, then retried next run
            ledger.fail(job, partition, f"{type(exc).__name__}: {exc}")
            return "failed", 0
        ledger.finish(job, partition, rows)
        return "loaded", rows


def run_parallel(
    job: str,
    loader: Loader,
    chunks: tuple[Chunk, ...] | list[Chunk],
    ledger_path: str | Path,
    *,
    workers: int = 4,
    lease_seconds: int = 1800,
    on_result: Callable[[str, str], None] | None = None,
) -> Result:
    """Load chunks with `workers` threads; one failure does not stop the rest."""
    if workers < 1:
        raise ValueError(f"workers must be >= 1, got {workers}")
    path = str(ledger_path)
    result = Result(planned=len(chunks))
    with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="backfill") as pool:
        futures = {
            pool.submit(_load_one, job, loader, chunk.label, path, lease_seconds): chunk.label
            for chunk in chunks
        }
        for future in as_completed(futures):
            partition = futures[future]
            try:
                kind, rows = future.result()
            except Exception as exc:  # noqa: BLE001 - ledger open failure, not the loader
                kind, rows = "failed", 0
            if kind == "loaded":
                result.loaded += 1
                result.rows += rows
            elif kind == "skipped":
                result.skipped += 1
            else:
                result.failed += 1
            if on_result is not None:
                on_result(partition, kind)
    return result
