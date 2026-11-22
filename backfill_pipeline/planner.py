"""Partition-range planning.

A chunk is one partition label. Planning is inclusive of both ends and refuses
ranges wider than the caller allows, so a typo in a date does not queue two
years of loads before anyone notices.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta

__all__ = ["Chunk", "plan", "parse_date"]


@dataclass(frozen=True, order=True, slots=True)
class Chunk:
    """A single partition to load, identified by its start date."""

    partition: date
    granularity_days: int = 1

    @property
    def label(self) -> str:
        return self.partition.isoformat()

    def last_day(self) -> date:
        """Inclusive end of the partition range this chunk covers."""
        return self.partition + timedelta(days=self.granularity_days - 1)


def parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"expected YYYY-MM-DD, got {value!r}") from exc


def plan(
    start: date,
    end: date,
    *,
    granularity_days: int = 1,
    max_chunks: int = 512,
) -> tuple[Chunk, ...]:
    """Return one chunk per `granularity_days` between `start` and `end`.

    `start` and `end` are both inclusive; the final chunk may run past `end`
    and the loader is responsible for clipping. Raises ValueError when the
    range is inverted, the step is not positive, or the plan is too wide.
    """
    if granularity_days < 1:
        raise ValueError(f"granularity_days must be >= 1, got {granularity_days}")
    if start > end:
        raise ValueError(f"start {start} is after end {end}")
    if max_chunks < 1:
        raise ValueError(f"max_chunks must be >= 1, got {max_chunks}")

    span = (end - start).days // granularity_days + 1
    if span > max_chunks:
        raise ValueError(
            f"plan covers {span} chunks, limit is {max_chunks}; "
            "narrow the range or raise max_chunks"
        )
    return tuple(
        Chunk(start + timedelta(days=offset * granularity_days), granularity_days)
        for offset in range(span)
    )
