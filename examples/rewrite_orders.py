"""Demo loader: rewrite one day of `orders_daily` from `raw_orders`.

Delete-then-insert inside one transaction is the cheapest idempotent shape and
what the loader contract asks for. Setup a throwaway warehouse with:

    python -m examples.rewrite_orders --setup
    python -m backfill_pipeline.cli run --db ledger.db --job orders_daily \
        --start 2026-03-01 --end 2026-03-03 \
        --loader examples.rewrite_orders:load
"""

from __future__ import annotations

import argparse
import os
import random
import sqlite3
from datetime import date, timedelta

DB = os.environ.get("WAREHOUSE_DB", "warehouse.db")

SCHEMA = """
CREATE TABLE IF NOT EXISTS raw_orders (
    order_id   INTEGER PRIMARY KEY,
    customer   TEXT NOT NULL,
    amount_paise INTEGER NOT NULL,
    placed_at  TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS orders_daily (
    order_id   INTEGER PRIMARY KEY,
    customer   TEXT NOT NULL,
    amount_paise INTEGER NOT NULL,
    partition_date TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS orders_daily_partition ON orders_daily (partition_date);
"""


def load(partition: str) -> int:
    """Replace every row of `orders_daily` that belongs to `partition`."""
    day = date.fromisoformat(partition)
    lo = f"{day.isoformat()}T00:00:00"
    hi = f"{(day + timedelta(days=1)).isoformat()}T00:00:00"
    connection = sqlite3.connect(DB)
    try:
        with connection:  # one transaction: a crash leaves the old partition intact
            connection.execute("DELETE FROM orders_daily WHERE partition_date = ?",
                               (partition,))
            cursor = connection.execute(
                "INSERT INTO orders_daily (order_id, customer, amount_paise, partition_date) "
                "SELECT order_id, customer, amount_paise, ? FROM raw_orders "
                "WHERE placed_at >= ? AND placed_at < ?",
                (partition, lo, hi),
            )
            return cursor.rowcount
    finally:
        connection.close()


def setup(days: int = 5) -> None:
    connection = sqlite3.connect(DB)
    try:
        with connection:
            connection.executescript(SCHEMA)
            rng = random.Random(7)
            base = date(2026, 3, 1)
            for i in range(days):
                for n in range(rng.randint(3, 9)):
                    placed = f"{base + timedelta(days=i)}T{rng.randint(0, 23):02d}:15:00"
                    connection.execute(
                        "INSERT INTO raw_orders (customer, amount_paise, placed_at) "
                        "VALUES (?, ?, ?)",
                        (f"c{n:03d}", rng.randint(1000, 90000), placed),
                    )
    finally:
        connection.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--setup", action="store_true", help="create demo tables and rows")
    parser.add_argument("--partition", help="load one partition and print the row count")
    args = parser.parse_args()
    if args.setup:
        setup()
        print(f"seeded {DB}")
    if args.partition:
        print(load(args.partition))
