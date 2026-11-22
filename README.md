# backfill-pipeline

Rerunnable backfills for partitioned warehouse tables. Point it at a partition
range and a loader; it plans one chunk per partition, records every attempt in a
local SQLite ledger, and refuses to load a partition twice unless the previous
attempt failed or its lease expired.

Why: the expensive part of a backfill is not the SQL, it is the second run after
the job died at 80%. A backfill is only safe if re-running it converges instead
of duplicating rows. The ledger makes that the default: `done` chunks are
skipped, `failed` chunks are retried on the next run, and `running` chunks are
held by a lease so two processes do not write the same partition at once.

This is a runner, not an orchestrator. Airflow, cron, or a terminal can all
invoke it. It has no dependencies outside the Python 3.10+ standard library.

## Install

```bash
git clone https://github.com/r0dmnjchir/backfill-pipeline.git
cd backfill-pipeline
python3 -m venv .venv && . .venv/bin/activate
pip install -e .
```

`pip install -e .` is optional; `python -m backfill_pipeline` works from the
checkout without it.

## Usage

Plan what a range would do, without touching the warehouse:

```bash
python -m backfill_pipeline plan --job orders_daily \
  --start 2026-03-01 --end 2026-03-31
```

Run it. The loader is any `module:function` that takes a partition label and
returns the number of rows loaded:

```bash
python -m backfill_pipeline run --db ledger.db --job orders_daily \
  --start 2026-03-01 --end 2026-03-31 \
  --loader examples.rewrite_orders:load
```

Inspect or release failed chunks:

```bash
python -m backfill_pipeline status --db ledger.db --job orders_daily
python -m backfill_pipeline reset --db ledger.db --job orders_daily --state failed
```

Exit codes: `0` all planned chunks are `done` (or already were), `1` at least
one chunk failed, `2` bad arguments or a plan that exceeds `--max-chunks`.

### Workers

`--workers N` loads up to N partitions concurrently. Each worker opens its own
ledger connection and claims before loading, so the ledger stays authoritative;
the loser of a claim moves on instead of double-loading. Keep N at or below the
warehouse's per-job concurrency budget. The loader must be safe for different
partitions at once, which the contract already requires.

```bash
python -m backfill_pipeline run --db ledger.db --job orders_daily \
  --start 2026-03-01 --end 2026-03-31 --workers 4 \
  --loader examples.rewrite_orders:load
```

### Loader contract

A loader receives one partition label (`YYYY-MM-DD`) and must be idempotent for
that partition: delete-and-insert, `INSERT OVERWRITE`, or a merge on the
partition key. Loading rows outside the given partition is a bug, because the
ledger will not retry that partition for those rows.

It returns a row count and raises on failure. Returning `0` is a success; the
chunk is marked `done` with `rows_loaded = 0`.

### Data contract

The target table must be partitioned on a date column and every row the loader
produces must derive its partition from the label it was handed. The ledger
keys on `(job, partition)`, so changing what a job means requires a new job
name — reuse a name for a different table and the ledger will skip partitions
that were never loaded into it.

Partition labels are date-only. Timestamp-partitioned tables need the loader to
fan a day out to its time slices internally.

### Sample DAG

```python
from datetime import date, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    "orders_backfill",
    start_date=date(2026, 3, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 0},  # the ledger retries, not the scheduler
) as dag:
    BashOperator(
        task_id="backfill_q1_orders",
        bash_command=(
            "cd /opt/backfill-pipeline && python -m backfill_pipeline run "
            "--db /var/lib/backfill/ledger.db --job orders_daily "
            "--start {{ ds }} --end {{ macros.ds_add(ds, 6) }} "
            "--loader jobs.orders:load_partition"
        ),
    )
```

Run one DAG task per week and let the ledger deduplicate; `max_active_runs=1`
keeps lease contention near zero.

## Runbook

**A chunk is stuck in `running`.** The process died between claim and finish.
The lease is 30 minutes by default; the next run re-claims it automatically
after expiry. To take it now, `reset --state running` and start the job again.

**A chunk keeps failing.** `status` prints the last error per partition. Fix
the loader, then `reset --state failed` for that range and re-run. Chunks that
already succeeded are untouched.

**The run was too wide.** `--max-chunks` defaults to 512 and aborts before any
claim. Narrow the range or raise `--granularity-days`.

**Two workers, same job.** Safe: claims are a single conditional `UPDATE`, and
the loser moves on. Two workers loading *different* chunks of the same
partitioned table is fine; two jobs loading the same table is not.

## Layout

```
backfill_pipeline/
  planner.py   partition-range planning, inclusive, chunk-count guard
  ledger.py    SQLite ledger: register, claim, finish, fail, reset
  runner.py    drives the plan, skips done chunks, records rows and errors
  cli.py       argparse front end: plan, run, status, reset
examples/
  rewrite_orders.py   demo loader that rewrites a local SQLite table
tests/
  test_pipeline.py    ledger idempotency and planner edge cases
```

## License

MIT, see [LICENSE](LICENSE).
