"""argparse front end: plan, run, status, reset."""

from __future__ import annotations

import argparse
import importlib
import sys

from .ledger import STATES, Ledger
from .planner import plan, parse_date
from .runner import run


def load_loader(spec: str):
    """Resolve `module:function` to a callable."""
    module_name, _, attr = spec.partition(":")
    if not module_name or not attr:
        raise SystemExit(f"--loader wants module:function, got {spec!r}")
    module = importlib.import_module(module_name)
    try:
        return getattr(module, attr)
    except AttributeError as exc:
        raise SystemExit(f"{module_name} has no attribute {attr!r}") from exc


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="backfill-pipeline",
        description="Idempotent backfills over partition ranges.",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    def common(target: argparse.ArgumentParser, *, db: bool = True) -> None:
        if db:
            target.add_argument("--db", required=True, help="ledger SQLite file")
        target.add_argument("--job", required=True, help="job name; the ledger key")

    for name in ("plan", "run"):
        target = sub.add_parser(name)
        common(target, db=name != "plan")
        target.add_argument("--start", required=True, help="first partition, YYYY-MM-DD")
        target.add_argument("--end", required=True, help="last partition, YYYY-MM-DD")
        target.add_argument("--granularity-days", type=int, default=1)
        target.add_argument("--max-chunks", type=int, default=512)
        if name == "run":
            target.add_argument("--loader", required=True, help="module:function")
            target.add_argument("--lease-seconds", type=int, default=1800)
            target.add_argument("--workers", type=int, default=1,
                                help="concurrent chunk loaders (one ledger connection each)")

    status = sub.add_parser("status")
    common(status)
    status.add_argument("--json", action="store_true", help="one JSON object per line")

    reset = sub.add_parser("reset")
    common(reset)
    reset.add_argument("--state", action="append", choices=STATES, default=None)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)

    if args.command == "plan":
        try:
            chunks = plan(parse_date(args.start), parse_date(args.end),
                          granularity_days=args.granularity_days,
                          max_chunks=args.max_chunks)
        except ValueError as exc:
            print(f"error: {exc}", file=sys.stderr)
            return 2
        for chunk in chunks:
            print(f"{chunk.label}\t{chunk.last_day().isoformat()}")
        print(f"{len(chunks)} chunk(s)", file=sys.stderr)
        return 0

    with Ledger(args.db) as ledger:
        if args.command == "run":
            loader = load_loader(args.loader)
            try:
                result = run(args.job, loader, args.start, args.end, ledger,
                             granularity_days=args.granularity_days,
                             max_chunks=args.max_chunks,
                             lease_seconds=args.lease_seconds,
                             workers=args.workers)
            except ValueError as exc:
                print(f"error: {exc}", file=sys.stderr)
                return 2
            print(
                f"planned={result.planned} loaded={result.loaded} "
                f"skipped={result.skipped} failed={result.failed} rows={result.rows}",
                file=sys.stderr,
            )
            return 0 if result.ok else 1

        if args.command == "status":
            rows = ledger.status(args.job)
            if args.json:
                for row in rows:
                    print(row)
            else:
                for row in rows:
                    print(f"{row['partition']}\t{row['state']}\tattempts={row['attempts']}"
                          f"\trows={row['rows_loaded']}\t{row['error'] or ''}")
            return 0

        changed = ledger.reset(args.job, tuple(args.state or ["failed"]))
        print(f"reset {changed} chunk(s)", file=sys.stderr)
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
