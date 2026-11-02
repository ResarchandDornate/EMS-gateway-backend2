"""CLI: backfill stream_records -> dedicated per-stream tables (S3-S9). READ-ONLY on the
source. Idempotent and safe to resume/re-run. Exits non-zero if any record failed or any
post-write verification mismatched, and writes a recoverable error report.

Usage (from backend/, with the app venv + DB env set):
    python -m scripts.backfill_structured --streams S3,S4,S5,S6,S7,S8,S9 --errors-out backfill_errors.json
"""
import argparse
import asyncio
import json
import sys

from src.database import SessionLocal
from src.ingestion.backfill import SPECS, backfill_stream


async def _run(streams):
    out = {}
    async with SessionLocal() as s:
        for st in streams:
            rep = await backfill_stream(s, stream=st)
            out[st] = rep.as_dict()
    return out


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--streams", default="S3,S4,S5,S6,S7,S8,S9")
    ap.add_argument("--errors-out", default=None, help="write errors/discrepancies JSON here")
    args = ap.parse_args()
    streams = [x.strip().upper() for x in args.streams.split(",") if x.strip()]
    bad = [x for x in streams if x not in SPECS]
    if bad:
        sys.exit(f"unsupported streams: {bad} (S3-S9 only)")

    out = asyncio.run(_run(streams))
    print(json.dumps(out, indent=2, default=str))

    issues = {s: {"errors": r["errors"], "discrepancies": r["discrepancies"]}
              for s, r in out.items() if r["errors"] or r["discrepancies"]}
    if args.errors_out and issues:
        with open(args.errors_out, "w") as f:
            json.dump(issues, f, indent=2, default=str)
        print(f"[!] wrote error report -> {args.errors_out}", file=sys.stderr)
    sys.exit(2 if issues else 0)


if __name__ == "__main__":
    main()
