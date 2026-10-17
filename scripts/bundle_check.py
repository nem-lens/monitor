#!/usr/bin/env python3
"""Check an event bundle (handover P1a acceptance).

1. Every output's SHA-256 in manifest.json matches the file.
2. Every source's SHA-256 matches the original AEMO file, when found in one of
   the --sources directories (searched by file name).
3. DuckDB reproduces the event's peak and count of intervals above a
   threshold from dispatch_price.parquet, for --expect-peak / --expect-above.

Usage: bundle_check.py BUNDLE_DIR [--sources DIR ...] [--high 5000]
                       [--expect-peak 23107] [--expect-above 11]
Needs the duckdb Python package for step 3 (skipped with a note otherwise).
"""
import argparse
import hashlib
import json
import sys
from pathlib import Path


def sha256(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("bundle")
    ap.add_argument("--sources", nargs="*", default=[])
    ap.add_argument("--high", type=float, default=5000)
    ap.add_argument("--expect-peak", type=float)
    ap.add_argument("--expect-above", type=int)
    a = ap.parse_args()
    d = Path(a.bundle)
    m = json.loads((d / "manifest.json").read_text())
    failures = []

    for o in m["outputs"]:
        ok = sha256(d / o["file"]) == o["sha256"]
        print(f"output {o['file']}: {o['rows']} rows, hash {'ok' if ok else 'MISMATCH'}")
        if not ok:
            failures.append(f"hash of {o['file']}")

    index = {}
    for s in a.sources:
        for p in Path(s).rglob("*"):
            if p.is_file():
                index.setdefault(p.name, p)
    checked = 0
    for s in m["sources"]:
        p = index.get(s["file"])
        if p is None:
            continue
        checked += 1
        if sha256(p) != s["sha256"]:
            failures.append(f"source hash {s['file']}")
    print(f"sources: {len(m['sources'])} in manifest, {checked} checked against originals")
    if a.sources and checked == 0:
        failures.append("no sources found to check")

    try:
        import duckdb
    except ImportError:
        print("duckdb not installed; skipping query checks")
    else:
        price = (d / "dispatch_price.parquet").as_posix()
        start, end = m["window_start"][:19], m["window_end"][:19]
        peak, above, intervals = duckdb.sql(f"""
            SELECT max(RRP), count(*) FILTER (WHERE RRP >= {a.high}), count(DISTINCT SETTLEMENTDATE)
            FROM '{price}'
            WHERE REGIONID = '{m['region']}' AND INTERVENTION = 0
              AND SETTLEMENTDATE BETWEEN TIMESTAMP '{start}' AND TIMESTAMP '{end}'
        """).fetchone()
        print(f"duckdb: {m['region']} peak {peak}, {above} intervals >= {a.high:g}, {intervals} intervals in window")
        if a.expect_peak is not None and peak != a.expect_peak:
            failures.append(f"peak {peak} != {a.expect_peak}")
        if a.expect_above is not None and above != a.expect_above:
            failures.append(f"intervals above {above} != {a.expect_above}")
        for f in ("bids_offerperiod.parquet", "unit_dispatch.parquet", "offer_trk.parquet"):
            if (d / f).exists():
                n = duckdb.sql(f"SELECT count(*) FROM '{(d / f).as_posix()}'").fetchone()[0]
                print(f"duckdb: {f} {n} rows")

    for f in failures:
        print(f"FAIL {f}")
    print("bundle OK" if not failures else f"{len(failures)} failure(s)")
    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    main()
