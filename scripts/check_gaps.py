#!/usr/bin/env python3
"""Soak-test check (P0.2): report gaps in region_intervals longer than a limit.

Usage: check_gaps.py DB [--max-gap-min 10] [--since-hours 48]
Exits non-zero when any gap exceeds the limit. A laptop that slept will show
gaps; compare them with its sleep log before treating them as failures.
"""
import argparse
import sqlite3
import sys
from datetime import datetime, timedelta


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("db")
    ap.add_argument("--max-gap-min", type=float, default=10)
    ap.add_argument("--since-hours", type=float, default=48)
    a = ap.parse_args()
    c = sqlite3.connect(f"file:{a.db}?mode=ro", uri=True)
    rows = [r[0] for r in c.execute("SELECT DISTINCT settlement FROM region_intervals ORDER BY settlement")]
    if not rows:
        sys.exit("no intervals recorded")
    times = [datetime.fromisoformat(t) for t in rows]
    cutoff = times[-1] - timedelta(hours=a.since_hours)
    times = [t for t in times if t >= cutoff]
    gaps = [(p, n) for p, n in zip(times, times[1:]) if (n - p) > timedelta(minutes=a.max_gap_min)]
    span = times[-1] - times[0]
    expected = int(span.total_seconds() // 300) + 1
    print(f"{len(times)} intervals from {times[0]} to {times[-1]} ({span}); expected {expected}")
    for p, n in gaps:
        print(f"GAP {p} -> {n} ({n - p})")
    print("no gaps over limit" if not gaps else f"{len(gaps)} gap(s) over {a.max_gap_min} min")
    sys.exit(1 if gaps else 0)


if __name__ == "__main__":
    main()
