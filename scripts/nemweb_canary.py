#!/usr/bin/env python3
"""Live NEMWeb canary: checks the assumptions nem-monitor makes about NEMWeb.

1. The DispatchIS listing is reachable and recent (newest file under 15 minutes old).
2. The newest N DispatchIS files replay through `nem-monitor replay` with no
   warnings and produce one interval per file.
3. With --data-ready, every directory in the monitor's default data_ready_dirs
   has a file for the previous trading day (run after about 04:30 AEST).

Usage: nemweb_canary.py [--files N] [--data-ready] [--user-agent UA]
Needs a built binary (target/debug or target/release).
"""
import argparse
import os
import re
import sqlite3
import subprocess
import sys
import tempfile
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
BASE = "https://nemweb.com.au/Reports/Current"
# Keep in step with SourceCfg::default() in src/config.rs.
DATA_READY_DIRS = ["Next_Day_Dispatch", "Next_Day_Offer_Energy_SPARSE"]
DISPATCH_RE = re.compile(r"PUBLIC_DISPATCHIS_(\d{12})_\d{16}\.zip")
DAILY_RE = re.compile(r"PUBLIC_[A-Z_]+?_(\d{8})_\d{16}\.zip")
AEST = timezone(timedelta(hours=10))


def binary():
    name = "nem-monitor.exe" if os.name == "nt" else "nem-monitor"
    for d in ("release", "debug"):
        p = REPO / "target" / d / name
        if p.exists():
            return p
    sys.exit("build nem-monitor first")


def fetch(url, ua):
    req = urllib.request.Request(url, headers={"User-Agent": ua})
    with urllib.request.urlopen(req, timeout=60) as r:
        return r.read()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--files", type=int, default=12)
    ap.add_argument("--data-ready", action="store_true")
    ap.add_argument("--user-agent", default="nem-lens-monitor-canary (+https://github.com/nem-lens/monitor)")
    a = ap.parse_args()
    failures = []

    listing = fetch(f"{BASE}/DispatchIS_Reports/", a.user_agent).decode()
    files = sorted({m.group(0) for m in DISPATCH_RE.finditer(listing)})
    print(f"DispatchIS listing: {len(files)} files")
    if not files:
        sys.exit("FAIL no DispatchIS files in listing")
    newest = datetime.strptime(DISPATCH_RE.match(files[-1]).group(1), "%Y%m%d%H%M").replace(tzinfo=AEST)
    age = datetime.now(AEST) - newest
    print(f"newest {files[-1]} ({age.total_seconds() / 60:.1f} min old)")
    if age > timedelta(minutes=15):
        failures.append(f"newest DispatchIS file is {age} old")

    with tempfile.TemporaryDirectory(prefix="nem-canary-") as work:
        data = Path(work) / "dispatchis"
        data.mkdir()
        for f in files[-a.files:]:
            (data / f).write_bytes(fetch(f"{BASE}/DispatchIS_Reports/{f}", a.user_agent))
        cfg = Path(work) / "canary.toml"
        cfg.write_text(f"[store]\npath = '{Path(work) / 'canary.db'}'\n")
        out = subprocess.run([str(binary()), "-c", str(cfg), "replay", str(data)],
                             capture_output=True, text=True, env=dict(os.environ, RUST_LOG="nem_monitor=info"))
        log = out.stdout + out.stderr
        warns = [l for l in log.splitlines() if " WARN " in l or " ERROR " in l]
        if out.returncode != 0:
            failures.append(f"replay exited {out.returncode}: {log[-500:]}")
        if warns:
            failures.append(f"replay warnings: {warns[:3]}")
        db = sqlite3.connect(Path(work) / "canary.db")
        intervals = db.execute("SELECT COUNT(DISTINCT settlement) FROM region_intervals").fetchone()[0]
        regions = db.execute("SELECT COUNT(DISTINCT region) FROM region_intervals").fetchone()[0]
        nulls = db.execute("SELECT COUNT(*) FROM region_intervals WHERE demand IS NULL").fetchone()[0]
        db.close()
        print(f"replayed {a.files} files: {intervals} intervals, {regions} regions, {nulls} rows without demand")
        if intervals != a.files or regions != 5 or nulls:
            failures.append(f"expected {a.files} intervals x 5 regions with demand")

    if a.data_ready:
        now = datetime.now(AEST)
        # The trading day that ended at 04:00 today (market time).
        day = (now - timedelta(hours=4) - timedelta(days=1)).strftime("%Y%m%d")
        for d in DATA_READY_DIRS:
            days = set(DAILY_RE.findall(fetch(f"{BASE}/{d}/", a.user_agent).decode()))
            ok = day in days
            print(f"{d}: {len(days)} trading days listed; {day} {'present' if ok else 'MISSING'}")
            if not ok:
                failures.append(f"{d} has no file for trading day {day}")

    for f in failures:
        print(f"FAIL {f}")
    print("canary OK" if not failures else f"{len(failures)} failure(s)")
    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    main()
