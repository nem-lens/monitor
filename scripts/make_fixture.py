#!/usr/bin/env python3
"""Generate SYNTHETIC DispatchIS files for testing nem-monitor offline.

The NSW price path is shaped on features reported for the 12 October 2026 NSW
event (a peak of $23,107/MWh, 11 intervals above $5,000/MWh, prices whipsawing
from about 3pm AEDT). It is NOT AEMO data: interval-by-interval values,
demand, constraint and interconnector figures are invented for testing.

Usage: make_fixture.py OUTPUT_DIR
"""
import io
import math
import sys
import zipfile
from datetime import datetime, timedelta
from pathlib import Path

# NSW1 prices by interval end, market time (AEST). Shaped, not real.
NSW_SPIKE = {
    "14:00": 220, "14:05": 1300, "14:10": 766, "14:15": 23107, "14:20": 410,
    "14:25": 9800, "14:30": 1450, "14:35": 16900, "14:40": 880, "14:45": 12400,
    "14:50": 5400, "14:55": 15200, "15:00": 1900, "15:05": 8700, "15:10": 6300,
    "15:15": 5600, "15:20": 7400, "15:25": 5200, "15:30": 920, "15:35": 480,
    "15:40": 350,
}
BASE = {"NSW1": 180.0, "QLD1": 140.0, "VIC1": 95.0, "SA1": 110.0, "TAS1": 85.0}
DEMAND = {"NSW1": 10400.0, "QLD1": 7600.0, "VIC1": 5200.0, "SA1": 1500.0, "TAS1": 1050.0}


def rows_for(t: datetime, run: int):
    hhmm = t.strftime("%H:%M")
    mins = (t - t.replace(hour=12, minute=0)).total_seconds() / 60
    sd = t.strftime('"%Y/%m/%d %H:%M:%S"')
    di = t.strftime("%Y%m%d") + f"{run:03d}"
    prices, demand = {}, {}
    for r, b in BASE.items():
        wobble = 15 * math.sin(mins / 23 + len(r))
        prices[r] = round(b + wobble + max(0, mins - 60) * 0.4, 2)
        demand[r] = DEMAND[r] + 3 * mins
    if hhmm in NSW_SPIKE:
        prices["NSW1"] = float(NSW_SPIKE[hhmm])
    elif "15:45" <= hhmm:
        prices["NSW1"] = 290.0 - (mins - 225) * 0.5
    # NSW demand ramps by about 2,000 MW between 13:15 and 14:15.
    ramp = min(max(mins - 75, 0), 60) / 60 * 2000
    demand["NSW1"] = DEMAND["NSW1"] + ramp - max(0, mins - 240) * 4
    spiking = prices["NSW1"] >= 1000

    out = [
        "C,NEMP.WORLD,DISPATCHIS,AEMO,PUBLIC,{},{},0000000542{:06d},DISPATCHIS,0000000542{:06d}".format(
            t.strftime("%Y/%m/%d"), (t - timedelta(minutes=5)).strftime("%H:%M:13"), run, run),
        "I,DISPATCH,PRICE,5,SETTLEMENTDATE,RUNNO,REGIONID,DISPATCHINTERVAL,INTERVENTION,RRP,EEP,ROP,APCFLAG,MARKETSUSPENDEDFLAG,LASTCHANGED",
    ]
    for r, p in prices.items():
        out.append(f"D,DISPATCH,PRICE,5,{sd},1,{r},{di},0,{p},0,{p},0,0,{sd}")
    out.append("I,DISPATCH,REGIONSUM,9,SETTLEMENTDATE,RUNNO,REGIONID,DISPATCHINTERVAL,INTERVENTION,TOTALDEMAND,AVAILABLEGENERATION,AVAILABLELOAD,DEMANDFORECAST,NETINTERCHANGE,LASTCHANGED")
    for r, d in demand.items():
        avail = d * (1.04 if r == "NSW1" and spiking else 1.25)
        net = -1650.0 if r == "NSW1" else 300.0
        out.append(f"D,DISPATCH,REGIONSUM,9,{sd},1,{r},{di},0,{d:.1f},{avail:.1f},0,0,{net},{sd}")
    out.append("I,DISPATCH,INTERCONNECTORRES,3,SETTLEMENTDATE,RUNNO,INTERCONNECTORID,DISPATCHINTERVAL,INTERVENTION,METEREDMWFLOW,MWFLOW,MWLOSSES,MARGINALVALUE,VIOLATIONDEGREE,EXPORTLIMIT,IMPORTLIMIT,LASTCHANGED")
    ics = [
        ("NSW1-QLD1", -700.0 if spiking else -350.0, 600.0, -700.0),
        ("VIC1-NSW1", 950.0 if spiking else 600.0, 950.0, -1000.0),
        ("V-SA", 120.0, 600.0, -550.0),
        ("T-V-MNSP1", 300.0, 478.0, -478.0),
    ]
    for ic, flow, exp, imp in ics:
        mv = -2500.0 if spiking and ic in ("NSW1-QLD1", "VIC1-NSW1") else 0.0
        out.append(f"D,DISPATCH,INTERCONNECTORRES,3,{sd},1,{ic},{di},0,{flow},{flow},12.0,{mv},0,{exp},{imp},{sd}")
    out.append("I,DISPATCH,CONSTRAINT,5,SETTLEMENTDATE,RUNNO,CONSTRAINTID,DISPATCHINTERVAL,INTERVENTION,RHS,MARGINALVALUE,VIOLATIONDEGREE,LASTCHANGED,DUID,GENCONID_EFFECTIVEDATE,GENCONID_VERSIONNO,LHS")
    cons = [("SYNTH_N>>N_SOUTH_OUTAGE", -4800.0 if spiking else -40.0), ("SYNTH_V>>N_IMPORT", -2600.0 if spiking else 0.0), ("SYNTH_Q_STABILITY", 0.0)]
    for cid, mv in cons:
        out.append(f"D,DISPATCH,CONSTRAINT,5,{sd},1,{cid},{di},0,1200,{mv},0,{sd},,,,1200")
    out.append('C,"END OF REPORT",{}'.format(len(out) + 1))
    return "\n".join(out) + "\n"


def main():
    outdir = Path(sys.argv[1] if len(sys.argv) > 1 else "fixtures/2026-10-12-synthetic")
    outdir.mkdir(parents=True, exist_ok=True)
    t = datetime(2026, 10, 12, 12, 0)
    end = datetime(2026, 10, 12, 17, 30)
    run = 0
    while t <= end:
        run += 1
        name = f"PUBLIC_DISPATCHIS_{t:%Y%m%d%H%M}_0000000542{run:06d}"
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
            z.writestr(name + ".CSV", rows_for(t, run))
        (outdir / (name + ".zip")).write_bytes(buf.getvalue())
        t += timedelta(minutes=5)
    print(f"wrote {run} synthetic DispatchIS files to {outdir}")


if __name__ == "__main__":
    main()
