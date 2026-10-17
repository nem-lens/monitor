#!/usr/bin/env python3
"""End-to-end smoke test on the SYNTHETIC fixture (not AEMO data).

Builds nothing: run `cargo build` first (a debug build is required, because the
test registers a webhook on localhost, which only debug builds allow).

Starts `serve --replay` with a local webhook receiver and two triggers, then
checks every public route, the SSE stream, feeds, webhook signatures and
management, trigger placeholders/env/stdin, trigger timeouts, and that the
server logged no warnings. Exits non-zero on any failure.

Usage: e2e_smoke.py [--keep]
"""
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
import xml.etree.ElementTree as ET
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
EXE = REPO / "target" / "debug" / ("nem-monitor.exe" if os.name == "nt" else "nem-monitor")
PORT, HOOK_PORT = 18081, 18999
BASE = f"http://127.0.0.1:{PORT}"
ADMIN = "admin-token-0123456789"

if os.name == "nt":
    ECHO = ["powershell", "-NoProfile", "-Command",
            'Write-Output "id={event_id} env=$env:NEM_REGION"; $input | Select-Object -First 1']
    SLOW = ["powershell", "-NoProfile", "-Command", "Start-Sleep 30"]
else:
    ECHO = ["sh", "-c", "echo id=$0 env=$NEM_REGION; head -c 200", "{event_id}"]
    SLOW = ["sleep", "30"]

results = []


def check(name, ok, detail=""):
    results.append((name, bool(ok)))
    print(("PASS " if ok else "FAIL ") + name + (f"  [{detail}]" if detail and not ok else ""), flush=True)


def http(path, headers=None, method="GET", body=None):
    req = urllib.request.Request(BASE + path, data=body, headers=headers or {}, method=method)
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            return r.status, r.headers, r.read()
    except urllib.error.HTTPError as e:
        return e.code, e.headers, e.read()


def post_json(path, obj):
    return http(path, {"content-type": "application/json"}, "POST", json.dumps(obj).encode())


def toml_list(argv):
    return "[" + ", ".join(json.dumps(a) for a in argv) + "]"


def wait_for(pred, timeout):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            if pred():
                return True
        except Exception:
            pass
        time.sleep(0.5)
    return False


def main():
    if not EXE.exists():
        sys.exit(f"{EXE} not found; run `cargo build` first")
    work = Path(tempfile.mkdtemp(prefix="nem-e2e-"))
    fixture = work / "fixture"
    subprocess.run([sys.executable, "-I", str(REPO / "scripts" / "make_fixture.py"), str(fixture)], check=True)
    log = work / "hook.log"
    cfg = work / "e2e.toml"
    cfg.write_text(f"""
[server]
bind = "127.0.0.1:{PORT}"
public_url = "{BASE}"
[store]
path = {json.dumps(str(work / "e2e.db"))}
raw_dir = {json.dumps(str(work / "raw"))}
bundle_dir = {json.dumps(str(work / "bundles"))}
[public]
allow_http_webhooks = true
api_requests_per_ip_per_minute = 0   # this script polls faster than the default limit
[[triggers]]
name = "echo"
on = ["confirmed"]
command = {toml_list(ECHO)}
timeout_secs = 60
[[triggers]]
name = "slow"
on = ["closed"]
command = {toml_list(SLOW)}
timeout_secs = 3
""")
    env = dict(os.environ, NEM_TEST_ALLOW_PRIVATE_WEBHOOKS="1", NEM_ADMIN_TOKEN=ADMIN, RUST_LOG="nem_monitor=info")
    recv = subprocess.Popen([sys.executable, "-I", str(REPO / "scripts" / "test_receiver.py"), str(HOOK_PORT), str(log)])
    srv = subprocess.Popen([str(EXE), "-c", str(cfg), "serve", "--replay", str(fixture), "--replay-delay-ms", "150"],
                           env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    try:
        run_checks(log)
        check_bundle(work, fixture)
    finally:
        srv.terminate()
        recv.terminate()
        out = srv.communicate(timeout=15)[0]
        warns = [l for l in out.splitlines() if " WARN " in l or " ERROR " in l]
        check("no WARN/ERROR in server log", not warns, warns[:5])
        if "--keep" in sys.argv:
            print(f"kept {work}")
        else:
            shutil.rmtree(work, ignore_errors=True)
    failed = [n for n, ok in results if not ok]
    print(f"\n{len(results) - len(failed)}/{len(results)} passed")
    sys.exit(1 if failed else 0)


def run_checks(log):
    check("server starts", wait_for(lambda: http("/healthz")[0] == 200, 30))

    # Register before the event arrives (replay starts 2 s after launch).
    st, _, b = post_json("/api/webhooks", {"url": f"http://127.0.0.1:{HOOK_PORT}/hook", "regions": ["NSW1"], "min_severity": "high"})
    reg = json.loads(b)
    check("webhook registers (201) after challenge echo", st == 201 and reg.get("secret", "").startswith("whsec_"), b)
    Path(str(log) + ".secret").write_text(reg.get("secret", ""))
    check("bad min_severity rejected 400", post_json("/api/webhooks", {"url": "https://127.0.0.1/x", "min_severity": "bogus"})[0] == 400)
    check("non-https scheme rejected 400", post_json("/api/webhooks", {"url": "ftp://x"})[0] == 400)
    check("unknown region rejected 400", post_json("/api/webhooks", {"url": "https://127.0.0.1/x", "regions": ["NSW"]})[0] == 400)

    sse = urllib.request.urlopen(BASE + "/api/stream", timeout=15)
    frames = [sse.readline().decode() for _ in range(12)]
    sse.close()
    check("SSE carries interval events", any(f.startswith("event: interval") for f in frames), frames)

    def runs():
        st, _, b = http("/api/admin/runs", {"Authorization": f"Bearer {ADMIN}"})
        return json.loads(b)["runs"] if st == 200 else []

    def finished():
        r = {x["trigger"]: x for x in runs()}
        lines = log.read_text().splitlines() if log.exists() else []
        return (all(r.get(t, {}).get("finished_at") for t in ("echo", "slow"))
                and len([l for l in lines if l.startswith("event.")]) >= 3)

    check("replay, deliveries and triggers finish", wait_for(finished, 90))

    s = json.loads(http("/api/status")[2])
    check("status lists 5 regions with series", len(s["regions"]) == 5 and len(s["regions"][0]["series"]) > 0)
    check("status reports fresh in replay mode", s.get("fresh") is True)
    evs = json.loads(http("/api/events")[2])["events"]
    check("exactly one event listed", len(evs) == 1, [e["id"] for e in evs])
    ev = evs[0]
    check("event peak 23107 and 11 intervals above high", ev["peak_price"] == 23107.0 and ev["intervals_above_high"] == 11)
    st, _, b = http(f"/api/events/{ev['id']}")
    d = json.loads(b)
    transitions = [n["transition"] for n in d["notifications"]]
    check("event detail has series and public notifications only", st == 200 and len(d["series"]) > 20 and transitions == ["confirmed", "flagged", "closed"], transitions)
    check("unknown event 404", http("/api/events/nope")[0] == 404)
    check("notifications are public only", all(n["public"] for n in json.loads(http("/api/notifications")[2])["notifications"]))
    st, h, b = http("/feed.atom")
    entries = ET.fromstring(b).findall("{http://www.w3.org/2005/Atom}entry")
    check("atom parses with 3 entries", h["content-type"].startswith("application/atom+xml") and len(entries) == 3)
    jf = json.loads(http("/feed.json")[2])
    check("json feed v1.1 with 3 items", jf["version"].endswith("1.1") and len(jf["items"]) == 3)
    st, _, b = http("/")
    check("index renders with title substituted", st == 200 and b"{{SITE_TITLE}}" not in b and b"NEM Event Monitor" in b)
    check("admin runs 404 without token", http("/api/admin/runs")[0] == 404)

    r = {x["trigger"]: x for x in runs()}
    echo, slow = r.get("echo") or {}, r.get("slow") or {}
    tail = echo.get("output_tail") or ""
    check("echo trigger ran with placeholder, env and stdin JSON",
          echo.get("exit_code") == 0 and f"id={ev['id']} env=NSW1" in tail and '"seq"' in tail, echo)
    check("slow trigger killed on timeout", slow.get("exit_code") is None and "timed out" in (slow.get("output_tail") or ""), slow)

    lines = log.read_text().splitlines() if log.exists() else []
    check("receiver saw verification", any("verification" in l for l in lines), lines)
    deliveries = [l for l in lines if l.startswith("event.")]
    check("3 deliveries, all signatures valid", len(deliveries) == 3 and all("signature_valid=True" in l for l in deliveries), deliveries)
    hid, tok = reg["id"], reg["manage_token"]
    check("test delivery with wrong token 404", http(f"/api/webhooks/{hid}/test", {"Authorization": "Bearer mt_wrong"}, "POST")[0] == 404)
    check("test delivery with token 200", http(f"/api/webhooks/{hid}/test", {"Authorization": f"Bearer {tok}"}, "POST")[0] == 200)
    check("delete without token 401", http(f"/api/webhooks/{hid}", method="DELETE")[0] == 401)
    check("delete with token 204", http(f"/api/webhooks/{hid}", {"Authorization": f"Bearer {tok}"}, "DELETE")[0] == 204)


def check_bundle(work, fixture):
    ev = json.loads(http("/api/events")[2])["events"][0]
    d = json.loads(http(f"/api/events/{ev['id']}")[2])
    check("event detail is served from the bundle", d.get("series_source") == "bundle" and d["bundle"]["status"] == "written", d.get("bundle"))
    bdir = work / "bundles" / ev["id"]
    m = json.loads((bdir / "manifest.json").read_text()) if (bdir / "manifest.json").exists() else {}
    files = sorted(o["file"] for o in m.get("outputs", []))
    check("bundle has the four DispatchIS tables",
          files == ["dispatch_constraint.parquet", "dispatch_interconnectorres.parquet", "dispatch_price.parquet", "dispatch_regionsum.parquet"], files)
    check("bundle window covers every interval", m.get("intervals_found") == m.get("intervals_expected") and m.get("intervals_found", 0) > 40, m.get("notes"))
    out = subprocess.run([sys.executable, "-I", str(REPO / "scripts" / "bundle_check.py"), str(bdir), "--sources", str(fixture),
                          "--expect-peak", "23107", "--expect-above", "11"], capture_output=True, text=True)
    check("bundle hashes match and DuckDB reproduces the peak", out.returncode == 0, out.stdout[-600:])
    if "duckdb not installed" in out.stdout:
        print("  (duckdb not installed: hash checks only)")


if __name__ == "__main__":
    main()
