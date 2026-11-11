#!/usr/bin/env python3
"""Live dashboard for the pub/sub + SDN demo (version 1: monitoring).

Run from the repo root (no sudo needed):
    python3 ui/app.py
then open http://localhost:8000 in Firefox.

It shows
  * the triangle network, with link up/down state from the controller
  * the HIGH / NORMAL traffic counters from the controller's GET /stats
  * a live message feed read from the broker's log (results/events.jsonl if it
    exists, otherwise results/broker.log or the newest results/d2/*/broker.log)
  * the results of failover runs made by tools/scenario.py (summary + latency chart)

Nothing here changes the system: it only reads files and calls GET /stats.
"""
import argparse
import csv
import glob
import json
import os
import re
import time
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs

ARGS = None

# ------------------------------------------------------------------ broker events
LOG_LINE = re.compile(r"^\[(?P<ts>[^\]]+)\]\s+(?P<msg>.*)$")


def newest(paths):
    paths = [p for p in paths if os.path.isfile(p)]
    return max(paths, key=os.path.getmtime) if paths else None


def event_source():
    r = ARGS.results
    jsonl = os.path.join(r, "events.jsonl")
    logs = [os.path.join(r, "broker.log")] + glob.glob(os.path.join(r, "d2", "*", "broker.log"))
    if os.path.isfile(jsonl) and os.path.getsize(jsonl) > 0:
        return jsonl
    return newest(logs)


HOSTS = {"10.0.0.1": "pub1", "10.0.0.2": "pub2", "10.0.0.3": "broker", "10.0.0.4": "sub1", "10.0.0.5": "sub2"}


def pretty_client(text):
    """'(\'10.0.0.4\', 57916)' -> 'sub1'"""
    m = re.search(r"\d+\.\d+\.\d+\.\d+", text or "")
    return HOSTS.get(m.group(0), m.group(0)) if m else (text or "")


def parse_log_line(line):
    m = LOG_LINE.match(line.strip())
    if not m:
        return None
    ts, msg = m.group("ts"), m.group("msg")
    ev = {"t": ts[-8:], "event": "info", "topic": "", "client": "", "delivered": None, "text": msg}
    p = re.match(r"Published: (\S+) from (.*?), delivered to (\d+) subscribers?", msg)
    if p:
        ev.update(event="publish", topic=p.group(1), client=p.group(2), delivered=int(p.group(3)))
        return ev
    p = re.match(r"(Subscribed|Unsubscribed): (.*?) (?:to|from) topic '(.*)'", msg)
    if p:
        ev.update(event=p.group(1).lower().replace("subscribed", "subscribe"), client=p.group(2), topic=p.group(3))
        ev["event"] = "subscribe" if p.group(1) == "Subscribed" else "unsubscribe"
        return ev
    if msg.startswith("Disconnected"):
        ev.update(event="disconnect", client=msg.split(":", 1)[-1].strip())
    elif msg.startswith("Connected") or msg.startswith("New connection"):
        ev.update(event="connect", client=msg.split(":", 1)[-1].strip())
    return ev


def read_events(limit=200):
    path = event_source()
    if not path:
        return {"source": None, "total": 0, "events": []}
    events = []
    with open(path, errors="replace") as f:
        lines = f.readlines()
    for line in lines:
        line = line.strip()
        if not line:
            continue
        if path.endswith(".jsonl"):
            try:
                d = json.loads(line)
            except ValueError:
                continue
            events.append({
                "t": time.strftime("%H:%M:%S", time.localtime(d.get("ts", 0))),
                "event": d.get("event", "info"), "topic": d.get("topic", ""),
                "client": d.get("client_ip", ""), "delivered": d.get("delivered"), "text": "",
            })
        else:
            ev = parse_log_line(line)
            if ev:
                events.append(ev)
    for ev in events:
        ev["client"] = pretty_client(ev["client"])
    return {"source": os.path.relpath(path), "total": len(events), "events": events[-limit:]}


# ------------------------------------------------------------------ controller stats
def read_stats():
    try:
        with urllib.request.urlopen(ARGS.controller.rstrip("/") + "/stats", timeout=1.5) as r:
            return {"ok": True, "data": json.loads(r.read().decode())}
    except Exception as e:  # unreachable, bad JSON, timeout...
        return {"ok": False, "error": str(e)}


# ------------------------------------------------------------------ failover runs
def list_runs():
    runs = []
    for d in sorted(glob.glob(os.path.join(ARGS.results, "d2", "*"))):
        if os.path.isfile(os.path.join(d, "meta.json")):
            runs.append(os.path.basename(d))
    return runs


def read_csv(path):
    rows = []
    try:
        with open(path, newline="") as f:
            for r in csv.DictReader(f):
                rows.append(r)
    except OSError:
        pass
    return rows


def thin(points, cap=600):
    if len(points) <= cap:
        return points
    step = len(points) / cap
    return [points[int(i * step)] for i in range(cap)]


def read_run(name):
    d = os.path.join(ARGS.results, "d2", os.path.basename(name))
    try:
        meta = json.load(open(os.path.join(d, "meta.json")))
    except (OSError, ValueError):
        return {"error": "run not found"}
    pub = read_csv(os.path.join(d, "pub.csv"))
    sub = read_csv(os.path.join(d, "sub.csv"))
    send = {}
    for r in pub:
        try:
            send[int(r["seq"])] = float(r["send_ts"])
        except (KeyError, ValueError):
            pass
    got = {}
    for r in sub:
        try:
            got.setdefault(int(r["seq"]), float(r["latency_ms"]))
        except (KeyError, ValueError):
            pass
    t0 = min(send.values()) if send else meta.get("t0", 0)
    received = sorted((round(send[s] - t0, 3), round(l, 2)) for s, l in got.items() if s in send)
    lost = sorted(round(send[s] - t0, 3) for s in send if s not in got)
    summary = ""
    try:
        summary = open(os.path.join(d, "summary.txt")).read()
    except OSError:
        pass
    return {
        "name": name, "mode": meta.get("mode"), "summary": summary,
        "t_down": round(meta["t_down"] - t0, 3), "t_up": round(meta["t_up"] - t0, 3),
        "duration": meta.get("duration"), "received": thin(received), "lost": thin(lost),
    }


# ------------------------------------------------------------------ HTTP
class Handler(BaseHTTPRequestHandler):
    def log_message(self, *a):  # keep the terminal quiet
        pass

    def send_json(self, obj):
        body = json.dumps(obj).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        u = urlparse(self.path)
        q = parse_qs(u.query)
        if u.path in ("/", "/index.html"):
            body = PAGE.encode()
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        elif u.path == "/favicon.ico":
            self.send_response(204)
            self.end_headers()
        elif u.path == "/api/events":
            self.send_json(read_events())
        elif u.path == "/api/stats":
            self.send_json(read_stats())
        elif u.path == "/api/runs":
            self.send_json(list_runs())
        elif u.path == "/api/run":
            self.send_json(read_run(q.get("name", [""])[0]))
        else:
            self.send_response(404)
            self.end_headers()


PAGE = r"""<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Pub/Sub SDN Demo</title>
<style>
:root{--bg:#f4f5f7;--card:#fff;--text:#1b1f24;--muted:#6a737d;--line:#d8dce1;--high:#d9480f;--normal:#1c7ed6;--ok:#2b8a3e;--bad:#c92a2a;--soft:#eef1f5}
@media (prefers-color-scheme:dark){:root{--bg:#14171a;--card:#1d2125;--text:#e8eaed;--muted:#9aa4ae;--line:#33393f;--high:#ff8c5a;--normal:#74b0f0;--ok:#5fcf80;--bad:#ff6b6b;--soft:#272c31}}
*{box-sizing:border-box}
body{margin:0;background:var(--bg);color:var(--text);font:15px/1.45 system-ui,-apple-system,"Segoe UI",Roboto,sans-serif}
header{display:flex;flex-wrap:wrap;gap:12px;align-items:center;justify-content:space-between;padding:16px;max-width:1200px;margin:0 auto}
h1{font-size:20px;margin:0}
h2{font-size:13px;margin:0 0 10px;text-transform:uppercase;letter-spacing:.06em;color:var(--muted)}
.pills{display:flex;gap:8px;flex-wrap:wrap}
.pill{padding:3px 10px;border-radius:999px;background:var(--soft);font-size:13px;color:var(--muted)}
.pill.ok{color:var(--ok)}.pill.bad{color:var(--bad)}
main{max-width:1200px;margin:0 auto;padding:0 16px 24px;display:grid;grid-template-columns:1fr 1fr;gap:16px}
.card{background:var(--card);border:1px solid var(--line);border-radius:10px;padding:14px;min-width:0}
.wide{grid-column:1/-1}
@media (max-width:820px){main{grid-template-columns:1fr}}
svg{width:100%;height:auto;display:block}
.classes{display:grid;grid-template-columns:1fr 1fr;gap:10px}
.cls{background:var(--soft);border-radius:8px;padding:10px}
.cls b{display:block;font-size:13px}
.cls .n{font-size:26px;font-weight:600;margin-right:6px}
.cls .r{color:var(--muted);font-size:13px}
.cls.HIGH b{color:var(--high)}.cls.NORMAL b{color:var(--normal)}
#feed{list-style:none;margin:0;padding:0;max-height:300px;overflow:auto}
#feed li{display:flex;gap:10px;align-items:baseline;padding:5px 0;border-bottom:1px solid var(--line);font-size:14px}
#feed .t{color:var(--muted);font-variant-numeric:tabular-nums;min-width:62px}
.tag{padding:1px 8px;border-radius:999px;font-size:12px;background:var(--soft)}
.tag.alerts{background:color-mix(in srgb,var(--high) 18%,transparent);color:var(--high)}
.tag.other{background:color-mix(in srgb,var(--normal) 18%,transparent);color:var(--normal)}
.muted{color:var(--muted)}
select,summary{font:inherit;color:inherit}
select{background:var(--soft);border:1px solid var(--line);border-radius:6px;padding:4px 8px}
pre{background:var(--soft);border-radius:8px;padding:10px;overflow:auto;font-size:13px;margin:10px 0 0;white-space:pre-wrap}
.empty{color:var(--muted);padding:8px 0}
.legend{display:flex;gap:14px;flex-wrap:wrap;font-size:13px;color:var(--muted);margin-top:6px}
.legend i{display:inline-block;width:18px;height:0;border-top:3px solid;vertical-align:middle;margin-right:5px}
</style></head><body>
<header>
  <h1>Pub/Sub over SDN: live demo</h1>
  <div class="pills"><span id="pill-ctrl" class="pill">controller: checking</span><span id="pill-feed" class="pill">broker log: checking</span></div>
</header>
<main>
  <section class="card"><h2>Network</h2><svg id="topo" viewBox="0 0 610 420" role="img" aria-label="Network topology"></svg>
    <div class="legend"><span><i style="border-color:var(--ok)"></i>link up</span><span><i style="border-color:var(--bad);border-top-style:dashed"></i>link down</span><span><i style="border-color:var(--muted)"></i>unknown</span></div></section>
  <section class="card"><h2>Traffic classes (from the controller)</h2>
    <div class="classes" id="classes"><div class="empty">waiting for the controller…</div></div>
    <div id="extra" class="muted" style="margin-top:12px"></div>
    <details style="margin-top:10px"><summary class="muted">raw /stats</summary><pre id="raw"></pre></details></section>
  <section class="card wide"><h2>Message feed</h2><ul id="feed"><li class="empty">no events yet</li></ul></section>
  <section class="card wide"><h2>Failover run</h2>
    <label class="muted">run <select id="runs"></select></label>
    <pre id="summary">No runs found. Create one with tools/scenario.py.</pre>
    <svg id="chart" viewBox="0 0 800 280" role="img" aria-label="Latency during the failover run"></svg></section>
</main>
<script>
const $ = s => document.querySelector(s);
const NS = "http://www.w3.org/2000/svg";
const get = async u => (await fetch(u, {cache: "no-store"})).json();
const el = (tag, attrs, text) => { const e = document.createElementNS(NS, tag); for (const k in attrs) e.setAttribute(k, attrs[k]); if (text !== undefined) e.textContent = text; return e; };

/* ---------- topology ---------- */
const NODES = {pub1:[45,70], pub2:[45,230], s1:[170,150], s3:[440,150], s2:[305,300], broker:[565,150], sub1:[215,385], sub2:[395,385]};
const SWITCH = new Set(["s1","s2","s3"]);
const LINKS = [["pub1","s1"],["pub2","s1"],["s1","s3"],["s1","s2"],["s2","s3"],["broker","s3"],["sub1","s2"],["sub2","s2"]];
let linkState = {};
function key(a, b) { return [a, b].sort().join("-"); }
function normaliseLinks(data) {
  const out = {};
  const list = Array.isArray(data.links) ? data.links : [];
  for (const l of list) {
    const a = l.a || l.src || l.from, b = l.b || l.dst || l.to;
    if (!a || !b) continue;
    let up = l.up;
    if (up === undefined && l.state !== undefined) up = String(l.state).toLowerCase() === "up";
    if (up === undefined && l.status !== undefined) up = String(l.status).toLowerCase() === "up";
    if (up !== undefined) out[key(String(a), String(b))] = !!up;
  }
  return out;
}
function drawTopo() {
  const svg = $("#topo"); svg.replaceChildren();
  for (const [a, b] of LINKS) {
    const s = linkState[key(a, b)];
    const color = s === undefined ? "var(--muted)" : s ? "var(--ok)" : "var(--bad)";
    const isCore = SWITCH.has(a) && SWITCH.has(b);
    const l = el("line", {x1: NODES[a][0], y1: NODES[a][1], x2: NODES[b][0], y2: NODES[b][1], stroke: color, "stroke-width": isCore ? 5 : 2.5, "stroke-linecap": "round"});
    if (s === false) l.setAttribute("stroke-dasharray", "8 7");
    svg.appendChild(l);
  }
  for (const n in NODES) {
    const [x, y] = NODES[n], sw = SWITCH.has(n);
    const g = el("g", {});
    if (sw) g.appendChild(el("rect", {x: x - 26, y: y - 18, width: 52, height: 36, rx: 8, fill: "var(--soft)", stroke: "var(--text)", "stroke-width": 1.5}));
    else g.appendChild(el("circle", {cx: x, cy: y, r: 29, fill: "var(--card)", stroke: n === "broker" ? "var(--high)" : "var(--muted)", "stroke-width": n === "broker" ? 2.5 : 1.5}));
    g.appendChild(el("text", {x: x, y: y + 5, "text-anchor": "middle", "font-size": sw ? 14 : 13, fill: "var(--text)"}, n));
    svg.appendChild(g);
  }
  const note = el("text", {x: 305, y: 118, "text-anchor": "middle", "font-size": 12, fill: "var(--muted)"}, "primary path");
  svg.appendChild(note);
}

/* ---------- controller stats ---------- */
let lastCls = {}, lastT = 0;
async function pollStats() {
  const r = await get("/api/stats");
  const pill = $("#pill-ctrl");
  if (!r.ok) {
    pill.textContent = "controller: not reachable"; pill.className = "pill bad";
    return;
  }
  pill.textContent = "controller: connected"; pill.className = "pill ok";
  const d = r.data; $("#raw").textContent = JSON.stringify(d, null, 2);
  linkState = normaliseLinks(d); drawTopo();
  const ex = $("#extra"); ex.replaceChildren();
  if (d.mode) { const m = document.createElement("div"); m.innerHTML = "controller mode: <b style='color:var(--text)'>" + d.mode + "</b>"; ex.appendChild(m); }
  for (const k in linkState) { const row = document.createElement("div"); row.innerHTML = "link " + k.replace("-", " &ndash; ") + ": <b style='color:var(--" + (linkState[k] ? "ok" : "bad") + ")'>" + (linkState[k] ? "up" : "DOWN") + "</b>"; ex.appendChild(row); }
  const box = $("#classes"); const now = Date.now() / 1000;
  const classes = d.classes || {};
  const names = Object.keys(classes);
  if (!names.length) { box.innerHTML = '<div class="empty">no class counters in /stats yet</div>'; return; }
  box.replaceChildren();
  for (const n of names) {
    const pk = Number((classes[n] || {}).packets || 0);
    const rate = lastT && lastCls[n] !== undefined ? Math.max(0, (pk - lastCls[n]) / (now - lastT)) : 0;
    const c = document.createElement("div"); c.className = "cls " + n;
    c.innerHTML = "<b>" + n + "</b><span class='n'>" + pk.toLocaleString() + "</span><span class='r'>packets &middot; " + rate.toFixed(1) + "/s</span>";
    box.appendChild(c); lastCls[n] = pk;
  }
  lastT = now;
}

/* ---------- message feed ---------- */
let lastTotal = -1;
async function pollEvents() {
  const d = await get("/api/events");
  const pill = $("#pill-feed");
  if (!d.source) { pill.textContent = "broker log: none found"; pill.className = "pill"; return; }
  pill.textContent = "broker log: " + d.source; pill.className = "pill ok";
  if (d.total === lastTotal) return;
  lastTotal = d.total;
  const ul = $("#feed"); ul.replaceChildren();
  if (!d.events.length) { ul.innerHTML = '<li class="empty">no events yet</li>'; return; }
  for (const e of d.events.slice().reverse()) {
    const li = document.createElement("li");
    const t = document.createElement("span"); t.className = "t"; t.textContent = e.t; li.appendChild(t);
    if (e.topic) { const g = document.createElement("span"); g.className = "tag " + (e.topic === "alerts" ? "alerts" : "other"); g.textContent = e.topic; li.appendChild(g); }
    const m = document.createElement("span");
    if (e.event === "publish") m.textContent = "published by " + e.client + ", delivered to " + e.delivered + (e.delivered === 1 ? " subscriber" : " subscribers");
    else if (e.event === "subscribe") m.textContent = e.client + " subscribed";
    else if (e.event === "unsubscribe") m.textContent = e.client + " unsubscribed";
    else if (e.event === "disconnect") m.textContent = e.client + " disconnected";
    else if (e.event === "connect") m.textContent = e.client + " connected";
    else m.textContent = e.text || e.event;
    if (e.event === "disconnect" || e.event === "info") m.className = "muted";
    li.appendChild(m); ul.appendChild(li);
  }
}

/* ---------- failover runs ---------- */
async function loadRuns() {
  const runs = await get("/api/runs"); const sel = $("#runs");
  const cur = sel.value; sel.replaceChildren();
  for (const r of runs) { const o = document.createElement("option"); o.value = o.textContent = r; sel.appendChild(o); }
  if (runs.includes(cur)) sel.value = cur; else if (runs.length) sel.value = runs[runs.length - 1];
  if (runs.length) showRun(sel.value);
}
async function showRun(name) {
  const d = await get("/api/run?name=" + encodeURIComponent(name));
  if (d.error) { $("#summary").textContent = d.error; return; }
  $("#summary").textContent = d.summary || "(no summary.txt in this run)";
  drawChart(d);
}
function drawChart(d) {
  const svg = $("#chart"); svg.replaceChildren();
  const W = 800, H = 280, L = 52, R = 14, T = 14, B = 38;
  const xmax = Math.max(d.duration || 0, ...d.received.map(p => p[0]), ...d.lost, 1);
  const ymax = Math.max(10, ...d.received.map(p => p[1])) * 1.1;
  const X = v => L + (v / xmax) * (W - L - R), Y = v => H - B - (v / ymax) * (H - T - B);
  svg.appendChild(el("rect", {x: X(d.t_down), y: T, width: Math.max(1, X(d.t_up) - X(d.t_down)), height: H - T - B, fill: "var(--bad)", opacity: 0.12}));
  svg.appendChild(el("text", {x: X(d.t_down) + 6, y: T + 14, "font-size": 12, fill: "var(--bad)"}, "link down"));
  for (let i = 0; i <= 4; i++) {
    const v = ymax * i / 4, y = Y(v);
    svg.appendChild(el("line", {x1: L, x2: W - R, y1: y, y2: y, stroke: "var(--line)"}));
    svg.appendChild(el("text", {x: L - 6, y: y + 4, "text-anchor": "end", "font-size": 11, fill: "var(--muted)"}, v.toFixed(0)));
  }
  for (let v = 0; v <= xmax; v += Math.max(5, Math.ceil(xmax / 8 / 5) * 5)) {
    svg.appendChild(el("text", {x: X(v), y: H - B + 16, "text-anchor": "middle", "font-size": 11, fill: "var(--muted)"}, v + "s"));
  }
  svg.appendChild(el("text", {x: 14, y: (H - B) / 2, "font-size": 11, fill: "var(--muted)", transform: "rotate(-90 14 " + (H - B) / 2 + ")", "text-anchor": "middle"}, "latency (ms)"));
  svg.appendChild(el("text", {x: (L + W - R) / 2, y: H - 6, "font-size": 11, fill: "var(--muted)", "text-anchor": "middle"}, "time since the first message"));
  for (const [x, y] of d.received) svg.appendChild(el("circle", {cx: X(x), cy: Y(y), r: 2.4, fill: "var(--normal)"}));
  for (const x of d.lost) svg.appendChild(el("line", {x1: X(x), x2: X(x), y1: H - B - 12, y2: H - B, stroke: "var(--bad)", "stroke-width": 1.5}));
  svg.appendChild(el("text", {x: W - R, y: T + 14, "text-anchor": "end", "font-size": 12, fill: "var(--muted)"}, "dots = delivered   red ticks = lost"));
}
$("#runs").addEventListener("change", e => showRun(e.target.value));

drawTopo(); pollStats(); pollEvents(); loadRuns();
setInterval(pollStats, 1500); setInterval(pollEvents, 1000); setInterval(loadRuns, 15000);
</script></body></html>
"""


def main():
    global ARGS
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--port", type=int, default=8000)
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--controller", default="http://127.0.0.1:8080", help="Ryu REST address (from the VM itself)")
    p.add_argument("--results", default="results")
    ARGS = p.parse_args()
    srv = ThreadingHTTPServer((ARGS.host, ARGS.port), Handler)
    print(f"Dashboard on http://{ARGS.host}:{ARGS.port}  (Ctrl+C to stop)")
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
