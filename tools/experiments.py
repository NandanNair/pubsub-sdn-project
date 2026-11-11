#!/usr/bin/env python3
"""D2 performance evaluation: experiments E1-E4 from docs/design_d2.md section 7.

Subcommands
  run      run one experiment (or all) inside Mininet  (needs sudo and Ryu running)
  pub      probe publisher  (started on a Mininet host by `run`)
  sub      probe subscriber (started on a Mininet host by `run`)
  summary  turn every finished run into results/d2/summary.csv and aggregate.csv
  plot     draw the graphs into results/d2/graphs/ (needs matplotlib)

Experiments
  E1  static mode, no failure, rates 10 / 50 / 100 / 200 msg/s          (baseline)
  E2  dynamic mode, same rates
  E3  link s1-s3 fails at 20 s and comes back at 40 s, static vs dynamic
  E4  HIGH (alerts, 5001) and NORMAL (news, 5002) at the same time on a
      bandwidth-limited triangle, static vs dynamic

Run from the repo root, with Ryu (controller/app.py) already running:
  sudo python3 tools/experiments.py run --exp all
  sudo chown -R $USER:$USER results
  python3 tools/experiments.py summary
  python3 tools/experiments.py plot

The controller mode is switched for you through POST /mode, so Ryu only has to be
started once. Each run builds a fresh triangle, so do NOT run `sudo mn -c` while
Ryu is running.

Where each component runs
  pub1 -> HIGH publisher      sub1 -> HIGH subscriber
  pub2 -> NORMAL publisher    sub2 -> NORMAL subscriber
  broker host -> broker on 5001 and a second broker process on 5002
Both publishers sit on s1, so in E4 their traffic competes for the s1-s3 link.

Latency is measured at the subscriber: receive time minus the publish time
carried in the message. All Mininet hosts share the VM clock, so the two are
directly comparable.
"""
import argparse
import csv
import json
import math
import os
import socket
import sys
import threading
import time
import urllib.request

BROKER_IP = "10.0.0.3"
DEFAULT_CONTROLLER = "http://127.0.0.1:8080"  # the VM's own view of Ryu
RESULTS = os.path.join("results", "d2")

# class -> (topic, broker port, publisher host, subscriber host)
CLASSES = {
    "HIGH": ("alerts", 5001, "pub1", "sub1"),
    "NORMAL": ("news", 5002, "pub2", "sub2"),
}

EXPERIMENTS = {
    "E1": {"modes": ["static"], "classes": ["HIGH"], "failure": False},
    "E2": {"modes": ["dynamic"], "classes": ["HIGH"], "failure": False},
    "E3": {"modes": ["static", "dynamic"], "classes": ["HIGH"], "failure": True},
    "E4": {"modes": ["static", "dynamic"], "classes": ["HIGH", "NORMAL"], "failure": False},
}


# ------------------------------------------------------------------- probes
def connect(host, port, retries=20):
    """Connect with retries, because the broker may still be starting."""
    last = None
    for _ in range(retries):
        try:
            return socket.create_connection((host, port), timeout=3)
        except OSError as e:
            last = e
            time.sleep(0.5)
    raise last


def cmd_pub(a):
    """Publish `count` numbered messages at a fixed rate and log each send time.

    The payload is the sequence number, so the subscriber can tell which
    messages were lost. `--pad` adds filler bytes to make each message bigger,
    which is how E4 fills a bandwidth-limited link.
    """
    state = {"sock": connect(a.host, a.port)}
    stop = threading.Event()
    pad = "x" * a.pad if a.pad else None

    def drain():
        # Read and throw away the broker's ACKs so they never pile up. Print ERRORs.
        buf = b""
        while not stop.is_set():
            try:
                data = state["sock"].recv(65536)
            except OSError:
                time.sleep(0.2)
                continue
            if not data:
                time.sleep(0.2)
                continue
            buf += data
            while b"\n" in buf:
                line, buf = buf.split(b"\n", 1)
                if b'"ERROR"' in line:
                    print("broker:", line.decode(errors="replace"), file=sys.stderr, flush=True)

    threading.Thread(target=drain, daemon=True).start()

    start = time.time()
    with open(a.out, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["seq", "send_ts", "ok"])
        for seq in range(a.count):
            wait = start + seq / a.rate - time.time()  # absolute schedule: no drift
            if wait > 0:
                time.sleep(wait)
            ts = time.time()
            msg = {"type": "PUBLISH", "topic": a.topic, "payload": str(seq), "ts": ts}
            if pad:
                msg["pad"] = pad
            ok = 1
            try:
                state["sock"].sendall((json.dumps(msg) + "\n").encode())
            except OSError:
                ok = 0
                try:
                    state["sock"].close()
                    state["sock"] = connect(a.host, a.port, retries=1)
                except OSError:
                    pass
            w.writerow([seq, f"{ts:.6f}", ok])
            f.flush()
    time.sleep(1)
    stop.set()


def cmd_sub(a):
    """Subscribe to one topic and log the arrival time and latency of every message."""
    sock = connect(a.host, a.port)
    sock.settimeout(None)
    sock.sendall((json.dumps({"type": "SUBSCRIBE", "topic": a.topic}) + "\n").encode())
    decoder = json.JSONDecoder()
    buf = ""
    with open(a.out, "w", newline="") as out:
        w = csv.writer(out)
        w.writerow(["seq", "recv_ts", "latency_ms"])
        out.flush()
        while True:
            data = sock.recv(65536)
            if not data:
                break
            now = time.time()
            buf += data.decode("utf-8", errors="replace")
            while True:
                buf = buf.lstrip()
                if not buf:
                    break
                try:
                    m, end = decoder.raw_decode(buf)
                except ValueError:
                    break  # incomplete object, wait for more bytes
                buf = buf[end:]
                if not isinstance(m, dict):
                    continue
                if m.get("type") == "ERROR":
                    print("broker:", json.dumps(m), file=sys.stderr, flush=True)
                    continue
                if m.get("type") not in ("DELIVER", "PUBLISH"):
                    continue
                try:
                    seq = int(m["payload"])
                    latency = (now - float(m.get("ts", now))) * 1000
                except (ValueError, KeyError, TypeError):
                    continue
                w.writerow([seq, f"{now:.6f}", f"{latency:.3f}"])
                out.flush()


# ------------------------------------------------------------- controller
def set_mode(url, mode):
    """Switch Ryu between static and dynamic through its REST API."""
    req = urllib.request.Request(url.rstrip("/") + "/mode", data=json.dumps({"mode": mode}).encode(),
                                 headers={"Content-Type": "application/json"}, method="POST")
    with urllib.request.urlopen(req, timeout=3) as r:
        reply = json.loads(r.read().decode() or "{}")
    if reply.get("mode") != mode:
        raise RuntimeError(f"controller did not switch to {mode}: {reply}")


# --------------------------------------------------------------------- run
def run_one(outdir, mode, classes, rate, duration, bw, pad, failure, settle):
    """Build a fresh triangle, run the probes for the given classes, save CSVs and meta.json."""
    from topo_triangle import build_network  # imported here so `summary` works without Mininet

    os.makedirs(outdir, exist_ok=True)
    count = int(rate * duration)
    net = build_network(bw=bw)
    meta = {"mode": mode, "classes": classes, "rate": rate, "duration": duration,
            "count": count, "bw": bw, "pad": pad, "link": None, "t_down": None, "t_up": None}
    me = "python3 -u tools/experiments.py"
    try:
        time.sleep(settle)  # let link discovery finish
        # Wait until every host can reach every other host (this also teaches the
        # controller where all hosts are), the same as a manual pingall.
        hosts = [net.get(h) for h in ("broker", "pub1", "pub2", "sub1", "sub2")]
        start_wait = time.time()
        while True:
            loss = net.ping(hosts, timeout=1)
            if loss == 0:
                print(f"    network ready: pingall 0% loss after {time.time() - start_wait:.0f} s")
                break
            if time.time() - start_wait > 60:
                raise RuntimeError(f"hosts still cannot reach each other after 60 s ({loss:.0f}% ping loss). "
                                   "Is Ryu running, and does a manual pingall work?")
            time.sleep(2)

        broker = net.get("broker")
        broker.cmd(f"python3 -u broker/broker.py 5001 > {outdir}/broker_5001.log 2>&1 &")
        if "NORMAL" in classes:
            broker.cmd(f"python3 -u broker/broker.py 5002 > {outdir}/broker_5002.log 2>&1 &")
        time.sleep(1.5)
        # Check that the broker accepts TCP connections before starting the probes.
        for c in classes:
            port, pubh = CLASSES[c][1], CLASSES[c][2]
            check = net.get(pubh).cmd(
                f"python3 -c \"import socket; socket.create_connection(('{BROKER_IP}', {port}), 3); print('tcp-ok')\" 2>&1")
            if "tcp-ok" not in check:
                log = broker.cmd(f"tail -n 5 {outdir}/broker_{port}.log")
                raise RuntimeError(f"{pubh} cannot open a TCP connection to the broker on port {port}.\n"
                                   f"connect error: {check.strip()[-300:]}\nbroker log: {log.strip()}")
        for c in classes:
            topic, port, _, subh = CLASSES[c]
            net.get(subh).cmd(f"{me} sub {BROKER_IP} {topic} --port {port} "
                              f"--out {outdir}/sub_{c}.csv > {outdir}/sub_{c}.log 2>&1 &")
        time.sleep(1.5)
        t0 = time.time()
        for c in classes:
            topic, port, pubh, _ = CLASSES[c]
            net.get(pubh).cmd(f"{me} pub {BROKER_IP} {topic} --port {port} --rate {rate} --count {count} "
                              f"--pad {pad} --out {outdir}/pub_{c}.csv > {outdir}/pub_{c}.log 2>&1 &")
        meta["t0"] = t0

        def wait_until(t):
            d = t - time.time()
            if d > 0:
                time.sleep(d)

        if failure:
            link, down_at, up_at = failure
            wait_until(t0 + down_at)
            meta["t_down"] = time.time()
            print(f"    [{meta['t_down'] - t0:5.1f}s] link {link[0]}-{link[1]} DOWN")
            net.configLinkStatus(link[0], link[1], "down")
            wait_until(t0 + up_at)
            meta["t_up"] = time.time()
            print(f"    [{meta['t_up'] - t0:5.1f}s] link {link[0]}-{link[1]} UP")
            net.configLinkStatus(link[0], link[1], "up")
            meta["link"] = list(link)
        wait_until(t0 + duration + 5)  # give late messages time to arrive
    finally:
        for h in ("pub1", "pub2", "sub1", "sub2", "broker"):
            try:
                net.get(h).cmd("pkill -f 'experiments.py (pub|sub)'; pkill -f broker/broker.py")
            except Exception:
                pass
        net.stop()
    with open(os.path.join(outdir, "meta.json"), "w") as f:
        json.dump(meta, f, indent=2)
    for row in analyse_run(outdir):
        flag = "  <-- nothing received, check the logs in this folder" if row["received"] == 0 else ""
        print(f"    {row['class']:6s} sent={row['sent']} recv={row['received']} loss={row['loss_pct']}% "
              f"mean={row['mean_ms']} ms p95={row['p95_ms']} ms{flag}")


def cmd_run(a):
    if not os.path.exists("topology/topo_triangle.py") or not os.path.exists("broker/broker.py"):
        sys.exit("Run this from the repo root.")
    if os.geteuid() != 0:
        sys.exit("Mininet needs root: sudo python3 tools/experiments.py run ...")
    sys.path.insert(0, os.path.abspath("topology"))
    from mininet.log import setLogLevel
    setLogLevel("warning")

    exps = list(EXPERIMENTS) if a.exp == "all" else [a.exp]
    for exp in exps:
        spec = EXPERIMENTS[exp]
        modes = [a.mode] if a.mode else spec["modes"]
        if exp in ("E1", "E2"):
            rates = a.rates
        elif exp == "E3":
            rates = [a.failure_rate]
        else:
            rates = [a.e4_rate]
        duration = a.failure_duration if spec["failure"] else a.duration
        bw = a.e4_bw if exp == "E4" else None
        pad = a.e4_pad if exp == "E4" else 0
        failure = (a.link, a.down_at, a.up_at) if spec["failure"] else None
        for mode in modes:
            try:
                set_mode(a.controller, mode)
            except Exception as e:
                sys.exit(f"Could not set controller mode via {a.controller}/mode: {e}\nIs Ryu running?")
            for rate in rates:
                for run in range(1, a.runs + 1):
                    outdir = os.path.join(RESULTS, exp, f"{mode}_r{rate:g}_run{run}")
                    print(f"{exp}  mode={mode}  rate={rate:g} msg/s  run {run}/{a.runs}  -> {outdir}")
                    try:
                        run_one(outdir, mode, spec["classes"], rate, duration, bw, pad, failure, a.settle)
                    except RuntimeError as e:
                        sys.exit(f"\nStopped: {e}")
                    meta_path = os.path.join(outdir, "meta.json")
                    meta = json.load(open(meta_path))
                    meta.update({"experiment": exp, "run": run})
                    json.dump(meta, open(meta_path, "w"), indent=2)
    print("\nDone. Next:  sudo chown -R $USER:$USER results  then  python3 tools/experiments.py summary")


# ---------------------------------------------------------------- analysis
def pct(values, p):
    if not values:
        return float("nan")
    v = sorted(values)
    return v[min(len(v) - 1, int(round(p / 100 * (len(v) - 1))))]


def read_csv(path):
    if not os.path.exists(path):
        return []
    with open(path, newline="") as f:
        return list(csv.DictReader(f))


def r2(x):
    return "" if x is None or (isinstance(x, float) and math.isnan(x)) else round(x, 2)


def analyse_run(outdir):
    """One row per traffic class: sent, received, loss, latency, throughput and,
    for failure runs, the delivery stall and whether delivery recovered before
    the link came back."""
    meta = json.load(open(os.path.join(outdir, "meta.json")))
    rows = []
    for c in meta["classes"]:
        pub = read_csv(os.path.join(outdir, f"pub_{c}.csv"))
        sub = read_csv(os.path.join(outdir, f"sub_{c}.csv"))
        send = {int(r["seq"]): float(r["send_ts"]) for r in pub}
        got = {}
        for r in sub:
            got.setdefault(int(r["seq"]), (float(r["recv_ts"]), float(r["latency_ms"])))
        lats = [v[1] for v in got.values()]
        sent, recv = len(send), len(got)
        thr = None
        if recv > 1 and send:
            span = max(v[0] for v in got.values()) - min(send.values())
            thr = recv / span if span > 0 else None
        stall = recovered = None
        if meta.get("t_down") and len(got) > 1:
            t_down, t_up = meta["t_down"], meta["t_up"]
            times = sorted(v[0] for v in got.values())
            stall, stall_end = 0.0, None
            for prev, cur in zip(times, times[1:]):
                if cur > t_down and prev < t_up and cur - prev > stall:
                    stall, stall_end = cur - prev, cur
            if stall_end is not None:
                recovered = 1 if stall_end < t_up - 0.5 else 0
        rows.append({
            "experiment": meta.get("experiment", ""), "mode": meta["mode"], "rate": meta["rate"],
            "run": meta.get("run", ""), "class": c, "sent": sent, "received": recv,
            "loss_pct": r2(100 * (sent - recv) / sent) if sent else "",
            "mean_ms": r2(sum(lats) / len(lats)) if lats else "",
            "p95_ms": r2(pct(lats, 95)) if lats else "",
            "throughput_msg_s": r2(thr), "stall_s": r2(stall), "recovered": "" if recovered is None else recovered,
        })
    return rows


SUMMARY_COLS = ["experiment", "mode", "rate", "run", "class", "sent", "received", "loss_pct",
                "mean_ms", "p95_ms", "throughput_msg_s", "stall_s", "recovered"]
METRICS = ["loss_pct", "mean_ms", "p95_ms", "throughput_msg_s", "stall_s"]


def collect():
    rows = []
    for exp in sorted(EXPERIMENTS):
        base = os.path.join(RESULTS, exp)
        if not os.path.isdir(base):
            continue
        for d in sorted(os.listdir(base)):
            if os.path.exists(os.path.join(base, d, "meta.json")):
                rows.extend(analyse_run(os.path.join(base, d)))
    return rows


def aggregate(rows):
    """Mean and standard deviation over repeated runs of the same condition."""
    groups = {}
    for r in rows:
        groups.setdefault((r["experiment"], r["mode"], r["rate"], r["class"]), []).append(r)
    out = []
    for (exp, mode, rate, c), rs in sorted(groups.items(), key=lambda kv: (kv[0][0], kv[0][1], float(kv[0][2]), kv[0][3])):
        row = {"experiment": exp, "mode": mode, "rate": rate, "class": c, "runs": len(rs)}
        for m in METRICS:
            vals = [float(r[m]) for r in rs if r[m] != ""]
            mean = sum(vals) / len(vals) if vals else None
            sd = math.sqrt(sum((v - mean) ** 2 for v in vals) / (len(vals) - 1)) if len(vals) > 1 else (0.0 if vals else None)
            row[f"{m}_mean"], row[f"{m}_sd"] = r2(mean), r2(sd)
        rec = [int(r["recovered"]) for r in rs if r["recovered"] != ""]
        row["recovered_runs"] = f"{sum(rec)}/{len(rec)}" if rec else ""
        out.append(row)
    return out


def write_csv(path, rows, cols):
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        w.writerows(rows)


def cmd_summary(a):
    rows = collect()
    if not rows:
        sys.exit(f"No finished runs under {RESULTS}/E1..E4 yet.")
    write_csv(os.path.join(RESULTS, "summary.csv"), rows, SUMMARY_COLS)
    agg = aggregate(rows)
    cols = ["experiment", "mode", "rate", "class", "runs"] + [f"{m}_{s}" for m in METRICS for s in ("mean", "sd")] + ["recovered_runs"]
    write_csv(os.path.join(RESULTS, "aggregate.csv"), agg, cols)
    print(f"{'exp':4} {'mode':8} {'rate':>5} {'class':6} {'runs':>4} {'loss%':>7} {'mean ms':>8} {'p95 ms':>8} {'msg/s':>7} {'stall s':>7} recovered")
    for r in agg:
        print(f"{r['experiment']:4} {r['mode']:8} {r['rate']:>5} {r['class']:6} {r['runs']:>4} {str(r['loss_pct_mean']):>7} "
              f"{str(r['mean_ms_mean']):>8} {str(r['p95_ms_mean']):>8} {str(r['throughput_msg_s_mean']):>7} "
              f"{str(r['stall_s_mean']):>7} {r['recovered_runs']}")
    print(f"\nWrote {RESULTS}/summary.csv (one row per run) and {RESULTS}/aggregate.csv (mean and sd).")


# -------------------------------------------------------------------- plots
COLORS = {"static": "#2a78d6", "dynamic": "#eb6834"}  # fixed: a mode keeps its colour in every graph
INK, MUTED, GRID = "#1f1f1e", "#6b6a64", "#e4e3de"


def style(ax, title, ylabel, xlabel=None):
    ax.set_title(title, loc="left", fontsize=11, color=INK)
    ax.set_ylabel(ylabel, color=MUTED)
    if xlabel:
        ax.set_xlabel(xlabel, color=MUTED)
    ax.grid(axis="y", color=GRID, linewidth=0.8)
    ax.set_axisbelow(True)
    for s in ("top", "right"):
        ax.spines[s].set_visible(False)
    for s in ("left", "bottom"):
        ax.spines[s].set_color(GRID)
    ax.tick_params(colors=MUTED)


def num(v):
    return float(v) if v not in ("", None) else float("nan")


def cmd_plot(a):
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        sys.exit("matplotlib is missing: sudo apt install python3-matplotlib")
    agg = aggregate(collect())
    if not agg:
        sys.exit(f"No finished runs under {RESULTS}/E1..E4 yet.")
    gdir = os.path.join(RESULTS, "graphs")
    os.makedirs(gdir, exist_ok=True)
    made = []

    # 1) E1 vs E2: latency, loss and throughput against publish rate
    rate_rows = [r for r in agg if r["experiment"] in ("E1", "E2")]
    if rate_rows:
        fig, axes = plt.subplots(1, 3, figsize=(13, 3.8))
        panels = [("mean_ms", "Mean latency", "ms"), ("loss_pct", "Message loss", "%"),
                  ("throughput_msg_s", "Delivered throughput", "messages / s")]
        for ax, (m, title, unit) in zip(axes, panels):
            for mode in ("static", "dynamic"):
                pts = sorted((num(r["rate"]), num(r[f"{m}_mean"]), num(r[f"{m}_sd"])) for r in rate_rows if r["mode"] == mode)
                if pts:
                    xs, ys, es = zip(*pts)
                    ax.errorbar(xs, ys, yerr=es, color=COLORS[mode], linewidth=2, marker="o", markersize=7,
                                capsize=3, label=f"{mode} ({'E1' if mode == 'static' else 'E2'})")
            style(ax, title, unit, "publish rate (messages / s)")
        axes[0].legend(frameon=False)
        fig.suptitle("E1 vs E2: static baseline vs dynamic SDN, no failure (mean ± sd over runs)", x=0.01, ha="left", fontsize=12)
        fig.tight_layout()
        made.append(os.path.join(gdir, "e1_e2_rate_sweep.png"))
        fig.savefig(made[-1], dpi=150)
        plt.close(fig)

    # 2) E3: delivery stall and loss, static vs dynamic
    e3 = sorted((r for r in agg if r["experiment"] == "E3"), key=lambda r: r["mode"] != "static")  # baseline first
    if e3:
        fig, axes = plt.subplots(1, 2, figsize=(9, 3.8))
        for ax, (m, title, unit) in zip(axes, [("stall_s", "Longest delivery stall", "seconds"),
                                               ("loss_pct", "Messages lost", "%")]):
            modes = [r["mode"] for r in e3]
            ax.bar(modes, [num(r[f"{m}_mean"]) for r in e3], yerr=[num(r[f"{m}_sd"]) for r in e3],
                   color=[COLORS[x] for x in modes], width=0.5, capsize=4, edgecolor="white", linewidth=2)
            for i, r in enumerate(e3):
                ax.annotate(f"{num(r[f'{m}_mean']):.2f}", (i, num(r[f"{m}_mean"])), textcoords="offset points",
                            xytext=(0, 4), ha="center", color=INK, fontsize=9)
            style(ax, title, unit)
        fig.suptitle("E3: link s1-s3 fails during traffic (mean ± sd over runs)", x=0.01, ha="left", fontsize=12)
        fig.tight_layout()
        made.append(os.path.join(gdir, "e3_failover.png"))
        fig.savefig(made[-1], dpi=150)
        plt.close(fig)

        # 3) E3 timeline: latency of every message around the failure, run 1 of each mode
        fig, ax = plt.subplots(figsize=(10, 4))
        shaded = False
        for mode in ("static", "dynamic"):
            d = os.path.join(RESULTS, "E3", f"{mode}_r{e3[0]['rate']:g}_run1")
            if not os.path.exists(os.path.join(d, "meta.json")):
                continue
            meta = json.load(open(os.path.join(d, "meta.json")))
            send = {int(r["seq"]): float(r["send_ts"]) for r in read_csv(os.path.join(d, "pub_HIGH.csv"))}
            pts = sorted((send[int(r["seq"])] - meta["t_down"], float(r["latency_ms"]))
                         for r in read_csv(os.path.join(d, "sub_HIGH.csv")) if int(r["seq"]) in send)
            if pts:
                ax.scatter(*zip(*pts), s=10, color=COLORS[mode], label=mode, alpha=0.8)
            if not shaded:
                ax.axvspan(0, meta["t_up"] - meta["t_down"], color=GRID, alpha=0.6, label="link s1-s3 down")
                shaded = True
        ax.set_yscale("log")
        style(ax, "E3 timeline: latency of each message (run 1)", "latency (ms, log scale)",
              "publish time relative to link-down (s)")
        ax.legend(frameon=False)
        fig.tight_layout()
        made.append(os.path.join(gdir, "e3_timeline.png"))
        fig.savefig(made[-1], dpi=150)
        plt.close(fig)

    # 4) E4: HIGH vs NORMAL latency under congestion
    e4 = [r for r in agg if r["experiment"] == "E4"]
    if e4:
        fig, axes = plt.subplots(1, 2, figsize=(9, 3.8))
        for ax, (m, title) in zip(axes, [("mean_ms", "Mean latency"), ("p95_ms", "95th percentile latency")]):
            classes = ["HIGH", "NORMAL"]
            for j, mode in enumerate(("static", "dynamic")):
                vals = [next((num(r[f"{m}_mean"]) for r in e4 if r["mode"] == mode and r["class"] == c), float("nan")) for c in classes]
                errs = [next((num(r[f"{m}_sd"]) for r in e4 if r["mode"] == mode and r["class"] == c), 0) for c in classes]
                xs = [i + (j - 0.5) * 0.36 for i in range(len(classes))]
                ax.bar(xs, vals, yerr=errs, width=0.34, color=COLORS[mode], label=mode, capsize=3,
                       edgecolor="white", linewidth=2)
            ax.set_xticks(range(len(classes)))
            ax.set_xticklabels(["HIGH (alerts, 5001)", "NORMAL (news, 5002)"])
            style(ax, title, "ms")
        axes[0].legend(frameon=False)
        fig.suptitle("E4: HIGH vs NORMAL on a bandwidth-limited triangle (mean ± sd over runs)", x=0.01, ha="left", fontsize=12)
        fig.tight_layout()
        made.append(os.path.join(gdir, "e4_priority.png"))
        fig.savefig(made[-1], dpi=150)
        plt.close(fig)

    print("Graphs written:\n  " + "\n  ".join(made))


# --------------------------------------------------------------------- main
def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sp = p.add_subparsers(dest="cmd", required=True)

    r = sp.add_parser("run", help="run experiments in Mininet (sudo)")
    r.add_argument("--exp", default="all", choices=["all"] + list(EXPERIMENTS))
    r.add_argument("--mode", choices=["static", "dynamic"], help="only this mode (default: the experiment's modes)")
    r.add_argument("--runs", type=int, default=3, help="repetitions per condition")
    r.add_argument("--rates", type=float, nargs="+", default=[10, 50, 100, 200], help="E1/E2 publish rates")
    r.add_argument("--duration", type=float, default=20, help="seconds of publishing for E1, E2, E4")
    r.add_argument("--failure-rate", type=float, default=10, help="E3 publish rate")
    r.add_argument("--failure-duration", type=float, default=60, help="E3 seconds of publishing")
    r.add_argument("--down-at", type=float, default=20)
    r.add_argument("--up-at", type=float, default=40)
    r.add_argument("--link", nargs=2, default=["s1", "s3"], metavar=("A", "B"))
    r.add_argument("--e4-bw", type=float, default=1.0, help="E4 switch-link bandwidth in Mbit/s")
    r.add_argument("--e4-rate", type=float, default=100, help="E4 publish rate per class")
    r.add_argument("--e4-pad", type=int, default=1000, help="E4 extra bytes per message")
    r.add_argument("--settle", type=float, default=5, help="seconds to wait for link discovery")
    r.add_argument("--controller", default=DEFAULT_CONTROLLER)
    r.set_defaults(fn=cmd_run)

    for name, fn in (("pub", cmd_pub), ("sub", cmd_sub)):
        s = sp.add_parser(name)
        s.add_argument("host")
        s.add_argument("topic")
        s.add_argument("--port", type=int, required=True)
        s.add_argument("--out", required=True)
        if name == "pub":
            s.add_argument("--rate", type=float, required=True)
            s.add_argument("--count", type=int, required=True)
            s.add_argument("--pad", type=int, default=0)
        s.set_defaults(fn=fn)

    sp.add_parser("summary", help="write summary.csv and aggregate.csv").set_defaults(fn=cmd_summary)
    sp.add_parser("plot", help="draw graphs into results/d2/graphs").set_defaults(fn=cmd_plot)

    a = p.parse_args()
    a.fn(a)


if __name__ == "__main__":
    main()
