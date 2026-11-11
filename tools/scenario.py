#!/usr/bin/env python3
"""D2 link-failure scenario runner and analyser.

Subcommands
  run      build the triangle, start broker + probe subscriber + probe publisher,
           fail a link mid-run, restore it, then analyse (needs sudo, Ryu running)
  pub      probe publisher (runs on a Mininet host): numbered messages at a steady rate
  sub      probe subscriber (runs on a Mininet host): logs when each message arrives
  analyze  print the summary for a finished run

Run from the repo root:
  sudo python3 tools/scenario.py run --mode dynamic
  python3 tools/scenario.py analyze results/d2/dynamic_failover

Timeline of `run` (defaults): publish at 10 msg/s for 60 s, link s1-s3 goes down at
20 s and comes back at 40 s. Everything uses the VM clock, so send and receive times
are comparable.

Note: TCP retransmits, so messages sent during a short outage usually arrive late
rather than never. The summary therefore reports the delivery stall (longest gap
between arrivals) and the latency spike, as well as the number of messages lost.
"""
import argparse
import csv
import json
import os
import socket
import sys
import threading
import time


def port_for(topic):
    return 5001 if topic == "alerts" else 5002


def connect(host, port, retries=20):
    last = None
    for _ in range(retries):
        try:
            return socket.create_connection((host, port), timeout=3)
        except OSError as e:
            last = e
            time.sleep(0.5)
    raise last


# ---------------------------------------------------------------- probe publisher
def cmd_pub(a):
    port = a.port or port_for(a.topic)
    state = {"sock": connect(a.host, port)}
    state["sock"].settimeout(2)
    stop = threading.Event()

    def drain():
        # Read and discard the broker's ACKs so its replies never back up; show ERRORs.
        buf = b""
        while not stop.is_set():
            try:
                data = state["sock"].recv(4096)
            except socket.timeout:
                continue
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
            wait = start + seq / a.rate - time.time()  # absolute schedule, so no drift
            if wait > 0:
                time.sleep(wait)
            ts = time.time()
            line = json.dumps({"type": "PUBLISH", "topic": a.topic, "payload": str(seq), "ts": ts}) + "\n"
            ok = 1
            try:
                state["sock"].sendall(line.encode())
            except OSError:
                ok = 0
                try:
                    state["sock"].close()
                except OSError:
                    pass
                try:
                    state["sock"] = connect(a.host, port, retries=1)
                    state["sock"].settimeout(2)
                except OSError:
                    pass
            w.writerow([seq, f"{ts:.6f}", ok])
            f.flush()
    time.sleep(1)
    stop.set()


# --------------------------------------------------------------- probe subscriber
def cmd_sub(a):
    """Reads a stream of JSON objects. It does not rely on newlines between messages,
    so it also works with a broker that forgets the terminator."""
    port = a.port or port_for(a.topic)
    sock = connect(a.host, port)
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
                    break  # incomplete object: wait for more data
                buf = buf[end:]
                if not isinstance(m, dict) or m.get("type") not in ("DELIVER", "PUBLISH"):
                    continue
                try:
                    seq = int(m["payload"])
                    latency = (now - float(m.get("ts", now))) * 1000
                except (ValueError, KeyError, TypeError):
                    continue
                w.writerow([seq, f"{now:.6f}", f"{latency:.3f}"])
                out.flush()


# ------------------------------------------------------------------------ analysis
def pct(values, p):
    if not values:
        return float("nan")
    v = sorted(values)
    return v[min(len(v) - 1, int(round(p / 100 * (len(v) - 1))))]


def read_csv(path):
    with open(path, newline="") as f:
        return list(csv.DictReader(f))


def ranges(missing):
    out, start, prev = [], None, None
    for s in missing:
        if start is None:
            start = prev = s
        elif s == prev + 1:
            prev = s
        else:
            out.append((start, prev))
            start = prev = s
    if start is not None:
        out.append((start, prev))
    return out


def lat_stats(vals):
    if not vals:
        return "no messages"
    return f"n={len(vals)} mean={sum(vals)/len(vals):.1f} ms  p95={pct(vals, 95):.1f} ms  max={max(vals):.1f} ms"


def analyze(outdir):
    meta = json.load(open(os.path.join(outdir, "meta.json")))
    pub = read_csv(os.path.join(outdir, "pub.csv"))
    sub = read_csv(os.path.join(outdir, "sub.csv"))
    t_down, t_up = meta["t_down"], meta["t_up"]
    rate = meta["rate"]

    send_ts = {int(r["seq"]): float(r["send_ts"]) for r in pub}
    got = {}
    for r in sub:
        got.setdefault(int(r["seq"]), (float(r["recv_ts"]), float(r["latency_ms"])))
    missing = sorted(s for s in send_ts if s not in got)
    sent = len(send_ts)

    before, during, after = [], [], []
    for seq, (_, lat) in got.items():
        t = send_ts.get(seq)
        if t is None:
            continue
        (before if t < t_down else during if t < t_up else after).append(lat)

    recv = sorted(v[0] for v in got.values())
    stall, stall_from, stall_to = 0.0, None, None
    for prev, cur in zip(recv, recv[1:]):
        if cur > t_down and prev < t_up and cur - prev > stall:
            stall, stall_from, stall_to = cur - prev, prev, cur

    lines = []
    lines.append(f"Scenario: {meta.get('name')}   mode={meta.get('mode')}   link {meta['link'][0]}-{meta['link'][1]} down for {t_up - t_down:.0f} s")
    lines.append(f"Rate {rate} msg/s, {sent} messages sent, {len(got)} received, {len(missing)} lost ({100*len(missing)/max(sent,1):.1f}%)")
    if missing:
        shown = ", ".join(f"{a}-{b}" if a != b else str(a) for a, b in ranges(missing)[:8])
        lines.append(f"  lost seq numbers: {shown}")
    lines.append(f"Latency before failure : {lat_stats(before)}")
    lines.append(f"Latency during failure : {lat_stats(during)}")
    lines.append(f"Latency after restore  : {lat_stats(after)}")
    if stall_from is not None:
        lines.append(f"Longest delivery stall : {stall:.2f} s (from {stall_from - t_down:+.2f} s to {stall_to - t_down:+.2f} s relative to link-down)")
        recovery = max(0.0, stall - 1.0 / rate)
        if stall_to > t_up - 0.5:
            lines.append("  -> delivery did NOT resume until the link was restored (no rerouting)")
        else:
            lines.append(f"  -> estimated recovery time after link-down: {recovery:.2f} s (stall minus one message interval)")
    else:
        lines.append("Longest delivery stall : no gap found around the failure window")
    text = "\n".join(lines)
    with open(os.path.join(outdir, "summary.txt"), "w") as f:
        f.write(text + "\n")
    print(text)


# ---------------------------------------------------------------------------- run
def cmd_run(a):
    repo = os.getcwd()
    for need in ("tools/scenario.py", "broker/broker.py", "topology/topo_triangle.py"):
        if not os.path.exists(os.path.join(repo, need)):
            sys.exit(f"Run this from the repo root (missing {need}).")
    sys.path.insert(0, os.path.join(repo, "topology"))
    from topo_triangle import build_network  # noqa: E402
    from mininet.log import setLogLevel  # noqa: E402

    name = a.name or f"{a.mode}_failover"
    outdir = os.path.join("results", "d2", name)
    os.makedirs(outdir, exist_ok=True)
    count = int(a.rate * a.duration)
    link = a.link
    probe = "python3 tools/scenario.py"

    setLogLevel("warning")
    net = build_network()
    try:
        if a.break_link:
            net.configLinkStatus(a.break_link[0], a.break_link[1], "down")
        print("Letting the controller settle...")
        time.sleep(4)
        broker, pub1, sub1 = net.get("broker"), net.get("pub1"), net.get("sub1")
        # Prime ARP and the first flow rules.
        for h in (pub1, sub1):
            h.cmd("ping -c 2 -W 2 10.0.0.3")

        print("Starting broker, subscriber, publisher...")
        broker.cmd(f"python3 broker/broker.py > {outdir}/broker.log 2>&1 &")
        time.sleep(1.5)
        sub1.cmd(f"{probe} sub 10.0.0.3 {a.topic} --out {outdir}/sub.csv > {outdir}/sub.log 2>&1 &")
        time.sleep(1.5)
        t0 = time.time()
        pub1.cmd(f"{probe} pub 10.0.0.3 {a.topic} --rate {a.rate} --count {count} --out {outdir}/pub.csv > {outdir}/pub.log 2>&1 &")

        def wait_until(t):
            d = t - time.time()
            if d > 0:
                time.sleep(d)

        wait_until(t0 + a.down_at)
        t_down = time.time()
        print(f"[{t_down - t0:5.1f}s] link {link[0]}-{link[1]} DOWN")
        net.configLinkStatus(link[0], link[1], "down")
        wait_until(t0 + a.up_at)
        t_up = time.time()
        print(f"[{t_up - t0:5.1f}s] link {link[0]}-{link[1]} UP")
        net.configLinkStatus(link[0], link[1], "up")
        wait_until(t0 + a.duration + 5)
        print("Run finished, collecting results...")
        json.dump({"name": name, "mode": a.mode, "link": link, "rate": a.rate, "duration": a.duration,
                   "count": count, "t0": t0, "t_down": t_down, "t_up": t_up},
                  open(os.path.join(outdir, "meta.json"), "w"), indent=2)
    finally:
        for h in ("pub1", "sub1", "broker"):
            try:
                net.get(h).cmd("pkill -f 'scenario.py (pub|sub)'; pkill -f broker.py")
            except Exception:
                pass
        net.stop()
    print()
    analyze(outdir)
    print(f"\nFiles are in {outdir}/  (fix ownership with: sudo chown -R $USER:$USER results)")


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sp = p.add_subparsers(dest="cmd", required=True)

    r = sp.add_parser("run")
    r.add_argument("--mode", default="unknown", help="label only: static or dynamic (how Ryu was started)")
    r.add_argument("--name", help="output folder name under results/d2 (default <mode>_failover)")
    r.add_argument("--topic", default="alerts")
    r.add_argument("--rate", type=float, default=10.0, help="messages per second")
    r.add_argument("--duration", type=float, default=60.0, help="seconds of publishing")
    r.add_argument("--down-at", type=float, default=20.0)
    r.add_argument("--up-at", type=float, default=40.0)
    r.add_argument("--link", nargs=2, default=["s1", "s3"], metavar=("A", "B"))
    r.add_argument("--break-link", nargs=2, metavar=("A", "B"),
                   help="take this link down at the start (use 's1 s2' to test with a controller that cannot handle the loop)")
    r.set_defaults(fn=cmd_run)

    for name, fn in (("pub", cmd_pub), ("sub", cmd_sub)):
        s = sp.add_parser(name)
        s.add_argument("host")
        s.add_argument("topic")
        s.add_argument("--port", type=int)
        s.add_argument("--out", required=True)
        if name == "pub":
            s.add_argument("--rate", type=float, required=True)
            s.add_argument("--count", type=int, required=True)
        s.set_defaults(fn=fn)

    an = sp.add_parser("analyze")
    an.add_argument("outdir")
    an.set_defaults(fn=lambda a: analyze(a.outdir))

    a = p.parse_args()
    a.fn(a)


if __name__ == "__main__":
    main()
