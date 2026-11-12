# D2 Test Cases

Environment: Ubuntu VM, Mininet triangle topology (`topology/topo_triangle.py`: s1 with pub1 and pub2, s2 with sub1 and sub2, s3 with the broker, links s1-s3, s1-s2, s2-s3, VM attached to s3 as 10.0.0.254), Ryu controller `controller/app.py`, broker `broker/broker.py`. The contract for D2 is `docs/design_d2.md`.

Status key: **Pass**, **Fail**, **Partial** (part of the test done), **Pending** (depends on a feature not yet delivered), **Preliminary** (measured with a stand-in setup, to be repeated).

Startup order, every time: `sudo mn -c`, then Ryu, then the topology. Never run `sudo mn -c` while Ryu is running. After any `sudo` run: `sudo chown -R $USER:$USER ~/pubsub-sdn-project`.

## Results

| # | Test | Procedure | Expected | Actual | Status |
|---|---|---|---|---|---|
| D2-1 | Reachability and no broadcast storm | Start the topology, then `pingall` at `mininet>` with all three switch links up | 0% dropped, no flood of packet-ins in the Ryu terminal | With all three links up: `0% dropped (20/20 received)`. Ryu discovered 6 directed links, installed class rules per host and then went quiet (no storm). Flows saved in `results/d2/d2-1_flows_s1.txt` | Pass |
| D2-2 | Controller REST reachable from a host | `pub1 ping -c 2 10.0.0.254`, then `pub1 curl -s http://10.0.0.254:8080/stats` | Ping replies; JSON from `/stats` | Ping 0% loss. `/stats` returned JSON with `"mode": "dynamic"`, switches `[1, 2, 3]`, all 6 directed links, `flow_counts`, `subscriptions` and `blocked_ips`; Ryu logged `GET /stats 200`. (First attempt crashed with a `TypeError` in the REST class constructor; fixed by Nithin, retested.) | Pass |
| D2-3 | Subscription reaches the controller | Broker started with `--controller=http://10.0.0.254:8080/subscription`, sub1 subscribes to `alerts`, then `pub1 curl -s http://10.0.0.254:8080/subscriptions` | `alerts` lists 10.0.0.4 | `{"alerts": ["10.0.0.4"]}`. (First attempt returned `{}` because the broker sent `client_ip` instead of `ip`; fixed by Prakul and retested.) | Pass |
| D2-4 | Broker survives an unreachable controller | Broker started with `--controller=http://10.0.0.254:9999/subscription` (nothing listening), then a probe subscriber on sub2 and a probe publisher on pub1 sending 10 `alerts` messages | Subscription still works; the failure is logged; broker keeps running | 10/10 messages delivered to sub2 (latency 1.4 to 66 ms); broker still running afterwards (`pgrep`). Retested after the broker fix: the broker prints `[Controller Notify Error] Failed to send subscribe ... Connection refused` and carries on | Pass |
| D2-5 | Failover with rerouting (dynamic) | `sudo python3 tools/experiments.py run --exp E3` (10 msg/s for 60 s, link s1-s3 down at 20 s, up at 40 s), 3 runs | Delivery resumes over s1-s2-s3 after a short stall; recovery time recorded | 600/600 delivered in every run, 0% loss; longest delivery gap 0.38 s on average (normal gap 0.1 s), so the outage was about 0.3 s, and delivery resumed while the link was still down in 3 of 3 runs; mean latency 6.96 ms, p95 18.97 ms. Output in `results/d2/E3/` and graphs `e3_failover.png`, `e3_timeline.png` | Pass |
| D2-6 | Failover baseline (no rerouting) | Same run as D2-5 with `--mode static` (mode set through `POST /mode`), 3 runs | No recovery until the link returns | Delivery stopped for 27.46 s on average (longer than the 20 s outage because of TCP retransmission backoff) and resumed only after the link came back, in 0 of 3 runs before; mean latency 6,309 ms, p95 24,410 ms; no messages lost (TCP retransmits). (First attempt: static mode also rerouted; fixed by Nithin and retested.) | Pass |
| D2-7 | Link restored | `link s1 s3 up` at 40 s in D2-5 | Traffic keeps flowing | All messages after the link came back were delivered (600/600 in D2-5), no second stall | Pass |
| D2-8 | HIGH vs NORMAL under congestion | `sudo python3 tools/experiments.py run --exp E4 --e4-rate 60`: switch links limited to 1 Mbit/s, HIGH (`alerts`, 5001) from pub1 and NORMAL (`news`, 5002) from pub2 at 60 msg/s each with 1,000-byte padding, static vs dynamic, 3 runs each | HIGH latency lower than NORMAL / protected from congestion | Dynamic: HIGH 10.88 ms mean (p95 50.89), NORMAL 11.62 ms (p95 56.37), both 0% loss at 60 msg/s, on separate paths (s1-s3 and s1-s2-s3). Static (shared s1-s3): HIGH 1,442.81 ms, NORMAL 677.13 ms, throughput below the publish rate. HIGH is about 130 times faster with class-based paths; within dynamic mode HIGH is only slightly faster than NORMAL because neither path is congested. (Earlier attempts in `results/d2/E4_overloaded_r100/` and `E4_before_fix_r60/` before the controller fix.) | Pass |
| D2-9 | Event log written | Broker started, probe subscriber on sub1 (`alerts`), probe publisher on pub1 sending 5 messages, then `broker tail -n 10 results/events.jsonl` | One valid JSON line per event with ts, event, topic, client_ip | One JSON line per event in the right order: sub1 (10.0.0.4) `CONNECT`, `SUBSCRIBE alerts`; pub1 (10.0.0.1) `CONNECT`, 5x `PUBLISH alerts` each with `"delivered": 1`, then `DISCONNECT`. All lines have `ts`, `event`, `topic`, `client_ip`, `delivered`. Event names are uppercase rather than the lowercase in the design doc; the UI accepts both | Pass |
| D2-10 | Block a host (stretch) | `pub2 ping -c 3 10.0.0.3`, then `pub1 curl -s -X POST -d '{"ip": "10.0.0.2"}' http://10.0.0.254:8080/block`, then `pub2 ping` and `pub1 ping` again | pub2's traffic is dropped, pub1 unaffected | Before: pub2 0% loss. `/block` replied `{"status": "success", "blocked_ip": "10.0.0.2"}`. After: pub2 100% loss, pub1 0% loss. (First attempt: traffic still passed because only packet-in checked the block list; fixed by Nithin with a drop flow, retested.) Note: there is no unblock yet, so a blocked host stays blocked until Ryu restarts | Pass |

## Issues found while testing (all fixed and retested)

- **Controller only worked for the first network after Ryu started.** Ports deleted when Mininet stopped stayed in `down_ports`, so every link was ignored in the next network (100% ping loss). Fixed by clearing a switch's state when it reconnects.
- **Static mode rerouted.** It recomputed paths on a link failure, so there was no baseline (D2-6). Fixed: static mode keeps the original paths.
- **Class-based paths never took effect.** Flows installed for ping traffic (matching only port and MAC) also caught the pub/sub TCP traffic (D2-8). Fixed with protocol-specific flows.
- **`/block` did not drop traffic.** Only packet-in checked the list (D2-10). Fixed with a drop flow on every switch.
- **Broker sent `client_ip` instead of `ip`** to `/subscription` (D2-3). Fixed.
- **D1 clients did not work with the D2 broker** (no newline framing, `content` instead of `payload`). Fixed, with the D1 command-line interface kept.

Open: there is no `POST /unblock`, so a blocked host stays blocked until Ryu restarts.

## Observations to explain in the viva

- **TCP and outages.** TCP retransmits with exponentially growing waits, so after a link returns it can take several seconds more before traffic flows again. In the preliminary baseline (D2-6) the stall (13.8 s) was longer than the outage (10 s).
- **Loss and delay.** Messages sent during the outage did not arrive late; they did not arrive at all. The scenario runner reports both the number lost and the delivery stall.
- **Why the baseline was run with `s1 s2` down.** With the loop present, the old controller floods packets around the triangle. Breaking the second path removes the loop, and also removes the backup path, which makes it behave like a network with no rerouting.

## Files produced by the scenario runner

`results/d2/<name>/` contains `pub.csv` (what was sent), `sub.csv` (what arrived and when), `meta.json` (times of the link-down and link-up events), `broker.log` and `summary.txt`.
