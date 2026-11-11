# D2 Test Cases

Environment: Ubuntu VM, Mininet triangle topology (`topology/topo_triangle.py`: s1 with pub1 and pub2, s2 with sub1 and sub2, s3 with the broker, links s1-s3, s1-s2, s2-s3, VM attached to s3 as 10.0.0.254), Ryu controller `controller/app.py`, broker `broker/broker.py`. The contract for D2 is `docs/design_d2.md`.

Status key: **Pass**, **Fail**, **Partial** (part of the test done), **Pending** (depends on a feature not yet delivered), **Preliminary** (measured with a stand-in setup, to be repeated).

Startup order, every time: `sudo mn -c`, then Ryu, then the topology. Never run `sudo mn -c` while Ryu is running. After any `sudo` run: `sudo chown -R $USER:$USER ~/pubsub-sdn-project`.

## Results

| # | Test | Procedure | Expected | Actual | Status |
|---|---|---|---|---|---|
| D2-1 | Reachability and no broadcast storm | Start the topology, then `pingall` at `mininet>` with all three switch links up | 0% dropped, no flood of packet-ins in the Ryu terminal | With all three links up: `0% dropped (20/20 received)`. Ryu discovered 6 directed links, installed class rules per host and then went quiet (no storm). Flows saved in `results/d2/d2-1_flows_s1.txt` | Pass |
| D2-2 | Controller REST reachable from a host | `pub1 ping -c 2 10.0.0.254`, then `pub1 curl -s http://10.0.0.254:8080/stats` | Ping replies; JSON from `/stats` | Ping 0% loss. `/stats` returned JSON with `"mode": "dynamic"`, switches `[1, 2, 3]`, all 6 directed links, `flow_counts`, `subscriptions` and `blocked_ips`; Ryu logged `GET /stats 200`. (First attempt crashed with a `TypeError` in the REST class constructor; fixed by Nithin, retested.) | Pass |
| D2-3 | Subscription reaches the controller | Broker started with `--controller=http://10.0.0.254:8080/subscription`, sub1 subscribes to `alerts`, then `pub1 curl -s http://10.0.0.254:8080/subscriptions` | `alerts` lists 10.0.0.4 | `{}`: the broker sends `{"action","topic","client_ip"}` but the agreed field (design doc, controller) is `ip`, so the controller rejects it; the broker swallows the error without logging. Reported to Prakul | Fail |
| D2-4 | Broker survives an unreachable controller | Broker started with `--controller=http://10.0.0.254:9999/subscription` (nothing listening), then a probe subscriber on sub2 and a probe publisher on pub1 sending 10 `alerts` messages | Subscription still works; the failure is logged; broker keeps running | 10/10 messages delivered to sub2 (latency 1.4 to 66 ms); broker still running afterwards (`pgrep`). Not shown: a log line for the failed notification (the broker currently ignores the error silently) | Pass (logging missing) |
| D2-5 | Failover with rerouting (dynamic) | Ryu freshly started, then `sudo python3 tools/experiments.py run --exp E3 --mode dynamic --runs 1` (10 msg/s for 60 s, link s1-s3 down at 20 s, up at 40 s) | Delivery resumes over s1-s2-s3 after a short stall; recovery time recorded | 600 sent, 600 received, 0% loss; mean latency 5.28 ms, p95 8.96 ms; longest delivery gap 0.41 s (normal gap 0.1 s), so the outage was about 0.3 s. Output in `results/d2/E3/dynamic_r10_run1/` | Pass |
| D2-6 | Failover baseline (no rerouting) | Same run with `--mode static` (mode set through `POST /mode`) | No recovery until the link returns | 600/600 received, longest gap 0.57 s, mean 12.23 ms, p95 41.06 ms: the controller's static mode still recomputes paths on link-down, so it is not yet a no-rerouting baseline (reported to Nithin). The earlier preliminary baseline with no backup path (`results/d2/test_run3/`) did behave as expected: 137 of 300 lost, 13.8 s stall | Fail (baseline not valid yet) |
| D2-7 | Link restored | `link s1 s3 up` at 40 s in D2-5 | Traffic keeps flowing | All messages after the link came back were delivered (600/600 in D2-5), no second stall | Pass |
| D2-8 | HIGH beats NORMAL under congestion | Experiment E4 from `docs/design_d2.md`, with a bandwidth-limited primary link, HIGH (`alerts`) and NORMAL traffic together | HIGH latency lower than NORMAL | | Pending |
| D2-9 | Event log written | Broker started, probe subscriber on sub1 (`alerts`), probe publisher on pub1 sending 5 messages, then `broker tail -n 10 results/events.jsonl` | One valid JSON line per event with ts, event, topic, client_ip | One JSON line per event in the right order: sub1 (10.0.0.4) `CONNECT`, `SUBSCRIBE alerts`; pub1 (10.0.0.1) `CONNECT`, 5x `PUBLISH alerts` each with `"delivered": 1`, then `DISCONNECT`. All lines have `ts`, `event`, `topic`, `client_ip`, `delivered`. Event names are uppercase rather than the lowercase in the design doc; the UI accepts both | Pass |
| D2-10 | Block a host (stretch) | `pub2 ping -c 3 10.0.0.3`, then `pub1 curl -s -X POST -d '{"ip": "10.0.0.2"}' http://10.0.0.254:8080/block`, then `pub2 ping` and `pub1 ping` again | pub2's traffic is dropped, pub1 unaffected | `/block` replied `{"status": "success", "blocked_ip": "10.0.0.2"}`, but pub2 still had 0% ping loss afterwards (pub1 also 0%). The block list is only checked for packets sent to the controller, and pub2's traffic already matched installed flows. Fix (install a drop flow for the IP on every switch) reported to Nithin | Fail |

## Known issues found while testing (reported)

- **Controller only works for the first network after Ryu starts.** When Mininet stops, every port is reported deleted and added to `down_ports`, which is never cleared; the next network has the same switch IDs and ports, so every link is ignored and pings get 100% loss. Workaround until fixed: restart Ryu before each run. Fix proposed to Nithin (clear a switch's `down_ports` and `mac_to_port` when it reconnects).
- **Static mode reroutes.** Needed for a valid D2-6 and the E1/E3 baseline. Reported to Nithin.
- **D1 clients no longer work with the D2 broker.** The broker now needs a newline after each message and reads `payload`; `clients/subscriber.py` and `publisher.py` send no newline and `publisher.py` sends `content`. Reported to Prakul. The experiment probes (`tools/experiments.py`) use the new format.
- **`/block` does not drop traffic.** It only adds the IP to a list checked in packet-in; existing flows keep forwarding the host's packets (D2-10). Reported to Nithin.
- **Broker sends `client_ip` instead of `ip`** to `/subscription` (D2-3). Reported to Prakul.

## Observations to explain in the viva

- **TCP and outages.** TCP retransmits with exponentially growing waits, so after a link returns it can take several seconds more before traffic flows again. In the preliminary baseline (D2-6) the stall (13.8 s) was longer than the outage (10 s).
- **Loss and delay.** Messages sent during the outage did not arrive late; they did not arrive at all. The scenario runner reports both the number lost and the delivery stall.
- **Why the baseline was run with `s1 s2` down.** With the loop present, the old controller floods packets around the triangle. Breaking the second path removes the loop, and also removes the backup path, which makes it behave like a network with no rerouting.

## Files produced by the scenario runner

`results/d2/<name>/` contains `pub.csv` (what was sent), `sub.csv` (what arrived and when), `meta.json` (times of the link-down and link-up events), `broker.log` and `summary.txt`.
