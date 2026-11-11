# D2 Test Cases

Environment: Ubuntu VM, Mininet triangle topology (`topology/topo_triangle.py`: s1 with pub1 and pub2, s2 with sub1 and sub2, s3 with the broker, links s1-s3, s1-s2, s2-s3, VM attached to s3 as 10.0.0.254), Ryu controller `controller/app.py`, broker `broker/broker.py`. The contract for D2 is `docs/design_d2.md`.

Status key: **Pass**, **Fail**, **Partial** (part of the test done), **Pending** (depends on a feature not yet delivered), **Preliminary** (measured with a stand-in setup, to be repeated).

Startup order, every time: `sudo mn -c`, then Ryu, then the topology. Never run `sudo mn -c` while Ryu is running. After any `sudo` run: `sudo chown -R $USER:$USER ~/pubsub-sdn-project`.

## Results

| # | Test | Procedure | Expected | Actual | Status |
|---|---|---|---|---|---|
| D2-1 | Reachability and no broadcast storm | Start the topology, then `pingall` at `mininet>` with all three switch links up | 0% dropped, no flood of packet-ins in the Ryu terminal | With `link s1 s2 down` (loop broken): `0% dropped (20/20 received)`. With all links up the controller keeps re-learning the same MAC on different ports (loop); loop handling is still to be delivered | Partial |
| D2-2 | Controller REST reachable from a host | `pub1 ping -c 2 10.0.0.254`, then `pub1 curl -s http://10.0.0.254:8080/stats` | Ping replies; JSON from `/stats` | Ping: 2 sent, 2 received, 0% loss (first reply 143 ms because of ARP, second 6.4 ms). `/stats` not available yet | Partial |
| D2-3 | Subscription reaches the controller | sub1 subscribes to `alerts`, then `pub1 curl -s http://10.0.0.254:8080/subscriptions` | `alerts` lists 10.0.0.4 | | Pending |
| D2-4 | Broker survives an unreachable controller | Stop Ryu, start a subscriber | Subscription still works; the failure is logged; broker keeps running | | Pending |
| D2-5 | Failover with rerouting (dynamic) | Ryu in dynamic mode, topology up, then `sudo python3 tools/scenario.py run --mode dynamic --name dynamic_failover` (link s1-s3 down at 20 s, up at 40 s) | Delivery resumes over s1-s2-s3 after a short stall; recovery time recorded | | Pending |
| D2-6 | Failover baseline (no rerouting) | Same run with `--mode static --name static_failover` (Ryu in static mode) | No recovery until the link returns | **Preliminary** (no backup path, `s1 s2` down, 30 s run, link s1-s3 down 10 s to 20 s): 300 sent, 163 received, 137 lost (seq 99-235); no messages arrived during the outage; delivery resumed about 3.75 s after the link came back; longest stall 13.8 s; latency before 2.5 ms mean, after 2.3 ms mean. Output in `results/d2/test_run3/` | Preliminary |
| D2-7 | Link restored | `link s1 s3 up` at the end of D2-5 | Traffic keeps flowing | | Pending |
| D2-8 | HIGH beats NORMAL under congestion | Experiment E4 from `docs/design_d2.md`, with a bandwidth-limited primary link, HIGH (`alerts`) and NORMAL traffic together | HIGH latency lower than NORMAL | | Pending |
| D2-9 | Event log written | After a publish, `tail -n 5 results/events.jsonl` | One valid JSON line per event with ts, event, topic, client_ip | | Pending |
| D2-10 | Block a host (stretch) | `POST /block` for pub2, then publish from pub2 | pub2's traffic is dropped | | Pending |

## Observations to explain in the viva

- **TCP and outages.** TCP retransmits with exponentially growing waits, so after a link returns it can take several seconds more before traffic flows again. In the preliminary baseline (D2-6) the stall (13.8 s) was longer than the outage (10 s).
- **Loss and delay.** Messages sent during the outage did not arrive late; they did not arrive at all. The scenario runner reports both the number lost and the delivery stall.
- **Why the baseline was run with `s1 s2` down.** With the loop present, the old controller floods packets around the triangle. Breaking the second path removes the loop, and also removes the backup path, which makes it behave like a network with no rerouting.

## Files produced by the scenario runner

`results/d2/<name>/` contains `pub.csv` (what was sent), `sub.csv` (what arrived and when), `meta.json` (times of the link-down and link-up events), `broker.log` and `summary.txt`.
