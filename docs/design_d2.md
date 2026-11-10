# Design Document, D2: Dynamic SDN, Failure Scenario, Performance Evaluation

This extends `docs/design.md` (D1). Everything in D1 (hosts, ports 5001/5002, the JSON protocol, the traffic classes) stays the same unless stated here. This file is the contract for D2: if something needs to change, change it here first and tell the group.

## 1. D2 goals

1. **Dynamic SDN behaviour.** The controller reacts to events instead of only installing static rules: link failure triggers rerouting, the HIGH class is prioritised, and the broker's subscription events reach the controller.
2. **A challenging scenario.** A link fails while traffic is flowing, and the network recovers on the other path.
3. **Performance evaluation.** Latency, throughput and loss are measured for dynamic SDN against a static baseline.
4. **Demo UI.** A web dashboard that shows the live demo (section 8).

## 2. Topology (triangle)

```
        pub1, pub2
            |
           s1 ----------- s3 --- broker (10.0.0.3)
             \           /        |
              \         /      VM / controller REST (10.0.0.254)
                  s2
                  |
              sub1, sub2
```

- Switches `s1`, `s2`, `s3` (dpid 1, 2, 3), OpenFlow 1.3, one remote controller at 127.0.0.1:6653.
- Paths from publishers to the broker: **primary** s1-s3 (direct), **backup** s1-s2-s3.
- Hosts and IPs are unchanged from D1.
- The VM is attached to `s3` with the address **10.0.0.254/24**. Mininet hosts cannot use 127.0.0.1 to reach the controller, so hosts call the REST API at `http://10.0.0.254:8080`.
- Port numbers as built by `topology/topo_triangle.py` (for debugging only; the controller must discover the topology, not hard-code it):

| Switch | Ports |
|---|---|
| s1 | 1 pub1, 2 pub2, 3 to s3, 4 to s2 |
| s2 | 1 sub1, 2 sub2, 3 to s1, 4 to s3 |
| s3 | 1 broker, 2 to s1, 3 to s2, 4 to the VM |

- **The triangle is a loop.** Flooding (ARP, unknown destinations) must not circulate forever. The controller must prevent broadcast storms (for example a spanning tree or topology-aware ARP handling).
- Fail the primary path from the Mininet CLI with `link s1 s3 down` (restore with `link s1 s3 up`).

## 3. Controller requirements

1. **Multi-switch learning and forwarding** across s1, s2 and s3, with loop-free flooding. Topology is discovered with `ryu.topology` (`ryu-manager --observe-links`).
2. **Class rules** from D1 are kept: priority 100 for TCP 5001 (HIGH) and priority 50 for 5002 (NORMAL), matching both `tcp_dst` and `tcp_src`, installed on every switch along the chosen path.
3. **Rerouting.** On a link-down event the controller recomputes paths and reinstalls flows so traffic uses the other path. On link-up it may move back.
4. **Prioritisation.** The HIGH class gets preferential treatment (preferred path, queue or meter). The choice and its trade-offs are documented in `docs/controller.md`.
5. **Monitoring.** The `CLASS=` log lines from D1 are kept, now per switch.
6. **Mode switch (for the performance evaluation).** The controller supports `static` mode (fixed shortest path, no rerouting, no prioritisation) and `dynamic` mode, chosen by a command line option or `POST /mode`. *Proposed; needs Nithin's confirmation.*

## 4. Controller REST API (Ryu WSGI, port 8080)

All bodies are JSON. Errors return an HTTP 4xx with `{"error": "..."}`.

| Method and path | Body | Response | Used by |
|---|---|---|---|
| `POST /subscription` | `{"action":"subscribe"\|"unsubscribe","topic":"alerts","ip":"10.0.0.4"}` | `200 {"ok":true}` | broker |
| `GET /stats` | none | `{"classes":{"HIGH":{"packets":N,"bytes":N},"NORMAL":{...}},"links":[{"a":"s1","b":"s3","up":true},...],"mode":"dynamic"}` | UI, tests |
| `GET /subscriptions` | none | `{"alerts":["10.0.0.4"],"sports":["10.0.0.5"]}` | UI, tests |
| `POST /block` | `{"ip":"10.0.0.2"}` | `200 {"ok":true}` (installs a drop rule) | optional, stretch |
| `POST /unblock` | `{"ip":"10.0.0.2"}` | `200 {"ok":true}` | optional, stretch |
| `POST /mode` | `{"mode":"static"\|"dynamic"}` | `200 {"ok":true}` | tests |

## 5. Broker requirements (additions to D1)

1. On every SUBSCRIBE and UNSUBSCRIBE, POST to `/subscription`. Non-blocking (background thread or queue), 1 second timeout, never crashes or stalls the broker if the controller is unreachable. The controller URL is a command line argument, default `http://10.0.0.254:8080`.
2. **Event log for the UI.** One JSON line per event in `results/events.jsonl`:
   `{"ts":1730000000.123,"event":"publish","topic":"alerts","client_ip":"10.0.0.1","delivered":2}`
   Events: `connect`, `subscribe`, `unsubscribe`, `publish`, `disconnect`. The file is flushed after every line.
3. Per-subscriber send queue and sender thread, replacing the blocking `sendall`. Do not use `conn.settimeout` (it also affects the read loop).

## 6. Challenging scenario: link failure with reroute

**Setup.** `dynamic` mode, sub1 subscribed to `alerts`, pub1 sending `alerts` messages at a steady rate (for example 10 per second) for 60 seconds.

**Event.** At about 20 seconds, run `link s1 s3 down`. At about 40 seconds, run `link s1 s3 up`.

**Expected.**
- Delivery continues over s1-s2-s3 after a short gap.
- After link-up, traffic may return to the direct path.
- No broker or client restarts.

**Measured.**
- **Recovery time:** time from `link down` to the next successful delivery.
- **Messages lost** during the gap.
- Latency before, during and after the failure.

The same run in `static` mode is the comparison: it should not recover.

## 7. Performance evaluation plan

Tool: `tools/loadgen.py` (publishes N messages at a given rate from a host; records latency at the subscriber, throughput and loss; writes a CSV).

| Experiment | Conditions | Metrics |
|---|---|---|
| E1 Baseline | `static` mode, no failure, increasing rates (10, 50, 100, 200 msg/s) | mean and p95 latency, throughput, loss |
| E2 Dynamic, no failure | `dynamic` mode, same rates | same |
| E3 Failure | scenario of section 6, `static` vs `dynamic` | recovery time, messages lost |
| E4 Congestion | HIGH (`alerts`) and NORMAL traffic at the same time on a bandwidth-limited primary link, `static` vs `dynamic` | latency of HIGH vs NORMAL |

- Each experiment is repeated at least 3 times, and results are reported as the mean with the spread.
- CSV columns: `experiment, mode, rate, run, class, sent, received, loss_pct, mean_ms, p95_ms, throughput_msg_s`.
- Results go in `results/d2/`, with graphs generated from the CSVs.
- E4 needs bandwidth-limited links (Mininet `TCLink`); the topology gets a `--bw` option for it.

## 8. Demo UI

A web dashboard started with one command on the VM. It shows a live message feed (from `results/events.jsonl`), per-class counters and link status (from `GET /stats`), a topology view, and buttons for the demo (start broker, add subscriber, publish, fail or restore a link). It runs Mininet in the same process so the buttons can run commands on hosts. Details to be added when the UI is started.

## 9. D2 test cases

| # | Test | Expected |
|---|---|---|
| D2-1 | Triangle topology up, then `pingall` | 0% dropped and no broadcast storm (CPU normal, no flood of packet-ins) |
| D2-2 | A host calls `curl http://10.0.0.254:8080/stats` | JSON response |
| D2-3 | sub1 subscribes to `alerts`, then `GET /subscriptions` | `alerts` lists 10.0.0.4 |
| D2-4 | Controller stopped, then a client subscribes | Broker keeps working, logs the failure |
| D2-5 | `link s1 s3 down` during traffic (dynamic) | Delivery resumes via s2, recovery time recorded |
| D2-6 | Same in `static` mode | No recovery (baseline) |
| D2-7 | `link s1 s3 up` | Traffic keeps flowing |
| D2-8 | HIGH and NORMAL under congestion (dynamic) | HIGH latency lower than NORMAL |
| D2-9 | `results/events.jsonl` after a publish | One valid JSON line per event |
| D2-10 | `POST /block` for a publisher (if implemented) | That host's traffic is dropped |

Results are recorded in `tests/test_cases_d2.md` in the same format as D1.

## 10. Ownership for D2

| Person | Owns |
|---|---|
| Nandan | `topology/topo_triangle.py`, scenario scripts, experiments E1 to E4, D2 test cases and docs, UI, this document |
| Prakul | broker REST notification, event log, send queues, `tools/loadgen.py` |
| Nithin | `controller/` (multi-switch, loop handling, rerouting, prioritisation, REST API, mode switch), `docs/controller.md` |

Everyone must be able to explain every part at the viva.
