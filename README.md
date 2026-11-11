# Publish-Subscribe Messaging System over TCP + SDN

Computer Networks (UE25CS243A) mini project, Project 9.

A topic-based publish-subscribe system built with raw TCP sockets (Python) and run on a Mininet network whose switch is controlled by a Ryu (OpenFlow 1.3) SDN controller. The controller classifies pub/sub traffic by TCP port and installs flow rules accordingly.

## Architecture

```
  pub1 (10.0.0.1) --+
  pub2 (10.0.0.2) --+--- s1 --- broker (10.0.0.3)   TCP 5001 (alerts), 5002 (other topics)
  sub1 (10.0.0.4) --+     |
  sub2 (10.0.0.5) --+     +--- OpenFlow 1.3 --- Ryu controller (127.0.0.1:6653)
```

- **Publishers** send messages tagged with a topic.
- **Subscribers** register for a topic and receive every message published to it.
- **Broker** receives published messages and forwards them to the subscribers of that topic (thread per client, newline-delimited JSON, shared topic table protected by a lock).
- **Ryu controller** is a learning switch that also installs priority-based flow rules by TCP port: 5001 is class HIGH (priority 100) and 5002 is class NORMAL (priority 50). A monitor logs the flow counters every 5 seconds.

Full design, message protocol and message flow: [`docs/design.md`](docs/design.md).

## Repository layout

| Path | Contents |
|---|---|
| `broker/broker.py` | TCP broker |
| `clients/publisher.py`, `clients/subscriber.py` | Publisher and subscriber clients |
| `controller/app.py` | Ryu controller |
| `topology/topo.py` | D1 Mininet topology (1 switch, 5 hosts, remote controller) |
| `topology/topo_triangle.py` | D2 topology: 3 switches in a triangle, two paths, VM attached as 10.0.0.254 |
| `tools/scenario.py` | Single link-failure run with a probe publisher and subscriber |
| `tools/experiments.py` | D2 performance evaluation: experiments E1 to E4, summary CSVs and graphs |
| `tools/loadgen.py` | Load generator |
| `ui/app.py` | Live demo dashboard (network, traffic classes, message feed, failover runs) |
| `tests/` | Test helpers (`pub.sh`, `bad_json.py`, `stand_in_server.py`) and [`test_cases.md`](tests/test_cases.md) |
| `results/` | Saved evidence: D1 in `results/`, D2 runs, CSVs and graphs in `results/d2/` |
| `docs/` | `setup.md`, `design.md` (D1), `design_d2.md` (D2), `controller.md` |

## Setup

Follow [`docs/setup.md`](docs/setup.md) to install Mininet and Ryu (Ryu needs a Python 3.9 virtualenv). Check that Mininet and Ryu work together before running the project.

## How to run (D1, single switch)

Always in this order. `sudo mn -c` kills any running Ryu, so never run it while Ryu is up.

1. Clean up: `sudo mn -c`
2. Terminal 1, start the controller:
   ```
   source ~/ryu-env/bin/activate
   cd ~/pubsub-sdn-project
   ryu-manager controller/app.py
   ```
   Wait until it prints `Switch 1 connected` after step 3.
3. Terminal 2, start the topology (it runs a connectivity check and opens the Mininet CLI):
   ```
   cd ~/pubsub-sdn-project
   sudo python3 topology/topo.py
   ```
   Expect `0% dropped (20/20 received)`.
4. At the `mininet>` prompt (one command per line), start the broker and two subscribers:
   ```
   broker python3 broker/broker.py > results/broker.log 2>&1 &
   sub1 python3 clients/subscriber.py 10.0.0.3 alerts > results/sub1.log 2>&1 &
   sub2 python3 clients/subscriber.py 10.0.0.3 sports > results/sub2.log 2>&1 &
   ```
5. Publish a message and check who received it:
   ```
   pub1 sh tests/pub.sh alerts "fire in building 3"
   sh cat results/sub1.log
   sh cat results/sub2.log
   ```
   sub1 receives the message; sub2 (subscribed to `sports`) does not.
6. See the flow rules (in a third terminal, while Mininet is running):
   ```
   sudo ovs-ofctl -O OpenFlow13 dump-flows s1 | grep -E "priority=(100|50)"
   ```
   About 5 seconds after traffic, the Ryu terminal prints lines like `CLASS=HIGH tcp_dst=5001 -> ... packets=N`.

### Client usage

```
python3 clients/publisher.py <broker_ip> <topic> [port]    # reads lines from stdin, one message per line
python3 clients/subscriber.py <broker_ip> <topic> [port]   # prints each delivered message with latency
sh tests/pub.sh <topic> "<message>" [port]                 # sends one message (run on a host in Mininet)
```

The topic `alerts` must use port 5001. Every other topic uses port 5002. If the port is omitted, the clients pick the correct one.

## D2: dynamic SDN

D2 moves to a triangle of three switches so there is a backup path, and makes the controller react to the network and to the application. The full D2 design is in [`docs/design_d2.md`](docs/design_d2.md).

```
            pub1 (10.0.0.1)   pub2 (10.0.0.2)
                    \        /
                       s1
                     /    \
       primary path /      \ backup path
                   /        \
   broker -------- s3 ------ s2 -------- sub1 (10.0.0.4)
   (10.0.0.3)      |                 \-- sub2 (10.0.0.5)
                   VM 10.0.0.254 (Ryu REST API on port 8080)
```

What D2 adds:
- **Loop-free forwarding.** The controller discovers the topology with `ryu.topology` (`--observe-links`) and forwards along computed paths instead of flooding, so the triangle's loop does not cause a broadcast storm.
- **Rerouting.** When a link goes down, the controller recomputes the paths and reinstalls the flows, so publisher traffic moves from s1-s3 to s1-s2-s3.
- **REST API** (port 8080): `POST /subscription` (sent by the broker on every subscribe and unsubscribe), `GET /subscriptions`, `GET /stats`, `POST /mode` (`static` or `dynamic`), `POST /block`.
- **Broker:** a send queue and sender thread per subscriber, REST notifications to the controller in the background, and an event log in `results/events.jsonl`.

### How to run D2

Always restart Ryu before starting a new Mininet network (see Known issues).

1. Clean up: `sudo mn -c` (never while Ryu is running).
2. Terminal 1, start the controller with link discovery:
   ```
   source ~/ryu-env/bin/activate
   cd ~/pubsub-sdn-project
   ryu-manager --observe-links controller/app.py
   ```
3. Terminal 2, start the triangle and wait about 10 seconds for link discovery:
   ```
   cd ~/pubsub-sdn-project
   sudo python3 topology/topo_triangle.py --skip-ping
   mininet> pingall          (expect 0% dropped, no broadcast storm)
   ```
4. Start the broker with the controller address, and a subscriber and publisher:
   ```
   broker python3 broker/broker.py --controller=http://10.0.0.254:8080/subscription &
   sub1 python3 tools/experiments.py sub 10.0.0.3 alerts --port 5001 --out /tmp/sub1.csv &
   pub1 python3 tools/experiments.py pub 10.0.0.3 alerts --port 5001 --rate 5 --count 10 --out /tmp/pub1.csv
   sub1 cat /tmp/sub1.csv
   ```
5. Fail and restore the primary path while traffic flows: `link s1 s3 down`, then `link s1 s3 up`.
6. Query the controller from a host: `pub1 curl -s http://10.0.0.254:8080/stats`.

### Link-failure scenario and performance evaluation

With Ryu running (and Mininet not running), from the repo root:
```
sudo python3 tools/experiments.py run --exp E3 --runs 1     # link failure, static vs dynamic
sudo python3 tools/experiments.py run --exp all             # E1 to E4, 3 runs each (about 40 minutes)
sudo chown -R $USER:$USER results
python3 tools/experiments.py summary                        # results/d2/summary.csv and aggregate.csv
python3 tools/experiments.py plot                           # graphs in results/d2/graphs/
```
The script switches the controller between `static` and `dynamic` through `POST /mode`, builds a fresh triangle for each run, waits until `pingall` succeeds, and measures latency at the subscriber, throughput, loss and, for the failure runs, the longest delivery stall.

| Experiment | What it compares |
|---|---|
| E1 | Static mode, no failure, 10 / 50 / 100 / 200 msg/s (baseline) |
| E2 | Dynamic mode, same rates |
| E3 | Link s1-s3 fails at 20 s and returns at 40 s, static vs dynamic |
| E4 | HIGH (`alerts`, 5001) and NORMAL (`news`, 5002) together on 1 Mbit/s links, static vs dynamic |

### Demo dashboard

```
python3 ui/app.py
```
Then open http://localhost:8000 in Firefox. It shows the triangle with link status, the HIGH / NORMAL counters from `GET /stats`, the live message feed from `results/events.jsonl`, and the latency chart of any failover run in `results/d2/`.

### Known issues

These were found while testing and have been reported; see [`tests/test_cases_d2.md`](tests/test_cases_d2.md).
- The controller only works for the first Mininet network after Ryu starts, so restart Ryu before each new network.
- The controller's `static` mode still reroutes on a link failure, so it is not yet a valid no-rerouting baseline.
- `POST /block` returns success but does not drop the host's traffic yet.
- The broker sends `client_ip` instead of `ip` to `POST /subscription`, so `GET /subscriptions` stays empty.
- The D1 clients (`clients/`, `tests/pub.sh`) do not end messages with a newline and the publisher sends `content`, which the D2 broker does not accept. The probes in `tools/experiments.py` use the D2 format.

## Tests

All D1 test cases, with procedure, expected and actual results, are in [`tests/test_cases.md`](tests/test_cases.md). Raw terminal output is in `results/d1_evidence.txt`. The D2 test cases and their results are in [`tests/test_cases_d2.md`](tests/test_cases_d2.md).

## Team

| Member | Part |
|---|---|
| Nandan Nair | Architecture and design doc, Mininet topology, integration, tests |
| Prakul Neelakanth Rathod | Broker, publisher, subscriber |
| Nithin | Ryu controller |

## Status

- **D1** (basic end-to-end system with SDN traffic classification): complete.
- **D2** (in progress):
  - Done and tested: triangle topology, loop-free forwarding (no broadcast storm), controller REST API, broker send queues and event log, the broker working when the controller is unreachable, and rerouting on link failure (600 of 600 messages delivered with about 0.3 s outage).
  - In progress: static baseline mode, HIGH-class prioritisation, blocking, the broker's subscription notification, full performance evaluation (E1 to E4) and results write-up.
