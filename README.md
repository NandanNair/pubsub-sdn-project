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
| `topology/topo.py` | Mininet topology (1 switch, 5 hosts, remote controller) |
| `tests/` | Test helpers (`pub.sh`, `bad_json.py`, `stand_in_server.py`) and [`test_cases.md`](tests/test_cases.md) |
| `results/` | Saved evidence (`d1_evidence.txt`) |
| `docs/` | `setup.md`, `design.md` |

## Setup

Follow [`docs/setup.md`](docs/setup.md) to install Mininet and Ryu (Ryu needs a Python 3.9 virtualenv). Check that Mininet and Ryu work together before running the project.

## How to run

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

## Tests

All D1 test cases, with procedure, expected and actual results, are in [`tests/test_cases.md`](tests/test_cases.md). Raw terminal output is in `results/d1_evidence.txt`.

## Team

| Member | Part |
|---|---|
| Nandan Nair | Architecture and design doc, Mininet topology, integration, tests |
| Prakul Neelakanth Rathod | Broker, publisher, subscriber |
| Nithin | Ryu controller |

## Status

D1 (basic end-to-end system with SDN traffic classification) is complete. Planned for D2: dynamic SDN behaviour (rerouting, prioritisation, blocking), link-failure and congestion scenarios, and performance comparison against a static baseline.
