# Design Document: Publish-Subscribe Messaging over TCP + SDN

Project 9, Computer Networks (UE25CS243A). This document is the shared contract for the whole team. Everyone codes against it. If something needs to change, change it here first and tell the group.

## 1. Overview

We build a topic-based publish-subscribe messaging system using raw TCP sockets and run it on a Mininet network whose switches are controlled by a Ryu (OpenFlow 1.3) SDN controller.

- **Publishers** send messages tagged with a topic.
- **Subscribers** register interest in topics.
- **The broker** receives published messages and forwards each one to every subscriber of that topic.
- **The SDN controller** decides how the switch forwards packets. In D1 it classifies traffic by TCP port and installs flow rules. In D2 it will react dynamically (rerouting, prioritisation, blocking).

Publishers and subscribers never talk to each other directly. The broker decouples them.

## 2. Architecture

```
  pub1 (10.0.0.1) --+
                    |
  pub2 (10.0.0.2) --+--- s1 --- broker (10.0.0.3)  listens on TCP 5001, 5002
                    |     |
  sub1 (10.0.0.4) --+     |
                    |     |
  sub2 (10.0.0.5) --+     +--- OpenFlow 1.3 --- Ryu controller (127.0.0.1:6653)
```

D1 uses one switch. D2 will extend this to a triangle of three switches (two paths between publishers and broker) so that rerouting and link-failure demos are possible.

### Components

| Component | Language / tool | Role |
|---|---|---|
| `broker/broker.py` | Python 3, `socket`, `threading` | TCP server, topic table, forwarding |
| `clients/publisher.py` | Python 3, `socket` | Connects to broker, sends PUBLISH messages |
| `clients/subscriber.py` | Python 3, `socket` | Connects to broker, sends SUBSCRIBE, prints DELIVER messages |
| `controller/app.py` | Ryu | Learning switch + TCP-port flow rules + logging |
| `topology/topo.py` | Mininet Python API | Builds hosts, switch, links, remote controller |

## 3. Addressing and ports

| Host | IP | Role |
|---|---|---|
| pub1 | 10.0.0.1 | publisher |
| pub2 | 10.0.0.2 | publisher |
| broker | 10.0.0.3 | broker |
| sub1 | 10.0.0.4 | subscriber |
| sub2 | 10.0.0.5 | subscriber |

The topology script sets these IPs explicitly.

**Broker listens on two TCP ports. The port is the traffic class** that the SDN controller uses to classify traffic:

| Port | Traffic class | Topics |
|---|---|---|
| 5001 | HIGH priority | `alerts` |
| 5002 | NORMAL | every other topic (`sports`, `news`, `weather`, ...) |

Rules:
- Both ports are served by the same broker process and share one topic table.
- A client must use port 5001 for topic `alerts` and port 5002 for any other topic. If it uses the wrong port, the broker replies with an ERROR and keeps the connection open.
- A client that wants several topics from both classes opens one connection per port.

## 4. Application protocol

- Transport: TCP.
- Encoding: UTF-8 JSON, **one JSON object per line, terminated by `\n`**.
- TCP is a byte stream and does not preserve message boundaries. Every reader must buffer incoming bytes and split on `\n`. Never assume one `recv()` equals one message. Messages must not contain a raw newline (JSON escapes it as `\n` inside strings, so this is automatic).

### Client to broker

```
{"type":"SUBSCRIBE","topic":"alerts"}
{"type":"UNSUBSCRIBE","topic":"alerts"}
{"type":"PUBLISH","topic":"alerts","payload":"fire in building 3","ts":1730000000.123}
```

`ts` is the publisher's `time.time()` at send. The VM has one clock, so latency = receive time minus `ts` is valid for D2 measurements.

### Broker to client

```
{"type":"ACK","ref":"SUBSCRIBE","topic":"alerts"}
{"type":"ACK","ref":"PUBLISH","topic":"alerts","delivered":2}
{"type":"DELIVER","topic":"alerts","payload":"fire in building 3","ts":1730000000.123}
{"type":"ERROR","msg":"wrong port for topic alerts, use 5001"}
```

- `ACK` for SUBSCRIBE/UNSUBSCRIBE confirms registration.
- `ACK` for PUBLISH reports how many subscribers received it (`delivered`, may be 0).
- `DELIVER` is what subscribers receive. `ts` is copied from the original PUBLISH.
- `ERROR` is returned for malformed JSON, unknown `type`, missing fields, or wrong port. The broker never closes the connection because of a bad message.

## 5. Broker behaviour

1. Create one listening socket per port (5001, 5002) with `SO_REUSEADDR`, `bind("0.0.0.0", port)`, `listen()`.
2. `accept()` loop per port, spawning **one thread per client connection**.
3. Shared state: `topics = {topic: set(client_sockets)}` protected by a `threading.Lock`.
4. On SUBSCRIBE: add the socket to the topic set, send ACK.
5. On UNSUBSCRIBE: remove it, send ACK.
6. On PUBLISH: copy the subscriber set under the lock, then send a DELIVER line to each subscriber (outside the lock). If a send fails, drop that subscriber. Send ACK with the delivered count to the publisher.
7. On disconnect or socket error: remove the socket from every topic and close it. The broker must keep running.
8. Log each event with a timestamp (connect, subscribe, publish, deliver, disconnect).

Use a per-socket send lock (or one sender per subscriber) if two publishers can write to the same subscriber at the same time, so lines are not interleaved.

## 6. Clients

### Publisher
`python3 publisher.py --broker 10.0.0.3 --topic alerts --count 10 --interval 0.5 --payload "text"`
Picks the port from the topic (alerts -> 5001, else 5002), connects, sends `count` PUBLISH messages, prints each ACK, exits.

### Subscriber
`python3 subscriber.py --broker 10.0.0.3 --topics alerts,sports`
Opens a connection per port needed, sends SUBSCRIBE for each topic, then prints every DELIVER line with a receive timestamp until Ctrl+C. Optional `--log file.csv` to record receive times (used in D2).

## 7. SDN controller (Ryu, OpenFlow 1.3)

The controller runs at `127.0.0.1:6653`.

### D1 requirements
1. Base behaviour: learning switch (as in `ryu.app.simple_switch_13`), so all hosts can reach each other.
2. Table-miss rule at priority 0 sends unknown packets to the controller.
3. **Traffic classification by TCP port.** For IPv4 TCP packets whose `tcp_dst` or `tcp_src` is 5001 or 5002, install a flow rule with higher priority than the plain L2 rules:

| Class | Match | Priority | Action |
|---|---|---|---|
| HIGH | `eth_type=0x0800, ip_proto=6, tcp_dst=5001` and the reverse direction `tcp_src=5001` | 100 | output to learned port |
| NORMAL | same with 5002 | 50 | output to learned port |
| L2 learned | `in_port, eth_dst` | 1 | output to learned port |
| table-miss | (all) | 0 | send to controller |

   Return traffic (broker to subscriber) has `tcp_src` equal to 5001/5002, so both directions must be matched.
4. **Logging.** Print a line each time a flow is classified, e.g. `CLASS=HIGH alerts tcp_dst=5001 10.0.0.1 -> 10.0.0.3`. This is visible evidence for the demo.
5. Flows must be visible with `sudo ovs-ofctl -O OpenFlow13 dump-flows s1`, and the packet counters on the 5001/5002 rules must increase while traffic flows.

### D2 preview (not needed now)
- REST endpoint so the broker can notify the controller of subscribe/unsubscribe events.
- Rerouting on link failure and congestion, using the second path in a three-switch topology.
- Prioritisation (meters/queues) for the HIGH class, blocking unauthorised hosts.

## 8. End-to-end message flow (one message)

1. `sub1` connects to broker:5001 and sends `SUBSCRIBE alerts`.
2. The first packets of this TCP connection reach `s1`, which has no matching rule, so it sends a packet-in to Ryu.
3. Ryu sees `tcp_dst=5001`, logs `CLASS=HIGH`, installs a priority-100 flow, and tells the switch to forward the packet.
4. The broker records `sub1`'s socket under topic `alerts` and replies with ACK.
5. `pub1` connects to broker:5001 and sends `PUBLISH alerts`. The same classification happens for this flow.
6. The broker looks up subscribers of `alerts`, sends `DELIVER` to each, and sends ACK (`delivered=1`) to `pub1`.
7. `sub1` prints the message. The switch forwarded all of this using flow rules the controller installed.

## 9. Test cases for D1

| # | Test | Expected |
|---|---|---|
| 1 | `pingall` in the topology | 0% dropped |
| 2 | sub1 subscribes to `alerts`, pub1 publishes `alerts` | sub1 receives it |
| 3 | sub1 subscribes to `sports` only, pub1 publishes `alerts` | sub1 receives nothing |
| 4 | sub1 and sub2 both subscribe to `alerts`, one publish | both receive it, ACK says `delivered=2` |
| 5 | A subscriber disconnects, then a publish happens | broker keeps running, no crash |
| 6 | Malformed JSON sent to the broker | ERROR reply, connection stays open |
| 7 | Publish `alerts` to port 5002 | ERROR (wrong port) |
| 8 | Traffic on 5001 and on 5002, then `dump-flows s1` | both rule sets exist and their `n_packets` counters increased |
| 9 | Ryu console during test 2 | shows `CLASS=HIGH` log line |

Record expected vs actual in `tests/test_cases.md`.

## 10. How to run (every time)

1. `sudo mn -c` (cleanup; this kills any running Ryu, so do it first)
2. Terminal 1: `source ~/ryu-env/bin/activate && ryu-manager controller/app.py`
3. Terminal 2: `sudo python3 topology/topo.py`
4. In the Mininet CLI: `broker python3 broker/broker.py &`, then start subscribers, then publishers (details added to the README once the topology script exists).
5. Terminal 3: `sudo ovs-ofctl -O OpenFlow13 dump-flows s1`

## 11. Ownership for D1

| Person | Owns |
|---|---|
| Nandan | Architecture doc, `topology/topo.py`, integration, test cases |
| Teammate 1 | `broker/broker.py`, `clients/publisher.py`, `clients/subscriber.py` |
| Teammate 2 | `controller/app.py` |

Everyone must be able to explain every part at the viva.

## 12. Git rules

- Work only in your own folder to avoid conflicts.
- `git pull` before you start and before you push.
- Small commits with clear messages.
