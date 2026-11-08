# OpenFlow SDN Controller Documentation

## Overview
The Ryu controller application (`controller/app.py`) manages the multi-switch network control plane. Building upon Deliverable 1, the D2 controller introduces dynamic topology discovery, loop-free shortest-path routing across a triangle topology, real-time link failure recovery, differentiated Quality of Service (QoS) traffic classification, and a REST API management interface.

---

## Flow Priority Table

| Priority | Match Criteria / Class | Action / Purpose |
| :--- | :--- | :--- |
| **100** | High-Priority Traffic (`CLASS=HIGH`, TCP Port 5001) | Fast-tracked routing and priority handling |
| **50** | Standard Traffic (`CLASS=NORMAL`, TCP Port 5002) | Standard L2/L3 forwarding rules |
| **1** | Learned L2 MAC Forwarding | Direct switch-level forwarding for known hosts |
| **0** | Default Table-Miss | Sends unmatched packets to the Controller (`OFPP_CONTROLLER`) |

---

## Architectural Components

### 1. Multi-Switch Discovery & Loop-Free Routing
* **Mechanism:** The controller utilizes `ryu.topology` API (`get_switch`, `get_link`) and listens to topology events (`EventSwitchEnter`, `EventSwitchLeave`, `EventLinkAdd`, `EventLinkDelete`) to maintain a dynamic adjacency graph of the network.
* **Loop Prevention:** Because the test topology forms a triangle (`s1`, `s2`, `s3`), naive flooding of ARP or unknown packets causes catastrophic broadcast storms. The controller implements a Breadth-First Search (BFS) shortest-path algorithm (`get_shortest_path`) to compute strict loop-free forwarding paths.

### 2. Dynamic Rerouting on Link Failures
* **Fault Tolerance:** When a physical link fails (captured via `EventLinkDelete`), the topology event handler instantly recalculates the adjacency graph.
* **Behavior:** Flow entries are updated dynamically so that active publisher-subscriber traffic bypasses the failed link and routes through the surviving path, preserving D1 class rules without dropping ongoing connections.

### 3. Traffic Prioritization & QoS Strategy
* **Classification:** Inspects Layer 4 TCP destination and source ports to categorize traffic into `CLASS=HIGH` (Port 5001, priority 100) and `CLASS=NORMAL` (Port 5002, priority 50).
* **Trade-off Analysis:** Path-based routing priority steers critical pub-sub traffic away from congested links. While OVS queues (Hierarchical Token Bucket) can shape physical bandwidth, combining path selection with flow priorities guarantees low-latency message delivery for alerts.

### 4. REST API Management Interface
Implemented using `ryu.app.wsgi` running on port `8080`:
* **`POST /subscription`**: Called by the broker to register dynamic topic subscriptions and client IP mappings.
* **`GET /stats`**: Returns a JSON payload containing active switch datapaths, link maps, flow packet counters, subscriptions, and blocked IPs.
* **`POST /block`**: Dynamically installs drop rules for specified host IPs to manage unauthorized traffic.

---

## MAC Learning & Monitoring
When an unknown host sends traffic, the switch triggers a `PacketIn` event. The controller records the host's MAC address and port mapping, installing proactive class rules. The background monitoring thread periodically audits flow statistics, logging classification events (`CLASS=HIGH` / `CLASS=NORMAL`) for verification and telemetry.
