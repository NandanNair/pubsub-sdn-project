# SDN Controller Implementation

## 1. Introduction

The SDN controller is implemented using the Ryu framework. It communicates with Open vSwitch switches using the OpenFlow 1.3 protocol.

The controller manages network forwarding, traffic classification, topology discovery, routing and link failure handling in the Pub-Sub system.

## 2. Controller Responsibilities

The controller performs the following functions:

- **MAC Learning:** Learns the MAC addresses of connected hosts and their switch ports.
- **Traffic Classification:** Classifies TCP traffic using destination and source ports.
- **Static Routing:** Uses predefined forwarding paths.
- **Dynamic Routing:** Calculates forwarding paths based on the discovered network topology.
- **Topology Discovery:** Maintains information about switches and links.
- **Link Failure Handling:** Detects link-status changes and updates forwarding rules.
- **Flow Monitoring:** Periodically collects flow statistics from switches.
- **REST API:** Provides endpoints for mode switching, subscriptions, statistics and IP blocking.

## 3. Traffic Classification

The controller classifies traffic based on TCP port numbers.

| TCP Port | Traffic Class | Flow Priority |
|---|---|---|
| 5001 | HIGH | 100 |
| 5002 | NORMAL | 50 |

Higher-priority rules are installed for HIGH traffic, while NORMAL traffic uses lower-priority rules.

These classifications are based on the project's configured TCP ports.

## 4. Routing Mechanism

The controller supports two routing modes:

- **Static Mode:** Uses predefined forwarding paths.
- **Dynamic Mode:** Uses topology information and a shortest-path algorithm to calculate forwarding paths.

The controller also uses a spanning-tree approach for controlled flooding to avoid forwarding loops.

## 5. Link Failure Handling

The controller monitors switch-port status changes.

When a link goes down, the controller refreshes the topology, removes stale flow rules and installs updated forwarding rules.

When the link becomes available again, the topology is refreshed and forwarding rules are recalculated.

## 6. Flow Monitoring

The controller periodically requests flow statistics from switches.

These statistics help observe packet counts and traffic activity and provide information for evaluating network behaviour.

## 7. REST API Endpoints

| Method | Endpoint | Purpose |
|---|---|---|
| POST | /subscription | Add a topic subscription |
| GET | /subscriptions | View subscriptions |
| POST | /mode | Change static/dynamic mode |
| GET | /stats | View controller statistics |
| POST | /block | Block an IP address |

The REST API is accessible through port 8080.

## 8. Testing and Results

The following tests were performed:

1. Checked network connectivity using Mininet pingall.
2. Checked controller statistics using the `/stats` endpoint.
3. Checked subscription registration and retrieval.
4. Tested switching between static and dynamic modes.
5. Tested link failure and recovery.
6. Inspected OpenFlow rules using ovs-ofctl.

Test outputs and logs are stored in the `results/` directory.

## 9. Conclusion

The Ryu controller provides centralized network management for the Pub-Sub system. It supports traffic classification, topology discovery, routing, link failure handling, monitoring and REST API operations.
