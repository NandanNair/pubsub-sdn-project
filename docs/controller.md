# OpenFlow SDN Controller Documentation

## Overview
The Ryu controller application (`controller/app.py`) manages the network control plane by dynamically learning host MAC addresses, installing OpenFlow 1.3 flow rules, and providing differentiated traffic handling (Publish-Subscribe QoS classification).

## Flow Priority Table

| Priority  | Match Criteria / Class               | Action / Purpose                                         |
|-----------|--------------------------------------|----------------------------------------------------------|
| **100**   | High-Priority Traffic (`CLASS=HIGH`) | Forwarded with high priority / specific QoS queue        |
| **50**    | Standard Traffic (`CLASS=NORMAL`)    | Standard L2/L3 forwarding rules                          |
| **0**     | Default Table-Miss                   | Send unmatched packets to Controller (`OFPP_CONTROLLER`) |

## Class Rules & MAC Learning
When an unknown host sends traffic, the switch forwards the packet to the controller via a `PacketIn` event. The controller records the host's MAC address and port mapping. 

Installing class rules (`CLASS=HIGH` or `CLASS=NORMAL`) upon MAC learning offloads forwarding decisions directly to the OpenFlow switch flow table. This eliminates the need for future packets from known MACs to trigger `PacketIn` events, significantly reducing latency and controller load.

## The Network Monitor
The controller's monitoring module periodically audits network performance by sending statistics request messages to switches. It tracks flow rates, packet counts, and link status, allowing the controller to log classification events (`CLASS=HIGH` / `CLASS=NORMAL`) and dynamically adjust rules if network congestion occurs.
