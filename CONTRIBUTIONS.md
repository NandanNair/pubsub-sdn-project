# Contributions

Computer Networks (UE25CS243A) mini project, Project 9: Publish-Subscribe Messaging System over TCP + SDN.

| Member | GitHub | Main area |
|---|---|---|
| Nandan Nair | [NandanNair](https://github.com/NandanNair) | Topology, integration, documentation, testing, experiments, demo UI |
| Prakul N.R | Prakul N.R / Prakul Neelakanth Rathod | Broker, publisher and subscriber clients, load generator |
| Nithin | [nithinjoker07](https://github.com/nithinjoker07) | SDN controller (Ryu) and its REST API |

The details below are taken from the commit history of this repository (`git log`, `git blame`). Every member can explain the whole system, as required for the individual viva.

## Nandan Nair

**Deliverable 1**
- Repository setup, project structure and VM setup guide (`docs/setup.md`).
- D1 design document (`docs/design.md`): architecture, message protocol, traffic classes.
- Single-switch Mininet topology (`topology/topo.py`).
- First version of the Ryu controller with TCP port classification (`controller/app.py`), and a stand-in server for testing it before the broker existed (`tests/stand_in_server.py`).
- D1 test cases, test helpers and evidence (`tests/test_cases.md`, `tests/pub.sh`, `tests/bad_json.py`, `results/d1_evidence.txt`), and the README.

**Deliverable 2**
- Triangle topology with a backup path and the controller address 10.0.0.254 reachable from the hosts, plus a `--bw` option for the congestion experiment (`topology/topo_triangle.py`).
- D2 design document (`docs/design_d2.md`): topology, REST API between broker and controller, link-failure scenario, evaluation plan, demo UI and the results section.
- Link-failure scenario runner (`tools/scenario.py`).
- Performance evaluation (`tools/experiments.py`): experiments E1 to E4 (static baseline vs dynamic SDN, link failure, HIGH vs NORMAL under congestion), 3 runs per condition, summary CSVs and graphs (`results/d2/`).
- Demo dashboard (`ui/app.py`): live network, link status, traffic classes, message feed and failover chart.
- D2 test cases D2-1 to D2-10, run and recorded with evidence (`tests/test_cases_d2.md`).
- Integration testing: found and reported the issues listed in `tests/test_cases_d2.md` (for example the controller state between networks, the static baseline rerouting, and the broker's REST field name), and retested them after the fixes.
- README D2 section and this file.

## Prakul N.R

**Deliverable 1**
- TCP broker (`broker/broker.py`): handles many clients at once, topic subscriptions, SUBSCRIBE / UNSUBSCRIBE / PUBLISH handling, error handling for malformed messages and disconnects.
- Publisher and subscriber clients (`clients/publisher.py`, `clients/subscriber.py`).
- D1 broker test results (`results/d1_broker_tests.txt`).

**Deliverable 2**
- Broker-to-controller REST notifications: `POST /subscription` on every subscribe and unsubscribe, sent from a background thread with a 1 second timeout, failures logged, controller URL as a command-line argument.
- Broker event log for the UI (`results/events.jsonl`): one JSON line per connect, subscribe, unsubscribe, publish and disconnect.
- Concurrent client handling with `select()` for reading, plus a send queue and sender thread per subscriber, so one slow subscriber cannot block the others.
- Newline message framing, with the D1 clients updated to match and their command-line interface kept.
- Load generator (`tools/loadgen.py`).

## Nithin

**Deliverable 1**
- Extended the controller (`controller/app.py`), wrote the controller documentation (`docs/controller.md`), and recorded the controller log and flow tables (`results/d1_controller_log.txt`, `results/d1_flows.txt`).

**Deliverable 2**
- Multi-switch controller (`controller/app.py`): topology discovery with `ryu.topology`, and loop-free forwarding on the triangle (computed paths and spanning-tree flooding instead of plain flooding), which prevents a broadcast storm.
- Dynamic rerouting: on a port-status or link event the controller recomputes paths and reinstalls flows, so traffic moves to the backup path.
- Static and dynamic modes, switchable through `POST /mode`, with static mode kept as the no-rerouting baseline.
- Class-based paths: HIGH traffic (port 5001) on the direct s1-s3 path and NORMAL traffic (port 5002) through s2, with protocol-specific flows so pub/sub traffic is always classified.
- REST API on port 8080: `POST /subscription`, `GET /subscriptions`, `GET /stats` (mode, switches, link status, per-class packet counts), `POST /mode` and `POST /block` (installs a drop rule for a host on every switch).
- Controller documentation (`docs/controller.md`) and controller test evidence (`results/controller_logs.txt`).

## Summary by file

| Area | Files | Main author |
|---|---|---|
| Controller | `controller/app.py`, `docs/controller.md` | Nithin (first D1 version by Nandan) |
| Broker and clients | `broker/broker.py`, `clients/`, `tools/loadgen.py` | Prakul |
| Topologies | `topology/topo.py`, `topology/topo_triangle.py` | Nandan |
| Experiments and scenario | `tools/experiments.py`, `tools/scenario.py`, `results/d2/` | Nandan |
| Demo UI | `ui/app.py` | Nandan |
| Design documents | `docs/design.md`, `docs/design_d2.md`, `docs/setup.md` | Nandan |
| Tests | `tests/` | Nandan (D1 broker test results by Prakul) |
| README | `README.md` | Nandan |

To check this against the history: `git shortlog -sn --no-merges` lists commits per member, and `git log --author="<name>" --stat` shows the files each member changed. GitHub's **Insights > Contributors** page shows the same.

