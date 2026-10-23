# D1 Test Cases

Environment: Ubuntu VM, Mininet (single switch `s1`, hosts pub1 10.0.0.1, pub2 10.0.0.2, broker 10.0.0.3, sub1 10.0.0.4, sub2 10.0.0.5), Ryu controller `controller/app.py` on 127.0.0.1:6653, broker `broker/broker.py` on 10.0.0.3 (TCP 5001 and 5002).

Run date: 23 Oct 2026. Raw output is saved in `results/d1_evidence.txt`.

## Setup used for every test

1. `sudo mn -c`
2. Terminal 1: `source ~/ryu-env/bin/activate && ryu-manager controller/app.py`
3. Terminal 2: `sudo python3 topology/topo.py`
4. At the `mininet>` prompt, start the broker and two subscribers:
   ```
   broker python3 broker/broker.py > results/broker.log 2>&1 &
   sub1 python3 clients/subscriber.py 10.0.0.3 alerts > results/sub1.log 2>&1 &
   sub2 python3 clients/subscriber.py 10.0.0.3 sports > results/sub2.log 2>&1 &
   ```

Helper scripts: `tests/pub.sh <topic> "<message>" [port]` publishes one message; `tests/bad_json.py` sends an invalid line to the broker.

## Results

| # | Test | Procedure (at `mininet>`) | Expected | Actual | Result |
|---|---|---|---|---|---|
| 1 | Basic connectivity | Printed by `topo.py` at startup (`pingall`) | 0% dropped | `Results: 0% dropped (20/20 received)` | Pass |
| 2 | Subscriber receives its topic | `pub1 sh tests/pub.sh alerts "fire in building 3"` then `sh cat results/sub1.log` | sub1 receives the message; publisher ACK shows `delivered: 1` | ACK `{"type": "ACK", "ref": "PUBLISH", "topic": "alerts", "delivered": 1}`; sub1.log shows `[alerts] fire in building 3 (3.0 ms)` | Pass |
| 3 | Subscriber does not receive other topics | `sh cat results/sub2.log` after test 2 (sub2 subscribed to `sports`) | sub2 receives nothing new | sub2.log contains only its Connected line and the SUBSCRIBE ACK | Pass |
| 4 | Two subscribers on one topic | Start a second `alerts` subscriber on sub2, then `pub1 sh tests/pub.sh alerts "second alert"` | Both receive it; ACK shows `delivered: 2` | ACK shows `"delivered": 2` | Pass |
| 5 | Subscriber disconnects, broker survives | Kill the second subscriber, then `pub1 sh tests/pub.sh alerts "after disconnect"`, then `broker pgrep -f broker.py` | Publish succeeds with `delivered: 1`; broker process still running | ACK shows `"delivered": 1`; `pgrep` printed the same PID (14267) before and after | Pass |
| 6 | Malformed JSON | `pub1 python3 tests/bad_json.py` | ERROR reply; connection stays open | `b'{"type": "ERROR", "msg": "malformed JSON"}\n'` | Pass |
| 7 | Wrong port for topic | `pub1 sh tests/pub.sh alerts "wrong port" 5002` | ERROR: use port 5001 | `{"type": "ERROR", "msg": "wrong port for topic alerts, use 5001"}` | Pass |
| 8 | Flow rules exist and counters increase | `sudo ovs-ofctl -O OpenFlow13 dump-flows s1 \| grep -E "priority=(100\|50)"` | Rules for 5001 (priority 100) and 5002 (priority 50) with `n_packets` above 0 | `priority=100,...,dl_dst=00:00:00:00:00:01,tp_dst=5001` n_packets=38; `priority=50,...,dl_dst=00:00:00:00:00:01,tp_dst=5002` n_packets=10; return-path `tp_src=5001` rules towards pub1 (16 packets), sub1 (6) and sub2 (5); `tp_src=5002` rule towards sub2 (3) | Pass |
| 9 | Controller classifies traffic | Watch the Ryu terminal during tests 2 to 7 | `CLASS=HIGH` lines for port 5001 and `CLASS=NORMAL` lines for 5002 | Lines such as `CLASS=HIGH tcp_dst=5001 -> 00:00:00:00:00:01 packets=38 (+6)` and `CLASS=NORMAL tcp_dst=5002 -> 00:00:00:00:00:01 packets=10 (+6)` | Pass |

## Reading the flow-rule evidence (test 8)

MAC addresses are assigned by Mininet in host order: `:01` broker, `:02` pub1, `:03` pub2, `:04` sub1, `:05` sub2.

- Rules with `tp_dst=5001` or `5002` and `dl_dst=...:01` match traffic going **to the broker**.
- Rules with `tp_src=5001` or `5002` match the broker's **replies and deliveries** going back to a client.
- Priority 100 is the HIGH class (port 5001, topic `alerts`); priority 50 is NORMAL (port 5002, all other topics).
- The sports subscription (sub2) shows up on a priority-50 rule, while the alerts subscription (sub1) shows up on a priority-100 rule. The network treats the two topic classes differently.
- pub2 (`:03`) shows 0 packets because it sent nothing in this run.

## Known limitations (D1)

- The subscriber follows one topic per process.
- The publisher is interactive (reads stdin); there is no `--count` option yet.
- The topology has a single switch, so there is no alternate path.
