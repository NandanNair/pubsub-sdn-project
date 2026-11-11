import socket
import select
import json
import sys
import time
import urllib.request
import threading
import queue

HOST = '0.0.0.0'
PORT = 5001
EVENT_LOG = 'results/events.jsonl'

subscriptions = {}  # topic -> list of client sockets
subscriber_queues = {}  # conn -> queue.Queue
controller_url = None

def log_event(event_type, topic, client_ip, delivered=0):
    try:
        entry = {
            "ts": time.time(),
            "event": event_type,
            "topic": topic or "",
            "client_ip": client_ip or "",
            "delivered": delivered
        }
        with open(EVENT_LOG, "a") as f:
            f.write(json.dumps(entry) + "\n")
    except Exception:
        pass

def notify_controller(action, topic, client_ip):
    if not controller_url:
        return
    def _send():
        try:
            data = json.dumps({"action": action, "topic": topic, "client_ip": client_ip}).encode('utf-8')
            req = urllib.request.Request(controller_url, data=data, headers={'Content-Type': 'application/json'}, method='POST')
            urllib.request.urlopen(req, timeout=1)
        except Exception:
            pass
    threading.Thread(target=_send, daemon=True).start()

def client_sender(conn, q):
    while True:
        try:
            data = q.get()
            if data is None:
                break
            conn.sendall(data)
        except Exception:
            break

def handle_line(conn, addr, line):
    client_ip = addr[0] if addr else ""
    try:
        msg = json.loads(line.decode('utf-8'))
    except Exception as e:
        err = json.dumps({"type": "ERROR", "msg": f"Invalid JSON: {str(e)}"}) + "\n"
        if conn in subscriber_queues:
            subscriber_queues[conn].put(err.encode('utf-8'))
        else:
            try:
                conn.sendall(err.encode('utf-8'))
            except Exception:
                pass
        log_event("ERROR", "", client_ip)
        return

    # Support both "type" and "action" keys (case-insensitive uppercase)
    raw_type = msg.get("type") or msg.get("action") or ""
    msg_type = raw_type.upper()

    if msg_type in ["SUBSCRIBE", "subscribe"]:
        topic = msg.get("topic")
        if not topic:
            err = json.dumps({"type": "ERROR", "msg": "Missing topic"}) + "\n"
            subscriber_queues[conn].put(err.encode('utf-8'))
            return
        if topic not in subscriptions:
            subscriptions[topic] = []
        if conn not in subscriptions[topic]:
            subscriptions[topic].append(conn)
        
        ack = json.dumps({"type": "ACK", "ref": "SUBSCRIBE", "topic": topic}) + "\n"
        subscriber_queues[conn].put(ack.encode('utf-8'))
        log_event("SUBSCRIBE", topic, client_ip)
        notify_controller("subscribe", topic, client_ip)

    elif msg_type in ["UNSUBSCRIBE", "unsubscribe"]:
        topic = msg.get("topic")
        if topic and topic in subscriptions and conn in subscriptions[topic]:
            subscriptions[topic].remove(conn)
        ack = json.dumps({"type": "ACK", "ref": "UNSUBSCRIBE", "topic": topic or ""}) + "\n"
        subscriber_queues[conn].put(ack.encode('utf-8'))
        log_event("UNSUBSCRIBE", topic or "", client_ip)
        notify_controller("unsubscribe", topic or "", client_ip)

    elif msg_type in ["PUBLISH", "publish"]:
        topic = msg.get("topic")
        payload = msg.get("payload") or msg.get("message")
        ts = msg.get("ts", time.time())

        if not topic or payload is None:
            err = json.dumps({"type": "ERROR", "msg": "Missing topic or payload"}) + "\n"
            subscriber_queues[conn].put(err.encode('utf-8'))
            return

        target_clients = subscriptions.get(topic, [])
        delivered_count = 0
        dead_clients = []

        deliver_msg = json.dumps({
            "type": "DELIVER",
            "topic": topic,
            "payload": payload,
            "ts": ts
        }) + "\n"

        for sub_conn in target_clients:
            try:
                if sub_conn in subscriber_queues:
                    subscriber_queues[sub_conn].put(deliver_msg.encode('utf-8'))
                else:
                    sub_conn.sendall(deliver_msg.encode('utf-8'))
                delivered_count += 1
            except Exception:
                dead_clients.append(sub_conn)

        for dead in dead_clients:
            if dead in subscriptions.get(topic, []):
                subscriptions[topic].remove(dead)
            if dead in subscriber_queues:
                del subscriber_queues[dead]

        ack = json.dumps({"type": "ACK", "ref": "PUBLISH", "topic": topic, "delivered": delivered_count}) + "\n"
        subscriber_queues[conn].put(ack.encode('utf-8'))
        log_event("PUBLISH", topic, client_ip, delivered_count)

    else:
        err = json.dumps({"type": "ERROR", "msg": f"Unknown message type: {msg_type}"}) + "\n"
        subscriber_queues[conn].put(err.encode('utf-8'))
        log_event("ERROR", "", client_ip)

def run_broker(port=PORT):
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((HOST, port))
    server.listen(128)
    print(f"Broker listening on {HOST}:{port}")

    buffers = {}
    inputs = [server]

    while True:
        readable, _, _ = select.select(inputs, [], [])
        for s in readable:
            if s is server:
                conn, addr = server.accept()
                inputs.append(conn)
                buffers[conn] = b""
                q = queue.Queue()
                subscriber_queues[conn] = q
                threading.Thread(target=client_sender, args=(conn, q), daemon=True).start()
                client_ip = addr[0] if addr else ""
                log_event("CONNECT", "", client_ip)
            else:
                try:
                    data = s.recv(4096)
                    if not data:
                        inputs.remove(s)
                        if s in buffers:
                            del buffers[s]
                        if s in subscriber_queues:
                            subscriber_queues[s].put(None)
                            del subscriber_queues[s]
                        for t in subscriptions:
                            if s in subscriptions[t]:
                                subscriptions[t].remove(s)
                        try:
                            peer = s.getpeername()
                            log_event("DISCONNECT", "", peer[0] if peer else "")
                        except Exception:
                            pass
                        s.close()
                    else:
                        buffers[s] += data
                        while b"\n" in buffers[s]:
                            line, buffers[s] = buffers[s].split(b"\n", 1)
                            if line.strip():
                                try:
                                    peer = s.getpeername()
                                except Exception:
                                    peer = None
                                handle_line(s, peer, line)
                except Exception:
                    inputs.remove(s)
                    if s in buffers:
                        del buffers[s]
                    if s in subscriber_queues:
                        subscriber_queues[s].put(None)
                        del subscriber_queues[s]
                    s.close()

if __name__ == "__main__":
    args = sys.argv[1:]
    port = PORT
    for i, arg in enumerate(args):
        if arg.startswith("--controller="):
            controller_url = arg.split("=", 1)[1]
        elif not arg.startswith("--"):
            try:
                port = int(arg)
            except ValueError:
                pass
    run_broker(port)
