import socket
import select
import json
import sys
import time

HOST = '0.0.0.0'
PORT = 5001
EVENT_LOG = 'events.jsonl'

subscriptions = {}  # topic -> list of client sockets

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

def handle_line(conn, addr, line):
    client_ip = addr[0] if addr else ""
    try:
        msg = json.loads(line.decode('utf-8'))
    except Exception as e:
        err = json.dumps({"type": "ERROR", "msg": f"Invalid JSON: {str(e)}"}) + "\n"
        conn.sendall(err.encode('utf-8'))
        log_event("ERROR", "", client_ip)
        return

    msg_type = msg.get("type")

    if msg_type == "SUBSCRIBE":
        topic = msg.get("topic")
        if not topic:
            err = json.dumps({"type": "ERROR", "msg": "Missing topic"}) + "\n"
            conn.sendall(err.encode('utf-8'))
            return
        if topic not in subscriptions:
            subscriptions[topic] = []
        if conn not in subscriptions[topic]:
            subscriptions[topic].append(conn)
        
        ack = json.dumps({"type": "ACK", "ref": "SUBSCRIBE", "topic": topic}) + "\n"
        conn.sendall(ack.encode('utf-8'))
        log_event("SUBSCRIBE", topic, client_ip)

    elif msg_type == "PUBLISH":
        topic = msg.get("topic")
        payload = msg.get("payload")
        ts = msg.get("ts", time.time())

        if not topic or payload is None:
            err = json.dumps({"type": "ERROR", "msg": "Missing topic or payload"}) + "\n"
            conn.sendall(err.encode('utf-8'))
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
                sub_conn.sendall(deliver_msg.encode('utf-8'))
                delivered_count += 1
            except Exception:
                dead_clients.append(sub_conn)

        for dead in dead_clients:
            if dead in subscriptions.get(topic, []):
                subscriptions[topic].remove(dead)

        ack = json.dumps({"type": "ACK", "ref": "PUBLISH", "topic": topic, "delivered": delivered_count}) + "\n"
        conn.sendall(ack.encode('utf-8'))
        log_event("PUBLISH", topic, client_ip, delivered_count)

    else:
        err = json.dumps({"type": "ERROR", "msg": f"Unknown message type: {msg_type}"}) + "\n"
        conn.sendall(err.encode('utf-8'))
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
                if port == 5002:
                    err = json.dumps({"type": "ERROR", "msg": "Use port 5001 for publishing alerts"}) + "\n"
                    try:
                        conn.sendall(err.encode('utf-8'))
                    except Exception:
                        pass
                    conn.close()
                    continue
                inputs.append(conn)
                buffers[conn] = b""
            else:
                try:
                    data = s.recv(4096)
                    if not data:
                        inputs.remove(s)
                        if s in buffers:
                            del buffers[s]
                        for t in subscriptions:
                            if s in subscriptions[t]:
                                subscriptions[t].remove(s)
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
                    s.close()

if __name__ == "__main__":
    p = int(sys.argv[1]) if len(sys.argv) > 1 else PORT
    run_broker(p)
