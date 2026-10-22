import socket, threading, json, time

HOST = "0.0.0.0"
PORTS = (5001, 5002)

topics = {}
lock = threading.Lock()
send_locks = {}

def port_for(topic):
    return 5001 if topic == "alerts" else 5002

def send(conn, msg):
    l = send_locks.get(conn)
    if l is None:
        raise OSError("closed")
    with l:
        conn.sendall((json.dumps(msg) + "\n").encode())

def safe_send(conn, msg):
    try:
        send(conn, msg)
        return True
    except OSError:
        return False

def error(conn, text):
    safe_send(conn, {"type": "ERROR", "msg": text})

def process(conn, port, msg):
    t = msg.get("type")
    if t not in ("SUBSCRIBE", "UNSUBSCRIBE", "PUBLISH"):
        return error(conn, "unknown type")
    topic = msg.get("topic")
    if not isinstance(topic, str) or not topic:
        return error(conn, "missing topic")
    if t == "PUBLISH" and "payload" not in msg:
        return error(conn, "missing payload")
    if port_for(topic) != port:
        return error(conn, f"wrong port for topic {topic}, use {port_for(topic)}")

    if t == "SUBSCRIBE":
        with lock:
            topics.setdefault(topic, set()).add(conn)
        safe_send(conn, {"type": "ACK", "ref": "SUBSCRIBE", "topic": topic})
    elif t == "UNSUBSCRIBE":
        with lock:
            topics.get(topic, set()).discard(conn)
        safe_send(conn, {"type": "ACK", "ref": "UNSUBSCRIBE", "topic": topic})
    else:
        with lock:
            targets = list(topics.get(topic, set()))
        out = {"type": "DELIVER", "topic": topic, "payload": msg["payload"],
               "ts": msg.get("ts", time.time())}
        delivered = sum(1 for s in targets if safe_send(s, out))
        safe_send(conn, {"type": "ACK", "ref": "PUBLISH", "topic": topic,
                         "delivered": delivered})

def handle(conn, port):
    send_locks[conn] = threading.Lock()
    f = conn.makefile("rb")
    try:
        for raw in f:
            try:
                msg = json.loads(raw.decode("utf-8"))
                if not isinstance(msg, dict):
                    raise ValueError
            except ValueError:
                error(conn, "malformed JSON")
                continue
            process(conn, port, msg)
    except OSError:
        pass
    finally:
        with lock:
            for s in topics.values():
                s.discard(conn)
        send_locks.pop(conn, None)
        conn.close()

def serve(port):
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind((HOST, port))
    srv.listen()
    print(f"Broker listening on {port}", flush=True)
    while True:
        conn, addr = srv.accept()
        threading.Thread(target=handle, args=(conn, port), daemon=True).start()

def main():
    for p in PORTS[1:]:
        threading.Thread(target=serve, args=(p,), daemon=True).start()
    serve(PORTS[0])

if __name__ == "__main__":
    main()
