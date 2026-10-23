import socket
import threading
import json
from datetime import datetime

# Helper function for timestamped logging
def log_event(message):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {message}", flush=True)

HOST = '0.0.0.0'
PORTS = [5001, 5002]

# Structure: { topic: [socket1, socket2, ...] }
subscriptions = {}
sub_lock = threading.Lock()

def handle_client(conn, addr, port):
    log_event(f"Connected: {addr}")
    try:
        while True:
            data = conn.recv(1024)
            if not data:
                break
            
            try:
                msg = json.loads(data.decode('utf-8'))
            except Exception:
                err_resp = json.dumps({"type": "ERROR", "reason": "Malformed JSON"})
                conn.sendall(err_resp.encode('utf-8'))
                continue

            msg_type = msg.get("type")
            topic = msg.get("topic", "")
            client_id = msg.get("client_id", str(addr))

            if msg_type == "SUBSCRIBE":
                with sub_lock:
                    if topic not in subscriptions:
                        subscriptions[topic] = []
                    if conn not in subscriptions[topic]:
                        subscriptions[topic].append(conn)
                log_event(f"Subscribed: {client_id} to topic '{topic}'")
                conn.sendall(json.dumps({"type": "ACK", "status": "Subscribed"}).encode('utf-8'))

            elif msg_type == "UNSUBSCRIBE":
                with sub_lock:
                    if topic in subscriptions and conn in subscriptions[topic]:
                        subscriptions[topic].remove(conn)
                log_event(f"Unsubscribed: {client_id} from topic '{topic}'")
                conn.sendall(json.dumps({"type": "ACK", "status": "Unsubscribed"}).encode('utf-8'))

            elif msg_type == "PUBLISH":
                delivered_count = 0
                payload = json.dumps(msg).encode('utf-8')
                with sub_lock:
                    subscribers = subscriptions.get(topic, [])
                    for sub_conn in list(subscribers):
                        try:
                            sub_conn.sendall(payload)
                            delivered_count += 1
                        except Exception:
                            subscribers.remove(sub_conn)
                log_event(f"Published: {topic} from {client_id}, delivered to {delivered_count} subscribers")
                ack = json.dumps({"type": "ACK", "delivered": delivered_count})
                conn.sendall(ack.encode('utf-8'))

            else:
                conn.sendall(json.dumps({"type": "ERROR", "reason": "Unknown command"}).encode('utf-8'))

    except Exception:
        pass
    finally:
        with sub_lock:
            for t, subs in subscriptions.items():
                if conn in subs:
                    subs.remove(conn)
        log_event(f"Disconnected: {addr}")
        conn.close()

def serve(port):
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind((HOST, port))
    srv.listen()
    print(f"Broker listening on port {port}", flush=True)
    while True:
        conn, addr = srv.accept()
        threading.Thread(target=handle_client, args=(conn, addr, port), daemon=True).start()

def main():
    for p in PORTS[1:]:
        threading.Thread(target=serve, args=(p,), daemon=True).start()
    serve(PORTS[0])

if __name__ == "__main__":
    main()
