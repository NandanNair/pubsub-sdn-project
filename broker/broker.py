import socket
import threading
import json
import argparse
import urllib.request
import urllib.error
import os
import queue
from datetime import datetime

def log_event(message, event_type="INFO", details=None):
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"[{timestamp}] {message}", flush=True)
    try:
        os.makedirs("results", exist_ok=True)
        log_entry = {
            "timestamp": timestamp,
            "type": event_type,
            "message": message
        }
        if details:
            log_entry.update(details)
        with open("results/events.jsonl", "a") as f:
            f.write(json.dumps(log_entry) + "\n")
    except Exception as e:
        print(f"[Log Error]: {e}", flush=True)

def notify_controller(controller_url, action, topic, client_ip):
    if not controller_url:
        return
    def _send():
        try:
            url = f"http://{controller_url}/subscription"
            payload = {"action": action, "topic": topic, "ip": client_ip}
            data = json.dumps(payload).encode('utf-8')
            req = urllib.request.Request(url, data=data, headers={'Content-Type': 'application/json'})
            with urllib.request.urlopen(req, timeout=1.0):
                pass
        except Exception as e:
            print(f"[SDN Notification Failed]: {e}", flush=True)
            
    threading.Thread(target=_send, daemon=True).start()

# Task 3: Subscriber session mapping: conn -> queue.Queue()
subscriber_queues = {}
subscriptions = {}
sub_lock = threading.Lock()

def client_sender_worker(conn, q):
    """Asynchronous worker thread to flush send queue per client."""
    try:
        while True:
            msg_bytes = q.get()
            if msg_bytes is None:
                break
            conn.sendall(msg_bytes)
            q.task_done()
    except Exception:
        pass

def handle_client(conn, addr, port, controller_url):
    log_event(f"Connected: {addr} on port {port}", "CONNECT", {"addr": str(addr), "port": port})
    client_ip = addr[0]
    
    # Initialize async send queue for this client
    send_q = queue.Queue()
    with sub_lock:
        subscriber_queues[conn] = send_q
        
    sender_thread = threading.Thread(target=client_sender_worker, args=(conn, send_q), daemon=True)
    sender_thread.start()
    
    try:
        while True:
            data = conn.recv(1024)
            if not data:
                break
            
            message_str = data.decode('utf-8').strip()
            if not message_str:
                continue
                
            try:
                msg = json.loads(message_str)
                action = msg.get("action")
                topic = msg.get("topic")
                
                if action == "subscribe" and topic:
                    with sub_lock:
                        if topic not in subscriptions:
                            subscriptions[topic] = []
                        if conn not in subscriptions[topic]:
                            subscriptions[topic].append(conn)
                    log_event(f"Client {client_ip} SUBSCRIBED to topic '{topic}'", "SUBSCRIBE", {"ip": client_ip, "topic": topic})
                    notify_controller(controller_url, "subscribe", topic, client_ip)
                    send_q.put(json.dumps({"status": "success", "message": f"Subscribed to {topic}"}).encode('utf-8') + b'\n')
                    
                elif action == "unsubscribe" and topic:
                    with sub_lock:
                        if topic in subscriptions and conn in subscriptions[topic]:
                            subscriptions[topic].remove(conn)
                    log_event(f"Client {client_ip} UNSUBSCRIBED from topic '{topic}'", "UNSUBSCRIBE", {"ip": client_ip, "topic": topic})
                    notify_controller(controller_url, "unsubscribe", topic, client_ip)
                    send_q.put(json.dumps({"status": "success", "message": f"Unsubscribed from {topic}"}).encode('utf-8') + b'\n')
                    
                elif action == "publish" and topic:
                    payload = msg.get("payload")
                    log_event(f"Publishing to topic '{topic}' from {client_ip}", "PUBLISH", {"ip": client_ip, "topic": topic})
                    pub_data = json.dumps({"topic": topic, "payload": payload}).encode('utf-8') + b'\n'
                    with sub_lock:
                        if topic in subscriptions:
                            for sub_conn in subscriptions[topic]:
                                if sub_conn in subscriber_queues:
                                    subscriber_queues[sub_conn].put(pub_data)
                    send_q.put(json.dumps({"status": "success", "message": "Published"}).encode('utf-8') + b'\n')
                    
            except json.JSONDecodeError:
                log_event(f"Invalid JSON received from {client_ip}", "ERROR", {"ip": client_ip})
    except Exception as e:
        log_event(f"Error with {addr}: {e}", "ERROR", {"addr": str(addr)})
    finally:
        with sub_lock:
            if conn in subscriber_queues:
                subscriber_queues[conn].put(None) # Signal sender thread to exit
                del subscriber_queues[conn]
            for top in list(subscriptions.keys()):
                if conn in subscriptions[top]:
                    subscriptions[top].remove(conn)
        conn.close()
        log_event(f"Disconnected: {addr}", "DISCONNECT", {"addr": str(addr)})

def start_server(port, controller_url):
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(('0.0.0.0', port))
    server.listen(5)
    log_event(f"Broker listening on port {port}...", "STARTUP", {"port": port})
    
    while True:
        conn, addr = server.accept()
        threading.Thread(target=handle_client, args=(conn, addr, port, controller_url), daemon=True).start()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--controller', default='10.0.0.254:8080', help='SDN Controller URL')
    args = parser.parse_args()
    
    ports = [5001, 5002]
    for p in ports:
        threading.Thread(target=start_server, args=(p, args.controller), daemon=True).start()
        
    try:
        while True:
            threading.Event().wait(1)
    except KeyboardInterrupt:
        log_event("Broker shutting down.", "SHUTDOWN")
