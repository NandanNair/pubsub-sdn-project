import socket
import json
import sys
import time

def run_subscriber(host, topic, port):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((host, port))

    sub_msg = json.dumps({
        "type": "SUBSCRIBE",
        "topic": topic
    }) + "\n"
    s.sendall(sub_msg.encode('utf-8'))

    buf = b""
    while True:
        data = s.recv(4096)
        if not data:
            break
        buf += data
        while b"\n" in buf:
            line, buf = buf.split(b"\n", 1)
            if line.strip():
                msg = json.loads(line.decode('utf-8'))
                if msg.get("type") == "DELIVER":
                    ts = msg.get('ts', time.time())
                    latency_ms = (time.time() - ts) * 1000
                    print(f"[{msg['topic']}] {msg['payload']} (latency: {latency_ms:.2f} ms)")
                elif msg.get("type") == "ACK":
                    print(f"Subscribed to {msg.get('topic')}")

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python3 subscriber.py <broker_ip> <topic> [port]")
        sys.exit(1)
    host = sys.argv[1]
    topic = sys.argv[2]
    if len(sys.argv) >= 4:
        port = int(sys.argv[3])
    else:
        port = 5001 if topic == "alerts" else 5002
    run_subscriber(host, topic, port)
