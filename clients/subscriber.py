import socket
import json
import sys

def run_subscriber(host, port, topic):
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
                    print(f"[{msg['topic']}] {msg['payload']} (ts: {msg.get('ts')})")
                elif msg.get("type") == "ACK":
                    print(f"Subscribed to {msg.get('topic')}")

if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("Usage: python3 subscriber.py <host> <port> <topic>")
        sys.exit(1)
    run_subscriber(sys.argv[1], int(sys.argv[2]), sys.argv[3])
