import socket
import json
import sys
import time

def run_publisher(host, port, topic):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((host, port))
    buf = b""

    for line in sys.stdin:
        line_str = line.strip()
        if not line_str:
            continue
        
        pub_msg = json.dumps({
            "type": "PUBLISH",
            "topic": topic,
            "payload": line_str,
            "ts": time.time()
        }) + "\n"
        
        s.sendall(pub_msg.encode('utf-8'))

        while b"\n" not in buf:
            chunk = s.recv(4096)
            if not chunk:
                break
            buf += chunk
        
        if b"\n" in buf:
            resp, buf = buf.split(b"\n", 1)

    s.close()

if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("Usage: python3 publisher.py <host> <port> <topic>")
        sys.exit(1)
    run_publisher(sys.argv[1], int(sys.argv[2]), sys.argv[3])
