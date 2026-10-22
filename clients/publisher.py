import socket, json, sys, time, threading

host = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1"
topic = sys.argv[2] if len(sys.argv) > 2 else "news"
port = int(sys.argv[3]) if len(sys.argv) > 3 else (5001 if topic == "alerts" else 5002)

s = socket.create_connection((host, port))

def reader():
    for line in s.makefile("r"):
        print("<-", line.strip(), flush=True)

threading.Thread(target=reader, daemon=True).start()
print(f"Connected to {host}:{port}. Type messages for '{topic}', Ctrl+C to quit.", flush=True)
try:
    for line in sys.stdin:
        line = line.strip()
        if line:
            msg = {"type": "PUBLISH", "topic": topic, "payload": line, "ts": time.time()}
            s.sendall((json.dumps(msg) + "\n").encode())
except KeyboardInterrupt:
    pass
s.close()
