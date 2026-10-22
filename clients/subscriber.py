import socket, json, sys, time

host = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1"
topic = sys.argv[2] if len(sys.argv) > 2 else "news"
port = int(sys.argv[3]) if len(sys.argv) > 3 else (5001 if topic == "alerts" else 5002)

s = socket.create_connection((host, port))
s.sendall((json.dumps({"type": "SUBSCRIBE", "topic": topic}) + "\n").encode())
print(f"Connected to {host}:{port}, subscribing to '{topic}'...", flush=True)
try:
    for line in s.makefile("r"):
        try:
            msg = json.loads(line)
        except ValueError:
            continue
        if msg.get("type") == "DELIVER":
            lat = (time.time() - msg.get("ts", time.time())) * 1000
            print(f"[{msg['topic']}] {msg['payload']}  ({lat:.1f} ms)", flush=True)
        else:
            print("<-", line.strip(), flush=True)
except KeyboardInterrupt:
    pass
s.close()
