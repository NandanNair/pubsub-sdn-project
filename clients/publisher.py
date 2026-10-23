import socket
import sys
import json

def main():
    if len(sys.argv) < 3:
        print("Usage: python3 publisher.py <broker_ip> <topic>", flush=True)
        sys.exit(1)

    broker_ip = sys.argv[1]
    topic = sys.argv[2]
    port = 5001

    # Read message from stdin
    message_content = sys.stdin.read().strip()
    if not message_content:
        sys.exit(0)

    payload = {
        "type": "PUBLISH",
        "topic": topic,
        "content": message_content
    }

    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(5.0)
        s.connect((broker_ip, port))
        s.sendall(json.dumps(payload).encode('utf-8'))
        
        response = s.recv(1024)
        if response:
            print(f"Server response: {response.decode('utf-8')}", flush=True)
    except Exception as e:
        print(f"Error connecting to broker at {broker_ip}:{port} - {e}", flush=True)
        sys.exit(1)
    finally:
        s.close()

if __name__ == "__main__":
    main()
