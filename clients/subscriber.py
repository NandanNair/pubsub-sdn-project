import socket
import sys
import json

def main():
    if len(sys.argv) < 3:
        print("Usage: python3 subscriber.py <broker_ip> <topic>", flush=True)
        sys.exit(1)

    broker_ip = sys.argv[1]
    topic = sys.argv[2]
    port = 5001

    sub_request = {
        "type": "SUBSCRIBE",
        "topic": topic
    }

    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((broker_ip, port))
        s.sendall(json.dumps(sub_request).encode('utf-8'))
    except Exception as e:
        print(f"Error connecting to broker at {broker_ip}:{port} - {e}", flush=True)
        sys.exit(1)

    try:
        while True:
            data = s.recv(1024)
            if not data:
                print("Connection closed by broker.", flush=True)
                break
            print(f"Received: {data.decode('utf-8')}", flush=True)
    except KeyboardInterrupt:
        pass
    except Exception as e:
        print(f"Error receiving data: {e}", flush=True)
    finally:
        s.close()

if __name__ == "__main__":
    main()
