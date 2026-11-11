import socket
import json
import argparse
import time
import sys

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=5001)
    parser.add_argument("--rate", type=int, default=50)
    parser.add_argument("--duration", type=int, default=5)
    parser.add_argument("--topic", default="alerts")
    args = parser.parse_args()

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.connect((args.host, args.port))
    except Exception as e:
        print(f"Failed to connect: {e}")
        sys.exit(1)

    interval = 1.0 / args.rate
    total_msgs = args.rate * args.duration

    buf = b""
    for i in range(total_msgs):
        start_time = time.time()
        msg = json.dumps({
            "type": "PUBLISH",
            "topic": args.topic,
            "payload": f"msg-{i}",
            "ts": time.time()
        }) + "\n"
        
        try:
            s.sendall(msg.encode('utf-8'))
        except Exception as e:
            print(f"Send failed: {e}")
            break

        elapsed = time.time() - start_time
        if elapsed < interval:
            time.sleep(interval - elapsed)

    s.close()
    print(f"Loadgen finished: sent {total_msgs} messages.")

if __name__ == "__main__":
    main()
