import socket
import json
import argparse
import time
import threading

def send_load(host, port, topic, count, client_id):
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((host, port))
        
        # Subscribe
        sub_msg = json.dumps({"action": "subscribe", "topic": topic}) + "\n"
        s.sendall(sub_msg.encode('utf-8'))
        s.recv(1024)
        
        # Publish messages
        for i in range(count):
            pub_msg = json.dumps({"action": "publish", "topic": topic, "payload": f"msg-{client_id}-{i}"}) + "\n"
            s.sendall(pub_msg.encode('utf-8'))
            s.recv(1024)
            
        s.close()
    except Exception as e:
        print(f"[Loadgen Error] Client {client_id}: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--host', default='127.0.0.1', help='Broker IP')
    parser.add_argument('--port', type=int, default=5001, help='Broker Port')
    parser.add_argument('--topic', default='sports', help='Topic to test')
    parser.add_argument('--clients', type=int, default=5, help='Number of concurrent clients')
    parser.add_argument('--count', type=int, default=10, help='Messages per client')
    args = parser.parse_args()
    
    threads = []
    start_time = time.time()
    
    for i in range(args.clients):
        t = threading.Thread(target=send_load, args=(args.host, args.port, args.topic, args.count, i))
        threads.append(t)
        t.start()
        
    for t in threads:
        t.join()
        
    duration = time.time() - start_time
    total_msgs = args.clients * args.count
    print(f"Load generation complete: {total_msgs} messages sent across {args.clients} clients in {duration:.2f} seconds.")
