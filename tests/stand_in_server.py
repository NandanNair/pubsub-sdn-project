#!/usr/bin/env python3
"""
Throwaway test server. NOT the real broker.

Listens on TCP 5001 and 5002 and echoes every line back with a prefix.
Use it to check that the topology and the Ryu 5001/5002 flow rules work
before the real broker exists.

Run on the broker host (inside Mininet):
    broker python3 tests/stand_in_server.py &
Test from a client host:
    pub1 python3 -c "import socket; s=socket.create_connection(('10.0.0.3',5001)); s.sendall(b'hello\\n'); print(s.recv(100)); s.close()"
"""

import socket
import threading

PORTS = (5001, 5002)


def handle(conn, addr, port):
    print("conn from %s on port %d" % (addr, port), flush=True)
    buf = b""
    try:
        while True:
            data = conn.recv(4096)
            if not data:
                break
            buf += data
            while b"\n" in buf:
                line, buf = buf.split(b"\n", 1)
                conn.sendall(b"ECHO[%d]: " % port + line + b"\n")
    except OSError:
        pass
    finally:
        conn.close()
        print("closed %s" % (addr,), flush=True)


def serve(port):
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("0.0.0.0", port))
    srv.listen(20)
    print("listening on %d" % port, flush=True)
    while True:
        conn, addr = srv.accept()
        threading.Thread(target=handle, args=(conn, addr, port),
                         daemon=True).start()


if __name__ == "__main__":
    for p in PORTS:
        threading.Thread(target=serve, args=(p,), daemon=True).start()
    threading.Event().wait()
