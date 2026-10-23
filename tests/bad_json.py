import socket
s = socket.create_connection(("10.0.0.3", 5001))
s.sendall(b"not json\n")
print(s.recv(200))
s.close()
