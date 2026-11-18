import socket, time

with socket.create_connection(("localhost", 6379)) as s:
    for i in range(0,2):
        s.sendall(b"*1\r\n$4\r\nPI")
        time.sleep(1)
        s.sendall(b"NG\r\n")
        s.settimeout(3)
        print(s.recv(1024))