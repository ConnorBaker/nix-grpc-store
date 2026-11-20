"""Relays connections from LISTEN to the nix-daemon socket TARGET, and
appends every byte the clients send to OUT."""

import os
import socket
import sys
import threading

listen, target, out = sys.argv[1:4]
lock = threading.Lock()


def pump(src, dst, record):
    while data := src.recv(65536):
        if record:
            with lock, open(out, "ab") as f:
                f.write(data)
        dst.sendall(data)
    try:
        dst.shutdown(socket.SHUT_WR)
    except OSError:
        pass


if os.path.exists(listen):
    os.unlink(listen)
server = socket.socket(socket.AF_UNIX)
server.bind(listen)
server.listen(64)
while True:
    client, _ = server.accept()
    daemon = socket.socket(socket.AF_UNIX)
    daemon.connect(target)
    threading.Thread(target=pump, args=(client, daemon, True), daemon=True).start()
    threading.Thread(target=pump, args=(daemon, client, False), daemon=True).start()
