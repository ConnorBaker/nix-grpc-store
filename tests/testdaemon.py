"""The keepalive tests' nix-grpc-daemon, on ports it binds itself: a port a
test picked and let go of could be another test's daemon's by then."""
import os
import re
import socket
import struct
import subprocess
import threading
import time


def greeter(path):
    """A nix-daemon that only says hello: the builder takes no builds while
    its local nix-daemon does not answer (Coordinator::askLocalDaemon)."""
    srv = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    srv.bind(path)
    srv.listen()

    def serve():
        while True:
            conn, _ = srv.accept()
            with conn:
                if conn.recv(8):
                    conn.sendall(struct.pack("<Q", 0x6478696F))
                while conn.recv(4096):
                    pass

    threading.Thread(target=serve, daemon=True).start()


def start(daemon, tmp, *args):
    """The daemon on port 0 for gRPC and metrics, its nix-daemon a greeter,
    and its log."""
    log = open(os.path.join(tmp, "daemon.log"), "w+")
    greeter(os.path.join(tmp, "nix-daemon"))
    argv = [daemon, "--listen", "127.0.0.1:0", "--metrics-listen", "127.0.0.1:0", "--proxy-socket", os.path.join(tmp, "nix-daemon")]
    return subprocess.Popen([*argv, *args], stdout=log, stderr=subprocess.STDOUT), log


def bound(proc, log, deadline):
    """Its gRPC and metrics ports, from its startup line."""
    while proc.poll() is None and time.monotonic() < deadline:
        with open(log.name) as f:
            m = re.search(r" port=(\d+) metrics_port=(\d+)", f.read())
        if m:
            return int(m[1]), int(m[2])
        time.sleep(0.1)
    return None
