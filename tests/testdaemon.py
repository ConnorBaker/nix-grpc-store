"""The keepalive tests' nix-grpc-daemon, on ports it binds itself: a port a
test picked and let go of could be another test's daemon's by then."""
import atexit
import os
import re
import socketserver
import struct
import subprocess
import threading
import time


class Hello(socketserver.BaseRequestHandler):
    def handle(self):
        if self.request.recv(8):
            self.request.sendall(struct.pack("<Q", 0x6478696F))
        while self.request.recv(4096):
            pass


def greeter(path):
    """A nix-daemon that only says hello: the builder takes no builds while
    its local nix-daemon does not answer (Coordinator::askLocalDaemon).

    A connection's error is printed, and the greeter goes on serving. It stops
    and is joined at exit, before the interpreter finalizes: a daemon thread
    still writing then holds stderr's lock, and finalization aborts the test
    (testdaemon-test.py)."""
    srv = socketserver.UnixStreamServer(path, Hello)
    thread = threading.Thread(target=srv.serve_forever, daemon=True)
    thread.start()

    # Exit functions run before finalization, and after the interpreter joins
    # the non-daemon threads, which is why this thread is a daemon one.
    @atexit.register
    def stop():
        srv.shutdown()
        thread.join()
        srv.server_close()


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
