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

# Readings of a daemon asleep, every thread of it, with nothing new in its
# log, after which it is taken never to log its startup line.
IDLE_READINGS = 100


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


def asleep(pid):
    """Every thread of `pid` asleep, state S in /proc (none, once it is gone).
    A task in a frozen cgroup reads S as well: the farm freezes a build with
    the cgroup v2 freezer, so bound counts a frozen daemon as asleep, and
    relies on the test counting being frozen with it (the same build). Without
    /proc (not Linux) this is never true, and bound waits for the line or the
    daemon's exit only."""
    try:
        names = os.listdir(f"/proc/{pid}/task")
    except OSError:
        return False
    states = []
    for name in names:
        try:
            with open(f"/proc/{pid}/task/{name}/stat") as f:
                line = f.readline()
        except OSError:
            continue
        states.append(line[line.rfind(")") + 2])
    return bool(states) and all(s == "S" for s in states)


def bound(proc, log):
    """Its gRPC and metrics ports, from its startup line. None once it exits,
    or once every thread of it is found asleep with its log unchanged
    IDLE_READINGS readings running: a daemon that will not log the line. A
    daemon hung otherwise (running, in D or T, or logging other lines) is
    still waited for."""
    idle = 0
    seen = -1
    while proc.poll() is None and idle < IDLE_READINGS:
        with open(log.name) as f:
            text = f.read()
        m = re.search(r" port=(\d+) metrics_port=(\d+)", text)
        if m:
            return int(m[1]), int(m[2])
        idle = idle + 1 if len(text) == seen and asleep(proc.pid) else 0
        seen = len(text)
        time.sleep(0.1)
    return None
