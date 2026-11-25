#!/usr/bin/env python3
# The daemon's stderr is a pipe its reader can hold up (journald under
# systemd). A call must not wait for it: FetchNars writes its access line
# (Rpc::done) before it returns. With the daemon's stderr a full pipe that
# nobody reads, a FetchNars call must be answered; a daemon thread other than
# its log writer found asleep writing to stderr first means the call waited
# for it (the log writer holds no lock while it writes, so a thread waiting
# for stderr sleeps in a write of its own). Then the daemon is told to stop, while its stderr
# is still full; once it no longer accepts connections and every thread of it
# is asleep, three readings running, the pipe is read, and every line it
# logged must be there, the call's and its exit's, and it must exit 0. That
# every way out writes what is queued first is logfmt-test's: here the daemon
# may still be in its shutdown grace when the pipe is read. Readings and
# steps, not a clock, bound every wait; the one for the startup line ends at
# the line, the daemon's exit, or its sleep with nothing logged,
# testdaemon.IDLE_READINGS readings of a tenth of a second running. The test holds the pipe's write
# end for the filler, so the daemon's exit is seen from its pidfd, not from
# the pipe's end.
#
# usage: stalled-stderr-test.py <nix-grpc-daemon>
# exit 0: pass, 1: fail, 2: the test never reached the state it tests.
import fcntl
import importlib.util
import os
import re
import select
import signal
import socket
import subprocess
import sys
import tempfile
import threading

import testdaemon

spec = importlib.util.spec_from_file_location("keepalive", os.path.join(os.path.dirname(__file__), "keepalive-test.py"))
ka = importlib.util.module_from_spec(spec)
spec.loader.exec_module(ka)

STEPS = 1_000_000
SYS_WRITE = {"x86_64": 1, "aarch64": 64}[os.uname().machine]


def first_line(path):
    try:
        with open(path) as f:
            return f.readline()
    except OSError:
        return ""


def state(task):
    line = first_line(f"{task}/stat")
    end = line.rfind(")")
    return line[end + 2] if end >= 0 and end + 2 < len(line) else "?"


def tasks(pid):
    try:
        return [f"/proc/{pid}/task/{t}" for t in os.listdir(f"/proc/{pid}/task")]
    except OSError:
        return []


def writes_to(task, fd):
    fields = first_line(f"{task}/syscall").split()
    return state(task) == "S" and len(fields) > 1 and fields[0] == str(SYS_WRITE) and int(fields[1], 16) == fd


def writes_to_stderr(task):
    return writes_to(task, 2)


def held_writer(pid):
    """A thread of the daemon's other than its log writer, asleep writing to stderr."""
    return any(first_line(f"{t}/comm").strip() != "log-writer" and writes_to_stderr(t) for t in tasks(pid))


def frame_now(conn):
    """The next frame already received, or None."""
    try:
        return conn.next_frame(0)
    except (BlockingIOError, EOFError):
        return None


def main():
    daemon = sys.argv[1]
    tmp = tempfile.mkdtemp()
    testdaemon.greeter(os.path.join(tmp, "nix-daemon"))
    r, w = os.pipe()
    argv = [daemon, "--listen", "127.0.0.1:0", "--metrics-listen", "127.0.0.1:0", "--proxy-socket", os.path.join(tmp, "nix-daemon"),
            "--proxy-store", f"local?root={os.path.join(tmp, 'root')}"]
    proc = subprocess.Popen(argv, stdout=w, stderr=w)
    gone = os.pidfd_open(proc.pid)
    out = b""
    reader = None
    try:
        # Read until the startup line, then nothing more.
        port = None
        idle = 0
        while port is None:
            ready, _, _ = select.select([r, gone], [], [], 0.1)
            if r not in ready:
                if gone in ready:
                    print(f"SETUP FAIL: the daemon exited ({proc.wait()}) before its startup line\n{out.decode(errors='replace')}")
                    return 2
                idle = idle + 1 if testdaemon.asleep(proc.pid) else 0
                if idle >= testdaemon.IDLE_READINGS:
                    print(f"SETUP FAIL: the daemon sleeps with no startup line\n{out.decode(errors='replace')}")
                    return 2
                continue
            idle = 0
            out += os.read(r, 1)
            m = re.search(rb"event=startup .* port=(\d+) ", out)
            port = int(m[1]) if m and out.endswith(b"\n") else None

        # Fill the pipe: a write of more than it holds cannot fit, so the
        # filler sleeps in it once it is full, until it is read.
        size = fcntl.fcntl(w, fcntl.F_GETPIPE_SZ) + 1
        filler_tid = []

        def fill():
            filler_tid.append(threading.get_native_id())
            os.write(w, b"\0" * size)

        threading.Thread(target=fill, daemon=True).start()
        filler = lambda: f"/proc/self/task/{filler_tid[0]}"  # noqa: E731
        if not any(filler_tid and writes_to(filler(), w) for _ in range(STEPS)):
            print("SETUP FAIL: the pipe never filled")
            return 2

        sock = socket.create_connection(("127.0.0.1", port))
        conn = ka.Conn(sock)
        sock.sendall(b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n" + ka.frame(ka.SETTINGS, 0, 0))
        block = b"".join(
            ka.literal(k, v)
            for k, v in [
                (":method", "POST"),
                (":scheme", "http"),
                (":path", "/nix.remote.NixRemote/FetchNars"),
                (":authority", f"127.0.0.1:{port}"),
                ("content-type", "application/grpc"),
                ("te", "trailers"),
            ]
        )
        sock.sendall(ka.frame(ka.HEADERS, ka.END_HEADERS, 1, block))
        sock.sendall(ka.frame(ka.DATA, ka.END_STREAM, 1, b"\x00\x00\x00\x00\x00"))  # an empty FetchNarsRequest

        # Held up: a daemon thread other than the log writer asleep writing to
        # stderr, with the call unanswered. Each reading takes the threads
        # before the frames: an answer sent in between is read then.
        answered = held = False
        status = None
        for _ in range(STEPS):
            held = held_writer(proc.pid)
            got = frame_now(conn)
            while got is not None and not answered:
                kind, flags, stream, payload = got
                if kind == ka.SETTINGS and not flags & ka.ACK:
                    sock.sendall(ka.frame(ka.SETTINGS, ka.ACK, 0))
                elif kind == ka.PING and not flags & ka.ACK:
                    sock.sendall(ka.frame(ka.PING, ka.ACK, 0, payload))
                elif stream == 1 and (kind == ka.RST_STREAM or (kind == ka.HEADERS and flags & ka.END_STREAM)):
                    answered = True
                    status = re.search(rb"grpc-status.(\d+)", payload)
                got = frame_now(conn) if not answered else None
            if answered or held:
                break
        sock.close()
        print(f"stalled-stderr: answered={answered} held={held} status={status[1].decode() if status else None}")

        # Stop, with the pipe still full.
        proc.send_signal(signal.SIGTERM)
        refused = False
        for _ in range(STEPS):
            try:
                socket.create_connection(("127.0.0.1", port)).close()
            except (ConnectionRefusedError, ConnectionResetError):
                # Refused, or reset as the listener closed under it.
                refused = True
                break
            if proc.poll() is not None:
                break
        waits = gone = False
        answers = 0
        for _ in range(STEPS):
            if not refused:
                break
            gone = proc.poll() is not None
            if gone:
                break
            answers = answers + 1 if all(state(t) == "S" for t in tasks(proc.pid)) else 0
            if answers == 3:
                waits = True
                break

        def drain():
            nonlocal out
            os.close(w)
            while chunk := os.read(r, 65536):
                out += chunk

        reader = threading.Thread(target=drain)
        reader.start()
        code = proc.wait()
        reader.join()
        reader = None
        text = out.replace(b"\0", b"").decode(errors="replace")
        print(f"stalled-stderr: refused={refused} waits={waits} gone={gone} exit={code}")
        fails = []
        if held or not answered:
            fails.append("a call waited for the daemon's stderr (Rpc::done)")
        if not re.search(r"event=rpc method=FetchNars ", text):
            fails.append("the call's access line is missing")
        if not re.search(r"event=signal_exit", text):
            fails.append("the exit's line is missing")
        if code != 0:
            fails.append(f"the daemon exited {code}")
        if fails:
            print("FAIL: " + "; ".join(fails) + f"\n--- daemon log ---\n{text}")
            return 1
        print("PASS")
        return 0
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait()
        if reader is None:
            try:
                os.close(w)
            except OSError:
                pass


if __name__ == "__main__":
    sys.exit(main())
