#!/usr/bin/env python3
# The tests' greeter (testdaemon.greeter) must be done before their
# interpreter finalizes. A nix-grpc-daemon terminated between sending its hello
# and reading the answer (nixDaemonAnswers) leaves the answer unread, so the
# greeter's next recv fails with ECONNRESET, and the greeter's thread writes
# the traceback to stderr. If the test's main thread is then exiting, it
# finalizes while that thread holds stderr's lock. The thread stops when it
# next needs the GIL, still holding the lock. At finalization, flushing stderr
# waits 1 s for the lock and then aborts with "Fatal Python error:
# _enter_buffered_busy", after the test has printed PASS.
#
# The window for that is the time the thread takes to write a traceback. This
# test makes it as long as it needs: the greeter's stderr is a full pipe, so
# the write blocks until this test drains it. The child exits while the write
# is blocked, and the drain starts after the child's exit has waited longer
# than finalization's 1 s for the lock.
#
# usage: testdaemon-test.py
# exit 0: pass, 1: fail, 2: the greeter never saw the reset, 77: skipped.
import os
import subprocess
import sys
import time

CHILD = r"""
import os, select, socket, sys, tempfile, time
import testdaemon

path = os.path.join(tempfile.mkdtemp(), "nix-daemon")
testdaemon.greeter(path)
s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
s.connect(path)
s.sendall(bytes(8))  # a hello
select.select([s], [], [])  # its answer is here
s.close()  # unread: the greeter's next recv gets ECONNRESET
time.sleep(1)  # its traceback's write is blocked on the full stderr by now
print("exiting", flush=True)
"""


def main():
    # On Linux, closing a Unix socket with unread data resets its peer
    # (unix_release_sock); elsewhere the greeter may see a plain EOF.
    if not sys.platform.startswith("linux"):
        return 77
    r, w = os.pipe()
    os.set_blocking(w, False)
    filled = 0
    for chunk in (4096, 1):
        try:
            while True:
                filled += os.write(w, b"x" * chunk)
        except BlockingIOError:
            pass
    os.set_blocking(w, True)
    env = dict(os.environ, PYTHONPATH=os.path.dirname(os.path.abspath(__file__)))
    child = subprocess.Popen([sys.executable, "-c", CHILD], stdout=subprocess.PIPE, stderr=w, env=env, text=True)
    os.close(w)
    line = child.stdout.readline()
    # Longer than finalization waits for stderr's lock.
    time.sleep(2)
    with os.fdopen(r, "rb") as f:
        err = f.read()[filled:].decode(errors="replace")
    code = child.wait()
    print(f"child: {line.strip()!r}, exit {code}\n--- child stderr ---\n{err}--- end ---")
    if "Fatal Python error" in err or code != 0:
        print(f"FAIL: the child aborted at exit (exit {code}): the greeter's thread outlived the interpreter")
        return 1
    if "ConnectionResetError" not in err:
        print("SETUP FAIL: the greeter wrote no ConnectionResetError: the reset never reached it")
        return 2
    print("PASS: the greeter's traceback was written in full and the child exited 0")
    return 0


if __name__ == "__main__":
    sys.exit(main())
