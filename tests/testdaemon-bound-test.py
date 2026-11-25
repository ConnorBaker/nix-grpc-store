#!/usr/bin/env python3
# testdaemon.bound waits for a daemon's startup line until the line, the
# daemon's exit, or the daemon found asleep with its log unchanged
# testdaemon.IDLE_READINGS readings running. A daemon that stays alive but
# never logs the line must end the wait rather than hold the test (and its
# build) up for good. A daemon slow to log it must not: one busy all along,
# or one asleep but logging other lines.
# The busy one is busy in a second thread while its first sleeps in join():
# every thread of it must be asleep, not any one, nor its first alone.
#
# Each fake daemon here gets its own bound, in a thread. The busy and the
# logging one log the startup line once this test has read 2 *
# IDLE_READINGS times, a tenth of a second apart, as bound does; one still
# waiting at 3 * IDLE_READINGS readings fails the test.
#
# usage: testdaemon-bound-test.py
# exit 0: pass, 1: fail, 77: skipped (no /proc).
import os
import subprocess
import sys
import tempfile
import threading
import time

import testdaemon

LINE = "event=startup port=1 metrics_port=2"
FAKES = {
    "silent": "import time\ntime.sleep(3600)",
    "busy": f"import os, sys, threading, time\ndef spin():\n    while not os.path.exists(sys.argv[1]): pass\nt = threading.Thread(target=spin)\nt.start()\nt.join()\nprint({LINE!r}, flush=True)\ntime.sleep(3600)",
    "logging": f"import os, sys, time\nwhile not os.path.exists(sys.argv[1]):\n    print('starting', flush=True)\n    time.sleep(0.5)\nprint({LINE!r}, flush=True)\ntime.sleep(3600)",
}
WANT = {"silent": None, "busy": (1, 2), "logging": (1, 2)}


def main():
    if not os.path.isdir("/proc/self/task"):
        return 77
    tmp = tempfile.mkdtemp()
    go = os.path.join(tmp, "go")
    procs, got, threads = {}, {}, {}
    try:
        for name, code in FAKES.items():
            log = open(os.path.join(tmp, f"{name}.log"), "w+")
            procs[name] = proc = subprocess.Popen([sys.executable, "-c", code, go], stdout=log, stderr=subprocess.STDOUT)
            threads[name] = threading.Thread(target=lambda n=name, p=proc, l=log: got.__setitem__(n, testdaemon.bound(p, l)), daemon=True)
            threads[name].start()
        for reading in range(3 * testdaemon.IDLE_READINGS):
            if not any(t.is_alive() for t in threads.values()):
                break
            if reading == 2 * testdaemon.IDLE_READINGS:
                open(go, "w").close()
            time.sleep(0.1)
    finally:
        for proc in procs.values():
            proc.kill()
            proc.wait()
    ok = True
    for name, want in WANT.items():
        if name not in got:
            print(f"FAIL: {name}: bound still waits for the startup line after {3 * testdaemon.IDLE_READINGS} readings")
            ok = False
        elif got[name] != want:
            print(f"FAIL: {name}: bound gave {got[name]}, not {want}")
            ok = False
        else:
            print(f"ok: {name}: bound gave {want}")
    print("PASS" if ok else "FAIL")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
