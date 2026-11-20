#!/usr/bin/env python3
# A placement nobody claims, whose outputs the node's store has, all of them,
# is settled from that store: the builder's own thread asks it
# (Coordinator's builtHereThread, LocalStore::builtHere) and finishes the
# placement as built, which frees its slot. After a takeover such placements
# are the builds a client runs through the predecessor and never claims here.
#
# The daemon's store is a real one (--proxy-store local?root=...) that holds a
# drv and its output (local-store-test --built-drv); its nix-daemon is a
# greeter, so the builder takes builds. A client sends one Want for the drv,
# reads the Assigned, and then stays connected and quiet: it never calls the
# builder. It passes only if the daemon counts expect_built_here and holds
# nothing while that client is still connected, so the placement ended by the
# store check, not by the client leaving.
#
# usage: built-here-test.py <nix-grpc-daemon> <local-store-test>
# exit 0: pass, 1: fail, 2: the test never reached the state it tests.
import importlib.util
import math
import os
import socket
import struct
import subprocess
import sys
import tempfile
import time

import testdaemon

spec = importlib.util.spec_from_file_location("keepalive", os.path.join(os.path.dirname(__file__), "keepalive-test.py"))
ka = importlib.util.module_from_spec(spec)
spec.loader.exec_module(ka)

# Builder::probeAfter (10 s) and one round of its thread (1 s), with room.
# Counted in the loop's own steps, each at most STEP_S: a pause of the whole
# sandbox (a farm freezing it) pauses the daemon too, and spends none of it.
SETTLE_S = 60
STEP_S = 2


def main():
    daemon, local_store_test = sys.argv[1:3]
    tmp = tempfile.mkdtemp()
    root = os.path.join(tmp, "root")
    drv = subprocess.run([local_store_test, "--built-drv", root], check=True, capture_output=True, text=True).stdout.strip()
    proc, log = testdaemon.start(daemon, tmp, "--proxy-store", f"local?root={root}")

    def daemon_log():
        log.seek(0)
        return f"--- daemon log ---\n{log.read()}"

    def setup_fail(msg):
        print(f"SETUP FAIL: {msg}\n{daemon_log()}")
        return 2

    def fail(msg):
        print(f"FAIL: {msg}\n{daemon_log()}")
        return 1

    try:
        ports = testdaemon.bound(proc, log, math.inf)
        if ports is None:
            return setup_fail(f"no startup line with the bound ports; daemon exit {proc.poll()}")
        port, mport = ports
        while True:
            if proc.poll() is not None:
                return setup_fail(f"daemon exited with {proc.returncode}")
            vals = ka.scrape(mport)
            if vals and vals.get(("nix_grpc_sched", "workers"), 0) >= 1:
                break
            time.sleep(0.2)

        sock = socket.create_connection(("127.0.0.1", port))
        conn = ka.Conn(sock)
        sock.sendall(b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n" + ka.frame(ka.SETTINGS, 0, 0))
        block = b"".join(
            ka.literal(k, v)
            for k, v in [
                (":method", "POST"),
                (":scheme", "http"),
                (":path", "/nix.remote.Scheduler/Schedule"),
                (":authority", f"127.0.0.1:{port}"),
                ("content-type", "application/grpc"),
                ("te", "trailers"),
            ]
        )
        sock.sendall(ka.frame(ka.HEADERS, ka.END_HEADERS, 1, block))
        msgs = ka.field(1, ka.field(1, ka.field(1, drv.encode())))  # ClientMsgs{msgs{want{drv_path}}}
        sock.sendall(ka.frame(ka.DATA, 0, 1, b"\x00" + struct.pack(">I", len(msgs)) + msgs))  # stream stays open

        # A live client that never calls the builder: it acks, and that is all.
        assigned = False
        spent = 0.0
        last = time.monotonic()
        while True:
            try:
                got = conn.next_frame(0.5)
            except EOFError:
                return fail("the daemon closed the client's connection")
            if got is not None:
                kind, flags, stream, payload = got
                if kind == ka.SETTINGS and not flags & ka.ACK:
                    sock.sendall(ka.frame(ka.SETTINGS, ka.ACK, 0))
                elif kind == ka.PING and not flags & ka.ACK:
                    sock.sendall(ka.frame(ka.PING, ka.ACK, 0, payload))
                elif kind == ka.RST_STREAM and stream == 1:
                    return setup_fail("Schedule stream reset")
                elif kind == ka.DATA and stream == 1:
                    conn.data[1] = conn.data.get(1, b"") + payload
                    kinds, used = ka.sched_msg_kinds(conn.data[1])
                    conn.data[1] = conn.data[1][used:]
                    for what, name in kinds:
                        print(f"scheduler: {what} {name.decode(errors='replace')}")
                        if what == "assigned" and name == drv.encode():
                            assigned = True
                        elif what != "assigned":
                            return setup_fail(f"the drv was {what}, not placed")
            if proc.poll() is not None:
                return fail(f"the daemon exited with {proc.returncode}")
            now = time.monotonic()
            if assigned:
                spent += min(now - last, STEP_S)
            last = now
            if not assigned:
                continue

            def settled_now():
                vals = ka.scrape(mport)
                settled = vals.get(("nix_grpc_events_total", "expect_built_here"), 0) if vals else 0
                return vals, settled, settled >= 1 and ka.holding(vals) == 0

            vals, settled, done = settled_now()
            if done:
                break
            if spent > SETTLE_S:
                # Past the budget: one more round of the daemon's, then its word.
                time.sleep(STEP_S)
                vals, settled, done = settled_now()
                if done:
                    break
                return fail(f"not settled from the store within {SETTLE_S} s: expect_built_here={settled} {ka.show(vals)}")
        print(f"settled: expect_built_here={settled} {ka.show(vals)}")
        if ka.clients(vals) != 1:
            return fail(f"the client was gone when the placement ended: {ka.show(vals)}")
        if settled != 1:
            return fail(f"one placement, settled {settled} times")
        print("PASS: the placement nobody claimed was settled from the node's store")
        return 0
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()


if __name__ == "__main__":
    sys.exit(main())
