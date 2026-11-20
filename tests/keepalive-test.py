#!/usr/bin/env python3
# A client that vanishes without closing its connection (host gone, network
# partition, lost FIN) must not keep its builds wanted. The scheduler lets a
# client's builds go only when its Schedule stream ends (Dispatcher::clientGone),
# and gRPC ends the stream only once it knows the transport is dead. It holds
# them for the client's return first (sched::Core::holdMs), and the builder
# gives back one nobody claimed (Builder::tick): a build ends either way.
#
# Speaks just enough HTTP/2 and protobuf to open Scheduler/Schedule, send one
# Want, and read the Assigned for it from the daemon's in-process builder. It
# behaves like a live client for a moment (acks SETTINGS and PINGs), then plays
# dead: it stops answering anything but keeps the socket open. It passes only
# if the daemon, still running, pinged the silent client, sent the GOAWAY that
# gRPC sends when a ping goes unanswered (ENHANCE_YOUR_CALM "ping_timeout",
# grpc_chttp2_ping_timeout) and closed the connection, and then counts no
# Schedule client, no drv placed, queued or kept, and a Done for every Expect:
# the build that only this client wanted ended and nothing places it again.
#
# It sees the daemon's side through its Prometheus endpoint:
#   nix_grpc_sched{kind="clients"}                +1 in connectClient, -1 in clientGone
#   nix_grpc_events_total{kind="expect"}          the in-process builder got the Expect
#   nix_grpc_sched_system{kind="running"|"queued"|"lingering"}  drvs it still holds
#   nix_grpc_events_total{kind="done_*"}          the builder ended a placement
#
# usage: keepalive-test.py <nix-grpc-daemon>
# exit 0: pass, 1: fail, 2: the test never reached the state it tests.
import math
import os
import re
import socket
import struct
import subprocess
import sys
import tempfile
import time
import urllib.request

import testdaemon

ALIVE_S = 3  # answer pings this long after Assigned, like a live client

DATA, HEADERS, RST_STREAM, SETTINGS, PING, GOAWAY = 0x0, 0x1, 0x3, 0x4, 0x6, 0x7
ENHANCE_YOUR_CALM = 0xB
END_STREAM, ACK, END_HEADERS = 0x1, 0x1, 0x4
DRV = "/nix/store/" + "0" * 32 + "-keepalive-schedule.drv"


def frame(kind, flags, stream, payload=b""):
    return struct.pack(">I", len(payload))[1:] + bytes([kind, flags]) + struct.pack(">I", stream) + payload


def literal(name, value):
    # HPACK literal header field without indexing, new name, no Huffman.
    name, value = name.encode(), value.encode()
    return b"\x00" + bytes([len(name)]) + name + bytes([len(value)]) + value


def varint(n):
    out = b""
    while True:
        b = n & 0x7F
        n >>= 7
        if n:
            out += bytes([b | 0x80])
        else:
            return out + bytes([b])


def field(num, payload):  # length-delimited protobuf field
    return varint(num << 3 | 2) + varint(len(payload)) + payload


def read_varint(buf, i):
    shift = value = 0
    while True:
        b = buf[i]
        i += 1
        value |= (b & 0x7F) << shift
        shift += 7
        if not b & 0x80:
            return value, i


def sched_msg_kinds(grpc_payload):
    # SchedMsgs{repeated SchedMsg msgs = 1}; SchedMsg oneof: 1 assigned,
    # 2 cached, 3 unplaceable, 4 restarting. Returns ((kind, drv bytes)
    # pairs, bytes consumed): only complete gRPC messages are consumed.
    kinds = []
    i = 0
    while i + 5 <= len(grpc_payload):
        n = struct.unpack(">I", grpc_payload[i + 1 : i + 5])[0]
        if i + 5 + n > len(grpc_payload):
            break
        msgs = grpc_payload[i + 5 : i + 5 + n]
        i += 5 + n
        j = 0
        while j < len(msgs):
            tag, j = read_varint(msgs, j)
            ln, j = read_varint(msgs, j)
            msg = msgs[j : j + ln]
            j += ln
            if tag >> 3 != 1 or not msg:
                continue
            inner_tag, k = read_varint(msg, 0)
            inner_len, k = read_varint(msg, k)
            inner = msg[k : k + inner_len]
            drv = b""
            if inner:
                t2, m = read_varint(inner, 0)
                if t2 >> 3 == 1:
                    l2, m = read_varint(inner, m)
                    drv = inner[m : m + l2]
            kinds.append(({1: "assigned", 2: "cached", 3: "unplaceable", 4: "restarting"}.get(inner_tag >> 3, "?"), drv))
    return kinds, i


class Conn:
    def __init__(self, sock):
        self.sock = sock
        self.buf = b""
        self.data = {}  # stream -> bytes

    # One frame, or None once `timeout` (None: no limit) is over. Raises
    # EOFError when the peer closed.
    def next_frame(self, timeout):
        self.sock.settimeout(timeout)
        while True:
            if len(self.buf) >= 9:
                length = int.from_bytes(self.buf[:3], "big")
                if len(self.buf) >= 9 + length:
                    head, self.buf = self.buf[: 9 + length], self.buf[9 + length :]
                    kind, flags = head[3], head[4]
                    stream = struct.unpack(">I", head[5:9])[0] & 0x7FFFFFFF
                    return kind, flags, stream, head[9:]
            try:
                chunk = self.sock.recv(65536)
            except socket.timeout:
                return None
            except ConnectionResetError:
                raise EOFError
            if not chunk:
                raise EOFError
            self.buf += chunk


def scrape(port):
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/metrics") as resp:
            text = resp.read().decode()
    except OSError:
        return None
    vals = {}
    for line in text.splitlines():
        m = re.match(r'^(nix_grpc_sched|nix_grpc_events_total)\{kind="([^"]+)"[^}]*\}\s+(\S+)$', line)
        if m:
            vals[(m.group(1), m.group(2))] = float(m.group(3))
        # Per system and feature set: summed.
        m = re.match(r'^nix_grpc_sched_system\{[^}]*kind="([^"]+)"[^}]*\}\s+(\S+)$', line)
        if m:
            key = ("nix_grpc_sched_system", m.group(1))
            vals[key] = vals.get(key, 0) + float(m.group(2))
    return vals


def clients(vals):
    return vals.get(("nix_grpc_sched", "clients"), 0)


def cancelled(vals):
    return vals.get(("nix_grpc_events_total", "done_CANCELLED"), 0)


def holding(vals):
    """Drvs the scheduler still places, queues or keeps."""
    return sum(vals.get(("nix_grpc_sched_system", kind), 0) for kind in ("running", "queued", "lingering"))


def expects(vals):
    return vals.get(("nix_grpc_events_total", "expect"), 0)


def dones(vals):
    return sum(v for (name, kind), v in vals.items() if name == "nix_grpc_events_total" and kind.startswith("done_"))


def show(vals):
    if vals is None:
        return "metrics unreachable"
    return "clients={} workers={} holding={} expect={} done={} done_CANCELLED={}".format(
        clients(vals),
        vals.get(("nix_grpc_sched", "workers"), 0),
        holding(vals),
        expects(vals),
        dones(vals),
        cancelled(vals),
    )


def main():
    daemon = sys.argv[1]
    tmp = tempfile.mkdtemp()
    # Past the old cap of 63.
    probe = subprocess.run([daemon, "--max-jobs", "100", "--no-such-flag"], capture_output=True, text=True)
    if "unknown flag" not in probe.stderr:
        print(f"FAIL: --max-jobs 100 refused: {probe.stderr}")
        return 1
    # max-jobs, but no more than the 128 auto-allocated uid slots.
    os.environ["NIX_CONFIG"] = "max-jobs = 200\nextra-experimental-features = auto-allocate-uids\nauto-allocate-uids = true"
    # The farm it hands builds to, from a file it reads again when it changes.
    farm_dir = os.path.join(tmp, "farm")
    os.mkdir(farm_dir)
    farm_file = os.path.join(farm_dir, "farm.json")
    with open(farm_file, "w") as f:
        f.write('{"machines": [{"systems": {"keepalive-test-system": []}, "jobs": 3}]}')
    proc, log = testdaemon.start(daemon, tmp, "--farm-file", farm_file)

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
        # Its startup line names the ports it bound: wait for it, or its exit.
        ports = testdaemon.bound(proc, log, math.inf)
        if ports is None:
            return setup_fail(f"no startup line with the bound ports; daemon exit {proc.poll()}")
        port, mport = ports
        # The port is this daemon's alone: a second one asked for it must not
        # start (gRPC's default SO_REUSEPORT let it, and share the clients).
        # It exits, or this waits.
        second, second_log = testdaemon.start(daemon, tempfile.mkdtemp(), "--listen", f"127.0.0.1:{port}")
        code = second.wait()
        if code == 0 or "failed to start gRPC server" not in open(second_log.name).read():
            return fail(f"a second daemon on port {port} exited {code}, not for its listener")
        # The metrics say when the in-process worker is there, and nothing
        # tells of it: ask until they do.
        while True:
            if proc.poll() is not None:
                return setup_fail(f"daemon exited with {proc.returncode}")
            vals = scrape(mport)
            if vals and vals.get(("nix_grpc_sched", "workers"), 0) >= 1:
                break
            time.sleep(0.2)
        print(f"before: {show(vals)}")
        if "max_jobs=128" not in daemon_log():
            return fail("not advertising min(max-jobs 200, 128 build user slots)")
        if 'event=farm machines=1 said="keepalive-test-system[]=3"' not in daemon_log():
            return fail("--farm-file not read at start")

        def farm_logged(line):
            # The serve loop looks about once a second.
            deadline = time.monotonic() + 30
            while line not in daemon_log():
                if proc.poll() is not None or time.monotonic() > deadline:
                    return False
                time.sleep(0.2)
            return True

        with open(farm_file + ".tmp", "w") as f:
            f.write('{"machines": [{"systems": {"keepalive-test-system": []}, "jobs": 5}]}')
        os.rename(farm_file + ".tmp", farm_file)
        if not farm_logged('event=farm machines=1 said="keepalive-test-system[]=5"'):
            return fail("--farm-file not read again after it changed")
        # Unreadable, then gone: no farm, and said so.
        with open(farm_file + ".tmp", "w") as f:
            f.write('{"machines": [{"systems": {"keepalive-test-system": []}}]}')
        os.rename(farm_file + ".tmp", farm_file)
        if not farm_logged("event=farm_unreadable"):
            return fail("an unreadable --farm-file not logged")
        # One it cannot even stat is unreadable too, not absent.
        os.chmod(farm_dir, 0)
        denied = farm_logged("Permission denied")
        os.chmod(farm_dir, 0o755)
        if not denied or "event=farm_absent" in daemon_log():
            return fail("a --farm-file the daemon cannot stat not logged as unreadable")
        os.unlink(farm_file)
        if not farm_logged("event=farm_absent"):
            return fail("an absent --farm-file not logged")
        sock = socket.create_connection(("127.0.0.1", port))
        conn = Conn(sock)

        sock.sendall(b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n" + frame(SETTINGS, 0, 0))
        block = b"".join(
            literal(k, v)
            for k, v in [
                (":method", "POST"),
                (":scheme", "http"),
                (":path", "/nix.remote.Scheduler/Schedule"),
                (":authority", f"127.0.0.1:{port}"),
                ("content-type", "application/grpc"),
                ("te", "trailers"),
            ]
        )
        sock.sendall(frame(HEADERS, END_HEADERS, 1, block))
        msgs = field(1, field(1, field(1, DRV.encode())))  # ClientMsgs{msgs{want{drv_path}}}
        sock.sendall(frame(DATA, 0, 1, b"\x00" + struct.pack(">I", len(msgs)) + msgs))  # stream stays open

        assigned = None
        while assigned is None:
            try:
                kind, flags, stream, payload = conn.next_frame(None)
            except EOFError:
                return setup_fail("connection closed before Assigned")
            if kind == SETTINGS and not flags & ACK:
                sock.sendall(frame(SETTINGS, ACK, 0))
            elif kind == PING and not flags & ACK:
                sock.sendall(frame(PING, ACK, 0, payload))
            elif kind == RST_STREAM and stream == 1:
                return setup_fail("Schedule stream reset")
            elif kind == HEADERS and stream == 1 and flags & END_STREAM:
                return setup_fail("Schedule stream finished (trailers) before Assigned")
            elif kind == DATA and stream == 1:
                conn.data[1] = conn.data.get(1, b"") + payload
                kinds, used = sched_msg_kinds(conn.data[1])
                conn.data[1] = conn.data[1][used:]
                for what, drv in kinds:
                    print(f"scheduler: {what} {drv.decode(errors='replace')}")
                    if what == "assigned" and drv == DRV.encode():
                        assigned = time.monotonic()
        vals = scrape(mport)
        print(f"assigned: {show(vals)}")
        if not vals or clients(vals) != 1 or vals.get(("nix_grpc_events_total", "expect"), 0) < 1:
            return setup_fail(f"after Assigned expected clients=1, expect>=1: {show(vals)}")
        if cancelled(vals) != 0:
            return setup_fail(f"a Done CANCELLED before the client went silent: {show(vals)}")

        silent = None
        pings = 0
        goaways = []  # (error code, debug data)
        # Until the daemon closes the connection.
        while True:
            alive_left = assigned + ALIVE_S - time.monotonic()
            if silent is None and alive_left <= 0:
                silent = time.monotonic()
                print("client goes silent")
            try:
                got = conn.next_frame(alive_left if silent is None else None)
            except EOFError:
                closed = time.monotonic()
                break
            if got is None:
                continue
            kind, flags, stream, payload = got
            if silent is not None:
                if kind == PING and not flags & ACK:
                    pings += 1
                    print(f"ping {pings} at +{time.monotonic() - silent:.1f}s, not answering")
                elif kind == GOAWAY:
                    code = struct.unpack(">I", payload[4:8])[0] if len(payload) >= 8 else None
                    goaways.append((code, payload[8:]))
                    print(f"server sent GOAWAY at +{time.monotonic() - silent:.1f}s: code {code} {payload[8:].decode(errors='replace')!r}")
                continue
            if kind == SETTINGS and not flags & ACK:
                sock.sendall(frame(SETTINGS, ACK, 0))
            elif kind == PING and not flags & ACK:
                sock.sendall(frame(PING, ACK, 0, payload))
        if silent is None:
            return fail("connection closed while the client was still answering")
        print(f"server closed the connection at +{closed - silent:.1f}s ({pings} unanswered pings)")
        # Only the keepalive may end it: a crash or any other close does not count.
        if proc.poll() is not None:
            return fail(f"the daemon exited with {proc.returncode}")
        if pings == 0:
            return fail("closed without an unanswered ping")
        if (ENHANCE_YOUR_CALM, b"ping_timeout") not in goaways:
            return fail(f"closed without the keepalive's GOAWAY ENHANCE_YOUR_CALM ping_timeout: {goaways}")
        # Nothing tells of clientGone and the builder's Dones but the metrics.
        # With no client and nothing held, nothing places the build again.
        while True:
            if proc.poll() is not None:
                return fail(f"the daemon exited with {proc.returncode}")
            vals = scrape(mport)
            if vals and clients(vals) == 0 and holding(vals) == 0:
                break
            time.sleep(0.2)
        print(f"after close: {show(vals)}")
        if dones(vals) != expects(vals):
            return fail(f"expected a Done for every Expect: {show(vals)}")
        if proc.poll() is not None:
            return fail(f"the daemon exited with {proc.returncode}")
        print(f"PASS: clientGone ran and the build ended {time.monotonic() - silent:.1f}s after the client went silent")
        return 0
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()


if __name__ == "__main__":
    sys.exit(main())
