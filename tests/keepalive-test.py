#!/usr/bin/env python3
# A client that vanishes without closing its connection (host gone, network
# partition, lost FIN) must not keep its builds wanted. The scheduler lets a
# client's builds go only when its Schedule stream ends (Dispatcher::clientGone),
# and gRPC ends the stream only once it knows the transport is dead.
#
# Speaks just enough HTTP/2 and protobuf to open Scheduler/Schedule, send one
# Want, and read the Assigned for it from the daemon's in-process builder. It
# behaves like a live client for a moment (acks SETTINGS and PINGs), then plays
# dead: it stops answering anything but keeps the socket open. It passes only
# if the daemon, still running, pinged the silent client, sent the GOAWAY that
# gRPC sends when a ping goes unanswered (ENHANCE_YOUR_CALM "ping_timeout",
# grpc_chttp2_ping_timeout) and closed the connection, and then counts no
# Schedule client and one cancelled Done: the build that only this client
# wanted was revoked.
#
# It sees the daemon's side through its Prometheus endpoint:
#   nix_grpc_sched{kind="clients"}                +1 in connectClient, -1 in clientGone
#   nix_grpc_events_total{kind="expect"}          the in-process builder got the Expect
#   nix_grpc_events_total{kind="done_CANCELLED"}  the builder answered the Revoke
#
# usage: keepalive-test.py <nix-grpc-daemon>
# exit 0: pass, 1: fail, 2: the test never reached the state it tests.
import os
import re
import socket
import struct
import subprocess
import sys
import tempfile
import time
import urllib.request

ALIVE_S = 3  # answer pings this long after Assigned, like a live client
DEADLINE_S = 120  # keepalive time (20 s) + gRPC ping timeout (60 s), plus slack
SETUP_S = 30
REVOKE_S = 15  # from the close to clientGone and the builder's Done

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

    # One frame, or None on timeout. Raises EOFError when the peer closed.
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
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/metrics", timeout=5) as resp:
            text = resp.read().decode()
    except OSError:
        return None
    vals = {}
    for line in text.splitlines():
        m = re.match(r'^(nix_grpc_sched|nix_grpc_events_total)\{kind="([^"]+)"[^}]*\}\s+(\S+)$', line)
        if m:
            vals[(m.group(1), m.group(2))] = float(m.group(3))
    return vals


def clients(vals):
    return vals.get(("nix_grpc_sched", "clients"), 0)


def cancelled(vals):
    return vals.get(("nix_grpc_events_total", "done_CANCELLED"), 0)


def show(vals):
    if vals is None:
        return "metrics unreachable"
    return "clients={} workers={} expect={} done_CANCELLED={}".format(
        clients(vals),
        vals.get(("nix_grpc_sched", "workers"), 0),
        vals.get(("nix_grpc_events_total", "expect"), 0),
        cancelled(vals),
    )


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def main():
    daemon = sys.argv[1]
    port, mport = free_port(), free_port()
    tmp = tempfile.mkdtemp()
    log = open(os.path.join(tmp, "daemon.log"), "w+")
    proc = subprocess.Popen(
        [
            daemon,
            "--listen", f"127.0.0.1:{port}",
            "--proxy-socket", os.path.join(tmp, "no-nix-daemon"),
            "--metrics-listen", f"127.0.0.1:{mport}",
        ],
        stdout=log,
        stderr=subprocess.STDOUT,
    )

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
        start = time.monotonic()
        vals = None
        while time.monotonic() - start < SETUP_S:
            if proc.poll() is not None:
                return setup_fail(f"daemon exited with {proc.returncode}")
            vals = scrape(mport)
            if vals and vals.get(("nix_grpc_sched", "workers"), 0) >= 1:
                break
            time.sleep(0.2)
        else:
            return setup_fail(f"no in-process worker in the metrics: {show(vals)}")
        print(f"before: {show(vals)}")
        sock = socket.create_connection(("127.0.0.1", port), timeout=5)
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
            if time.monotonic() - start > 2 * SETUP_S:
                return setup_fail("no Assigned for the Want")
            try:
                got = conn.next_frame(1)
            except EOFError:
                return setup_fail("connection closed before Assigned")
            if got is None:
                continue
            kind, flags, stream, payload = got
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
        closed = None
        while True:
            now = time.monotonic()
            if silent is None and now - assigned > ALIVE_S:
                silent = now
                print("client goes silent")
            if silent is not None and now - silent > DEADLINE_S:
                break
            try:
                got = conn.next_frame(1)
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
        if closed is None:
            return fail(
                f"Schedule stream still open {DEADLINE_S}s after the client went silent ({pings} unanswered pings); "
                f"the scheduler still counts it and the build was not revoked: {show(scrape(mport))}"
            )
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
        vals = None
        while time.monotonic() - closed < REVOKE_S:
            vals = scrape(mport)
            if vals and clients(vals) == 0 and cancelled(vals) >= 1:
                break
            time.sleep(0.2)
        else:
            return fail(f"connection closed but clientGone and the revoke not seen within {REVOKE_S}s: {show(vals)}")
        print(f"after close: {show(vals)}")
        if cancelled(vals) != 1:
            return fail(f"expected one cancelled Done, the revoked build: {show(vals)}")
        if proc.poll() is not None:
            return fail(f"the daemon exited with {proc.returncode}")
        print(f"PASS: clientGone ran and the build was revoked {time.monotonic() - silent:.1f}s after the client went silent")
        return 0
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()


if __name__ == "__main__":
    sys.exit(main())
