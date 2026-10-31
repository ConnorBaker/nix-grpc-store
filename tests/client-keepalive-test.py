#!/usr/bin/env python3
# A client waiting on its Schedule stream with nothing to send must notice by
# itself that the daemon is gone when no FIN reaches it. After a partition, or
# with the daemon's host gone, the daemon pings its end of the connection
# closed and lets the client's builds go (keepalive-test.py), but its FIN is
# lost: without pings of its own the client would wait forever for builds
# nobody runs. Its pings must not get it thrown out either (GOAWAY
# too_many_pings), and a store query they cut off must be asked again.
#
# Runs three keepalive-clients (the channel arguments and the Schedule call the
# plugin uses) and a store-query-client (the plugin itself, as nix loads it)
# against a real nix-grpc-daemon, each through a TCP proxy here:
#   partitioned: once the client has its Assigned, the proxy stops forwarding
#     in both directions but keeps both sockets open, and does not pass the
#     daemon's close on. The client must end its stream itself, UNAVAILABLE.
#     Still waiting AFTER_CLOSE_S after the daemon closed its end fails.
#   idle: the same, but the proxy stops forwarding IDLE_S after the Assigned.
#     By then the client has pinged twice without sending data, and gRPC
#     sends such a client one ping a minute (ping_rate_policy.cc:72-81): its
#     next ping comes up to a minute into the partition, and gRPC drops the
#     connection when that ping has gone a minute unanswered. The client must
#     end its stream, UNAVAILABLE, within IDLE_DEADLINE_S of the partition.
#     The daemon closed its end long before, which the proxy keeps from it.
#   healthy: forwarded throughout. The client wants a drv for a system no
#     worker offers, so after one Unplaceable neither end has anything to
#     send, as for a client waiting for a worker that has not come. (An
#     Assigned would not do: the builder gives an unclaimed drv back after a
#     minute and the daemon sends a new Assigned, which resets the strikes.)
#     The proxy counts the client's PINGs since the daemon last sent HEADERS,
#     DATA or WINDOW_UPDATE, which reset the daemon's ping strikes. Once
#     there were MAX_STRIKES + 2 of them, enough for a GOAWAY had the daemon
#     struck them, no GOAWAY may have come and the stream must still be open.
#   query: after one answered QueryValidPaths, the proxy stops forwarding the
#     connections it has and the client asks again. That query must be
#     answered, over a connection the proxy took after it stopped forwarding.
#
# usage: client-keepalive-test.py <nix-grpc-daemon> <keepalive-client> <store-query-client> <plugin>
# exit 0: pass, 1: fail, 2: the test never reached the state it tests.
import os
import re
import socket
import struct
import subprocess
import sys
import tempfile
import threading
import time
import urllib.request

ALIVE_S = 3  # forward this long after Assigned, like a live connection
SETUP_S = 30
AFTER_CLOSE_S = 20  # the partitioned client may wait this long after the daemon closed
PARTITION_DEADLINE_S = 150  # the client's keepalive (20 s) + gRPC's ping timeout (60 s), plus slack
IDLE_S = 44  # after the client's keepalive pings at +20 s and +40 s
IDLE_DEADLINE_S = 130  # up to a minute to its next ping, gRPC's 1 min ping timeout, plus slack
HEALTHY_DEADLINE_S = 220  # from Unplaceable: gRPC sends a client that sends no data one ping a minute
GRACE_S = 2  # for a GOAWAY to follow the last ping counted
MAX_STRIKES = 2  # gRPC's default GRPC_ARG_HTTP2_MAX_PING_STRIKES; the daemon sets none
UNAVAILABLE = 14

DATA, HEADERS, SETTINGS, PING, GOAWAY, WINDOW_UPDATE = 0x0, 0x1, 0x4, 0x6, 0x7, 0x8
ACK = 0x1
PREFACE = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"
DRV_PARTITIONED = "/nix/store/" + "0" * 32 + "-client-keepalive-partitioned.drv"
DRV_HEALTHY = "/nix/store/" + "1" * 32 + "-client-keepalive-healthy.drv"
DRV_IDLE = "/nix/store/" + "2" * 32 + "-client-keepalive-idle.drv"
SYSTEM_NOWHERE = "client-keepalive-nowhere"  # no worker offers it


class Frames:
    """One direction of an HTTP/2 connection, split into frames."""

    def __init__(self, from_client):
        self.skip = len(PREFACE) if from_client else 0
        self.buf = b""

    def feed(self, chunk):
        self.buf += chunk
        if self.skip:
            n = min(self.skip, len(self.buf))
            self.buf = self.buf[n:]
            self.skip -= n
        out = []
        while len(self.buf) >= 9:
            length = int.from_bytes(self.buf[:3], "big")
            if len(self.buf) < 9 + length:
                break
            out.append((self.buf[3], self.buf[4], self.buf[9 : 9 + length]))
            self.buf = self.buf[9 + length :]
        return out


class Link:
    """One connection through a Proxy: forwarded until hole(), its frames watched."""

    def __init__(self, down, up):
        self.lock = threading.Lock()
        self.holed = None  # when forwarding stopped
        self.pings = []  # when the client sent each PING
        self.run = 0  # the client's PINGs since the daemon last reset its strikes
        self.longest = 0
        self.goaway = None
        self.server_closed = None
        self.client_closed = None
        self.socks = [down, up]
        threading.Thread(target=self.pump, args=(down, up, True), daemon=True).start()
        threading.Thread(target=self.pump, args=(up, down, False), daemon=True).start()

    def hole(self, now):
        with self.lock:
            if self.holed is None:
                self.holed = now

    def pump(self, src, dst, from_client):
        frames = Frames(from_client)
        while True:
            try:
                chunk = src.recv(65536)
            except OSError:
                chunk = b""
            now = time.monotonic()
            with self.lock:
                holed = self.holed is not None
                if not chunk:
                    if from_client:
                        self.client_closed = now
                    else:
                        self.server_closed = now
                for kind, flags, payload in frames.feed(chunk):
                    self.saw(from_client, kind, flags, payload, now)
            if not chunk:
                if not holed:
                    try:
                        dst.shutdown(socket.SHUT_WR)
                    except OSError:
                        pass
                return
            if not holed:
                try:
                    dst.sendall(chunk)
                except OSError:
                    pass

    # Under the lock.
    def saw(self, from_client, kind, flags, payload, now):
        if from_client:
            if kind == PING and not flags & ACK:
                self.pings.append(now)
                self.run += 1
                self.longest = max(self.longest, self.run)
        elif kind in (HEADERS, DATA, WINDOW_UPDATE):
            self.run = 0
        elif kind == GOAWAY and self.goaway is None:
            code = struct.unpack(">I", payload[4:8])[0] if len(payload) >= 8 else None
            self.goaway = (now, code, payload[8:].decode(errors="replace"))

    def state(self):
        with self.lock:
            return {
                "pings": list(self.pings),
                "longest": self.longest,
                "goaway": self.goaway,
                "server_closed": self.server_closed,
                "client_closed": self.client_closed,
            }

    def close(self):
        for sock in self.socks:
            try:
                sock.close()
            except OSError:
                pass


class Proxy:
    """Forwards each connection to the daemon; hole() stops those it has so far."""

    def __init__(self, upstream):
        self.upstream = upstream
        self.listener = socket.socket()
        self.listener.bind(("127.0.0.1", 0))
        self.listener.listen(8)
        self.port = self.listener.getsockname()[1]
        self.lock = threading.Lock()
        self.links = []
        threading.Thread(target=self.serve, daemon=True).start()

    def serve(self):
        while True:
            try:
                down, _ = self.listener.accept()
                up = socket.create_connection(("127.0.0.1", self.upstream))
            except OSError as err:
                print(f"proxy on {self.port}: {err}")
                return
            with self.lock:
                self.links.append(Link(down, up))

    def hole(self):
        now = time.monotonic()
        with self.lock:
            for link in self.links:
                link.hole(now)
        return now

    def link(self, i):
        with self.lock:
            return self.links[i] if i < len(self.links) else None

    def count(self):
        with self.lock:
            return len(self.links)

    def close(self):
        try:
            self.listener.close()
        except OSError:
            pass
        with self.lock:
            for link in self.links:
                link.close()


class Client:
    """A client process and the lines it printed, with when."""

    def __init__(self, argv):
        self.proc = subprocess.Popen(
            argv, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True
        )
        self.lock = threading.Lock()
        self.lines = []
        threading.Thread(target=self.read, daemon=True).start()

    def read(self):
        for line in self.proc.stdout:
            # nix colours its error messages.
            line = re.sub(r"\x1b\[[0-9;]*m", "", line.rstrip("\n"))
            with self.lock:
                self.lines.append((time.monotonic(), line))

    def starting(self, prefix):
        with self.lock:
            return [(at, line) for at, line in self.lines if line.startswith(prefix)]

    def first(self, prefix):
        found = self.starting(prefix)
        return found[0] if found else None

    def say(self, line):
        self.proc.stdin.write(line + "\n")
        self.proc.stdin.flush()

    def output(self):
        with self.lock:
            return "\n".join(line for _, line in self.lines)

    def stop(self):
        if self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.proc.kill()


def scrape(port):
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/metrics", timeout=5) as resp:
            text = resp.read().decode()
    except OSError:
        return None
    vals = {}
    for line in text.splitlines():
        m = re.match(r'^(nix_grpc_sched)\{kind="([^"]+)"[^}]*\}\s+(\S+)$', line)
        if m:
            vals[(m.group(1), m.group(2))] = float(m.group(3))
    return vals


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def since(times, origin):
    return ", ".join(f"+{t - origin:.1f}s" for t in times)


def main():
    daemon, client_exe, query_exe, plugin = sys.argv[1:5]
    port, mport = free_port(), free_port()
    tmp = tempfile.mkdtemp()
    log = open(os.path.join(tmp, "daemon.log"), "w+")
    proc = subprocess.Popen(
        [
            daemon,
            "--listen", f"127.0.0.1:{port}",
            "--proxy-socket", os.path.join(tmp, "no-nix-daemon"),
            # Store queries get an answer without a nix-daemon: nothing is valid.
            "--proxy-store", "dummy://",
            "--metrics-listen", f"127.0.0.1:{mport}",
            # A slot for each client's build.
            "--max-jobs", "3",
        ],
        stdout=log,
        stderr=subprocess.STDOUT,
    )
    clients = {}
    proxies = []

    def daemon_log():
        log.seek(0)
        return f"--- daemon log ---\n{log.read()}"

    def client_logs():
        return "\n".join(f"--- {name} client ---\n{c.output()}" for name, c in clients.items())

    def setup_fail(msg):
        print(f"SETUP FAIL: {msg}\n{client_logs()}\n{daemon_log()}")
        return 2

    try:
        start = time.monotonic()
        while True:
            if proc.poll() is not None:
                return setup_fail(f"daemon exited with {proc.returncode}")
            vals = scrape(mport)
            if vals and vals.get(("nix_grpc_sched", "workers"), 0) >= 1:
                break
            if time.monotonic() - start > SETUP_S:
                return setup_fail(f"no in-process worker in the metrics: {vals}")
            time.sleep(0.2)

        partitioned, healthy, quiet, queried = Proxy(port), Proxy(port), Proxy(port), Proxy(port)
        proxies += [partitioned, healthy, quiet, queried]
        part = clients["partitioned"] = Client([client_exe, f"127.0.0.1:{partitioned.port}", DRV_PARTITIONED])
        heal = clients["healthy"] = Client([client_exe, f"127.0.0.1:{healthy.port}", DRV_HEALTHY, SYSTEM_NOWHERE])
        idle = clients["idle"] = Client([client_exe, f"127.0.0.1:{quiet.port}", DRV_IDLE])
        query = clients["query"] = Client([query_exe, plugin, f"127.0.0.1:{queried.port}"])
        while True:
            part_assigned, idle_assigned = (c.first("assigned ") for c in (part, idle))
            heal_unplaced = heal.first("unplaceable ")
            answered = query.first("answered ")
            if part_assigned and heal_unplaced and idle_assigned and answered:
                break
            if any(c.first("ended ") for c in (part, heal, idle)):
                return setup_fail("a client's stream ended before its Assigned or Unplaceable")
            if query.first("failed ") or query.proc.poll() is not None:
                return setup_fail("the first store query was not answered")
            if time.monotonic() - start > 2 * SETUP_S:
                return setup_fail("no Assigned for two clients, no Unplaceable for the third, or no answer to the first store query")
            time.sleep(0.1)
        part_assigned, heal_unplaced, idle_assigned = part_assigned[0], heal_unplaced[0], idle_assigned[0]
        print(f"two clients have their Assigned, the third its Unplaceable; the first store query was {answered[1]!r}")

        time.sleep(max(0.0, part_assigned + ALIVE_S - time.monotonic()))
        holed = partitioned.hole()
        print("partitioned: the proxy stops forwarding, both sockets stay open")
        query_holed = queried.hole()
        query_links = queried.count()
        query.say("again")
        print(f"query: the proxy stops forwarding its {query_links} connections; the client asks again")

        part_verdict = heal_verdict = idle_verdict = query_verdict = None
        idle_holed = None
        told_close = False
        while part_verdict is None or heal_verdict is None or idle_verdict is None or query_verdict is None:
            now = time.monotonic()
            if idle_holed is None and idle_verdict is None:
                ended = idle.first("ended ")
                if ended is not None:
                    idle_verdict = (False, f"stream ended before the partition: {ended[1]!r}")
                    print(f"idle: FAIL: {idle_verdict[1]}")
                elif now - idle_assigned >= IDLE_S:
                    st = quiet.link(0).state()
                    idle_holed = quiet.hole()
                    print(f"idle: the proxy stops forwarding; client pings so far at {since(st['pings'], idle_assigned)}")
            if idle_verdict is None and idle_holed is not None:
                st = quiet.link(0).state()
                ended = idle.first("ended ")
                if ended is not None:
                    at, line = ended
                    fields = line.split(" ", 2)
                    code = int(fields[1])
                    msg = fields[2] if len(fields) > 2 else ""
                    what = (
                        f"client ended its stream at +{at - idle_holed:.1f}s: code {code} {msg!r}; "
                        f"client pings at {since(st['pings'], idle_assigned)} after its Assigned"
                    )
                    if st["server_closed"] is not None:
                        what += f" (daemon closed at +{st['server_closed'] - idle_holed:.1f}s)"
                    ok = code == UNAVAILABLE and at - idle_holed <= IDLE_DEADLINE_S
                    idle_verdict = (ok, what if ok else f"{what}: not UNAVAILABLE within {IDLE_DEADLINE_S}s")
                elif now - idle_holed > IDLE_DEADLINE_S:
                    idle_verdict = (
                        False,
                        f"client still waiting {IDLE_DEADLINE_S}s into the partition; "
                        f"client pings at {since(st['pings'], idle_assigned)} after its Assigned",
                    )
                if idle_verdict is not None:
                    print(f"idle: {'PASS' if idle_verdict[0] else 'FAIL'}: {idle_verdict[1]}")
            if part_verdict is None:
                st = partitioned.link(0).state()
                closed = st["server_closed"]
                if closed is not None and not told_close:
                    told_close = True
                    print(f"partitioned: the daemon closed its end at +{closed - holed:.1f}s; the proxy keeps it from the client")
                ended = part.first("ended ")
                if ended is not None:
                    at, line = ended
                    fields = line.split(" ", 2)
                    code = int(fields[1])
                    msg = fields[2] if len(fields) > 2 else ""
                    what = f"client ended its stream at +{at - holed:.1f}s: code {code} {msg!r}"
                    if st["client_closed"] is not None:
                        what += f", closed its connection at +{st['client_closed'] - holed:.1f}s"
                    if closed is not None:
                        what += f" (daemon closed at +{closed - holed:.1f}s)"
                    part_verdict = (code == UNAVAILABLE, what if code == UNAVAILABLE else f"{what}, not UNAVAILABLE")
                elif closed is not None and now - closed > AFTER_CLOSE_S:
                    part_verdict = (
                        False,
                        f"client still waiting on its stream {now - closed:.0f}s after the daemon closed its end "
                        f"(+{closed - holed:.1f}s), {len(st['pings'])} client pings ({since(st['pings'], holed)})",
                    )
                elif now - holed > PARTITION_DEADLINE_S:
                    part_verdict = (False, f"client still waiting {PARTITION_DEADLINE_S}s in, and the daemon has not closed")
                if part_verdict is not None:
                    print(f"partitioned: {'PASS' if part_verdict[0] else 'FAIL'}: {part_verdict[1]}")
            if heal_verdict is None:
                st = healthy.link(0).state()
                pings = st["pings"]
                ended = heal.first("ended ")
                if st["goaway"] is not None:
                    at, code, debug = st["goaway"]
                    heal_verdict = (
                        False,
                        f"GOAWAY at +{at - heal_unplaced:.1f}s, code {code} {debug!r}, after client pings at {since(pings, heal_unplaced)}",
                    )
                elif ended is not None:
                    heal_verdict = (False, f"stream ended: {ended[1]!r}, client pings at {since(pings, heal_unplaced)}")
                elif st["longest"] >= MAX_STRIKES + 2 and now - pings[-1] > GRACE_S:
                    heal_verdict = (
                        True,
                        f"{st['longest']} client pings in a row with nothing from the daemon that resets its strikes, "
                        f"no GOAWAY, stream open; client pings at {since(pings, heal_unplaced)}",
                    )
                elif now - heal_unplaced > HEALTHY_DEADLINE_S:
                    heal_verdict = (
                        False,
                        f"only {st['longest']} client pings in a row in {HEALTHY_DEADLINE_S}s, at {since(pings, heal_unplaced)}",
                    )
                if heal_verdict is not None:
                    print(f"healthy: {'PASS' if heal_verdict[0] else 'FAIL'}: {heal_verdict[1]}")
            if query_verdict is None:
                answers = query.starting("answered ")
                failed = query.first("failed ")
                # GrpcStore::retryingWithProgress says so before each retry.
                retried = sum(1 for _, line in query.starting("") if line.endswith(", retrying"))
                if len(answers) >= 2:
                    at, line = answers[1]
                    links = queried.count()
                    what = (
                        f"second query {line!r} at +{at - query_holed:.1f}s, after {retried} retry lines, "
                        f"over {links - query_links} connection(s) the proxy took after it stopped forwarding"
                    )
                    query_verdict = (links > query_links, what if links > query_links else f"{what}: not a new one")
                elif failed is not None:
                    query_verdict = (False, f"second query {failed[1]!r} at +{failed[0] - query_holed:.1f}s")
                elif now - query_holed > PARTITION_DEADLINE_S:
                    query_verdict = (False, f"second query still unanswered {PARTITION_DEADLINE_S}s in")
                if query_verdict is not None:
                    print(f"query: {'PASS' if query_verdict[0] else 'FAIL'}: {query_verdict[1]}")
            time.sleep(0.2)
        if proc.poll() is not None:
            print(f"FAIL: the daemon exited with {proc.returncode}\n{daemon_log()}")
            return 1
        if part_verdict[0] and heal_verdict[0] and idle_verdict[0] and query_verdict[0]:
            print("PASS")
            return 0
        print(f"FAIL\n{client_logs()}\n{daemon_log()}")
        return 1
    finally:
        for c in clients.values():
            c.stop()
        for p in proxies:
            p.close()
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()


if __name__ == "__main__":
    sys.exit(main())
