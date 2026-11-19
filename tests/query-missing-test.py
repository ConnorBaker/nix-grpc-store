#!/usr/bin/env python3
# queryMissing with an evaluation store must not cost a round trip per
# output. `nix build --store grpc://… --eval-store auto` asks it before it
# builds anything (printMissing); Nix's walk asks isValidPath one output at a
# time, so a large graph waited a round trip per output before the first
# build was sent.
#
# Runs query-missing-client (the plugin itself, as nix loads it) against a
# real nix-grpc-daemon serving a local store, which does not trust it, and
# counts the daemon's QueryValidPaths RPCs (nix_grpc_rpcs_total) for each run:
#   chain: CHAIN derivations in a chain, none built. At most one call per
#     topDownBudget (64) derivations, where a call per output made CHAIN,
#     plus one for the client's check that an output added after
#     queryMissing is valid (what queryMissing asked must not answer it).
#   built: BUILT derivations whose outputs the store has. One call.
#   deep: a target only the client's substituter has, over DEEP levels of
#     WIDE derivations nobody has. Nix's walk stops at the target, so asking
#     ahead must too: one call, not one per level below.
#   mixed: outputs valid, missing and substitutable, and derivations only
#     the store has; again with no substituter on the client's side, though
#     the daemon keeps it.
#   nosubst: a derivation that disallows substitutes, whose narinfo in the
#     substituter is corrupt; Nix never asks for it.
#   probe: while the client's queryMissing runs, what it asked ahead must
#     not answer another grpc store, or the same one on another thread.
#   toposort-probe: what topoSortPaths found absent must not answer
#     another thread while the sort runs, or any lookup after it.
# Each time the client's answer must be what Nix's own walk answers over the
# same store.
#
# usage: query-missing-test.py <nix-grpc-daemon> <query-missing-client> <plugin>
# exit 0: pass, 1: fail, 2: the test never reached the state it tests,
# 77: this Nix has no queryMissing with an evaluation store.
import os
import re
import subprocess
import sys
import tempfile
import time
import urllib.request

import testdaemon

CHAIN = 200
BUILT = 100
BUDGET = 64  # topDownBudget in src/top-down.hh
SETUP_S = 30
CLIENT_S = 120


def calls(port, method="QueryValidPaths"):
    with urllib.request.urlopen(f"http://127.0.0.1:{port}/metrics", timeout=5) as resp:
        text = resp.read().decode()
    pattern = re.compile(rf'^nix_grpc_rpcs_total\{{[^}}]*method="{method}"[^}}]*\}}\s+(\S+)$')
    return sum(float(m.group(1)) for m in map(pattern.match, text.splitlines()) if m)


def main():
    daemon, client, plugin = sys.argv[1:4]
    tmp = tempfile.mkdtemp()
    build = os.path.join(tmp, "build")
    log = open(os.path.join(tmp, "daemon.log"), "w+")

    def nix_env(home, substituters=f"file://{build}-cache?compression=none"):
        return dict(
            os.environ,
            HOME=home,
            XDG_CACHE_HOME=os.path.join(home, "xdg-cache"),
            NIX_CONFIG=f"experimental-features = ca-derivations\nsubstituters = {substituters}\n",
        )

    # One daemon serves every run: each has its own evaluation store, and
    # their derivations differ by name.
    proc = subprocess.Popen(
        [
            daemon,
            "--listen", "127.0.0.1:0",
            "--proxy-socket", os.path.join(tmp, "no-nix-daemon"),
            "--proxy-store", f"local?root={build}",
            "--metrics-listen", "127.0.0.1:0",
        ],
        stdout=log,
        stderr=subprocess.STDOUT,
        env=nix_env(os.path.join(tmp, "daemon-home")),
    )
    try:
        ports = testdaemon.bound(proc, log, time.monotonic() + SETUP_S)
        if ports is None:
            print(f"SETUP FAIL: no startup line with the bound ports; daemon exit {proc.poll()}")
            return 2
        port, mport = ports
        verdicts = []
        # printMissing then sorts the derivations to build on the grpc store,
        # which has none of them, and `nix path-info` sorts, then looks up
        # each path: one QueryPathInfos for all, not one per absent path.
        work = os.path.join(tmp, "toposort")
        os.makedirs(work)
        before = calls(mport, "QueryPathInfos")
        run = subprocess.run(
            [client, plugin, f"127.0.0.1:{port}", build, work, "toposort", str(BUILT)],
            env=nix_env(work), stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, timeout=CLIENT_S,
        )
        made = calls(mport, "QueryPathInfos") - before
        ok = run.returncode == 0 and made <= 1
        print(f"--- toposort: exit {run.returncode}, {made:.0f} QueryPathInfos ---\n{run.stdout}")
        if run.returncode == 77:
            return 77
        print(f"{'PASS' if ok else 'FAIL'}: toposort, {made:.0f} QueryPathInfos (at most 1)")
        verdicts.append(ok)
        for name, limit, substituters, scenario in (
            ("chain", -(-CHAIN // BUDGET) + 1, None, ("chain", str(CHAIN))),
            ("built", 1, None, ("built", str(BUILT))),
            ("deep", 1, None, ("deep", "5", str(BUDGET + 1))),
            ("mixed", None, None, ("mixed",)),
            ("mixed-own", None, "", ("mixed",)),
            ("nosubst", None, None, ("nosubst",)),
            ("probe", None, None, ("probe",)),
            ("toposort-probe", None, None, ("toposort-probe",)),
        ):
            work = os.path.join(tmp, name)
            os.makedirs(work)
            env = nix_env(work) if substituters is None else nix_env(work, substituters)
            before = calls(mport)
            run = subprocess.run(
                [client, plugin, f"127.0.0.1:{port}", build, work, *scenario],
                env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, timeout=CLIENT_S,
            )
            made = calls(mport) - before
            print(f"--- {name}: exit {run.returncode}, {made:.0f} QueryValidPaths ---\n{run.stdout}")
            if run.returncode == 77:
                print("SKIP: this Nix has no queryMissing with an evaluation store")
                return 77
            if run.returncode not in (0, 1):
                print(f"SETUP FAIL: {name} client exited with {run.returncode}")
                return 2
            ok = run.returncode == 0 and (limit is None or made <= limit)
            bound_text = "" if limit is None else f" (at most {limit})"
            print(f"{'PASS' if ok else 'FAIL'}: {name}, {made:.0f} QueryValidPaths{bound_text}")
            verdicts.append(ok)
        if all(verdicts):
            print("PASS")
            return 0
        log.seek(0)
        print(f"FAIL\n--- daemon log ---\n{log.read()}")
        return 1
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()


if __name__ == "__main__":
    sys.exit(main())
