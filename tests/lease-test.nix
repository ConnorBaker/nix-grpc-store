# NixOS VM test of a chain of time-bounded clients through a Tribuchet
# farm's path: contrib/build-client.sh on a client, its nix-daemon and
# grpc:// build hook, this scheduler on the hub, the hub's nix-daemon,
# Tribuchet's `tribuchet attach` and hub, and a Tribuchet worker with one
# slot.
#
# The chain's clients run 45 s each (CLIENT_S) and each next one reaches
# the scheduler 20 s after the last left (a `nix` that starts every build
# but the first that late), so its 100 s build has no client for 20 s at
# a time. Its first gap comes while the build waits in the hub's queue
# behind two others (`hold1` running, `hold2` staged on the worker as its
# one build ahead), and the slot frees in the gap; its second while it
# runs and builds of the hub's own, one after another, wait for the
# slot. Each client declares a linger (`--option grpc-linger 600`): the
# scheduler keeps the build between clients, under the chain's lease
# (`grpc-lease` in NIX_CONFIG), so its attach stays and the hub keeps it,
# queued in its place and then running, and it is never dropped, started
# once and runs once, through a hub restart in the second gap (the
# attach queues it again in the restarted hub); each gap is checked to
# last 14 s or more, the second by the follow after the second client's
# end. The chain then ends its lease (a Release, which the scheduler
# answers). Without the linger the scheduler revokes the build at each gap, which kills its
# attach: the hub drops it from its queue when its slot frees ("dropping
# queued build: no client attached"), then
# gives it up for the waiting builds (the hub's rule, Tribuchet's
# hub/tests/clientless_chain.rs), and the chain never gets it. The test
# notes each failure and goes on, so a run without the linger shows
# both.
#
# scenario = "queue" gives the hub's nix-grpc-store builder one job, so
# that a chain's build waits in the scheduler's own queue: (A) its first
# link leaves while a blocker holds the slot and a rival waits behind it;
# the slot frees in the gap and goes to the rival, since nobody is there
# to take the chain's; the next link is back and gets the next slot
# ahead of a build that came in the gap, where without the linger the
# chain's build is forgotten and comes back behind that one. (B) An
# untrusted user's chain declares a lease that the client's nix-daemon
# drops: its build is kept for no lease, only for the scheduler's
# reconnect grace (10 s), which every broken stream gets, and its next
# link follows it within that. (C) A trusted chain
# stopped (systemctl stop) while its build runs releases its lease at
# once, instead of leaving the build kept; the revoke is timed from the
# stop. Its lease id is on no command line (`ps`), and the scheduler logs
# only its first 8 characters. (D) The release reaches the scheduler
# before the link has left it (a race of the release with the leave): the
# build is revoked as the link leaves, not kept; its report shows only
# the lease id's first 8 characters.
{
  # Tribuchet's package and NixOS module.
  tribuchet,
  nixosModule,
  # Tribuchet's nix/tests/cancel.nix: a build that logs for a while.
  slowBuild,
  # nix-grpc-store's source.
  nixGrpcStore,
  # Its unit tests run as it builds; a mutant's are expected to fail.
  unitTests ? true,
  # "chain" (the hub's queue and a running build) or "queue" (above).
  scenario ? "chain",
}:
{ pkgs, lib, ... }:
let
  build = name: seconds: ''
    import ${slowBuild} {
      bash = "${pkgs.bash}";
      name = "${name}";
      seconds = ${toString seconds};
    }
  '';
  # Daemon and plugin, against the Nix that loads the plugin.
  nixGrpcStoreFor =
    nix:
    let
      package = (pkgs.callPackage "${nixGrpcStore}/nix/packages" { nixPackages = nix.libs; }).default;
    in
    if unitTests then package else package.overrideAttrs { doCheck = false; };
  gapS = 20;
in
{
  name = "tribuchet-lease" + lib.optionalString (scenario == "queue") "-queue";
  defaults.documentation.enable = false;

  nodes = {
    hub =
      { config, ... }:
      {
        imports = [
          nixosModule
          "${nixGrpcStore}/nixos/server.nix"
        ];
        environment.systemPackages = [ tribuchet ];
        networking.firewall.allowedTCPPorts = [
          7437
          50051
        ];
        virtualisation.writableStore = true;
        virtualisation.memorySize = 2048;
        virtualisation.additionalPaths = [ pkgs.bash ];
        nix.settings = {
          substituters = lib.mkForce [ ];
          max-jobs = 8;
        };
        # Hold the worker's one slot and its one staged build ahead
        # (worker/session.rs, declare_capacity) until the test frees them.
        environment.etc."tt/hold1.nix".text = build "hold1" 600;
        environment.etc."tt/hold2.nix".text = build "hold2" 600;
        environment.etc."tt/after.nix".text = ''
          { i }:
          import ${slowBuild} {
            bash = "${pkgs.bash}";
            name = "after-''${toString i}";
            seconds = 10;
          }
        '';
        services.tribuchet-hub = {
          enable = true;
          externalBuilders = {
            enable = true;
            systems = [ "x86_64-linux" ];
          };
          package = tribuchet;
        };
        systemd.sockets.tribuchet-hub.wantedBy = lib.mkForce [ ];
        systemd.services.tribuchet-hub.wantedBy = lib.mkForce [ ];
        # The scheduler and its only builder, on the hub's host.
        services.nix-grpc-daemon = {
          enable = true;
          package = nixGrpcStoreFor config.nix.package;
          listen = "[::]:50051";
          idleTimeout = null;
          trustClients = true;
          workerName = "hub";
          minFree = "0";
          maxJobs = lib.mkIf (scenario == "queue") 1;
        };
      };

    worker =
      { ... }:
      {
        environment.systemPackages = [ tribuchet ];
        virtualisation.useNixStoreImage = true;
        virtualisation.writableStore = true;
        virtualisation.memorySize = 2048;
        virtualisation.cores = 2;
        imports = [ nixosModule ];
        services.tribuchet-worker = {
          enable = true;
          package = tribuchet;
          agents = 1;
          # the VM has less than the default 8 GiB to keep back
          agentMemoryReserve = 512 * 1024 * 1024;
          keyFile = "/etc/tribuchet/tls/worker.key";
          settings = {
            hub = "https://hub:7437";
            cert = "/etc/tribuchet/tls/worker.crt";
            ca-cert = "/etc/tribuchet/tls/ca.crt";
            max-jobs = 1;
          };
        };
        systemd.services.tribuchet-worker.wantedBy = lib.mkForce [ ];
      };

    client =
      { config, nodes, ... }:
      {
        imports = [ "${nixGrpcStore}/nixos/client.nix" ];
        # One Nix, so that one build of nix-grpc-store serves both ends.
        nix.package = nodes.hub.nix.package;
        programs.nix-grpc-store = {
          enable = true;
          package = nixGrpcStoreFor config.nix.package;
        };
        nix.distributedBuilds = true;
        nix.buildMachines = [
          {
            hostName = "grpc://hub:50051?insecure=1&connect-timeout=5";
            protocol = null;
            systems = [ "x86_64-linux" ];
            maxJobs = 4;
          }
        ];
        nix.settings = {
          substituters = lib.mkForce [ ];
          experimental-features = [ "nix-command" ];
          # Only a trusted user's grpc-linger reaches the build hook.
          trusted-users = [
            "root"
            "alice"
          ];
        };
        users.users.alice.isNormalUser = true;
        # Not trusted: the client's nix-daemon drops the grpc-linger it sets.
        users.users.bob.isNormalUser = true;
        environment.systemPackages = [
          pkgs.python3
          pkgs.zstd
        ];
        virtualisation.writableStore = true;
        virtualisation.memorySize = 1536;
        virtualisation.additionalPaths = [ pkgs.bash ];
        environment.etc."tt/chain.nix".text = build "chain" 100;
        environment.etc."tt/qchain.nix".text = build "qchain" 15;
        environment.etc."tt/blocker.nix".text = build "blocker" 600;
        environment.etc."tt/rival.nix".text = build "rival" 30;
        environment.etc."tt/late.nix".text = build "late" 10;
        environment.etc."tt/bob.nix".text = build "bob" 120;
        environment.etc."tt/stopme.nix".text = build "stopme" 300;
        environment.etc."tt/racer.nix".text = build "racer" 300;
        environment.etc."tt/build-client.sh".source = ../contrib/build-client.sh;
        environment.etc."tt/gap/nix" = {
          mode = "0755";
          text = ''
            #!${pkgs.runtimeShell}
            # Each build but the first reaches the scheduler ${toString gapS} s late.
            if [ "$1" = build ]; then
              if [ -e "$HOME/gap-started" ]; then sleep ${toString gapS}; fi
              touch "$HOME/gap-started"
            fi
            exec ${config.nix.package}/bin/nix "$@"
          '';
        };
      };
  };

  testScript = ''
    import datetime
    import json
    import re
    import shlex
    import time

    chain = "/home/alice/chain"
    failures = []

    def check(ok, what):
        if not ok:
            failures.append(what)
            print(f"FAILED: {what}")

    ansi = re.compile(r"\x1b\[[0-9;]*m")

    def lines(machine, unit, *needles):
        # Filtered in the VM: a whole journal read through the driver's
        # shell can come back with chunks missing (the worker's "build
        # resumed" once read 0 here). The hub's and the worker's lines
        # colour each field's name and `=`; stripped first.
        grep = "".join(f" | grep -F -e {shlex.quote(n)}" for n in needles)
        return machine.succeed(
            f"journalctl -a -u {unit} --no-pager -o cat | sed 's/\\x1b\\[[0-9;]*m//g'{grep} || true"
        ).splitlines()

    def count(machine, unit, needle):
        return len(lines(machine, unit, needle))

    def running(name):
        # [g]: pgrep must not match the shell that runs it
        return f"pgrep -f '{name}-marker-runnin[g]'"

    def clients_done():
        return int(client.succeed(f"wc -l < {chain}/clients.jsonl").strip())

    def restart_hub():
        registered = count(hub, "tribuchet-hub", "worker registered")
        hub.succeed("systemctl restart tribuchet-hub")
        hub.wait_until_succeeds(
            "[ $(journalctl -a -u tribuchet-hub --no-pager | grep -c 'worker registered')"
            f" -gt {registered} ]",
            timeout=90,
        )

    def of_chain(needle):
        # lines that name one of the chain's builds by id only
        ids = re.findall(r"id=(\S+)", "\n".join(lines(hub, "tribuchet-hub", "queueing build", "tt-chain")))
        return sum(1 for f in lines(hub, "tribuchet-hub", needle) if any(f"id={i}" in f for i in ids))

    def dropped():
        return of_chain("dropping queued build")

    def started():
        return len(lines(hub, "tribuchet-hub", "dispatching build", "tt-chain", "resumed=false"))

    def field(pattern, line):
        found = re.search(pattern, line)
        assert found, f"no {pattern} in {line}"
        return found.group(1)

    def epoch(line):
        # the ts= of a nix-grpc-daemon line, in seconds
        stamp = field(r"ts=(\S+)", line)
        return datetime.datetime.strptime(stamp, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=datetime.timezone.utc).timestamp()

    def wait_for(cond, timeout, what):
        deadline = time.monotonic() + timeout
        while not cond():
            assert time.monotonic() < deadline, f"timed out waiting for {what}"
            time.sleep(1)

    def seen():
        followed = "\n".join(lines(hub, "nix-grpc-daemon", "event=linger_followed", "tt-chain"))
        return {
            "queued": len(lines(hub, "tribuchet-hub", "queueing build", "tt-chain")),
            "dropped": dropped(),
            "started": started(),
            "resumed": count(worker, "tribuchet-worker", "build resumed"),
            "given_up": of_chain("want the slot of a build with no client"),
            "lingering": len(lines(hub, "nix-grpc-daemon", "event=lingering", "tt-chain")),
            "followed_idle_s": [int(s) for s in re.findall(r"idle_s=(\d+)", followed)],
            "revoked": len(lines(hub, "nix-grpc-daemon", "event=revoked_running", "tt-chain")),
        }

    start_all()
    hub.wait_for_unit("multi-user.target")
    worker.wait_for_unit("multi-user.target")
    client.wait_for_unit("multi-user.target")

    with subtest("certificates and registration"):
        hub.succeed("tribuchet ca init --dir /root/ca")
        hub.succeed("tribuchet ca issue hub --dir /root/ca")
        hub.succeed("tribuchet ca issue worker --dir /root/ca")
        hub.succeed("mkdir -p /etc/tribuchet/ca")
        hub.succeed("cp /root/ca/hub.crt /root/ca/hub.key /root/ca/ca.crt /etc/tribuchet/ca/")
        worker.succeed("mkdir -p /etc/tribuchet/tls")
        for f in ["worker.crt", "worker.key", "ca.crt"]:
            pem = hub.succeed(f"cat /root/ca/{f}")
            worker.succeed(f"cat > /etc/tribuchet/tls/{f} << 'PEMEOF'\n{pem}PEMEOF")
        worker.succeed("chmod 600 /etc/tribuchet/tls/worker.key")
        hub.succeed("systemctl start tribuchet-hub.socket tribuchet-hub")
        worker.succeed("systemctl start tribuchet-worker")
        hub.wait_until_succeeds("journalctl -u tribuchet-hub | grep -q 'worker registered'")
        hub.wait_for_unit("nix-grpc-daemon.service")
        hub.wait_for_open_port(50051)

  ''
  + (
    if scenario == "queue" then
      ''
        def of(name, *needles):
            return len(lines(hub, "nix-grpc-daemon", *needles, f"tt-{name}"))

        def first(unit, machine, needle, name):
            found = [i for i, line in enumerate(lines(machine, unit, needle)) if f"tt-{name}" in line]
            return found[0] if found else None

        def chain_of(user, name, client_s, extra="", gap=False):
            path = "/etc/tt/gap:/run/current-system/sw/bin" if gap else "/run/current-system/sw/bin"
            drv = client.succeed(f"nix-instantiate /etc/tt/{name}.nix").strip()
            client.succeed(
                f"systemd-run --unit={name}-chain --uid={user} --setenv=HOME=/home/{user} --setenv=PATH={path}"
                f" --setenv=CLIENT_S={client_s} {extra} bash /etc/tt/build-client.sh /home/{user}/{name} {drv}"
            )
            return f"/home/{user}/{name}"

        def root_build(name):
            # remote only: it takes the scheduler's slot, never the client's own
            client.succeed(f"systemd-run --unit={name} nix-build /etc/tt/{name}.nix --no-out-link --max-jobs 0")

        def settle(cond, timeout):
            deadline = time.monotonic() + timeout
            while not cond() and time.monotonic() < deadline:
                time.sleep(1)

        with subtest("(A) a gap while the chain's build waits in the scheduler's own queue"):
            root_build("blocker")
            worker.wait_until_succeeds(running("blocker"), timeout=180)
            qchain = chain_of("alice", "qchain", 60, "--setenv=QUEUE_S=0 --setenv=RUNNING_S=240", gap=True)
            client.wait_until_succeeds("test -e /home/alice/gap-started", timeout=60)
            time.sleep(10)
            root_build("rival")
            client.wait_until_succeeds(f"test -s {qchain}/clients.jsonl", timeout=120)
            # kept (event=lingering), or, without the linger, forgotten without a word
            settle(lambda: of("qchain", "event=lingering") > 0, 10)
            kept = lines(hub, "nix-grpc-daemon", "event=lingering", "tt-qchain")
            print(f"in the gap: {kept}")
            check(any("state=queued" in k for k in kept), f"the chain's build was not kept in the scheduler's queue: {kept}")
            # free the slot in the gap: the blocker's client and builder go
            client.succeed("systemctl kill --signal=SIGKILL blocker")
            worker.execute("pkill -KILL -f 'blocker-marker-runnin[g]'")
            time.sleep(3)
            root_build("late")
            wait_for(lambda: first("tribuchet-hub", hub, "queueing build", "rival") is not None, 60, "the rival to get the freed slot")
            check(
                first("tribuchet-hub", hub, "queueing build", "qchain") is None,
                "the chain's build was placed with no client there to take it",
            )
            client.wait_until_succeeds(f"test -s {qchain}/status.json", timeout=240)
            status = json.loads(client.succeed(f"cat {qchain}/status.json"))
            q, late = first("tribuchet-hub", hub, "queueing build", "qchain"), first("tribuchet-hub", hub, "queueing build", "late")
            followed = re.findall(r"idle_s=(\d+)", "\n".join(lines(hub, "nix-grpc-daemon", "event=linger_followed", "tt-qchain")))
            print(f"status {status}; queueing order: qchain {q}, late {late}; followed {followed}")
            check(status["exit"] == 0, f"the chain did not get its build: {status}")
            check(followed and int(followed[0]) >= 14, f"the next link did not follow the kept build after the gap: {followed}")
            check(q is not None and (late is None or q < late), f"the chain's build lost its place to a build that came in the gap: qchain {q}, late {late}")
            client.wait_until_succeeds("! systemctl is-active -q late", timeout=120)

        with subtest("(B) an untrusted user's linger is dropped: its build is kept for the reconnect grace only"):
            bob = chain_of("bob", "bob", 20, "--setenv=QUEUE_S=0 --setenv=RUNNING_S=25 --setenv=RELEASE_S=10")
            client.wait_until_succeeds(f"test -s {bob}/status.json", timeout=120)
            dropped_note = ansi.sub("", client.succeed(f"cat {bob}/client-1.log"))
            print(f"bob: lingering {of('bob', 'event=lingering')}, revoked {of('bob', 'event=revoked_running')}")
            check(
                "ignoring the client-specified setting 'grpc-linger'" in dropped_note
                and "ignoring the client-specified setting 'grpc-lease'" in dropped_note,
                "the client's nix-daemon did not drop bob's grpc-linger and grpc-lease",
            )
            check(of("bob", "event=lingering") == 0, "an untrusted user's build was kept for a linger")
            idle = [int(i) for i in re.findall(r"idle_s=(\d+)", "\n".join(lines(hub, "nix-grpc-daemon", "event=linger_followed", "tt-bob")))]
            print(f"bob: followed after {idle} s")
            # Links follow each other at once, so the next one comes within
            # the grace: a build revoked as its link left would not be
            # followed again at all.
            check(idle, "an untrusted user's build was not kept for the reconnect grace: not followed again")
            check(all(i <= 10 for i in idle), f"an untrusted user's build was kept past the reconnect grace: followed after {idle} s")

        with subtest("(C) a trusted chain stopped while its build runs releases it at once"):
            stop = chain_of("alice", "stopme", 120, "--setenv=QUEUE_S=0 --setenv=RUNNING_S=600 --setenv=RELEASE_S=15")
            worker.wait_until_succeeds(running("stopme"), timeout=120)
            lease = client.succeed(f"cat {stop}/lease").strip()
            # the id is in the links' environment, on no command line
            procs = [p for p in client.succeed("ps -eo args").splitlines() if lease in p]
            check(not procs, f"the lease id is on a command line: {procs}")
            # timed from the stop itself, not from when systemctl returns
            stopped_at = time.monotonic()
            client.succeed("systemctl stop --no-block stopme-chain")
            wait_for(lambda: of("stopme", "event=revoked_running") > 0, 90, "the stopped chain's build to be revoked")
            took = time.monotonic() - stopped_at
            client.wait_until_succeeds(f"test -s {stop}/status.json", timeout=60)
            status = json.loads(client.succeed(f"cat {stop}/status.json"))
            released = lines(hub, "nix-grpc-daemon", "event=released", f"lease={lease[:8]}")
            check(not lines(hub, "nix-grpc-daemon", lease), "the scheduler logged the lease id in full")
            print(f"stopme: revoked {took:.1f} s after the stop; released {released}; status {status}")
            check(took < 20, f"the stopped chain's build was revoked {took:.1f} s after the stop, not at once")
            check(any("builds=1" in r for r in released), f"the scheduler did not release the chain's lease on its build: {released}")
            check(
                status["exit"] == 143 and status["release"]["acknowledged"] and not status["release"]["interrupted"],
                f"the stop was not recorded with its answered release: {status}",
            )

        with subtest("(D) the release reaches the scheduler before the link has left: let go as it leaves"):
            race = chain_of("alice", "racer", 300, "--setenv=QUEUE_S=0 --setenv=RUNNING_S=600")
            worker.wait_until_succeeds(running("racer"), timeout=120)
            lease = client.succeed(f"cat {race}/lease").strip()
            # the release, while the chain's link still follows the build
            answer = client.succeed(
                f"NIX_CONFIG='grpc-lease = {lease}'"
                " nix store info --store 'grpc://hub:50051?insecure=1&connect-timeout=5&release-lease=true' 2>&1"
            )
            print(f"racer: {answer}")
            check(f"released lease {lease[:8]} on" in answer and ": 1 builds" in answer, f"the release was not answered for the build: {answer}")
            check(lease not in answer, f"the release report shows the whole lease id: {answer}")
            check(of("racer", "event=revoked_running") == 0, "the release revoked a build its link still followed")
            # the link leaves without releasing: SIGKILL, no trap
            left_at = time.monotonic()
            client.succeed("systemctl kill --signal=SIGKILL racer-chain")
            wait_for(lambda: of("racer", "event=revoked_running") > 0, 90, "the raced build to be revoked")
            took = time.monotonic() - left_at
            print(f"racer: revoked {took:.1f} s after its link was killed; lingering {of('racer', 'event=lingering')}")
            check(of("racer", "event=lingering") == 0, "the released lease kept the build once its link left")
            check(took < 30, f"the raced build was revoked {took:.1f} s after its link was killed")
            assert not failures, "; ".join(failures)
      ''
    else
      ''
        with subtest("a gap while the chain's build is queued, and its slot frees in the gap"):
            hub.succeed("systemd-run --unit=hold1 nix-build /etc/tt/hold1.nix --no-out-link")
            worker.wait_until_succeeds(running("hold1"), timeout=180)
            hub.succeed("systemd-run --unit=hold2 nix-build /etc/tt/hold2.nix --no-out-link")
            hub.wait_until_succeeds(
                "journalctl -u tribuchet-hub | grep 'dispatching build' | grep -q tt-hold2", timeout=120
            )
            drv = client.succeed("nix-instantiate /etc/tt/chain.nix").strip()
            client.succeed(
                "systemd-run --unit=chain --uid=alice --setenv=HOME=/home/alice"
                " --setenv=PATH=/etc/tt/gap:/run/current-system/sw/bin"
                " --setenv=CLIENT_S=45 --setenv=QUEUE_S=0 --setenv=RUNNING_S=240"
                f" bash /etc/tt/build-client.sh {chain} {drv}"
            )
            hub.wait_until_succeeds(
                "journalctl -u tribuchet-hub | grep 'queueing build' | grep -q tt-chain", timeout=180
            )
            client.wait_until_succeeds(f"test -s {chain}/clients.jsonl", timeout=120)
            # the first client is gone: the scheduler has kept the build, or
            # (without the linger) revoked it
            hub.wait_until_succeeds(
                "journalctl -u nix-grpc-daemon | grep -e event=lingering -e event=revoked_running | grep -q tt-chain",
                timeout=30,
            )
            time.sleep(5)
            before = seen()
            print(f"in the first gap: {before}")
            check(
                before["queued"] == 1 and before["started"] == 0 and not before["followed_idle_s"],
                f"the chain's build was not waiting in the hub's queue in the gap: {before}",
            )
            # free the slot in the gap: hold1's client and builder go, hold2
            # starts, and the hub sends the head of its queue in its place
            hub.succeed("systemctl kill --signal=SIGKILL hold1")
            worker.execute("pkill -KILL -f 'hold1-marker-runnin[g]'")
            wait_for(lambda: started() + dropped() > 0, 60, "the chain's build to be dispatched or dropped")
            gap1 = seen()
            print(f"at the first slot after the first gap: {gap1}")
            check(gap1["dropped"] == 0, f"the queued build was dropped in the gap: {gap1}")
            check(gap1["started"] == 1, f"the queued build did not get the slot that freed in the gap: {gap1}")
            check(not gap1["followed_idle_s"], f"the next client was back before the slot freed, no gap tested: {gap1}")
            # and the chain's build has the slot
            hub.succeed("systemctl kill --signal=SIGKILL hold2")
            worker.wait_until_succeeds("pkill -KILL -f 'hold2-marker-runnin[g]'", timeout=30)

        with subtest("a gap while it runs and other builds wait for its slot"):
            hub.succeed(
                "systemd-run --unit=after bash -lc"
                " 'for i in $(seq 1 40); do nix-build /etc/tt/after.nix --arg i $i --no-out-link"
                " > /tmp/after-$i.path 2> /tmp/after-$i.log; done'"
            )
            hub.wait_until_succeeds(
                "journalctl -u tribuchet-hub | grep 'queueing build' | grep -q tt-after-1", timeout=60
            )
            client.wait_until_succeeds(
                f"[ $(wc -l < {chain}/clients.jsonl) -ge 2 ] || test -s {chain}/status.json", timeout=180
            )
            if client.execute(f"test -s {chain}/status.json")[0] != 0:
                # the second client is gone, and a build waits for the slot
                check(hub.execute("test -s /tmp/after-1.path")[0] != 0, "no build waited in the second gap")
                restart_hub()
            else:
                check(False, f"the chain ended after {clients_done()} clients, before a second gap")
            client.wait_until_succeeds(f"test -s {chain}/status.json", timeout=420)
            status = json.loads(client.succeed(f"cat {chain}/status.json"))
            clients = client.succeed(f"cat {chain}/clients.jsonl")
            evidence = seen()
            print(f"status {status}\nclients\n{clients}evidence {evidence}")
            check(status["exit"] == 0, f"the chain did not get its build: {status}")
            check(status["clients"] >= 2, f"the build did not outlive a client: {status}")
            path_info = json.loads(client.succeed(f"cat {chain}/path-info.json"))
            check(path_info and all(p["path_info_exit"] == 0 for p in path_info), f"outputs not valid: {path_info}")
            # an attach resubmits to a restarted hub, so `queued` counts that too
            check(evidence["dropped"] == 0, f"dropped from the queue: {evidence}")
            check(evidence["started"] == 1, f"started anew: {evidence}")
            check(evidence["given_up"] == 0, f"given up: {evidence}")
            check(evidence["lingering"] >= 2 and evidence["revoked"] == 0, f"not kept: {evidence}")
            check(max(evidence["followed_idle_s"], default=0) >= 14, f"no gap of 14 s or more: {evidence}")
            # gap 2 itself: a follow of the build while the third client ran,
            # 14 s or more after the second left (the clocks agree to a second)
            links = [json.loads(line) for line in clients.splitlines()]
            follows = [
                (epoch(line), int(field(r"idle_s=(\d+)", line)))
                for line in lines(hub, "nix-grpc-daemon", "event=linger_followed", "tt-chain")
            ]
            gap2 = [
                idle for at, idle in follows
                if len(links) >= 3 and links[1]["end"] - 2 <= at <= links[2]["end"] + 2
            ]
            print(f"links {[(c['start'], c['end']) for c in links]}; follows {follows}; gap 2 {gap2}")
            check(len(gap2) == 1 and gap2[0] >= 14, f"the third client did not follow the build after a second gap of 14 s or more: {gap2}")
            check(status["release"]["acknowledged"], f"the chain's release was not answered: {status}")
            check(evidence["resumed"] >= 1, f"the hub restart was not ridden out: {evidence}")
            assert not failures, "; ".join(failures)
      ''
  );
}
