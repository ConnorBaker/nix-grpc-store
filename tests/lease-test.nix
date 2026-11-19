# NixOS VM test of a chain of time-bounded clients through a Tribuchet
# farm's path: contrib/build-client.sh on a client, its nix-daemon and
# grpc:// build hook, this scheduler on the hub, the hub's nix-daemon,
# Tribuchet's `tribuchet attach` and hub, and a Tribuchet worker with one
# slot.
#
# The test ends each of the chain's clients as its CLIENT_S bound would
# (timeout's SIGALRM: it exits 124 and the next follows), and holds the
# next one in its gap before it reaches the scheduler (a `nix` that waits
# for the test's word before every build but the first) until the gap has
# had what it is for and has outlasted the scheduler's 10 s reconnect
# grace; the builds the test acts on run until it releases them. The
# chain's build has no client in each gap. Its first gap comes while the
# build waits in the hub's queue
# behind two others (`hold1` running, `hold2` staged on the worker as its
# one build ahead), and the slot frees in the gap; its second while it
# runs and builds of the hub's own, one after another, wait for the
# slot. Each client declares a linger (`--option grpc-linger 600`): the
# scheduler keeps the build between clients, under the chain's lease
# (`grpc-lease` in NIX_CONFIG), so its attach stays and the hub keeps it,
# queued in its place and then running, and it is never dropped, started
# once and runs once, through a hub restart in the second gap (the
# attach queues it again in the restarted hub); each follow is checked to
# come after a gap longer than the grace, so the linger kept the build.
# The chain then ends its lease (a Release, which the scheduler
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
# reconnect grace, which every broken stream gets: with its next link
# held in its gap, the grace ends (linger_expired) before any revoke.
# (C) A trusted chain
# stopped (systemctl stop) while its build runs releases its lease at
# once, instead of leaving the build kept: its revoke comes from the
# release, with no end of a linger or of the reconnect grace before it.
# Its lease id is on no command line (`ps`), and the scheduler logs
# only its first 8 characters. (D) The release reaches the scheduler
# before the link has left it (a race of the release with the leave): the
# build is revoked as the link leaves, not kept (no linger or grace ends
# before the revoke); its report shows only
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
  # As slowBuild's builds, but it logs about once a second until the test
  # sends its builder SIGUSR1: the inner shell (a read of a pipe it holds
  # open itself, paced by read -t, which takes no CPU) dies of it, and the
  # builder, which ignores it, writes $out.
  held = name: ''
    derivation {
      name = "tt-${name}";
      system = "x86_64-linux";
      builder = builtins.storePath "${pkgs.bash}" + "/bin/bash";
      args = [ "-c" "trap : USR1; echo ${name}-marker-running >&2; : | { exec 3<>/proc/self/fd/0; while :; do read -t 1 -u 3 x; echo still running >&2; done; }; echo ${name}-done > $out" ];
    }
  '';
  # Daemon and plugin, against the Nix that loads the plugin.
  nixGrpcStoreFor =
    nix:
    let
      package = (pkgs.callPackage "${nixGrpcStore}/nix/packages" { nixPackages = nix.libs; }).default;
    in
    if unitTests then package else package.overrideAttrs { doCheck = false; };
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
        environment.etc."tt/hold1.nix".text = held "hold1";
        environment.etc."tt/hold2.nix".text = held "hold2";
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
          maxJobs = lib.mkIf (scenario == "queue") 1;
          # event=want: the test waits for a build to be in its queue.
          logLevel = lib.mkIf (scenario == "queue") "debug";
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
        environment.etc."tt/chain.nix".text = held "chain";
        # These end by themselves; nothing checks how long they run.
        environment.etc."tt/qchain.nix".text = build "qchain" 15;
        environment.etc."tt/late.nix".text = build "late" 10;
        environment.etc."tt/bob.nix".text = held "bob";
        environment.etc."tt/blocker.nix".text = held "blocker";
        environment.etc."tt/rival.nix".text = held "rival";
        environment.etc."tt/stopme.nix".text = held "stopme";
        environment.etc."tt/racer.nix".text = held "racer";
        environment.etc."tt/build-client.sh".source = ../contrib/build-client.sh;
        environment.etc."tt/gap/nix" = {
          mode = "0755";
          text = ''
            #!${pkgs.runtimeShell}
            # Each build but the first waits in its gap for the test's word:
            # a line it writes into the fifo $HOME/gap.
            if [ "$1" = build ]; then
              if [ -e "$HOME/gap-started" ]; then read -r _ < "$HOME/gap"; else mkfifo "$HOME/gap"; fi
              touch "$HOME/gap-started"
            fi
            exec ${config.nix.package}/bin/nix "$@"
          '';
        };
      };
  };

  testScript = ''
    ${import ./lib/waits.nix}
    import json
    import re

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

    def release(name, unit):
        # a held build ends: its builder takes SIGUSR1 (the leading space:
        # `chain` must not match `qchain`); `unit` on the client waits for it
        wait_succeeds(worker, f"pkill -USR1 -f ' {name}-marker-runnin[g]'", clients=[(client, unit)])

    def end_link(path, unit, n, user="alice"):
        # The running link ends as its CLIENT_S bound would end it, by
        # timeout's SIGALRM: it exits 124, and the next one follows into its
        # gap. Then its record is written.
        client.succeed(f"pkill -ALRM -u {user} -x timeout")
        wait_succeeds(client, f"[ $(wc -l < {path}/clients.jsonl) -ge {n} ]", clients=[(client, unit)])

    # The scheduler's reconnect grace (src/daemon/dispatcher.hh,
    # defaultDepartedGraceMs), which every broken stream gets.
    grace_s = 10
    gap_word = "import os, sys; fd = os.open(sys.argv[1], os.O_WRONLY | os.O_NONBLOCK); os.write(fd, b'go\\n')"

    def end_gap(home, unit, began):
        # A lower bound: the gap outlasts the grace, so that only the
        # linger can have kept the build. Then the next link, which reads
        # $HOME/gap in its gap, gets a line and goes on: the open (no
        # reader: ENXIO) succeeds once it waits there.
        time.sleep(max(0.0, grace_s + 5 - (time.monotonic() - began)))
        wait_succeeds(client, f"python3 -c {shlex.quote(gap_word)} {home}/gap", clients=[(client, unit)])

    def restart_hub():
        registered = count(hub, "tribuchet-hub", "worker registered")
        hub.succeed("systemctl restart tribuchet-hub")
        wait_journal(hub, "tribuchet-hub", "worker registered", more_than=registered, clients=[(hub, "tribuchet-hub"), (worker, "tribuchet-worker")])

    def of_chain(needle):
        # lines that name one of the chain's builds by id only
        ids = re.findall(r"id=(\S+)", "\n".join(lines(hub, "tribuchet-hub", "queueing build", "tt-chain")))
        return sum(1 for f in lines(hub, "tribuchet-hub", needle) if any(f"id={i}" in f for i in ids))

    def dropped():
        return of_chain("dropping queued build")

    def started():
        return len(lines(hub, "tribuchet-hub", "dispatching build", "tt-chain", "resumed=false"))

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
    for m in [hub, worker, client]:
        wait_unit(m, "multi-user.target")

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
        wait_succeeds(hub, "journalctl -u tribuchet-hub | grep -q 'worker registered'", clients=[(worker, "tribuchet-worker")])
        wait_unit(hub, "nix-grpc-daemon.service")
        wait_succeeds(hub, "nc -z localhost 50051")

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

        def revoked_by_release(name):
            # Its revoke came with no end of a lease's linger or of the
            # reconnect grace before it, which would log linger_expired
            # first (src/daemon/dispatcher.cc, expireLingersLocked).
            seq = lines(hub, "nix-grpc-daemon", f"tt-{name}")
            at = next((i for i, line in enumerate(seq) if "event=revoked_running " in line), None)
            return at is not None and not any("event=linger_expired " in line for line in seq[:at])

        # Links that run until the test ends them (CLIENT_S at its most, a
        # product input), builds with no time limit (RUNNING_S=0, Nix's
        # `--timeout 0`), a budget for following that no run reaches
        # (QUEUE_S, a day), and a release asked for as long as
        # build-client.sh allows (RELEASE_S at its most, 600): none decides
        # an outcome.
        unbounded = "--setenv=QUEUE_S=86400 --setenv=RUNNING_S=0 --setenv=RELEASE_S=600"

        with subtest("(A) a gap while the chain's build waits in the scheduler's own queue"):
            root_build("blocker")
            wait_succeeds(worker, running("blocker"), clients=[(client, "blocker")])
            qchain = chain_of("alice", "qchain", 600, unbounded, gap=True)
            # the chain's build in the scheduler's queue, then the rival behind it
            wait_journal(hub, "nix-grpc-daemon", "event=want ", "tt-qchain", clients=[(client, "qchain-chain")])
            root_build("rival")
            wait_journal(hub, "nix-grpc-daemon", "event=want ", "tt-rival", clients=[(client, "rival")])
            end_link(qchain, "qchain-chain", 1)
            # kept (event=lingering); without the linger, forgotten without
            # a word, and the test waits here
            wait_journal(hub, "nix-grpc-daemon", "event=lingering ", "tt-qchain", clients=[(client, "qchain-chain")])
            gap = time.monotonic()
            kept = lines(hub, "nix-grpc-daemon", "event=lingering", "tt-qchain")
            print(f"in the gap: {kept}")
            check(any("state=queued" in k for k in kept), f"the chain's build was not kept in the scheduler's queue: {kept}")
            # free the slot in the gap: the blocker's client and builder go
            client.succeed("systemctl kill --signal=SIGKILL blocker")
            worker.execute("pkill -KILL -f 'blocker-marker-runnin[g]'", timeout=None)
            wait_until("the rival to get the freed slot", lambda: first("tribuchet-hub", hub, "queueing build", "rival") is not None, clients=[(client, "rival")])
            # a build that comes in the gap, once the slot is gone, into the
            # scheduler's queue
            root_build("late")
            wait_journal(hub, "nix-grpc-daemon", "event=want ", "tt-late", clients=[(client, "late")])
            check(
                first("tribuchet-hub", hub, "queueing build", "qchain") is None,
                "the chain's build was placed with no client there to take it",
            )
            end_gap("/home/alice", "qchain-chain", gap)
            wait_journal(hub, "nix-grpc-daemon", "event=linger_followed ", "tt-qchain", clients=[(client, "qchain-chain")])
            # the next slot frees with the chain's link back
            release("rival", "rival")
            wait_succeeds(client, f"test -s {qchain}/status.json", clients=[(client, "qchain-chain")])
            status = json.loads(client.succeed(f"cat {qchain}/status.json"))
            q, late = first("tribuchet-hub", hub, "queueing build", "qchain"), first("tribuchet-hub", hub, "queueing build", "late")
            followed = re.findall(r"idle_s=(\d+)", "\n".join(lines(hub, "nix-grpc-daemon", "event=linger_followed", "tt-qchain")))
            print(f"status {status}; queueing order: qchain {q}, late {late}; followed {followed}")
            check(status["exit"] == 0, f"the chain did not get its build: {status}")
            check(followed and int(followed[0]) >= grace_s + 4, f"the next link did not follow the kept build after the gap: {followed}")
            check(q is not None and (late is None or q < late), f"the chain's build lost its place to a build that came in the gap: qchain {q}, late {late}")
            wait_until("late ended", lambda: ended(client, "late"))

        with subtest("(B) an untrusted user's linger is dropped: its build is kept for the reconnect grace only"):
            bob = chain_of("bob", "bob", 600, unbounded, gap=True)
            wait_succeeds(worker, running("bob"), clients=[(client, "bob-chain")])
            end_link(bob, "bob-chain", 1, user="bob")
            # The grace ends with the next link held in its gap: the build
            # was kept (not revoked as the link left), then let go with no
            # lease's linger (no event=lingering), by the grace's end.
            ends = "event=(linger_expired|revoked_running) "
            wait_journal(hub, "nix-grpc-daemon", ends, "tt-bob", clients=[(client, "bob-chain")])
            seq = [line for line in lines(hub, "nix-grpc-daemon", "tt-bob") if re.search(ends, line)]
            print(f"bob: {seq}")
            check("event=linger_expired " in seq[0], f"an untrusted user's build was revoked as its link left, not kept for the grace: {seq}")
            check(of("bob", "event=lingering") == 0, "an untrusted user's build was kept for a linger")
            dropped_note = ansi.sub("", client.succeed(f"cat {bob}/client-1.log"))
            check(
                "ignoring the client-specified setting 'grpc-linger'" in dropped_note
                and "ignoring the client-specified setting 'grpc-lease'" in dropped_note,
                "the client's nix-daemon did not drop bob's grpc-linger and grpc-lease",
            )
            # the next link builds it again: released until the chain ends
            wait_succeeds(client, f"python3 -c {shlex.quote(gap_word)} /home/bob/gap", clients=[(client, "bob-chain")])

            def bob_done():
                worker.execute("pkill -USR1 -f ' bob-marker-runnin[g]'", timeout=None)
                return client.execute(f"test -s {bob}/status.json", timeout=None)[0] == 0

            wait_until("bob's chain to end", bob_done, clients=[(client, "bob-chain")])

        with subtest("(C) a trusted chain stopped while its build runs releases it at once"):
            # systemd's stop takes as long as the release does
            stop = chain_of("alice", "stopme", 600, f"{unbounded} --property=TimeoutStopSec=infinity")
            wait_succeeds(worker, running("stopme"), clients=[(client, "stopme-chain")])
            lease = client.succeed(f"cat {stop}/lease").strip()
            # the id is in the links' environment, on no command line
            procs = [p for p in client.succeed("ps -eo args").splitlines() if lease in p]
            check(not procs, f"the lease id is on a command line: {procs}")
            stopped_at = time.monotonic()
            client.succeed("systemctl stop --no-block stopme-chain")
            # the chain may end once its release is answered, before the
            # revoke reaches the builder that logs it: the daemon writes the line
            wait_journal(hub, "nix-grpc-daemon", "event=revoked_running ", "tt-stopme", clients=[(hub, "nix-grpc-daemon")])
            took = time.monotonic() - stopped_at
            wait_succeeds(client, f"test -s {stop}/status.json", clients=[(client, "stopme-chain")])
            status = json.loads(client.succeed(f"cat {stop}/status.json"))
            released = lines(hub, "nix-grpc-daemon", "event=released", f"lease={lease[:8]}")
            check(not lines(hub, "nix-grpc-daemon", lease), "the scheduler logged the lease id in full")
            print(f"stopme: revoked {took:.1f} s after the stop; released {released}; status {status}")
            check(revoked_by_release("stopme"), f"the stopped chain's build was revoked once a linger or grace ended, not by the release: {lines(hub, 'nix-grpc-daemon', 'tt-stopme')}")
            check(any("builds=1" in r for r in released), f"the scheduler did not release the chain's lease on its build: {released}")
            check(
                status["exit"] == 143 and status["release"]["acknowledged"] and not status["release"]["interrupted"],
                f"the stop was not recorded with its answered release: {status}",
            )

        with subtest("(D) the release reaches the scheduler before the link has left: let go as it leaves"):
            race = chain_of("alice", "racer", 600, unbounded)
            wait_succeeds(worker, running("racer"), clients=[(client, "racer-chain")])
            lease = client.succeed(f"cat {race}/lease").strip()
            # the release, while the chain's link still follows the build;
            # its call ends at connect-timeout + 10 s (src/client/build.cc,
            # releaseLease): with a day's, that decides nothing
            answer = client.succeed(
                f"NIX_CONFIG='grpc-lease = {lease}'"
                " nix store info --store 'grpc://hub:50051?insecure=1&connect-timeout=86400&release-lease=true' 2>&1"
            )
            print(f"racer: {answer}")
            check(f"released lease {lease[:8]} on" in answer and ": 1 builds" in answer, f"the release was not answered for the build: {answer}")
            check(lease not in answer, f"the release report shows the whole lease id: {answer}")
            check(of("racer", "event=revoked_running") == 0, "the release revoked a build its link still followed")
            # the link leaves without releasing: SIGKILL, no trap
            left_at = time.monotonic()
            client.succeed("systemctl kill --signal=SIGKILL racer-chain")
            # the link is gone: the scheduler is the one to log it
            wait_journal(hub, "nix-grpc-daemon", "event=revoked_running ", "tt-racer", clients=[(hub, "nix-grpc-daemon")])
            took = time.monotonic() - left_at
            print(f"racer: revoked {took:.1f} s after its link was killed; lingering {of('racer', 'event=lingering')}")
            check(of("racer", "event=lingering") == 0, "the released lease kept the build once its link left")
            check(revoked_by_release("racer"), f"the raced build was revoked once a linger or grace ended, not as its link left: {lines(hub, 'nix-grpc-daemon', 'tt-racer')}")
            assert not failures, "; ".join(failures)
      ''
    else
      ''
        with subtest("a gap while the chain's build is queued, and its slot frees in the gap"):
            hub.succeed("systemd-run --unit=hold1 nix-build /etc/tt/hold1.nix --no-out-link")
            wait_succeeds(worker, running("hold1"), clients=[(hub, "hold1")])
            hub.succeed("systemd-run --unit=hold2 nix-build /etc/tt/hold2.nix --no-out-link")
            wait_succeeds(hub, "journalctl -u tribuchet-hub | grep 'dispatching build' | grep -q tt-hold2", clients=[(hub, "hold2")])
            drv = client.succeed("nix-instantiate /etc/tt/chain.nix").strip()
            client.succeed(
                "systemd-run --unit=chain --uid=alice --setenv=HOME=/home/alice"
                " --setenv=PATH=/etc/tt/gap:/run/current-system/sw/bin"
                # links the test ends, no build timeout, a budget no run
                # reaches: see the queue scenario's `unbounded`
                " --setenv=CLIENT_S=600 --setenv=QUEUE_S=86400 --setenv=RUNNING_S=0 --setenv=RELEASE_S=600"
                f" bash /etc/tt/build-client.sh {chain} {drv}"
            )
            wait_succeeds(hub, "journalctl -u tribuchet-hub | grep 'queueing build' | grep -q tt-chain", clients=[(client, "chain")])
            end_link(chain, "chain", 1)
            # the first client is gone: the scheduler has kept the build, or
            # (without the linger) revoked it
            wait_journal(hub, "nix-grpc-daemon", "event=(lingering|revoked_running) ", "tt-chain", clients=[(client, "chain")])
            gap = time.monotonic()
            before = seen()
            print(f"in the first gap: {before}")
            check(
                before["queued"] == 1 and before["started"] == 0 and not before["followed_idle_s"],
                f"the chain's build was not waiting in the hub's queue in the gap: {before}",
            )
            # free the slot in the gap: hold1's client and builder go, hold2
            # starts, and the hub sends the head of its queue in its place
            hub.succeed("systemctl kill --signal=SIGKILL hold1")
            worker.execute("pkill -KILL -f 'hold1-marker-runnin[g]'", timeout=None)
            wait_until("the chain's build to be dispatched or dropped", lambda: started() + dropped() > 0)
            gap1 = seen()
            print(f"at the first slot after the first gap: {gap1}")
            check(gap1["dropped"] == 0, f"the queued build was dropped in the gap: {gap1}")
            check(gap1["started"] == 1, f"the queued build did not get the slot that freed in the gap: {gap1}")
            check(not gap1["followed_idle_s"], f"the next client was back before the slot freed, no gap tested: {gap1}")
            # and the chain's build has the slot
            # hold2, staged on the worker, runs once hold1's slot freed
            wait_succeeds(worker, running("hold2"), clients=[(hub, "hold2")])
            hub.succeed("systemctl kill --signal=SIGKILL hold2")
            worker.execute("pkill -KILL -f 'hold2-marker-runnin[g]'", timeout=None)
            end_gap("/home/alice", "chain", gap)
            wait_journal(hub, "nix-grpc-daemon", "event=linger_followed ", "tt-chain", clients=[(client, "chain")])

        with subtest("a gap while it runs and other builds wait for its slot"):
            hub.succeed(
                "systemd-run --unit=after bash -lc"
                " 'for i in $(seq 1 40); do nix-build /etc/tt/after.nix --arg i $i --no-out-link"
                " > /tmp/after-$i.path 2> /tmp/after-$i.log; done'"
            )
            wait_succeeds(hub, "journalctl -u tribuchet-hub | grep 'queueing build' | grep -qE 'tt-after-1([^0-9]|$)'", clients=[(hub, "after")])
            end_link(chain, "chain", 2)
            wait_journal(hub, "nix-grpc-daemon", "event=(lingering|revoked_running) ", "tt-chain", more_than=1, clients=[(client, "chain")])
            gap = time.monotonic()
            # the second client is gone, and a build waits for the slot
            check(hub.execute("test -s /tmp/after-1.path", timeout=None)[0] != 0, "no build waited in the second gap")
            restart_hub()
            end_gap("/home/alice", "chain", gap)
            wait_journal(hub, "nix-grpc-daemon", "event=linger_followed ", "tt-chain", more_than=1, clients=[(client, "chain")])
            release("chain", "chain")
            wait_succeeds(client, f"test -s {chain}/status.json", clients=[(client, "chain")])
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
            # one follow per gap, in order, each after a gap longer than the
            # grace (the scheduler's own count, from the leave to the follow)
            followed = evidence["followed_idle_s"]
            check(len(followed) == 2 and min(followed) >= grace_s + 4, f"not followed once after each of two gaps longer than the grace: {evidence}")
            check(status["release"]["acknowledged"], f"the chain's release was not answered: {status}")
            check(evidence["resumed"] >= 1, f"the hub restart was not ridden out: {evidence}")
            assert not failures, "; ".join(failures)
      ''
  );
}
