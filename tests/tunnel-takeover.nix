# A node that is its own scheduler's only builder and gets its builds and
# queries through tunnels to the balancer, reloaded mid-build: its successor
# opens tunnels of its own, and the balancer has to stop using the old
# process's, which it shut down when it handed over (src/daemon/takeover.hh).
{
  pkgs,
  nixPkgs,
  module,
  niks3,
}:

let
  system = pkgs.stdenv.hostPlatform.system;
  apiToken = "farm-token-that-is-at-least-36-characters-long";
  niks3Url = "http://lb:5751";
  tunnelPort = 9443;

  # nixos test framework: nodes get 192.168.1.<n> in attribute-name order.
  singleIp = "192.168.1.3";

  common = {
    virtualisation.memorySize = 1536;
    imports = [ ./lib/test-certs.nix ];
    nix.package = nixPkgs.nix-everything;
    nix.settings.experimental-features = [ "nix-command" ];
  };

  tls = {
    certFile = "/run/test-certs/node.pem";
    keyFile = "/run/test-certs/node.key";
    clientCaFile = "/run/test-certs/ca.pem";
  };
  cert = commonName: subjectAltName: { inherit commonName subjectAltName; };

  # Says when it starts and, with `hold` other than 0, runs until the test
  # kills its inner sh (a read of a pipe it also holds open for writing),
  # without inputs to upload.
  jobExpr = pkgs.writeText "job.nix" ''
    { tag, hold }:
    derivation {
      name = "takeover-''${tag}";
      system = builtins.currentSystem;
      builder = "/bin/sh";
      args = [ "-c" "echo STARTED-''${tag} >&2; [ ''${hold} = 0 ] || /bin/sh -c ': | { exec 3<>/proc/self/fd/0; read x <&3; }' takeover-''${tag}-holding; echo ''${tag} > $out" ];
    }
  '';
in
pkgs.testers.runNixOSTest {
  name = "nix-grpc-tunnel-takeover";
  globalTimeout = 600;

  nodes = {
    client = {
      imports = [
        common
        module
      ];
      programs.nix-grpc-store.enable = true;
      testCerts.certs.node = cert "ci-1" "DNS:client";
      nix.settings.substituters = pkgs.lib.mkForce [ ];
    };

    lb = {
      imports = [
        common
        module
        (import ./lib/niks3-node.nix {
          inherit pkgs niks3 apiToken;
          listenHost = "lb";
        })
      ];
      services.nix-grpc-farm-lb = {
        enable = true;
        # Builds and queries go through the tunnels; only the scheduler is dialled.
        systems = [ ];
        scheduler = [ "${singleIp}:50051" ];
        healthCheckInterval = "1s";
        tunnel.listen = "[::]:${toString tunnelPort}";
        tls = tls // {
          upstream = {
            inherit (tls) certFile keyFile;
            caFile = tls.clientCaFile;
          };
        };
      };
      testCerts = {
        createsCas = true;
        certs.node = cert "lb-1" "DNS:lb";
        neededBy = [ "envoy.service" ];
      };
      networking.firewall.allowedTCPPorts = [
        50051
        tunnelPort
      ];
      environment.systemPackages = [ pkgs.curl ];
    };

    # Both roles, no scheduler, socket-activated: it hands over on reload.
    single =
      { config, lib, ... }:
      {
        imports = [
          common
          module
        ];
        # Two slots: the old process's build keeps one until it ends.
        virtualisation.cores = 2;
        testCerts.certs.node = cert "worker-1" "DNS:single,IP:${singleIp}";
        services.nix-grpc-daemon = {
          enable = true;
          listen = "[::]:50051";
          logLevel = "debug";
          package = config.programs.nix-grpc-store.package;
          inherit tls;
          trustedProxies = [ "lb-*" ];
          accessRules = map (cn: {
            inherit cn;
            role = "trusted";
          }) [ "ci-*" "lb-*" "worker-*" ];
          niks3 = {
            package = niks3.packages.${system}.niks3;
            url = niks3Url;
            tokenFile = toString (pkgs.writeText "niks3-token" apiToken);
            cacheUrl = niks3Url;
            publicKeys = [ "farm-test-1:RkClDwvfixdOwourBI4UD9hudE3xfU5EBQcMFUVuRV8=" ];
          };
          tunnel.balancers = [ "lb:${toString tunnelPort}" ];
          # More idle tunnels per balancer thread than its route has retries:
          # a request must never be offered a tunnel the old process cannot take.
          tunnel.count = 32;
        };
        programs.nix-grpc-store.enable = true;
        testCerts.neededBy = [ "nix-grpc-daemon.service" ];
        networking.firewall.allowedTCPPorts = [ 50051 ];
        # The same daemon under another store path, so the unit reloads.
        specialisation.next.configuration =
          { config, lib, ... }:
          {
            services.nix-grpc-daemon.package = lib.mkForce (
              pkgs.runCommand "nix-grpc-daemon-next" { meta.mainProgram = "nix-grpc-daemon"; } ''
                install -Dm755 ${lib.getExe' config.programs.nix-grpc-store.package "nix-grpc-daemon"} $out/bin/nix-grpc-daemon
              ''
            );
          };
      };
  };

  testScript = ''
    ${import ./lib/waits.nix}
    start_all()
    wait_unit(lb, "niks3.service")
    wait_succeeds(lb, "nc -z localhost 5751 && nc -z localhost 50051")
    # The tunnels come from the daemon, not from its socket.
    single.execute("systemctl start nix-grpc-daemon.service", timeout=None)

    wait_unit(client, "test-certs.service")
    certs = "/run/test-certs"
    store = f"grpc://lb:50051?client-cert={certs}/node.pem&client-key={certs}/node.key&ca-cert={certs}/ca.pem"
    unit = "nix-grpc-daemon.service"

    def build(tag: str, hold: int) -> str:
        return f"nix build -L --no-link --print-out-paths --store '{store}' --eval-store auto -f ${jobExpr} --argstr tag {tag} --argstr hold {hold}"

    def stats() -> dict[str, int]:
        keys = "upstream_(rq_total|rq_5xx|rq_retry|cx_total|cx_destroy_remote_with_active_rq)"
        out = lb.succeed(f"curl -sf 'localhost:9901/stats?filter=^cluster\\.tunnelled(-any)?\\.{keys}$'")
        return {key: int(value) for key, value in (line.split(": ") for line in out.strip().splitlines())}

    def main_pid() -> str:
        return single.succeed(f"systemctl show -P MainPID {unit}").strip()

    def calls(pid: str) -> int:
        return int(single.succeed(f"journalctl -o cat _PID={pid} | grep -c event=rpc_start || true"))

    with subtest("the node gets its builds and queries through its tunnels"):
        # The tunnels may still be opening, so the first build can find nobody.
        warm = wait_succeeds(client, build("warm", 0)).strip()
        client.succeed(f"nix path-info --store '{store}' {warm}")
        now = stats()
        assert now["cluster.tunnelled.upstream_rq_total"] > 0, now
        assert now["cluster.tunnelled-any.upstream_rq_total"] > 0, now

    with subtest("after a takeover the balancer sends nothing to the old process"):
        client.succeed(f"systemd-run --unit long --setenv=PATH=/run/current-system/sw/bin sh -c \"{build('long', 1)} > /root/long.out\"")
        wait_succeeds(client, "journalctl -u long | grep -q STARTED-long", clients=[(client, "long")])
        old = main_pid()
        # "event=handed_over pid=": not the old process's handed_over_exit
        handed = int(single.succeed(f"journalctl -u {unit} | grep -c 'event=handed_over pid=' || true"))
        single.succeed("/run/current-system/specialisation/next/bin/switch-to-configuration test")
        wait_journal(single, unit, "event=handed_over pid=", more_than=handed, clients=[(single, unit)])
        new = main_pid()
        assert new != old, f"no new main process: {new}"
        # The successor says ready before its tunnels are open: the builds
        # below go once they are.
        tunnels = f"journalctl -a -o cat _PID={new}"
        wait_stream(single, f"{tunnels} -f --no-tail", "event=tunnels_open ", clients=[(single, unit)], now=tunnels)
        before, old_calls = stats(), calls(old)
        for i in range(4):
            client.succeed(build(f"after{i}", 0))
            client.succeed(f"nix path-info --store '{store}' {warm}")
        after = stats()
        # Envoy creates some counters, such as rq_5xx, only when they first count.
        keys = [f"cluster.{c}.upstream_{s}" for c in ["tunnelled", "tunnelled-any"] for s in ["rq_total", "rq_5xx", "rq_retry", "cx_total", "cx_destroy_remote_with_active_rq"]]
        delta = {key: after.get(key, 0) - before.get(key, 0) for key in keys}
        print(f"envoy counters during the takeover's drain: {delta}")
        assert client.execute("systemctl is-active long", timeout=None)[0] == 0, "the builds after the takeover waited for the old process"
        assert calls(old) == old_calls, single.succeed(f"journalctl -o cat _PID={old}")
        # Once handed over, the old process offers no tunnel and has none taken.
        since = single.succeed(f"journalctl -o cat _PID={old}").split("event=handed_over pid=", 1)[1]
        assert "event=tunnels_open" not in since and "event=tunnel_failed" not in since, since
        assert delta["cluster.tunnelled.upstream_rq_total"] >= 4, delta
        assert delta["cluster.tunnelled.upstream_rq_5xx"] == 0, delta
        assert delta["cluster.tunnelled-any.upstream_rq_5xx"] == 0, delta

    with subtest("the old process finishes its build and leaves"):
        single.succeed("pkill -f 'takeover-long-holdin[g]$'")
        wait_until("long ended", lambda: ended(client, "long"))
        journal = client.succeed("journalctl -u long")
        assert client.succeed("systemctl show -P Result long").strip() == "success", journal
        assert journal.count("STARTED-long") == 1, journal
        # The successor says so once its connection to the old process ends,
        # which may come before or after the old pid is reaped: both are
        # waited for, each irreversible.
        wait_journal(single, unit, f"event=predecessor_exited pid={old}( |$)", clients=[(single, unit)])
        wait_succeeds(single, f"! test -d /proc/{old}")
        client.succeed(build("later", 0))
  '';
}
