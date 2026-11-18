# A node that is its own scheduler's only builder, configured as such nodes
# are deployed: both roles, socket-activated with the default idle timeout,
# proxying to the local nix-daemon. Switches between this configuration and
# its specialisations reload the unit mid-build, see src/daemon/takeover.hh
# and tests/e2e/handover.rs. `part` picks the tests: "upgrade" (what a
# reload hands over, and what a stop or idle timeout does after it) or
# "failures" (takeovers that fail, crash or are refused).
{
  pkgs,
  nixPkgs,
  module,
  e2eTests,
  part,
}:

let
  # Says when it starts, so the test knows the build is under way and how
  # often it ran, and if `hold`, runs until the test writes a line to the
  # fifo `release` in its build directory (tests/e2e/handover.rs release).
  slowExpr = pkgs.writeText "slow.nix" ''
    { tag, hold ? true }:
    derivation {
      name = "slow-''${tag}";
      system = builtins.currentSystem;
      builder = "/bin/sh";
      PATH = builtins.storePath "${pkgs.coreutils}" + "/bin";
      args = [ "-c" "mkfifo release; echo SLOW-STARTED-''${tag} >&2; ''${if hold then "read x < release" else ":"}; echo ''${tag} > $out" ];
    }
  '';
in
pkgs.testers.runNixOSTest {
  name = "nix-grpc-handover-${part}";

  sshBackdoor.enable = true;
  # The ssh generator only opens the vsock socket if /dev/vsock exists when it runs.
  defaults.boot.initrd.kernelModules = [ "vmw_vsock_virtio_transport" ];

  nodes.machine =
    { config, lib, ... }:
    {
      imports = [ module ];

      virtualisation.memorySize = 2048;
      virtualisation.cores = 2;

      nix.package = nixPkgs.nix-everything;
      nix.settings = {
        experimental-features = [ "nix-command" ];
        substituters = lib.mkForce [ ];
      };
      system.extraDependencies = [ pkgs.coreutils ];
      # Connects to a takeover offer as a process the offer has to refuse.
      environment.systemPackages = [ pkgs.python3 ];

      programs.nix-grpc-store.enable = true;
      services.nix-grpc-daemon = {
        enable = true;
        listen = "[::]:50051";
        trustClients = true;
        package = config.programs.nix-grpc-store.package;
      };

      specialisation = {
        # The same daemon under another store path, so the unit changes and
        # the process that serves afterwards can be told apart by its
        # executable.
        next.configuration =
          { config, lib, ... }:
          {
            services.nix-grpc-daemon.package = lib.mkForce (
              pkgs.runCommand "nix-grpc-daemon-next" { meta.mainProgram = "nix-grpc-daemon"; } ''
                install -Dm755 ${lib.getExe' config.programs.nix-grpc-store.package "nix-grpc-daemon"} $out/bin/nix-grpc-daemon
              ''
            );
          };
        # Fails to start once it has the sockets: systemd's socket already
        # holds the metrics port.
        broken.configuration.services.nix-grpc-daemon.metricsListen = "127.0.0.1:50051";
        # systemd gives a reload 5 s.
        quick-timeout.configuration.systemd.services.nix-grpc-daemon.serviceConfig.TimeoutStartSec = "5s";
        # Idles out after 20 s.
        idle.configuration.services.nix-grpc-daemon.idleTimeout = lib.mkForce 20;
      };
    };

  testScript =
    { nodes, ... }:
    ''
      ${import ./lib/e2e.nix { inherit pkgs e2eTests; }}
      ${import ./lib/waits.nix}
      wait_unit(machine, "nix-daemon.socket")
      wait_unit(machine, "nix-grpc-daemon.socket")

      with subtest("${part}"):
          run_e2e(
              {"machine": machine},
              {"NGS_SLOW_EXPR": "${slowExpr}", "NGS_BASE_SYSTEM": "${nodes.machine.system.build.toplevel}"},
              "handover::${part}::",
              "--test-threads=1",
          )
    '';
}
