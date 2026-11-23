# Latency benchmark, not part of `checks` so CI never runs it:
#   nix build .#bench-latency -L
#
# netem on lo delays the gRPC TCP path (unix sockets are unaffected), so a
# single VM can measure how well the protocol hides RTT, for both `nix copy`
# and remote builds. 25 ms each way = 50 ms RTT.
{
  pkgs,
  nixPkgs,
  module,
  e2eTests,
}:
let
  system = pkgs.stdenv.hostPlatform.system;
  chainNix = pkgs.writeText "chain.nix" ''
    { salt }:
    let
      mk = name: deps: derivation {
        inherit name deps;
        system = builtins.currentSystem;
        builder = "/bin/sh";
        args = [ "-c" "echo ''${name} ''${salt} ''${toString deps} > $out" ];
      };
      go = n: prev: if n == 0 then prev else go (n - 1) [ (mk "link-''${toString n}-''${salt}" prev) ];
    in
    builtins.head (go 20 [ ])
  '';
in
pkgs.testers.runNixOSTest {
  name = "nix-grpc-store-bench-latency";
  globalTimeout = 1200;
  sshBackdoor.enable = true;
  defaults.virtualisation.qemu.enableSharedMemory = true;
  # The ssh generator only opens the vsock socket if /dev/vsock exists when it runs.
  defaults.boot.initrd.kernelModules = [ "vmw_vsock_virtio_transport" ];

  nodes.machine =
    { config, ... }:
    {
      imports = [ module ];

      virtualisation.memorySize = 2048;
      virtualisation.cores = 2;

      nix.package = nixPkgs.nix-everything;
      nix.settings = {
        experimental-features = [ "nix-command" ];
        substituters = [ ];
        substitute = false;
      };

      programs.nix-grpc-store.enable = true;
      services.nix-grpc-daemon = {
        enable = true;
        listen = "127.0.0.1:50051";
        # The RPC counts the test asserts on.
        metricsListen = "127.0.0.1:9464";
        trustClients = true;
        package = config.programs.nix-grpc-store.package;
      };

      boot.kernelModules = [ "sch_netem" ];
      environment.systemPackages = [ pkgs.curl ];
    };

  testScript = ''
    ${import ./lib/e2e.nix { inherit pkgs e2eTests; }}
    ${import ./lib/waits.nix}
    wait_unit(machine, "nix-grpc-daemon.socket")
    wait_succeeds(machine, "nc -z localhost 50051")
    env = {"NGS_CHAIN_EXPR": "${chainNix}", "NGS_SYSTEM": "${system}"}
    run_e2e({"machine": machine}, env, "latency::", "--test-threads=1")
  '';
}
