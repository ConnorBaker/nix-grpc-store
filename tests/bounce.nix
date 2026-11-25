# A node that is its own scheduler's only builder, with one build slot, and
# a client with one build thread on it: a job the node gives back keeps no
# placement its client does not take (tests/e2e/bounce.rs). The node has a
# niks3 cache (--niks3-cache-url) and no substituter: an input it lost that
# the cache still holds is given back unnamed, since the cache did not lose
# it, and the node cannot fetch it (gatherInputs).
{
  pkgs,
  nixPkgs,
  module,
  e2eTests,
  niks3,
}:

let
  system = pkgs.stdenv.hostPlatform.system;
  apiToken = "farm-token-that-is-at-least-36-characters-long";
  niks3Url = "http://localhost:5751";
  # `held` runs until the test writes a line to the fifo `release` in its
  # build directory; `user` needs it and `input`, which the test deletes on
  # the node meanwhile; `top` needs `user`, which puts `user` ahead of
  # `wanted` in the client's and the scheduler's order (cp_hint).
  bounceExpr = pkgs.writeText "bounce.nix" ''
    { tag }:
    let
      step =
        name: script: inputs:
        derivation (
          {
            name = "''${name}-''${tag}";
            system = builtins.currentSystem;
            builder = "/bin/sh";
            PATH = builtins.storePath "${pkgs.coreutils}" + "/bin";
            args = [ "-c" script ];
          }
          // inputs
        );
    in
    rec {
      input = step "input" "echo input > $out" { };
      held = step "held" "mkfifo release; echo HELD-STARTED-''${tag} >&2; read x < release; echo held > $out" { };
      user = step "user" "cat $held $input > $out" { inherit held input; };
      top = step "top" "cat $user > $out" { inherit user; };
      wanted = step "wanted" "echo wanted > $out" { };
    }
  '';
in
pkgs.testers.runNixOSTest {
  name = "nix-grpc-bounce";
  globalTimeout = 600;

  # For the vsock device the agent of tests/e2e listens on.
  sshBackdoor.enable = true;
  defaults.imports = [ (import ./lib/e2e-agent.nix { inherit e2eTests; }) ];

  nodes.machine =
    { config, lib, ... }:
    {
      imports = [
        module
        (import ./lib/niks3-node.nix {
          inherit pkgs niks3 apiToken;
          listenHost = "localhost";
        })
      ];

      virtualisation.memorySize = 2048;
      virtualisation.cores = 2;

      nix.package = nixPkgs.nix-everything;
      nix.settings = {
        experimental-features = [ "nix-command" ];
        # Load-bearing: the niks3 module adds the cache as a substituter, from
        # which the node would fetch the input the test deletes, and build
        # the job instead of giving it back.
        substituters = lib.mkForce [ ];
      };
      system.extraDependencies = [ pkgs.coreutils ];
      # Writes the held build's fifo without blocking; calls the node's
      # BuildDerivation itself.
      environment.systemPackages = [
        pkgs.python3
        pkgs.grpcurl
      ];

      programs.nix-grpc-store.enable = true;
      services.nix-grpc-daemon = {
        enable = true;
        listen = "[::]:50051";
        trustClients = true;
        package = config.programs.nix-grpc-store.package;
        maxJobs = 1;
        niks3 = {
          package = niks3.packages.${system}.niks3;
          url = niks3Url;
          tokenFile = toString (pkgs.writeText "niks3-token" apiToken);
          cacheUrl = niks3Url;
          publicKeys = [ "farm-test-1:RkClDwvfixdOwourBI4UD9hudE3xfU5EBQcMFUVuRV8=" ];
        };
        # Whichever session holds the input (the one that built it and, with
        # farm-hold-planned-inputs, the client's, whose plan found it present)
        # lets it go 2 s after its last call, so the test can delete it
        # (tests/e2e/bounce.rs).
        extraFlags = [
          "--session-ttl"
          "2"
        ];
      };
    };

  testScript = ''
    ${import ./lib/e2e.nix { inherit e2eTests; }}
    ${import ./lib/waits.nix}
    wait_unit(machine, "nix-daemon.socket")
    wait_unit(machine, "nix-grpc-daemon.socket")
    wait_unit(machine, "niks3.service")
    wait_listening(machine, 5751)

    run_e2e({"machine": machine}, {"NGS_BOUNCE_EXPR": "${bounceExpr}", "NGS_PROTO": "${../proto}"}, "bounce::", "--test-threads=1")
  '';
}
