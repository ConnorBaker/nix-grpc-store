# Helm chart in k3s. Workers and envoy run from our images, niks3, garage
# and postgres as NixOS services on the node. Workers authenticate to niks3
# with their service account token, and the client uses one for the farm.
# The test builds through the balancer, runs a Job with the client image,
# fills a store to trigger harmonia-gc, and deletes a worker pod mid-build.
{
  pkgs,
  niks3,
  chart,
  dockerWorker,
  dockerLb,
  dockerClient,
  nixPkgs,
  clientModule,
}:
let
  inherit (pkgs) lib;
  niks3Pkgs = niks3.packages.${pkgs.stdenv.hostPlatform.system};
  system = pkgs.stdenv.hostPlatform.system;

  chartTgz =
    pkgs.runCommand "nix-grpc-farm-chart.tgz" { nativeBuildInputs = [ pkgs.kubernetes-helm ]; }
      ''
        cp -r ${chart} chart && chmod -R u+w chart
        helm package chart
        mv nix-grpc-farm-*.tgz $out
      '';

  hostIP = "192.168.1.1";
  nodePort = 30051;
  signingPublicKey = "farm-test-1:RkClDwvfixdOwourBI4UD9hudE3xfU5EBQcMFUVuRV8=";
  niks3Url = "http://${hostIP}:5751";

  # Bearer tokens need TLS on the client side, so the balancer and workers
  # get certificates. SANs cover the NodePort address and the headless
  # Service name envoy dials.
  certs = pkgs.runCommand "k3s-farm-certs" { nativeBuildInputs = [ pkgs.openssl ]; } ''
    mkdir $out && cd $out
    openssl req -x509 -newkey rsa:2048 -nodes -days 3650 -keyout ca.key -out ca.crt -subj /CN=farm-ca
    issue() {
      openssl req -newkey rsa:2048 -nodes -keyout $1.key -out $1.csr -subj /CN=$2
      openssl x509 -req -in $1.csr -days 3650 -CA ca.crt -CAkey ca.key -set_serial 0x$(openssl rand -hex 8) \
        -extfile <(printf "subjectAltName=$3") -out $1.crt
    }
    issue lb lb "IP:127.0.0.1,IP:${hostIP},DNS:nix-grpc-farm.farm.svc"
    issue lb-renewed lb "IP:127.0.0.1,IP:${hostIP},DNS:nix-grpc-farm.farm.svc,DNS:renewed.example"
    issue worker worker "DNS:worker,DNS:nix-grpc-farm-scheduler.farm.svc"
  '';

  jobExpr = pkgs.writeText "job.nix" ''
    { tag }:
    let
      mk = name: deps: derivation {
        inherit name;
        system = builtins.currentSystem;
        builder = "/bin/sh";
        args = [ "-c" "echo ''${name}-''${tag} ''${toString deps} > $out" ];
      };
      a = mk "k3s-a" [ ];
      b = mk "k3s-b" [ a ];
    in
    mk "k3s-top" [ a b ]
  '';
  nfbFlake = pkgs.writeText "flake.nix" ''
    {
      outputs = _: {
        checks.${system}.t = derivation {
          name = "nfb-ci";
          system = "${system}";
          builder = "/bin/sh";
          args = [ "-c" "echo > $out" ];
        };
      };
    }
  '';
  slowExpr = pkgs.writeText "slow.nix" ''
    { tag }:
    derivation {
      name = "slow-''${tag}";
      system = builtins.currentSystem;
      builder = "/bin/sh";
      # Runs until the test kills its inner sh: a read of a pipe it also
      # holds open for writing, which never ends nor spins.
      args = [ "-c" "/bin/sh -c ': | { exec 3<>/proc/self/fd/0; read x <&3; }' slow-''${tag}-holding; echo > $out" ];
    }
  '';
  groupName = lib.replaceStrings [ "_" ] [ "-" ] system;
  # The pod spec from docs/kubernetes.md "Connect CI jobs in the cluster".
  ciJob = (pkgs.formats.json { }).generate "ci-job.json" {
    apiVersion = "batch/v1";
    kind = "Job";
    metadata = {
      name = "ci";
      namespace = "ci";
    };
    spec = {
      backoffLimit = 0;
      template.spec = {
        serviceAccountName = "builder";
        restartPolicy = "Never";
        containers = [
          {
            name = "build";
            image = "${dockerClient.imageName}:${dockerClient.imageTag}";
            env = [
              {
                name = "FARM";
                value = "grpc://nix-grpc-farm.farm.svc:50051?token-file=/var/run/secrets/nix-grpc-farm/token&ca-cert=/var/run/secrets/nix-grpc-farm/ca.crt";
              }
              {
                name = "NIX_CONFIG";
                value = ''
                  substituters = ${niks3Url}
                  trusted-public-keys = ${signingPublicKey}
                '';
              }
            ];
            command = [
              "bash"
              "-euc"
              ''
                nix build -L --store "$FARM" --eval-store auto -f /job/job.nix --argstr tag ci
                cp /job/flake.nix /tmp && cd /tmp && git init -q && git add flake.nix
                nix-fast-build --store "$FARM" --no-download --skip-cached
                out=$(nix eval --raw .#checks.${system}.t.outPath)
                if test -e "$out"; then echo "$out was downloaded" >&2; exit 1; fi
              ''
            ];
            volumeMounts = [
              {
                name = "nix-grpc-farm";
                mountPath = "/var/run/secrets/nix-grpc-farm";
                readOnly = true;
              }
              {
                name = "job";
                mountPath = "/job";
              }
            ];
          }
        ];
        volumes = [
          {
            name = "nix-grpc-farm";
            projected.sources = [
              {
                serviceAccountToken = {
                  audience = "nix-grpc-farm";
                  path = "token";
                };
              }
              {
                configMap = {
                  name = "nix-grpc-farm-ca";
                  items = [
                    {
                      key = "ca.crt";
                      path = "ca.crt";
                    }
                  ];
                };
              }
            ];
          }
          {
            name = "job";
            configMap.name = "job";
          }
        ];
      };
    };
  };
  chartValues = {
    image = {
      repository = dockerWorker.imageName;
      tag = dockerWorker.imageTag;
    };
    lbImage = {
      repository = dockerLb.imageName;
      tag = dockerLb.imageTag;
    };
    niks3 = {
      serverURL = niks3Url;
      cacheURL = niks3Url;
      publicKeys = [ signingPublicKey ];
      auth.serviceAccountToken.enabled = true;
    };
    auth.workloadIdentity = {
      enabled = true;
      allowedServiceAccounts = [ "ci:builder" ];
    };
    tls = {
      clientCA.existingSecret = "farm-ca";
      lb.existingSecret = "farm-lb-tls";
      worker.existingSecret = "farm-worker-tls";
      worker.serverName = "worker";
    };
    workers.${groupName} = {
      inherit system;
      replicas = 2;
      maxJobs = 2;
    };
    gc = {
      ensureFree = "2G";
      interval = 10;
    };
    lb = {
      replicas = 1;
      healthCheckInterval = "1s";
      service = {
        type = "NodePort";
        inherit nodePort;
      };
    };
    logLevel = "debug";
  };
in
pkgs.testers.runNixOSTest {
  name = "nix-grpc-farm-k3s";
  globalTimeout = 2100;
  # checks.helm lints these without booting a VM.
  passthru = { inherit chartValues; };

  nodes.machine =
    { config, ... }:
    {
      imports = [
        clientModule
        (import ./lib/niks3-node.nix {
          inherit pkgs niks3;
          listenHost = hostIP;
          apiToken = "farm-token-that-is-at-least-36-characters-long";
        })
      ];
      virtualisation = {
        memorySize = 4096;
        cores = 4;
        diskSize = 16384;
      };
      networking.firewall.enable = false;
      assertions = [
        {
          assertion = config.networking.primaryIPAddress == hostIP;
          message = "hostIP";
        }
      ];
      nix.package = nixPkgs.nix-everything;
      nix.settings = {
        experimental-features = [ "nix-command" ];
        substituters = lib.mkForce [ ];
        trusted-public-keys = [ signingPublicKey ];
      };
      programs.nix-grpc-store.enable = true;
      environment.systemPackages = [
        pkgs.kubectl
        pkgs.grpc-health-probe
        pkgs.openssl
      ];
      environment.sessionVariables.KUBECONFIG = "/etc/rancher/k3s/k3s.yaml";

      services.niks3 = {
        # What the niks3 chart's workloadIdentity helper renders, pointed at
        # k3s from outside the cluster.
        oidc.allowInsecure = true;
        oidc.providers.kubernetes = {
          audience = "niks3";
          jwksUrl = "https://127.0.0.1:6443/openid/v1/jwks";
          caFile = "/run/niks3-k8s-ca.crt";
          bearerTokenFile = "/run/niks3-k8s-token";
          boundSubject = [ "system:serviceaccount:farm:nix-grpc-farm" ];
          scopes = [ "write" ];
        };
      };
      # niks3 reads the issuer from a k8s token and the API server CA. In a
      # pod both come from the service account mount.
      systemd.services.niks3 = {
        after = [ "k3s.service" ];
        serviceConfig.ExecStartPre = [
          "+${pkgs.writeShellScript "k8s-token" ''
            until ${pkgs.kubectl}/bin/kubectl --kubeconfig /etc/rancher/k3s/k3s.yaml -n kube-system create token default > /run/niks3-k8s-token 2>/dev/null \
                && [ -s /run/niks3-k8s-token ] \
                && install -m0644 /var/lib/rancher/k3s/server/tls/server-ca.crt /run/niks3-k8s-ca.crt; do
              sleep 2
            done
          ''}"
        ];
        serviceConfig.TimeoutStartSec = "infinity";
      };

      services.k3s = {
        enable = true;
        role = "server";
        # The gc subtest fills the disk on purpose.
        extraFlags = [ "--kubelet-arg=eviction-hard=nodefs.available<1%,imagefs.available<1%" ];
        disable = [
          "traefik"
          "metrics-server"
          "local-storage"
        ];
        images = [
          config.services.k3s.package.airgap-images
          dockerWorker
          dockerLb
          dockerClient
        ];
        manifests.ci.content = [
          {
            apiVersion = "v1";
            kind = "Namespace";
            metadata.name = "ci";
          }
          {
            apiVersion = "v1";
            kind = "Namespace";
            metadata.name = "farm";
          }
          {
            apiVersion = "v1";
            kind = "ServiceAccount";
            metadata = {
              name = "builder";
              namespace = "ci";
            };
          }
        ];
        autoDeployCharts.nix-grpc-farm = {
          package = chartTgz;
          targetNamespace = "farm";
          values = chartValues;
        };
      };
    };

  testScript = ''
    ${import ./lib/waits.nix}
    def kubectl(args: str) -> str:
        return machine.succeed("kubectl -n farm " + args)
    def release(tag: str, in_pods: list[str], unit: str) -> None:
        # Kills the held build's inner sh in whichever of in_pods runs it
        # (pgrep and the shell's kill: the image need not have pkill), until
        # one does; unit, the client, ending first fails the wait.
        kill = shlex.quote(f"kill $(pgrep -f '{tag}-holdin[g]$')")
        def released() -> bool:
            return any(machine.execute(f"kubectl -n farm exec {p} -c nix-daemon -- sh -c {kill}", timeout=None)[0] == 0 for p in in_pods)
        wait_until(f"the held build {tag} released", released, clients=[(machine, unit)])
    def pod_ip(selector: str) -> str:
        return kubectl(f"get pod {selector} -o jsonpath='{{.items[0].status.podIP}}{{.status.podIP}}'").strip()
    def rollout(deployment: str) -> None:
        # Complete as `rollout status` judges it (its spec seen, every
        # replica updated and available, none old), without its progress
        # deadline. The chart's helm-install Job creates it first.
        # A container of its pods that exited (restarted, or crash looping)
        # never makes it complete: that fails it, after dump().
        fields = "{.metadata.generation} {.status.observedGeneration} {.spec.replicas} {.status.replicas} {.status.updatedReplicas} {.status.availableReplicas}"
        labels = "{{range $k, $v := .spec.selector.matchLabels}}{{$k}}={{$v}},{{end}}"
        restarts = "{.items[*].status.containerStatuses[*].restartCount}"
        def complete() -> bool:
            status, out = machine.execute(f"kubectl -n farm get deployment {deployment} -o jsonpath='{fields}'", timeout=None)
            f = out.split()
            if status == 0 and len(f) == 6 and f[0] == f[1] and f[2] == f[3] == f[4] == f[5]:
                return True
            status, sel = machine.execute(f"kubectl -n farm get deployment {deployment} -o go-template='{labels}'", timeout=None)
            if status == 0 and sel.strip(","):
                out = machine.execute(f"kubectl -n farm get pods -l {sel.strip(',')} -o jsonpath='{restarts}'", timeout=None)[1]
                if any(int(n) > 0 for n in out.split()):
                    dump()
                    raise RequestedAssertionFailed(f"a container of deployment {deployment} exited before it rolled out: restarts {out}")
            return False
        wait_until(f"deployment {deployment} rolled out", complete)
    def dump() -> None:
        machine.execute(
            "{ kubectl get pods -A; kubectl get events -A --sort-by=.lastTimestamp | tail -40;"
            " kubectl -n kube-system logs -l batch.kubernetes.io/job-name=helm-install-nix-grpc-farm --tail=40;"
            " kubectl -n farm describe pods | tail -80;"
            " kubectl -n farm logs -l app.kubernetes.io/component=worker --all-containers --prefix --tail=30;"
            " kubectl -n farm logs -l app.kubernetes.io/component=scheduler --tail=30; } >&2")

    wait_unit(machine, "k3s.service")
    # Pods wait in ContainerCreating until these exist.
    wait_succeeds(machine, "kubectl get ns farm")
    kubectl("create secret tls farm-lb-tls --cert=${certs}/lb.crt --key=${certs}/lb.key")
    kubectl("create secret tls farm-worker-tls --cert=${certs}/worker.crt --key=${certs}/worker.key")
    kubectl("create secret generic farm-ca --from-file=ca.crt=${certs}/ca.crt")

    with subtest("chart deploys and workers become Ready"):
        for d in ["lb", "scheduler", "worker-${groupName}"]:
            rollout(f"nix-grpc-farm-{d}")
        wait_unit(machine, "niks3.service")
        # envoy has health-checked both workers and the scheduler
        lb_ip = pod_ip("-l app.kubernetes.io/component=lb")
        wait_succeeds(
            machine,
            f"test $(curl -sf {lb_ip}:9901/stats/prometheus | grep -v '^#' | grep '^envoy_cluster_membership_healthy' | grep -v 'envoy_cluster_name=\"admin\"' | awk '{{s+=$2}} END {{print s+0}}') -ge 3",
        )
        # The admin API is not reachable from the pod network.
        machine.fail(f"curl -sf {lb_ip}:9901/clusters")
        sched_ip = pod_ip("-l app.kubernetes.io/component=scheduler")
        wait_succeeds(machine, f"curl -sf http://{sched_ip}:9464/metrics | grep -qx 'nix_grpc_sched{{kind=\"workers\"}} 2'")

    pods = kubectl("get pods -l app.kubernetes.io/component=worker --field-selector=status.phase=Running -o jsonpath='{.items[*].metadata.name}'").split()
    assert len(pods) == 2, pods

    machine.succeed("kubectl -n ci create token builder --audience nix-grpc-farm > /tmp/builder.jwt")
    store = "grpc://127.0.0.1:${toString nodePort}?ca-cert=${certs}/ca.crt&token-file=/tmp/builder.jwt"
    def build(expr: str, tag: str) -> str:
        return machine.succeed(f"nix build -L --store '{store}' --eval-store auto -f {expr} --argstr tag {tag} 2>&1")
    def out_path(expr: str, tag: str) -> str:
        return machine.succeed(f"nix eval --raw -f {expr} --argstr tag {tag} outPath").strip()

    with subtest("build through the balancer lands in the cache"):
        build("${jobExpr}", "t1")
        top = out_path("${jobExpr}", "t1")
        machine.fail(f"test -e {top}")
        machine.succeed(f"nix copy --from ${niks3Url} {top} && grep k3s-top-t1 {top}")
        # --worker-name from the downward API
        out = build("${jobExpr}", "t2")
        assert "machine/nix-grpc-farm-worker-" in out, out

    with subtest("a Job with the client image builds through the farm with its service account"):
        machine.succeed("kubectl -n ci create configmap nix-grpc-farm-ca --from-file=ca.crt=${certs}/ca.crt")
        machine.succeed("kubectl -n ci create configmap job --from-file=job.nix=${jobExpr} --from-file=flake.nix=${nfbFlake}")
        machine.succeed("kubectl apply -f ${ciJob}")
        try:
            # Complete or Failed, whichever comes first.
            wait_succeeds(machine, "kubectl -n ci get job ci -o jsonpath='{.status.conditions[*].type}' | grep -qE 'Complete|Failed'")
            machine.succeed("kubectl -n ci get job ci -o jsonpath='{.status.succeeded}' | grep -qx 1")
        finally:
            machine.execute("kubectl -n ci logs job/ci >&2")
        machine.succeed(f"curl -sf ${niks3Url}/{out_path('${jobExpr}', 'ci').removeprefix('/nix/store/').split('-')[0]}.narinfo > /dev/null")

    with subtest("a renewed balancer certificate is served without a rollout"):
        kubectl("create secret tls farm-lb-tls --cert=${certs}/lb-renewed.crt --key=${certs}/lb-renewed.key --dry-run=client -o yaml | kubectl -n farm apply -f -")
        # kubelet syncs Secret volumes about once a minute.
        lb_pod = kubectl("get pods -l app.kubernetes.io/component=lb -o jsonpath='{.items[0].metadata.name}'")
        wait_succeeds(machine, f"kubectl -n farm exec {lb_pod} -- cat /etc/envoy/tls/lb/tls.crt | openssl x509 -noout -ext subjectAltName | grep -q renewed.example")
        wait_succeeds(
            machine,
            "openssl s_client -connect 127.0.0.1:${toString nodePort} </dev/null 2>/dev/null "
            "| openssl x509 -noout -ext subjectAltName | grep -q renewed.example",
        )
        build("${jobExpr}", "t-renewed")

    with subtest("metrics carry build_info"):
        machine.succeed(f"curl -sf http://{pod_ip(pods[0])}:9464/metrics | grep -E 'nix_grpc_build_info\\{{.*worker=\"machine/{pods[0]}\"'")

    with subtest("harmonia-gc frees space and the worker stays Ready"):
        pod = pods[0]
        def sh(container: str, cmd: str) -> str:
            return kubectl(f"exec {pod} -c {container} -- sh -c {shlex.quote(cmd)}")
        # emptyDir sizeLimit is not a filesystem, so gc sees the node's disk.
        # Garbage for gc to find, then leave ~1G free: below ensureFree (2G).
        avail = int(sh("gc", "df -B1 /nix/store | awk 'NR==2 {print $4}'"))
        sh("nix-daemon", "for i in 1 2 3; do echo $i > /tmp/g$i; nix-store --add /tmp/g$i; done")
        runs = int(kubectl(f"logs {pod} -c gc | grep -c 'store paths deleted' || true"))
        sh("gc", f"fallocate -l {avail - 1024**3} /nix/fill")
        wait_stream(machine, f"kubectl -n farm logs -f {pod} -c gc", "store paths deleted", more_than=runs)
        sh("gc", "rm /nix/fill")
        ready = kubectl(f"get pod {pod} -o jsonpath='{{.status.conditions[?(@.type==\"Ready\")].status}}'").strip()
        assert ready == "True", ready
        sh("gc", "command -v nix-daemon && command -v niks3")  # tools survived gc

    with subtest("losing the balancer mid-build: the client reconnects and the build succeeds"):
        machine.succeed(f"systemd-run --unit vialb nix build -L --store '{store}' --eval-store auto -f ${slowExpr} --argstr tag lb1")
        wait_succeeds(machine, "journalctl -u vialb | grep -q \"building '/nix/store/.*-slow-lb1.drv'\"", clients=[(machine, "vialb")])
        lb_pod = kubectl("get pods -l app.kubernetes.io/component=lb -o jsonpath='{.items[0].metadata.name}'")
        kubectl(f"delete pod {lb_pod} --wait=false")
        # the balancer gone and back while the build runs; then it may end
        wait_succeeds(machine, f"! kubectl -n farm get pod {lb_pod}")
        rollout("nix-grpc-farm-lb")
        release("lb1", pods, "vialb")
        wait_until("vialb ended", lambda: ended(machine, "vialb"))
        machine.succeed("systemctl show -p Result --value vialb | grep -qx success || { journalctl -u vialb >&2; false; }")

    with subtest("deleting a worker pod mid-build drains it: the build finishes there and publishes"):
        machine.succeed(f"systemd-run --unit slow nix build -L --store '{store}' --eval-store auto -f ${slowExpr} --argstr tag s1")
        def holder() -> str | None:
            # [g] keeps the probe from matching itself, $ the build's outer sh.
            return next((p for p in pods if machine.execute(f"kubectl -n farm exec {p} -c nix-daemon -- pgrep -f 'slow-s1-holdin[g]$'", timeout=None)[0] == 0), None)
        wait_until("a worker pod to run the slow build", lambda: holder() is not None, clients=[(machine, "slow")])
        busy = holder()
        assert busy is not None
        kubectl(f"delete pod {busy} --wait=false")
        # The worker has its SIGTERM and drains; its build still runs after.
        wait_stream(machine, f"kubectl -n farm logs -f {busy} -c nix-grpc-daemon", "event=draining( |$)")
        assert holder() == busy, "pod delete killed the build instead of draining"
        release("s1", [busy], "slow")
        wait_until("slow ended", lambda: ended(machine, "slow"))
        machine.succeed("systemctl show -p Result --value slow | grep -qx success || { journalctl -u slow >&2; false; }")
        machine.fail("journalctl -u slow | grep -q 'asking the scheduler again'")
        narinfo = out_path("${slowExpr}", "s1").removeprefix("/nix/store/").split("-")[0] + ".narinfo"
        machine.succeed(f"curl -sf ${niks3Url}/{narinfo} > /dev/null")
  '';
}
