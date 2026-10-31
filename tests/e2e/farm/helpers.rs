use std::sync::Once;

use crate::harness::*;

pub const UNIT: &str = "nix-grpc-daemon";

pub const CLIENT: Node = Node("client");
pub const LB: Node = Node("lb");
pub const WORKER1: Node = Node("worker1");
pub const WORKER2: Node = Node("worker2");
pub const WORKERS: [Node; 2] = [WORKER1, WORKER2];

pub const BUILDER: &[&str] = &["pgrep", "-f", "read -t [9]0 x"];

pub fn system() -> String {
    env("NGS_SYSTEM")
}

pub fn certs() -> String {
    env("NGS_CERTS")
}

pub fn expr(name: &str) -> String {
    env(&format!("NGS_{name}_EXPR"))
}

pub fn ci() -> String {
    let c = certs();
    format!("client-cert={c}/ci.pem&client-key={c}/ci.key")
}

pub fn envoy() -> String {
    format!("grpc://lb:50051?{}", ci())
}

pub fn direct(w: Node) -> String {
    format!("grpc://{}:50051?{}&ca-cert={}/ca.pem", w.0, ci(), certs())
}

/// Options that send builds through the farm instead of building locally.
pub fn hook() -> Vec<String> {
    let sys = system();
    vec![
        "--max-jobs".into(),
        "0".into(),
        "--builders".into(),
        format!("{}&system={sys} {sys} - 4", envoy()),
    ]
}

pub fn build(store: &str, tag: &str, extra: &[&str]) -> String {
    let job = expr("JOB");
    let mut args = vec!["--argstr", "tag", tag];
    args.extend_from_slice(extra);
    // nix prints its log on stdout, which is not the path we want back.
    let mut cmd = vec![
        "sh",
        "-c",
        r#"exec "$@" >&2"#,
        "sh",
        "nix",
        "build",
        "-L",
        "--store",
        store,
        "--eval-store",
        "auto",
        "-f",
        &job,
    ];
    cmd.extend_from_slice(&args);
    succeed(CLIENT, &cmd);
    let mut cmd = vec!["nix", "eval", "--raw", "-f", &job];
    cmd.extend_from_slice(&args);
    cmd.push("outPath");
    succeed(CLIENT, &cmd).trim().to_string()
}

pub fn holders(path: &str) -> Vec<Node> {
    WORKERS
        .into_iter()
        .filter(|&w| run_t(w, &["test", "-e", path], 60).code == 0)
        .collect()
}

pub fn worker_metrics(w: Node) -> Vec<String> {
    crate::harness::metrics(w, 9464)
}

fn value(line: &str) -> i64 {
    line.split_whitespace()
        .last()
        .and_then(|v| v.parse::<f64>().ok())
        .unwrap_or(0.0) as i64
}

pub fn gauge(w: Node, name: &str) -> i64 {
    let prefix = format!("{name} ");
    worker_metrics(w)
        .iter()
        .find(|l| l.starts_with(&prefix))
        .map_or(0, |l| value(l))
}

pub fn gauge_sum(w: Node, label: &str) -> i64 {
    worker_metrics(w)
        .iter()
        .filter(|l| l.starts_with("nix_grpc_sched_system") && l.contains(label))
        .map(|l| value(l))
        .sum()
}

pub fn events(w: Node, kind: &str) -> i64 {
    gauge(w, &format!(r#"nix_grpc_events_total{{kind="{kind}"}}"#))
}

pub fn failures(reason: &str) -> i64 {
    WORKERS
        .into_iter()
        .map(|w| {
            gauge(
                w,
                &format!(r#"nix_grpc_build_failures_total{{reason="{reason}"}}"#),
            )
        })
        .sum()
}

pub fn sched_workers(w: Node) -> i64 {
    gauge(w, r#"nix_grpc_sched{kind="workers"}"#)
}

fn node_for(addr: &str) -> Option<Node> {
    let ip = addr.split(':').next()?;
    WORKERS
        .into_iter()
        .find(|w| env(&format!("NGS_{}_IP", w.0.to_uppercase())) == ip)
}

fn health(cluster: &str) -> Vec<(Node, String)> {
    succeed(LB, &["curl", "-sf", "localhost:9901/clusters"])
        .lines()
        .filter_map(|l| {
            let mut p = l.split("::");
            let (c, addr, key, flags) = (p.next()?, p.next()?, p.next()?, p.next()?);
            (c == cluster && key == "health_flags").then_some(())?;
            Some((node_for(addr)?, flags.to_string()))
        })
        .collect()
}

pub fn members(cluster: &str) -> Vec<Node> {
    health(cluster)
        .into_iter()
        .filter(|(_, f)| f == "healthy")
        .map(|(n, _)| n)
        .collect()
}

pub fn unhealthy(cluster: &str) -> Vec<Node> {
    health(cluster)
        .into_iter()
        .filter(|(_, f)| f.contains("failed_active_hc"))
        .map(|(n, _)| n)
        .collect()
}

pub fn wait_members(cluster: &str, want: &[Node]) {
    let mut want = want.to_vec();
    want.sort_by_key(|n| n.0);
    retry(
        90,
        &format!("healthy members of {cluster} are {want:?}"),
        || {
            let mut got = members(cluster);
            got.sort_by_key(|n| n.0);
            got == want
        },
    );
}

pub fn leader() -> Node {
    let down = unhealthy("sched");
    let up: Vec<Node> = WORKERS.into_iter().filter(|w| !down.contains(w)).collect();
    assert_eq!(up.len(), 1, "{up:?}");
    up[0]
}

pub fn standby() -> Node {
    if leader() == WORKER1 {
        WORKER2
    } else {
        WORKER1
    }
}

pub fn settled() -> bool {
    let down = unhealthy("sched");
    if down.len() != 1 {
        return false;
    }
    let ldr = if down[0] == WORKER1 { WORKER2 } else { WORKER1 };
    sched_workers(ldr) == 2
}

pub fn farm_ready() {
    static READY: Once = Once::new();
    READY.call_once(|| {
        wait_members(&system(), &WORKERS);
        retry(60, "one scheduler with both workers", settled);
    });
    // A test that stopped a daemon and then failed must not fail every later one.
    for w in WORKERS {
        run_t(
            w,
            &[
                "sh",
                "-c",
                "systemctl is-active -q nix-grpc-daemon.service || systemctl start nix-grpc-daemon.service",
            ],
            60,
        );
    }
    retry(60, "one scheduler with both workers", settled);
}

pub fn building() -> Vec<Node> {
    WORKERS
        .into_iter()
        .filter(|&w| run_t(w, BUILDER, 60).code == 0)
        .collect()
}

pub fn release_slow() {
    for w in WORKERS {
        run_t(w, &["pkill", "-f", "read -t [9]0 x"], 60);
    }
}

pub fn spawn_build(unit: &str, tag: &str) {
    succeed(
        CLIENT,
        &[
            "systemd-run",
            "--unit",
            unit,
            "nix",
            "build",
            "--store",
            &envoy(),
            "--eval-store",
            "auto",
            "-f",
            &expr("SLOW"),
            "--argstr",
            "tag",
            tag,
        ],
    );
}

pub fn wait_unit_done(units: &str, secs: u64) {
    let mut cmd = vec!["sh", "-c", r#"! systemctl is-active "$@""#, "sh"];
    cmd.extend(units.split_whitespace());
    wait_until_succeeds(CLIENT, &cmd, secs);
}

pub fn assert_unit_success(unit: &str) {
    let result = unit_result(CLIENT, unit);
    if result != "success" {
        let log = query(CLIENT, &["journalctl", "-u", unit]);
        panic!("{unit} ended with {result}\n{log}");
    }
}

pub fn wait_building() {
    retry(60, "a slow build is running", || !building().is_empty());
}

/// DEP's `input` for `tag`, needing vip, with other bytes on every build if
/// `nondet`: built on the client (which offers vip for that build only),
/// uploaded to worker1 alone, which publishes it to niks3, then deleted from
/// the client and worker1. Worker1 never asked a substituter for it. Returns
/// its store path.
pub fn input_only_in_the_cache(tag: &str, nondet: bool) -> String {
    let dep = expr("DEP");
    let inp = succeed(
        CLIENT,
        &[
            "nix-build",
            "--no-out-link",
            "--option",
            "extra-system-features",
            "vip",
            &dep,
            "-A",
            "input",
            "--argstr",
            "tag",
            tag,
            "--arg",
            "features",
            r#"["vip"]"#,
            "--arg",
            "nondet",
            if nondet { "true" } else { "false" },
        ],
    )
    .trim()
    .to_string();
    succeed(
        CLIENT,
        &[
            "nix",
            "copy",
            "--no-check-sigs",
            "--to",
            &direct(WORKER1),
            &inp,
        ],
    );
    succeed(CLIENT, &["nix-store", "--delete", &inp]);
    succeed(WORKER1, &["nix-store", "--delete", &inp]);
    assert!(holders(&inp).is_empty(), "{:?} hold {inp}", holders(&inp));
    inp
}

/// The NAR object the cache's narinfo for `path` names.
pub fn nar_of(path: &str) -> String {
    let hash = &path["/nix/store/".len()..][..32];
    let narinfo = succeed(
        LB,
        &["curl", "-sf", &format!("{}/{hash}.narinfo", env("NGS_NIKS3_URL"))],
    );
    narinfo
        .lines()
        .find_map(|l| l.strip_prefix("URL: "))
        .unwrap_or_else(|| panic!("no URL in the narinfo of {path}: {narinfo}"))
        .to_string()
}

/// The HTTP status the cache answers for `key`.
pub fn cache_status(key: &str) -> String {
    succeed(
        LB,
        &[
            "curl",
            "-s",
            "-o",
            "/dev/null",
            "-w",
            "%{http_code}",
            &format!("{}/{key}", env("NGS_NIKS3_URL")),
        ],
    )
}

/// Deletes `key` from the cache's bucket behind niks3's back, which keeps
/// listing it.
pub fn lose(key: &str) {
    succeed(
        LB,
        &[
            "sh",
            "-c",
            r#"curl -sf -X DELETE --aws-sigv4 aws:amz:us-east-1:s3 --user "$(cat /etc/niks3-test/s3-credentials)" "http://lb:9000/farm/$1""#,
            "sh",
            key,
        ],
    );
    assert_eq!(cache_status(key), "404", "{key} is still in the cache");
}

/// `nix build` of DEP's `job` for `tag` through `store`, its input needing
/// vip (and `nondet` as in input_only_in_the_cache) and the job
/// `job_features`.
pub fn build_dep_job(store: &str, tag: &str, job_features: &str, nondet: bool, secs: u64) -> Out {
    run_t(
        CLIENT,
        &[
            "nix",
            "build",
            "-L",
            "--no-link",
            "--print-out-paths",
            "--store",
            store,
            "--eval-store",
            "auto",
            "-f",
            &expr("DEP"),
            "job",
            "--argstr",
            "tag",
            tag,
            "--arg",
            "features",
            r#"["vip"]"#,
            "--arg",
            "jobFeatures",
            job_features,
            "--arg",
            "nondet",
            if nondet { "true" } else { "false" },
        ],
        secs,
    )
}

/// Starts `w`'s daemon again unless it runs, then waits for the farm to settle.
pub fn restart_daemon(w: Node) {
    wait_until_succeeds(
        w,
        &[
            "sh",
            "-c",
            "systemctl is-active nix-grpc-daemon.service || systemctl start nix-grpc-daemon.service",
        ],
        60,
    );
    wait_members(&system(), &WORKERS);
    retry(90, "scheduler settled", settled);
}
