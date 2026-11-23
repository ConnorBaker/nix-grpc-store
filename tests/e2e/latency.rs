//! netem on lo delays the gRPC TCP path (unix sockets are unaffected), so
//! one VM can measure how well the protocol hides RTT.
//!
//! The timings are printed, not asserted: on a loaded host they move for
//! reasons unrelated to the code. What is asserted are the steps that pay a
//! round trip each, counted by the daemon (RPCs by method, and the paths
//! their requests name) and the kernel (accepted TCP connections), and that
//! nothing goes through the Connect tunnel, whose worker-protocol round
//! trips the daemon cannot see.
//!
//! Not covered, because a stall changes the same counts or none: round trips
//! inside one stream (a client that waited for each NAR of a FetchNars
//! stream before taking the next would make the same RPCs over the same
//! connections), and how well the closure walk batches its QueryPathInfos.
//! Only the printed timings show them.
use std::collections::BTreeMap;

use crate::fixtures::*;
use crate::harness::{MACHINE, metrics};

const N_PATHS: usize = 200;
const N_DRVS: usize = 20;
const RTT_MS: u64 = 50;
const STORE_URI: &str = "grpc://127.0.0.1:50051?insecure=1";
const METRICS_PORT: u16 = 9464;
// The most of each RPC a download of the paths may make. FetchNars is one
// stream per NAR connection. QueryPathInfos is topoSortPaths' one prefetch
// plus the closure walk's batches: runInfoBatches sends whatever queries are
// waiting when it wakes, at least one, so a stalled walk lawfully takes up to
// one RPC per path. That bound cannot tell a stall from a client that stopped
// batching; only the printed timings can. Any other method, Connect
// included, is a step too many.
const DOWNLOAD_RPCS: [(&str, u64); 4] = [
    ("QueryMissing", 1),
    ("QueryValidPaths", 1),
    ("QueryPathInfos", N_PATHS as u64 + 1),
    ("FetchNars", 4),
];
// Each path exactly once in the closure walk (Nix's computeClosure visits a
// path once, and each query lands in exactly one batch) and once in
// topoSortPaths' prefetch, however the walk's queries are batched.
const DOWNLOAD_PATHS_ASKED: u64 = 2 * N_PATHS as u64;
const DOWNLOAD_CONNECTIONS: u64 = 5;
// What a build-remote helper does per derivation today, over two connections,
// the same in every run. Each helper asks about its own derivation alone, so
// there is nothing for batching to split: its one QueryPathInfos names one
// path. Any other method is a step too many. Lower these when a change saves
// a step.
const BUILD_RPCS_PER_DRV: [(&str, u64); 6] = [
    ("StoreInfo", 1),
    ("QueryValidPaths", 2),
    ("AddMultipleToStore", 1),
    ("BuildDerivation", 1),
    ("QueryPathInfos", 1),
    ("FetchNars", 1),
];
const BUILD_CONNECTIONS_PER_DRV: u64 = 2;
// What the client prints when it repeats a call after a transport failure
// (GrpcStore::retryingWithProgress, NarFetcher, buildAssigned, recover,
// logReconnect). None is allowed: a retry on loopback is a fault in itself.
const RETRY_MARKERS: [&str; 4] = [
    ", retrying",
    "asking the scheduler again",
    "uploading it again",
    ", reconnecting",
];

/// Runs `argv` on the VM and returns the elapsed seconds, so ssh latency stays out of it.
fn timed(argv: &[&str]) -> f64 {
    let script = r#"s=$(date +%s%N); rc=0; "$@" >/root/timed.log 2>&1 || rc=$?; e=$(date +%s%N);
                    echo $(( (e - s) / 1000000 )); exit $rc"#;
    let mut cmd = vec!["sh", "-c", script, "sh"];
    cmd.extend(argv);
    let o = run(&cmd);
    assert_eq!(
        o.code,
        0,
        "{argv:?}\n{}",
        succeed(&["tail", "-n", "30", "/root/timed.log"])
    );
    let ms: f64 = o.stdout.trim().parse().expect("elapsed ms");
    ms / 1000.0
}

/// TCP connections the VM's kernel has accepted so far.
fn tcp_accepts() -> u64 {
    let snmp = succeed(&["cat", "/proc/net/snmp"]);
    let mut tcp = snmp.lines().filter(|l| l.starts_with("Tcp:"));
    let (names, values) = (
        tcp.next().expect("Tcp header"),
        tcp.next().expect("Tcp values"),
    );
    names
        .split_whitespace()
        .zip(values.split_whitespace())
        .find(|(name, _)| *name == "PassiveOpens")
        .and_then(|(_, v)| v.parse().ok())
        .expect("PassiveOpens")
}

/// The daemon's RPCs handled and paths asked so far, each by method.
fn daemon_counts() -> [BTreeMap<String, u64>; 2] {
    let lines = metrics(MACHINE, METRICS_PORT);
    ["nix_grpc_rpcs_total{", "nix_grpc_paths_asked_total{"].map(|family| by_method(&lines, family))
}

fn by_method(lines: &[String], family: &str) -> BTreeMap<String, u64> {
    let mut rpcs = BTreeMap::new();
    for line in lines {
        let Some(rest) = line.strip_prefix(family) else {
            continue;
        };
        let method = rest
            .split_once(r#"method=""#)
            .and_then(|(_, m)| m.split('"').next())
            .expect("method label");
        let count: f64 = line
            .split_whitespace()
            .last()
            .and_then(|v| v.parse().ok())
            .expect("counter value");
        *rpcs.entry(method.to_string()).or_default() += count as u64;
    }
    rpcs
}

/// The round-trip-paying steps of one command.
struct Steps {
    rpcs: BTreeMap<String, u64>,
    paths_asked: BTreeMap<String, u64>,
    connections: u64,
    /// The client's log lines about repeated calls.
    retries: Vec<String>,
}

/// Runs `argv` as `timed` does and counts its steps. The metrics scrapes,
/// however often the harness repeats them, happen outside the window of the
/// TCP counter, so it counts the command's connections only.
fn counted(argv: &[&str]) -> (f64, Steps) {
    let before = daemon_counts();
    let accepts = tcp_accepts();
    let t = timed(argv);
    let connections = tcp_accepts() - accepts;
    let retries = succeed(&["cat", "/root/timed.log"])
        .lines()
        .filter(|l| RETRY_MARKERS.iter().any(|m| l.contains(m)))
        .map(String::from)
        .collect();
    let [rpcs, paths_asked] = daemon_counts()
        .into_iter()
        .zip(before)
        .map(|(after, before)| {
            after
                .into_iter()
                .map(|(m, n)| {
                    let n = n - before.get(&m).copied().unwrap_or(0);
                    (m, n)
                })
                .filter(|(_, n)| *n > 0)
                .collect()
        })
        .collect::<Vec<_>>()
        .try_into()
        .expect("two families");
    let steps = Steps {
        rpcs,
        paths_asked,
        connections,
        retries,
    };
    (t, steps)
}

impl Steps {
    /// No call repeated: on loopback a retry is a fault in itself.
    fn assert_no_retries(&self, what: &str) {
        assert!(
            self.retries.is_empty(),
            "{what} repeated calls: {:?}; it made {:?} RPCs over {} connections",
            self.retries,
            self.rpcs,
            self.connections
        );
    }

    /// Each method at most `n` times as often as `allowed` says; one it does
    /// not list, at most never.
    fn assert_methods(&self, what: &str, allowed: &[(&str, u64)], n: u64) {
        for (method, &made) in &self.rpcs {
            let most = n * allowed
                .iter()
                .find(|(m, _)| m == method)
                .map_or(0, |(_, most)| *most);
            let why = if method == "Connect" {
                ", the Connect tunnel hides round trips from these counts"
            } else {
                ""
            };
            assert!(
                made <= most,
                "{what} made {made} {method} RPCs, expected at most {most}{why}: {:?}",
                self.rpcs
            );
        }
    }

    /// DOWNLOAD_PATHS_ASKED paths asked, and each method at most as often as
    /// DOWNLOAD_RPCS allows.
    fn assert_download(&self, what: &str) {
        self.assert_no_retries(what);
        let asked = self.paths_asked.get("QueryPathInfos").copied().unwrap_or(0);
        assert_eq!(
            asked, DOWNLOAD_PATHS_ASKED,
            "{what} asked QueryPathInfos for {asked} paths, expected {DOWNLOAD_PATHS_ASKED}: {:?}",
            self.rpcs
        );
        self.assert_methods(what, &DOWNLOAD_RPCS, 1);
        assert!(
            self.connections <= DOWNLOAD_CONNECTIONS,
            "{what} opened {} connections, expected at most {DOWNLOAD_CONNECTIONS}",
            self.connections
        );
    }

    /// One path asked per derivation, each method at most as often per
    /// derivation as BUILD_RPCS_PER_DRV allows, over at most
    /// BUILD_CONNECTIONS_PER_DRV connections each.
    fn assert_build(&self, what: &str) {
        self.assert_no_retries(what);
        let drvs = N_DRVS as u64;
        let asked = self.paths_asked.get("QueryPathInfos").copied().unwrap_or(0);
        assert_eq!(
            asked, drvs,
            "{what} asked QueryPathInfos for {asked} paths, expected one per derivation: {:?}",
            self.rpcs
        );
        self.assert_methods(what, &BUILD_RPCS_PER_DRV, drvs);
        let connections = drvs * BUILD_CONNECTIONS_PER_DRV;
        assert!(
            self.connections <= connections,
            "{what} opened {} connections, expected at most {connections}",
            self.connections
        );
    }
}

struct Netem;

impl Netem {
    fn add(one_way_ms: u64) -> Netem {
        succeed(&[
            "tc",
            "qdisc",
            "add",
            "dev",
            "lo",
            "root",
            "netem",
            "delay",
            &format!("{one_way_ms}ms"),
        ]);
        Netem
    }
}

impl Drop for Netem {
    fn drop(&mut self) {
        let _ = run(&["tc", "qdisc", "del", "dev", "lo", "root", "netem"]);
    }
}

#[test]
fn copy_and_remote_build_hide_the_round_trip() {
    wait_for_unit("nix-grpc-daemon.socket");
    wait_for_open_port(50051);
    let chain = env("NGS_CHAIN_EXPR");
    let system = env("NGS_SYSTEM");

    succeed(&["mkdir", "-p", "/root/small"]);
    let files: Vec<String> = (0..N_PATHS).map(|i| format!("/root/small/f{i}")).collect();
    random_files(&files, 4096);
    let mut add = vec!["nix-store", "--store", "/root/src", "--add"];
    add.extend(files.iter().map(String::as_str));
    let added = succeed(&add);
    let paths: Vec<&str> = added.lines().collect();
    assert_eq!(paths.len(), N_PATHS);
    let mut copy = vec![
        "nix",
        "copy",
        "--no-check-sigs",
        "--from",
        "/root/src",
        "--to",
        STORE_URI,
    ];
    copy.extend(&paths);
    succeed(&copy);

    let download = |label: &str| -> (f64, Steps) {
        succeed(&["rm", "-rf", "/root/dst"]);
        let mut copy = vec![
            "nix",
            "copy",
            "--no-check-sigs",
            "--from",
            STORE_URI,
            "--to",
            "/root/dst",
        ];
        copy.extend(&paths);
        let (t, steps) = counted(&copy);
        println!(
            "[bench] download/{label:9} {t:6.2}s ({N_PATHS} paths) rpcs {:?} paths asked {:?} connections {} retries {}",
            steps.rpcs,
            steps.paths_asked,
            steps.connections,
            steps.retries.len()
        );
        (t, steps)
    };
    let build = |label: &str| -> (f64, Steps) {
        let (t, steps) = counted(&[
            "nix",
            "build",
            "-f",
            &chain,
            "--argstr",
            "salt",
            label,
            "--store",
            "/root/bstore",
            "--no-link",
            "--max-jobs",
            "0",
            "--builders",
            &format!("{STORE_URI} {system} - 4 1"),
        ]);
        println!(
            "[bench] build/{label:12} {t:6.2}s ({N_DRVS} drvs) rpcs {:?} paths asked {:?} connections {} retries {}",
            steps.rpcs,
            steps.paths_asked,
            steps.connections,
            steps.retries.len()
        );
        (t, steps)
    };

    let loopback = (download("loopback"), build("loopback"));
    let build_base = loopback.1.0;
    let delayed = {
        let _netem = Netem::add(RTT_MS / 2);
        (download("50ms-rtt"), build("delayed"))
    };
    let (copy_delayed, build_delayed) = (delayed.0.0, delayed.1.0);

    // A client paying one round trip per path would need at least this long.
    let floor = N_PATHS as f64 * RTT_MS as f64 / 1000.0;
    println!("[bench] sequential floor {floor:6.2}s, delayed download {copy_delayed:6.2}s");
    let per_drv = (build_delayed - build_base) / N_DRVS as f64;
    println!(
        "[bench] build RTT overhead {:6.0}ms per drv",
        per_drv * 1000.0
    );

    for (pass, ((_, copy), (_, remote))) in [("loopback", &loopback), ("50ms-rtt", &delayed)] {
        // The download pays a fixed number of round trips, none per path.
        copy.assert_download(&format!("{pass} download of {N_PATHS} paths"));
        // A remote build pays a fixed number of round trips per derivation.
        remote.assert_build(&format!("{pass} remote build of {N_DRVS} derivations"));
    }
}
