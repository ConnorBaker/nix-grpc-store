use std::time::Instant;

use super::helpers::*;
use crate::harness::*;

const UNIT: &str = "nix-grpc-daemon";

fn probe(query: &str) -> String {
    let system = succeed(CLIENT, &["readlink", "-f", "/run/current-system"]);
    let o = run_t(
        CLIENT,
        &[
            "nix",
            "path-info",
            "--store",
            &format!("grpc://lb:50051?{query}"),
            system.trim(),
        ],
        120,
    );
    let out = o.combined();
    if o.code == 0 || out.contains("is not valid") {
        "ok".into()
    } else {
        out
    }
}

#[test]
fn balancer_auth_client_cert_token_nothing_unknown_cn() {
    farm_ready();
    let certs = certs();
    let aud = env("NGS_OIDC_AUDIENCE");
    assert_eq!(probe(&ci()), "ok");
    succeed(
        CLIENT,
        &[
            "sh",
            "-c",
            r#"curl -sfG "$@" > /root/dev.jwt && test -s /root/dev.jwt"#,
            "sh",
            "http://lb:8081/issue",
            "--data-urlencode",
            &format!("aud={aud}"),
            "--data-urlencode",
            "sub=dev:alice",
        ],
    );
    for (from, to) in [("foreign.pem", "client.crt"), ("foreign.key", "client.key")] {
        succeed(
            CLIENT,
            &[
                "install",
                "-D",
                &format!("{certs}/{from}"),
                &format!("/var/lib/nix-grpc-store/{to}"),
            ],
        );
    }
    let out = probe("token-file=/root/dev.jwt");
    succeed(CLIENT, &["rm", "-r", "/var/lib/nix-grpc-store"]);
    assert_eq!(out, "ok");
    let out = probe("");
    assert!(out.contains("client certificate or bearer token"), "{out}");
    let out = probe(&format!(
        "client-cert={certs}/stranger.pem&client-key={certs}/stranger.key"
    ));
    assert!(out.contains("no access rule matches 'stranger'"), "{out}");
}

#[test]
fn one_node_schedules_and_both_builders_hold_a_worker_session() {
    farm_ready();
    let ldr = leader();
    let opened = journal_matches(ldr, "nix-grpc-daemon", "event=worker_session_open").join("\n");
    assert!(
        opened.contains("cn=worker-1 ") && opened.contains("cn=oidc:mock:node:worker2 "),
        "worker sessions opened on the leader:\n{opened}"
    );
    assert_eq!(gauge(ldr, r#"nix_grpc_sched{kind="leader"}"#), 1);
    assert_eq!(gauge(standby(), r#"nix_grpc_sched{kind="leader"}"#), 0);
    let slots = gauge_sum(ldr, r#"kind="slots""#);
    assert!(slots >= 2, "{slots}");
    assert_eq!(gauge_sum(ldr, r#"kind="free""#), slots);
    assert_eq!(gauge_sum(ldr, r#"kind="queued""#), 0);
}

fn input_counts() -> (i64, i64) {
    WORKERS.into_iter().fold((0, 0), |(inputs, builds), w| {
        (
            inputs
                + gauge(w, r#"nix_grpc_build_inputs_total{state="local"}"#)
                + gauge(w, r#"nix_grpc_build_inputs_total{state="fetched"}"#),
            builds + gauge(w, "nix_grpc_build_input_seconds_count"),
        )
    })
}

#[test]
fn dag_build_is_scheduled_across_workers_and_lands_in_the_cache() {
    farm_ready();
    let (inputs0, builds0) = input_counts();
    let top = build(&envoy(), "t1", &[]);
    assert_eq!(holders(&top).len(), 1, "{:?}", holders(&top));
    fail(CLIENT, &["test", "-e", &top]);
    succeed(
        CLIENT,
        &[
            "nix",
            "copy",
            "--from",
            &env("NGS_NIKS3_URL"),
            "--no-check-sigs",
            &top,
        ],
    );
    let content = succeed(CLIENT, &["cat", &top]);
    assert!(content.contains("farm-top-t1"), "{top} holds: {content}");
    let (inputs1, builds1) = input_counts();
    assert!(inputs1 - inputs0 >= 2, "{inputs0} -> {inputs1}");
    assert_eq!(builds1 - builds0, 3);
}

#[test]
fn repeat_is_answered_from_the_cache_without_building() {
    farm_ready();
    let top = build(&envoy(), "cached1", &[]);
    for w in WORKERS {
        // The client's session still roots it on the builder: end that
        // nix-daemon connection, as its expiry would.
        let script = format!(
            "nix-store -q --roots {top} | grep -o 'temp:[0-9]*' | cut -d: -f2 | xargs -r kill -9; \
             for _ in $(seq 50); do nix-store --delete {top} && exit; sleep 0.2; done; exit 1"
        );
        succeed(w, &["sh", "-c", &script]);
    }
    let (_, builds) = input_counts();
    build(&envoy(), "cached1", &[]);
    assert!(holders(&top).is_empty(), "{:?}", holders(&top));
    assert_eq!(input_counts().1, builds);
}

#[test]
fn inputs_already_in_the_cache_are_not_sent_to_the_scheduler() {
    farm_ready();
    build(&envoy(), "wants1", &[]);
    let wants = || journal_count(leader(), UNIT, "event=want ");
    let before = wants();
    build(&envoy(), "wants1", &["--argstr", "top", "wants1b"]);
    assert_eq!(wants() - before, 1);
}

#[test]
fn an_output_a_substituter_offers_unsigned_is_built() {
    farm_ready();
    let dep = expr("DEP");
    let offer = env("NGS_OFFER_URL");
    let tag = "offered1";
    // Built on the client only, then offered unsigned and deleted there.
    let out = succeed(
        CLIENT,
        &[
            "nix-build",
            "--no-out-link",
            &dep,
            "-A",
            "input",
            "--argstr",
            "tag",
            tag,
        ],
    );
    let out = out.trim();
    succeed(CLIENT, &["nix", "copy", "--to", &offer, out]);
    succeed(CLIENT, &["nix-store", "--delete", out]);
    for w in WORKERS {
        fail(w, &["test", "-e", out]);
        let dry = succeed(
            w,
            &[
                "sh",
                "-c",
                r#"NIX_REMOTE=daemon nix-store --realise --dry-run "$1" 2>&1"#,
                "sh",
                out,
            ],
        );
        assert!(
            dry.contains("will be fetched"),
            "{} is not offered {out}: {dry}",
            w.0
        );
    }
    let said = run_t(
        CLIENT,
        &["nix-store", "--store", &envoy(), "--check-validity", out],
        120,
    );
    eprintln!(
        "before the build, the farm answered {out} (exit {}): {}",
        said.code,
        said.combined()
    );
    let wants = || journal_count(leader(), UNIT, "event=want ");
    let before = wants();
    succeed(
        CLIENT,
        &[
            "sh",
            "-c",
            r#"exec "$@" >&2"#,
            "sh",
            "nix",
            "build",
            "-L",
            "--no-link",
            "--store",
            &envoy(),
            "--eval-store",
            "auto",
            "-f",
            &dep,
            "input",
            "--argstr",
            "tag",
            tag,
        ],
    );
    let wanted = wants().saturating_sub(before);
    // The builders cannot fetch an unsigned path, so the build must have made it.
    let got = run_t(CLIENT, &["nix", "path-info", "--store", &envoy(), out], 120);
    assert_eq!(
        got.code,
        0,
        "the build succeeded, but the farm cannot give {out} ({wanted} wants sent): {}",
        got.combined()
    );
    assert!(
        !holders(out).is_empty(),
        "no builder holds {out} ({wanted} wants sent)"
    );
}

#[test]
fn two_clients_wanting_the_same_drv_build_it_once() {
    farm_ready();
    let attached = || {
        WORKERS
            .into_iter()
            .map(|w| events(w, "attached"))
            .sum::<i64>()
    };
    let before = attached();
    spawn_build("dedup1", "dedup");
    sleep(1);
    spawn_build("dedup2", "dedup");
    wait_building();
    sleep(2);
    release_slow();
    wait_unit_done("dedup1 dedup2", 60);
    for unit in ["dedup1", "dedup2"] {
        assert_unit_success(unit);
    }
    let mut ids: Vec<Vec<String>> = WORKERS
        .into_iter()
        .map(|w| {
            query(w, &["journalctl", "-u", "nix-grpc-daemon", "-o", "cat"])
                .lines()
                .filter(|l| l.contains("method=BuildDerivation") && l.contains("slow-dedup"))
                .filter_map(|l| l.split_whitespace().find(|t| t.starts_with("assign_id=")))
                .map(String::from)
                .collect()
        })
        .collect();
    ids.sort_by_key(Vec::len);
    let (idle, busy) = (&ids[0], &ids[1]);
    assert!(
        idle.is_empty() && busy.len() == 2 && busy[0] == busy[1],
        "{ids:?}"
    );
    assert_eq!(attached() - before, 1);
}

#[test]
fn timeout_and_max_silent_time_reach_the_builder() {
    farm_ready();
    let before = failures("TimedOut");
    for (flag, tag) in [("--timeout", "tmot"), ("--max-silent-time", "tmom")] {
        let t0 = Instant::now();
        let o = run_t(
            CLIENT,
            &[
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
                flag,
                "5",
            ],
            120,
        );
        let out = o.combined();
        assert!(
            o.code != 0 && out.contains("timed out") && t0.elapsed().as_secs() < 60,
            "{flag} {} {out}",
            o.code
        );
    }
    assert!(building().is_empty());
    assert_eq!(failures("TimedOut") - before, 2);
}

#[test]
fn a_failing_build_is_counted_with_its_own_reason() {
    farm_ready();
    let failed = || failures("PermanentFailure");
    let build_failed = || {
        WORKERS
            .into_iter()
            .map(|w| events(w, "build_failed"))
            .sum::<i64>()
    };
    let (before, events_before) = (failed(), build_failed());
    let o = run_t(
        CLIENT,
        &[
            "nix",
            "build",
            "-L",
            "--store",
            &envoy(),
            "--eval-store",
            "auto",
            "-f",
            &expr("FAIL"),
            "--argstr",
            "tag",
            "boom",
        ],
        120,
    );
    assert!(
        o.code != 0 && o.combined().contains("BOOM-boom"),
        "{}",
        o.combined()
    );
    assert_eq!(failed() - before, 1);
    assert_eq!(build_failed() - events_before, 1);
}

#[test]
fn the_first_client_leaving_does_not_fail_the_second() {
    farm_ready();
    spawn_build("share1", "share");
    sleep(1);
    spawn_build("share2", "share");
    wait_building();
    sleep(2);
    succeed(CLIENT, &["systemctl", "kill", "-s", "INT", "share1"]);
    wait_unit_done("share1", 30);
    wait_building();
    release_slow();
    wait_unit_done("share2", 120);
    assert_unit_success("share2");
}

#[test]
fn upload_whose_reference_the_worker_lacks_is_completed_from_the_cache() {
    farm_ready();
    let dep = expr("DEP");
    let sys = system();
    let nix_build = |attr: &str| {
        succeed(
            CLIENT,
            &[
                "nix-build",
                "--no-out-link",
                &dep,
                "-A",
                attr,
                "--argstr",
                "tag",
                "viacache",
            ],
        )
        .trim()
        .to_string()
    };
    let (rf, referrer) = (nix_build("input"), nix_build("referrer"));
    succeed(
        CLIENT,
        &[
            "sh",
            "-c",
            r#"nix-store --export "$1" > /tmp/shared/ref.closure"#,
            "sh",
            &rf,
        ],
    );
    succeed(
        WORKER1,
        &["sh", "-c", "nix-store --import < /tmp/shared/ref.closure"],
    );
    succeed(WORKER2, &["systemctl", "stop", "nix-grpc-daemon.service"]);
    wait_members(&sys, &[WORKER1]);
    succeed(CLIENT, &["nix", "path-info", "--store", &envoy(), &rf]);
    fail(WORKER2, &["test", "-e", &rf]);
    succeed(WORKER2, &["systemctl", "start", "nix-grpc-daemon.service"]);
    succeed(WORKER1, &["systemctl", "stop", "nix-grpc-daemon.service"]);
    wait_members(&sys, &[WORKER2]);
    succeed(
        CLIENT,
        &[
            "nix",
            "copy",
            "--no-check-sigs",
            "--to",
            &envoy(),
            &referrer,
        ],
    );
    succeed(WORKER2, &["test", "-e", &rf]);
    succeed(WORKER2, &["test", "-e", &referrer]);
    succeed(WORKER1, &["systemctl", "start", "nix-grpc-daemon.service"]);
    wait_members(&sys, &WORKERS);
}

/// Deletes the NAR of `path` from the cache's bucket behind niks3's back,
/// which keeps listing the path.
fn lose_nar(path: &str) {
    succeed(
        LB,
        &[
            "sh",
            "-c",
            r#"set -e
nar=$(curl -sf "$1/$2.narinfo" | sed -n 's/^URL: //p')
test -n "$nar"
curl -sf -X DELETE --aws-sigv4 aws:amz:us-east-1:s3 --user "$(cat /etc/niks3-test/s3-credentials)" "http://lb:9000/farm/$nar"
test "$(curl -s -o /dev/null -w '%{http_code}' "$1/$nar")" = 404"#,
            "sh",
            &env("NGS_NIKS3_URL"),
            &path["/nix/store/".len()..][..32],
        ],
    );
}

/// An input that niks3 lists but whose NAR the cache lost is built again in
/// the run that needs it, and published so that every worker can fetch it.
/// Its users, one on each worker, miss it at the same time and both wait.
#[test]
fn an_input_whose_nar_the_cache_lost_is_built_again() {
    users_of_a_lost_input("lostnar", false);
}

/// As above, but the input has other bytes on every build, so niks3 puts up
/// a narinfo for the rebuild's NAR. worker2's nix-daemon, which kept the old
/// narinfo when it missed the input, looks the input up again.
#[test]
fn an_input_whose_nar_the_cache_lost_is_built_again_with_other_bytes() {
    users_of_a_lost_input("lostnondet", true);
}

fn users_of_a_lost_input(tag: &str, nondet: bool) {
    farm_ready();
    let dep = expr("DEP");
    let nondet = if nondet { "true" } else { "false" };
    // Only worker1 can build it (vip). It reaches niks3 through worker1.
    let input = succeed(
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
            nondet,
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
            &input,
        ],
    );
    succeed(CLIENT, &["nix-store", "--delete", &input]);
    succeed(WORKER1, &["nix-store", "--delete", &input]);
    lose_nar(&input);
    let missed = |w| events(w, "input_not_substitutable");
    let (missed1, missed2) = (missed(WORKER1), missed(WORKER2));
    let users = format!(
        r#"map (f: (import {dep} {{ tag = "{tag}"; salt = f; features = [ "vip" ]; jobFeatures = [ f ]; nondet = {nondet}; }}).job) [ "vip" "w2" ]"#
    );
    let o = run_t(
        CLIENT,
        &[
            "nix",
            "build",
            "-L",
            "--no-link",
            "--store",
            &envoy(),
            "--eval-store",
            "auto",
            "--impure",
            "--expr",
            &users,
        ],
        300,
    );
    assert_eq!(o.code, 0, "{}", o.combined());
    assert!(
        missed(WORKER1) > missed1 && missed(WORKER2) > missed2,
        "not both users missed {input}"
    );
}

#[test]
fn a_niks3_restart_does_not_cost_the_leader_its_role() {
    farm_ready();
    let was = leader();
    let yields = || journal_count(was, UNIT, "event=scheduler_yield");
    let before = yields();
    succeed(LB, &["systemctl", "restart", "niks3.service"]);
    wait_for_unit(LB, "niks3.service");
    retry(60, "scheduler settled", settled);
    assert_eq!(leader(), was);
    assert_eq!(yields(), before);
    build(&envoy(), "after-niks3-restart", &[]);
}

#[test]
fn scheduler_down_the_other_node_takes_the_lock_and_keeps_it() {
    farm_ready();
    let (was, nxt) = (leader(), standby());
    let pattern = "event=scheduler_take_over";
    let before = journal_count(nxt, UNIT, pattern);
    succeed(was, &["systemctl", "stop", "nix-grpc-daemon.service"]);
    wait_journal_above(nxt, UNIT, pattern, before, 30);
    retry(30, "one worker on the new scheduler", || {
        sched_workers(nxt) == 1
    });
    build(&envoy(), "on-standby", &[]);
    succeed(was, &["systemctl", "start", "nix-grpc-daemon.service"]);
    retry(60, "scheduler settled", settled);
    assert_eq!(leader(), nxt);
    assert_eq!(sched_workers(was), 0);
}

#[test]
fn clean_scheduler_restart_peers_are_told_and_running_builds_survive() {
    farm_ready();
    spawn_build("rs", "rs");
    wait_building();
    let who = building();
    let (was, nxt) = (leader(), standby());
    let pattern = "event=scheduler_restarting addr=";
    let before = journal_count(nxt, UNIT, pattern);
    // --no-block: a build on the leader drains first, so the restart only
    // completes after release_slow().
    succeed(
        was,
        &[
            "systemctl",
            "restart",
            "--no-block",
            "nix-grpc-daemon.service",
        ],
    );
    sleep(5);
    assert_eq!(building(), who, "restart killed or moved the build");
    release_slow();
    wait_journal_above(nxt, UNIT, pattern, before, 60);
    wait_unit_done("rs", 120);
    assert_unit_success("rs");
    assert_no_journal(CLIENT, "rs", "reconnecting");
    retry(60, "scheduler settled", settled);
}

#[test]
fn scheduler_restart_mid_build_finishes_without_a_double_build() {
    farm_ready();
    spawn_build("mid", "mid");
    wait_building();
    assert_eq!(building().len(), 1, "{:?}", building());
    // The scheduler forgets the build. If it ran on the leader it dies with
    // the daemon. Either way no second copy may start while one is alive.
    let ldr = leader();
    succeed(
        ldr,
        &["systemctl", "kill", "-s", "KILL", "nix-grpc-daemon.service"],
    );
    succeed(ldr, &["systemctl", "start", "nix-grpc-daemon.service"]);
    sleep(5);
    assert!(building().len() <= 1, "{:?}", building());
    release_slow();
    wait_unit_done("mid", 120);
    assert_unit_success("mid");
}

#[test]
fn interrupting_the_client_stops_the_build_on_the_worker() {
    farm_ready();
    spawn_build("intr", "intr");
    wait_building();
    succeed(CLIENT, &["systemctl", "kill", "-s", "INT", "intr"]);
    retry(20, "the build stopped", || building().is_empty());
}

#[test]
fn a_deploy_mid_build_drains_and_the_new_generation_takes_over() {
    farm_ready();
    spawn_build("sw", "sw");
    wait_building();
    let busy = if building() == [WORKER1] {
        WORKER1
    } else {
        WORKER2
    };
    if busy == WORKER2 {
        succeed(
            WORKER2,
            &[
                "timeout",
                "120",
                "/run/current-system/specialisation/next/bin/switch-to-configuration",
                "test",
            ],
        );
    } else {
        succeed(WORKER1, &["systemctl", "reload", "nix-grpc-daemon.service"]);
    }
    succeed(busy, BUILDER);
    build(&envoy(), "during-drain", &[]);
    succeed(busy, &["pkill", "-f", "read -t [9]0 x"]);
    wait_unit_done("sw", 90);
    assert_unit_success("sw");
    wait_until_succeeds(
        busy,
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

#[test]
fn queue_gauges_show_a_burst_while_every_build_blocks() {
    farm_ready();
    let sys = system();
    let queued = || gauge_sum(leader(), r#"kind="queued""#);
    let slots = gauge_sum(leader(), r#"kind="slots""#);
    let tags = (0..slots + 2)
        .map(|i| format!("\"q{i}\""))
        .collect::<Vec<_>>()
        .join(" ");
    let burst = format!(
        "map (tag: import {} {{ inherit tag; }}) [ {tags} ]",
        expr("SLOW")
    );
    succeed(
        CLIENT,
        &[
            "systemd-run",
            "--unit",
            "burst",
            "nix",
            "build",
            "--store",
            &envoy(),
            "--eval-store",
            "auto",
            "--impure",
            "--expr",
            &burst,
        ],
    );
    // The Wants arrive within a second and nothing else happens until a build
    // ends, so only the trailing export can show the queue.
    retry(10, "two builds queued", || queued() == 2);
    retry(90, "burst finished", || {
        release_slow();
        run_t(CLIENT, &["systemctl", "is-active", "burst"], 60).code != 0
    });
    assert_unit_success("burst");
    retry(10, "queue empty", || queued() == 0);
    let waits = gauge(
        leader(),
        &format!(r#"nix_grpc_queue_seconds_count{{system="{sys}"}}"#),
    );
    assert!(waits >= slots + 2, "{waits}");
}

fn has_vip_build_info(line: &str) -> bool {
    line.starts_with("nix_grpc_build_info{")
        && line.ends_with("} 1")
        && line.contains(r#"worker="node-a""#)
        && line
            .split_once(r#"features=""#)
            .is_some_and(|(_, rest)| rest.split('"').next().unwrap_or("").contains("vip"))
}

#[test]
fn required_system_features_are_placed_or_refused() {
    farm_ready();
    let job = expr("JOB");
    let vip = format!(
        r#"map (tag: import {job} {{ inherit tag; features = ["vip"]; }}) ["f1" "f2" "f3"]"#
    );
    let o = run_t(
        CLIENT,
        &[
            "nix",
            "build",
            "-L",
            "--store",
            &envoy(),
            "--eval-store",
            "auto",
            "--expr",
            &vip,
            "--impure",
        ],
        300,
    );
    assert_eq!(o.code, 0, "{}", o.combined());
    let out = o.combined();
    assert!(out.contains("node-a: building "), "{out}");
    assert!(
        out.lines()
            .filter(|l| l.contains(": building"))
            .all(|l| l.contains("node-a")),
        "{out}"
    );
    assert!(
        worker_metrics(WORKER1)
            .iter()
            .any(|l| has_vip_build_info(l))
    );

    succeed(
        CLIENT,
        &[
            "systemd-run",
            "--unit",
            "gpu",
            "nix",
            "build",
            "-L",
            "--store",
            &format!("{}&restart-grace=30", envoy()),
            "--eval-store",
            "auto",
            "-f",
            &job,
            "--argstr",
            "tag",
            "f5",
            "--arg",
            "features",
            r#"["gpu"]"#,
        ],
    );
    let unplaceable = format!(
        r#"nix_grpc_sched_system{{features="gpu",kind="unplaceable",system="{}"}}"#,
        system()
    );
    retry(60, "one unplaceable build", || {
        gauge(leader(), &unplaceable) == 1
    });
    retry(60, "gpu build failed", || {
        unit_result(CLIENT, "gpu") == "exit-code"
    });
    let out = query(CLIENT, &["journalctl", "-u", "gpu", "-o", "cat"]);
    assert!(out.contains("features {gpu}"), "{out}");
    retry(30, "no unplaceable build", || {
        gauge(leader(), &unplaceable) == 0
    });
}

#[test]
fn low_disk_drains_a_worker_and_builds_go_to_the_other() {
    farm_ready();
    let count = |p: &str| journal_count(WORKER1, UNIT, p);
    let (down, up) = (
        "event=unhealthy reason=min_free",
        "event=healthy reason=min_free",
    );
    let (down0, up0) = (count(down), count(up));
    let avail = succeed(WORKER1, &["df", "--output=avail", "-B1", "/nix/store"]);
    let avail: u64 = avail.lines().last().unwrap().trim().parse().unwrap();
    succeed(
        WORKER1,
        &[
            "fallocate",
            "-l",
            &(avail - 100 * 1024 * 1024).to_string(),
            "/nix/.rw-store/fill",
        ],
    );
    wait_journal_above(WORKER1, UNIT, down, down0, 30);
    let top = build(&envoy(), "drain", &[]);
    assert_eq!(holders(&top), [WORKER2]);
    succeed(WORKER1, &["rm", "/nix/.rw-store/fill"]);
    wait_journal_above(WORKER1, UNIT, up, up0, 30);
}

/// Until this is dropped, the cache (port 5751) resets worker1's downloads
/// of one NAR as root, which its nix-daemon runs as. Everything else still
/// comes, and nix-grpc-daemon, another user, still reaches that NAR.
struct NarRefused(String);

impl NarRefused {
    fn new(nar: &str) -> Self {
        let refused = Self(format!("GET /{nar}"));
        for tables in ["iptables", "ip6tables"] {
            succeed(WORKER1, &refused.rule(tables, "-I"));
        }
        refused
    }

    fn rule<'a>(&'a self, tables: &'a str, op: &'a str) -> Vec<&'a str> {
        vec![
            tables,
            op,
            "OUTPUT",
            "-p",
            "tcp",
            "--dport",
            "5751",
            "-m",
            "owner",
            "--uid-owner",
            "0",
            "-m",
            "string",
            "--algo",
            "bm",
            "--string",
            self.0.as_str(),
            "-j",
            "REJECT",
            "--reject-with",
            "tcp-reset",
        ]
    }
}

impl Drop for NarRefused {
    fn drop(&mut self) {
        for tables in ["iptables", "ip6tables"] {
            run_t(WORKER1, &self.rule(tables, "-D"), 60);
        }
    }
}

#[test]
fn an_input_fetch_that_fails_is_given_back_not_rebuilt() {
    farm_ready();
    let tag = "fetchfail";
    let inp = input_only_in_the_cache(tag, false);
    let store = format!("{}&reschedule-retries=2", envoy());
    let count = |kind| events(WORKER1, kind);
    let (lost0, missed0) = (count("input_lost"), count("input_not_substitutable"));
    let failed = {
        let _refused = NarRefused::new(&nar_of(&inp));
        build_dep_job(&store, tag, r#"["vip"]"#, false, 240)
    };
    let failed_at = std::time::Instant::now();
    assert!(
        failed.code != 0 && failed.stderr.contains("gave up after 3 reschedules"),
        "{}",
        failed.combined()
    );
    // What nix says for a NAR the cache lost too.
    assert!(
        failed
            .stderr
            .contains("there is no substituter that can build it"),
        "not the NAR download that failed: {}",
        failed.stderr
    );
    assert!(
        !failed.stderr.contains("farm lacks"),
        "built anew on a fetch that failed: {}",
        failed.stderr
    );
    assert_eq!(count("input_lost"), lost0);
    // One per placement: 3 the client counts, and maybe one it does not, for
    // a message it reads as the worker going away: goneAway (retry.hh)
    // matches the "Connection reset" in nix's error.
    let missed = count("input_not_substitutable") - missed0;
    assert!((3..=4).contains(&missed), "{missed} placements missed the input");
    assert!(holders(&inp).is_empty(), "{:?}", holders(&inp));
    // Without the reset worker1 gets the input, no build of it needed: on a
    // connection of its own while the ones that failed still shun the cache.
    let mut last = String::new();
    retry(150, "the build gets the input again", || {
        let o = build_dep_job(&store, tag, r#"["vip"]"#, false, 120);
        last = o.stderr;
        o.code == 0
    });
    assert!(!last.contains("farm lacks"), "{last}");
    assert_eq!(holders(&inp), [WORKER1]);
    // nix shuns a cache for 60 s on a connection a download from it failed
    // on, and worker1's daemon keeps those connections: their queries fail
    // until then. Leave the farm as the next tests expect it.
    std::thread::sleep(std::time::Duration::from_secs(65).saturating_sub(failed_at.elapsed()));
}

/// An input that niks3 no longer lists, which another substituter offers but
/// worker1 cannot get from it, is given back, not named and built anew: the
/// cache did not lose it. niks3's GC takes it after the client counted the
/// farm holding it, while the job waits for worker1's one slot. (A client
/// that asked later would have had a builder fetch it from the other
/// substituter and publish it to niks3 again.) Named to run before
/// an_input_fetch_that_fails_is_given_back_not_rebuilt: if that one fails
/// early, worker1's connections still shun the cache for 60 s.
#[test]
fn a_root_niks3_dropped_after_planning_is_given_back_not_rebuilt() {
    farm_ready();
    let tag = "dropped";
    let inp = input_only_in_the_cache(tag, false);
    let narinfo = format!("{}.narinfo", &inp["/nix/store/".len()..][..32]);
    let nar = nar_of(&inp);
    // Offered by the other substituter too, signed with the cache's key,
    // which the builders trust.
    let offer = env("NGS_OFFER_URL");
    succeed(
        CLIENT,
        &[
            "nix",
            "copy",
            "--no-check-sigs",
            "--from",
            &env("NGS_NIKS3_URL"),
            "--to",
            &offer,
            &inp,
        ],
    );
    let slow = "dropped-slow";
    succeed(
        CLIENT,
        &[
            "systemd-run",
            "--unit",
            slow,
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
            slow,
            "--arg",
            "features",
            r#"["vip"]"#,
        ],
    );
    retry(60, "worker1 runs the slow build", || {
        building().contains(&WORKER1)
    });
    let job = "dropped-job";
    succeed(
        CLIENT,
        &[
            "systemd-run",
            "--unit",
            job,
            "nix",
            "build",
            "-L",
            "--no-link",
            "--store",
            &format!("{}&reschedule-retries=2", envoy()),
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
        ],
    );
    retry(60, "the job waits for worker1", || {
        gauge_sum(leader(), r#"kind="queued""#) >= 1
    });
    // niks3's GC takes it: no closure root any more, nothing in the bucket.
    succeed(
        LB,
        &[
            "runuser",
            "-u",
            "postgres",
            "--",
            "psql",
            "-d",
            "niks3",
            "-v",
            "ON_ERROR_STOP=1",
            "-c",
            &format!(
                "UPDATE objects SET deleted_at = timezone('UTC', now()), \
                 first_deleted_at = timezone('UTC', now()) WHERE key = '{narinfo}'"
            ),
        ],
    );
    lose(&narinfo);
    lose(&nar);
    // And no builder gets its NAR from the other substituter.
    succeed(
        CLIENT,
        &[
            "sh",
            "-c",
            r#"rm "$1/$(sed -n 's/^URL: //p' "$1/$2")""#,
            "sh",
            offer.trim_start_matches("file://"),
            &narinfo,
        ],
    );
    let count = |kind| events(WORKER1, kind);
    let (lost0, missed0) = (count("input_lost"), count("input_not_substitutable"));
    release_slow();
    wait_unit_done(&format!("{slow} {job}"), 240);
    let out = query(CLIENT, &["journalctl", "-u", job, "-o", "cat", "--no-pager"]);
    assert_ne!(unit_result(CLIENT, job), "success", "{out}");
    assert!(out.contains("gave up after 3 reschedules"), "{out}");
    assert!(
        out.contains("there is no substituter that can build it"),
        "not the input that failed: {out}"
    );
    assert!(!out.contains("farm lacks"), "built anew: {out}");
    assert_eq!(count("input_lost"), lost0);
    assert!(count("input_not_substitutable") - missed0 >= 3);
}

fn farm_gives(path: &str) {
    let got = run_t(CLIENT, &["nix", "path-info", "--store", &envoy(), path], 120);
    assert_eq!(
        got.code,
        0,
        "the farm cannot give {path}: {}",
        got.combined()
    );
}

/// An input the farm counts as held whose NAR the cache lost: the dependant
/// below `tag`'s job cannot get it anywhere. Returns its path and NAR.
fn lost_input(tag: &str, nondet: bool) -> (String, String) {
    let inp = input_only_in_the_cache(tag, nondet);
    let nar = nar_of(&inp);
    lose(&nar);
    // The farm still counts it as held: niks3 lists it as a closure root.
    succeed(
        CLIENT,
        &["nix-store", "--store", &envoy(), "--check-validity", &inp],
    );
    (inp, nar)
}

/// The same through nix's build hook (`--builders`), which hands the plugin
/// one derivation at a time: the job's lost input is found among its input
/// derivations and built on the farm, not uploaded again to a worker that
/// counts it as held.
#[test]
fn lost_input_is_built_for_a_build_hook_s_job() {
    farm_ready();
    let dep = expr("DEP");
    let eval = |tag: &str, attr: &str| {
        succeed(
            CLIENT,
            &[
                "nix", "eval", "--raw", "-f", &dep, attr, "--argstr", "tag", tag, "--arg", "features",
                r#"["vip"]"#, "--arg", "jobFeatures", "[]",
            ],
        )
        .trim()
        .to_string()
    };
    // A tag whose input's derivation sorts before the job's: the run then
    // holds the job behind its producer, and the hook's job is still the
    // one whose result the plugin returns.
    let base = |path: &str| path.rsplit('/').next().unwrap_or_default().to_string();
    let tag = (0..32)
        .map(|i| format!("losthook{i}"))
        .find(|tag| {
            let job = eval(tag, "job.drvPath");
            let refs = succeed(CLIENT, &["nix-store", "--query", "--references", &job]);
            refs.lines()
                .filter(|r| r.ends_with(&format!("-local-input-{tag}.drv")))
                .any(|r| base(r) < base(&job))
        })
        .expect("no tag whose input's derivation sorts first");
    let tag = tag.as_str();
    let (inp, nar) = lost_input(tag, false);
    // The client has it, built again here: the hook does not upload it,
    // since the farm counts it as held.
    let again = succeed(
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
        ],
    );
    assert_eq!(again.trim(), inp);
    let hook = hook();
    let mut cmd = vec![
        "sh",
        "-c",
        r#"NIX_REMOTE=daemon exec "$@""#,
        "sh",
        "nix",
        "build",
        "-L",
    ];
    cmd.extend(hook.iter().map(String::as_str));
    cmd.extend([
        "--print-out-paths",
        "--no-link",
        "-f",
        &dep,
        "job",
        "--argstr",
        "tag",
        tag,
        "--arg",
        "features",
        r#"["vip"]"#,
        "--arg",
        "jobFeatures",
        "[]",
    ]);
    let built = run_t(CLIENT, &cmd, 240);
    assert_eq!(built.code, 0, "the build failed: {}", built.combined());
    assert_eq!(built.stdout.trim(), eval(tag, "job.outPath"), "{}", built.combined());
    assert!(
        built.combined().contains(&format!("farm lacks '{inp}'")),
        "{inp} was not built anew: {}",
        built.combined()
    );
    assert_eq!(cache_status(&nar), "200");
}

#[test]
fn lost_input_is_built_for_its_user_on_the_same_worker() {
    farm_ready();
    let (inp, nar) = lost_input("lostsame", false);
    // Nothing drains here, so a give-back of worker1's own is a placement
    // that expired unclaimed (Builder::tick).
    let no_shows = || events(WORKER1, "give_back");
    let no_shows0 = no_shows();
    let built = build_dep_job(&envoy(), "lostsame", r#"["vip"]"#, false, 240);
    assert_eq!(built.code, 0, "the build failed: {}", built.stderr);
    assert!(
        built.stderr.contains(&format!("farm lacks '{inp}'")),
        "{inp} was not built anew: {}",
        built.stderr
    );
    assert_eq!(holders(&inp), [WORKER1]);
    // Published with niks3 checking the bucket, which took the NAR again.
    assert_eq!(cache_status(&nar), "200");
    // Worker1, the only vip builder, has one slot. Its user, given back and
    // placed again while it waited, was cancelled: the input's build did not
    // wait for that placement to expire.
    assert_eq!(no_shows() - no_shows0, 0, "a placement expired unused");
    farm_gives(built.stdout.trim());
}

#[test]
fn lost_input_rebuilt_on_one_worker_reaches_its_user_on_another() {
    farm_ready();
    let (inp, nar) = lost_input("lostcross", false);
    // The input builds on worker1 alone (vip), its user on worker2 alone (w2).
    let built = build_dep_job(&envoy(), "lostcross", r#"["w2"]"#, false, 240);
    assert_eq!(built.code, 0, "the build failed: {}", built.stderr);
    assert!(
        built.stderr.contains(&format!("farm lacks '{inp}'")),
        "{inp} was not built anew: {}",
        built.stderr
    );
    assert_eq!(cache_status(&nar), "200");
    // Built on worker1, fetched from the cache by worker2.
    assert_eq!(holders(&inp), WORKERS);
    farm_gives(built.stdout.trim());
}

#[test]
fn lost_input_rebuilt_with_other_bytes_reaches_its_user_on_another() {
    farm_ready();
    // As the test above, but the input has other bytes on every build, so
    // its rebuild's NAR is another object than the one the cache lost.
    let (inp, nar) = lost_input("lostbytes", true);
    let built = build_dep_job(&envoy(), "lostbytes", r#"["w2"]"#, true, 240);
    assert_eq!(built.code, 0, "the build failed: {}", built.stderr);
    assert!(
        built.stderr.contains(&format!("farm lacks '{inp}'")),
        "{inp} was not built anew: {}",
        built.stderr
    );
    // The cache's narinfo names the rebuild's NAR now, which it has.
    let rebuilt = nar_of(&inp);
    assert_ne!(rebuilt, nar, "the rebuild has the same bytes");
    assert_eq!(cache_status(&rebuilt), "200");
    assert_eq!(holders(&inp), WORKERS);
    farm_gives(built.stdout.trim());
}

#[test]
fn lost_input_missed_by_two_users_at_once_is_built_once() {
    farm_ready();
    let (inp, nar) = lost_input("losttwo", false);
    let lost = |w| events(w, "input_lost");
    let (lost1, lost2) = (lost(WORKER1), lost(WORKER2));
    let builds = || events(WORKER1, "built") + events(WORKER2, "built");
    let builds0 = builds();
    // One user per worker, so both try the input at once.
    let users = format!(
        r#"map (u: (import {} {{ tag = "losttwo"; salt = u.salt; features = [ "vip" ]; jobFeatures = u.features; }}).job) [ {{ salt = "1"; features = [ "vip" ]; }} {{ salt = "2"; features = [ "w2" ]; }} ]"#,
        expr("DEP")
    );
    let built = run_t(
        CLIENT,
        &[
            "nix",
            "build",
            "-L",
            "--no-link",
            "--print-out-paths",
            "--store",
            &envoy(),
            "--eval-store",
            "auto",
            "--impure",
            "--expr",
            &users,
        ],
        240,
    );
    assert_eq!(built.code, 0, "the build failed: {}", built.stderr);
    assert!(
        lost(WORKER1) > lost1 && lost(WORKER2) > lost2,
        "not both users missed {inp}"
    );
    // Each user that missed it says so, but the run builds it once: the
    // input and the two users are the only builds.
    assert!(
        built.stderr.contains(&format!("farm lacks '{inp}'")),
        "{inp} was not built anew: {}",
        built.stderr
    );
    let anew = builds() - builds0;
    assert_eq!(anew, 3, "{anew} builds for the input and its two users: {}", built.stderr);
    assert_eq!(cache_status(&nar), "200");
    let outs: Vec<&str> = built.stdout.split_whitespace().collect();
    assert_eq!(outs.len(), 2, "{}", built.stdout);
    for out in outs {
        farm_gives(out);
    }
}

#[test]
fn an_input_a_substituter_offers_unsigned_is_built() {
    farm_ready();
    let dep = expr("DEP");
    let offer = env("NGS_OFFER_URL");
    let tag = "offered2";
    // The input, built on the client only, then offered unsigned and deleted there.
    let inp = succeed(
        CLIENT,
        &[
            "nix-build",
            "--no-out-link",
            &dep,
            "-A",
            "input",
            "--argstr",
            "tag",
            tag,
        ],
    );
    let inp = inp.trim();
    succeed(CLIENT, &["nix", "copy", "--to", &offer, inp]);
    succeed(CLIENT, &["nix-store", "--delete", inp]);
    for w in WORKERS {
        fail(w, &["test", "-e", inp]);
        let dry = succeed(
            w,
            &[
                "sh",
                "-c",
                r#"NIX_REMOTE=daemon nix-store --realise --dry-run "$1" 2>&1"#,
                "sh",
                inp,
            ],
        );
        assert!(
            dry.contains("will be fetched"),
            "{} is not offered {inp}: {dry}",
            w.0
        );
    }
    // No builder can fetch the input, so the build must make it first.
    let built = run_t(
        CLIENT,
        &[
            "nix",
            "build",
            "-L",
            "--no-link",
            "--print-out-paths",
            "--store",
            &envoy(),
            "--eval-store",
            "auto",
            "-f",
            &dep,
            "job",
            "--argstr",
            "tag",
            tag,
        ],
        240,
    );
    assert_eq!(
        built.code, 0,
        "the build on an input offered unsigned failed: {}",
        built.stderr
    );
    for path in [inp, built.stdout.trim()] {
        let got = run_t(
            CLIENT,
            &["nix", "path-info", "--store", &envoy(), path],
            120,
        );
        assert_eq!(
            got.code,
            0,
            "the farm cannot give {path}: {}",
            got.combined()
        );
    }
}

#[test]
fn sighup_rereads_nix_max_jobs_and_the_scheduler_counts_it() {
    farm_ready();
    let pid = || query(WORKER1, &["systemctl", "show", "-p", "MainPID", "--value", UNIT]);
    let own = || gauge(WORKER1, "nix_grpc_build_slots");
    let total = || gauge_sum(leader(), r#"kind="slots""#);
    let hup = || {
        succeed(
            WORKER1,
            &["systemctl", "kill", "--kill-whom=main", "--signal=SIGHUP", UNIT],
        )
    };
    let reads = || journal_count(WORKER1, UNIT, "event=max_jobs ");
    let (pid0, own0, total0) = (pid(), own(), total());
    // worker1's nix.conf includes this file, as a Tribuchet hub's does the
    // fragment it rewrites (farm.nix).
    let conf = "/run/nix-max-jobs.conf";
    succeed(
        WORKER1,
        &[
            "sh",
            "-c",
            r#"printf 'max-jobs = %s\n' "$1" > "$2.tmp" && mv "$2.tmp" "$2""#,
            "sh",
            &(own0 + 3).to_string(),
            conf,
        ],
    );
    // Nothing is re-read without the signal.
    let reads0 = reads();
    std::thread::sleep(std::time::Duration::from_secs(3));
    assert_eq!(own(), own0);
    hup();
    wait_journal_above(WORKER1, UNIT, "event=max_jobs ", reads0, 30);
    retry(30, "worker1 took nix's new max-jobs", || own() == own0 + 3);
    retry(30, "the scheduler counts the new slots", || total() == total0 + 3);
    succeed(WORKER1, &["rm", conf]);
    hup();
    retry(30, "back to nix.conf's own max-jobs", || {
        own() == own0 && total() == total0
    });
    assert_eq!(pid(), pid0, "nix-grpc-daemon restarted");
}

#[test]
fn an_upload_slower_than_the_expect_timeout_keeps_the_assignment() {
    farm_ready();
    let big = succeed(
        CLIENT,
        &[
            "sh",
            "-c",
            "head -c 24M /dev/urandom > /tmp/slow && nix-store --add /tmp/slow",
        ],
    )
    .trim()
    .to_string();
    // ~96 s for the upload, longer than a builder keeps an Expect nobody claimed (60 s).
    succeed(
        CLIENT,
        &[
            "tc", "qdisc", "add", "dev", "eth1", "root", "tbf", "rate", "2mbit", "burst", "32kbit",
            "latency", "400ms",
        ],
    );
    let o = run_t(
        CLIENT,
        &[
            "nix",
            "build",
            "--store",
            &envoy(),
            "--eval-store",
            "auto",
            "-f",
            &expr("DEP"),
            "job",
            "--argstr",
            "tag",
            "slow-upload",
            "--argstr",
            "inputPath",
            &big,
        ],
        300,
    );
    succeed(CLIENT, &["tc", "qdisc", "del", "dev", "eth1", "root"]);
    let out = o.combined();
    assert_eq!(o.code, 0, "{out}");
    // Before, the Expect ran out mid-upload: "not scheduled here", and the
    // next worker got the same upload.
    assert!(!out.contains("asking the scheduler again"), "{out}");
}

#[test]
fn a_draining_builder_lets_a_client_attach_to_the_build_it_runs() {
    farm_ready();
    spawn_build("da1", "drain-attach");
    wait_building();
    let busy = building()[0];
    let draining = "event=draining";
    let before = journal_count(busy, UNIT, draining);
    succeed(
        busy,
        &["systemctl", "stop", "--no-block", "nix-grpc-daemon.service"],
    );
    wait_journal_above(busy, UNIT, draining, before, 30);
    // The scheduler sends whoever wants the drv to the worker that runs it,
    // draining or not. Like a client whose stream to it broke.
    let attached = events(busy, "attached");
    spawn_build("da2", "drain-attach");
    retry(60, "the second client attached", || {
        events(busy, "attached") > attached
    });
    release_slow();
    wait_unit_done("da1 da2", 90);
    for unit in ["da1", "da2"] {
        assert_unit_success(unit);
    }
    // Before, the worker answered "worker draining" until reschedule-retries ran out.
    assert_no_journal(CLIENT, "da2", "asking the scheduler again");
    restart_daemon(busy);
}

#[test]
fn a_builder_killed_mid_build_sends_the_build_elsewhere() {
    farm_ready();
    spawn_build("killed", "killed");
    wait_building();
    let busy = building()[0];
    succeed(
        busy,
        &["systemctl", "kill", "-s", "KILL", "nix-grpc-daemon.service"],
    );
    // How the stream ends depends on the path to the worker: here a reset,
    // in a farm behind tunnels also CANCELLED. Neither may fail the build.
    retry(180, "the build finished elsewhere", || {
        release_slow();
        run_t(CLIENT, &["systemctl", "is-active", "killed"], 60).code != 0
    });
    assert_unit_success("killed");
    restart_daemon(busy);
}

#[test]
fn a_store_query_cut_off_by_a_builder_shutting_down_is_retried() {
    farm_ready();
    let sys = system();
    // The query has to be in flight on the builder that stops.
    let (gone, other) = (leader(), standby());
    succeed(other, &["systemctl", "stop", "nix-grpc-daemon.service"]);
    wait_members(&sys, &[gone]);
    // Socket-activated, so there is one to stop whichever tests ran before.
    succeed(gone, &["systemctl", "start", "nix-daemon.service"]);
    succeed(gone, &["pkill", "-STOP", "-x", "nix-daemon"]);
    // QueryMissing, or QueryValidPaths where Nix hands queryMissing its
    // evaluation store (NixOS/nix#16433): then the client walks the graph
    // itself and sends no QueryMissing.
    let pattern = "event=rpc_start method=Query";
    let before = journal_count(gone, UNIT, pattern);
    succeed(
        CLIENT,
        &[
            "systemd-run",
            "--unit",
            "qm",
            "nix",
            "build",
            "--dry-run",
            "--store",
            &envoy(),
            "--eval-store",
            "auto",
            "-f",
            &expr("JOB"),
            "--argstr",
            "tag",
            "qm",
        ],
    );
    wait_journal_above(gone, UNIT, pattern, before, 60);
    // Nothing builds there, so it shuts down at once and cuts the query off.
    succeed(
        gone,
        &["systemctl", "stop", "--no-block", "nix-grpc-daemon.service"],
    );
    succeed(other, &["systemctl", "start", "nix-grpc-daemon.service"]);
    // Stopped, not just stopping: while it deactivates the server still
    // holds the query, and a nix-daemon resumed then answers it.
    wait_until_succeeds(
        gone,
        &[
            "sh",
            "-c",
            "systemctl is-active nix-grpc-daemon.service | grep -qxE 'inactive|failed'",
        ],
        60,
    );
    succeed(gone, &["pkill", "-CONT", "-x", "nix-daemon"]);
    wait_unit_done("qm", 120);
    assert_unit_success("qm");
    // The server cancelled the query when it stopped, and the client asked
    // again instead of failing.
    assert_journal(CLIENT, "qm", "Cancelling all calls, retrying");
    restart_daemon(gone);
}

#[test]
fn an_upload_cut_off_after_longer_than_the_restart_grace_is_retried() {
    farm_ready();
    // An upload is answered once niks3 has it, so a stopped niks3 push holds it.
    // Bracketed so pkill does not stop the shell running it.
    let push = "push --[s]tdin";
    for w in WORKERS {
        let path = succeed(
            CLIENT,
            &[
                "sh",
                "-c",
                &format!("echo prime-{} > /tmp/prime && nix-store --add /tmp/prime", w.0),
            ],
        )
        .trim()
        .to_string();
        succeed(
            CLIENT,
            &["nix", "copy", "--no-check-sigs", "--to", &direct(w), &path],
        );
        succeed(w, &["pkill", "-STOP", "-f", push]);
    }
    succeed(
        CLIENT,
        &[
            "systemd-run",
            "--unit",
            "held",
            "nix",
            "build",
            "--store",
            &format!("{}&restart-grace=10", envoy()),
            "--eval-store",
            "auto",
            "-f",
            &expr("JOB"),
            "--argstr",
            "tag",
            "held",
        ],
    );
    let uploading = |w: Node| gauge(w, r#"nix_grpc_inflight{kind="AddMultipleToStore"}"#) > 0;
    let mut busy = None;
    retry(60, "the derivations are uploading", || {
        busy = WORKERS.into_iter().find(|&w| uploading(w));
        busy.is_some()
    });
    let busy = busy.unwrap();
    // In CI a busy niks3 held the derivation upload for minutes.
    sleep(15);
    assert!(uploading(busy), "the upload did not wait for niks3");
    // Like the builder's tunnel going away: "reset reason: connection termination".
    let ip = env(&format!("NGS_{}_IP", busy.0.to_uppercase()));
    succeed(LB, &["ss", "-tK", "dst", &ip, "dport", "=", ":50051"]);
    for w in WORKERS {
        succeed(w, &["pkill", "-CONT", "-f", push]);
    }
    wait_unit_done("held", 180);
    assert_unit_success("held");
    // Cut off after the builder took the paths: a reset, no envoy reply.
    assert_journal(CLIENT, "held", ", retrying");
}

#[test]
fn a_builder_stopped_mid_upload_lets_it_land_and_the_build_moves_on() {
    farm_ready();
    let big = succeed(
        CLIENT,
        &[
            "sh",
            "-c",
            "head -c 16M /dev/urandom > /tmp/big && nix-store --add /tmp/big",
        ],
    )
    .trim()
    .to_string();
    // ~16 s for the upload, so the stop lands while it is in flight.
    succeed(
        CLIENT,
        &[
            "tc", "qdisc", "add", "dev", "eth1", "root", "tbf", "rate", "8mbit", "burst", "32kbit",
            "latency", "400ms",
        ],
    );
    succeed(
        CLIENT,
        &[
            "systemd-run",
            "--unit",
            "up",
            "nix",
            "build",
            "--store",
            // A worker leaving is not the job's fault, so no retry is spent on it.
            &format!("{}&reschedule-retries=0", envoy()),
            "--eval-store",
            "auto",
            "-f",
            &expr("DEP"),
            "job",
            "--argstr",
            "tag",
            "drain-upload",
            "--argstr",
            "inputPath",
            &big,
        ],
    );
    let uploading = |w: Node| gauge(w, r#"nix_grpc_inflight{kind="AddMultipleToStore"}"#) > 0;
    let mut busy = None;
    retry(60, "an upload is in flight", || {
        busy = WORKERS.into_iter().find(|&w| uploading(w));
        busy.is_some()
    });
    let busy = busy.unwrap();
    let landed = || journal_count(busy, UNIT, "event=rpc method=AddMultipleToStore");
    let landed0 = landed();
    // Like a CI job leaving: one SIGTERM, nothing building there.
    succeed(
        busy,
        &["systemctl", "stop", "--no-block", "nix-grpc-daemon.service"],
    );
    wait_unit_done("up", 180);
    succeed(CLIENT, &["tc", "qdisc", "del", "dev", "eth1", "root"]);
    assert_unit_success("up");
    assert_no_journal(CLIENT, "up", "Cancelling all calls");
    // The stopping builder took the upload in and answered it (published to
    // niks3 too), rather than the client sending it again elsewhere.
    assert!(
        landed() > landed0,
        "the stopping builder did not answer the upload in flight: {}",
        journal(busy, UNIT)
    );
    restart_daemon(busy);
}

#[test]
fn a_builder_the_balancer_cannot_reach_hands_its_builds_to_another() {
    farm_ready();
    let sys = system();
    // Still connected to the scheduler, like a CI builder whose tunnel from
    // the balancer is not up yet.
    let cut = standby();
    let reject = |op: &str| {
        succeed(
            cut,
            &[
                "iptables", op, "INPUT", "-p", "tcp", "-s", "lb", "--dport", "50051", "-j", "REJECT",
                "--reject-with", "tcp-reset",
            ],
        );
    };
    reject("-I");
    retry(30, "the balancer lost the builder", || {
        unhealthy(&sys).contains(&cut)
    });
    let bounced = events(leader(), "bounced");
    let tags = (0..4)
        .map(|i| format!("\"cut{i}\""))
        .collect::<Vec<_>>()
        .join(" ");
    let o = run_t(
        CLIENT,
        &[
            "nix",
            "build",
            "--store",
            // Before, the scheduler sent the build back to the same builder
            // until the retries ran out.
            &format!("{}&reschedule-retries=0", envoy()),
            "--eval-store",
            "auto",
            "--impure",
            "--expr",
            &format!("map (tag: import {} {{ inherit tag; }}) [ {tags} ]", expr("JOB")),
        ],
        300,
    );
    reject("-D");
    assert_eq!(o.code, 0, "{}", o.combined());
    assert!(events(leader(), "bounced") > bounced);
    wait_members(&sys, &WORKERS);
    retry(60, "scheduler settled", settled);
}

/// Moves the scheduler to `w`.
fn lead(w: Node) {
    if leader() == w {
        return;
    }
    let was = leader();
    let pattern = "event=scheduler_take_over";
    let before = journal_count(w, UNIT, pattern);
    succeed(was, &["systemctl", "stop", "nix-grpc-daemon.service"]);
    wait_journal_above(w, UNIT, pattern, before, 30);
    succeed(was, &["systemctl", "start", "nix-grpc-daemon.service"]);
    wait_members(&system(), &WORKERS);
    retry(60, "scheduler settled", settled);
    assert_eq!(leader(), w);
}

#[test]
fn a_build_whose_stream_breaks_goes_on_and_the_client_follows_it_again() {
    farm_ready();
    // Then the balancer's connections to worker1 carry the build only.
    lead(WORKER2);
    succeed(
        CLIENT,
        &[
            "systemd-run",
            "--unit",
            "flap",
            "nix",
            "build",
            "--store",
            &format!("{}&reschedule-retries=1", envoy()),
            "--eval-store",
            "auto",
            "-f",
            &expr("SLOW"),
            "--argstr",
            "tag",
            "flap",
            "--arg",
            "features",
            r#"["vip"]"#,
        ],
    );
    wait_building();
    assert_eq!(building(), [WORKER1]);
    let ip = env("NGS_WORKER1_IP");
    for _ in 0..3 {
        let attached = events(WORKER1, "attached");
        // Like a tunnel or proxy on the way resetting the stream.
        succeed(LB, &["ss", "-tK", "dst", &ip, "dport", "=", ":50051"]);
        retry(60, "the client attached again", || {
            events(WORKER1, "attached") > attached
        });
        assert_eq!(building(), [WORKER1], "the build stopped or moved");
    }
    release_slow();
    wait_unit_done("flap", 90);
    assert_unit_success("flap");
}
