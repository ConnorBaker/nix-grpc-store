use std::time::Instant;

use super::helpers::*;
use crate::harness::*;

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

/// Deletes the NAR of `path` from the bucket without telling niks3.
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

/// An input whose NAR the bucket lost is rebuilt and re-published, and both
/// of its users (one per worker) wait for that build.
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
    // vip pins it to worker1; copying it there publishes it to niks3.
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
fn interrupting_the_client_stops_the_build_on_the_worker() {
    farm_ready();
    spawn_build("intr", "intr");
    wait_building();
    succeed(CLIENT, &["systemctl", "kill", "-s", "INT", "intr"]);
    retry(20, "the build stopped", || building().is_empty());
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
    // In nix's default configuration, without the experimental features
    // that add to them, a builder offers and shows its `system-features`.
    let mut offered = Vec::new();
    for w in WORKERS {
        let set = succeed(w, &["nix", "config", "show", "system-features"]);
        let set = set.split_whitespace().collect::<Vec<_>>().join(",");
        assert_eq!(features_label(&build_info(w)), Some(set.as_str()));
        offered.extend(set.split(',').map(String::from));
    }
    offered.sort();
    offered.dedup();
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
    let why = format!("features {{gpu}}, the workers offer {{{}}}", offered.join(","));
    assert!(out.contains(&why), "{out}");
    retry(30, "no unplaceable build", || {
        gauge(leader(), &unplaceable) == 0
    });
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

/// A vip build holds worker1, so the scheduler places the next drv on
/// worker2, which `full` leaves without disk.
struct Worker2Full<F> {
    busy: String,
    _full: F,
}

impl<F> Worker2Full<F> {
    fn new(tag: &str, full: impl FnOnce() -> F) -> Self {
        let busy = format!("busy-{tag}");
        let slow = format!(
            r#"derivation {{ name = "{busy}"; system = builtins.currentSystem; requiredSystemFeatures = [ "vip" ]; builder = "/bin/sh"; args = [ "-c" "/bin/sh -c 'read -t 90 x < /dev/zero'; echo {tag} > $out" ]; }}"#
        );
        spawn(&busy, &["--impure", "--expr", &slow]);
        retry(60, "a slow build holds worker1", || building() == [WORKER1]);
        Worker2Full { busy, _full: full() }
    }

    /// Builds `drv` while worker2 is full and checks that worker2 got it and
    /// gave it back, then frees both workers. Returns the build's unit.
    fn build(self, drv: &str) -> String {
        let given = || {
            WORKERS
                .into_iter()
                .map(|w| events(w, "given_back_TRANSIENT_FAILURE"))
                .sum::<i64>()
        };
        let (before, calls) = (given(), build_calls(WORKER2));
        let unit = format!("{}-build", self.busy);
        spawn(&unit, &[format!("{drv}^*").as_str()]);
        retry(120, "worker2 gave the drv back, or the build ended", || {
            given() > before || unit_state(CLIENT, &unit) != "active"
        });
        assert!(build_calls(WORKER2) > calls, "worker2 never got the drv");
        assert!(given() > before, "worker2 did not give the drv back");
        drop(self);
        wait_unit_done(&unit, 120);
        unit
    }
}

impl<F> Drop for Worker2Full<F> {
    fn drop(&mut self) {
        release_slow();
        wait_unit_done(&self.busy, 120);
    }
}

fn build_calls(w: Node) -> i64 {
    worker_metrics(w)
        .iter()
        .filter(|l| l.starts_with("nix_grpc_rpcs_total{") && l.contains(r#"method="BuildDerivation""#))
        .filter_map(|l| l.split_whitespace().last()?.parse::<f64>().ok())
        .sum::<f64>() as i64
}

/// `nix build` through the balancer in a unit of its own.
fn spawn(unit: &str, args: &[&str]) {
    let envoy = envoy();
    let mut cmd = vec!["systemd-run", "--unit", unit, "nix", "build", "--store", envoy.as_str()];
    cmd.extend(["--eval-store", "auto"]);
    cmd.extend_from_slice(args);
    succeed(CLIENT, &cmd);
}

fn instantiate(attr: &str, tag: &str, extra: &[&str]) -> String {
    let dep = expr("DEP");
    let mut cmd = vec!["nix-instantiate", dep.as_str(), "-A", attr, "--argstr", "tag", tag];
    cmd.extend_from_slice(extra);
    succeed(CLIENT, &cmd).trim().to_string()
}

fn assert_built_by_worker1(unit: &str, drv: &str) {
    assert_unit_success(unit);
    let out = succeed(CLIENT, &["nix-store", "-q", "--outputs", drv]);
    assert_eq!(holders(out.trim()), [WORKER1]);
}

/// Worker2's build logs go to a directory that takes no new file while its
/// store has room: only Nix's ENOSPC text says that its disk failed.
struct NoLogDir;

impl NoLogDir {
    fn new() -> Self {
        let cmd = "mkdir -p /nix/var/log/nix/drvs && mount -t tmpfs -o nr_inodes=1 nolog /nix/var/log/nix/drvs";
        succeed(WORKER2, &["sh", "-c", cmd]);
        fail(WORKER2, &["mkdir", "/nix/var/log/nix/drvs/probe"]);
        NoLogDir
    }
}

impl Drop for NoLogDir {
    fn drop(&mut self) {
        succeed(WORKER2, &["umount", "-l", "/nix/var/log/nix/drvs"]);
    }
}

/// A drv whose worker has no disk to set the build up is given back, and the
/// other worker builds it.
#[test]
fn a_drv_whose_log_its_worker_cannot_create_is_built_by_the_other() {
    farm_ready();
    let drv = instantiate("input", "nolog", &[]);
    succeed(CLIENT, &["nix", "copy", "--to", &direct(WORKER2), &drv]);
    let unit = Worker2Full::new("nolog", NoLogDir::new).build(&drv);
    assert_built_by_worker1(&unit, &drv);
}

/// Worker2's store takes no new files: they fail with ENOSPC while it has
/// bytes to spare.
struct NoInodes {
    inodes: String,
}

impl NoInodes {
    fn new() -> Self {
        let df = succeed(WORKER2, &["df", "--output=itotal,iused", "/nix/.rw-store"]);
        let mut cols = df.lines().last().unwrap().split_whitespace().map(String::from);
        let (inodes, used) = (cols.next().unwrap(), cols.next().unwrap());
        let opts = format!("remount,nr_inodes={used}");
        succeed(WORKER2, &["mount", "-o", &opts, "/nix/.rw-store"]);
        fail(WORKER2, &["touch", "/nix/.rw-store/probe"]);
        NoInodes { inodes }
    }
}

impl Drop for NoInodes {
    fn drop(&mut self) {
        let opts = format!("remount,nr_inodes={}", self.inodes);
        succeed(WORKER2, &["mount", "-o", &opts, "/nix/.rw-store"]);
    }
}

/// A client-local file, in the client's store.
fn random_source(name: &str) -> String {
    let cmd = format!("head -c 65536 /dev/urandom | base64 > /tmp/{name} && nix store add-file /tmp/{name}");
    succeed(CLIENT, &["sh", "-c", &cmd]).trim().to_string()
}

/// The client's upload of a drv's closure lands on whichever worker the
/// balancer picks, and the drv on worker2, which has no disk to fetch it.
#[test]
fn a_drv_whose_closure_its_worker_has_no_disk_for_is_built_by_the_other() {
    farm_ready();
    let src = random_source("noroom");
    let drv = instantiate("job", "noroom", &["--argstr", "inputPath", &src]);
    let unit = Worker2Full::new("noroom", NoInodes::new).build(&drv);
    assert_built_by_worker1(&unit, &drv);
}

/// An input in the cache that worker2 has no disk to fetch does not count as
/// lost: the drv moves to worker1 and nothing is rebuilt.
#[test]
fn an_input_its_worker_has_no_disk_for_is_not_rebuilt() {
    farm_ready();
    // vip builds the input on worker1, the job may run anywhere.
    let vip = ["--arg", "features", r#"["vip"]"#, "--arg", "jobFeatures", "[]"];
    let input = instantiate("input", "noroomin", &vip);
    spawn("noroomin-input", &[format!("{input}^*").as_str()]);
    wait_unit_done("noroomin-input", 120);
    assert_unit_success("noroomin-input");
    let drv = instantiate("job", "noroomin", &vip);
    succeed(CLIENT, &["nix", "copy", "--to", &direct(WORKER2), &drv]);
    let unit = Worker2Full::new("noroomin", NoInodes::new).build(&drv);
    assert_built_by_worker1(&unit, &drv);
    let log = journal(CLIENT, &unit);
    assert!(!log.contains("farm lacks"), "{log}");
}

/// `w` with `left` bytes free, until dropped.
struct LowDisk(Node);

impl LowDisk {
    fn leaving(w: Node, left: u64) -> Self {
        let avail = succeed(w, &["df", "--output=avail", "-B1", "/nix/store"]);
        let avail: u64 = avail.lines().last().unwrap().trim().parse().unwrap();
        let size = (avail - left).to_string();
        succeed(w, &["fallocate", "-l", &size, "/nix/.rw-store/fill"]);
        LowDisk(w)
    }
}

impl Drop for LowDisk {
    fn drop(&mut self) {
        succeed(self.0, &["rm", "/nix/.rw-store/fill"]);
        wait_members(&system(), &WORKERS);
    }
}

/// A full worker answers an upload or a fetch that needs a path only the
/// cache has as unavailable, which the client retries, not as an error.
#[test]
fn uploads_and_fetches_a_worker_has_no_disk_for_are_retried() {
    farm_ready();
    let src = random_source("noroomref");
    succeed(CLIENT, &["nix", "copy", "--to", &direct(WORKER1), &src]);
    let drv = instantiate("job", "noroomref", &["--argstr", "inputPath", &src]);
    let store = format!("{}&restart-grace=1", direct(WORKER2));
    let _full = NoInodes::new();
    let upload = run_t(CLIENT, &["nix", "copy", "--to", &store, &drv], 60);
    let fetch = run_t(
        CLIENT,
        &["sh", "-c", r#"nix store dump-path --store "$1" "$2" > /dev/null"#, "sh", &store, &src],
        60,
    );
    for o in [upload, fetch] {
        assert!(o.code != 0 && o.combined().contains("retrying"), "{}", o.combined());
    }
}

/// A fetch from the cache bigger than what worker2 has left fails without
/// ENOSPC in Nix's error: still unavailable, which the client retries.
#[test]
fn a_fetch_bigger_than_what_its_worker_has_left_is_retried() {
    farm_ready();
    let cmd = "head -c 64M /dev/urandom > /tmp/big && nix store add-file /tmp/big";
    let src = succeed(CLIENT, &["sh", "-c", cmd]).trim().to_string();
    succeed(CLIENT, &["nix", "copy", "--to", &direct(WORKER1), &src]);
    let _low = LowDisk::leaving(WORKER2, 32 << 20);
    let store = format!("{}&restart-grace=1", direct(WORKER2));
    let fetch = run_t(
        CLIENT,
        &["sh", "-c", r#"nix store dump-path --store "$1" "$2" > /dev/null"#, "sh", &store, &src],
        60,
    );
    assert!(fetch.code != 0 && fetch.combined().contains("retrying"), "{}", fetch.combined());
}

/// Worker2 is low on disk but not out of it, and a client pinned to it asks
/// for a path nobody has: absent at once, as with disk to spare.
#[test]
fn a_path_nobody_has_is_absent_on_a_worker_low_on_disk() {
    farm_ready();
    let _low = LowDisk::leaving(WORKER2, 100 << 20);
    let store = format!("{}&restart-grace=1", direct(WORKER2));
    let absent = "/nix/store/00000000000000000000000000000000-absent";
    let o = run_t(CLIENT, &["nix", "path-info", "--store", &store, absent], 120);
    assert!(o.code != 0 && o.combined().contains("is not valid"), "{}", o.combined());
}

/// A drv whose NAR the cache lost still fails at once as missing on workers
/// with disk to spare, and its lease ends.
#[test]
fn a_drv_the_cache_lost_fails_as_missing() {
    farm_ready();
    let drv = instantiate("input", "lostdrv", &[]);
    succeed(CLIENT, &["nix", "copy", "--to", &direct(WORKER1), &drv]);
    succeed(WORKER1, &["nix-store", "--delete", &drv]);
    lose_nar(&drv);
    let done = || WORKERS.into_iter().map(|w| events(w, "done_CANCELLED")).sum::<i64>();
    let before = done();
    let o = run_t(
        CLIENT,
        &["nix", "build", "--store", &envoy(), "--eval-store", "auto", &format!("{drv}^*")],
        120,
    );
    let out = o.combined();
    assert!(o.code != 0 && out.contains("missing: ") && !out.contains("gave up"), "{out}");
    retry(30, "the lease ended", || done() > before);
}

/// Both workers low on disk but not out of it, and the cache lost the drv:
/// "missing" at once, as with disk to spare.
#[test]
fn a_drv_the_cache_lost_fails_as_missing_on_workers_low_on_disk() {
    farm_ready();
    let drv = instantiate("input", "lostdrvlow", &[]);
    succeed(CLIENT, &["nix", "copy", "--to", &direct(WORKER1), &drv]);
    succeed(WORKER1, &["nix-store", "--delete", &drv]);
    lose_nar(&drv);
    let _low = [LowDisk::leaving(WORKER1, 100 << 20), LowDisk::leaving(WORKER2, 100 << 20)];
    let o = run_t(
        CLIENT,
        &["nix", "build", "--store", &envoy(), "--eval-store", "auto", &format!("{drv}^*")],
        300,
    );
    let out = o.combined();
    assert!(o.code != 0 && out.contains("missing: ") && !out.contains("gave up"), "{out}");
}
