use super::helpers::*;
use crate::harness::*;

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

#[test]
fn a_builder_that_cannot_publish_gets_no_work_while_another_can_take_it() {
    farm_ready();
    let sys = system();
    // Like a CI builder whose niks3 token file came out empty.
    let broken = standby();
    let token = succeed(broken, &["sh", "-c", "ls /nix/store/*-niks3-token"])
        .trim()
        .to_string();
    succeed(
        broken,
        &[
            "sh",
            "-c",
            // The next push starts afresh and reads it.
            r#": > /tmp/empty-token && mount --bind /tmp/empty-token "$1" && { pkill -f 'push --[s]tdin' || true; }"#,
            "sh",
            &token,
        ],
    );
    let failing = || journal_count(leader(), UNIT, "event=publish_failing");
    let before = failing();
    let tags = (0..4)
        .map(|i| format!("\"nopub{i}\""))
        .collect::<Vec<_>>()
        .join(" ");
    let o = run_t(
        CLIENT,
        &[
            "nix",
            "build",
            "--store",
            // Before, the scheduler sent a build back to the builder that
            // had just failed to publish it until the retries ran out, and
            // an upload or query that landed there failed outright.
            &format!("{}&reschedule-retries=0", envoy()),
            "--eval-store",
            "auto",
            "--impure",
            "--expr",
            &format!("map (tag: import {} {{ inherit tag; }}) [ {tags} ]", expr("JOB")),
        ],
        300,
    );
    succeed(broken, &["umount", &token]);
    assert_eq!(o.code, 0, "{}", o.combined());
    assert!(failing() > before, "the broken builder got no work");
    // Out of the balancer until a publish works there again.
    assert!(!members(&sys).contains(&broken));
    let path = succeed(
        CLIENT,
        &["sh", "-c", "echo republish > /tmp/republish && nix-store --add /tmp/republish"],
    )
    .trim()
    .to_string();
    succeed(
        CLIENT,
        &["nix", "copy", "--no-check-sigs", "--to", &direct(broken), &path],
    );
    wait_members(&sys, &WORKERS);
    retry(60, "scheduler settled", settled);
}

#[test]
fn an_upload_cut_off_again_and_again_while_it_imports_is_retried() {
    farm_ready();
    let tag = "import-held";
    let top = succeed(
        CLIENT,
        &["nix-instantiate", &expr("JOB"), "--argstr", "tag", tag],
    )
    .trim()
    .to_string();
    // In CI the builders spent minutes completing the derivations'
    // references from a throttled cache, before they held the paths. A held
    // lock on one of them keeps the import from finishing here.
    let lock = format!("{top}.lock");
    for w in WORKERS {
        // Like nix-daemon, in a mount namespace of its own with the store writable.
        let hold = format!(
            "mount -o remount,rw,bind /nix/store 2>/dev/null; exec flock {lock} sleep 900"
        );
        succeed(
            w,
            &[
                "systemd-run",
                "--unit",
                "hold-import",
                "--setenv=PATH=/run/current-system/sw/bin",
                "unshare",
                "-m",
                "sh",
                "-c",
                &hold,
            ],
        );
        let held = format!("flock -n -E 99 {lock} true; [ $? = 99 ]");
        wait_until_succeeds(w, &["sh", "-c", &held], 30);
    }
    let pattern = "event=rpc_start method=AddMultipleToStore";
    let started = || WORKERS.map(|w| journal_count(w, UNIT, pattern));
    let mut seen = started();
    succeed(
        CLIENT,
        &[
            "systemd-run",
            "--unit",
            "held-import",
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
            tag,
        ],
    );
    let retries = || journal_count(CLIENT, "held-import", ", retrying");
    // In CI the builders holding it left one after the other, over longer
    // than the restart grace in all. Before, the client gave up then.
    for cut in 0..2 {
        let mut busy = None;
        retry(60, "an upload started", || {
            let now = started();
            busy = (0..WORKERS.len()).find(|&i| now[i] > seen[i]).map(|i| WORKERS[i]);
            seen = now;
            busy.is_some()
        });
        sleep(12);
        // Like the builder's tunnel going away: "reset reason: connection termination".
        let ip = env(&format!("NGS_{}_IP", busy.unwrap().0.to_uppercase()));
        succeed(LB, &["ss", "-tK", "dst", &ip, "dport", "=", ":50051"]);
        retry(30, "the client retried", || retries() > cut);
    }
    for w in WORKERS {
        succeed(w, &["systemctl", "stop", "hold-import"]);
    }
    wait_unit_done("held-import", 180);
    assert_unit_success("held-import");
}
