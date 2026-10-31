//! tests/handover.nix: a node that is its own scheduler's only builder,
//! reloaded mid-build into other configurations (its specialisations).
use std::time::{Duration, Instant};

use crate::fixtures::*;
use crate::harness::{MACHINE, env, journal_count, query, retry, sleep, unit_result};

const UNIT: &str = "nix-grpc-daemon.service";
const CGROUP_PROCS: &str = "/sys/fs/cgroup/system.slice/nix-grpc-daemon.service/cgroup.procs";

fn main_pid() -> String {
    query(MACHINE, &["systemctl", "show", "-P", "MainPID", UNIT])
        .trim()
        .to_string()
}

fn exe(pid: &str) -> String {
    query(MACHINE, &["readlink", &format!("/proc/{pid}/exe")])
        .trim()
        .to_string()
}

fn alive(pid: &str) -> bool {
    run_t(&["test", "-d", &format!("/proc/{pid}")], 60).code == 0
}

// The processes of the unit, main and others. None once it has stopped.
fn unit_pids() -> Vec<String> {
    query(
        MACHINE,
        &[
            "sh",
            "-c",
            &format!("cat {CGROUP_PROCS} 2>/dev/null || true"),
        ],
    )
    .split_whitespace()
    .map(String::from)
    .collect()
}

// systemd's own view of the main process, for the record.
fn main_pid_dump() -> String {
    query(
        MACHINE,
        &[
            "sh",
            "-c",
            &format!("systemd-analyze dump {UNIT} | grep 'Main PID' || true"),
        ],
    )
}

fn store() -> String {
    // Short, so a refused build fails while the test still waits for it.
    format!("{STORE}&restart-grace=20")
}

fn build_argv<'a>(store: &'a str, slow: &'a str, tag: &'a str, secs: &'a str) -> Vec<&'a str> {
    vec![
        "nix",
        "build",
        "-L",
        "--no-link",
        "--print-out-paths",
        "--store",
        store,
        "--eval-store",
        "local",
        "-f",
        slow,
        "--argstr",
        "tag",
        tag,
        "--argstr",
        "secs",
        secs,
    ]
}

// A build of `secs` seconds through gRPC in unit `tag`; returns once it runs.
fn start_build(tag: &str, secs: &str) {
    let slow = env("NGS_SLOW_EXPR");
    let store = store();
    let out = format!(r#""$@" > /root/{tag}.out"#);
    let mut argv = vec![
        "systemd-run",
        "--unit",
        tag,
        // systemd-run looks up only `sh` on our PATH, the unit's is bare.
        "--setenv=PATH=/run/current-system/sw/bin",
        "sh",
        "-c",
        &out,
        "sh",
    ];
    argv.extend(build_argv(&store, &slow, tag, secs));
    succeed(&argv);
    wait_until_succeeds(
        &[
            "sh",
            "-c",
            &format!("journalctl -u {tag} | grep -q SLOW-STARTED-{tag}"),
        ],
        120,
    );
}

fn starts(tag: &str) -> usize {
    query(MACHINE, &["journalctl", "-u", tag])
        .lines()
        .filter(|l| l.contains(&format!("SLOW-STARTED-{tag}")))
        .count()
}

// Waits for the build in unit `tag` and checks it ran once, succeeded, and
// left a valid output.
fn finished_once(tag: &str, secs: u64) {
    wait_until_succeeds(&["sh", "-c", &format!("! systemctl is-active {tag}")], secs);
    let result = unit_result(MACHINE, tag);
    assert_eq!(
        result,
        "success",
        "{}",
        query(MACHINE, &["journalctl", "-u", tag])
    );
    assert_eq!(starts(tag), 1, "build {tag} was started again");
    assert_no_journal(tag, "asking the scheduler again");
    let out = succeed(&["cat", &format!("/root/{tag}.out")])
        .trim()
        .to_string();
    succeed(&["nix-store", "--verify-path", &out]);
    assert_eq!(succeed(&["cat", &out]).trim(), tag);
}

// A short build, which has to succeed within 30 s.
fn quick_build(tag: &str) {
    let slow = env("NGS_SLOW_EXPR");
    let store = store();
    let started = Instant::now();
    let out = run_t(&build_argv(&store, &slow, tag, "1"), 120);
    let took = started.elapsed();
    assert_eq!(
        out.code,
        0,
        "build {tag} failed after {took:?}:\n{}",
        out.combined()
    );
    assert!(took < Duration::from_secs(30), "build {tag} took {took:?}");
}

fn system(name: &str) -> String {
    let base = env("NGS_BASE_SYSTEM");
    if name == "base" {
        base
    } else {
        format!("{base}/specialisation/{name}")
    }
}

fn current() -> String {
    query(MACHINE, &["readlink", "-f", "/run/current-system"])
        .trim()
        .to_string()
}

// The one of `a` and `b` that is not the running configuration.
fn other(a: &'static str, b: &'static str) -> &'static str {
    let a_path = query(MACHINE, &["readlink", "-f", &system(a)])
        .trim()
        .to_string();
    if current() == a_path { b } else { a }
}

fn switch_to(name: &str) -> crate::harness::Out {
    let bin = format!("{}/bin/switch-to-configuration", system(name));
    run_t(&["timeout", "120", &bin, "test"], 150)
}

// A reload into configuration `name` with a different daemon: returns the
// main process after it.
fn take_over_into(name: &str) -> String {
    let old = main_pid();
    let handed = journal_count(MACHINE, UNIT, "event=handed_over");
    let out = switch_to(name);
    assert_eq!(out.code, 0, "switch to {name}:\n{}", out.combined());
    wait_journal_above(UNIT, "event=handed_over", handed, 30);
    let new = main_pid();
    assert_ne!(new, old, "no new main process after the switch to {name}");
    new
}

// The daemon runs, and serves.
fn warm(tag: &str) {
    quick_build(tag);
    assert_eq!(unit_state(UNIT), "active");
}

pub mod upgrade {
    use super::*;

    #[test]
    fn a_reload_into_a_new_package_takes_builds_while_the_old_generation_finishes_its_own() {
        warm("warm-reload");
        let target = other("next", "base");
        // 1. A long build through gRPC.
        start_build("long", "75");
        let old = main_pid();
        let old_exe = exe(&old);
        let draining = journal_count(MACHINE, UNIT, "event=draining");

        // 2. The upgrade: a switch to a configuration with another daemon package.
        let out = switch_to(target);
        assert_eq!(out.code, 0, "{}", out.combined());

        // 3. A build while the old generation drains, as builds arrived when
        // this node refused them, is taken while the first one still runs.
        wait_journal_above(UNIT, "event=draining", draining, 30);
        quick_build("after");
        assert_eq!(
            unit_state("long"),
            "active",
            "the first build was over before the second, so the second proves nothing"
        );

        // 4. The first build completes, once, and its output is valid.
        finished_once("long", 180);

        // 5. The old process is gone and the new generation serves.
        retry(60, "the old generation exited", || !alive(&old));
        let new = main_pid();
        assert_ne!(new, old);
        let new_exe = exe(&new);
        assert_ne!(new_exe, old_exe);
        if target == "next" {
            assert!(new_exe.contains("nix-grpc-daemon-next"), "{new_exe}");
        }
        assert_journal(UNIT, &format!("event=handed_over pid={new}"));
        assert_journal(UNIT, &format!("event=predecessor_exited pid={old}"));
        assert_eq!(unit_state(UNIT), "active");
        quick_build("later");
    }

    // After a takeover the unit's main process is one systemd did not start.
    // A stop still has to drain it, as it drains one systemd started, and
    // with it the old generation that still drains.
    #[test]
    fn a_stop_after_a_takeover_drains_and_the_running_builds_complete_once() {
        warm("warm-stop");
        start_build("stopped-old", "45");
        let old = main_pid();
        let new = take_over_into(other("next", "base"));
        println!("after the takeover: {}", main_pid_dump());
        start_build("stopped-new", "40");
        let kills = journal_count(MACHINE, UNIT, "SIGKILL");
        succeed(&["systemctl", "stop", "--no-block", UNIT]);
        sleep(5);
        assert_eq!(
            unit_state(UNIT),
            "deactivating",
            "5 s after the stop the unit is not draining; journal since:\n{}",
            query(
                MACHINE,
                &["journalctl", "-u", UNIT, "-n", "40", "--no-pager"]
            )
        );
        assert!(alive(&old) && alive(&new), "a generation did not drain");
        finished_once("stopped-old", 120);
        finished_once("stopped-new", 120);
        wait_until_succeeds(&["sh", "-c", &format!("! systemctl is-active {UNIT}")], 60);
        assert_eq!(
            journal_count(MACHINE, UNIT, "SIGKILL"),
            kills,
            "the stop killed processes"
        );
        assert_eq!(unit_result(MACHINE, UNIT), "success");
        // The socket starts it again.
        warm("after-stop");
    }

    // A reload while the previous generation still drains: each new one
    // waits for its predecessor, and every build completes once.
    #[test]
    fn two_reloads_in_a_row_chain_and_every_build_completes_once() {
        warm("warm-chain");
        start_build("chain", "60");
        let first = main_pid();
        let second = take_over_into(other("next", "base"));
        quick_build("chain-second");
        let third = take_over_into(other("next", "base"));
        quick_build("chain-third");
        assert_eq!(
            unit_state("chain"),
            "active",
            "the long build was over too soon"
        );
        assert!(alive(&first) && alive(&second), "a predecessor left early");
        finished_once("chain", 120);
        retry(60, "both predecessors exited", || {
            !alive(&first) && !alive(&second)
        });
        assert_eq!(main_pid(), third);
        assert_journal(UNIT, &format!("event=predecessor_exited pid={second}"));
        assert_eq!(unit_state(UNIT), "active");
        quick_build("chain-later");
    }

    // A new generation with an idle timeout outlives its draining
    // predecessor, then idles out, and systemd sees it go.
    #[test]
    fn idle_exit_after_a_takeover_waits_for_the_predecessor() {
        warm("warm-idle");
        start_build("idle-long", "30");
        let old = main_pid();
        let new = take_over_into("idle");
        finished_once("idle-long", 90);
        // 20 s idle timeout, counted from the last call.
        wait_until_succeeds(&["sh", "-c", &format!("! systemctl is-active {UNIT}")], 90);
        assert!(!alive(&old) && !alive(&new));
        let log = query(MACHINE, &["journalctl", "-u", UNIT, "--no-pager"]);
        let at = |what: &str| {
            log.lines()
                .position(|l| l.contains(&format!("nix-grpc-daemon[{new}]:")) && l.contains(what))
                .unwrap_or_else(|| panic!("no {what} from {new}:\n{log}"))
        };
        assert!(
            at(&format!("event=predecessor_exited pid={old}")) < at("event=idle_exit"),
            "the new generation idled out while its predecessor drained"
        );
        assert_eq!(unit_result(MACHINE, UNIT), "success");
        assert!(unit_pids().is_empty(), "{:?}", unit_pids());
        warm("after-idle");
    }
}

pub mod failures {
    use super::*;

    // The successor gets the sockets and then fails: the old generation
    // takes builds again at once.
    #[test]
    fn a_takeover_that_fails_leaves_the_old_generation_serving() {
        warm("warm-broken");
        start_build("broken-long", "30");
        let old = main_pid();
        let failed = journal_count(MACHINE, UNIT, "event=takeover_failed");
        let out = switch_to("broken");
        assert_ne!(
            out.code, 0,
            "the reload into a daemon that cannot start succeeded"
        );
        assert!(journal_count(MACHINE, UNIT, "event=takeover_failed") > failed);
        assert_eq!(main_pid(), old);
        quick_build("broken-after");
        finished_once("broken-long", 90);
        assert_eq!(unit_pids(), vec![old]);
    }

    // systemd kills a reload that takes too long. The successor it started
    // dies with it, not on its own timeout, and the old generation serves on.
    #[test]
    fn a_reload_that_systemd_times_out_leaves_the_old_generation_serving() {
        warm("warm-timeout");
        let old = main_pid();
        // The old process answers nothing, so the successor waits.
        succeed(&["kill", "-STOP", &old]);
        let out = switch_to("quick-timeout");
        let timed_out = query(MACHINE, &["journalctl", "-u", UNIT, "--no-pager"])
            .contains("Reload operation timed out");
        let others: Vec<String> = unit_pids().into_iter().filter(|p| *p != old).collect();
        // Within 10 s of systemd's 5 s: its own wait for the sockets is 30 s.
        let started = Instant::now();
        let mut left = others.clone();
        while !left.is_empty() && started.elapsed() < Duration::from_secs(10) {
            sleep(1);
            left.retain(|p| alive(p));
        }
        succeed(&["kill", "-CONT", &old]);
        assert_ne!(out.code, 0, "the reload succeeded:\n{}", out.combined());
        assert!(timed_out, "systemd did not time the reload out");
        assert!(
            left.is_empty(),
            "the successor outlived the reload: {left:?} of {others:?}"
        );
        assert_eq!(main_pid(), old);
        quick_build("timeout-after");
        assert_eq!(unit_pids(), vec![old]);
    }

    // systemd stops the unit when its main process dies, whoever started it.
    // The predecessor's builds go with it, and their clients build again.
    #[test]
    fn a_crash_of_the_new_generation_is_recovered_from() {
        warm("warm-crash");
        start_build("crash-long", "30");
        let new = take_over_into(other("next", "base"));
        succeed(&["kill", "-KILL", &new]);
        wait_until_succeeds(&["sh", "-c", "! systemctl is-active crash-long"], 180);
        assert_eq!(
            unit_result(MACHINE, "crash-long"),
            "success",
            "{}",
            query(MACHINE, &["journalctl", "-u", "crash-long"])
        );
        println!("crash-long started {} times", starts("crash-long"));
        warm("after-crash");
    }

    // Connects to the main process's takeover offer from a process that is
    // not its successor: it is refused and the main process serves on.
    fn refused(prefix: &str, what: &str) {
        let pid = main_pid();
        let refusals = journal_count(MACHINE, UNIT, "event=takeover_refused");
        let script = format!(
            "import socket; s = socket.socket(socket.AF_UNIX, socket.SOCK_SEQPACKET); \
             s.connect(b'\\0nix-grpc-daemon/takeover/{pid}'); s.settimeout(10); \
             print('received', len(s.recv(8192)))"
        );
        let out = succeed(&[
            "sh",
            "-c",
            &format!(r#"{prefix} python3 -c "$1""#),
            "connect",
            &script,
        ]);
        assert_eq!(out.trim(), "received 0", "{what}: the offer answered");
        assert!(
            journal_count(MACHINE, UNIT, "event=takeover_refused") > refusals,
            "{what}: no takeover_refused"
        );
        assert_eq!(main_pid(), pid);
        quick_build(&format!("refused-{}", refusals + 1));
    }

    #[test]
    fn the_offer_refuses_another_unit_and_another_user() {
        warm("warm-refused");
        // Same user, another cgroup: this ssh session's.
        refused(
            "exec setpriv --reuid=nix-grpc-daemon --regid=nix-grpc-daemon --clear-groups",
            "another cgroup",
        );
        // Same cgroup, another user: root moved into the unit.
        refused(&format!("echo $$ > {CGROUP_PROCS} && exec"), "another user");
    }
}
