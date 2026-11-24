//! tests/handover.nix: a node that is its own scheduler's only builder,
//! reloaded mid-build into other configurations (its specialisations).
use crate::fixtures::*;
use crate::harness::{
    MACHINE, env, journal, journal_count, query, retry, unit_result,
};

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
    run(&["test", "-d", &format!("/proc/{pid}")]).code == 0
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

fn build_argv<'a>(store: &'a str, slow: &'a str, tag: &'a str, hold: bool) -> Vec<&'a str> {
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
        "--arg",
        "hold",
        if hold { "true" } else { "false" },
    ]
}

// A build through gRPC in unit `tag` that runs until `release`d; returns
// once it runs.
fn start_build(tag: &str) {
    start_build_with(tag, &[]);
}

// start_build with more options for its `nix build`.
fn start_build_with(tag: &str, extra: &[&str]) {
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
    argv.extend(build_argv(&store, &slow, tag, true));
    argv.extend_from_slice(extra);
    succeed(&argv);
    retry(&[(MACHINE, tag)],"the build started", || starts(tag) > 0);
}

// Lets the running build of `tag` end: its builder reads a line from the
// fifo in its build directory. Nix starts a builder with its base name as
// argv[0], so the command line is `sh -c mkfifo ...`. This probe's own
// command line does not start as the builder's does, so pgrep finds the
// builder alone. The fifo is opened without blocking: with no reader (the
// builder not there yet, or gone) the open fails, ENXIO, instead of
// waiting for a reader that may never come.
fn try_release(tag: &str) -> bool {
    let script = r#"p=$(pgrep -f "^sh -c mkfifo release; echo SLOW-STARTED-$1 ") && python3 -c '
import os, sys
os.write(os.open(sys.argv[1], os.O_WRONLY | os.O_NONBLOCK), b"\n")' "/proc/$p/cwd/release""#;
    run(&["sh", "-c", script, "sh", tag]).code == 0
}

// Releases the build of `tag` once its builder reads the fifo; its client
// ending first fails the wait.
fn release(tag: &str) {
    retry(&[(MACHINE, tag)],&format!("{tag} released"), || try_release(tag));
}

// Re-checks `cond` until it holds, failing the test once it has been
// checked for `secs` in steps of 500 ms: for what follows another event at
// once, which a fault would otherwise have the test wait for forever,
// since the VM test has no global timeout (tests/lib/waits.nix cancels
// it). A build left running by a fault keeps its generation alive, and
// with it every later wait for that generation's exit in this module.
// Counted, not timed: a pause of the whole sandbox (a farm freezing it)
// pauses the nodes too, and spends at most one step of it, the sleep that
// spans it ending at the thaw.
fn within(secs: u64, what: &str, mut cond: impl FnMut() -> bool) {
    for _ in 0..secs * 2 {
        if cond() {
            return;
        }
        std::thread::sleep(std::time::Duration::from_millis(500));
    }
    assert!(cond(), "not within {secs}s: {what}");
}

fn starts(tag: &str) -> usize {
    query(MACHINE, &["journalctl", "-u", tag])
        .lines()
        .filter(|l| l.contains(&format!("SLOW-STARTED-{tag}")))
        .count()
}

// Waits for the build in unit `tag` and checks it ran once, succeeded, and
// left a valid output.
fn finished_once(tag: &str) {
    // Started again is a fault at once: it would only end later.
    retry(&[], &format!("{tag} ended"), || {
        assert_eq!(starts(tag), 1, "build {tag} was started again");
        ended(tag)
    });
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

// A short build, which has to succeed: one the daemon refuses fails once
// the store's restart-grace has run out. Returns its output.
fn quick_build(tag: &str) -> String {
    let slow = env("NGS_SLOW_EXPR");
    let store = store();
    let out = run(&build_argv(&store, &slow, tag, false));
    assert_eq!(out.code, 0, "build {tag} failed:\n{}", out.combined());
    out.stdout.trim().to_string()
}

// The lines of process `pid` in the unit's journal that end with `event`.
// Read whole: journal_count's `--grep` sees the message alone, not who
// wrote it.
fn logged_by(pid: &str, event: &str) -> usize {
    journal(MACHINE, UNIT)
        .lines()
        .filter(|l| l.contains(&format!("nix-grpc-daemon[{pid}]:")) && l.trim_end().ends_with(event))
        .count()
}

// The live processes whose temp roots name `path`: the nix-daemon
// connections that keep it from the garbage collector.
fn holders(path: &str) -> Vec<String> {
    query(
        MACHINE,
        &[
            "sh",
            "-c",
            r#"for f in $(grep -lF -e "$1" /nix/var/nix/temproots/* 2>/dev/null); do
  p=${f##*/}; [ -d "/proc/$p" ] && echo "$p"; done; true"#,
            "sh",
            path,
        ],
    )
    .split_whitespace()
    .map(String::from)
    .collect()
}

// Whether the temp-roots file of nix-daemon process `pid` is there.
fn temp_roots_file(pid: &str) -> bool {
    run(&["test", "-e", &format!("/nix/var/nix/temproots/{pid}")]).code == 0
}

// The connections of process `pid` to the nix-daemon: its unix sockets whose
// peer is the nix-daemon socket's side of a connection. `ss -xpn` fields:
// netid, state, queues, local path and inode, peer path and inode, process.
fn daemon_connections(pid: &str) -> usize {
    query(
        MACHINE,
        &[
            "bash",
            "-c",
            r#"set -o pipefail
ss -xpn | awk -v me="pid=$1," '
  { line[NR] = $0; peer[NR] = $8 }
  $5 ~ /daemon-socket\/socket$/ { daemon[$6] = 1 }
  END { for (i = 1; i <= NR; i++) if (index(line[i], me) && (peer[i] in daemon)) n++; print n + 0 }'"#,
            "sh",
            pid,
        ],
    )
    .trim()
    .parse()
    .expect("a count")
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
    run(&[&bin, "test"])
}

// A reload into configuration `name` with a different daemon: returns the
// main process after it.
fn take_over_into(name: &str) -> String {
    let old = main_pid();
    // " pid=": not handed_over_exit, the old process's last line.
    let handed = journal_count(MACHINE, UNIT, "event=handed_over pid=");
    let out = switch_to(name);
    assert_eq!(out.code, 0, "switch to {name}:\n{}", out.combined());
    wait_journal_above(UNIT, "event=handed_over pid=", handed, &[UNIT]);
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
        start_build("long");
        let old = main_pid();
        let old_exe = exe(&old);
        let draining = journal_count(MACHINE, UNIT, "event=draining");

        // 2. The upgrade: a switch to a configuration with another daemon package.
        let out = switch_to(target);
        assert_eq!(out.code, 0, "{}", out.combined());

        // 3. A build while the old generation drains, as builds arrived when
        // this node refused them, is taken while the first one still runs.
        wait_journal_above(UNIT, "event=draining", draining, &[UNIT]);
        quick_build("after");
        assert_eq!(
            unit_state("long"),
            "active",
            "the first build was over before the second, so the second proves nothing"
        );

        // 4. The first build completes, once, and its output is valid.
        release("long");
        finished_once("long");

        // 5. The old process is gone and the new generation serves.
        // With its one build over, it has nothing left to drain (it exited
        // within 0.4 s in earlier runs).
        within(120, "the old generation exited", || !alive(&old));
        let new = main_pid();
        assert_ne!(new, old);
        let new_exe = exe(&new);
        assert_ne!(new_exe, old_exe);
        if target == "next" {
            assert!(new_exe.contains("nix-grpc-daemon-next"), "{new_exe}");
        }
        assert_journal(UNIT, &format!("event=handed_over pid={new}$"));
        // Written once the successor has seen its predecessor go, which can
        // come after the pid is gone: the line is the event.
        wait_journal_above(UNIT, &format!("event=predecessor_exited pid={old}$"), 0, &[UNIT]);
        assert_eq!(unit_state(UNIT), "active");
        quick_build("later");
    }

    // After a takeover the unit's main process is one systemd did not start.
    // A stop still has to drain it, as it drains one systemd started, and
    // with it the old generation that still drains.
    #[test]
    fn a_stop_after_a_takeover_drains_and_the_running_builds_complete_once() {
        warm("warm-stop");
        start_build("stopped-old");
        let old = main_pid();
        let new = take_over_into(other("next", "base"));
        println!("after the takeover: {}", main_pid_dump());
        start_build("stopped-new");
        let kills = journal_count(MACHINE, UNIT, "SIGKILL");
        succeed(&["systemctl", "stop", "--no-block", UNIT]);
        // The stop drains: had it killed the held builds, they would not
        // finish once below, and the journal would count a SIGKILL.
        retry(&[], "the stop drains", || match unit_state(UNIT).as_str() {
            "deactivating" => true,
            "active" => false,
            s => panic!("the stop did not drain: {s}\n{}", journal(MACHINE, UNIT)),
        });
        assert!(alive(&old) && alive(&new), "a generation did not drain");
        release("stopped-old");
        release("stopped-new");
        finished_once("stopped-old");
        finished_once("stopped-new");
        // Within 1.5 s in earlier runs.
        within(120, "the stopped daemon ended", || ended(UNIT));
        assert_eq!(
            journal_count(MACHINE, UNIT, "SIGKILL"),
            kills,
            "the stop killed processes"
        );
        assert_eq!(unit_result(MACHINE, UNIT), "success");
        // The socket starts it again.
        warm("after-stop");
    }

    // The client of a build that runs across a takeover is sent to the new
    // generation and follows its build on: its call stays on the old one.
    // The old one's linger for that client runs out (longer than the hold,
    // Core::holdMs, so the linger binds) while the build runs; the build
    // must still finish once.
    #[test]
    fn a_build_followed_across_a_takeover_outlives_the_old_generations_linger() {
        warm("warm-follow");
        let expired = journal_count(MACHINE, UNIT, "event=linger_expired drv=.*-slow-follow.drv state=assigned");
        start_build_with("follow", &["--option", "grpc-linger", "70"]);
        take_over_into(other("next", "base"));
        wait_journal_above(
            UNIT,
            "event=linger_expired drv=.*-slow-follow.drv state=assigned",
            expired,
            &[UNIT, "follow"],
        );
        // Revoked there once no call waits for it: its client's does. The
        // mark comes with the linger's end (under 2 ms later in earlier
        // runs); a revoke outright never marks it, and its client builds
        // again.
        within(60, "the followed build marked to be revoked once uncalled", || {
            journal_count(MACHINE, UNIT, "event=revoke_once_uncalled drv=.*-slow-follow.drv") > 0
        });
        // Marked, it runs on while its client's call waits: held for
        // `FOLLOW_HOLD_STEPS` steps after the mark, through as many of the
        // builder's cancel polls (every 200 ms), it is neither ended nor
        // started again.
        for _ in 0..FOLLOW_HOLD_STEPS {
            assert_eq!(
                journal_count(MACHINE, UNIT, "event=revoked_uncalled drv=.*-slow-follow.drv"),
                0,
                "the followed build was revoked while its client waited"
            );
            assert_eq!(starts("follow"), 1, "build follow was started again");
            std::thread::sleep(std::time::Duration::from_millis(500));
        }
        release("follow");
        finished_once("follow");
    }

    // Steps of 500 ms a followed build is held after its mark: 20 s at the
    // least, 100 of the builder's cancel polls.
    const FOLLOW_HOLD_STEPS: u32 = 40;

    // The client of a build that runs across a takeover goes for good. Once
    // its linger on the old generation is over, its call there gone, the
    // build is revoked, and the old generation, with nothing left to drain,
    // exits.
    #[test]
    fn a_build_nobody_follows_after_a_takeover_is_revoked_once_its_linger_is_over() {
        warm("warm-orphan");
        let expired = journal_count(MACHINE, UNIT, "event=linger_expired drv=.*-slow-orphan.drv state=assigned");
        start_build_with("orphan", &["--option", "grpc-linger", "70"]);
        let old = main_pid();
        take_over_into(other("next", "base"));
        succeed(&["systemctl", "kill", "--signal=KILL", "orphan"]);
        wait_journal_above(
            UNIT,
            "event=linger_expired drv=.*-slow-orphan.drv state=assigned",
            expired,
            &[UNIT],
        );
        // Its call is gone, so the build ends within a poll of the revoke
        // (0.011 to 0.203 s after the linger's end in earlier runs), and the
        // old generation with it (0.02 to 4.3 s later in earlier runs).
        within(60, "the orphan's build revoked once uncalled", || {
            journal_count(MACHINE, UNIT, "event=revoked_uncalled drv=.*-slow-orphan.drv") > 0
        });
        within(120, "the old generation exited", || !alive(&old));
        warm("after-orphan");
    }

    // A new generation with an idle timeout outlives its draining
    // predecessor, then idles out, and systemd sees it go.
    #[test]
    fn idle_exit_after_a_takeover_waits_for_the_predecessor() {
        warm("warm-idle");
        start_build("idle-long");
        let old = main_pid();
        let new = take_over_into("idle");
        release("idle-long");
        finished_once("idle-long");
        // 20 s idle timeout, counted from the last call (28.5 s at most in
        // earlier runs).
        within(120, "the daemon idled out", || ended(UNIT));
        assert!(!alive(&old) && !alive(&new));
        let log = query(MACHINE, &["journalctl", "-u", UNIT, "--no-pager"]);
        let at = |what: &str| {
            log.lines()
                .position(|l| l.contains(&format!("nix-grpc-daemon[{new}]:")) && l.trim_end().ends_with(what))
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

// Reloads while older generations still build. The unit's main process
// (the newest generation) may not end while one of them lives; any other
// generation exits once it has drained.
pub mod chain {
    use super::*;

    // Steps of 500 ms the main process is watched draining while the oldest
    // generation still builds: 10 s, ten of its exit checks (one a second).
    const STOP_HOLD_STEPS: u32 = 20;

    // A reload while the previous generation still drains, and another: the
    // second generation, with nothing of its own to drain, exits while the
    // first still builds, and the main process sees the first one go itself.
    // Every build completes once.
    #[test]
    fn a_middle_generation_exits_once_drained_while_the_first_still_builds() {
        warm("warm-chain");
        start_build("chain");
        let first = main_pid();
        let second = take_over_into(other("next", "base"));
        quick_build("chain-second");
        let third = take_over_into(other("next", "base"));
        quick_build("chain-third");
        // Its one build is over: nothing left to drain.
        within(120, "the second generation exited", || !alive(&second));
        assert_eq!(
            unit_state("chain"),
            "active",
            "the long build was over too soon"
        );
        assert!(alive(&first), "the first generation left early");
        release("chain");
        finished_once("chain");
        within(120, "the first generation exited", || !alive(&first));
        assert_eq!(main_pid(), third);
        // Written at the main process's next check after the exit.
        within(10, "the main process saw the first generation exit", || {
            logged_by(&third, &format!("event=predecessor_exited pid={first}")) > 0
        });
        assert_eq!(unit_state(UNIT), "active");
        quick_build("chain-later");
    }

    // After two reloads the second generation is gone, by itself or killed,
    // while the first still builds. A stop then drains the main process,
    // which stays until the first generation's build is over: had it exited,
    // systemd would have killed that build (KillMode=mixed).
    #[test]
    fn the_main_process_outlives_the_first_generation_once_the_second_is_gone() {
        warm("warm-oldest");
        start_build("oldest");
        let first = main_pid();
        let second = take_over_into(other("next", "base"));
        let third = take_over_into(other("next", "base"));
        // A generation that waits for its predecessor would not go by itself.
        let _ = run(&["kill", "-KILL", &second]);
        within(120, "the second generation gone", || !alive(&second));
        within(10, "the main process saw the second generation go", || {
            logged_by(&third, &format!("event=predecessor_exited pid={second}")) > 0
        });
        let kills = journal_count(MACHINE, UNIT, "SIGKILL");
        succeed(&["systemctl", "stop", "--no-block", UNIT]);
        // Within a second of the stop (its next check).
        within(10, "the main process drains", || logged_by(&third, "event=draining") > 0);
        for _ in 0..STOP_HOLD_STEPS {
            assert!(
                alive(&third),
                "the main process exited while the first generation still built\n{}",
                journal(MACHINE, UNIT)
            );
            assert!(alive(&first), "the first generation was gone before its build was over");
            assert_eq!(unit_state("oldest"), "active", "the first generation's build ended early");
            std::thread::sleep(std::time::Duration::from_millis(500));
        }
        release("oldest");
        finished_once("oldest");
        within(120, "the stopped daemon ended", || ended(UNIT));
        assert_eq!(
            journal_count(MACHINE, UNIT, "SIGKILL"),
            kills,
            "the stop killed processes"
        );
        assert_eq!(unit_result(MACHINE, UNIT), "success");
        warm("after-oldest");
    }

    // A generation that handed over and drained keeps no nix-daemon
    // connection, though an older one still builds: the paths its sessions
    // held go back to the garbage collector.
    #[test]
    fn a_drained_generation_keeps_no_nix_daemon_connection_while_an_older_one_builds() {
        warm("warm-roots");
        start_build("roots");
        let second = take_over_into(other("next", "base"));
        let held = quick_build("roots-second");
        let workers = holders(&held);
        assert!(
            !workers.is_empty() && daemon_connections(&second) > 0,
            "the second generation holds nothing, so its letting go proves nothing: {workers:?}"
        );
        take_over_into(other("next", "base"));
        within(120, "the second generation's nix-daemon connections ended", || {
            daemon_connections(&second) == 0
        });
        within(10, "its nix-daemon processes' temp roots are gone", || {
            workers.iter().all(|w| !temp_roots_file(w))
        });
        assert_eq!(holders(&held), Vec::<String>::new());
        assert_eq!(
            unit_state("roots"),
            "active",
            "the first generation's build was over, so this proves nothing"
        );
        release("roots");
        finished_once("roots");
    }

    // Steps of 500 ms a rollback's main process is watched draining while
    // the first generation still builds: 20 s, past the 6 s in which a
    // second generation that left at the takeover would have exited (its
    // 5 s shutdown grace and a second).
    const ROLLBACK_HOLD_STEPS: u32 = 40;

    // A rollback: the newest generation runs a daemon that predates the
    // links between generations (tests/handover.nix `old`) and watches its
    // predecessor alone. That one, having handed over to a successor that
    // does not watch the older generations, waits for them itself, so a
    // stop drains them all and the first generation's build completes.
    #[test]
    fn a_rollback_to_a_daemon_without_links_keeps_the_first_generations_build() {
        warm("warm-rollback");
        start_build("rollback");
        let first = main_pid();
        take_over_into(other("next", "base"));
        let third = take_over_into("old");
        let old = env("NGS_OLD_DAEMON");
        assert!(exe(&third).starts_with(&old), "{} is not {old}", exe(&third));
        let kills = journal_count(MACHINE, UNIT, "SIGKILL");
        succeed(&["systemctl", "stop", "--no-block", UNIT]);
        within(10, "the main process drains", || logged_by(&third, "event=draining") > 0);
        for _ in 0..ROLLBACK_HOLD_STEPS {
            assert!(
                alive(&third),
                "the main process exited while the first generation still built\n{}",
                journal(MACHINE, UNIT)
            );
            assert!(alive(&first), "the first generation was gone before its build was over");
            assert_eq!(unit_state("rollback"), "active", "the first generation's build ended early");
            std::thread::sleep(std::time::Duration::from_millis(500));
        }
        release("rollback");
        finished_once("rollback");
        within(120, "the stopped daemon ended", || ended(UNIT));
        assert_eq!(
            journal_count(MACHINE, UNIT, "SIGKILL"),
            kills,
            "the stop killed processes"
        );
        assert_eq!(unit_result(MACHINE, UNIT), "success");
        // Back to this daemon for the tests after: the socket starts it.
        let out = switch_to("base");
        assert_eq!(out.code, 0, "switch to base:\n{}", out.combined());
        warm("after-rollback");
    }

    // A generation that handed over and still builds lets its sessions go as
    // one that serves does, `SESSION_TTL` after their last call, though it
    // lives on for its build.
    #[test]
    fn a_generation_still_building_after_a_takeover_ends_its_sessions() {
        warm("warm-sessions");
        take_over_into("short-sessions");
        let held = quick_build("sessions-held");
        assert!(
            !holders(&held).is_empty(),
            "the session holds nothing, so its end proves nothing"
        );
        start_build("sessions-long");
        let old = main_pid();
        take_over_into(other("next", "base"));
        // The session's last call was quick_build's; the bound is twice the
        // TTL, in steps.
        within(2 * SESSION_TTL, "the old generation's session ended", || {
            holders(&held).is_empty()
        });
        assert!(alive(&old), "the old generation exited, so this proves nothing");
        assert_eq!(unit_state("sessions-long"), "active");
        release("sessions-long");
        finished_once("sessions-long");
        within(120, "the old generation exited", || !alive(&old));
    }

    // tests/handover.nix's short-sessions: --session-ttl.
    const SESSION_TTL: u64 = 60;
}

pub mod failures {
    use super::*;

    // The successor gets the sockets and then fails: the old generation
    // takes builds again at once.
    #[test]
    fn a_takeover_that_fails_leaves_the_old_generation_serving() {
        warm("warm-broken");
        start_build("broken-long");
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
        release("broken-long");
        finished_once("broken-long");
        assert_eq!(unit_pids(), vec![old]);
    }

    // systemd kills a reload that takes too long. The successor it started
    // dies with it, not on its own timeout, and the old generation serves on.
    #[test]
    fn a_reload_that_systemd_times_out_leaves_the_old_generation_serving() {
        warm("warm-timeout");
        let old = main_pid();
        let unanswered = journal_count(MACHINE, UNIT, "sent no sockets");
        // The old process answers nothing, so the successor waits.
        succeed(&["kill", "-STOP", &old]);
        let out = switch_to("quick-timeout");
        let timed_out = query(MACHINE, &["journalctl", "-u", UNIT, "--no-pager"])
            .contains("Reload operation timed out");
        let others: Vec<String> = unit_pids().into_iter().filter(|p| *p != old).collect();
        // Gone because systemd gave up on the reload, not after its own wait
        // for the sockets (30 s), which ends in "sent no sockets": by what
        // it says, not by when.
        retry(&[], "the successors exited", || others.iter().all(|p| !alive(p)));
        succeed(&["kill", "-CONT", &old]);
        assert_eq!(
            journal_count(MACHINE, UNIT, "sent no sockets"),
            unanswered,
            "the successor left on its own wait, not with the reload: {others:?}"
        );
        assert_ne!(out.code, 0, "the reload succeeded:\n{}", out.combined());
        assert!(timed_out, "systemd did not time the reload out");
        assert_eq!(main_pid(), old);
        quick_build("timeout-after");
        assert_eq!(unit_pids(), vec![old]);
    }

    // systemd stops the unit when its main process dies, whoever started it.
    // The predecessor's builds go with it, and their clients build again.
    #[test]
    fn a_crash_of_the_new_generation_is_recovered_from() {
        warm("warm-crash");
        start_build("crash-long");
        let new = take_over_into(other("next", "base"));
        succeed(&["kill", "-KILL", &new]);
        // Its client builds again: release each build until it ends.
        retry(&[], "crash-long ended", || {
            let _ = try_release("crash-long");
            ended("crash-long")
        });
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
             s.connect(b'\\0nix-grpc-daemon/takeover/{pid}'); \
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
        // Same user, another cgroup: that of the e2e agent's unit.
        refused(
            "exec setpriv --reuid=nix-grpc-daemon --regid=nix-grpc-daemon --clear-groups",
            "another cgroup",
        );
        // Same cgroup, another user: root moved into the unit.
        refused(&format!("echo $$ > {CGROUP_PROCS} && exec"), "another user");
    }
}

// tests/handover.nix's "gc": the farm's store loses an input between the
// client's plan, which counted it present, and the build that needs it.
pub mod gc {
    use super::*;

    // `input` of NGS_GC_EXPR for `tag`, built by the node itself: no farm
    // session holds it. Returns its path.
    fn input_built_here(tag: &str) -> String {
        let expr = env("NGS_GC_EXPR");
        succeed(&[
            "nix", "build", "--no-link", "--print-out-paths", "-f", &expr, "--argstr", "tag", tag, "input",
        ])
        .trim()
        .to_string()
    }

    // The client builds `consumer` in unit `tag`. Returns once the slow
    // build it waits for runs: the client has planned, with the input
    // present.
    fn start_consumer(tag: &str) {
        let expr = env("NGS_GC_EXPR");
        let store = format!("{}&reschedule-retries=2", store());
        let out = format!(r#""$@" > /root/{tag}.out"#);
        succeed(&[
            "systemd-run", "--unit", tag, "--setenv=PATH=/run/current-system/sw/bin", "sh", "-c", &out, "sh",
            "nix", "build", "-L", "--no-link", "--print-out-paths", "--store", &store, "--eval-store", "local",
            "-f", &expr, "--argstr", "tag", tag, "consumer",
        ]);
        retry(&[(MACHINE, tag)], "the slow build started", || starts(tag) > 0);
    }

    // The processes whose temp roots name `path` and that serve a
    // connection of nix-grpc-daemon's (a nix-daemon worker whose unix
    // socket's peer is in nix-grpc-daemon's unit), not a `nix` client's.
    // `ss -xpn` fields: netid, state, queues, local path and inode, peer
    // path and inode, process.
    fn daemon_holders(path: &str) -> Vec<String> {
        let served = query(
            MACHINE,
            &[
                "bash",
                "-c",
                r#"set -o pipefail
ss -xpn | awk -v ours=" $1 " '
  match($0, /pid=[0-9]+/) { pid = substr($0, RSTART + 4, RLENGTH - 4); line[NR] = pid; local[NR] = $6
    if (index(ours, " " pid " ")) theirs[$8] = 1 }
  END { for (i = 1; i <= NR; i++) if ((i in line) && (local[i] in theirs)) print line[i] }'"#,
                "sh",
                &unit_pids().join(" "),
            ],
        );
        let served: Vec<&str> = served.split_whitespace().collect();
        holders(path).into_iter().filter(|p| served.contains(&p.as_str())).collect()
    }

    // The input goes as a garbage collector that wins the race takes it:
    // the connections holding it end first, as a session's expiry would.
    // Within 20 tries, else the test fails with what holds it.
    fn collect(path: &str) {
        let mut last = String::new();
        for _ in 0..20 {
            for p in daemon_holders(path) {
                run(&["kill", "-9", &p]);
            }
            let out = run(&["nix-store", "--delete", path]);
            if out.code == 0 {
                return;
            }
            last = out.combined();
            std::thread::sleep(std::time::Duration::from_millis(500));
        }
        let roots = run(&["nix-store", "-q", "--roots", path]).combined();
        panic!("{path} not deleted: {last}\nroots: {roots}\nnix-grpc-daemon's: {:?}", unit_pids());
    }

    fn log_of(tag: &str) -> String {
        query(MACHINE, &["journalctl", "-u", tag])
    }

    // Built again on the one builder, with no cache to tell whether it was
    // lost: given back unnamed, the client would ask the scheduler until
    // its reschedule-retries ran out.
    #[test]
    fn an_input_collected_after_the_plan_is_built_again() {
        let tag = "gc-lost";
        let input = input_built_here(tag);
        start_consumer(tag);
        collect(&input);
        let gate = format!("{tag}-gate");
        retry(&[(MACHINE, tag)], "the slow build released", || try_release(&gate));
        retry(&[], &format!("{tag} ended"), || ended(tag));
        let log = log_of(tag);
        assert!(log.contains(&format!("farm lacks '{input}'")), "not built again:\n{log}");
        assert_eq!(unit_result(MACHINE, tag), "success", "{log}");
        assert!(!log.contains("gave up after"), "{log}");
        assert_eq!(log.matches(&format!("GC-INPUT-BUILT-{tag}")).count(), 1, "{log}");
        succeed(&["nix-store", "--verify-path", &input]);
    }

    // The farm holds an input it told the client is present, for the
    // client's session, from the plan on: as a local nix-daemon's goals
    // root an output before they check it.
    #[test]
    fn an_input_the_plan_counted_present_stays_held() {
        let tag = "gc-held";
        let input = input_built_here(tag);
        assert!(holders(&input).is_empty(), "{:?} hold {input} before the plan", holders(&input));
        start_consumer(tag);
        assert!(!daemon_holders(&input).is_empty(), "the plan left {input} unheld");
        let deleted = run(&["nix-store", "--delete", &input]);
        assert_ne!(deleted.code, 0, "{input} deleted although held");
        assert!(deleted.combined().contains("still alive"), "{}", deleted.combined());
        let gate = format!("{tag}-gate");
        retry(&[(MACHINE, tag)], "the slow build released", || try_release(&gate));
        retry(&[], &format!("{tag} ended"), || ended(tag));
        let log = log_of(tag);
        assert_eq!(unit_result(MACHINE, tag), "success", "{log}");
        assert!(!log.contains("farm lacks"), "{log}");
        assert!(!log.contains(&format!("GC-INPUT-BUILT-{tag}")), "built again: {log}");
    }

    // What the garbage collector takes between the worker's query and its
    // hold is not answered present. A client copies PATHS paths the worker
    // has, from a store of its own. The worker holds them for its session
    // one temp root at a time, in store path order. Once it holds the
    // first, the test stops its nix-daemon connection and the collector
    // deletes the last 2000; then the connection goes on. (A connection
    // stopped inside the collector's lock is let go and stopped again.)
    // A path answered
    // present but held only after it was gone is neither copied nor there
    // afterwards. A path answered absent is copied, and is there.
    #[test]
    fn a_path_collected_before_it_is_held_is_not_answered_present() {
        const PATHS: usize = 40000;
        let script = r#"set -eu
# In memory: the node's disk has too few inodes for these.
n=$1; dir=/dev/shm/gc-race; mkdir -p $dir/files
nonce=$(cat /proc/sys/kernel/random/uuid)
i=0; while [ $i -lt $n ]; do echo "gc-race $nonce $i" > $dir/files/$i; i=$((i + 1)); done
find $dir/files -type f | xargs nix-store --add | sort > $dir/paths
nix copy --no-check-sigs --to "local?root=$dir/src" --stdin < $dir/paths
roots=/nix/var/nix/temproots
before=" $(ls $roots | tr '\n' ' ') "
nix copy --no-check-sigs --from "local?root=$dir/src" --to "$2" --stdin < $dir/paths > $dir/copy.out 2>&1 &
copy=$!
# The worker's session opens its nix-daemon connection to hold the
# first path; that connection's temp roots file is the one new here.
# It stops at once, mid-hold, so the collector surely comes first for
# the paths it holds last.
polls=0; worker=
while [ -z "$worker" ]; do
  for f in $roots/*; do
    [ -e "$f" ] || continue
    case $before in *" ${f##*/} "*) ;; *) worker=${f##*/} ;; esac
  done
  polls=$((polls + 1))
  kill -0 $copy 2>/dev/null || { echo "the copy ended before any hold"; cat $dir/copy.out; exit 1; }
done
sort -r $dir/paths | head -n 2000 > $dir/last
# Stopped while it holds the garbage collector's lock (nix takes it
# shared around adding temp roots), the connection keeps the collector
# waiting: then it goes on for a moment and is stopped again.
tries=0; waits=0; clear=0; : > $dir/delete.out
while :; do
  tries=$((tries + 1))
  kill -STOP $worker
  nix-store --delete $(cat $dir/last) >> $dir/delete.out 2>&1 &
  gc=$!
  w=0
  while kill -0 $gc 2>/dev/null && [ $w -lt 50 ]; do sleep 0.1; w=$((w + 1)); done
  waits=$((waits + w))
  kill -0 $gc 2>/dev/null || { clear=1; break; }
  kill $gc 2>/dev/null || true; wait $gc || true
  kill -CONT $worker
  [ $tries -lt 10 ] || break
done
held=$(tr '\0' '\n' < $roots/$worker | grep -c -F -f $dir/paths || true)
kill -CONT $worker
wait $gc || true
copied=0; wait $copy || copied=$?
lost=$(xargs nix-store --check-validity --print-invalid < $dir/paths | wc -l)
echo "polls=$polls tries=$tries clear=$clear held=$held waits=$waits copy=$copied lost=$lost deleted=$(awk '/store paths deleted/ { n += $1 } END { print n + 0 }' $dir/delete.out)"
"#;
        let store = store();
        let out = succeed(&["bash", "-c", script, "sh", &PATHS.to_string(), &store]);
        println!("[e2e] gc race: {}", out.trim());
        let field = |name: &str| -> usize {
            out.split_whitespace()
                .find_map(|f| f.strip_prefix(&format!("{name}=")))
                .and_then(|v| v.parse().ok())
                .unwrap_or_else(|| panic!("no {name} in {out}"))
        };
        assert_eq!(field("copy"), 0, "{out}");
        // Stopped inside the garbage collector's lock every time: the
        // collector never ran beside the stopped hold.
        assert_eq!(field("clear"), 1, "the connection held the collector's lock each time it stopped, so this proves nothing: {out}");
        // The hold adds one temp root at a time, in path order, so a stop
        // after its first leaves the last 2000 to the collector. A hold
        // that added them all at once (addTempRoots) would leave none, and
        // this test would prove nothing: it fails here instead.
        assert!(field("deleted") > 0, "the collector took nothing, so this proves nothing: {out}");
        assert_eq!(field("lost"), 0, "answered present, then gone: {out}");
    }
}
