//! tests/bounce.nix: a node that is its own scheduler's only builder, with
//! one build slot, and a client with one build thread. A job whose input the
//! node lost and cannot fetch is given back by the worker, unnamed, and
//! placed again at once; its client bounces it and, the third time, lets it
//! rest. Unnamed because the node's niks3 cache (--niks3-cache-url) holds
//! the input, which the node published when it built it (and again when it
//! answered the client's query): the cache did not lose it (Cache::lost),
//! so another worker might get it (gatherInputs). This node cannot, as it
//! has no substituter (tests/bounce.nix). A job
//! the client wants meanwhile must get the slot: the bounced job's
//! placements are cancelled while the client does not follow it
//! (src/client/executor.hh, Job::followed), instead of holding the slot
//! until each expires unclaimed, again and again.
use crate::fixtures::*;
use crate::harness::{MACHINE, env, journal_count, retry};

const UNIT: &str = "nix-grpc-daemon.service";

/// One build thread, so the client wants one job at a time; three
/// reschedules, so the bounced job rests twice before it gives up.
const STORE_ARGS: &str = "&max-builds=1&reschedule-retries=3";

/// From the held build's release to the client's exit, which takes the
/// bounced job's five calls and rests (about 4 s with the fix), in polls of
/// 500 ms: 120 s. Counted, not timed: a pause of the whole sandbox (a farm
/// freezing it) pauses the client too, and spends at most one poll of it.
/// Without the fix the client never exits, and the first placement it left
/// expires after the worker's 60 s (Builder::expectTimeout).
const RELEASE_TO_EXIT_POLLS: u32 = 240;

fn instantiate(attr: &str, tag: &str) -> String {
    let expr = env("NGS_BOUNCE_EXPR");
    succeed(&["nix-instantiate", &expr, "-A", attr, "--argstr", "tag", tag])
        .trim()
        .to_string()
}

fn output(drv: &str) -> String {
    succeed(&["nix-store", "-q", "--outputs", drv]).trim().to_string()
}

/// The scheduler's `event` lines about `drv`.
fn scheduled(drv: &str, event: &str) -> u64 {
    let base = drv.trim_start_matches("/nix/store/");
    journal_count(MACHINE, UNIT, &format!("event={event} drv={base} "))
}

/// The scheduler's lines of a Done for `drv` that gave it back for `reason`.
fn done(drv: &str, reason: &str) -> u64 {
    let base = drv.trim_start_matches("/nix/store/");
    journal_count(
        MACHINE,
        UNIT,
        &format!("event=done drv={base} .*reason={reason}"),
    )
}

/// Lets the held build of `tag` end: its builder reads a line from the
/// fifo in its build directory (as tests/e2e/handover.rs release). The
/// open does not block: no reader yet fails it, ENXIO.
fn try_release(tag: &str) -> bool {
    let script = r#"p=$(pgrep -f "^sh -c mkfifo release; echo HELD-STARTED-$1 ") && python3 -c '
import os, sys
os.write(os.open(sys.argv[1], os.O_WRONLY | os.O_NONBLOCK), b"\n")' "/proc/$p/cwd/release""#;
    run(&["sh", "-c", script, "sh", tag]).code == 0
}

#[test]
fn a_bounced_job_s_placements_do_not_starve_the_job_wanted_instead() {
    let tag = "starve";
    let expr = env("NGS_BOUNCE_EXPR");
    let store = format!("{STORE}{STORE_ARGS}");
    // The input, built through the node first: valid there, so the client
    // plans no job for it.
    let built = run(&[
        "nix", "build", "--no-link", "--print-out-paths", "--store", &store, "--eval-store", "local",
        "-f", &expr, "input", "--argstr", "tag", tag,
    ]);
    assert_eq!(built.code, 0, "the input failed: {}", built.combined());
    let input = built.stdout.trim().to_string();
    let user = instantiate("user", tag);
    let wanted = instantiate("wanted", tag);

    let cmd = format!(r#""$@" > /root/{tag}.out 2> /root/{tag}.err"#);
    succeed(&[
        "systemd-run", "--unit", tag, "--setenv=PATH=/run/current-system/sw/bin", "sh", "-c", &cmd,
        "sh", "nix", "build", "-L", "--keep-going", "--no-link", "--print-out-paths", "--store",
        &store, "--eval-store", "local", "-f", &expr, "top", "wanted", "--argstr", "tag", tag,
    ]);
    retry(&[(MACHINE, tag)], "the held build started", || held_started(tag));
    // Lost on the node, with no substituter, and held by niks3, so not lost
    // there: the worker gives the user back without naming the input
    // (gatherInputs), so its client bounces it.
    // Deletable once no session holds it: the one that built it and, with
    // farm-hold-planned-inputs, the client's, whose plan found it present
    // (--session-ttl in tests/bounce.nix).
    retry(&[(MACHINE, tag)], "the input deleted", || {
        run(&["nix-store", "--delete", &input]).code == 0
    });
    fail(&["nix-store", "--check-validity", &input]);
    retry(&[(MACHINE, tag)], "the held build released", || try_release(tag));

    let mut polls = 0;
    while !ended(tag) {
        assert_eq!(
            done(&user, "UNREACHABLE"),
            0,
            "a placement of the bounced job expired unclaimed: the client left it to the scheduler"
        );
        assert!(
            polls < RELEASE_TO_EXIT_POLLS,
            "the client did not exit within {RELEASE_TO_EXIT_POLLS} polls of the release"
        );
        polls += 1;
        std::thread::sleep(std::time::Duration::from_millis(500));
    }
    assert_eq!(done(&user, "UNREACHABLE"), 0, "a placement of the bounced job expired unclaimed");

    let err = succeed(&["cat", &format!("/root/{tag}.err")]);
    assert!(
        !err.contains("farm lacks"),
        "the worker named the input, so the client built it instead of bouncing the job: {err}"
    );
    assert!(err.contains("gave up after"), "the bounced job did not give up: {err}");
    succeed(&["nix-store", "--check-validity", &output(&wanted)]);
    // At most three placements are never called. Each call is given back
    // for the missing input (FAILED_PRECONDITION: bounceOf in bounce.hh
    // makes it refused and left), and the scheduler places the job again at
    // once, once: the node has one slot, which that placement holds until
    // the client calls it or it expires, and none expired (asserted above).
    // Executor::bounced with reschedule-retries 3 then
    // - 1st give-back: free (`left`, the first per worker), released and
    //   wanted again at once;
    // - 2nd: bounced 1, waits 1, wanted again at once;
    // - 3rd and 4th: bounced 2 and 3, waits > 1, so it rests;
    // - 5th: bounced 4 > 3, so it gives up.
    // Wanted again at once: the released job is the one the client's single
    // build thread wants next, as `user` outranks `wanted` (top's cp_hint,
    // tests/bounce.nix). Its Want goes out in the batch that would carry
    // the Cancel, and takeBatch takes Wants first, so takeCancels drops the
    // Cancel; Dispatcher::onWant sends the placement the scheduler already
    // made (`res.assigned`), and Executor::onAssigned takes it while the job
    // is Wanted, so it is called. After a rest or the give-up the client
    // cancels (Dispatcher::onCancel drops the entry and revokes that
    // placement): one placement each, never called. A rest's end sends a
    // new Want whose placement is called. So placements <= calls + 3
    // (2 rests + the give-up), equal when every give-back arrives before
    // the Want or Cancel that follows it; when the Want comes first, the
    // scheduler places the job there once, and the bound still holds. A
    // client that cancels a job it wants again loses a placement each time,
    // and may call one after its revoke.
    let calls = done(&user, "INPUT_UNAVAILABLE");
    let placed = scheduled(&user, "assigned");
    assert!(
        placed <= calls + 3,
        "{placed} placements of the bounced job for {calls} calls: more than one per rest and its end: {err}"
    );
    assert!(
        !err.contains("not scheduled here"),
        "the client called a placement of the bounced job that it had cancelled: {err}"
    );
}

/// The held build of `tag` runs (try_release's pattern).
fn held_started(tag: &str) -> bool {
    run(&["pgrep", "-f", &format!("^sh -c mkfifo release; echo HELD-STARTED-{tag} ")]).code == 0
}

/// The worker marks every status its BuildDerivation handler returns
/// (src/build-answer.hh), so its client tells a give-back whose message
/// quotes a broken stream from a stream that broke (nixgrpc::buildGoesOn).
/// Here one it refuses, a call in a protocol it does not speak.
#[test]
fn a_worker_marks_its_build_answers() {
    let proto = env("NGS_PROTO");
    let out = run(&[
        "grpcurl", "-plaintext", "-v", "-import-path", &proto, "-proto", "nix_remote.proto", "-d",
        r#"{"protocol": 0}"#, "127.0.0.1:50051", "nix.remote.NixRemote/BuildDerivation",
    ]);
    let text = out.combined();
    assert!(text.contains("unsupported build protocol or mode"), "not the handler's answer: {text}");
    assert!(text.contains("x-nix-answered: 1"), "the answer is not marked: {text}");
}
