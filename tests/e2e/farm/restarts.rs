use super::helpers::*;
use crate::harness::*;

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
