use crate::fixtures::*;
use crate::harness::{MACHINE, journal_count, query, retry, unit_result};

#[test]
fn idle_exit_and_socket_activation() {
    succeed(&["systemctl", "stop", "nix-grpc-daemon.service"]);
    assert_eq!(unit_state("nix-grpc-daemon.service"), "inactive");
    let idle0 = journal_count(MACHINE, "nix-grpc-daemon", "event=idle_exit");
    let out = succeed(&["nix", "store", "info", "--json", "--store", STORE]);
    assert!(out.contains(r#""url":"grpc://127.0.0.1:50051"#), "{out}");
    assert_eq!(unit_state("nix-grpc-daemon.service"), "active");
    let pid = query(MACHINE, &["systemctl", "show", "-P", "MainPID", "nix-grpc-daemon.service"]);
    // The daemon exits when idle: its line and then its process gone, which
    // no new activation undoes, unlike the unit's state.
    wait_journal_above("nix-grpc-daemon", "event=idle_exit", idle0, &["nix-grpc-daemon"]);
    wait_until_succeeds(&["sh", "-c", &format!("! test -d /proc/{}", pid.trim())]);
    succeed(&["nix", "store", "info", "--store", STORE]);
}

#[test]
fn nix_daemon_restart_does_not_fail_the_next_rpc() {
    let p = hello_path();
    write_file("/tmp/warm", "warm");
    succeed(&["nix", "store", "add", "--store", STORE, "/tmp/warm"]);
    assert_journal(
        "nix-daemon",
        "accepted connection from pid .*nix-grpc-daemon",
    );
    let main_pid = || {
        query(
            MACHINE,
            &["systemctl", "show", "-P", "MainPID", "nix-daemon.service"],
        )
    };
    let pid = main_pid();
    // SIGKILL, like a crashed container: no orderly close of the pooled connections.
    succeed(&["systemctl", "kill", "-s", "KILL", "nix-daemon.service"]);
    // Gone is all this waits for: every builder probes nix-daemon every 10 s,
    // and through its socket the next probe starts a new one, maybe at once.
    wait_until_succeeds(&["sh", "-c", &format!("! test -d /proc/{}", pid.trim())]);
    succeed(&["systemctl", "start", "nix-daemon.socket"]);
    succeed(&["nix", "path-info", "--store", STORE, p]);
    write_file("/tmp/warm", "again");
    succeed(&["nix", "store", "add", "--store", STORE, "/tmp/warm"]);
}

fn build_argv<'a>(store: &'a str, file: &'a str) -> Vec<&'a str> {
    vec![
        "nix",
        "build",
        "--store",
        store,
        "--eval-store",
        "local",
        "--impure",
        "--no-link",
        "-f",
        file,
    ]
}

#[test]
fn a_failed_connection_while_nix_daemon_is_down_does_not_poison_the_worker() {
    let unit = "nix-grpc-daemon-mtls.service";
    wait_for_unit(unit);
    wait_for_open_port(50052);
    let store = cert_store("client");
    let main_pid = || query(MACHINE, &["systemctl", "show", "-P", "MainPID", unit]);
    let pid = main_pid();

    // Pool a connection first, then keep the daemon down long enough for an RPC to fail.
    succeed(&build_argv(&store, "/etc/hello.nix"));
    write_file(
        "/tmp/reconnect.nix",
        &drv_expr("reconnect-after-daemon-restart", "echo reconnected > $out"),
    );
    succeed(&[
        "systemctl",
        "stop",
        "nix-daemon.socket",
        "nix-daemon.service",
    ]);
    // A client that may retry for long: it is stopped once its RPC has
    // failed on the unavailable daemon.
    let down = "build-while-down";
    let mut argv = vec!["systemd-run", "--unit", down];
    argv.extend(build_argv(&store, "/tmp/reconnect.nix"));
    succeed(&argv);
    let lost = "local nix-daemon connection lost";
    retry(&[(MACHINE, down)],"the RPC reached the unavailable daemon", || {
        journal_count(MACHINE, down, lost) > 0
    });
    // Still running, or ended by itself with a failure: a stopped unit
    // would read as a success.
    let gone = ended(down);
    assert!(
        !gone || unit_result(MACHINE, down) != "success",
        "build succeeded with the daemon down"
    );
    run(&["systemctl", "stop", down]);
    succeed(&["systemctl", "start", "nix-daemon.socket"]);

    wait_until_succeeds(&["nix", "store", "info", "--store", "daemon"]);
    wait_until_succeeds(&build_argv(&store, "/tmp/reconnect.nix"));
    assert_eq!(
        pid,
        main_pid(),
        "the worker recovered only because it restarted"
    );
}

#[test]
fn renewed_server_certificate_is_served_without_restart() {
    wait_for_unit("nix-grpc-daemon-mtls.service");
    wait_for_open_port(50052);
    let d = cert_dir();
    succeed(&[
        "bash",
        "-c",
        r#"cd "$1" &&
           openssl req -newkey rsa:2048 -nodes -keyout new.key -out new.csr -subj /CN=localhost 2>/dev/null &&
           openssl x509 -req -in new.csr -days 1 -CA ca.pem -CAkey ca.key -set_serial 0x$RANDOM \
             -extfile <(printf 'subjectAltName=DNS:localhost,DNS:renewed.example') -out new.pem &&
           mv new.key server.key && mv new.pem server.pem"#,
        "bash",
        &d,
    ]);
    retry(&[], "the server presents the renewed certificate", || {
        // s_client may exit non-zero because the server wants a client
        // certificate, so only the printed SANs count.
        let sans = succeed(&[
            "sh",
            "-c",
            "openssl s_client -connect localhost:50052 </dev/null 2>/dev/null \
             | openssl x509 -noout -ext subjectAltName || true",
        ]);
        sans.contains("renewed.example")
    });
    succeed(&[
        "nix",
        "store",
        "info",
        "--json",
        "--store",
        &cert_store("client"),
    ]);
}

#[test]
fn default_client_cert_lookup() {
    wait_for_unit("nix-grpc-daemon-mtls.service");
    wait_for_open_port(50052);
    let d = cert_dir();
    succeed(&["install", "-d", "/var/lib/nix-grpc-store"]);
    succeed(&[
        "install",
        "-m",
        "0644",
        &format!("{d}/client.pem"),
        "/var/lib/nix-grpc-store/client.crt",
    ]);
    succeed(&[
        "install",
        "-m",
        "0600",
        &format!("{d}/client.key"),
        "/var/lib/nix-grpc-store/client.key",
    ]);
    let out = run(&[
        "nix",
        "store",
        "add",
        "--store",
        &anon_store(),
        "/etc/hello.nix",
    ]);
    succeed(&["rm", "-r", "/var/lib/nix-grpc-store"]);
    assert_eq!(out.code, 0, "{}", out.combined());
}

/// The relay nix-grpc-daemon-recorded reaches nix-daemon through outlives a
/// connection it could not pass on while nix-daemon was down, as the daemon's
/// own probe of nix-daemon makes during a restart.
#[test]
fn the_recorder_relays_again_once_nix_daemon_is_back() {
    let relay = "/run/nix-grpc-recorded/socket";
    wait_for_unit("nix-daemon-recorder.service");
    succeed(&["sh", "-c", "until test -S \"$1\"; do sleep 0.1; done", "sh", relay]);
    succeed(&["systemctl", "stop", "nix-daemon.socket", "nix-daemon.service"]);
    let store = format!("unix://{relay}");
    fail(&["nix", "store", "info", "--store", &store]);
    succeed(&["systemctl", "start", "nix-daemon.socket"]);
    succeed(&["nix", "store", "info", "--store", &store]);
    wait_for_unit("nix-grpc-daemon-recorded.service");
    write_file(
        "/tmp/recorder-after-refusal.nix",
        &drv_expr("recorder-after-refusal", "echo relayed > $out"),
    );
    succeed(&build_argv(
        "grpc://127.0.0.1:50055?insecure=1",
        "/tmp/recorder-after-refusal.nix",
    ));
}
