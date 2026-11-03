use std::time::Instant;

use crate::fixtures::*;
use crate::harness::{MACHINE, journal_matches};

#[test]
fn add_path_and_read_it_back_locally() {
    let p = succeed(&["nix", "store", "add", "--store", STORE, "/etc/hello.nix"]);
    let p = p.trim();
    succeed(&["nix", "path-info", p]);
    succeed(&["test", "-e", p]);
}

#[test]
fn build_over_grpc() {
    let p = hello_path();
    let built = succeed(&["cat", p]);
    assert!(built.contains("hello-over-grpc"), "{built}");
}

#[test]
fn build_whose_log_is_not_utf8() {
    let p = succeed(&[
        "nix",
        "build",
        "--store",
        STORE,
        "--impure",
        "-f",
        "/etc/rawlog.nix",
        "--no-link",
        "--print-out-paths",
    ]);
    let built = succeed(&["cat", p.trim()]);
    assert!(built.contains("rawlog-over-grpc"), "{built}");
}

/// `nix log --store grpc://…` reads the log the server's nix-daemon kept,
/// by derivation and by output. It asks that store first, then the
/// substituters (none configured in the VM), never the local store.
#[test]
fn build_log_is_read_from_the_server() {
    write_file(
        "/tmp/logged.nix",
        &drv_expr("logged-grpc", "echo line-from-the-build >&2; echo logged > $out"),
    );
    let out = succeed(&[
        "nix",
        "build",
        "--store",
        STORE,
        "--impure",
        "-f",
        "/tmp/logged.nix",
        "--no-link",
        "--print-out-paths",
    ]);
    let drv = succeed(&["nix-instantiate", "/tmp/logged.nix"]);
    for installable in [drv.trim(), out.trim()] {
        let log = succeed(&["nix", "log", "--store", STORE, installable]);
        assert!(log.contains("line-from-the-build"), "{installable}: {log}");
    }
    let name = drv.trim().rsplit('/').next().unwrap();
    assert_journal("nix-grpc-daemon", &format!("method=GetBuildLog .* drv={name}"));

    write_file("/tmp/unbuilt.nix", &drv_expr("unbuilt-grpc", "echo never > $out"));
    let unbuilt = succeed(&["nix-instantiate", "/tmp/unbuilt.nix"]);
    let err = fail(&["nix", "log", "--store", STORE, unbuilt.trim()]);
    assert!(err.contains("is not available"), "{err}");
}

/// A log that compresses to more than one message arrives whole.
#[test]
fn long_build_log_streams_in_chunks() {
    let drv = succeed(&["nix-instantiate", "/etc/biglog.nix"]);
    let drv = drv.trim();
    succeed(&[
        "nix",
        "build",
        "--store",
        STORE,
        "--impure",
        "-f",
        "/etc/biglog.nix",
        "--no-link",
    ]);
    // The VM's store is the server's: both read the file its nix-daemon wrote.
    succeed(&[
        "sh",
        "-c",
        r#"nix log --store "$1" "$2" > /tmp/biglog.grpc && nix log "$2" > /tmp/biglog.local && cmp /tmp/biglog.grpc /tmp/biglog.local"#,
        "sh",
        STORE,
        drv,
    ]);
    let size: u64 = succeed(&["stat", "-c", "%s", "/tmp/biglog.grpc"])
        .trim()
        .parse()
        .unwrap();
    assert!(size > 4_000_000, "{size}");
    let name = drv.rsplit('/').next().unwrap();
    let lines = journal_matches(
        MACHINE,
        "nix-grpc-daemon",
        &format!("method=GetBuildLog .* drv={name}"),
    );
    println!("[log] {lines:?}");
    let chunks: Vec<u64> = lines
        .iter()
        .filter_map(|l| l.split(" chunks=").nth(1)?.split_whitespace().next()?.parse().ok())
        .collect();
    assert!(!chunks.is_empty() && chunks.iter().all(|&c| c > 1), "{lines:?}");
}

#[test]
fn copy_to_a_local_scratch_store() {
    let p = hello_path();
    let dir = workdir("scratch");
    succeed(&[
        "nix",
        "copy",
        "--no-check-sigs",
        "--from",
        STORE,
        "--to",
        &format!("{dir}/store"),
        p,
    ]);
    let name = p.rsplit('/').next().unwrap();
    succeed(&["test", "-e", &format!("{dir}/store/nix/store/{name}")]);
}

#[test]
fn bulk_upload_is_not_pinned_after_the_rpc_returns() {
    let dir = workdir("bulk");
    succeed(&[
        "dd",
        "if=/dev/urandom",
        &format!("of={dir}/blob"),
        "bs=1M",
        "count=128",
        "status=none",
    ]);
    let up = succeed(&[
        "nix",
        "store",
        "add",
        "--store",
        &format!("{dir}/src"),
        "--mode",
        "flat",
        &format!("{dir}/blob"),
    ]);
    let up = up.trim();
    succeed(&[
        "nix",
        "copy",
        "--no-check-sigs",
        "--from",
        &format!("{dir}/src"),
        "--to",
        STORE,
        up,
    ]);
    succeed(&["test", "-e", up]);
    succeed(&["nix-store", "--delete", up]);
    fail(&["test", "-e", up]);
}

#[test]
fn many_small_paths_round_trip() {
    let dir = workdir("small");
    succeed(&["mkdir", "-p", &format!("{dir}/files")]);
    let files: Vec<String> = (0..200).map(|i| format!("{dir}/files/f{i}")).collect();
    for f in &files {
        random_file(f, 4096);
    }
    let mut add = vec!["nix-store", "--store", "", "--add"];
    let src = format!("{dir}/src");
    add[2] = &src;
    add.extend(files.iter().map(String::as_str));
    let added = succeed(&add);
    let small: Vec<&str> = added.lines().collect();
    assert_eq!(small.len(), 200, "{added}");

    let t0 = Instant::now();
    let mut copy_up = vec![
        "nix",
        "copy",
        "--no-check-sigs",
        "--from",
        &src,
        "--to",
        STORE,
    ];
    copy_up.extend(&small);
    succeed(&copy_up);
    println!(
        "[bench] small/upload   {:6.2}s (200 paths)",
        t0.elapsed().as_secs_f64()
    );
    for p in &small[..3] {
        succeed(&["test", "-e", p]);
    }

    let t0 = Instant::now();
    let dst = format!("{dir}/dst");
    let mut copy_down = vec![
        "nix",
        "copy",
        "--no-check-sigs",
        "--from",
        STORE,
        "--to",
        &dst,
    ];
    copy_down.extend(&small);
    succeed(&copy_down);
    println!(
        "[bench] small/download {:6.2}s (200 paths)",
        t0.elapsed().as_secs_f64()
    );
    for p in &small[..3] {
        let name = p.rsplit('/').next().unwrap();
        succeed(&[
            "cmp",
            &format!("{dir}/src/nix/store/{name}"),
            &format!("{dir}/dst/nix/store/{name}"),
        ]);
    }
}

#[test]
fn add_path_through_envoy() {
    wait_for_unit("envoy.service");
    wait_for_open_port(50060);
    let dir = workdir("envoy");
    random_file(&format!("{dir}/f"), 4096);
    let p = succeed(&[
        "nix",
        "store",
        "add",
        "--store",
        ENVOY_STORE,
        &format!("{dir}/f"),
    ]);
    succeed(&["test", "-e", p.trim()]);
    assert_journal("nix-grpc-daemon", "event=rpc method=Connect");
}
