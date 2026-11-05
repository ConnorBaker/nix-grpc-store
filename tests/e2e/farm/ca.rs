//! Floating content-addressed derivations, and a dynamic one, through the
//! build hook: the client's nix-daemon resolves them and hands the farm one
//! at a time, and registers the realisations of what the farm built.

use super::helpers::*;
use crate::harness::*;

/// `nix build` of CA's `attr` for `tag` through the build hook, as argv
/// after `prefix`. The builder offers `ca-derivations`, which nix requires
/// of a floating content-addressed derivation.
fn ca_build(prefix: &[&str], tag: &str, attr: &str) -> Vec<String> {
    let sys = system();
    let builders = format!("{}&system={sys} {sys} - 4 1 ca-derivations", envoy());
    let ca = expr("CA");
    let build = [
        "nix", "build", "-L", "--print-out-paths", "--no-link", "--max-jobs", "0", "--builders", &builders, "-f",
        &ca, attr, "--argstr", "tag", tag,
    ];
    prefix.iter().chain(&build).map(|arg| arg.to_string()).collect()
}

/// The output of `ca_build` through the client's nix-daemon.
fn build_ca(tag: &str, attr: &str) -> String {
    let built = run_t(CLIENT, &strs(&ca_build(&["env", "NIX_REMOTE=daemon"], tag, attr)), 300);
    assert_eq!(built.code, 0, "building {attr} failed: {}", built.combined());
    built.stdout.trim().to_string()
}

/// The output path the client's store has a realisation for, of CA's `attr`
/// for `tag`. Nothing is built for this: a missing one fails.
fn realised(store: &str, tag: &str, attr: &str) -> String {
    let ca = expr("CA");
    let info = run_t(
        CLIENT,
        &[
            "nix",
            "store",
            "build-trace",
            "info",
            "--store",
            store,
            "-f",
            &ca,
            attr,
            "--argstr",
            "tag",
            tag,
        ],
        60,
    );
    assert_eq!(
        info.code,
        0,
        "no realisation of {attr}: {}",
        info.combined()
    );
    info.stdout
        .split_whitespace()
        .last()
        .unwrap_or_default()
        .to_string()
}

fn content(store: &str, path: &str) -> String {
    succeed(CLIENT, &["nix", "store", "cat", "--store", store, path])
        .trim()
        .to_string()
}

/// The client resolves `b`, on the floating `a`, and the farm builds both.
#[test]
fn a_floating_derivation_on_another_is_resolved_by_the_client() {
    farm_ready();
    // Unchanged: the farm's own planner needs output paths before the build.
    let planned = run_t(
        CLIENT,
        &[
            "nix",
            "build",
            "--store",
            &envoy(),
            "--eval-store",
            "auto",
            "-f",
            &expr("CA"),
            "a",
            "--argstr",
            "tag",
            "planned",
        ],
        120,
    );
    assert_ne!(planned.code, 0, "{}", planned.combined());
    assert!(
        planned
            .combined()
            .contains("needs statically known input and output paths"),
        "{}",
        planned.combined()
    );

    let out = build_ca("fb", "b");
    assert_eq!(content("auto", &out), "b-a-fb");
    assert_eq!(realised("auto", "fb", "b"), out);
    assert_eq!(content("auto", &realised("auto", "fb", "a")), "a-fb");
}

#[test]
fn a_dynamic_derivation_is_built_on_the_farm() {
    farm_ready();
    let out = build_ca("fc", "dynamic");
    assert_eq!(content("auto", &out), "dynamic-inner-fc");
    assert_eq!(realised("auto", "fc", "dynamic"), out);
    // The farm built the producer, a derivation byte for byte, and then
    // that floating derivation.
    let inner = succeed(
        CLIENT,
        &[
            "nix",
            "eval",
            "--raw",
            "-f",
            &expr("CA"),
            "inner.drvPath",
            "--argstr",
            "tag",
            "fc",
        ],
    );
    assert_eq!(realised("auto", "fc", "producer"), inner);
    assert_eq!(
        content("auto", &realised("auto", "fc", "inner")),
        "inner-fc"
    );
}

/// Two nix stores, so two build hooks ask the farm for the same floating
/// derivation at once: one store's nix-daemon would build it once itself.
/// Through NIX_REMOTE: a `--store` nix-daemon takes from its client reaches
/// its build hook too, which then registers the realisations through the
/// daemon, which requires them signed.
#[test]
fn a_second_client_attaches_to_a_floating_build() {
    farm_ready();
    let attached = || {
        WORKERS
            .into_iter()
            .map(|w| events(w, "attached"))
            .sum::<i64>()
    };
    let before = attached();
    let other = "/tmp/ca-other-store";
    for (unit, store) in [("ca-share1", "daemon"), ("ca-share2", other)] {
        let remote = format!("NIX_REMOTE={store}");
        let spawn = ["systemd-run", "--unit", unit, "--setenv", &remote];
        succeed(CLIENT, &strs(&ca_build(&spawn, "share", "slow")));
        if unit == "ca-share1" {
            wait_building();
        }
    }
    retry(120, "the second client attached", || {
        attached() - before == 1
    });
    release_slow();
    wait_unit_done("ca-share1 ca-share2", 120);
    for unit in ["ca-share1", "ca-share2"] {
        assert_unit_success(unit);
    }
    assert_eq!(attached() - before, 1);
    for store in ["auto", other] {
        assert_eq!(
            content(store, &realised(store, "share", "slow")),
            "slow-share"
        );
    }
}

/// nix_grpc_build_info shows the features a builder offers, ca-derivations
/// among them here.
#[test]
fn build_info_shows_the_features_a_builder_offers() {
    farm_ready();
    for w in WORKERS {
        let info = build_info(w);
        let ca = features_label(&info).is_some_and(|f| f.split(',').any(|f| f == "ca-derivations"));
        assert!(ca, "{info}");
    }
}
