//! Run by tests/nixos-test.nix, tests/farm.nix and tests/handover.nix on the driver host, one module per call.
mod access;
mod acme;
mod bench;
mod farm;
mod fixtures;
mod handover;
mod harness;
mod latency;
mod lifecycle;
mod transfer;
mod upload;
