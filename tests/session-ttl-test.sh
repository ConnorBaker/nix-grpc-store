#!/bin/sh
# --session-ttl takes a positive number of seconds: 0 would end each session
# at the next tick, between its calls. Each case ends in a flag that does
# not exist, which a valid one gets past to, so that no case starts a daemon.
set -eu
daemon=$1
check() {
  want=$1; shift
  if out=$("$daemon" "$@" 2>&1); then
    echo "nix-grpc-daemon $* started" >&2; exit 1
  fi
  # Nix colours parts of its messages.
  out=$(printf '%s' "$out" | sed 's/\x1b\[[0-9;]*m//g')
  case $out in
    *"$want"*) ;;
    *) echo "nix-grpc-daemon $*: expected '$want', got: $out" >&2; exit 1 ;;
  esac
}
check "--session-ttl expects a positive number of seconds" --session-ttl 0 --no-such-flag
check "--session-ttl expects a positive number of seconds" --session-ttl x --no-such-flag
check "unknown flag '--no-such-flag'" --session-ttl 1 --no-such-flag
