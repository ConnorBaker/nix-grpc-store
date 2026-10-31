#!/usr/bin/env bash
# build-client.sh OUTDIR INSTALLABLE...: a client for builds sent through a Tribuchet hub
# whose workers run the CPU arbiter. It applies the same rules to every build; nothing
# about a build changes them.
#
# Why: the CPU arbiter may freeze any build, for at most 10 s of any 30 s (Tribuchet's
# crates/tribuchet/src/worker/arbiter/bound.rs). The worker counts a build's running time
# alone, not the time it waits or is frozen, toward the build's `timeout` and
# `max-silent-time`, which reach it from each `nix build` (the hub's Nix, patched, leaves them
# to `tribuchet attach`). A client that counts wall time spends a freeze.
#
# Between one `nix build` of the chain and the next, a build has no client. The farm's
# path is: this machine's nix-daemon, its build hook's grpc:// store (nix-grpc-store),
# the scheduler on the hub host, that host's nix-daemon, and `tribuchet attach` beside it.
# When a build's last client leaves, the scheduler revokes it, which kills its attach, and
# the hub keeps the build only by its own rule (14 s, then while no build waits for the
# slot, Tribuchet's crates/tribuchet/src/hub/state/clientless.rs), which gives a chain's
# next link no better chance than any client's: under
# demand a build much longer than one link would never finish. So each `nix build` here
# declares that it will come back, under a lease that
# names the chain: nix-grpc-store's settings `grpc-lease = LEASE`, in NIX_CONFIG, and
# `--option grpc-linger LINGER_S` (default 600), which its Want carries (Want.lease_id,
# Want.linger_s). LEASE is a UUID made once per chain. Whoever knows it can end the
# chain's lease, so it is on no command line, where every user's `ps` would show it.
# The scheduler keeps a build while any lease on it is live, as it
# is, queued or running, whatever waits, until LINGER_S after the lease's last client
# left, capped by its --max-linger (600 by default), and the next link follows it again;
# the attach, and the hub's build in its queue or on a worker, never see the gap. Each
# lease counts on its own: a client without one (an interactive `nix build`) neither
# keeps a build nor ends another chain's lease. When the chain ends, however it ends,
# it gives its builds back: it ends its lease on every grpc:// builder (`nix store info
# --store 'URI&release-lease=true'` with the lease in NIX_CONFIG, a Release, which places
# and starts nothing), every builder at once, each asked again within RELEASE_S (default
# 30) of its own until it answers, so a scheduler that is down holds up no other's
# release.
# A link cannot know beforehand that it is the last. Only a trusted user's settings reach
# the build hook. A gap longer
# than the linger falls back to the hub's rule. SIGINT, SIGTERM and SIGHUP stop the chain:
# the running link is ended at once (or as it starts, should the stop come between
# links), and waited for, then the lease is released; a further signal ends the release
# at once, and one that comes before the release (a second Ctrl-C while the link ends)
# skips it: the chain's builds then stay kept until the linger runs out, LINGER_S at
# most. So:
#   - Each `nix build` runs at most CLIENT_S (default and most 600) s (`timeout -k 5
#     CLIENT_S`); one that ends by that bound (124, or 137 if it needed the KILL) is
#     followed at once by the next, which follows the build the scheduler kept for it.
#   - Each passes `--timeout RUNNING_S` (default 600), the longest running time expected of
#     one derivation, and `--max-silent-time SILENT_S` (default 0, none): the worker counts
#     the build's running time toward both. So RUNNING_S is both each derivation's timeout
#     and the chain's budget for one derivation.
#   - Another client follows while the wall time since the first is under QUEUE_S (default
#     600, hub queueing allowed for) + 8/5 RUNNING_S + 17: a build frozen at most 10.5 s of
#     any 30 s (10 s, and the sample and thaw after it) runs RUNNING_S within
#     (RUNNING_S + 10.5) / 0.65 s of wall time.
#   - Options are not passed through: every argument after OUTDIR is an installable, and
#     a NIX_CONFIG that sets max-silent-time or timeout is refused. Never --option
#     builders; --max-jobs 0 (remote only); --keep-going.
#
# Records in OUTDIR: launch.tsv (installable, drv, output, read before the first client),
# clients.jsonl (start, end, exit), client-N.out,
# client-N.log and .log.zst, lease (the chain's), release-S.T.out and release-S.T.log
# (builder S's release attempt T), release.tsv (store, attempt, exit, acknowledged,
# builds), status.json (the last client's own exit, 128 + the signal on a stop, and the
# release: acknowledged by every grpc:// builder or not, and per store its outcome:
# "released" N builds, "nothing held by this scheduler" for an answer of 0, or "no
# answer"; 0 is what that scheduler held for the lease, not that nothing of the chain
# runs: a build a restarted scheduler heard of only from its builder is nobody's, and a
# release cannot reach it), path-info.json (`nix path-info` of each
# launch-time output, after the chain).
#
# build-client.sh --self-test: the checks below against a fake `nix` and clock; no build
# runs. Set TMPDIR to a scratch directory first.
set -u

die() { echo "build-client: $*" >&2; exit 2; }
now_s() { date +%s; }

# The grpc:// stores among the builders (`nix config show builders`): entries split by
# `;` or newlines, an `@FILE` entry read from FILE, `#` starting a comment; the first
# field of an entry is its store URI.
grpc_builders() {
  python3 -I - "$(nix config show builders 2> /dev/null)" <<'PY'
import sys
def entries(spec, depth=0):
    for line in spec.replace(";", "\n").splitlines():
        line = line.split("#", 1)[0].strip()
        if not line:
            continue
        if line.startswith("@"):
            if depth < 2:
                try:
                    with open(line[1:].strip()) as f:
                        yield from entries(f.read(), depth + 1)
                except OSError:
                    pass
            continue
        yield line.split()[0]
seen = []
for uri in entries(sys.argv[1]):
    if uri.startswith("grpc://") and uri not in seen:
        seen.append(uri)
        print(uri)
PY
}

main() {
  [ $# -ge 2 ] || die "usage: build-client.sh OUTDIR INSTALLABLE..."
  local out=$1; shift
  local a
  for a in "$@"; do
    case $a in -*) die "options are not passed through ($a): installables only" ;; esac
  done
  if [[ ${NIX_CONFIG:-} =~ (^|$'\n')[[:space:]]*(max-silent-time|timeout)[[:space:]]*= ]]; then
    die "NIX_CONFIG sets max-silent-time or timeout; set SILENT_S or RUNNING_S instead"
  fi
  local queue=${QUEUE_S:-600} running=${RUNNING_S:-600} silent=${SILENT_S:-0}
  local client=${CLIENT_S:-600} linger=${LINGER_S:-600} release_s=${RELEASE_S:-30}
  [[ $queue =~ ^[0-9]+$ && $running =~ ^[0-9]+$ && $silent =~ ^[0-9]+$ && $client =~ ^[0-9]+$ && $linger =~ ^[0-9]+$ &&
     $release_s =~ ^[0-9]+$ ]] ||
    die "QUEUE_S, RUNNING_S, SILENT_S, CLIENT_S, LINGER_S and RELEASE_S are seconds"
  if [ "$client" -lt 1 ] || [ "$client" -gt 600 ]; then
    die "CLIENT_S=$client: a client runs 1 to 600 s"
  fi
  if [ "$release_s" -lt 1 ] || [ "$release_s" -gt 600 ]; then
    die "RELEASE_S=$release_s: a release runs 1 to 600 s"
  fi
  # OUTDIR holds the chain's lease id (lease, status.json, the release logs), and
  # whoever knows it can end the lease: what the script writes is the caller's alone.
  umask 077
  mkdir -p "$out" || die "cannot create $out"
  : > "$out/launch.tsv"
  local i drv outputs o
  for i in "$@"; do
    drv=$(nix path-info --derivation "$i" 2> "$out/eval.err") || die "cannot evaluate $i (see $out/eval.err)"
    outputs=$(nix derivation show "$drv" 2>> "$out/eval.err" | python3 -I -c 'import json, sys
d = json.load(sys.stdin); d = d.get("derivations", d)
for v in d.values():
    for o in v["outputs"].values():
        p = o.get("path", "")
        print(p if p.startswith("/nix/store/") else "/nix/store/" + p)')
    for o in $outputs; do printf '%s\t%s\t%s\n' "$i" "$drv" "$o" >> "$out/launch.tsv"; done
  done
  local drvs; mapfile -t drvs < <(cut -f2 "$out/launch.tsv" | sort -u)
  local args=(); for drv in "${drvs[@]}"; do args+=("$drv^*"); done
  # The chain's name for the scheduler: every link's Want carries it, and the release
  # ends it alone. Whoever knows it can end the lease, so it goes in the environment
  # (NIX_CONFIG, which reaches the plugin's grpc-lease as --option would), never on a
  # command line, where every user's `ps` shows it.
  local lease leased nl=$'\n'
  lease=$(python3 -I -c 'import uuid; print(uuid.uuid4())') || die "cannot make a lease id"
  printf '%s\n' "$lease" > "$out/lease"
  leased="${NIX_CONFIG:+$NIX_CONFIG$nl}grpc-lease = $lease"
  : > "$out/clients.jsonl"
  local t0 rc start end n child='' stop='' cut='' phase=links releasing=() p
  # The first signal stops the chain: it ends the running link at once (the link runs
  # in the background so that the trap is not held until it ends), and one not yet
  # started as soon as it starts. Any signal after it, or during the release, ends the
  # release.
  signalled() {
    if [ "$phase" = release ]; then
      cut=1
      for p in "${releasing[@]}"; do kill -TERM "$p" 2> /dev/null; done
    elif [ -n "$stop" ]; then
      cut=1
    else
      stop=$1
      [ -z "$child" ] || kill -TERM "$child" 2> /dev/null
    fi
  }
  trap 'signalled 130' INT
  trap 'signalled 143' TERM
  trap 'signalled 129' HUP
  t0=$(now_s)
  for ((n = 1; ; n++)); do
    start=$(now_s)
    # Its records exist even if a stop ends it before it opens them.
    : > "$out/client-$n.out"; : > "$out/client-$n.log"
    NIX_CONFIG=$leased timeout -k 5 "$client" nix build --max-jobs 0 --keep-going --max-silent-time "$silent" \
      --timeout "$running" --option grpc-linger "$linger" --no-link --print-out-paths -L "${args[@]}" \
      > "$out/client-$n.out" 2> "$out/client-$n.log" &
    child=$!
    # A stop that came before $child was known (`wait` would wait out the whole
    # link).
    [ -z "$stop" ] || kill -TERM "$child" 2> /dev/null
    wait "$child"; rc=$?
    if [ -n "$stop" ]; then
      # The trap, or the check above, sent it SIGTERM. Until it is gone (timeout's -k
      # bounds it): a further signal returns `wait` early.
      while kill -0 "$child" 2> /dev/null; do wait "$child"; done
      rc=$stop
    fi
    child=''
    end=$(now_s)
    zstd -q -f "$out/client-$n.log" -o "$out/client-$n.log.zst"
    printf '{"client":%d,"start":%s,"end":%s,"exit":%d}\n' "$n" "$start" "$end" "$rc" >> "$out/clients.jsonl"
    [ -z "$stop" ] || break
    [[ $rc -eq 124 || $rc -eq 137 ]] || break
    [ $(( end - t0 )) -lt $(( queue + running * 8 / 5 + 17 )) ] || break
  done
  # The chain is over, however it ended: end its lease on every grpc:// builder, so that
  # the scheduler lets go of the builds it kept for the chain alone. Every builder at
  # once, each asked again within RELEASE_S of its own until it answers: a scheduler
  # that does not answer holds up no other's release (in turn under one deadline, a
  # dead first one would use it all).
  # One builder's release, in the background. Wall time (bash's SECONDS): the release
  # is the chain's, not a build's, and a freeze cannot hold it.
  release_one() {
    local s=$1 store=$2 uri deadline left tries acked rrc builds attempt=''
    trap '[ -z "$attempt" ] || kill -TERM "$attempt" 2> /dev/null; exit 143' TERM
    if [[ $store == *\?* ]]; then uri="$store&release-lease=true"; else uri="$store?release-lease=true"; fi
    deadline=$(( SECONDS + release_s ))
    for ((tries = 1; ; tries++)); do
      left=$(( deadline - SECONDS ))
      [ "$left" -gt 0 ] || break
      NIX_CONFIG=$leased timeout -k 2 "$left" nix store info --store "$uri" \
        > "$out/release-$s.$tries.out" 2> "$out/release-$s.$tries.log" &
      attempt=$!
      wait "$attempt"; rrc=$?
      attempt=''
      # nix-grpc-store's own report: nothing else is an answer (an older plugin ignores
      # release-lease and still succeeds). It names the lease by its first 8 characters
      # (by all of it in older nix-grpc-store revisions).
      builds=$(sed -n "s/^released lease \(${lease:0:8}\|$lease\) on .*: \([0-9][0-9]*\) builds\$/\2/p" \
        "$out/release-$s.$tries.log" | tail -n 1)
      acked=0
      if [ "$rrc" -eq 0 ] && [ -n "$builds" ]; then acked=1; fi
      printf '%s\t%d\t%d\t%d\t%s\n' "$store" "$tries" "$rrc" "$acked" "$builds" >> "$out/release.tsv"
      [ "$acked" -eq 0 ] || break
      sleep 1 & wait $!
    done
  }
  phase=release
  : > "$out/release.tsv"
  local stores=() s
  mapfile -t stores < <(grpc_builders)
  for ((s = 0; s < ${#stores[@]}; s++)); do
    [ -z "$cut" ] || break
    release_one "$s" "${stores[s]}" &
    releasing+=("$!")
  done
  # A further signal before the releases were known ends them now.
  if [ -n "$cut" ]; then
    for p in "${releasing[@]}"; do kill -TERM "$p" 2> /dev/null; done
  fi
  for p in "${releasing[@]}"; do
    while kill -0 "$p" 2> /dev/null; do wait "$p"; done
  done
  trap - INT TERM HUP
  local release
  release=$(python3 -I - "$out/release.tsv" "$lease" "${#stores[@]}" "$cut" <<'PY'
import json, sys
path, lease, n, cut = sys.argv[1], sys.argv[2], int(sys.argv[3]), sys.argv[4]
rows = {}
for line in open(path):
    store, tries, rc, acked, builds = line.rstrip("\n").split("\t")
    # An answer of 0 builds says that scheduler held nothing for the lease; not that
    # nothing runs for it (a build a restarted scheduler only heard of from its
    # builder is nobody's, and a release cannot reach it).
    held = int(builds) if builds else None
    rows[store] = {"store": store, "attempts": int(tries), "exit": int(rc), "acknowledged": acked == "1",
                   "builds": held,
                   "outcome": "no answer" if acked != "1" else "released" if held else "nothing held by this scheduler"}
stores = list(rows.values())
print(json.dumps({"lease": lease,
                  "acknowledged": n > 0 and len(stores) == n and all(s["acknowledged"] for s in stores),
                  "interrupted": cut == "1", "stores": stores}, separators=(",", ":")))
PY
)
  printf '{"exit":%d,"clients":%d,"queue_s":%d,"running_s":%d,"silent_s":%d,"client_s":%d,"linger_s":%d,"release":%s,"ended":"%s"}\n' \
    "$rc" "$n" "$queue" "$running" "$silent" "$client" "$linger" "${release:-null}" "$(date -u +%FT%TZ)" > "$out/status.json"
  {
    printf '['
    local first=1 pirc
    while IFS=$'\t' read -r _ _ o; do
      nix path-info "$o" > /dev/null 2>&1; pirc=$?
      [[ $first -eq 1 ]] || printf ','
      first=0
      printf '{"out":"%s","path_info_exit":%d,"checked":"%s"}' "$o" "$pirc" "$(date -u +%FT%TZ)"
    done < "$out/launch.tsv"
    printf ']\n'
  } > "$out/path-info.json"
  return "$rc"
}

# The fake `nix`. `build` records its arguments, its NIX_CONFIG in $FAKE/config.<call>
# and the grpc-lease set there in $FAKE/leases, prints $FAKE/log.<call> to stderr if it
# exists, sleeps $FAKE/block.<call> seconds if that exists (a link still running; with
# $FAKE/stubborn, living on through SIGTERM, which it notes in $FAKE/termed, so only the
# KILL ends it early), moves the clock
# 600 s on (a client that ran out its bound) and exits with the next code in
# $FAKE/exits. `store info` records its arguments and its NIX_CONFIG's grpc-lease
# (`… lease=ID`), its whole NIX_CONFIG in $FAKE/release-config.<release>, and when it
# began in $FAKE/release-times; a store on a host named
# `dead` never answers; otherwise it sleeps
# $FAKE/rblock.<call> seconds if that exists and answers as the next line of
# $FAKE/answers says, its last line once they run out: `ok` (exit 0 and nix-grpc-store's
# report of 1 build, naming the lease by its first 8 characters, the default), `full`
# (the same, naming all of it, as older revisions do), `none` (0 builds), `silent` (exit 0
# and no report: an older plugin) or an exit code. `config show builders` prints
# $FAKE/builders, by default one grpc:// store. The other subcommands answer what main
# reads.
fake_nix() {
  # This bash, not /usr/bin/env's: a Nix build sandbox has no /usr/bin/env.
  printf '#!%s\n' "$BASH" > "$1/nix"
  cat >> "$1/nix" <<'FAKE'
case "$1 $2" in
  "path-info --derivation") echo "/nix/store/aaaa-x.drv" ;;
  "derivation show") echo '{"/nix/store/aaaa-x.drv":{"outputs":{"out":{"path":"/nix/store/bbbb-x"}}}}' ;;
  "path-info "*) exit 0 ;;
  "config show") if [ -f "$FAKE/builders" ]; then cat "$FAKE/builders"; else echo "grpc://fake:1?insecure=1"; fi ;;
  "store info")
    lease=$(printf '%s\n' "${NIX_CONFIG:-}" | sed -n 's/^grpc-lease = //p' | tail -n 1)
    printf '%s lease=%s\n' "$*" "$lease" >> "$FAKE/releases"
    printf '%s %s\n' "$(date +%s.%N)" "$4" >> "$FAKE/release-times"
    r=$(wc -l < "$FAKE/releases")
    printf '%s\n' "${NIX_CONFIG:-}" > "$FAKE/release-config.$r"
    case $4 in *//dead[:/?]*) exec sleep 60 ;; esac
    [ -f "$FAKE/rblock.$r" ] && sleep "$(cat "$FAKE/rblock.$r")"
    answer=$(sed -n "${r}p" "$FAKE/answers" 2> /dev/null)
    [ -n "$answer" ] || answer=$(tail -n 1 "$FAKE/answers" 2> /dev/null)
    case ${answer:-ok} in
      ok) echo "released lease ${lease:0:8} on fake:1: 1 builds" >&2 ;;
      none) echo "released lease ${lease:0:8} on fake:1: 0 builds" >&2 ;;
      full) echo "released lease $lease on fake:1: 1 builds" >&2 ;;
      silent) ;;
      *) echo "error: no answer" >&2; exit "$answer" ;;
    esac ;;
  "build "*)
    printf '%s\n' "$*" >> "$FAKE/calls"
    k=$(wc -l < "$FAKE/calls")
    printf '%s\n' "${NIX_CONFIG:-}" > "$FAKE/config.$k"
    printf '%s\n' "$(sed -n 's/^grpc-lease = //p' "$FAKE/config.$k" | tail -n 1)" >> "$FAKE/leases"
    [ -f "$FAKE/log.$k" ] && cat "$FAKE/log.$k" >&2
    if [ -f "$FAKE/block.$k" ]; then
      if [ -f "$FAKE/stubborn" ]; then
        # Lives on through SIGTERM, noting it in $FAKE/termed; link.pid says it does.
        # timeout signals its whole process group: the sleep, started while SIGTERM is
        # ignored, ignores it too.
        trap '' TERM
        sleep "$(cat "$FAKE/block.$k")" &
        trap 'echo 1 > "$FAKE/termed"' TERM
        echo $$ > "$FAKE/link.pid"
        while kill -0 "$!" 2> /dev/null; do wait "$!"; done
      else
        echo $$ > "$FAKE/link.pid"
        sleep "$(cat "$FAKE/block.$k")"
      fi
    fi
    echo $(( $(cat "$FAKE/clock") + 600 )) > "$FAKE/clock"
    code=$(head -1 "$FAKE/exits"); sed -i 1d "$FAKE/exits"; exit "${code:-0}" ;;
esac
FAKE
  chmod +x "$1/nix"
}

self_test() {
  local root="${TMPDIR:?set TMPDIR to a scratch directory}"
  local T; T=$(mktemp -d "$root/build-client-test.XXXXXX")
  fake_nix "$T"
  # The fake clock. With $FAKE/pause.<call>, that call of it first writes $FAKE/paused
  # and sleeps that many seconds: main holds there, between one link and the next.
  now_s() {
    local c=1
    [ -f "$FAKE/nows" ] && c=$(( $(cat "$FAKE/nows") + 1 ))
    echo "$c" > "$FAKE/nows"
    if [ -f "$FAKE/pause.$c" ]; then echo 1 > "$FAKE/paused"; sleep "$(cat "$FAKE/pause.$c")"; fi
    cat "$FAKE/clock"
  }
  local fails=0 k=0 RC CALLS RELEASES ELAPSED
  # Files for the next run (`next NAME CONTENT`): log.N, block.N, answers, builders, ...
  next() { mkdir -p "$T/next"; printf '%s\n' "$2" > "$T/next/$1"; }
  setup() {
    k=$((k + 1)); mkdir -p "$T/run-$k"
    [ -d "$T/next" ] && mv "$T/next"/* "$T/run-$k"/ 2> /dev/null
    rmdir "$T/next" 2> /dev/null
    : > "$T/run-$k/calls"; : > "$T/run-$k/leases"; : > "$T/run-$k/releases"; echo 0 > "$T/run-$k/clock"
    [ -f "$T/run-$k/exits" ] || : > "$T/run-$k/exits"
  }
  # run EXITS... -- ARGS...: main with the fake nix and clock, in a directory of its own,
  # for at most 60 s (RC 124 past that).
  run() {
    setup
    while [ "$1" != -- ]; do echo "$1" >> "$T/run-$k/exits"; shift; done; shift
    local t; t=$(date +%s)
    (PATH="$T:$PATH" FAKE="$T/run-$k" timeout 60 bash -c "set -u; umask 022; $(declare -f now_s die grpc_builders main); main \"\$@\"" main "$T/run-$k/out" "$@") > "$T/run-$k/stdout" 2>&1; RC=$?
    ELAPSED=$(( $(date +%s) - t ))
    counts
  }
  counts() {
    CALLS=$(wc -l < "$T/run-$k/calls")
    RELEASES=$(wc -l < "$T/run-$k/releases")
  }
  # stopped SIGNALS WHEN ARGS...: main as run does, its links exiting as $FAKE/exits says
  # (0 by default), sent each of SIGNALS, half a second apart, once its first link (WHEN
  # link), its first release (WHEN release) or a pause of its clock (WHEN pause) started;
  # RC -1 if it is not over within 20 s.
  stopped() {
    setup
    [ -s "$T/run-$k/exits" ] || echo 0 > "$T/run-$k/exits"
    local sigs=$1 when=$2 pid i s t; shift 2
    local watch="$T/run-$k/calls"; [ "$when" = release ] && watch="$T/run-$k/releases"
    [ "$when" = pause ] && watch="$T/run-$k/paused"
    # A stubborn link takes the first signal only once it is stubborn.
    [ "$when" = link ] && [ -f "$T/run-$k/stubborn" ] && watch="$T/run-$k/link.pid"
    t=$(date +%s)
    # Job control, so that the job does not ignore SIGINT as a background job would.
    set -m
    (PATH="$T:$PATH" FAKE="$T/run-$k" main "$T/run-$k/out" "$@") > "$T/run-$k/stdout" 2>&1 &
    pid=$!
    set +m
    for ((i = 0; i < 100; i++)); do [ -s "$watch" ] && break; sleep 0.1; done
    for s in $sigs; do
      kill -"$s" "$pid"
      # Against a stubborn link, the next signal only once the script has acted on this
      # one (the link notes the SIGTERM it was sent): two signals that reach bash before
      # it runs the trap of the first are one.
      if [ -f "$T/run-$k/stubborn" ]; then
        for ((i = 0; i < 100; i++)); do [ -s "$T/run-$k/termed" ] && break; sleep 0.1; done
      fi
      sleep 0.5
    done
    for ((i = 0; i < 200; i++)); do kill -0 "$pid" 2> /dev/null || break; sleep 0.1; done
    if kill -0 "$pid" 2> /dev/null; then RC=-1; kill -KILL "$pid"; wait "$pid"; else wait "$pid"; RC=$?; fi
    ELAPSED=$(( $(date +%s) - t ))
    counts
  }
  status() { python3 -I -c 'import json, sys; s = json.load(open(sys.argv[1])); print(eval(sys.argv[2], {"s": s}))' "$T/run-$k/out/status.json" "$1" 2> /dev/null; }
  # check NAME COMMAND...: the check passes if COMMAND does.
  check() { if "${@:2}"; then echo "ok   $1"; else echo "FAIL $1 (rc=$RC calls=$CALLS releases=$RELEASES, records in $T/run-$k)"; fails=$((fails + 1)); fi; }
  # ended RC CALLS: the last run ended with RC after CALLS builds.
  ended() { [ "$RC" -eq "$1" ] && [ "$CALLS" -eq "$2" ]; }
  limits_passed() {
    grep -q -- "--max-silent-time 0 --timeout 600 " "$T/run-$k/calls" &&
      grep -q -- "--max-jobs 0 --keep-going" "$T/run-$k/calls" && ! grep -q builders "$T/run-$k/calls"
  }
  # Every link carries the chain's one lease in its NIX_CONFIG, and no argument names it.
  one_lease() {
    local lease; lease=$(cat "$T/run-$k/out/lease")
    [[ $lease =~ ^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$ ]] &&
      [ "$(grep -cxF -- "$lease" "$T/run-$k/leases")" -eq "$CALLS" ] && ! grep -qF -- "$lease" "$T/run-$k/calls"
  }
  # released STORES: one release per store, in any order, each with release-lease=true
  # and the chain's lease in its NIX_CONFIG, and the status says every one was
  # acknowledged.
  released() {
    local lease; lease=$(cat "$T/run-$k/out/lease")
    [ "$(cut -d' ' -f4- "$T/run-$k/releases" | sort)" = "$(for s in "$@"; do
      if [[ $s == *\?* ]]; then echo "$s&release-lease=true lease=$lease"; else echo "$s?release-lease=true lease=$lease"; fi
      done | sort)" ] &&
      [ "$(status 's["release"]["acknowledged"]')" = True ]
  }
  link_gone() { ! kill -0 "$(cat "$T/run-$k/link.pid")" 2> /dev/null; }
  # asked_within S: 1 if every store was first asked within S seconds of the first.
  asked_within() {
    python3 -I - "$T/run-$k/release-times" "$1" <<'PY2'
import sys
first = {}
for line in open(sys.argv[1]):
    at, store = line.split()
    first.setdefault(store.split("?")[0], float(at))
print(int(max(first.values()) - min(first.values()) <= float(sys.argv[2])))
PY2
  }

  # Kills "options-pass-through": a caller's own limit reaching nix.
  run 0 -- --max-silent-time 600 x
  check "an option among the installables is refused before any build" ended 2 0
  # Kills "nix-config-unchecked": a limit smuggled in through NIX_CONFIG.
  NIX_CONFIG=$'substituters = \ntimeout = 1800' run 0 -- x
  check "a NIX_CONFIG timeout is refused" ended 2 0
  # Kills "no-explicit-limits": nix.conf's limits would apply.
  run 0 -- x
  check "every build passes --max-silent-time 0 --timeout 600, remote only, no builders" limits_passed
  # Kills "no-linger": a link that does not say it will come back, whose build the
  # scheduler revokes at every gap between links.
  run 0 -- x
  check "every build declares its linger, 600 s by default" grep -q -- "--option grpc-linger 600 " "$T/run-$k/calls"
  LINGER_S=30 run 0 -- x
  check "LINGER_S sets the linger declared" grep -q -- "--option grpc-linger 30 " "$T/run-$k/calls"
  # Kills "no-lease" (links without one: nothing to release, and an id-less linger),
  # "lease-per-link" (each link its own: a lease the next link does not renew) and
  # "lease-on-argv" (an id every user's `ps` shows).
  QUEUE_S=0 RUNNING_S=3000 run 124 124 0 -- x
  check "every link carries the chain's one lease, a UUID recorded in OUTDIR, in NIX_CONFIG, not its arguments" one_lease
  # Kills "config-replaced": the caller's own NIX_CONFIG dropped for the lease.
  NIX_CONFIG='substituters = ' run 0 -- x
  check "the caller's NIX_CONFIG is kept beside the lease" grep -qx 'substituters = ' "$T/run-$k/config.1"
  # Kills "release-drops-caller-config": the release with the lease alone, without what
  # the caller's NIX_CONFIG may need to reach the scheduler.
  check "and so it is in the release's" \
    test "$(grep -cx 'substituters = ' "$T/run-$k/release-config.1")$(grep -cx "grpc-lease = $(cat "$T/run-$k/out/lease")" "$T/run-$k/release-config.1")" = 11
  # Kills "no-umask": records holding the lease id that other users can read. The
  # run's caller has umask 022.
  check "the chain's records, which hold its lease, are the caller's alone" \
    test "$(cd "$T/run-$k/out" && stat -c %a lease status.json release-0.1.log client-1.log | sort -u)" = 600
  # Kills "no-release": a chain that ends leaves its builds kept for 600 s.
  run 0 -- x
  check "a chain that ends on its own releases its lease" released "grpc://fake:1?insecure=1"
  QUEUE_S=0 RUNNING_S=600 run 124 124 0 -- x
  check "and so does one stopped on its budget" released "grpc://fake:1?insecure=1"
  next builders $'@'"$T/machines"
  printf '%s\n' "grpc://a:1?insecure=1 x86_64-linux - 4" "ssh-ng://b x86_64-linux" "# grpc://commented" > "$T/machines"
  printf '%s\n' "grpc://c:1 x86_64-linux;grpc://a:1?insecure=1" >> "$T/machines"
  run 0 -- x
  check "it releases on every grpc:// builder once, on no other" released "grpc://a:1?insecure=1" "grpc://c:1"
  # Kills "ack-by-exit": an older plugin ignores release-lease and still exits 0.
  next answers silent
  RELEASE_S=2 run 0 -- x
  check "a store info that exits 0 without nix-grpc-store's report is no answer" \
    test "$(cut -f4 "$T/run-$k/out/release.tsv" | head -1)$(status 's["release"]["acknowledged"]')" = 0False
  # Kills "no-retry": an unanswered release leaves the builds kept for 600 s.
  next answers $'1\nok'
  run 0 -- x
  check "an unanswered release is asked again until answered" \
    test "$RELEASES$(status 's["release"]["acknowledged"]')" = 2True
  # Kills "retry-forever": a scheduler that never answers holds the chain's end.
  next answers 1
  RELEASE_S=3 run 0 -- x
  check "a release never answered is given up within RELEASE_S" \
    test "$RC$(status 's["release"]["acknowledged"]')$((ELAPSED <= 6))" = 0False1
  # Kills "in-turn-shared" (schedulers asked in turn under one RELEASE_S: the first, down,
  # uses it all and the others are never asked) and "in-turn" (in
  # turn, each its own RELEASE_S: the last is asked only once both others gave up).
  next builders "grpc://dead:1;grpc://dead:2;grpc://alive:1"
  RELEASE_S=3 run 0 -- x
  check "schedulers that do not answer hold up no other's release" \
    test "$(grep -c '//alive:1' "$T/run-$k/releases")$(status 'sorted((x["store"], x["acknowledged"]) for x in s["release"]["stores"])')$(asked_within 2)" \
    = "1[('grpc://alive:1', True), ('grpc://dead:1', False), ('grpc://dead:2', False)]1"
  # The control: an answering scheduler listed first is released either way.
  next builders "grpc://alive:1;grpc://dead:1"
  RELEASE_S=3 run 0 -- x
  check "a scheduler that answers is released whatever comes after it" \
    test "$(grep -c '//alive:1' "$T/run-$k/releases")$(status 'sorted((x["store"], x["acknowledged"]) for x in s["release"]["stores"])')" \
    = "1[('grpc://alive:1', True), ('grpc://dead:1', False)]"
  # Kills "zero-is-released": an answer of 0 builds recorded as a release of the chain's
  # builds, while a build a restarted scheduler lost track of may still hold its slot.
  next answers none
  run 0 -- x
  check "an answer of 0 builds is recorded as nothing held by that scheduler" \
    test "$(status '[x["outcome"] for x in s["release"]["stores"]]')" = "['nothing held by this scheduler']"
  run 0 -- x
  check "an answer of builds is recorded as released" \
    test "$(status '[(x["outcome"], x["builds"]) for x in s["release"]["stores"]]')" = "[('released', 1)]"
  # Kills "prefix-only": the report of an older plugin, which names all of the
  # lease, taken for no answer.
  next answers full
  run 0 -- x
  check "a report naming all of the lease is an answer too" \
    test "$(status '[(x["outcome"], x["builds"]) for x in s["release"]["stores"]]')" = "[('released', 1)]"
  # Kills "no-trap": a stop that kills the script leaves the running link's build kept,
  # and no record. Kills "foreground-link": a trap held until the link ends. Kills
  # "trap-no-kill": a trap that does not end the running link, which then runs its course.
  next block.1 30
  stopped TERM link x
  check "SIGTERM ends the running link at once and releases" \
    test "$((ELAPSED <= 5))$RC$(status 's["release"]["acknowledged"]')" = 1143True
  check "a stop is recorded as 128 + the signal" ended 143 1
  next block.1 30
  stopped INT link x
  check "so does SIGINT" test "$RC$(status 's["release"]["acknowledged"]')" = 130True
  # Kills "no-stop-check": a stop that comes between one link's end and the next one's
  # start, before the script knows the next link to end, lets that link run its whole
  # course. The first link ends 124, so a second follows; the
  # clock's 4th reading, the second link's start, holds 3 s, and the stop comes then.
  next exits 124; next pause.4 3; next block.2 30
  stopped TERM pause x
  check "a stop between links ends the next link as it starts, and releases" \
    test "$((ELAPSED <= 8))$RC$(status 's["clients"]')$(status 's["release"]["acknowledged"]')" = 11432True
  # Kills "wait-once": a second signal returns `wait` early, and the script ends while
  # its link still runs.
  next block.1 30; next stubborn 1
  stopped "TERM TERM" link x
  check "a second signal neither leaves the stopped link running nor waits for a release" \
    test "$(link_gone && echo gone)$RELEASES$(status 's["release"]["interrupted"]')" = gone0True
  # Kills "release-foreground": a release a signal cannot cut short (its own process
  # group, the trap held until it ends).
  next rblock.1 30
  stopped TERM release x
  check "a signal during the release ends it at once" \
    test "$((ELAPSED <= 4))$RC$(status 's["release"]["interrupted"]')$(status 's["release"]["acknowledged"]')" = 10TrueFalse
  # Kills "client-bound-unchecked": a client longer than the farm's 600 s.
  CLIENT_S=601 run 0 -- x
  check "a client bound over 600 s is refused" ended 2 0
  RELEASE_S=601 run 0 -- x
  check "a release bound over 600 s is refused" ended 2 0
  # Kills "kill-ends-chain": a client that needed the KILL leaves the build.
  QUEUE_S=0 RUNNING_S=3000 run 124 137 0 124 -- x
  check "clients ending 124 or 137 are followed; one ending 0 is the last" ended 0 3
  # Kills "no-stretch": the chain's budget as QUEUE_S + RUNNING_S of wall time (1000 s),
  # which a build frozen a third of the time would run out: 400 + 960 + 17 = 1377 s, and
  # each link takes 600.
  QUEUE_S=400 RUNNING_S=600 run 124 124 124 124 124 124 -- x
  check "the chain allows a build its running time stretched by its freezes" ended 124 3

  echo "self-test: $fails failed (records in $T)"
  [ "$fails" -eq 0 ]
}

case ${1:-} in
  --self-test) self_test ;;
  *) main "$@" ;;
esac
