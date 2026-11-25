# Python for a NixOS testScript: waits without a deadline. The driver's own
# waits (wait_for_unit, wait_until_succeeds, wait_for_open_port, retry) and
# its execute all take one, 15 minutes unless given another. An event that
# never comes hangs the wait here; that hang is the failure to diagnose. A
# client the wait names that ends first fails it, with its journal.
#
# The whole test keeps one bound, its globalTimeout, so that a hang ends and
# frees the machine it holds. The driver's own timer counts wall time, which
# a build the farm's CPU arbiter freezes spends without running. Here a
# clock ticks every 0.05 s, and a tick that comes more than 0.1 s after the
# last counts 0.1 s: each freeze (or other stop) adds at most 0.1 s to the
# count, whatever the arbiter's policy. Time the test is not stopped but
# starved of CPU on a loaded host counts in full; the bounds' margin is for
# that. At the bound the test fails with where its script is. Each test
# sets its globalTimeout to three times its longest passing run on the farm
# (wall time, so no less than its running time), rounded up to 300 s, at
# least 600 s, and never below the value upstream gives it.
''
  import shlex
  import signal
  import sys
  import threading
  import time
  import traceback
  from test_driver.errors import RequestedAssertionFailed

  class RunningBound(threading.Timer):
      # Stands in for the driver's race_timer, so the driver cancels it as it
      # would its own once the test script is over (join_all, cleanup).
      def __init__(self, bound, tick=0.05, most=0.1):
          super().__init__(bound, lambda: None)
          self.tick, self.most = tick, most

      def run(self):
          ran, last = 0.0, time.monotonic()
          while ran < self.interval:
              if self.finished.wait(self.tick):
                  return
              now = time.monotonic()
              ran, last = ran + min(now - last, self.most), now
          # The finally ends the test however the report goes;
          # log_test_error keeps the report itself free of log.error's
          # sys.exit.
          try:
              self.report()
          finally:
              self.end()

      def report(self):
          main = sys._current_frames().get(threading.main_thread().ident)
          code = driver.tests.splitlines()
          at = [f"  {code[f.lineno - 1].strip()}" for f in traceback.extract_stack(main) if f.filename == "<string>" and f.lineno] if main else []
          log.log_test_error(
              f"the test ran {self.interval:.0f} s, its globalTimeout (a freeze counts at most {self.most} s),"
              + " and did not finish; its script is at:\n" + ("\n".join(at) or "  (no frame of the script)")
          )

      def end(self):
          # terminate_test releases the machines and sends the driver SIGTERM;
          # the SIGTERM goes even if a release fails.
          try:
              driver.terminate_test()
          finally:
              signal.raise_signal(signal.SIGTERM)

  # Only where the driver armed its own (run_tests; not interactively).
  if driver.race_timer.is_alive():
      driver.race_timer.cancel()
      driver.race_timer = RunningBound(driver.config.global_timeout.total_seconds())
      driver.race_timer.start()
      log.info(f"the test's {driver.race_timer.interval:.0f} s global timeout counts time not frozen")

  def ended(m, unit):
      # Inactive or failed: a unit still deactivating, or activating, runs
      # yet, though `systemctl is-active` says it is not active.
      return m.execute(f"systemctl show -P ActiveState {unit}", timeout=None)[1].strip() in ("inactive", "failed")

  def wait_until(what, cond, clients=(), last=None):
      # For states with no event to wait on: cond() polled. clients are
      # (machine, unit) pairs that bring the state about; once one is gone,
      # last() (cond() unless given) has the last look.
      with log.nested(f"waiting for {what}"):
          while not cond():
              for m, unit in clients:
                  if ended(m, unit):
                      if (last or cond)():
                          return
                      journal = m.execute(f"journalctl -a --no-pager -u {unit}", timeout=None)[1]
                      raise RequestedAssertionFailed(f"{unit} on {m.name} ended while waiting for {what}:\n{journal}")
              time.sleep(1)

  def wait_succeeds(m, cmd, clients=()):
      out = [""]

      def ok():
          status, out[0] = m.execute(cmd, timeout=None)
          return status == 0

      wait_until(f"{cmd} on {m.name}", ok, clients)
      return out[0]

  def listening(*ports):
      # A shell test that something listens on each of ports, as listening
      # in tests/e2e/harness.rs. Not a connect (nc -z): a connect to a port
      # in the ephemeral range (32768-60999 by default) that nothing listens
      # on can be given that same port as its source and meet itself, a TCP
      # simultaneous open, which finds the port open. Nor does it start a
      # socket-activated service. If ss fails, so does the test, as when
      # nothing listens.
      return " && ".join(f"out=$(ss -Hltn 'sport = :{p}') && [ -n \"$out\" ]" for p in ports)

  def wait_listening(m, *ports, clients=()):
      wait_succeeds(m, listening(*ports), clients)

  def wait_unit(m, unit):
      # wait_for_unit's: failed, or inactive with no job to start it, ends it.
      def state():
          return m.execute(f"systemctl show -P ActiveState {unit}", timeout=None)[1].strip()

      def active():
          s = state()
          if s == "inactive" and "No jobs" in m.execute("systemctl list-jobs --full 2>&1", timeout=None)[1] and state() == "inactive":
              s = "inactive with no jobs"
          if s in ["failed", "inactive with no jobs"]:
              journal = m.execute(f"journalctl -a --no-pager -u {unit}", timeout=None)[1]
              raise RequestedAssertionFailed(f"unit {unit} on {m.name} is {s}:\n{journal}")
          return s == "active"

      wait_until(f"unit {unit} on {m.name}", active)

  streams = iter(range(1 << 62))

  def wait_stream(m, cmd, *patterns, more_than=0, clients=(), now=None):
      # The (more_than + 1)th line of cmd's output that matches every pattern
      # (awk EREs; colour escapes dropped), read as cmd hands it over: an
      # event. cmd follows a log from its start, as journalctl -f --no-tail,
      # in the background; its end first fails the wait. So does a client
      # (see wait_until) gone with the line not in `now`, the log so far.
      awk = (
          "BEGIN { m = ARGC - 1; for (i = 1; i <= m; i++) p[i] = ARGV[i]; ARGC = 1 }"
          ' { gsub(/\\033\\[[0-9;]*m/, ""); for (i = 1; i <= m; i++) if ($0 !~ p[i]) next; if (++c >= k) exit }'
          " END { exit (c < k) }"
      )
      match = f"awk -v k={more_than + 1} {shlex.quote(awk)} {' '.join(shlex.quote(x) for x in patterns)}"
      done = f"/tmp/wait-stream-{next(streams)}"
      # (the driver's shell has set -eu: every status is caught)
      m.succeed(
          f"{{ r=0; mkfifo {done}.io; {cmd} > {done}.io 2>/dev/null </dev/null & p=$!; {match} < {done}.io || r=$?;"
          + f" kill $p 2>/dev/null || true; echo $r > {done}.new; mv {done}.new {done}; }} >/dev/null 2>&1 &"
      )

      def seen():
          status, out = m.execute(f"cat {done} 2>/dev/null", timeout=None)
          if status == 0 and out.strip() != "0":
              raise RequestedAssertionFailed(f"{cmd} on {m.name} ended before line {more_than + 1} with {patterns}")
          return status == 0

      def seen_now():
          return seen() or m.execute(f"{match} < <({now})", timeout=None)[0] == 0

      wait_until(f"line {more_than + 1} with {patterns} from {cmd} on {m.name}", seen, clients, seen_now if now else None)

  def wait_journal(m, unit, *patterns, more_than=0, clients=()):
      journal = f"journalctl -a -o cat -u {unit}"
      wait_stream(m, f"{journal} -f --no-tail", *patterns, more_than=more_than, clients=clients, now=journal)
''
