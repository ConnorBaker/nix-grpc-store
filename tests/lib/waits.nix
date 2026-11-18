# Python for a NixOS testScript: waits without a deadline. The driver's own
# waits (wait_for_unit, wait_until_succeeds, wait_for_open_port, retry) and
# its execute all take one, 15 minutes unless given another, and the test
# one more (globalTimeout). An event that never comes hangs the test here;
# that hang is the failure to diagnose. A client the wait names that ends
# first fails it, with its journal.
''
  import shlex
  import time
  from test_driver.errors import RequestedAssertionFailed

  driver.race_timer.cancel()
  log.info("the test's global timeout is cancelled: its waits have no deadline")

  def wait_until(what, cond, clients=(), last=None):
      # For states with no event to wait on: cond() polled. clients are
      # (machine, unit) pairs that bring the state about; once one is gone,
      # last() (cond() unless given) has the last look.
      with log.nested(f"waiting for {what}"):
          while not cond():
              for m, unit in clients:
                  if m.execute(f"systemctl is-active -q {unit}", timeout=None)[0] != 0 and not (last or cond)():
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
      m.succeed(
          f"{{ coproc S {{ exec {cmd} 2>/dev/null </dev/null; }}; p=$S_PID; {match}"
          + ' <&"''${S[0]}"; echo $? > '
          + f'{done}.new; mv {done}.new {done}; kill "$p"; }} >/dev/null 2>&1 &'
      )

      def seen():
          status, out = m.execute(f"cat {done}", timeout=None)
          if status == 0 and out.strip() != "0":
              raise RequestedAssertionFailed(f"{cmd} on {m.name} ended before line {more_than + 1} with {patterns}")
          return status == 0

      def seen_now():
          return seen() or m.execute(f"{now} | {match}", timeout=None)[0] == 0

      wait_until(f"line {more_than + 1} with {patterns} from {cmd} on {m.name}", seen, clients, seen_now if now else None)

  def wait_journal(m, unit, *patterns, more_than=0, clients=()):
      journal = f"journalctl -a -o cat -u {unit}"
      wait_stream(m, f"{journal} -f --no-tail", *patterns, more_than=more_than, clients=clients, now=journal)
''
