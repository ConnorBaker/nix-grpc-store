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

  def wait_until(what, cond, clients=()):
      # For states with no event to wait on: cond() polled. clients are
      # (machine, unit) pairs that bring the state about.
      with log.nested(f"waiting for {what}"):
          while not cond():
              for m, unit in clients:
                  if m.execute(f"systemctl is-active -q {unit}", timeout=None)[0] != 0 and not cond():
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

  def wait_port(m, port, addr="localhost"):
      wait_succeeds(m, f"nc -z {addr} {port}")

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

  def wait_stream(m, cmd, *patterns, more_than=0):
      # The (more_than + 1)th line of cmd's output that matches every pattern
      # (awk EREs; colour escapes dropped), read as cmd hands it over: an
      # event. cmd follows a log from its start, as journalctl -f --no-tail;
      # its end first fails the wait.
      awk = (
          "BEGIN { m = ARGC - 1; for (i = 1; i <= m; i++) p[i] = ARGV[i]; ARGC = 1 }"
          ' { gsub(/\\033\\[[0-9;]*m/, ""); for (i = 1; i <= m; i++) if ($0 !~ p[i]) next; if (++c >= k) exit }'
          " END { exit (c < k) }"
      )
      with log.nested(f"waiting for line {more_than + 1} with {patterns} from {cmd} on {m.name}"):
          m.succeed(
              f"coproc S {{ exec {cmd} 2>/dev/null </dev/null; }}; p=$S_PID; r=0; "
              + f"awk -v k={more_than + 1} {shlex.quote(awk)} {' '.join(shlex.quote(x) for x in patterns)}"
              + ' <&"''${S[0]}" || r=$?; kill "$p" 2>/dev/null || true; exit "$r"'
          )

  def wait_journal(m, unit, *patterns, more_than=0):
      wait_stream(m, f"journalctl -a -f --no-tail -o cat -u {unit}", *patterns, more_than=more_than)
''
