# The global bound of tests/lib/waits.nix, on the driver alone (no
# machines), each with a 20 s globalTimeout:
# - hang: waits for an event that never comes; fails at its bound, saying
#   where it waited, and ends by terminate_test;
# - hang-report: the same with a report that raises; still ends by
#   terminate_test;
# - hang-release: the same with a terminate_test that raises; still ends
#   by SIGTERM (exit 143);
# - stopped: stopped (SIGSTOP, as the arbiter's freezer stops every thread)
#   for 60 s, three times its bound, running for little of it; passes;
# - flap: stopped 1.8 s and continued 0.2 s for 60 s, about 6 s running;
#   passes.
# A hang test that its bound does not end is ended by SIGALRM after 300 s
# of wall time, without terminate_test's line, and the check fails.
{ pkgs }:
let
  test =
    name: script:
    pkgs.testers.runNixOSTest {
      inherit name;
      globalTimeout = 20;
      nodes = { };
      testScript = ''
        ${import ./lib/waits.nix}
        ${script}
      '';
    };

  hang =
    name: script:
    pkgs.testers.testBuildFailure
      (test name ''
        signal.alarm(300)
        ${script}
        wait_until("an event that never comes", lambda: False)
      '').config.rawTestDerivation;

  stopper = pattern: ''
    import os, subprocess
    me = os.getpid()
    subprocess.run([sys.executable, "-c", f"import os, signal, time\n${pattern}"], check=True)
  '';
in
pkgs.runCommand "waits-bound"
  {
    hang = hang "waits-bound-hang" "";
    # The report reads the script's lines from driver.tests.
    hangReport = hang "waits-bound-hang-report" ''driver.tests = ""'';
    hangRelease = hang "waits-bound-hang-release" ''
      def release_fails():
          raise RuntimeError("a machine's release failed")

      setattr(driver, "terminate_test", release_fails)
    '';
    stopped = test "waits-bound-stopped" (
      stopper "os.kill({me}, signal.SIGSTOP); time.sleep(60); os.kill({me}, signal.SIGCONT)"
    );
    flap = test "waits-bound-flap" (
      stopper ''end = time.monotonic() + 60\nwhile time.monotonic() < end:\n    os.kill({me}, signal.SIGSTOP); time.sleep(1.8)\n    os.kill({me}, signal.SIGCONT); time.sleep(0.2)''
    );
  }
  ''
    grep -F "the test ran 20 s, its globalTimeout (a freeze counts at most 0.1 s), and did not finish" $hang/testBuildFailure.log
    grep -F 'wait_until("an event that never comes", lambda: False)' $hang/testBuildFailure.log
    grep -F "timeout reached; test terminating" $hang/testBuildFailure.log
    grep -F "timeout reached; test terminating" $hangReport/testBuildFailure.log
    grep -F "the test ran 20 s" $hangRelease/testBuildFailure.log
    [ "$(cat $hangRelease/testBuildFailure.exit)" = 143 ]
    echo $stopped $flap > $out
  ''
