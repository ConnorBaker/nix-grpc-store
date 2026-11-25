use std::env as std_env;
use std::io::{self, Read};
use std::os::unix::net::UnixStream;
use std::path::Path;
use std::sync::{Mutex, Once};
use std::thread;
use std::time::{Duration, Instant};

use crate::wire;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Node(pub &'static str);

pub const MACHINE: Node = Node("machine");

/// Between two looks at a condition. A look costs a round trip of about 20ms.
const POLL: Duration = Duration::from_millis(100);

pub fn env(key: &str) -> String {
    std_env::var(key).unwrap_or_else(|_| panic!("missing env var {key}"))
}

pub struct Out {
    pub code: i32,
    pub stdout: String,
    pub stderr: String,
    /// The connection to the node failed and the command never reported its
    /// status, so we do not know whether it ran to the end.
    pub lost: bool,
    /// The node took the command, so it may have run.
    started: bool,
}

impl Out {
    pub fn combined(&self) -> String {
        format!("{}{}", self.stdout, self.stderr)
    }
}

/// Asks vhost-device-vsock, which listens on `sock` for one VM, for the
/// agent's port on that VM. No deadline: a VM that is gone ends the
/// connection, an event; one that answers nothing hangs the run.
fn connect(sock: &Path) -> io::Result<UnixStream> {
    use std::io::Write;
    let stream = UnixStream::connect(sock)?;
    (&stream).write_all(format!("CONNECT {}\n", wire::PORT).as_bytes())?;
    let mut answer = Vec::new();
    let mut byte = [0u8; 1];
    while byte[0] != b'\n' {
        (&stream).read_exact(&mut byte)?;
        answer.push(byte[0]);
    }
    if !answer.starts_with(b"OK ") {
        return Err(io::Error::other(format!(
            "no agent: {}",
            String::from_utf8_lossy(&answer).trim()
        )));
    }
    Ok(stream)
}

/// Runs `argv` through the agent behind `sock` to its end, however long it
/// takes.
fn exchange(sock: &Path, argv: &[&str]) -> Out {
    let mut started = false;
    let mut output = wire::Output::default();
    let code = (|| {
        let stream = connect(sock)?;
        started = true;
        wire::write_args(&mut &stream, argv)?;
        wire::receive(&mut &stream, &mut &stream, &mut output)
    })();
    let mut stderr = String::from_utf8_lossy(&output.stderr).into_owned();
    if let Err(e) = &code {
        stderr.push_str(&e.to_string());
    }
    Out {
        code: code.as_ref().map_or(-1, |c| *c),
        stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
        stderr,
        lost: code.is_err(),
        started,
    }
}

/// Runs `argv` on `node` to its end, however long it takes.
fn run_once(node: Node, argv: &[&str]) -> Out {
    let sock = env(&format!("NGS_{}_SOCK", node.0.to_uppercase()));
    exchange(Path::new(&sock), argv)
}

static READY: Mutex<Vec<&'static str>> = Mutex::new(Vec::new());
static HEARTBEAT: Once = Once::new();

fn ensure_ready(node: Node) {
    HEARTBEAT.call_once(|| {
        let start = Instant::now();
        thread::spawn(move || {
            loop {
                thread::sleep(Duration::from_secs(30));
                println!("[e2e] heartbeat t={}s", start.elapsed().as_secs());
            }
        });
    });
    let mut ready = READY.lock().unwrap_or_else(|e| e.into_inner());
    if ready.contains(&node.0) {
        return;
    }
    // No deadline: an agent that never comes up hangs the run.
    while run_once(node, &["true"]).code != 0 {
        thread::sleep(POLL);
    }
    ready.push(node.0);
}

/// Runs `attempt` again while the connection fails, with no count. A
/// repeatable command is retried even if it may have started; any other only
/// if the node never took it, so it cannot run twice.
fn retrying(repeatable: bool, mut attempt: impl FnMut() -> Out) -> Out {
    let mut o = attempt();
    while o.lost && (repeatable || !o.started) {
        thread::sleep(Duration::from_secs(2));
        o = attempt();
    }
    o
}

/// `argv` for failure messages. The node gets the arguments themselves.
fn shell_join(argv: &[&str]) -> String {
    argv.join(" ")
}

fn run_agent(node: Node, argv: &[&str], repeatable: bool) -> Out {
    let cmd = shell_join(argv);
    ensure_ready(node);
    let o = retrying(repeatable, || run_once(node, argv));
    assert!(
        !o.lost,
        "[{}] connection lost: {cmd}\n{}",
        node.0,
        o.combined()
    );
    o
}
/// Runs `argv` on `node` to its end, whatever its status.
pub fn run(node: Node, argv: &[&str]) -> Out {
    run_agent(node, argv, false)
}

/// An argv of `String`s as one of `&str`s.
pub fn strs(args: &[String]) -> Vec<&str> {
    args.iter().map(String::as_str).collect()
}

fn expect_ok(node: Node, argv: &[&str], repeatable: bool) -> String {
    let cmd = shell_join(argv);
    let o = run_agent(node, argv, repeatable);
    assert_eq!(
        o.code,
        0,
        "[{}] failed ({}): {cmd}\n{}",
        node.0,
        o.code,
        o.combined()
    );
    o.stdout
}

pub fn succeed(node: Node, argv: &[&str]) -> String {
    expect_ok(node, argv, false)
}

/// Like `succeed`, for read-only commands such as `journalctl` or `systemctl
/// show`, which are safe to repeat after a dropped connection.
pub fn query(node: Node, argv: &[&str]) -> String {
    expect_ok(node, argv, true)
}

pub fn fail(node: Node, argv: &[&str]) -> String {
    let cmd = shell_join(argv);
    let o = run(node, argv);
    assert_ne!(
        o.code,
        0,
        "[{}] expected failure: {cmd}\n{}",
        node.0,
        o.combined()
    );
    o.combined()
}

/// Re-runs `argv` until it succeeds, with no deadline: for a state the node
/// gives no event of. One that never comes hangs the test.
pub fn wait_until_succeeds(node: Node, argv: &[&str]) {
    println!("[e2e] waiting: [{}] {}", node.0, shell_join(argv));
    while run_agent(node, argv, true).code != 0 {
        thread::sleep(POLL);
    }
}

/// Waits for `unit` to be active. A unit that failed never gets there by
/// itself, so that ends the wait too, showing its journal.
pub fn wait_for_unit(node: Node, unit: &str) {
    println!("[e2e] waiting: [{}] {unit} active", node.0);
    loop {
        match unit_state(node, unit).as_str() {
            "active" => return,
            "failed" => panic!(
                "[{}] {unit} failed while waited for:\n{}",
                node.0,
                journal(node, unit)
            ),
            _ => thread::sleep(POLL),
        }
    }
}

/// A shell test that something listens on `port`. Not a connect: a connect
/// to a port in the ephemeral range (32768-60999 by default) that nothing
/// listens on can be given that same port as its source and meet itself, a
/// TCP simultaneous open, which finds the port open. If `ss` fails, so does
/// the test, as when nothing listens.
pub fn listening(port: u16) -> String {
    format!("out=$(ss -Hltn 'sport = :{port}') && [ -n \"$out\" ]")
}

pub fn wait_for_open_port(node: Node, port: u16) {
    wait_until_succeeds(node, &["bash", "-c", &listening(port)]);
}

pub fn unit_state(node: Node, unit: &str) -> String {
    query(node, &["systemctl", "show", "-P", "ActiveState", unit])
        .trim()
        .to_string()
}

/// Inactive or failed: a unit still deactivating, or activating, runs yet,
/// though `systemctl is-active` says it is not active.
pub fn ended(node: Node, unit: &str) -> bool {
    matches!(unit_state(node, unit).as_str(), "inactive" | "failed")
}

pub fn journal(node: Node, unit: &str) -> String {
    query(node, &["journalctl", "-u", unit, "--no-pager"])
}

/// Whether `line` matches `pattern`, where `.*` stands for any text and a
/// trailing `$` for the line's end. That is all the patterns in these tests
/// use of a regular expression.
fn line_matches(line: &str, pattern: &str) -> bool {
    let (pattern, anchored) = match pattern.strip_suffix('$') {
        Some(p) => (p, true),
        None => (pattern, false),
    };
    let parts: Vec<&str> = pattern.split(".*").collect();
    let mut rest = if anchored { line.trim_end() } else { line };
    for (i, part) in parts.iter().enumerate() {
        if anchored && i + 1 == parts.len() {
            return rest.ends_with(part);
        }
        match rest.find(part) {
            Some(at) => rest = &rest[at + part.len()..],
            None => return false,
        }
    }
    true
}

/// `pattern` as a regular expression for `journalctl --grep`. A trailing `$`
/// anchors it at the end of the line, as in `line_matches`.
fn grep_pattern(pattern: &str) -> String {
    let (pattern, anchor) = match pattern.strip_suffix('$') {
        Some(p) => (p, "$"),
        None => (pattern, ""),
    };
    let body = pattern
        .split(".*")
        .map(|part| {
            part.chars()
                .map(|c| {
                    if r".[]()*+?{}|^$\".contains(c) {
                        format!("\\{c}")
                    } else {
                        c.to_string()
                    }
                })
                .collect::<String>()
        })
        .collect::<Vec<_>>()
        .join(".*");
    format!("{body}{anchor}")
}

/// Filters on the node, so a long journal need not cross to the host.
pub fn journal_matches(node: Node, unit: &str, pattern: &str) -> Vec<String> {
    let regex = grep_pattern(pattern);
    let argv = [
        "journalctl",
        "-u",
        unit,
        "--no-pager",
        "--case-sensitive=true",
        "--grep",
        &regex,
    ];
    let o = run_agent(node, &argv, true);
    // 1: no line matched.
    assert!(
        o.code <= 1,
        "[{}] failed ({}): journalctl\n{}",
        node.0,
        o.code,
        o.combined()
    );
    o.stdout
        .lines()
        .filter(|l| line_matches(l, pattern))
        .map(String::from)
        .collect()
}

pub fn journal_count(node: Node, unit: &str, pattern: &str) -> u64 {
    journal_matches(node, unit, pattern).len() as u64
}

pub fn assert_journal(node: Node, unit: &str, pattern: &str) {
    if journal_matches(node, unit, pattern).is_empty() {
        panic!(
            "[{}] journal of {unit} has no line matching: {pattern}\n{}",
            node.0,
            journal(node, unit)
        );
    }
}

pub fn assert_no_journal(node: Node, unit: &str, pattern: &str) {
    let hits: Vec<_> = journal_matches(node, unit, pattern);
    assert!(
        hits.is_empty(),
        "[{}] journal of {unit} has lines matching: {pattern}\n{}",
        node.0,
        hits.join("\n")
    );
}

/// Waits for `unit`'s journal to have more than `before` lines matching
/// `pattern`, as journal_count counts them. An event: journalctl -f hands
/// over each line as it is written, and awk ends at the one past `before`;
/// journalctl is killed then. No deadline; but a unit of `clients` that is to
/// bring the line about and is no longer active, with the line still not
/// there, fails the wait, with its journal.
pub fn wait_journal_above(node: Node, unit: &str, pattern: &str, before: u64, clients: &[&str]) {
    let script = r#"coproc J { exec journalctl -u "$1" -f -q --no-tail --no-pager --case-sensitive=true --grep "$2" 2>/dev/null </dev/null; }
p=$J_PID
exec 3<&"${J[0]}"
awk -v n="$3" 'NR > n { f = 1; exit } END { exit !f }' <&3 & a=$!
exec 3<&-
u=$1 g=$2 n=$3; shift 3
gone() { for c in "$@"; do case $(systemctl show -P ActiveState "$c") in inactive|failed) echo "$c"; return 0;; esac; done; return 1; }
while kill -0 "$a" 2>/dev/null && ! c=$(gone "$@"); do sleep 1; done
if kill -0 "$a" 2>/dev/null; then
  kill "$a" "$p" 2>/dev/null
  # The line may have come just before the client ended.
  [ "$(journalctl -u "$u" -q --no-pager --case-sensitive=true --grep "$g" | wc -l)" -gt "$n" ] && exit 0
  echo "$c ended before the line" >&2; journalctl -u "$c" --no-pager >&2; exit 3
fi
wait "$a" && r=0 || r=$?
kill "$p" 2>/dev/null || true
exit $r"#;
    println!("[e2e] waiting: [{}] {unit} line {} matching {pattern}", node.0, before + 1);
    let (regex, n) = (grep_pattern(pattern), before.to_string());
    query(node, &[&["bash", "-c", script, "follow", unit, &regex, &n], clients].concat());
}

pub fn write_file(node: Node, path: &str, content: &str) {
    succeed(
        node,
        &[
            "sh",
            "-c",
            r#"printf '%s\n' "$1" > "$2""#,
            "write",
            content,
            path,
        ],
    );
}

/// Re-checks `f` until it holds, with no deadline: for a state the nodes
/// give no event of. `what` names it for whoever reads a hung run. A unit of
/// `clients` that brings the state about and is no longer active, with the
/// state still not there, can never bring it about: that ends the wait too,
/// a failure showing its journal.
pub fn retry(clients: &[(Node, &str)], what: &str, mut f: impl FnMut() -> bool) {
    println!("[e2e] waiting: {what}");
    loop {
        if f() {
            return;
        }
        if let Some(&(node, c)) = clients.iter().find(|&&(node, c)| ended(node, c)) {
            // It may have brought the state about just before it ended.
            if f() {
                return;
            }
            panic!("[{}] {c} ended before: {what}\n{}", node.0, journal(node, c));
        }
        // Not `POLL`: the farm tests count these polls as 500 ms each
        // (scheduling.rs, LAST_BUILD_TO_EXIT_POLLS and BOTH_MISSED_POLLS).
        thread::sleep(Duration::from_millis(500));
    }
}

pub fn sleep(secs: u64) {
    thread::sleep(Duration::from_secs(secs));
}

pub fn metrics(node: Node, port: u16) -> Vec<String> {
    query(
        node,
        &["curl", "-sf", &format!("http://127.0.0.1:{port}/metrics")],
    )
    .lines()
    .map(String::from)
    .collect()
}

pub fn unit_result(node: Node, unit: &str) -> String {
    query(
        node,
        &["systemctl", "show", "-p", "Result", "--value", unit],
    )
    .trim()
    .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::io::{BufRead, BufReader, Write};
    use std::os::unix::net::UnixListener;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU32, Ordering};

    fn out(lost: bool, started: bool) -> Out {
        Out {
            code: if lost { -1 } else { 0 },
            stdout: String::new(),
            stderr: String::new(),
            lost,
            started,
        }
    }

    fn calls(repeatable: bool, started: bool) -> u32 {
        let mut n = 0;
        retrying(repeatable, || {
            n += 1;
            out(n < 3, started)
        });
        n
    }

    #[test]
    fn a_repeatable_command_is_retried_after_any_drop() {
        assert_eq!(calls(true, true), 3);
    }

    #[test]
    fn another_command_is_retried_only_if_the_node_never_took_it() {
        assert_eq!(calls(false, true), 1);
        assert_eq!(calls(false, false), 3);
    }

    /// Stands in for vhost-device-vsock and, if `agent`, the agent behind it.
    fn node(agent: bool) -> PathBuf {
        static N: AtomicU32 = AtomicU32::new(0);
        let sock = std::env::temp_dir().join(format!(
            "ngs-{}-{}.sock",
            std::process::id(),
            N.fetch_add(1, Ordering::Relaxed)
        ));
        let listener = UnixListener::bind(&sock).unwrap();
        thread::spawn(move || {
            for stream in listener.incoming() {
                let mut stream = stream.unwrap();
                thread::spawn(move || {
                    let mut hello = String::new();
                    BufReader::new(&stream).read_line(&mut hello).unwrap();
                    assert_eq!(hello, format!("CONNECT {}\n", wire::PORT));
                    if !agent {
                        return;
                    }
                    stream.write_all(b"OK 1073741824\n").unwrap();
                    let argv = wire::read_args(&mut stream).unwrap();
                    let mut acks = stream.try_clone().unwrap();
                    let _ = wire::serve(&[], &argv, &mut stream, || acks.read_exact(&mut [0u8; 1]));
                });
            }
        });
        sock
    }

    #[test]
    fn a_command_reports_its_status_and_both_streams() {
        let o = exchange(
            &node(true),
            &["bash", "-c", "echo out; echo err >&2; exit 3"],
        );
        assert_eq!(
            (o.code, o.stdout.as_str(), o.stderr.as_str()),
            (3, "out\n", "err\n")
        );
        assert!(!o.lost);
    }

    #[test]
    fn output_larger_than_one_piece_arrives_whole() {
        let argv = ["head", "-c", "300000", "/dev/zero"];
        let o = exchange(&node(true), &argv);
        assert_eq!((o.code, o.stdout.len()), (0, 300000));
    }

    #[test]
    fn arguments_reach_the_command_untouched() {
        let argv = ["printf", "<%s>", "a b", "", "it's", "$HOME", "x;y\n*"];
        let o = exchange(&node(true), &argv);
        assert_eq!(o.stdout, "<a b><><it's><$HOME><x;y\n*>");
    }

    #[test]
    fn a_command_killed_by_a_signal_reports_it_like_a_shell() {
        let o = exchange(&node(true), &["bash", "-c", "kill -KILL $$"]);
        assert_eq!(o.code, 137);
        assert!(!o.lost);
    }

    #[test]
    fn a_node_without_an_agent_never_took_the_command() {
        let o = exchange(&node(false), &["true"]);
        assert!(o.lost && !o.started, "{}", o.stderr);
    }

    #[test]
    fn a_pattern_matches_text_with_a_wildcard_between_its_parts() {
        let line = "event=denied method=Connect cn=stranger role=none";
        assert!(line_matches(
            line,
            "event=denied method=.* cn=stranger role=none"
        ));
        assert!(line_matches(line, "cn=stranger"));
        assert!(!line_matches(line, "cn=stranger.*event=denied"));
        assert!(!line_matches(line, "role=read-only"));
    }

    #[test]
    fn a_trailing_dollar_anchors_a_pattern_at_the_line_end() {
        let line = "level=info event=predecessor_exited pid=1234";
        assert!(line_matches(line, "event=predecessor_exited pid=1234$"));
        assert!(!line_matches(line, "event=predecessor_exited pid=123$"));
        assert!(line_matches(line, "event=.* pid=1234$"));
        assert!(!line_matches("event=handed_over_exit", "event=handed_over pid="));
        assert_eq!(grep_pattern("pid=1.2$"), r"pid=1\.2$");
    }
}
