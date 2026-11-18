use std::env as std_env;
use std::io::Read;
use std::process::{Command, Stdio};
use std::sync::{Mutex, Once};
use std::thread;
use std::time::{Duration, Instant};

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Node(pub &'static str);

pub const MACHINE: Node = Node("machine");

pub fn env(key: &str) -> String {
    std_env::var(key).unwrap_or_else(|_| panic!("missing env var {key}"))
}

fn ssh_args(node: Node) -> Vec<String> {
    let mut args = vec!["-F".into(), env("NGS_SSH_CONFIG")];
    for opt in [
        "User=root",
        "StrictHostKeyChecking=no",
        "UserKnownHostsFile=/dev/null",
        // No ConnectTimeout or ServerAlive: a VM that is gone ends the vsock
        // connection, an event; one that answers nothing hangs the run.
        "LogLevel=ERROR",
    ] {
        args.push("-o".into());
        args.push(opt.into());
    }
    args.push(format!(
        "vsock-mux/{}",
        env(&format!("NGS_{}_SOCK", node.0.to_uppercase()))
    ));
    args
}

pub struct Out {
    pub code: i32,
    pub stdout: String,
    pub stderr: String,
    /// ssh itself failed (exit 255) and the command never reported its
    /// status, so we do not know whether it ran to the end. A command that
    /// returns 255 on its own still reports, so it is not mistaken for this.
    pub lost: bool,
}

impl Out {
    pub fn combined(&self) -> String {
        format!("{}{}", self.stdout, self.stderr)
    }
}

fn drain<R: Read + Send + 'static>(mut r: R) -> thread::JoinHandle<String> {
    thread::spawn(move || {
        let mut b = Vec::new();
        let _ = r.read_to_end(&mut b);
        String::from_utf8_lossy(&b).into_owned()
    })
}

/// Runs `cmd` on `node` to its end, however long it takes.
fn run_once(node: Node, cmd: &str) -> Out {
    let mut child = Command::new(env("NGS_SSH"))
        .args(ssh_args(node))
        .arg(format!(
            "( set -euo pipefail\n{cmd}\n)\necho \"{RC_MARK}$?\"\n"
        ))
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn ssh");
    // Readers keep a chatty command from filling the pipe and blocking.
    let out = drain(child.stdout.take().unwrap());
    let err = drain(child.stderr.take().unwrap());
    let status = child.wait().expect("wait");
    let mut stdout = out.join().unwrap();
    // ssh exits 255 when the remote command dies from a signal, so the
    // command's own status travels in the output.
    let (code, lost) = match stdout.rfind(RC_MARK) {
        Some(at) => {
            let rc = stdout[at + RC_MARK.len()..].trim().parse().unwrap_or(-1);
            stdout.truncate(at);
            (rc, false)
        }
        None => (status.code().unwrap_or(-1), status.code() == Some(255)),
    };
    Out {
        code,
        stdout,
        stderr: err.join().unwrap(),
        lost,
    }
}

const RC_MARK: &str = "__NGS_RC=";

fn connect_failed(stderr: &str) -> bool {
    [
        "banner exchange",
        "Connection refused",
        "Connection timed out",
        "Connection closed by remote host",
        "kex_exchange_identification",
    ]
    .iter()
    .any(|m| stderr.contains(m))
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
    // No deadline: a backdoor that never comes up hangs the run.
    while run_once(node, "true").code != 0 {
        thread::sleep(Duration::from_millis(500));
    }
    ready.push(node.0);
}

/// Runs `attempt` again while ssh fails, with no count. A repeatable command
/// is retried even if it may have started; any other only if ssh could not
/// connect, so it cannot run twice.
fn retrying(repeatable: bool, mut attempt: impl FnMut() -> Out) -> Out {
    let mut o = attempt();
    while o.lost && (repeatable || connect_failed(&o.stderr)) {
        thread::sleep(Duration::from_secs(2));
        o = attempt();
    }
    o
}

/// Quotes `arg` for the remote shell, so it arrives as one word.
fn quote(arg: &str) -> String {
    let plain = !arg.is_empty()
        && arg
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || "-_./:=@%+,".contains(c));
    if plain {
        arg.to_string()
    } else {
        format!("'{}'", arg.replace('\'', r"'\''"))
    }
}

/// The remote command line for `argv`. ssh hands it to the login shell.
fn shell_join(argv: &[&str]) -> String {
    argv.iter().map(|a| quote(a)).collect::<Vec<_>>().join(" ")
}

fn run_ssh(node: Node, argv: &[&str], repeatable: bool) -> Out {
    let cmd = shell_join(argv);
    let cmd = cmd.as_str();
    ensure_ready(node);
    let o = retrying(repeatable, || run_once(node, cmd));
    assert!(
        !o.lost,
        "[{}] ssh connection lost: {cmd}\n{}",
        node.0,
        o.combined()
    );
    o
}

/// Runs `argv` on `node` to its end, whatever its status.
pub fn run(node: Node, argv: &[&str]) -> Out {
    run_ssh(node, argv, false)
}

/// An argv of `String`s as one of `&str`s.
pub fn strs(args: &[String]) -> Vec<&str> {
    args.iter().map(String::as_str).collect()
}

fn expect_ok(node: Node, argv: &[&str], repeatable: bool) -> String {
    let cmd = shell_join(argv);
    let o = run_ssh(node, argv, repeatable);
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
    while run_ssh(node, argv, true).code != 0 {
        thread::sleep(Duration::from_millis(500));
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
            _ => thread::sleep(Duration::from_millis(500)),
        }
    }
}

pub fn wait_for_open_port(node: Node, port: u16) {
    wait_until_succeeds(
        node,
        &["bash", "-c", &format!(": < /dev/tcp/127.0.0.1/{port}")],
    );
}

pub fn unit_state(node: Node, unit: &str) -> String {
    query(node, &["systemctl", "show", "-P", "ActiveState", unit])
        .trim()
        .to_string()
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

/// Filters on the node: long journals over the vsock ssh sometimes stall.
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
    let o = run_ssh(node, &argv, true);
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
        if let Some(&(node, c)) = clients.iter().find(|&&(node, c)| unit_state(node, c) != "active") {
            // It may have brought the state about just before it ended.
            if f() {
                return;
            }
            panic!("[{}] {c} ended before: {what}\n{}", node.0, journal(node, c));
        }
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

    fn out(lost: bool, stderr: &str) -> Out {
        Out {
            code: if lost { 255 } else { 0 },
            stdout: String::new(),
            stderr: stderr.into(),
            lost,
        }
    }

    fn calls(repeatable: bool, stderr: &str) -> u32 {
        let mut n = 0;
        retrying(repeatable, || {
            n += 1;
            out(n < 3, stderr)
        });
        n
    }

    #[test]
    fn a_repeatable_command_is_retried_after_any_drop() {
        assert_eq!(calls(true, "Connection reset by peer"), 3);
    }

    #[test]
    fn another_command_is_retried_only_if_ssh_never_connected() {
        assert_eq!(calls(false, "Connection reset by peer"), 1);
        assert_eq!(calls(false, "Connection refused"), 3);
    }

    #[test]
    fn arguments_reach_the_remote_shell_as_single_words() {
        assert_eq!(shell_join(&["ls", "-l", "/nix/store"]), "ls -l /nix/store");
        assert_eq!(
            shell_join(&["echo", "a b", "", "it's", "$HOME", "x;y"]),
            r#"echo 'a b' '' 'it'\''s' '$HOME' 'x;y'"#
        );
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
