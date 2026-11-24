#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <exception>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <filesystem>
#include <ios>
#include <optional>
#include <print>
#include <sstream>
#include <stdexcept>
#include <stop_token>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <utility>
#include <vector>

#include <fcntl.h>
#include <signal.h> // NOLINT(modernize-deprecated-headers): SIGPIPE is POSIX, not in <csignal>.
#include <stdlib.h> // NOLINT(modernize-deprecated-headers): mkdtemp is POSIX, not in <cstdlib>.
#include <sched.h>
#include <sys/poll.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include <nix/util/file-descriptor.hh>

#include "logfmt.hh"
#include "push.hh"

// While the test sends our stderr to a file, a crash would lose what it
// printed there, an assertion's or a sanitizer's report among it: these say
// where our stderr went and replay the file to it. Only what a signal handler
// may call: open, read, write, close, dup2, poll, signal, raise and
// lock-free atomics.
namespace {
constexpr size_t pathBytes = 4096;
constexpr size_t chunkBytes = 4096;
// replayTo holds the descriptor to replay to, or one of these.
constexpr int nothingToReplay = -1;
constexpr int replaying = -2;
// A thread that crashes while another replays waits for it, at most this many
// naps of a millisecond, then dies: dying at once would cut the replay short.
constexpr int napsForAReplay = 10000;
// A signal handler can reach only globals.
std::atomic<int> replayTo = nothingToReplay; // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
static_assert(std::atomic<int>::is_always_lock_free);
std::array<char, pathBytes> capturedPath{}; // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
} // namespace

extern "C" void replayCaptured()
{
    int out = replayTo.load();
    while (out >= 0 && !replayTo.compare_exchange_weak(out, replaying)) {
    }
    if (out == replaying) {
        for (int naps = 0; naps < napsForAReplay && replayTo.load() == replaying; naps++) {
            static_cast<void>(::poll(nullptr, 0, 1));
        }
        return;
    }
    if (out < 0) {
        return;
    }
    int const captured = ::open(capturedPath.data(), O_RDONLY | O_CLOEXEC); // NOLINT(cppcoreguidelines-pro-type-vararg)
    if (captured == -1) {
        // Nothing to replay, but stderr goes back, and a waiting thread stops waiting.
        static_cast<void>(::dup2(out, STDERR_FILENO));
        replayTo.store(nothingToReplay);
        return;
    }
    std::array<char, chunkBytes> chunk{};
    for (ssize_t got = 0; (got = ::read(captured, chunk.data(), chunk.size())) > 0;) {
        for (ssize_t put = 0; put < got;) {
            ssize_t const wrote = ::write(out, chunk.data() + put, got - put); // NOLINT(cppcoreguidelines-pro-bounds-pointer-arithmetic)
            if (wrote <= 0) {
                break;
            }
            put += wrote;
        }
    }
    ::close(captured);
    static_cast<void>(::dup2(out, STDERR_FILENO));
    replayTo.store(nothingToReplay);
}

extern "C" void replayOnAbort(int signal)
{
    replayCaptured();
    static_cast<void>(std::signal(signal, SIG_DFL));
    static_cast<void>(std::raise(signal));
}

// A sanitizer calls this after its report, before it ends the process.
extern "C" void __sanitizer_set_death_callback(void (*callback)()) __attribute__((weak)); // NOLINT(bugprone-reserved-identifier,cert-dcl37-c,cert-dcl51-cpp)

namespace {

// One thread of ours as the kernel sees it: its state, and the syscall it
// sleeps in with its arguments ("running" when it is in none). A thread gone
// since it was listed reads as neither.
struct Task
{
    char state = '?';
    std::string syscall;
};

// The first line of a /proc file, or nothing where it cannot be read. Opened
// close-on-exec, as anything open while startProcess forks goes to niks3.
auto firstLine(const std::string & path) -> std::string
{
    constexpr size_t lineBytes = 1024;
    nix::AutoCloseFD const file{::open(path.c_str(), O_RDONLY | O_CLOEXEC)}; // NOLINT(cppcoreguidelines-pro-type-vararg)
    std::array<char, lineBytes> bytes{};
    ssize_t const got = file ? ::read(file.get(), bytes.data(), bytes.size()) : -1;
    if (got <= 0) {
        return {};
    }
    std::string const text(bytes.data(), static_cast<size_t>(got));
    return text.substr(0, text.find('\n'));
}

auto task(const std::string & tid) -> Task
{
    Task task;
    std::string const line = firstLine("/proc/self/task/" + tid + "/stat");
    if (auto const end = line.rfind(')'); end != std::string::npos && end + 2 < line.size()) {
        task.state = line.at(end + 2);
    }
    task.syscall = firstLine("/proc/self/task/" + tid + "/syscall");
    return task;
}

// The descriptor a sleeping task writes to.
auto sleepsWritingTo(const Task & task) -> std::optional<long>
{
    // The kernel prints the arguments in hex, after "0x".
    std::istringstream fields(task.syscall);
    long number = 0;
    long descriptor = 0;
    if (task.state != 'S' || !(fields >> number >> std::hex >> descriptor) || number != SYS_write) {
        return std::nullopt;
    }
    return descriptor;
}

auto isPipe(long descriptor) -> bool
{
    std::error_code error;
    auto const target = std::filesystem::read_symlink("/proc/self/fd/" + std::to_string(descriptor), error);
    return !error && target.string().starts_with("pipe:");
}

auto threads() -> std::vector<std::string>
{
    std::vector<std::string> tids;
    std::error_code error;
    for (std::filesystem::directory_iterator it("/proc/self/task", error), end; !error && it != end; it.increment(error)) {
        tids.push_back(it->path().filename());
    }
    return tids;
}

// A thread of ours sleeps writing into a pipe that only the child drains.
auto aWriteToTheChildSleeps() -> bool
{
    return std::ranges::any_of(threads(), [](const std::string & tid) -> bool {
        auto const descriptor = sleepsWritingTo(task(tid));
        return descriptor && *descriptor > STDERR_FILENO && isPipe(*descriptor);
    });
}

// The caller waits on the child: it sleeps while every other thread of ours
// but the watcher sleeps too, so nothing of ours can wake it, only the child
// or a timer. A caller that does not wait on the child sleeps only on a lock
// that a running thread holds: no thread holds a lock that pushLog takes
// while it sleeps, nor across a write to the child. logLine holds the stdio
// lock on stderr across its write, but the test's stderr is a file, which a
// write does not sleep on.
auto waitsOnChild(const std::string & caller, const std::string & watcher) -> bool
{
    return task(caller).state == 'S' && std::ranges::all_of(threads(), [&](const std::string & tid) -> bool {
        return tid == caller || tid == watcher || task(tid).state == 'S';
    });
}

// How often our stderr has said the queue gave way: its oldest log was
// dropped. It reads on from where it last stopped.
class Drops
{
public:
    explicit Drops(const std::filesystem::path & stderrFile)
        : captured(::open(stderrFile.c_str(), O_RDONLY | O_CLOEXEC)) // NOLINT(cppcoreguidelines-pro-type-vararg)
    {
    }

    auto operator()() -> int
    {
        std::array<char, chunkBytes> bytes{};
        for (ssize_t got = 0; captured && (got = ::read(captured.get(), bytes.data(), bytes.size())) > 0;) {
            partial.append(bytes.data(), static_cast<size_t>(got));
        }
        for (size_t end = 0; (end = partial.find('\n')) != std::string::npos; partial.erase(0, end + 1)) {
            count += partial.substr(0, end).contains("event=build_log_dropped") ? 1 : 0;
        }
        return count;
    }

private:
    nix::AutoCloseFD captured;
    std::string partial; // read, but not yet to its end of line
    int count = 0;
};

// Replays the captured stderr when it goes out of scope. Declared after the
// descriptor replayTo names, it replays while that is still open, however the
// test leaves.
struct ReplayOnExit
{
    ReplayOnExit() = default;
    ReplayOnExit(const ReplayOnExit &) = delete;
    ReplayOnExit(ReplayOnExit &&) = delete;
    auto operator=(const ReplayOnExit &) -> ReplayOnExit & = delete;
    auto operator=(ReplayOnExit &&) -> ReplayOnExit & = delete;
    ~ReplayOnExit()
    {
        replayCaptured();
    }
};

// Runs `act` when it goes out of scope, however the scope is left. An act
// that throws aborts, which replays the captured stderr, rather than
// escaping a destructor.
template<typename Act>
class OnExit
{
public:
    explicit OnExit(Act act)
        : act(std::move(act))
    {
    }
    OnExit(const OnExit &) = delete;
    OnExit(OnExit &&) = delete;
    auto operator=(const OnExit &) -> OnExit & = delete;
    auto operator=(OnExit &&) -> OnExit & = delete;
    ~OnExit()
    {
        try {
            act();
        } catch (...) {
            std::abort();
        }
    }

private:
    Act act;
};

// PushProcess drops past a queue of a few dozen logs. A queue that has not
// given way after this many logs never will, and a test says so instead of
// logging on: a step count, not a time. Waits for another thread are bounded
// by as many readings.
constexpr int dropWithin = 1'000'000;

// A build log must not wait for a child that does not read its stdin, nor
// for a write to it that the child holds up, nor for its ack, nor for room
// in its queue. The child reads nothing until its gate opens, and only a
// caller found waiting on it opens the gate, so a pushLog that waits ends
// with the gate open instead of hanging. The caller logs until a write to the
// child sleeps, then on behind it until the queue gives way, and once more.
//
// Known limits: a pushLog that waits a short while and then drops, rather
// than until the child acts, is caught only when the watcher reads it
// asleep three times running, which under load it may not be; and one that
// spins instead of sleeping is never read asleep, so the test hangs on it
// instead of failing. What the test printed while its stderr went to the
// file is lost where no handler runs: a stack overflow without a sanitizer
// (there is no alternate signal stack), SIGTERM or SIGKILL (a hung test
// killed by the build's timeout among them), _exit and exit.
void aLogDoesNotWaitForTheChild()
{
    constexpr mode_t ownerOnly = S_IRUSR | S_IWUSR;
    // Its own directory: a run that aborted leaves its directory behind.
    std::string unique = (std::filesystem::temp_directory_path() / "push-test-XXXXXX").string();
    if (mkdtemp(unique.data()) == nullptr) {
        throw std::system_error(errno, std::generic_category(), "mkdtemp");
    }
    auto const dir = std::filesystem::path(unique);
    auto const gate = dir / "gate";
    if (mkfifo(gate.c_str(), ownerOnly) != 0) {
        throw std::runtime_error("mkfifo");
    }
    // A writer kept open, and kept from the child: the child's open does not
    // block, its read does.
    nix::AutoCloseFD shut{open(gate.c_str(), O_RDWR | O_CLOEXEC)}; // NOLINT(cppcoreguidelines-pro-type-vararg)
    assert(shut);
    // Our stderr goes to a file while the test runs, where the caller reads
    // whether the queue gave way; it is passed on afterwards.
    auto const stderrFile = dir / "stderr";
    // Kept from the child: startProcess passes on every descriptor not marked
    // close-on-exec.
    nix::AutoCloseFD const ourStderr{fcntl(STDERR_FILENO, F_DUPFD_CLOEXEC, 0)}; // NOLINT(cppcoreguidelines-pro-type-vararg)
    // Replays while ourStderr is still open, however the test leaves.
    ReplayOnExit const replayOnExit;
    {
        nix::AutoCloseFD const file{open(stderrFile.c_str(), O_WRONLY | O_CREAT | O_APPEND | O_CLOEXEC, ownerOnly)}; // NOLINT(cppcoreguidelines-pro-type-vararg)
        assert(ourStderr && file && stderrFile.string().size() < capturedPath.size());
        std::ranges::copy(stderrFile.string(), capturedPath.begin());
        capturedPath.at(stderrFile.string().size()) = '\0';
        replayTo = ourStderr.get();
        nixgrpc::flushLog(); // what was logged before goes where stderr was
        assert(dup2(file.get(), STDERR_FILENO) != -1);
    }
    auto const openGate = [&] -> void {
        if (!shut) {
            return;
        }
        // Children started later read an empty file; the one reading now sees EOF.
        nix::AutoCloseFD const empty{::open((dir / "open").c_str(), O_WRONLY | O_CREAT | O_CLOEXEC, ownerOnly)}; // NOLINT(cppcoreguidelines-pro-type-vararg)
        std::filesystem::rename(dir / "open", gate);
        shut.close();
    };
    std::atomic<bool> opened = false;
    std::atomic<bool> threw = false;
    std::atomic<bool> detectorThrew = false;
    bool gaveWay = false;
    {
        nixgrpc::PushProcess push({"sh", "-c", "read -r _ < \"$0\"", gate.string()});
        std::string const caller = std::to_string(gettid());
        std::atomic<bool> done = false;
        std::atomic<int> pushed = 0;
        std::thread watch([&] -> void {
            try {
                std::string const watcher = std::to_string(gettid());
                // The threads are read one at a time, so one reading can catch
                // the caller asleep just before a thread that wakes it falls
                // asleep itself. Read as waiting three times running, with no
                // pushLog returned in between, the caller slept through them.
                constexpr int sameAnswers = 3;
                int answers = 0;
                int last = -1;
                while (!done) {
                    int const now = pushed;
                    answers = now == last && waitsOnChild(caller, watcher) ? answers + 1 : 0;
                    last = now;
                    if (answers == sameAnswers) {
                        opened = true;
                        openGate();
                        return;
                    }
                    std::this_thread::yield();
                }
            } catch (std::exception & err) {
                static_cast<void>(std::fputs((std::string("push-test: the detector threw: ") + err.what() + "\n").c_str(), stderr));
                detectorThrew = true;
            }
        });
        auto const log = [&] -> bool {
            try {
                push.pushLog("/nix/store/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-" + std::to_string(pushed) + ".drv");
            } catch (...) {
                threw = true;
                return false;
            }
            pushed++;
            return true;
        };
        try {
            // Ends: the child drains nothing, so the writes fill its pipe.
            while (!opened && !aWriteToTheChildSleeps() && log()) {
            }
            // Ends: with the writer held up the queue fills, and its oldest
            // gives way, or the logs reach dropWithin.
            Drops drops(stderrFile);
            int const dropped = drops();
            for (int i = 0; i < dropWithin && !opened && drops() == dropped && log(); i++) {
            }
            gaveWay = drops() != dropped || opened || threw;
            static_cast<void>(log());
        } catch (std::exception & err) {
            static_cast<void>(std::fputs((std::string("push-test: the detector threw: ") + err.what() + "\n").c_str(), stderr));
            detectorThrew = true;
        }
        done = true;
        watch.join();
        openGate();
    }
    replayCaptured();
    assert(!detectorThrew && "the detector threw");
    assert(!opened && "pushLog waited for the child");
    assert(!threw && "pushLog threw");
    assert(gaveWay && "the queue never gave way: no build_log_dropped within dropWithin logs");
    std::filesystem::remove_all(dir);
}

// The state of a process of ours, or '?' where it is gone or not yet known.
auto processState(const std::string & pid) -> char
{
    std::string const line = firstLine("/proc/" + pid + "/stat");
    auto const end = line.rfind(')');
    return end != std::string::npos && end + 2 < line.size() ? line.at(end + 2) : '?';
}

// All of a file, or nothing where it cannot be opened. Opened close-on-exec,
// as firstLine is.
auto contents(const std::filesystem::path & file) -> std::string
{
    nix::AutoCloseFD const input{::open(file.c_str(), O_RDONLY | O_CLOEXEC)}; // NOLINT(cppcoreguidelines-pro-type-vararg)
    std::string text;
    std::array<char, chunkBytes> bytes{};
    // NOLINTNEXTLINE(clang-analyzer-unix.BlockInCriticalSection): flushLog let go of the queue's lock; the analyzer does not follow it through the wait.
    for (ssize_t got = 0; input && (got = ::read(input.get(), bytes.data(), bytes.size())) > 0;) {
        text.append(bytes.data(), static_cast<size_t>(got));
    }
    return text;
}

// The pid the child wrote, or none before it did.
auto pidIn(const std::filesystem::path & file) -> std::optional<std::string>
{
    auto const text = contents(file);
    auto const end = text.find('\n');
    if (end == std::string::npos || end == 0) {
        return std::nullopt;
    }
    return text.substr(0, end);
}

// How a step that waits for another thread ended.
enum class Wait : std::uint8_t
{
    done,
    heldUp,
    // The child is gone: nothing of ours can be read as held up behind it,
    // and nothing it was to do will happen.
    childEnded,
    // unheldReadings readings found the step neither done nor held up.
    noAnswer,
};

// What a step of the stalled-stderr tests guards against, read directly: a
// thread of ours other than the log writer asleep writing to our stderr,
// which nothing reads until the test is done, so it sleeps there for good;
// or a child of the push whose stderr is ours, where its lines would wait
// for that stderr too. That fd 2 of the child is ours says so before the
// child writes a line there, and a child that never writes one holds
// nothing up; its own pipe is the child's stderr. A correct run never shows
// either, however slow it is.
auto heldOnOurStderr(const std::filesystem::path & childPid) -> bool
{
    if (std::ranges::any_of(threads(), [](const std::string & tid) -> bool {
            return firstLine("/proc/self/task/" + tid + "/comm") != "log-writer" && sleepsWritingTo(task(tid)) == STDERR_FILENO;
        })) {
        return true;
    }
    auto const child = pidIn(childPid);
    if (!child) {
        return false;
    }
    std::error_code childError;
    std::error_code ourError;
    auto const childs = std::filesystem::read_symlink("/proc/" + *child + "/fd/2", childError);
    auto const ours = std::filesystem::read_symlink("/proc/self/fd/2", ourError);
    return !childError && !ourError && childs == ours;
}

// Reads until `done` holds, or the step is read held up on our stderr
// (heldOnOurStderr). Each reading takes the hold before `done`: a caller
// that returned in between is read as done. A child that is gone ends the
// step, unless `childMayEnd`: nothing it was to do will happen then. Held
// up is never read on a correct run; what bounds a correct step is
// unheldReadings readings, a liveness bound: a step neither done nor held
// within them ends as noAnswer, and the test fails saying which step.
template<typename Done>
auto awaitUnheld(const std::filesystem::path & childPid, Done done, bool childMayEnd = false) -> Wait
{
    constexpr long unheldReadings = 10L * dropWithin;
    for (long i = 0; i < unheldReadings; i++) {
        bool const held = heldOnOurStderr(childPid);
        if (done()) {
            return Wait::done;
        }
        if (held) {
            return Wait::heldUp;
        }
        auto const child = pidIn(childPid);
        if (!childMayEnd && child && std::string_view("ZXx?").contains(processState(*child))) {
            return Wait::childEnded;
        }
        std::this_thread::yield();
    }
    return Wait::noAnswer;
}

// Ends the child named in `childPid` if it is a live child of ours, so that
// PushProcess can end: a write to it fails, and the ack reader reads the end
// of its stdout. A child that never reads would otherwise hold the log
// writer, and the joins that end PushProcess, forever. A pid that is no
// longer a child of ours is left alone.
void endChild(const std::filesystem::path & childPid)
{
    auto const child = pidIn(childPid);
    if (!child) {
        return;
    }
    std::string const line = firstLine("/proc/" + *child + "/stat");
    auto const end = line.rfind(')');
    if (end == std::string::npos) {
        return;
    }
    std::istringstream fields(line.substr(end + 1));
    char state = '?';
    // NOLINTNEXTLINE(misc-include-cleaner): pid_t comes from <sys/types.h>.
    pid_t parent = 0;
    if (fields >> state >> parent && parent == getpid() && state != 'Z') {
        static_cast<void>(::kill(std::stoi(*child), SIGKILL));
    }
}

// A log line that cannot be written holds up the thread that writes it, as
// every logLine does, and nothing else: not the caller whose log it names,
// nor the log writer, nor another pushLog. With the queue full and the log
// writer held up by the child, a first caller logs while our stderr is a
// full pipe that nobody reads, until a thread of ours sleeps writing that a
// log was dropped; the first caller must return rather than be that
// thread. Then the child reads all it is sent, and the queue must drain to
// it, and a second pushLog must return. A step is held up once a thread of
// ours other than the log writer is found asleep writing to our stderr
// (heldOnOurStderr), which a correct run never shows. Only then is our
// stderr read, which lets the line's writer go. Each step that waits for the
// next ends as awaitUnheld says: done, held up, without an answer, or with
// the child ended, when nothing it was to do will happen. After a step
// without an answer every child PushProcess starts is ended until it has
// ended, so that it can; however the test is left, the gate opens before
// PushProcess ends.
//
// Known limits: a pushLog that spins, or sleeps on something other than our
// stderr, is never read as held up; its step has no answer after
// unheldReadings readings, and the test then hangs joining it instead of
// failing. An exception while our stderr is the full pipe ends the test in
// std::terminate, whose message waits for stderr behind the stalled write,
// which waits for room: the test hangs.
void aStalledStderrHoldsUpOnlyItsWriter()
{
    constexpr mode_t ownerOnly = S_IRUSR | S_IWUSR;
    // Its own directory: a run that aborted leaves its directory behind.
    std::string unique = (std::filesystem::temp_directory_path() / "push-test-XXXXXX").string();
    if (mkdtemp(unique.data()) == nullptr) {
        throw std::system_error(errno, std::generic_category(), "mkdtemp");
    }
    auto const dir = std::filesystem::path(unique);
    auto const gate = dir / "gate";
    if (mkfifo(gate.c_str(), ownerOnly) != 0) {
        throw std::runtime_error("mkfifo");
    }
    nix::AutoCloseFD shut{open(gate.c_str(), O_RDWR | O_CLOEXEC)}; // NOLINT(cppcoreguidelines-pro-type-vararg)
    assert(shut);
    auto const childOut = dir / "child-stdin";
    auto const childPid = dir / "child-pid";
    auto const stderrFile = dir / "stderr";
    // Kept from the child, as in aLogDoesNotWaitForTheChild.
    nix::AutoCloseFD const ourStderr{fcntl(STDERR_FILENO, F_DUPFD_CLOEXEC, 0)}; // NOLINT(cppcoreguidelines-pro-type-vararg)
    ReplayOnExit const replayOnExit;
    {
        nix::AutoCloseFD const file{open(stderrFile.c_str(), O_WRONLY | O_CREAT | O_APPEND | O_CLOEXEC, ownerOnly)}; // NOLINT(cppcoreguidelines-pro-type-vararg)
        assert(ourStderr && file && stderrFile.string().size() < capturedPath.size());
        std::ranges::copy(stderrFile.string(), capturedPath.begin());
        capturedPath.at(stderrFile.string().size()) = '\0';
        replayTo = ourStderr.get();
        nixgrpc::flushLog(); // what was logged before goes where stderr was
        assert(dup2(file.get(), STDERR_FILENO) != -1);
    }
    // The child now reads all it is sent.
    auto const openGate = [&] -> void {
        if (!shut) {
            return;
        }
        nix::AutoCloseFD const empty{::open((dir / "open").c_str(), O_WRONLY | O_CREAT | O_CLOEXEC, ownerOnly)}; // NOLINT(cppcoreguidelines-pro-type-vararg)
        std::filesystem::rename(dir / "open", gate);
        shut.close();
    };
    bool filled = false;
    bool gaveWay = false;
    bool stderrStalled = false;
    bool firstReturned = false;
    bool drained = false;
    bool secondReturned = false;
    // A step that waited and found neither its answer nor that it was held up.
    std::string unanswered;
    std::string stalled;
    {
        // After a step without an answer, ends each child PushProcess starts
        // until PushProcess has ended: its log writer starts another for the
        // next log. Declared before PushProcess, it outlives it.
        std::jthread ender;
        // The child keeps its stdout, where acks go, open as fd 3, and after
        // the gate copies what it is sent to a file.
        nixgrpc::PushProcess push(
            {"sh",
             "-c",
             R"(exec 3>&1; echo $$ > "$2"; read -r _ < "$0"; exec cat > "$1")",
             gate.string(),
             childOut.string(),
             childPid.string()});
        // However the block is left, the child reads all it is sent before
        // PushProcess ends, which joins a log writer the child may hold up.
        OnExit const opened(openGate);
        int pushed = 0;
        auto const drv = [](const std::string & name) -> std::string {
            return "/nix/store/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-" + name + ".drv";
        };
        for (int i = 0; i < dropWithin && !(filled = aWriteToTheChildSleeps()); i++) {
            push.pushLog(drv(std::to_string(pushed++)));
        }
        if (filled) {
            Drops drops(stderrFile);
            int const dropped = drops();
            for (int i = 0; i < dropWithin && drops() == dropped; i++) {
                push.pushLog(drv(std::to_string(pushed++)));
            }
            gaveWay = drops() != dropped;
        }
        if (gaveWay) {
            // Our stderr becomes a pipe with no room left, which nothing
            // reads until the test is done. No assertion may fail while it
            // is: its message would wait for room too.
            nix::Pipe err;
            err.create();
            {
                int const room = fcntl(err.writeSide.get(), F_GETPIPE_SZ); // NOLINT(cppcoreguidelines-pro-type-vararg)
                assert(room > 0);
                nix::writeFull(err.writeSide.get(), std::string(static_cast<size_t>(room), '\0'));
            }
            nixgrpc::flushLog(); // what was logged before goes where stderr was
            assert(dup2(err.writeSide.get(), STDERR_FILENO) != -1);

            // The first caller logs until told to stop: the queue may have
            // room again (a log writer read asleep in its write may have
            // been only slow), and only a log that drops another has a line
            // to write.
            std::atomic<int> firstTid = 0;
            std::atomic<bool> firstDone = false;
            std::atomic<bool> stopFirst = false;
            std::atomic<int> lastFirst = 0;
            std::thread first([&] -> void {
                firstTid = gettid();
                for (int seq = 0; seq < dropWithin && !stopFirst; seq++) {
                    push.pushLog(drv("first-" + std::to_string(seq)));
                    lastFirst = seq;
                }
                firstDone = true;
            });
            auto const writesToStderr = [](const std::string & tid) -> bool {
                return sleepsWritingTo(task(tid)) == STDERR_FILENO;
            };
            // Ends: a line saying the oldest log was dropped goes to the
            // full pipe, and a write there sleeps.
            for (int i = 0; i < dropWithin && !stderrStalled && !firstDone; i++) {
                stderrStalled = std::ranges::any_of(threads(), writesToStderr);
                std::this_thread::yield();
            }
            stopFirst = true;
            // Ends: the first caller returns, or sleeps writing that line itself.
            bool firstHeld = false;
            for (int i = 0; i < dropWithin && stderrStalled && !(firstReturned = firstDone) && !firstHeld; i++) {
                firstHeld = firstTid != 0 && writesToStderr(std::to_string(firstTid));
                std::this_thread::yield();
            }

            std::atomic<bool> secondDone = false;
            std::optional<std::thread> second;
            if (stderrStalled) {
                openGate();
                auto const finished = [&](Wait const wait, const char * step) -> bool {
                    if (unanswered.empty() && wait == Wait::childEnded) {
                        unanswered = std::string("the child ended while ") + step + ": nothing could move it";
                    } else if (unanswered.empty() && wait == Wait::noAnswer) {
                        unanswered = std::string("no answer within unheldReadings readings while ") + step;
                    }
                    return wait == Wait::done;
                };
                drained = finished(
                    awaitUnheld(childPid, [&] -> bool { return contents(childOut).contains(drv("first-" + std::to_string(lastFirst))); }),
                    "the queue drained to the child");

                // A step with no answer leaves nothing the next could rely on.
                if (unanswered.empty()) {
                    second.emplace([&] -> void {
                        push.pushLog(drv("second"));
                        secondDone = true;
                    });
                    secondReturned = finished(awaitUnheld(childPid, [&] -> bool { return secondDone; }), "a second pushLog returned");
                }
                if (!unanswered.empty()) {
                    ender = std::jthread([&](const std::stop_token & stop) -> void {
                        while (!stop.stop_requested()) {
                            endChild(childPid);
                            std::this_thread::yield();
                        }
                    });
                }
            }

            // Read our stderr: the line's writer, and any caller held up
            // behind it, goes on; so does one that waits for the child.
            openGate();
            std::thread reader([&, readSide = err.readSide.get()] -> void { stalled = nix::drainFD(readSide); });
            first.join();
            if (second) {
                second->join();
            }
            dup2(ourStderr.get(), STDERR_FILENO);
            err.writeSide.close();
            reader.join();
        }
    }
    replayCaptured();
    dup2(ourStderr.get(), STDERR_FILENO);
    // The pipe was filled with NULs before the drop's line went to it.
    if (auto const start = stalled.find_first_not_of('\0'); start != std::string::npos) {
        static_cast<void>(std::fputs(stalled.substr(start).c_str(), stderr));
    }
    static_cast<void>(std::fputs(
        ("push-test: firstReturned=" + std::to_string(static_cast<int>(firstReturned))
         + " drained=" + std::to_string(static_cast<int>(drained))
         + " secondReturned=" + std::to_string(static_cast<int>(secondReturned)) + "\n")
            .c_str(),
        stderr));
    if (!unanswered.empty()) {
        static_cast<void>(std::fputs(("push-test: " + unanswered + "\n").c_str(), stderr));
    }
    assert(filled && "no write to the child slept within dropWithin logs");
    assert(gaveWay && "the queue never gave way: no build_log_dropped within dropWithin logs");
    assert(stderrStalled && "nothing slept writing to stderr within dropWithin readings");
    assert(firstReturned && "a pushLog that dropped a log waited behind a stalled stderr write");
    assert(unanswered.empty() && "a step that waits had no answer: the push-test: line before says which");
    assert(drained && "the log writer waited behind a stalled stderr write");
    assert(secondReturned && "a second pushLog waited behind a stalled stderr write");
    std::filesystem::remove_all(dir);
}

// A pushWait of ours, run on a thread of its own.
struct Call
{
    std::atomic<int> tid = 0;
    std::atomic<bool> done = false;
    std::atomic<bool> threw = false;
};

// A build's result waits for its paths' acks, and nothing it waits for
// writes to our stderr: not the caller that starts the child, nor the ack
// reader as an ack ends a build log's upload, nor the ack reader as the child
// exits, failing a build log's upload with it. With our stderr a full pipe
// that nobody reads from the start, and a child that acks each request, a
// pushWait must return, starting the child; so must one acked after a build
// log's ack; with a build log the child does not ack, a request the child
// exits on must fail its pushWait, and the next pushWait must start another
// child and return, while a process the exited child left behind still
// holds that child's stderr. A step is held up once a thread of ours other
// than the log writer is found asleep writing to our stderr, or the child has
// our stderr for its own (heldOnOurStderr): the defects the steps exist for,
// which a correct run never shows, however slow. The first step not done ends the steps:
// those after it would wait behind it. Then our stderr is read, which lets
// everything go, and every line logged must reach it: each child's start and
// exit, each build log's outcome, and each line the children logged.
//
// Known limits: as aStalledStderrHoldsUpOnlyItsWriter's, and a step without
// an answer leaves its caller to end with the child, which the test does not
// end for it.
void aStalledStderrHoldsUpNoPushWait()
{
    std::string unique = (std::filesystem::temp_directory_path() / "push-test-XXXXXX").string();
    if (mkdtemp(unique.data()) == nullptr) {
        throw std::system_error(errno, std::generic_category(), "mkdtemp");
    }
    auto const dir = std::filesystem::path(unique);
    auto const childPid = dir / "child-pid";
    auto const childIn = dir / "child-stdin";
    // The pids of processes the children leave holding their stderr, as
    // niks3's token command may outlive niks3.
    auto const holders = dir / "holders";
    nix::AutoCloseFD const ourStderr{fcntl(STDERR_FILENO, F_DUPFD_CLOEXEC, 0)}; // NOLINT(cppcoreguidelines-pro-type-vararg)
    assert(ourStderr);
    auto const path = [](const std::string & name) -> std::string {
        return "/nix/store/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-" + name;
    };
    bool started = false;
    bool afterLog = false;
    bool failedOnExit = false;
    bool restarted = false;
    std::string unanswered;
    std::string stalled;
    {
        // No assertion may fail while our stderr is this pipe: its message
        // would wait for room too.
        nix::Pipe err;
        err.create();
        {
            int const room = fcntl(err.writeSide.get(), F_GETPIPE_SZ); // NOLINT(cppcoreguidelines-pro-type-vararg)
            assert(room > 0);
            nix::writeFull(err.writeSide.get(), std::string(static_cast<size_t>(room), '\0'));
        }
        nixgrpc::flushLog(); // what was logged before goes where stderr was
        assert(dup2(err.writeSide.get(), STDERR_FILENO) != -1);
        std::thread reader;
        {
            // Each child writes its pid, records what it is sent, and acks
            // each request but one for a path ending in "-hold", which it
            // leaves, and one ending in "-exit", on which it exits, its last
            // line to its stderr without a newline. Each starts a process
            // that holds its stderr and outlives it, until the test kills it. Before each ack it logs
            // to its stderr, as niks3 push does a path it uploads.
            std::optional<nixgrpc::PushProcess> push;
            push.emplace(nixgrpc::PushProcess::Argv{
                "sh",
                "-c",
                R"(sleep 2147483647 > /dev/null & echo $! >> "$2"; echo $$ > "$0"; while IFS= read -r line; do printf '%s\n' "$line" >> "$1"; case $line in *-exit*) printf Leaving >&2; exit ;; *-hold*) continue ;; esac; echo Uploading >&2; id=${line#*\"id\":}; printf '{"id":%s,"status":"ok"}\n' "${id%%,*}"; done)",
                childPid.string(),
                childIn.string(),
                holders.string()});
            std::deque<Call> calls;
            std::vector<std::thread> callers;
            auto const call = [&](const std::string & name) -> Call & {
                Call & one = calls.emplace_back();
                callers.emplace_back([&push, &path, ours = &one, name] -> void {
                    ours->tid = gettid();
                    try {
                        push->pushWait({path(name)}, [] -> bool { return false; });
                    } catch (...) {
                        ours->threw = true;
                    }
                    ours->done = true;
                });
                return one;
            };
            auto const finished = [&](Wait const wait, const char * step) -> bool {
                if (unanswered.empty() && wait == Wait::childEnded) {
                    unanswered = std::string("the child ended while ") + step;
                } else if (unanswered.empty() && wait == Wait::noAnswer) {
                    unanswered = std::string("no answer within unheldReadings readings while ") + step;
                }
                return wait == Wait::done;
            };
            auto const returns = [&](Call & one, const char * step) -> bool {
                return finished(awaitUnheld(childPid, [&] -> bool { return one.done; }), step) && !one.threw;
            };

            // A build log, sent once the child has its request.
            auto const sendsLog = [&](const std::string & name) -> bool {
                push->pushLog(path(name));
                return finished(
                    awaitUnheld(childPid, [&] -> bool { return contents(childIn).contains(path(name)); }),
                    "a build log went to the child");
            };

            started = returns(call("first"), "a pushWait started the child");
            if (started) {
                // Its ack goes to the ack reader ahead of the next request's.
                afterLog = sendsLog("log.drv") && returns(call("after-log"), "a pushWait was acked after the build log");
            }
            if (afterLog && sendsLog("hold.drv")) {
                Call & exiting = call("exit");
                failedOnExit = finished(awaitUnheld(childPid, [&] -> bool { return exiting.done; }, /*childMayEnd=*/true), "the child exited")
                               && exiting.threw;
            }
            if (failedOnExit) {
                // The next child writes its own.
                std::filesystem::remove(childPid);
                restarted = returns(call("next"), "a pushWait started the next child");
            }

            reader = std::thread([&, readSide = err.readSide.get()] -> void { stalled = nix::drainFD(readSide); });
            // The processes left holding the children's stderr go: those
            // started so far, which a caller may wait for, then the rest.
            auto const killHolders = [&] -> void {
                std::istringstream pids(contents(holders));
                for (pid_t holder = 0; pids >> holder;) {
                    kill(holder, SIGKILL);
                }
            };
            killHolders();
            for (auto & caller : callers) {
                caller.join();
            }
            killHolders();
            push.reset();
        }
        // Every line logged goes to the pipe before our stderr is back.
        nixgrpc::flushLog();
        // The children had the pipe as their stderr too: they are gone.
        dup2(ourStderr.get(), STDERR_FILENO);
        err.writeSide.close();
        reader.join();
    }
    // The pipe was filled with NULs before anything of ours wrote to it.
    if (auto const start = stalled.find_first_not_of('\0'); start != std::string::npos) {
        static_cast<void>(std::fputs(stalled.substr(start).c_str(), stderr));
    }
    static_cast<void>(std::fputs(
        ("push-test: started=" + std::to_string(static_cast<int>(started)) + " afterLog="
         + std::to_string(static_cast<int>(afterLog)) + " failedOnExit=" + std::to_string(static_cast<int>(failedOnExit))
         + " restarted=" + std::to_string(static_cast<int>(restarted)) + "\n")
            .c_str(),
        stderr));
    if (!unanswered.empty()) {
        static_cast<void>(std::fputs(("push-test: " + unanswered + "\n").c_str(), stderr));
    }
    auto const times = [&](const std::string & line) -> size_t {
        size_t count = 0;
        for (size_t at = 0; (at = stalled.find(line, at)) != std::string::npos; at += line.size()) {
            count++;
        }
        return count;
    };
    // First: a step held up while the reader logged a failed read shows that line.
    assert(times("event=niks3_push_stderr_lost") == 0 && "a read of the child's stderr that found it empty was taken for a failure");
    assert(unanswered.empty() && "a step that waits had no answer: the push-test: line before says which");
    assert(started && "a pushWait that started the child waited behind a stalled stderr write");
    assert(afterLog && "a pushWait acked after a build log waited behind a stalled stderr write");
    assert(failedOnExit && "a pushWait whose child exited waited behind a stalled stderr write");
    assert(restarted && "a pushWait after the child exited waited behind a stalled stderr write");
    assert(times("event=niks3_push_started") == 2 && "a child's start is missing from our stderr");
    assert(times("event=build_log_pushed drv=" + path("log.drv")) == 1 && "a build log's upload is missing from our stderr");
    assert(times("event=build_log_push_failed drv=" + path("hold.drv")) == 1 && "a failed build log upload is missing from our stderr");
    assert(times("event=niks3_push_exited") == 2 && "a child's exit is missing from our stderr");
    assert(times("Uploading\n") == 4 && "a line the child logged is missing from our stderr");
    assert(times("Leaving\n") == 1 && "the child's last line, without its newline, is missing from our stderr");
    std::filesystem::remove_all(dir);
}

// What the child writes to its stderr just before it exits reaches ours:
// its stdout ends with its exit, and the copy goes on to the end of what is
// in its stderr's pipe. pushWait's wait asks `cancelled` under its
// request's lock, which the reader needs to take the child's ack. The child
// acks only once `cancelled` tells it to, through `goFifo`; so the reader is
// held in that ack while the child writes its last lines, more than one
// read takes but fewer than its pipe holds, and exits. `cancelled` returns
// once the child is a zombie: exited, and not reaped, so the reader has not
// gone past the ack. A copy that ended with the child's stdout would then
// lose all but a read of them.
void aChildsLastLinesArriveWhileTheReaderIsHeld()
{
    constexpr int lastLines = 4'000; // about 39 KB: more than a read of 4096, less than a pipe of 64 KiB
    constexpr int zombieReadings = 3;
    constexpr int writingReadings = 1'000;
    constexpr long steps = 50'000'000;
    std::string unique = (std::filesystem::temp_directory_path() / "push-test-XXXXXX").string();
    if (mkdtemp(unique.data()) == nullptr) {
        throw std::system_error(errno, std::generic_category(), "mkdtemp");
    }
    auto const dir = std::filesystem::path(unique);
    auto const file = dir / "stderr";
    auto const goFifo = dir / "go";
    auto const childPid = dir / "child-pid";
    assert(mkfifo(goFifo.c_str(), 0600) == 0);
    nix::AutoCloseFD const ourStderr{fcntl(STDERR_FILENO, F_DUPFD_CLOEXEC, 0)}; // NOLINT(cppcoreguidelines-pro-type-vararg)
    nix::AutoCloseFD const out{::open(file.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0600)}; // NOLINT(cppcoreguidelines-pro-type-vararg)
    assert(ourStderr && out);
    nixgrpc::flushLog(); // what was logged before goes where stderr was
    assert(dup2(out.get(), STDERR_FILENO) != -1);
    std::string status;
    bool asked = false;
    bool opened = false;
    bool held = false;
    bool writing = false;
    {
        nixgrpc::PushProcess push(nixgrpc::PushProcess::Argv{
            "sh",
            "-c",
            R"(echo $$ > "$1"; read -r line; read -r _ < "$2"; id=${line#*\"id\":}; printf '{"id":%s,"status":"ok"}\n' "${id%%,*}"; last=$(seq -f 'last %.0f' 0 "$0"); printf '%s\n' "$last" >&2; exit)",
            std::to_string(lastLines - 1),
            childPid.string(),
            goFifo.string()});
        auto const cancelled = [&] -> bool {
            if (asked) {
                return false;
            }
            asked = true;
            int gateFd = -1;
            for (long step = 0; step < steps && gateFd < 0; step++) {
                gateFd = ::open(goFifo.c_str(), O_WRONLY | O_NONBLOCK | O_CLOEXEC); // NOLINT(cppcoreguidelines-pro-type-vararg)
                if (gateFd < 0) {
                    std::this_thread::yield();
                }
            }
            if (gateFd < 0) {
                // The child never came to read the gate: it is ended, so
                // that the push fails rather than waits for it for good.
                if (auto const pid = pidIn(childPid)) {
                    static_cast<void>(::kill(std::stoi(*pid), SIGKILL));
                }
                return false;
            }
            opened = true;
            {
                nix::AutoCloseFD const gate{gateFd};
                nix::writeFull(gate.get(), "\n", false);
            }
            int zombie = 0;
            int sleepsWriting = 0;
            for (long step = 0; step < steps && zombie < zombieReadings && sleepsWriting < writingReadings; step++) {
                auto const pid = pidIn(childPid);
                char const state = pid ? processState(*pid) : '?';
                std::istringstream fields(pid ? firstLine("/proc/" + *pid + "/syscall") : std::string{});
                long number = -1;
                bool const inWrite = state == 'S' && (fields >> number) && number == SYS_write;
                zombie = state == 'Z' ? zombie + 1 : 0;
                sleepsWriting = inWrite ? sleepsWriting + 1 : 0;
                std::this_thread::yield();
            }
            held = zombie == zombieReadings;
            writing = sleepsWriting == writingReadings;
            return false;
        };
        try {
            push.pushWait({"/nix/store/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-held-lines"}, cancelled);
            status = "ok";
        } catch (std::exception & err) {
            status = err.what();
        }
    }
    nixgrpc::flushLog();
    assert(dup2(ourStderr.get(), STDERR_FILENO) != -1);
    auto const text = contents(file);
    std::filesystem::remove_all(dir);
    int next = 0;
    std::string_view const key = "last ";
    for (size_t at = 0; (at = text.find(key, at)) != std::string::npos; at += 1) {
        next += std::stoi(text.substr(at + key.size())) == next ? 1 : 0;
    }
    std::println("push-test: held lines: asked={} opened={} held={} writing={} status={} lines={}", asked, opened, held, writing, status, next);
    static_cast<void>(std::fflush(stdout)); // before an assertion ends us
    assert(opened && "the child never opened its gate within the steps");
    assert(asked && !writing && "the child's last lines did not fit its pipe while the reader was held");
    assert(held && "the child was not a zombie while the reader was held in its ack");
    assert(status == "ok");
    assert(next == lastLines && "a line the child wrote before it exited, while the reader was held, is missing from our stderr");
}

// A child that closes its stderr and goes on leaves the reader asleep: the
// pipe's end is read once, and the pipe polled no more. The child closes its
// stderr, says its pid, and waits for a request, then for `goFifo`. While
// pushWait asks `cancelled`, once the child has said its pid, a thread the
// push started must be found asleep, awakeReadings readings running; one
// that polled the ended pipe again would find it ended at once, and never
// sleep. Each wait, for the pid and for the sleep, is bounded by dropWithin
// readings.
void aStderrThatEndsFirstLeavesTheReaderAsleep()
{
    constexpr int awakeReadings = 1'000;
    std::string unique = (std::filesystem::temp_directory_path() / "push-test-XXXXXX").string();
    if (mkdtemp(unique.data()) == nullptr) {
        throw std::system_error(errno, std::generic_category(), "mkdtemp");
    }
    auto const dir = std::filesystem::path(unique);
    auto const goFifo = dir / "go";
    auto const childPid = dir / "child-pid";
    assert(mkfifo(goFifo.c_str(), 0600) == 0);
    auto const before = threads();
    std::string status;
    bool asleep = false;
    bool opened = false;
    {
        nixgrpc::PushProcess push(nixgrpc::PushProcess::Argv{
            "sh",
            "-c",
            R"(exec 2>&-; echo $$ > "$0"; read -r line; read -r _ < "$1"; id=${line#*\"id\":}; printf '{"id":%s,"status":"ok"}\n' "${id%%,*}"; read -r _)",
            childPid.string(),
            goFifo.string()});
        bool asked = false;
        auto const cancelled = [&] -> bool {
            if (asked) {
                return false;
            }
            asked = true;
            for (int step = 0; step < dropWithin && !pidIn(childPid); step++) {
                std::this_thread::yield();
            }
            int found = 0;
            for (int step = 0; pidIn(childPid) && step < dropWithin && found < awakeReadings; step++) {
                bool any = false;
                for (auto const & tid : threads()) {
                    any = any || (std::ranges::find(before, tid) == before.end() && task(tid).state == 'S');
                }
                found = any ? found + 1 : 0;
                std::this_thread::yield();
            }
            asleep = found == awakeReadings;
            int gateFd = -1;
            for (int step = 0; step < dropWithin && gateFd < 0; step++) {
                gateFd = ::open(goFifo.c_str(), O_WRONLY | O_NONBLOCK | O_CLOEXEC); // NOLINT(cppcoreguidelines-pro-type-vararg)
                if (gateFd < 0) {
                    std::this_thread::yield();
                }
            }
            if (gateFd < 0) {
                // The child never came to read the gate: it is ended, so
                // that the push fails rather than waits for it for good.
                if (auto const pid = pidIn(childPid)) {
                    static_cast<void>(::kill(std::stoi(*pid), SIGKILL));
                }
                return false;
            }
            opened = true;
            nix::AutoCloseFD const gate{gateFd};
            nix::writeFull(gate.get(), "\n", false);
            return false;
        };
        try {
            push.pushWait({"/nix/store/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-stderr-ended"}, cancelled);
            status = "ok";
        } catch (std::exception & err) {
            status = err.what();
        }
    }
    std::filesystem::remove_all(dir);
    std::println("push-test: stderr ended first: opened={} asleep={} status={}", opened, asleep, status);
    static_cast<void>(std::fflush(stdout)); // before an assertion ends us
    assert(opened && "the child never opened its gate within the steps");
    assert(status == "ok");
    assert(asleep && "the reader polled the child's ended stderr without sleeping");
}

// The test's own hold on the child's stderr pipe, for
// aWriterLeftHoldingStderrIsCopiedNoFurther's hook.
int heldStderr = -1; // NOLINT(cppcoreguidelines-avoid-non-const-global-variables): written by the test, read by the reader's hook
// The bytes that hook wrote into the pipe; less than its lines, or none, and
// the test tells nothing.
std::atomic<long> lateWritten = -1; // NOLINT(cppcoreguidelines-avoid-non-const-global-variables): written by the reader's hook

// A process the child leaves holding its stderr (niks3's token command
// inherits niks3's stderr and may outlive it) has what it wrote by the
// child's reaping copied, and no more: the copy after the reaping, under the
// State lock, ends at what the pipe held then. The test holds the pipe
// itself, opened from the child's fd 2 while the child waits on `goFifo`,
// and plays that process: once the child is reaped and what the pipe holds
// is measured (PushProcess::afterStderrMeasured), it writes lateLines
// lines there, and must have written them all. The child acks once `cancelled` opens the gate, which holds
// the reader in that ack, as in aChildsLastLinesArriveWhileTheReaderIsHeld;
// then it writes its own lines, earlyLines of them, more than a read takes,
// and exits, and `cancelled` returns once it is a zombie. So the pipe holds
// some of them when it is measured. Every early line must reach our stderr,
// and no late one.
void aWriterLeftHoldingStderrIsCopiedNoFurther()
{
    constexpr int earlyLines = 1000; // about 9 KB: more than a read of 4096
    constexpr int lateLines = 10;
    std::string unique = (std::filesystem::temp_directory_path() / "push-test-XXXXXX").string();
    if (mkdtemp(unique.data()) == nullptr) {
        throw std::system_error(errno, std::generic_category(), "mkdtemp");
    }
    auto const dir = std::filesystem::path(unique);
    auto const file = dir / "stderr";
    auto const goFifo = dir / "go";
    auto const childPid = dir / "child-pid";
    assert(mkfifo(goFifo.c_str(), 0600) == 0);
    nix::AutoCloseFD const ourStderr{fcntl(STDERR_FILENO, F_DUPFD_CLOEXEC, 0)}; // NOLINT(cppcoreguidelines-pro-type-vararg)
    nix::AutoCloseFD const out{::open(file.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0600)}; // NOLINT(cppcoreguidelines-pro-type-vararg)
    assert(ourStderr && out);
    nixgrpc::flushLog(); // what was logged before goes where stderr was
    assert(dup2(out.get(), STDERR_FILENO) != -1);
    std::string status;
    bool opened = false;
    bool held = false;
    {
        nixgrpc::PushProcess push(nixgrpc::PushProcess::Argv{
            "sh",
            "-c",
            R"(echo $$ > "$0"; read -r line; read -r _ < "$1"; id=${line#*\"id\":}; printf '{"id":%s,"status":"ok"}\n' "${id%%,*}"; seq -f 'early %.0f' 0 "$2" >&2; exit)",
            childPid.string(),
            goFifo.string(),
            std::to_string(earlyLines - 1)});
        bool asked = false;
        auto const cancelled = [&] -> bool {
            if (asked) {
                return false;
            }
            asked = true;
            for (int step = 0; step < dropWithin && !pidIn(childPid); step++) {
                std::this_thread::yield();
            }
            if (auto const pid = pidIn(childPid)) {
                heldStderr = ::open(("/proc/" + *pid + "/fd/2").c_str(), O_WRONLY | O_NONBLOCK | O_CLOEXEC); // NOLINT(cppcoreguidelines-pro-type-vararg)
            }
            held = heldStderr >= 0;
            int gateFd = -1;
            for (int step = 0; step < dropWithin && gateFd < 0; step++) {
                gateFd = ::open(goFifo.c_str(), O_WRONLY | O_NONBLOCK | O_CLOEXEC); // NOLINT(cppcoreguidelines-pro-type-vararg)
                if (gateFd < 0) {
                    std::this_thread::yield();
                }
            }
            if (gateFd < 0) {
                // The child never came to read the gate: it is ended, so
                // that the push fails rather than waits for it for good.
                if (auto const pid = pidIn(childPid)) {
                    static_cast<void>(::kill(std::stoi(*pid), SIGKILL));
                }
                return false;
            }
            opened = true;
            {
                nix::AutoCloseFD const gate{gateFd};
                nix::writeFull(gate.get(), "\n", false);
            }
            // A zombie: exited, its lines written, and not reaped, so the
            // reader is still held. It exits once it has written them.
            for (auto const pid = pidIn(childPid); pid && processState(*pid) != 'Z';) {
                std::this_thread::yield();
            }
            return false;
        };
        nixgrpc::PushProcess::afterStderrMeasured = [] -> void {
            std::string late;
            for (int seq = 0; seq < lateLines; seq++) {
                late += "late " + std::to_string(seq) + "\n";
            }
            lateWritten = ::write(heldStderr, late.data(), late.size());
        };
        try {
            push.pushWait({"/nix/store/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-writer-left"}, cancelled);
            status = "ok";
        } catch (std::exception & err) {
            status = err.what();
        }
    }
    nixgrpc::PushProcess::afterStderrMeasured = nullptr;
    long const wroteLate = lateWritten.exchange(-1);
    if (heldStderr >= 0) {
        ::close(heldStderr);
        heldStderr = -1;
    }
    nixgrpc::flushLog();
    assert(dup2(ourStderr.get(), STDERR_FILENO) != -1);
    auto const text = contents(file);
    std::filesystem::remove_all(dir);
    auto const count = [&](std::string_view key) -> int {
        int found = 0;
        for (size_t at = 0; (at = text.find(key, at)) != std::string::npos; at += key.size()) {
            found++;
        }
        return found;
    };
    int const early = count("early ");
    int const late = count("late ");
    std::string lateText;
    for (int seq = 0; seq < lateLines; seq++) {
        lateText += "late " + std::to_string(seq) + "\n";
    }
    std::println(
        "push-test: a writer left holding stderr: opened={} held={} status={} wroteLate={} early={} late={}",
        opened, held, status, wroteLate, early, late);
    static_cast<void>(std::fflush(stdout)); // before an assertion ends us
    assert(opened && held && "the child's stderr was not held, or its gate not opened, within the steps");
    assert(std::cmp_equal(wroteLate, lateText.size()) && "the late lines were not written into the pipe after it was measured");
    assert(status == "ok");
    assert(early == earlyLines && "a line the child wrote before it was reaped is missing from our stderr");
    assert(late == 0 && "the reader copied on past what the pipe held when the child was reaped");
}

// Two threads start, and a third does not: the limit on tasks counts this
// process's own, from one.
auto twoMoreTasksAndNoThird() -> bool
{
    nix::Pipe hold;
    hold.create();
    auto const sit = [held = hold.readSide.get()] -> void {
        char byte = 0;
        static_cast<void>(::read(held, &byte, 1));
    };
    std::vector<std::thread> sitting;
    bool third = true;
    try {
        sitting.emplace_back(sit);
        sitting.emplace_back(sit);
        try {
            std::thread([] -> void {}).join();
        } catch (std::system_error &) {
            third = false;
        }
    } catch (std::system_error &) {
        third = true; // fewer than two: not the limit this test takes
    }
    hold.writeSide.close();
    for (auto & one : sitting) {
        one.join();
    }
    // A joined thread counts until the kernel lets it go, gone from
    // /proc/self/task.
    constexpr int steps = 1'000'000;
    for (int step = 0; step < steps && threads().size() > 1; step++) {
        std::this_thread::yield();
    }
    return sitting.size() == 2 && !third;
}

// Every thread of the process `pid` asleep on a futex: one that waits on a
// lock or a condition no thread of it will release. A process with a thread
// asleep in anything else, a reader in poll, say, is never read so, though
// it may wait for good too.
auto allOnAFutex(pid_t pid) -> bool
{
    auto const dir = "/proc/" + std::to_string(pid) + "/task";
    std::error_code error;
    bool any = false;
    for (std::filesystem::directory_iterator it(dir, error), end; !error && it != end; it.increment(error)) {
        auto const tid = dir + "/" + it->path().filename().string();
        std::string const line = firstLine(tid + "/stat");
        auto const close = line.rfind(')');
        std::istringstream fields(firstLine(tid + "/syscall"));
        long number = -1;
        if (close == std::string::npos || close + 2 >= line.size() || line.at(close + 2) != 'S' || !(fields >> number) || number != SYS_futex) {
            return false;
        }
        any = true;
    }
    return any && !error;
}

// Waits for the child `pid` of the test by readings: one found with every
// thread asleep on a futex stuckReadings readings running waits for good,
// and is killed. Whether it exited of itself.
auto exitedUnstuck(pid_t pid, int & status) -> bool
{
    constexpr int stuckReadings = 100;
    bool exited = false;
    for (int asleep = 0; !exited && asleep < stuckReadings;) {
        exited = waitpid(pid, &status, WNOHANG) == pid;
        asleep = !exited && allOnAFutex(pid) ? asleep + 1 : 0;
        std::this_thread::yield();
    }
    if (!exited) {
        kill(pid, SIGKILL);
        waitpid(pid, &status, 0);
    }
    return exited;
}

// At a limit of threads (EAGAIN) a push works on: one thread reads the
// child's acks and its stderr, and the push needs no other; and where that
// reader cannot start, the push fails to its caller and leaves no child
// behind, so the next push starts another. A child of the test, in a user
// namespace of its own, where the limit on its user's tasks counts its own,
// is first checked to have room for two more tasks and no third, or the
// test is skipped (exit 77). One task short of the reader, its pushWait must
// fail, and no child of it may be left; at three tasks (itself, the push
// child and the reader) the next pushWait must return, and the push child's
// line must reach its stderr. The test waits for its child as exitedUnstuck
// does: one whose push has no reader, waiting for an ack, is found so; one
// that waits for good with its reader asleep in poll is not, and hangs the
// test.
auto aPushAtTheLimitOfThreadsWorks() -> bool
{
    constexpr int returned = 0;
    constexpr int setupFailed = 2;
    constexpr int secondFailed = 3;
    constexpr int childLeft = 4;
    constexpr int firstReturned = 5;
    constexpr int skipped = 77;
    constexpr rlim_t tasks = 3;
    std::string unique = (std::filesystem::temp_directory_path() / "push-test-XXXXXX").string();
    if (mkdtemp(unique.data()) == nullptr) {
        throw std::system_error(errno, std::generic_category(), "mkdtemp");
    }
    auto const file = std::filesystem::path(unique) / "stderr";
    nixgrpc::flushLog();
    static_cast<void>(std::fflush(stdout)); // a child that exits would write it again
    pid_t const child = fork();
    assert(child >= 0);
    if (child == 0) {
        int const out = ::open(file.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0600); // NOLINT(cppcoreguidelines-pro-type-vararg)
        if (out < 0 || dup2(out, STDERR_FILENO) < 0) {
            std::_Exit(setupFailed);
        }
        rlimit const limit{.rlim_cur = tasks, .rlim_max = tasks};
        if (unshare(CLONE_NEWUSER) != 0 || setrlimit(RLIMIT_NPROC, &limit) != 0 || !twoMoreTasksAndNoThird()) {
            std::_Exit(skipped);
        }
        rlimit const noReader{.rlim_cur = tasks - 1, .rlim_max = tasks};
        if (setrlimit(RLIMIT_NPROC, &noReader) != 0) {
            std::_Exit(setupFailed);
        }
        nixgrpc::PushProcess push(nixgrpc::PushProcess::Argv{
            "sh",
            "-c",
            R"(while read -r line; do echo Uploading >&2; id=${line#*\"id\":}; printf '{"id":%s,"status":"ok"}\n' "${id%%,*}"; done)"});
        bool firstFailed = false;
        try {
            push.pushWait({"/nix/store/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-no-reader"}, [] -> bool { return false; });
        } catch (std::system_error &) {
            firstFailed = true;
        }
        if (!firstFailed) {
            std::_Exit(firstReturned);
        }
        if (waitpid(-1, nullptr, WNOHANG) != -1 || errno != ECHILD) {
            std::_Exit(childLeft);
        }
        if (setrlimit(RLIMIT_NPROC, &limit) != 0) {
            std::_Exit(setupFailed);
        }
        try {
            push.pushWait({"/nix/store/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-at-the-limit"}, [] -> bool { return false; });
        } catch (std::exception &) {
            std::_Exit(secondFailed);
        }
        nixgrpc::exitLogged(returned);
    }
    int status = 0;
    bool const exited = exitedUnstuck(child, status);
    auto const text = contents(file);
    std::filesystem::remove_all(unique);
    int const code = WIFEXITED(status) ? WEXITSTATUS(status) : -1;
    if (code == skipped) {
        std::println("push-test: at the limit of threads: skipped, no user namespace or limit as taken");
        return false;
    }
    std::println("push-test: at the limit of threads: stuck={} status={:#x} uploading={}", !exited, status, text.contains("Uploading"));
    static_cast<void>(std::fflush(stdout)); // before an assertion ends us
    assert(exited && "a child of the test waited for good");
    assert(code != firstReturned && "a push one task short of its reader returned");
    assert(code != childLeft && "a push whose reader could not start left its child");
    assert(code == returned && "a push at the limit of threads did not return");
    assert(text.contains("Uploading") && "the push child's line at the limit of threads is missing");
    return true;
}

// A build log at a limit of threads fails as any upload of one does, said on
// our stderr: the thread that sends build logs goes on, and the process with
// it. A child of the test, as in aPushAtTheLimitOfThreadsWorks, has room for
// three tasks: itself, the thread that sends build logs, and the push child,
// and none for the reader. It sends a build log, waits by readings for its
// failure on its stderr, and exits; it must exit 0, with the failure there,
// not die of the exception that left the sending thread.
auto aBuildLogAtTheLimitOfThreadsFails() -> bool
{
    constexpr int sent = 0;
    constexpr int setupFailed = 2;
    constexpr int noFailure = 3;
    constexpr int skipped = 77;
    constexpr rlim_t tasks = 3;
    std::string unique = (std::filesystem::temp_directory_path() / "push-test-XXXXXX").string();
    if (mkdtemp(unique.data()) == nullptr) {
        throw std::system_error(errno, std::generic_category(), "mkdtemp");
    }
    auto const file = std::filesystem::path(unique) / "stderr";
    auto const drvPath = std::string("/nix/store/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-log-at-the-limit.drv");
    nixgrpc::flushLog();
    static_cast<void>(std::fflush(stdout)); // a child that exits would write it again
    pid_t const child = fork();
    assert(child >= 0);
    if (child == 0) {
        int const out = ::open(file.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0600); // NOLINT(cppcoreguidelines-pro-type-vararg)
        if (out < 0 || dup2(out, STDERR_FILENO) < 0) {
            std::_Exit(setupFailed);
        }
        rlimit const limit{.rlim_cur = tasks, .rlim_max = tasks};
        if (unshare(CLONE_NEWUSER) != 0 || setrlimit(RLIMIT_NPROC, &limit) != 0 || !twoMoreTasksAndNoThird()) {
            std::_Exit(skipped);
        }
        bool failed = false;
        {
            nixgrpc::PushProcess push(nixgrpc::PushProcess::Argv{"sh", "-c", "while read -r line; do :; done"});
            push.pushLog(drvPath);
            for (int step = 0; step < dropWithin && !failed; step++) {
                failed = contents(file).contains("event=build_log_push_failed drv=" + drvPath);
                std::this_thread::yield();
            }
        }
        nixgrpc::exitLogged(failed ? sent : noFailure);
    }
    int status = 0;
    bool const exited = exitedUnstuck(child, status);
    auto const text = contents(file);
    std::filesystem::remove_all(unique);
    int const code = WIFEXITED(status) ? WEXITSTATUS(status) : -1;
    if (code == skipped) {
        std::println("push-test: a build log at the limit of threads: skipped, no user namespace or limit as taken");
        return false;
    }
    std::println("push-test: a build log at the limit of threads: stuck={} status={:#x}", !exited, status);
    static_cast<void>(std::fflush(stdout)); // before an assertion ends us
    assert(exited && "a child of the test waited for good");
    assert(!WIFSIGNALED(status) && "a build log at the limit of threads ended the process");
    assert(code == sent && text.contains("event=build_log_push_failed drv=" + drvPath) && "a build log at the limit of threads did not fail on our stderr");
    return true;
}

} // namespace

// Nix's startProcess() never frees the logger it makes for the child.
extern "C" auto __lsan_default_suppressions() -> const char * // NOLINT(bugprone-reserved-identifier,cert-dcl37-c,cert-dcl51-cpp)
{
    return "leak:libnixutil";
}

// With "at-the-limit", only the tests at a limit of threads, its own test
// (exit 77 where both are skipped); otherwise every other test.
auto main(int argc, char ** argv) -> int // NOLINT(misc-const-correctness): main's own signature
try {
    // As in the daemon: a child that is gone fails the write.
    (void) ::signal(SIGPIPE, SIG_IGN); // NOLINT(misc-include-cleaner): its header differs per platform
    // A crash replays the captured stderr: an abort, std::terminate's among
    // them (its own handler has printed the exception there), or a fault. A
    // signal a sanitizer already handles is left to it; its death callback
    // replays after its report.
    for (int const signal : {SIGABRT, SIGSEGV, SIGBUS, SIGFPE, SIGILL, SIGTRAP}) {
        struct sigaction current{};
        if (sigaction(signal, nullptr, &current) == 0 && current.sa_handler == SIG_DFL) {
            static_cast<void>(std::signal(signal, replayOnAbort));
        }
    }
    if (__sanitizer_set_death_callback != nullptr) {
        __sanitizer_set_death_callback(replayCaptured);
    }
    if (argc == 2 && std::string_view(argv[1]) == "at-the-limit") { // NOLINT(cppcoreguidelines-pro-bounds-pointer-arithmetic)
        constexpr int skippedTest = 77;
        bool const pushRan = aPushAtTheLimitOfThreadsWorks();
        bool const logRan = aBuildLogAtTheLimitOfThreadsFails();
        return pushRan || logRan ? 0 : skippedTest;
    }
    aLogDoesNotWaitForTheChild();
    aStalledStderrHoldsUpOnlyItsWriter();
    aStalledStderrHoldsUpNoPushWait();
    aChildsLastLinesArriveWhileTheReaderIsHeld();
    aStderrThatEndsFirstLeavesTheReaderAsleep();
    aWriterLeftHoldingStderrIsCopiedNoFurther();
    return 0;
} catch (std::exception & err) {
    replayCaptured();
    static_cast<void>(std::fputs((std::string("push-test: ") + err.what() + "\n").c_str(), stderr));
    return 1;
}
