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
#include <filesystem>
#include <ios>
#include <optional>
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
#include <sys/poll.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <unistd.h>

#include <nix/util/file-descriptor.hh>

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

// Nothing but `self` can move: every other thread of ours sleeps, none on a
// write to the child, and the child sleeps too, reading what we send it.
auto settled(const std::string & self, const std::optional<std::string> & child) -> bool
{
    return child && processState(*child) == 'S' && std::ranges::all_of(threads(), [&](const std::string & tid) -> bool {
        if (tid == self) {
            return true;
        }
        auto const sleeping = task(tid);
        if (sleeping.state != 'S') {
            return false;
        }
        auto const descriptor = sleepsWritingTo(sleeping);
        return !descriptor || *descriptor <= STDERR_FILENO || !isPipe(*descriptor);
    });
}

// How a step that waits for another thread ended.
enum class Wait : std::uint8_t
{
    done,
    heldUp,
    // The child is gone: nothing of ours can be read as held up behind it,
    // and nothing it was to do will happen.
    childEnded,
    // dropWithin readings found the step neither done nor held up.
    noAnswer,
};

// Reads until `done` holds, or until nothing but `self` can move three
// readings running with `steady` holding at each: then the step is held up.
// Each reading takes settled before `done`: a thread or child that moved in
// between reads as unsettled then, or `done` sees what it did.
template<typename Done, typename Steady>
auto await(const std::string & self, const std::filesystem::path & childPid, Done done, Steady steady) -> Wait
{
    constexpr int sameAnswers = 3;
    int answers = 0;
    for (int i = 0; i < dropWithin; i++) {
        auto const child = pidIn(childPid);
        bool const still = settled(self, child);
        if (done()) {
            return Wait::done;
        }
        // Exited, reaped or not, or killed: '?' is a pid that no longer reads.
        if (child && std::string_view("ZXx?").contains(processState(*child))) {
            return Wait::childEnded;
        }
        answers = still && steady() ? answers + 1 : 0;
        if (answers == sameAnswers) {
            return Wait::heldUp;
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
// every logLine does, and nothing else: not the log writer, nor another
// pushLog. With the queue full and the log writer held up by the child, a
// first caller drops the oldest log while our stderr is a full pipe that
// nobody reads, and sleeps writing that it did. Then the child reads all it
// is sent, and the queue must drain to it, and a second pushLog must
// return. A queue that stays put while nothing of ours or the child's can
// move, read three times running with the child's output unchanged, is held
// up; so is a second caller asleep while every other thread is. Only then
// is our stderr read, which lets the first caller go. Each step that waits
// for the next gives up after dropWithin readings and says which, and fails
// at once, saying so, if the child has ended: then nothing can move, and
// the child is not there to be read as settled. After a step without an
// answer every child PushProcess starts is ended until it has ended, so that
// it can; however the test is left, the gate opens before PushProcess ends.
//
// Known limits: a pushLog that spins, or sleeps on something other than our
// stderr or the child, is never read as held up; its step has no answer
// after dropWithin readings, and the test then hangs joining it instead of
// failing. An exception while our stderr is the full pipe ends the test in
// std::terminate, whose message waits for stderr behind the first caller's
// write, which waits for room: the test hangs.
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
    bool firstStalled = false;
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
            assert(dup2(err.writeSide.get(), STDERR_FILENO) != -1);

            std::atomic<int> firstTid = 0;
            std::thread first([&] -> void {
                firstTid = gettid();
                push.pushLog(drv("first"));
            });
            for (int i = 0; i < dropWithin && !firstStalled; i++) {
                firstStalled = firstTid != 0 && sleepsWritingTo(task(std::to_string(firstTid))) == STDERR_FILENO;
                std::this_thread::yield();
            }

            std::atomic<int> secondTid = 0;
            std::atomic<bool> secondDone = false;
            std::optional<std::thread> second;
            if (firstStalled) {
                openGate();
                std::string const self = std::to_string(gettid());
                auto const finished = [&](Wait const wait, const char * step) -> bool {
                    if (unanswered.empty() && wait == Wait::childEnded) {
                        unanswered = std::string("the child ended while ") + step + ": nothing could move it";
                    } else if (unanswered.empty() && wait == Wait::noAnswer) {
                        unanswered = std::string("no answer within dropWithin readings while ") + step;
                    }
                    return wait == Wait::done;
                };
                std::string sent;
                size_t lastSize = 0;
                drained = finished(
                    await(
                        self,
                        childPid,
                        [&] -> bool {
                            sent = contents(childOut);
                            return sent.contains(drv("first"));
                        },
                        [&] -> bool {
                            bool const same = sent.size() == lastSize;
                            lastSize = sent.size();
                            return same;
                        }),
                    "the queue drained to the child");

                // A step with no answer leaves nothing the next could rely on.
                if (unanswered.empty()) {
                    second.emplace([&] -> void {
                        secondTid = gettid();
                        push.pushLog(drv("second"));
                        secondDone = true;
                    });
                    secondReturned = finished(
                        await(
                            self, childPid, [&] -> bool { return secondDone; }, [&] -> bool { return secondTid != 0; }),
                        "a second pushLog returned");
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

            // Read our stderr: the first caller, and any caller held up
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
    // The pipe was filled with NULs before the first caller wrote to it.
    if (auto const start = stalled.find_first_not_of('\0'); start != std::string::npos) {
        static_cast<void>(std::fputs(stalled.substr(start).c_str(), stderr));
    }
    static_cast<void>(std::fputs(
        ("push-test: drained=" + std::to_string(static_cast<int>(drained))
         + " secondReturned=" + std::to_string(static_cast<int>(secondReturned)) + "\n")
            .c_str(),
        stderr));
    if (!unanswered.empty()) {
        static_cast<void>(std::fputs(("push-test: " + unanswered + "\n").c_str(), stderr));
    }
    assert(filled && "no write to the child slept within dropWithin logs");
    assert(gaveWay && "the queue never gave way: no build_log_dropped within dropWithin logs");
    assert(firstStalled && "the first caller did not sleep writing to stderr within dropWithin readings");
    assert(unanswered.empty() && "a step that waits had no answer: the push-test: line before says which");
    assert(drained && "the log writer waited behind a stalled stderr write");
    assert(secondReturned && "a second pushLog waited behind a stalled stderr write");
    std::filesystem::remove_all(dir);
}

} // namespace

// Nix's startProcess() never frees the logger it makes for the child.
extern "C" auto __lsan_default_suppressions() -> const char * // NOLINT(bugprone-reserved-identifier,cert-dcl37-c,cert-dcl51-cpp)
{
    return "leak:libnixutil";
}

auto main() -> int
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
    aLogDoesNotWaitForTheChild();
    aStalledStderrHoldsUpOnlyItsWriter();
    return 0;
} catch (std::exception & err) {
    replayCaptured();
    static_cast<void>(std::fputs((std::string("push-test: ") + err.what() + "\n").c_str(), stderr));
    return 1;
}
