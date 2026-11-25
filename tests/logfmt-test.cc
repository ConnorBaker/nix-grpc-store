// logLine queues a line for our stderr and returns: a write there that its
// reader holds up holds up the thread that writes, not the one that logs. The
// lines a thread logs go out in the order it logged them, and the process
// does not leave without them: not at a normal exit, nor through exitLogged
// (the shutdown watchdog's way out, and every other _Exit of the daemon's),
// nor on a fatal signal.

#include <algorithm>
#include <array>
#include <atomic>
#include <cassert>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <ctime>
#include <exception>
#include <filesystem>
#include <fstream>
#include <ios>
#include <iterator>
#include <map>
#include <optional>
#include <print>
#include <sstream>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <vector>

#include <fcntl.h>
#include <stdlib.h> // NOLINT(modernize-deprecated-headers): mkdtemp is POSIX, not in <cstdlib>.
#include <linux/prctl.h>
#include <sys/prctl.h>
#include <sys/mman.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <time.h> // NOLINT(modernize-deprecated-headers): tzset is POSIX, not in <ctime>.
#include <unistd.h>

#include <signal.h> // NOLINT(modernize-deprecated-headers): kill is POSIX, not in <csignal>.

#include "logfmt.hh"

namespace {

// Waits for another process are bounded by this many readings, not a clock.
constexpr int steps = 1'000'000;
constexpr int linesPerThread = 1000;
constexpr int threadsLogging = 4;
constexpr size_t chunkBytes = 4096;
constexpr int stopRounds = 1000;

// Set in a child of the test that is to log once it has begun to leave.
std::atomic<bool> logWhileLeaving = false; // NOLINT(cppcoreguidelines-avoid-non-const-global-variables): set by a child, read as it leaves
// A thread of a child of the test, joined once the log writer is.
std::thread joinWhileLeaving; // NOLINT(cppcoreguidelines-avoid-non-const-global-variables): started by a child, joined as it leaves

auto firstLine(const std::string & path) -> std::string
{
    std::ifstream file(path);
    std::string line;
    std::getline(file, line);
    return line;
}

// A task's state, or '?' once it is gone.
auto state(const std::string & task) -> char
{
    auto const line = firstLine(task + "/stat");
    auto const end = line.rfind(')');
    return end != std::string::npos && end + 2 < line.size() ? line.at(end + 2) : '?';
}

auto writesToStderr(const std::string & task) -> bool
{
    std::istringstream fields(firstLine(task + "/syscall"));
    long number = -1;
    long descriptor = -1;
    return state(task) == 'S' && fields >> number >> std::hex >> descriptor && number == SYS_write
           && descriptor == STDERR_FILENO;
}

auto waitsOnAFutex(const std::string & task) -> bool
{
    std::istringstream fields(firstLine(task + "/syscall"));
    long number = -1;
    return state(task) == 'S' && fields >> number && number == SYS_futex;
}

auto voluntarySwitches(const std::string & task) -> long
{
    std::ifstream status(task + "/status");
    std::string_view const key = "voluntary_ctxt_switches:";
    for (std::string line; std::getline(status, line);) {
        if (line.starts_with(key)) {
            return std::stol(line.substr(key.size()));
        }
    }
    return -1;
}

auto textOf(const std::filesystem::path & file) -> std::string
{
    std::ifstream stream(file);
    return {std::istreambuf_iterator<char>(stream), std::istreambuf_iterator<char>()};
}

auto freshDir() -> std::filesystem::path
{
    std::string unique = (std::filesystem::temp_directory_path() / "logfmt-test-XXXXXX").string();
    assert(mkdtemp(unique.data()) != nullptr);
    return unique;
}

auto tasks(pid_t pid) -> std::vector<std::string> // NOLINT(misc-include-cleaner): pid_t comes from <sys/types.h>.
{
    std::vector<std::string> found;
    std::error_code error;
    auto const dir = "/proc/" + std::to_string(pid) + "/task";
    for (std::filesystem::directory_iterator it(dir, error), end; !error && it != end; it.increment(error)) {
        found.push_back(it->path().string());
    }
    return found;
}

// Each of a few threads logs its lines numbered in order; on our stderr, a
// file, each thread's lines must come in that order. How the threads' lines
// interleave is not asked: the order they were queued in, which no caller
// chose.
void aThreadsLinesKeepTheirOrder()
{
    std::string unique = (std::filesystem::temp_directory_path() / "logfmt-test-XXXXXX").string();
    assert(mkdtemp(unique.data()) != nullptr);
    auto const file = std::filesystem::path(unique) / "stderr";
    nixgrpc::flushLog();
    int const ourStderr = fcntl(STDERR_FILENO, F_DUPFD_CLOEXEC, 0); // NOLINT(cppcoreguidelines-pro-type-vararg)
    int const out = open(file.c_str(), O_WRONLY | O_CREAT | O_CLOEXEC, 0600); // NOLINT(cppcoreguidelines-pro-type-vararg)
    assert(ourStderr >= 0 && out >= 0 && dup2(out, STDERR_FILENO) >= 0);
    {
        std::vector<std::jthread> threads;
        threads.reserve(threadsLogging);
        for (int thread = 0; thread < threadsLogging; thread++) {
            threads.emplace_back([thread] -> void {
                for (int seq = 0; seq < linesPerThread; seq++) {
                    nixgrpc::logLine(
                        nixgrpc::LogLevel::info,
                        {{"event", "ordered"}, {"thread", std::to_string(thread)}, {"n", std::to_string(seq)}});
                }
            });
        }
    }
    nixgrpc::flushLog();
    dup2(ourStderr, STDERR_FILENO);
    close(out);
    close(ourStderr);
    std::ifstream lines(file);
    std::map<std::string, int> next;
    int count = 0;
    for (std::string line; std::getline(lines, line);) {
        std::string_view const threadKey = " thread=";
        std::string_view const seqKey = " n=";
        auto const thread = line.substr(line.find(threadKey) + threadKey.size(), 1);
        auto const seq = std::stoi(line.substr(line.find(seqKey) + seqKey.size()));
        assert(seq == next[thread] && "a thread's lines came out of the order it logged them in");
        next[thread] = seq + 1;
        count++;
    }
    assert(count == threadsLogging * linesPerThread && "a line logged is missing");
    std::filesystem::remove_all(unique);
}

// The sandbox wants the process to have one thread when it applies: a child
// of the test, which has only the threads it starts, logs, which starts the
// log writer, then lets the writer go: it must then have one thread, and a
// line it logs after must reach its stderr, as must the one before. Many
// rounds of it: a joined thread is listed a while after its join, seldom
// long enough for one round to see. A child whose every thread is found
// asleep on a futex, stuckReadings readings running, waits to join a writer
// that was never told to go.
void aStoppedWriterLeavesOneThread()
{
    std::string unique = (std::filesystem::temp_directory_path() / "logfmt-test-XXXXXX").string();
    assert(mkdtemp(unique.data()) != nullptr);
    auto const file = std::filesystem::path(unique) / "stderr";
    nixgrpc::flushLog();
    static_cast<void>(std::fflush(stdout)); // a child that exits would write it again
    pid_t const child = fork();
    assert(child >= 0);
    if (child == 0) {
        int const out = open(file.c_str(), O_WRONLY | O_CREAT | O_CLOEXEC, 0600); // NOLINT(cppcoreguidelines-pro-type-vararg)
        if (out < 0 || dup2(out, STDERR_FILENO) < 0) {
            std::_Exit(2);
        }
        size_t threads = 1;
        for (int round = 0; round < stopRounds && threads == 1; round++) {
            nixgrpc::logLine(nixgrpc::LogLevel::info, {{"event", "before"}});
            nixgrpc::stopLogWriter();
            threads = tasks(getpid()).size();
        }
        nixgrpc::logLine(nixgrpc::LogLevel::info, {{"event", "after"}});
        nixgrpc::flushLog();
        std::_Exit(threads == 1 ? 0 : 1);
    }
    constexpr int stuckReadings = 100;
    int status = 0;
    bool exited = false;
    int asleep = 0;
    while (!exited && asleep < stuckReadings) {
        exited = waitpid(child, &status, WNOHANG) == child;
        auto const all = exited ? std::vector<std::string>{} : tasks(child);
        bool const still = !all.empty() && std::ranges::all_of(all, [](const std::string & task) -> bool { return waitsOnAFutex(task); });
        asleep = still ? asleep + 1 : 0;
        std::this_thread::yield();
    }
    if (!exited) {
        kill(child, SIGKILL);
        waitpid(child, &status, 0);
    }
    std::ifstream lines(file);
    std::string const text{std::istreambuf_iterator<char>(lines), std::istreambuf_iterator<char>()};
    std::println(
        "logfmt-test: stopped writer: stuck={} status={:#x} before={} after={}",
        !exited,
        status,
        text.contains("event=before"),
        text.contains("event=after"));
    static_cast<void>(std::fflush(stdout)); // before an assertion ends us
    assert(exited && "letting the log writer go waited for a writer never told to go");
    // NOLINTNEXTLINE(misc-include-cleaner): <sys/wait.h> has them.
    assert(WIFEXITED(status) && WEXITSTATUS(status) == 0 && "a thread ran on after the log writer was let go");
    assert(text.contains("event=before") && text.contains("event=after") && "a line logged around letting the writer go is missing");
    std::filesystem::remove_all(unique);
}

enum class Leave : std::uint8_t { exit, exitLogged, abort, fault, fpe, ill };

// The crash's own wait, in a child whose lines the test reads: its
// crashPatience polls of a millisecond are a clock, which a test held off
// the CPU could outlast. About seventeen minutes: the test reads at once.
constexpr int readerPatience = 1'000'000;

#ifdef __has_feature
#if __has_feature(address_sanitizer)
#define LOGFMT_TEST_SANITIZER_SIGNALS 1
#endif
#endif

// A fault the kernel raises: a write to a page that may not be written.
void writeToAPageThatForbidsIt()
{
    auto const page = static_cast<size_t>(sysconf(_SC_PAGESIZE));
    void * const where = mmap(nullptr, page, PROT_NONE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if (where == MAP_FAILED) {
        std::_Exit(2);
    }
    *static_cast<volatile char *>(where) = 1;
}

// A child makes our stderr a full pipe that nobody reads, logs, and says on
// a pipe of its own that every logLine returned. Then it leaves as `leave`
// says. It must be found waiting to leave: alive, every thread of it asleep,
// three readings running; one asleep writing to stderr before it said so
// waited for stderr in logLine. Only then is the pipe read, and every line
// must be on it, in order, and the child must leave as it was to. A crash
// waits so for crashPatience polls with nothing written, which the child
// makes readerPatience. The crashes: an abort; a fault the kernel raises,
// a write to a page that forbids it, which recurs once the handler returns;
// SIGFPE and SIGILL, raised by the child itself, since a fault that raises
// either differs between machines. The fault and SIGFPE are skipped under
// AddressSanitizer, whose own handlers take them before ours.
void leavesWithItsLines(Leave leave, std::string_view how)
{
#ifdef LOGFMT_TEST_SANITIZER_SIGNALS
    if (leave == Leave::fault || leave == Leave::fpe) {
        std::println("logfmt-test: {}: skipped, AddressSanitizer handles its signal", how);
        return;
    }
#endif
    bool const crashes = leave != Leave::exit && leave != Leave::exitLogged;
    std::array<int, 2> err{};
    std::array<int, 2> said{};
    assert(pipe2(err.data(), O_CLOEXEC) == 0 && pipe2(said.data(), O_CLOEXEC) == 0);
    auto const [errRead, errWrite] = err;
    auto const [saidRead, saidWrite] = said;
    std::string const room(static_cast<size_t>(fcntl(errWrite, F_GETPIPE_SZ)), '\0'); // NOLINT(cppcoreguidelines-pro-type-vararg)
    // NOLINTNEXTLINE(misc-include-cleaner): ssize_t comes from <sys/types.h>.
    assert(write(errWrite, room.data(), room.size()) == static_cast<ssize_t>(room.size()));
    nixgrpc::flushLog();
    static_cast<void>(std::fflush(stdout)); // a child that exits would write it again
    pid_t const child = fork();
    assert(child >= 0);
    if (child == 0) {
        dup2(errWrite, STDERR_FILENO);
        if (crashes) {
            nixgrpc::flushLogOnCrash();
            nixgrpc::StderrLines::crashPatience = readerPatience;
        }
        for (int seq = 0; seq < linesPerThread; seq++) {
            nixgrpc::logLine(nixgrpc::LogLevel::info, {{"event", "leaving"}, {"n", std::to_string(seq)}});
        }
        char const byte = 1;
        static_cast<void>(write(saidWrite, &byte, 1));
        switch (leave) {
        case Leave::exit:
            std::exit(0); // NOLINT(concurrency-mt-unsafe): what is tested, a normal exit.
        case Leave::exitLogged:
            nixgrpc::exitLogged(0);
        case Leave::abort:
            std::abort();
        case Leave::fault:
            writeToAPageThatForbidsIt();
            std::_Exit(3);
        case Leave::fpe:
            static_cast<void>(::raise(SIGFPE));
            std::_Exit(3);
        case Leave::ill:
            static_cast<void>(::raise(SIGILL));
            std::_Exit(3);
        }
    }
    close(errWrite);
    close(saidWrite);
    fcntl(saidRead, F_SETFL, O_NONBLOCK); // NOLINT(cppcoreguidelines-pro-type-vararg)
    auto const main = "/proc/" + std::to_string(child) + "/task/" + std::to_string(child);
    bool logged = false;
    bool heldInLogLine = false;
    for (int i = 0; i < steps && !logged && !heldInLogLine; i++) {
        char byte = 0;
        heldInLogLine = writesToStderr(main);
        logged = read(saidRead, &byte, 1) == 1;
        std::this_thread::yield();
    }
    // Waiting to leave, or gone.
    bool waits = false;
    bool gone = false;
    int answers = 0;
    for (int i = 0; logged && i < steps && !waits && !gone; i++) {
        auto const all = tasks(child);
        gone = all.empty() || std::string_view("ZXx?").contains(state(main));
        bool const still = !gone && std::ranges::all_of(all, [](const std::string & task) -> bool { return state(task) == 'S'; });
        answers = still ? answers + 1 : 0;
        waits = answers == 3;
        std::this_thread::yield();
    }
    std::string text;
    std::array<char, chunkBytes> chunk{};
    for (ssize_t got = 0; (got = read(errRead, chunk.data(), chunk.size())) > 0;) {
        text.append(chunk.data(), static_cast<size_t>(got));
    }
    int status = 0;
    waitpid(child, &status, 0);
    close(errRead);
    close(saidRead);
    int next = 0;
    std::string_view const key = "event=leaving n=";
    for (size_t at = 0; (at = text.find(key, at)) != std::string::npos; at += 1) {
        next += std::stoi(text.substr(at + key.size())) == next ? 1 : 0;
    }
    std::println(
        "logfmt-test: {}: logged={} heldInLogLine={} waits={} gone={} lines={} status={:#x}",
        how, logged, heldInLogLine, waits, gone, next, status);
    assert(logged && !heldInLogLine && "logLine waited for a stalled stderr");
    assert(next == linesPerThread && "lines logged before leaving are missing from stderr");
    assert(waits && "the process left before writing what it had logged");
    switch (leave) {
    case Leave::exit:
    case Leave::exitLogged:
        // NOLINTNEXTLINE(misc-include-cleaner): <sys/wait.h> has them.
        assert(WIFEXITED(status) && WEXITSTATUS(status) == 0 && "the process did not leave as it was to");
        break;
    case Leave::abort:
    case Leave::fault:
    case Leave::fpe:
    case Leave::ill: {
        std::map<Leave, int> const signals{{Leave::abort, SIGABRT}, {Leave::fault, SIGSEGV}, {Leave::fpe, SIGFPE}, {Leave::ill, SIGILL}};
        // NOLINTNEXTLINE(misc-include-cleaner): <sys/wait.h> has them.
        assert(WIFSIGNALED(status) && WTERMSIG(status) == signals.at(leave) && "the process did not die of its signal");
        break;
    }
    }
}

// A child of fork has neither its parent's writer nor the lines its parent
// queued: they are its parent's to write. With our stderr a full pipe, so
// that lines of ours wait in the queue, a child logs to a file of its own.
// Its line must get there, and no line of ours. A child that took its
// parent's writer for its own would wait for that writer for good: found
// with one thread, asleep on a futex, which nothing will wake, three
// readings running.
void aForkedChildLogsOnlyItsOwn()
{
    auto const dir = freshDir();
    auto const file = dir / "stderr";
    std::array<int, 2> err{};
    assert(pipe2(err.data(), O_CLOEXEC) == 0);
    int const errRead = err.at(0);
    int const errWrite = err.at(1);
    std::string const room(static_cast<size_t>(fcntl(errWrite, F_GETPIPE_SZ)), '\0'); // NOLINT(cppcoreguidelines-pro-type-vararg)
    // NOLINTNEXTLINE(misc-include-cleaner): ssize_t comes from <sys/types.h>.
    assert(write(errWrite, room.data(), room.size()) == static_cast<ssize_t>(room.size()));
    nixgrpc::flushLog();
    static_cast<void>(std::fflush(stdout)); // a child that exits would write it again
    int const ourStderr = fcntl(STDERR_FILENO, F_DUPFD_CLOEXEC, 0); // NOLINT(cppcoreguidelines-pro-type-vararg)
    assert(ourStderr >= 0 && dup2(errWrite, STDERR_FILENO) >= 0);
    // No assertion may fail while our stderr is the pipe: its message would
    // wait for room too.
    // Logged by a thread of ours, which returns, the lines queued; or, with
    // logLine writing in its caller, sleeps writing them.
    std::atomic<int> loggerTid = 0;
    std::atomic<bool> logged = false;
    std::thread logger([&] -> void {
        loggerTid = gettid();
        for (int seq = 0; seq < threadsLogging; seq++) {
            nixgrpc::logLine(nixgrpc::LogLevel::info, {{"event", "parents"}, {"n", std::to_string(seq)}});
        }
        logged = true;
    });
    bool held = false;
    for (int i = 0; i < steps && !logged && !held; i++) {
        held = loggerTid != 0 && writesToStderr("/proc/self/task/" + std::to_string(loggerTid));
        std::this_thread::yield();
    }
    pid_t const child = fork();
    if (child == 0) {
        int const out = open(file.c_str(), O_WRONLY | O_CREAT | O_CLOEXEC, 0600); // NOLINT(cppcoreguidelines-pro-type-vararg)
        if (out < 0 || dup2(out, STDERR_FILENO) < 0) {
            std::_Exit(2);
        }
        nixgrpc::logLine(nixgrpc::LogLevel::info, {{"event", "childs"}});
        nixgrpc::flushLog();
        std::_Exit(0);
    }
    int status = 0;
    bool exited = false;
    bool stuck = false;
    int alone = 0;
    auto const main = "/proc/" + std::to_string(child) + "/task/" + std::to_string(child);
    for (int i = 0; child > 0 && i < steps && !exited && !stuck; i++) {
        exited = waitpid(child, &status, WNOHANG) == child;
        alone = !exited && tasks(child).size() == 1 && waitsOnAFutex(main) ? alone + 1 : 0;
        stuck = alone == 3;
        std::this_thread::yield();
    }
    if (child > 0 && !exited) {
        kill(child, SIGKILL);
        waitpid(child, &status, 0);
    }
    // Our writer goes on once the pipe is read.
    std::string stalled;
    std::thread reader([&] -> void {
        std::array<char, chunkBytes> chunk{};
        for (ssize_t got = 0; (got = read(errRead, chunk.data(), chunk.size())) > 0;) {
            stalled.append(chunk.data(), static_cast<size_t>(got));
        }
    });
    logger.join();
    nixgrpc::flushLog();
    dup2(ourStderr, STDERR_FILENO);
    close(ourStderr);
    close(errWrite);
    reader.join();
    close(errRead);
    auto const text = textOf(file);
    std::println(
        "logfmt-test: forked: child={} exited={} stuck={} status={:#x} childs={} parents={} ours={}",
        child, exited, stuck, status, text.contains("event=childs"), text.contains("event=parents"),
        stalled.contains("event=parents n=" + std::to_string(threadsLogging - 1)));
    assert(child > 0 && (logged || held));
    assert(!stuck && exited && "a child of fork waited for its parent's log writer");
    assert(!text.contains("event=parents") && "a child of fork wrote lines its parent had queued");
    assert(text.contains("event=childs") && "a line a child of fork logged is missing");
    assert(stalled.contains("event=parents n=" + std::to_string(threadsLogging - 1)) && "a line we logged before a fork is missing");
    std::filesystem::remove_all(dir);
}

// A line logged once the process has begun to leave, after the writer is
// done (an atexit handler registered before it, a static destructor), is
// written by its caller, after the others.
void aLineLoggedWhileLeavingGoesOut()
{
    auto const dir = freshDir();
    auto const file = dir / "stderr";
    nixgrpc::flushLog();
    static_cast<void>(std::fflush(stdout)); // a child that exits would write it again
    pid_t const child = fork();
    assert(child >= 0);
    if (child == 0) {
        int const out = open(file.c_str(), O_WRONLY | O_CREAT | O_CLOEXEC, 0600); // NOLINT(cppcoreguidelines-pro-type-vararg)
        if (out < 0 || dup2(out, STDERR_FILENO) < 0) {
            std::_Exit(2);
        }
        logWhileLeaving = true;
        nixgrpc::logLine(nixgrpc::LogLevel::info, {{"event", "early"}});
        std::exit(0); // NOLINT(concurrency-mt-unsafe): what is tested, a normal exit.
    }
    int status = 0;
    waitpid(child, &status, 0);
    auto const text = textOf(file);
    std::println("logfmt-test: leaving: status={:#x} early={} late={}", status, text.contains("event=early"), text.contains("event=late"));
    // NOLINTNEXTLINE(misc-include-cleaner): <sys/wait.h> has them.
    assert(WIFEXITED(status) && WEXITSTATUS(status) == 0);
    assert(text.contains("event=early") && "a line logged before leaving is missing");
    assert(text.contains("event=late") && "a line logged while leaving is missing");
    assert(text.find("event=early") < text.find("event=late") && "a line logged while leaving came first");
    std::filesystem::remove_all(dir);
}

// Waits for the child `pid` by readings: true once it exited, and `status`
// says how. Found with every thread asleep on a futex, stuckReadings
// readings running, it waits for what nothing will do: it is killed, and
// false. There is no other bound: a child that cannot end so must end.
auto exitedUnstuck(pid_t pid, int & status) -> bool
{
    constexpr int stuckReadings = 100;
    bool exited = false;
    int asleep = 0;
    while (!exited && asleep < stuckReadings) {
        exited = waitpid(pid, &status, WNOHANG) == pid;
        auto const all = exited ? std::vector<std::string>{} : tasks(pid);
        bool const still = !all.empty() && std::ranges::all_of(all, [](const std::string & task) -> bool { return waitsOnAFutex(task); });
        asleep = still ? asleep + 1 : 0;
        std::this_thread::yield();
    }
    if (!exited) {
        kill(pid, SIGKILL);
        waitpid(pid, &status, 0);
    }
    return exited;
}

// A line logged while the process leaves, once the writer's atexit handler
// has told the writer the queue is all there is but before the writer has
// written it, goes out after what was queued: its caller waits for the
// queue, then writes it. A child makes its stderr a full pipe that nobody
// reads, logs, and leaves by exit, whose atexit handler then waits to join
// the writer, held up by the pipe. A thread of the child finds it so, asleep
// on a futex, says so, and logs a line; once that thread is found asleep,
// the pipe is read. The line must come after every line queued before.
void aLineLoggedWhileTheQueueDrainsGoesAfterIt()
{
    std::array<int, 2> err{};
    std::array<int, 2> said{};
    assert(pipe2(err.data(), O_CLOEXEC) == 0 && pipe2(said.data(), O_CLOEXEC) == 0);
    auto const [errRead, errWrite] = err;
    auto const [saidRead, saidWrite] = said;
    std::string const room(static_cast<size_t>(fcntl(errWrite, F_GETPIPE_SZ)), '\0'); // NOLINT(cppcoreguidelines-pro-type-vararg)
    // NOLINTNEXTLINE(misc-include-cleaner): ssize_t comes from <sys/types.h>.
    assert(write(errWrite, room.data(), room.size()) == static_cast<ssize_t>(room.size()));
    nixgrpc::flushLog();
    static_cast<void>(std::fflush(stdout)); // a child that exits would write it again
    pid_t const child = fork();
    assert(child >= 0);
    if (child == 0) {
        dup2(errWrite, STDERR_FILENO);
        for (int seq = 0; seq < linesPerThread; seq++) {
            nixgrpc::logLine(nixgrpc::LogLevel::info, {{"event", "queued"}, {"n", std::to_string(seq)}});
        }
        static std::atomic<bool> leaving = false;
        joinWhileLeaving = std::thread([main = "/proc/self/task/" + std::to_string(gettid()), saidWrite] -> void {
            bool found = false;
            for (int i = 0; i < steps && !found; i++) {
                found = leaving && waitsOnAFutex(main);
                std::this_thread::yield();
            }
            pid_t const tid = found ? gettid() : 0;
            static_cast<void>(write(saidWrite, &tid, sizeof tid));
            nixgrpc::logLine(nixgrpc::LogLevel::info, {{"event", "drainlate"}});
        });
        leaving = true;
        std::exit(0); // NOLINT(concurrency-mt-unsafe): what is tested, a normal exit.
    }
    close(errWrite);
    close(saidWrite);
    fcntl(saidRead, F_SETFL, O_NONBLOCK); // NOLINT(cppcoreguidelines-pro-type-vararg)
    // Its word, or its main thread found writing to stderr in logLine,
    // which would wait for the pipe for good.
    auto const childMain = "/proc/" + std::to_string(child) + "/task/" + std::to_string(child);
    pid_t tid = 0;
    bool told = false;
    bool heldInLogLine = false;
    for (int i = 0; i < steps && !told && !heldInLogLine; i++) {
        heldInLogLine = writesToStderr(childMain);
        // NOLINTNEXTLINE(clang-analyzer-unix.BlockInCriticalSection): flushLog let go of the queue's lock; the analyzer does not follow it through the wait.
        told = read(saidRead, &tid, sizeof tid) == sizeof tid;
        std::this_thread::yield();
    }
    close(saidRead);
    bool const found = told && tid > 0;
    if (!told) {
        kill(child, SIGKILL);
    }
    // Asleep in its line's write, or waiting to write it.
    auto const late = "/proc/" + std::to_string(child) + "/task/" + std::to_string(tid);
    bool asleep = false;
    for (int i = 0; found && i < steps && !asleep; i++) {
        asleep = state(late) == 'S';
        std::this_thread::yield();
    }
    std::string text;
    std::array<char, chunkBytes> chunk{};
    // NOLINTNEXTLINE(clang-analyzer-unix.BlockInCriticalSection): flushLog let go of the queue's lock; the analyzer does not follow it through the wait.
    for (ssize_t got = 0; (got = read(errRead, chunk.data(), chunk.size())) > 0;) {
        text.append(chunk.data(), static_cast<size_t>(got));
    }
    close(errRead);
    int status = 0;
    waitpid(child, &status, 0);
    auto const last = text.find("event=queued n=" + std::to_string(linesPerThread - 1));
    auto const drainLate = text.find("event=drainlate");
    std::println(
        "logfmt-test: logged while the queue drains: heldInLogLine={} found={} asleep={} status={:#x} last={} late={}",
        heldInLogLine, found, asleep, status, last != std::string::npos, drainLate != std::string::npos);
    static_cast<void>(std::fflush(stdout)); // before an assertion ends us
    assert(!heldInLogLine && "logLine waited for a stalled stderr");
    assert(found && asleep && "the child was not found leaving, its writer held up");
    // NOLINTNEXTLINE(misc-include-cleaner): <sys/wait.h> has them.
    assert(WIFEXITED(status) && WEXITSTATUS(status) == 0);
    assert(last != std::string::npos && drainLate != std::string::npos && "a line is missing");
    assert(last < drainLate && "a line logged while the queue drained went out before it");
}

// A child of fork waits for its own writer as it would for ours, though its
// parent's threads waited on the queue's condition variables as it forked,
// and the child's are those, waiters and all. A child logs, flushes, logs again and flushes,
// to a file: it must exit, with both lines there. Forked once with our
// writer waiting for a line (`stalled` false); once with our stderr a full
// pipe, the writer held up writing to it and a thread of ours waiting for it
// in flushLog (`stalled`). Found with every thread asleep on a futex a
// hundred readings running, it waits for what nothing will do.
void aForkedChildsWriterWakes(bool stalled, std::string_view how)
{
    auto const dir = freshDir();
    auto const file = dir / "stderr";
    nixgrpc::flushLog();
    std::array<int, 2> err{};
    assert(pipe2(err.data(), O_CLOEXEC) == 0);
    auto const [errRead, errWrite] = err;
    int const ourStderr = fcntl(STDERR_FILENO, F_DUPFD_CLOEXEC, 0); // NOLINT(cppcoreguidelines-pro-type-vararg)
    assert(ourStderr >= 0);
    std::optional<std::thread> flusher;
    std::atomic<int> flusherTid = 0;
    if (stalled) {
        std::string const room(static_cast<size_t>(fcntl(errWrite, F_GETPIPE_SZ)), '\0'); // NOLINT(cppcoreguidelines-pro-type-vararg)
        // NOLINTNEXTLINE(misc-include-cleaner): ssize_t comes from <sys/types.h>.
        assert(write(errWrite, room.data(), room.size()) == static_cast<ssize_t>(room.size()));
        assert(dup2(errWrite, STDERR_FILENO) >= 0);
        // No assertion may fail while our stderr is the pipe. Logged by
        // the flusher: a logLine that waited for the pipe holds up only it.
        flusher.emplace([&] -> void {
            flusherTid = gettid();
            nixgrpc::logLine(nixgrpc::LogLevel::info, {{"event", "parents"}});
            nixgrpc::flushLog();
        });
    } else {
        nixgrpc::logLine(nixgrpc::LogLevel::info, {{"event", "parents"}});
        nixgrpc::flushLog();
    }
    // The writer waits for a line or writes; the flusher waits for it.
    bool ready = false;
    for (int i = 0; i < steps && !ready; i++) {
        auto const ours = tasks(getpid());
        auto const writer = std::ranges::find_if(ours, [](const std::string & task) -> bool { return firstLine(task + "/comm") == "log-writer"; });
        ready = writer != ours.end() && (stalled ? writesToStderr(*writer) : waitsOnAFutex(*writer))
                && (!stalled || (flusherTid != 0 && waitsOnAFutex("/proc/self/task/" + std::to_string(flusherTid))));
        std::this_thread::yield();
    }
    static_cast<void>(std::fflush(stdout)); // a child that exits would write it again
    pid_t const child = fork();
    if (child == 0) {
        int const out = open(file.c_str(), O_WRONLY | O_CREAT | O_CLOEXEC, 0600); // NOLINT(cppcoreguidelines-pro-type-vararg)
        if (out < 0 || dup2(out, STDERR_FILENO) < 0) {
            std::_Exit(2);
        }
        nixgrpc::logLine(nixgrpc::LogLevel::info, {{"event", "one"}});
        nixgrpc::flushLog();
        nixgrpc::logLine(nixgrpc::LogLevel::info, {{"event", "two"}});
        nixgrpc::flushLog();
        std::_Exit(0);
    }
    int status = 0;
    bool const exited = child > 0 && exitedUnstuck(child, status);
    std::thread reader([errRead] -> void {
        std::array<char, chunkBytes> chunk{};
        while (read(errRead, chunk.data(), chunk.size()) > 0) {
        }
    });
    if (flusher) {
        flusher->join();
    }
    dup2(ourStderr, STDERR_FILENO);
    close(ourStderr);
    close(errWrite);
    reader.join();
    close(errRead);
    auto const text = textOf(file);
    std::println(
        "logfmt-test: forked, our writer {}: ready={} exited={} status={:#x} one={} two={}",
        how, ready, exited, status, text.contains("event=one"), text.contains("event=two"));
    static_cast<void>(std::fflush(stdout)); // before an assertion ends us
    assert(child > 0 && ready);
    assert(exited && "a child of fork waited for its writer for good");
    // NOLINTNEXTLINE(misc-include-cleaner): <sys/wait.h> has them.
    assert(WIFEXITED(status) && WEXITSTATUS(status) == 0);
    assert(text.contains("event=one") && text.contains("event=two") && "a line a child of fork logged is missing");
    std::filesystem::remove_all(dir);
}

// In a child of aLineLoggedWhileTheWriterGoesWaits: the thread that logs
// while stopWriter lets the writer go, and whether it has begun and is done.
std::thread lateLogger; // NOLINT(cppcoreguidelines-avoid-non-const-global-variables): started by stopWriter's hook in a child
std::atomic<bool> lateBegun = false; // NOLINT(cppcoreguidelines-avoid-non-const-global-variables): set by that thread
std::atomic<bool> lateDone = false; // NOLINT(cppcoreguidelines-avoid-non-const-global-variables): set by that thread

// A line logged while stopLogWriter lets the writer go waits until it is
// gone, then starts another: it does not build one where the writer being
// let go still is, which would leave that writer unjoined and the next
// lines with no writer. A child logs, flushes, and lets the writer go; at
// the point where the writer is told to go and not yet joined
// (StderrLines::whileWriterGoes), another thread of it logs a line, and
// stopWriter goes on once that line is logged, or once the thread has
// begun to log and steps readings have passed (it waits, as it must). Then
// the child logs, flushes and must exit 0, with every line on its stderr, a
// file. Found with every thread asleep on a futex a hundred readings
// running, it waits for a writer that is gone.
void aLineLoggedWhileTheWriterGoesWaits()
{
    auto const dir = freshDir();
    auto const file = dir / "stderr";
    nixgrpc::flushLog();
    static_cast<void>(std::fflush(stdout)); // a child that exits would write it again
    pid_t const child = fork();
    assert(child >= 0);
    if (child == 0) {
        int const out = open(file.c_str(), O_WRONLY | O_CREAT | O_CLOEXEC, 0600); // NOLINT(cppcoreguidelines-pro-type-vararg)
        if (out < 0 || dup2(out, STDERR_FILENO) < 0) {
            std::_Exit(2);
        }
        nixgrpc::logLine(nixgrpc::LogLevel::info, {{"event", "before"}});
        nixgrpc::flushLog();
        nixgrpc::StderrLines::whileWriterGoes = [] -> void {
            lateLogger = std::thread([] -> void {
                lateBegun = true;
                nixgrpc::logLine(nixgrpc::LogLevel::info, {{"event", "during"}});
                lateDone = true;
            });
            for (int i = 0; i < steps && !lateDone; i++) {
                std::this_thread::yield();
            }
            while (!lateBegun) {
                std::this_thread::yield();
            }
        };
        nixgrpc::stopLogWriter();
        nixgrpc::StderrLines::whileWriterGoes = nullptr;
        lateLogger.join();
        nixgrpc::logLine(nixgrpc::LogLevel::info, {{"event", "after"}});
        nixgrpc::flushLog();
        std::_Exit(0);
    }
    int status = 0;
    bool const exited = exitedUnstuck(child, status);
    auto const text = textOf(file);
    std::println(
        "logfmt-test: logged while the writer goes: exited={} status={:#x} before={} during={} after={}",
        exited, status, text.contains("event=before"), text.contains("event=during"), text.contains("event=after"));
    static_cast<void>(std::fflush(stdout)); // before an assertion ends us
    assert(exited && "a line logged while the writer went left the lines after it with no writer");
    // NOLINTNEXTLINE(misc-include-cleaner): <sys/wait.h> has them.
    assert(WIFEXITED(status) && WEXITSTATUS(status) == 0 && "a line logged while the writer went ended the process");
    assert(text.contains("event=before") && text.contains("event=during") && text.contains("event=after") && "a line logged around letting the writer go is missing");
    std::filesystem::remove_all(dir);
}

// In a child of aChildForkedWhileTheWriterGoesLogs: its own child, forked
// while stopWriter let the writer go.
pid_t forkedWhileGoing = -1; // NOLINT(cppcoreguidelines-avoid-non-const-global-variables): set by stopWriter's hook in a child

// A child of fork made while stopLogWriter lets the writer go has no writer
// being let go to wait for: a line it logs starts its own. A child of the
// test logs, flushes, and lets the writer go; where the writer is told to
// go and not yet joined (StderrLines::whileWriterGoes), it forks, and that
// child logs a line, flushes and exits 0. The child of the test waits for
// it as exitedUnstuck does, and exits 0 once it has exited 0. A child of
// the fork that waited for the writer being let go would be found with its
// one thread asleep on a futex: then the child of the test says so (exit 3).
// The line must be on their stderr, a file.
void aChildForkedWhileTheWriterGoesLogs()
{
    constexpr int stuck = 3;
    constexpr int failed = 4;
    auto const dir = freshDir();
    auto const file = dir / "stderr";
    nixgrpc::flushLog();
    static_cast<void>(std::fflush(stdout)); // a child that exits would write it again
    pid_t const child = fork();
    assert(child >= 0);
    if (child == 0) {
        int const out = open(file.c_str(), O_WRONLY | O_CREAT | O_CLOEXEC, 0600); // NOLINT(cppcoreguidelines-pro-type-vararg)
        if (out < 0 || dup2(out, STDERR_FILENO) < 0) {
            std::_Exit(2);
        }
        nixgrpc::logLine(nixgrpc::LogLevel::info, {{"event", "before"}});
        nixgrpc::flushLog();
        nixgrpc::StderrLines::whileWriterGoes = [] -> void {
            forkedWhileGoing = fork();
            if (forkedWhileGoing == 0) {
                nixgrpc::logLine(nixgrpc::LogLevel::info, {{"event", "forkedwhilegoing"}});
                nixgrpc::flushLog();
                std::_Exit(0);
            }
        };
        nixgrpc::stopLogWriter();
        nixgrpc::StderrLines::whileWriterGoes = nullptr;
        int status = 0;
        if (forkedWhileGoing < 0) {
            std::_Exit(failed);
        }
        if (!exitedUnstuck(forkedWhileGoing, status)) {
            std::_Exit(stuck);
        }
        // NOLINTNEXTLINE(misc-include-cleaner): <sys/wait.h> has them.
        std::_Exit(WIFEXITED(status) && WEXITSTATUS(status) == 0 ? 0 : failed);
    }
    int status = 0;
    bool const exited = exitedUnstuck(child, status);
    auto const text = textOf(file);
    std::println("logfmt-test: forked while the writer goes: exited={} status={:#x} logged={}", exited, status, text.contains("event=forkedwhilegoing"));
    static_cast<void>(std::fflush(stdout)); // before an assertion ends us
    assert(exited);
    // NOLINTNEXTLINE(misc-include-cleaner): <sys/wait.h> has them.
    assert((!WIFEXITED(status) || WEXITSTATUS(status) != stuck) && "a child forked while the writer went waited for it for good");
    // NOLINTNEXTLINE(misc-include-cleaner): <sys/wait.h> has them.
    assert(WIFEXITED(status) && WEXITSTATUS(status) == 0);
    assert(text.contains("event=forkedwhilegoing") && "a line a child forked while the writer went logged is missing");
    std::filesystem::remove_all(dir);
}

// glibc's gmtime_r takes its time-zone lock, and a child of fork has that
// lock as the thread that forked found it: held for good, if another thread
// held it then. The daemon's child of a takeover is the daemon that goes on,
// and logs. A thread of ours reads a time zone from a fifo nobody writes,
// holding that lock while it waits to open it; once it is found so, we fork,
// and the child logs. Its line must go out, and its time be the one gmtime_r
// gives for the clock the line reads, read before and after.
void aChildForkedWhileTheTimeZoneIsReadLogs()
{
    auto const dir = freshDir();
    auto const file = dir / "stderr";
    auto const zone = dir / "zone";
    assert(mkfifo(zone.c_str(), 0600) == 0);
    auto const utcNow = [] -> std::string {
        std::time_t const now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
        std::tm utc{};
        gmtime_r(&now, &utc);
        std::array<char, sizeof("ts=2000-01-01T00:00:00Z")> text{};
        static_cast<void>(std::strftime(text.data(), text.size(), "ts=%Y-%m-%dT%H:%M:%SZ", &utc));
        return text.data();
    };
    auto const before = utcNow();
    assert(setenv("TZ", zone.c_str(), 1) == 0); // NOLINT(concurrency-mt-unsafe): no other thread reads the environment.
    std::atomic<pid_t> readerTid = 0;
    std::thread reader([&] -> void {
        readerTid = gettid();
        tzset();
    });
    bool opening = false;
    for (int i = 0; i < steps && !opening; i++) {
        auto const task = "/proc/self/task/" + std::to_string(readerTid.load());
        std::istringstream fields(firstLine(task + "/syscall"));
        long number = -1;
        opening = readerTid != 0 && state(task) == 'S' && fields >> number && number == SYS_openat;
        std::this_thread::yield();
    }
    assert(opening && "the time zone's reader was not found opening it");
    pid_t const child = fork();
    if (child == 0) {
        int const out = open(file.c_str(), O_WRONLY | O_CREAT | O_CLOEXEC, 0600); // NOLINT(cppcoreguidelines-pro-type-vararg)
        if (out < 0 || dup2(out, STDERR_FILENO) < 0) {
            std::_Exit(2);
        }
        nixgrpc::logLine(nixgrpc::LogLevel::info, {{"event", "childs"}});
        nixgrpc::flushLog();
        std::_Exit(0);
    }
    int status = 0;
    bool const exited = child > 0 && exitedUnstuck(child, status);
    // The reader opens the fifo, finds it empty, and lets the lock go.
    close(open(zone.c_str(), O_WRONLY | O_CLOEXEC)); // NOLINT(cppcoreguidelines-pro-type-vararg)
    reader.join();
    // TZ stays the fifo's: nothing after reads the time zone anew.
    auto const after = utcNow();
    auto const text = textOf(file);
    auto const stamp = text.substr(0, text.find(' '));
    std::println("logfmt-test: forked while the time zone is read: exited={} status={:#x} line={}", exited, status, text.substr(0, text.find('\n')));
    static_cast<void>(std::fflush(stdout)); // before an assertion ends us
    assert(child > 0);
    assert(exited && "a child of fork waited for good for the time-zone lock its parent held");
    // NOLINTNEXTLINE(misc-include-cleaner): <sys/wait.h> has them.
    assert(WIFEXITED(status) && WEXITSTATUS(status) == 0);
    assert(text.contains("event=childs") && "a line a child of fork logged is missing");
    assert(stamp.size() == before.size() && before <= stamp && stamp <= after && "a line's time is not the one gmtime_r gives");
    std::filesystem::remove_all(dir);
}

// The log writer holds no lock while our stderr's reader holds up its
// write: a thread that writes to stderr itself (Nix's logger, gRPC's) then
// waits in a write of its own, not on a lock behind the writer. With our
// stderr a full pipe and the writer found asleep writing a line, another
// thread writes with stdio: it must be found asleep writing, not on a
// futex three readings running.
void aStalledWriterHoldsNoLock()
{
    std::array<int, 2> err{};
    assert(pipe2(err.data(), O_CLOEXEC) == 0);
    int const errRead = err.at(0);
    int const errWrite = err.at(1);
    std::string const room(static_cast<size_t>(fcntl(errWrite, F_GETPIPE_SZ)), '\0'); // NOLINT(cppcoreguidelines-pro-type-vararg)
    // NOLINTNEXTLINE(misc-include-cleaner): ssize_t comes from <sys/types.h>.
    assert(write(errWrite, room.data(), room.size()) == static_cast<ssize_t>(room.size()));
    nixgrpc::flushLog();
    int const ourStderr = fcntl(STDERR_FILENO, F_DUPFD_CLOEXEC, 0); // NOLINT(cppcoreguidelines-pro-type-vararg)
    assert(ourStderr >= 0 && dup2(errWrite, STDERR_FILENO) >= 0);
    // No assertion may fail while our stderr is the pipe.
    nixgrpc::logLine(nixgrpc::LogLevel::info, {{"event", "held"}});
    auto const writerWrites = [] -> bool {
        return std::ranges::any_of(tasks(getpid()), [](const std::string & task) -> bool {
            return firstLine(task + "/comm") == "log-writer" && writesToStderr(task);
        });
    };
    bool writerHeld = false;
    for (int i = 0; i < steps && !writerHeld; i++) {
        writerHeld = writerWrites();
        std::this_thread::yield();
    }
    std::atomic<int> otherTid = 0;
    std::thread other([&] -> void {
        otherTid = gettid();
        static_cast<void>(std::fputs("logfmt-test: direct\n", stderr));
    });
    bool writes = false;
    bool locked = false;
    int onAFutex = 0;
    for (int i = 0; writerHeld && i < steps && !writes && !locked; i++) {
        auto const task = "/proc/self/task/" + std::to_string(otherTid);
        writes = otherTid != 0 && writesToStderr(task);
        onAFutex = otherTid != 0 && waitsOnAFutex(task) ? onAFutex + 1 : 0;
        locked = onAFutex == 3;
        std::this_thread::yield();
    }
    std::string stalled;
    std::thread reader([&] -> void {
        std::array<char, chunkBytes> chunk{};
        for (ssize_t got = 0; (got = read(errRead, chunk.data(), chunk.size())) > 0;) {
            stalled.append(chunk.data(), static_cast<size_t>(got));
        }
    });
    other.join();
    nixgrpc::flushLog();
    dup2(ourStderr, STDERR_FILENO);
    close(ourStderr);
    close(errWrite);
    reader.join();
    close(errRead);
    std::println("logfmt-test: no lock: writerHeld={} writes={} locked={}", writerHeld, writes, locked);
    assert(writerHeld && "the log writer was never found writing to the full pipe");
    assert(writes && !locked && "a thread writing to stderr waited on a lock behind the log writer");
    assert(stalled.contains("event=held") && stalled.contains("logfmt-test: direct"));
}

// A crash with our stderr held up for good must still end the process:
// one that waits for its log writer for good neither dumps its core nor is
// restarted. A child makes its stderr a full pipe that nobody reads, logs,
// says which thread will crash, and aborts. It must die of SIGABRT, not be
// found polling ten times crashPatience, alive.
void aCrashWithStderrHeldUpStillLeaves()
{
    std::array<int, 2> err{};
    std::array<int, 2> said{};
    assert(pipe2(err.data(), O_CLOEXEC) == 0 && pipe2(said.data(), O_CLOEXEC) == 0);
    int const errRead = err.at(0);
    int const errWrite = err.at(1);
    int const saidRead = said.at(0);
    int const saidWrite = said.at(1);
    std::string const room(static_cast<size_t>(fcntl(errWrite, F_GETPIPE_SZ)), '\0'); // NOLINT(cppcoreguidelines-pro-type-vararg)
    // NOLINTNEXTLINE(misc-include-cleaner): ssize_t comes from <sys/types.h>.
    assert(write(errWrite, room.data(), room.size()) == static_cast<ssize_t>(room.size()));
    nixgrpc::flushLog();
    static_cast<void>(std::fflush(stdout)); // a child that exits would write it again
    pid_t const child = fork();
    assert(child >= 0);
    if (child == 0) {
        dup2(errWrite, STDERR_FILENO);
        nixgrpc::flushLogOnCrash();
        for (int seq = 0; seq < threadsLogging; seq++) {
            nixgrpc::logLine(nixgrpc::LogLevel::info, {{"event", "crashing"}, {"n", std::to_string(seq)}});
        }
        // No core dump: writing one, the process lingers after its crash.
        // NOLINTNEXTLINE(cppcoreguidelines-pro-type-vararg): prctl(2) is variadic.
        static_cast<void>(prctl(PR_SET_DUMPABLE, 0, 0, 0, 0));
        pid_t const tid = gettid();
        static_cast<void>(write(saidWrite, &tid, sizeof tid));
        std::abort();
    }
    close(errWrite);
    close(saidWrite);
    pid_t tid = 0;
    // NOLINTNEXTLINE(clang-analyzer-unix.BlockInCriticalSection): flushLog let go of the queue's lock; the analyzer does not follow it through the wait.
    bool const toldUs = read(saidRead, &tid, sizeof tid) == sizeof tid;
    close(saidRead);
    long const hungPolls = 10L * nixgrpc::StderrLines::crashPatience;
    auto const crashing = "/proc/" + std::to_string(child) + "/task/" + std::to_string(tid);
    long const before = voluntarySwitches(crashing);
    int status = 0;
    bool exited = false;
    bool waits = false;
    for (int i = 0; toldUs && i < steps && !exited && !waits; i++) {
        exited = waitpid(child, &status, WNOHANG) == child;
        auto const now = exited ? -1 : voluntarySwitches(crashing);
        waits = now >= 0 && now - before > hungPolls;
        std::this_thread::yield();
    }
    if (!exited) {
        kill(child, SIGKILL);
        waitpid(child, &status, 0);
    }
    close(errRead);
    std::println("logfmt-test: crash, stderr held up: exited={} waits={} status={:#x}", exited, waits, status);
    static_cast<void>(std::fflush(stdout)); // before an assertion ends us
    assert(toldUs && !waits && exited && "a crash waited for a held-up stderr for good");
    // NOLINTNEXTLINE(misc-include-cleaner): <sys/wait.h> has them.
    assert(WIFSIGNALED(status) && WTERMSIG(status) == SIGABRT && "the process did not die of its signal");
}

// A crash waits for its log writer only while it writes, but for as long
// as it writes: a reader that holds stderr up, yet lets a page through every
// so often, holds the crash up for as long as it takes to read. A child
// makes its stderr a pipe of a page, logs many pages, says which thread will
// crash, and aborts. The pipe is read a page at a time, once every
// twentieth of crashPatience polls of the crash: many times crashPatience
// polls in all. Every line must be on it, in order, and the child must die
// of SIGABRT.
void aCrashWaitsForAWriterThatWrites()
{
    constexpr int crashLines = 2500; // over sixty pages: a wait that gave up after crashPatience polls would read twenty
    // The child's crashPatience: ten seconds' polls, so that the test is
    // failed only by being held off the CPU for over nine and a half.
    constexpr int patience = 10'000;
    constexpr long pollsPerPage = patience / 20;
    constexpr size_t padBytes = 48;
    std::array<int, 2> err{};
    std::array<int, 2> said{};
    assert(pipe2(err.data(), O_CLOEXEC) == 0 && pipe2(said.data(), O_CLOEXEC) == 0);
    int const errRead = err.at(0);
    int const errWrite = err.at(1);
    int const saidRead = said.at(0);
    int const saidWrite = said.at(1);
    assert(fcntl(errWrite, F_SETPIPE_SZ, static_cast<int>(chunkBytes)) >= 0); // NOLINT(cppcoreguidelines-pro-type-vararg)
    nixgrpc::flushLog();
    static_cast<void>(std::fflush(stdout)); // a child that exits would write it again
    pid_t const child = fork();
    assert(child >= 0);
    if (child == 0) {
        dup2(errWrite, STDERR_FILENO);
        nixgrpc::flushLogOnCrash();
        nixgrpc::StderrLines::crashPatience = patience;
        std::string const pad(padBytes, 'x');
        for (int seq = 0; seq < crashLines; seq++) {
            nixgrpc::logLine(nixgrpc::LogLevel::info, {{"event", "slow"}, {"n", std::to_string(seq)}, {"pad", pad}});
        }
        // No core dump: writing one, the process lingers after its crash.
        // NOLINTNEXTLINE(cppcoreguidelines-pro-type-vararg): prctl(2) is variadic.
        static_cast<void>(prctl(PR_SET_DUMPABLE, 0, 0, 0, 0));
        pid_t const tid = gettid();
        static_cast<void>(write(saidWrite, &tid, sizeof tid));
        std::abort();
    }
    close(errWrite);
    close(saidWrite);
    pid_t tid = 0;
    // NOLINTNEXTLINE(clang-analyzer-unix.BlockInCriticalSection): flushLog let go of the queue's lock; the analyzer does not follow it through the wait.
    bool const toldUs = read(saidRead, &tid, sizeof tid) == sizeof tid;
    close(saidRead);
    auto const crashing = "/proc/" + std::to_string(child) + "/task/" + std::to_string(tid);
    std::string text;
    std::array<char, chunkBytes> chunk{};
    int pages = 0;
    for (ssize_t got = 1; toldUs && got > 0; pages++) {
        long const mark = voluntarySwitches(crashing);
        for (int i = 0; i < steps; i++) {
            auto const now = voluntarySwitches(crashing);
            if (now < 0 || now - mark >= pollsPerPage || std::string_view("ZXx?").contains(state(crashing))) {
                break;
            }
            std::this_thread::yield();
        }
        got = read(errRead, chunk.data(), chunk.size());
        if (got > 0) {
            text.append(chunk.data(), static_cast<size_t>(got));
        }
    }
    int status = 0;
    waitpid(child, &status, 0);
    close(errRead);
    int next = 0;
    std::string_view const key = "event=slow n=";
    for (size_t at = 0; (at = text.find(key, at)) != std::string::npos; at += 1) {
        next += std::stoi(text.substr(at + key.size())) == next ? 1 : 0;
    }
    std::println("logfmt-test: crash, stderr read slowly: pages={} lines={} status={:#x}", pages, next, status);
    static_cast<void>(std::fflush(stdout)); // before an assertion ends us
    assert(toldUs && next == crashLines && "a crash gave up on a writer that was still writing");
    // NOLINTNEXTLINE(misc-include-cleaner): <sys/wait.h> has them.
    assert(WIFSIGNALED(status) && WTERMSIG(status) == SIGABRT && "the process did not die of its signal");
}

// A line logged when no thread can be started for the log writer (EAGAIN,
// at a limit of threads) is written by its caller: logLine neither throws
// nor queues a line nobody will write, for which a flush would wait for
// good. A child at a limit of no processes of its user logs to a file of its
// own and leaves by exitLogged, which flushes. The line must be in the file,
// and the child must exit 0, not abort with what logLine threw. Skipped
// where the limit does not hold (CAP_SYS_RESOURCE, CAP_SYS_ADMIN or root).
void aLineWithNoThreadForItsWriterIsWritten()
{
    constexpr int skipped = 77;
    auto const unique = freshDir();
    auto const file = unique / "stderr";
    nixgrpc::flushLog();
    static_cast<void>(std::fflush(stdout)); // a child that exits would write it again
    pid_t const child = fork();
    assert(child >= 0);
    if (child == 0) {
        int const out = ::open(file.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0600); // NOLINT(cppcoreguidelines-pro-type-vararg)
        if (out < 0 || dup2(out, STDERR_FILENO) < 0) {
            std::_Exit(2);
        }
        rlimit const none{.rlim_cur = 0, .rlim_max = 0};
        static_cast<void>(setrlimit(RLIMIT_NPROC, &none));
        bool started = true;
        try {
            std::thread([] -> void {}).join();
        } catch (std::system_error &) {
            started = false;
        }
        if (started) {
            std::_Exit(skipped);
        }
        nixgrpc::logLine(nixgrpc::LogLevel::info, {{"event", "nothread"}});
        nixgrpc::exitLogged(0);
    }
    int status = 0;
    waitpid(child, &status, 0);
    auto const text = textOf(file);
    std::filesystem::remove_all(unique);
    // NOLINTNEXTLINE(misc-include-cleaner): <sys/wait.h> has them.
    if (WIFEXITED(status) && WEXITSTATUS(status) == skipped) {
        std::println("logfmt-test: no thread for the writer: skipped, a thread started at the limit");
        return;
    }
    std::println("logfmt-test: no thread for the writer: status={:#x} logged={}", status, text.contains("event=nothread"));
    static_cast<void>(std::fflush(stdout)); // before an assertion ends us
    // NOLINTNEXTLINE(misc-include-cleaner): <sys/wait.h> has them.
    assert(WIFEXITED(status) && WEXITSTATUS(status) == 0 && "logLine with no thread for its writer did not return");
    assert(text.contains("event=nothread") && "a line logged with no thread for its writer is missing");
}

// A memory error that needs no action (SIGBUS, BUS_MCEERR_AO) does not recur
// once its handler returns, as a fault does: the crash handler raises it
// anew, and the process dies of it, as it did with no handler. A child sends
// itself one as the kernel would (a process may give a signal it sends itself
// any code), and must die of SIGBUS, not go on. Skipped under
// AddressSanitizer, whose own handler takes SIGBUS before ours.
void aMemoryErrorThatNeedsNoActionStillKills()
{
#ifdef LOGFMT_TEST_SANITIZER_SIGNALS
    std::println("logfmt-test: memory error needing no action: skipped, AddressSanitizer handles SIGBUS");
#else
    constexpr int wentOn = 3;
    nixgrpc::flushLog();
    static_cast<void>(std::fflush(stdout)); // a child that exits would write it again
    pid_t const child = fork();
    assert(child >= 0);
    if (child == 0) {
        nixgrpc::flushLogOnCrash();
        // No core dump: writing one, the process lingers after its crash.
        // NOLINTNEXTLINE(cppcoreguidelines-pro-type-vararg): prctl(2) is variadic.
        static_cast<void>(prctl(PR_SET_DUMPABLE, 0, 0, 0, 0));
        siginfo_t info{}; // NOLINT(misc-include-cleaner): <signal.h> has it.
        info.si_signo = SIGBUS;
        info.si_code = BUS_MCEERR_AO;
        // NOLINTNEXTLINE(cppcoreguidelines-pro-type-vararg): syscall(2) is variadic.
        if (syscall(SYS_rt_tgsigqueueinfo, getpid(), gettid(), SIGBUS, &info) != 0) {
            std::_Exit(2);
        }
        std::_Exit(wentOn);
    }
    int status = 0;
    waitpid(child, &status, 0);
    std::println("logfmt-test: memory error needing no action: status={:#x}", status);
    static_cast<void>(std::fflush(stdout)); // before an assertion ends us
    // NOLINTNEXTLINE(misc-include-cleaner): <sys/wait.h> has them.
    assert(WIFSIGNALED(status) && WTERMSIG(status) == SIGBUS && "a memory error that needs no action left the process going");
#endif
}

constexpr std::string_view crashInsideMalloc = "crash-inside-malloc";
constexpr int saidFd = 3;

// The child of aCrashInsideMallocStillLeaves, our stderr the pipe and
// saidFd where it says which thread crashes.
[[noreturn]] void crashesInsideMalloc()
{
    nixgrpc::flushLogOnCrash();
    std::thread crashing([] -> void {
        std::string const pad(300, 'x');
        for (int seq = 0; seq < 2 * linesPerThread; seq++) {
            nixgrpc::logLine(nixgrpc::LogLevel::info, {{"event", "crashing"}, {"n", std::to_string(seq)}, {"pad", pad}});
        }
        // No core dump: writing one, the process lingers after its crash.
        // Only now: what /proc tells of a process that may not dump is
        // not for us to read, and the test reads it until here.
        // NOLINTNEXTLINE(cppcoreguidelines-pro-type-vararg): prctl(2) is variadic.
        static_cast<void>(prctl(PR_SET_DUMPABLE, 0, 0, 0, 0));
        pid_t const tid = gettid();
        static_cast<void>(write(saidFd, &tid, sizeof tid));
        constexpr size_t below = 100'000;
        constexpr size_t topSizeAt = below + 8; // the size field of the chunk after it: the heap's top
        // NOLINTBEGIN: corrupts the heap on purpose
        char * volatile chunk = static_cast<char *>(std::malloc(below)); // not one the compiler may leave out
        *reinterpret_cast<size_t *>(chunk + topSizeAt) = ~size_t{0} >> 8;
        void * volatile more = std::malloc(below + 10'000); // "malloc(): corrupted top size": abort
        static_cast<void>(more);
        // NOLINTEND
        std::abort();
    });
    crashing.join();
    std::_Exit(0);
}

#ifdef __has_feature
#if __has_feature(address_sanitizer)
#define LOGFMT_TEST_OTHER_MALLOC 1
#endif
#endif

// A crash inside malloc keeps the lock of its arena, which the log writer
// may need to free a line the crashing thread allocated: the crash's wait
// for the writer must end anyway, and the process die of its signal. A
// thread of a child logs lines, more than its stderr, a pipe, holds before
// it is read; then it makes malloc find its heap corrupt, which aborts
// holding the arena's lock. The crash waits polling: found to have polled
// ten times its patience since the writer last wrote, alive, it waits for
// good.
void aCrashInsideMallocStillLeaves()
{
#ifdef LOGFMT_TEST_OTHER_MALLOC
    std::println("logfmt-test: crash inside malloc: skipped, not glibc's malloc");
#else
    std::array<int, 2> err{};
    std::array<int, 2> said{};
    assert(pipe2(err.data(), O_CLOEXEC) == 0 && pipe2(said.data(), O_CLOEXEC) == 0);
    auto const [errRead, errWrite] = err;
    auto const [saidRead, saidWrite] = said;
    nixgrpc::flushLog();
    static_cast<void>(std::fflush(stdout)); // a child that exits would write it again
    pid_t const child = fork();
    assert(child >= 0);
    if (child == 0) {
        // A process of its own: its thread's arena then has only that
        // thread's memory, which the crash's corruption needs.
        constexpr size_t argBytes = 32;
        std::array<char, argBytes> self{};
        std::array<char, argBytes> what{};
        std::ranges::copy(std::string_view("/proc/self/exe"), self.begin());
        std::ranges::copy(crashInsideMalloc, what.begin());
        std::array<char *, 3> argv{self.data(), what.data(), nullptr};
        if (dup2(errWrite, STDERR_FILENO) < 0) {
            std::_Exit(2);
        }
        // dup2 onto itself would leave it to close on exec.
        if ((saidWrite == saidFd ? fcntl(saidWrite, F_SETFD, 0) : dup2(saidWrite, saidFd)) < 0) { // NOLINT(cppcoreguidelines-pro-type-vararg)
            std::_Exit(2);
        }
        execv(self.data(), argv.data());
        std::_Exit(2);
    }
    close(errWrite);
    close(saidWrite);
    // Its lines are read once it says which thread crashes; before, only
    // while a thread but its log writer sleeps writing them (logLine
    // writing in its caller).
    fcntl(errRead, F_SETFL, O_NONBLOCK); // NOLINT(cppcoreguidelines-pro-type-vararg)
    fcntl(saidRead, F_SETFL, O_NONBLOCK); // NOLINT(cppcoreguidelines-pro-type-vararg)
    pid_t tid = 0;
    std::array<char, chunkBytes> chunk{};
    auto const loggerWrites = [child] -> bool {
        return std::ranges::any_of(tasks(child), [](const std::string & task) -> bool {
            return firstLine(task + "/comm") != "log-writer" && writesToStderr(task);
        });
    };
    // NOLINTNEXTLINE(clang-analyzer-unix.BlockInCriticalSection): flushLog let go of the queue's lock; the analyzer does not follow it through the wait.
    for (int i = 0; i < steps && read(saidRead, &tid, sizeof tid) != sizeof tid; i++) {
        if (loggerWrites()) {
            while (read(errRead, chunk.data(), chunk.size()) > 0) {
            }
        }
        std::this_thread::yield();
    }
    close(saidRead);
    auto const crashing = "/proc/" + std::to_string(child) + "/task/" + std::to_string(tid);
    // The crash's polls since the writer last wrote, and how many say it
    // waits for good.
    long const hungPolls = 10L * nixgrpc::StderrLines::crashPatience;
    long since = voluntarySwitches(crashing);
    int status = 0;
    bool exited = false;
    bool waits = false;
    for (int i = 0; tid != 0 && i < steps && !exited && !waits; i++) {
        bool wrote = false;
        while (read(errRead, chunk.data(), chunk.size()) > 0) {
            wrote = true;
        }
        exited = waitpid(child, &status, WNOHANG) == child;
        auto const now = exited ? -1 : voluntarySwitches(crashing);
        since = wrote ? now : since;
        waits = now >= 0 && now - since > hungPolls;
        std::this_thread::yield();
    }
    if (!exited) {
        kill(child, SIGKILL);
        waitpid(child, &status, 0);
    }
    close(errRead);
    std::println("logfmt-test: crash inside malloc: exited={} waits={} status={:#x}", exited, waits, status);
    static_cast<void>(std::fflush(stdout)); // before an assertion ends us
    assert(!waits && exited && "a crash inside malloc waited for the log writer for good");
    // NOLINTNEXTLINE(misc-include-cleaner): <sys/wait.h> has them.
    assert(WIFSIGNALED(status) && WTERMSIG(status) == SIGABRT && "the process did not die of its signal");
#endif
}

} // namespace

auto main(int argc, char ** argv) -> int // NOLINT(misc-const-correctness): main's own signature
try {
    if (argc == 2 && std::string_view(argv[1]) == crashInsideMalloc) { // NOLINT(cppcoreguidelines-pro-bounds-pointer-arithmetic)
        crashesInsideMalloc();
    }
    // Before anything logs, so that it runs after the log writer's own.
    static_cast<void>(std::atexit([] -> void {
        if (logWhileLeaving) {
            nixgrpc::logLine(nixgrpc::LogLevel::info, {{"event", "late"}});
        }
        if (joinWhileLeaving.joinable()) {
            joinWhileLeaving.join();
        }
    }));
    aThreadsLinesKeepTheirOrder();
    // First of those whose child logs: the others wait for their children,
    // which a child that kept its parent's writer would hold up for good.
    aForkedChildLogsOnlyItsOwn();
    aStoppedWriterLeavesOneThread();
    aLineLoggedWhileLeavingGoesOut();
    aLineLoggedWhileTheQueueDrainsGoesAfterIt();
    aForkedChildsWriterWakes(false, "idle");
    aForkedChildsWriterWakes(true, "stalled");
    aLineLoggedWhileTheWriterGoesWaits();
    aChildForkedWhileTheWriterGoesLogs();
    aChildForkedWhileTheTimeZoneIsReadLogs();
    aCrashInsideMallocStillLeaves();
    leavesWithItsLines(Leave::exit, "exit");
    leavesWithItsLines(Leave::exitLogged, "exitLogged");
    leavesWithItsLines(Leave::abort, "abort");
    leavesWithItsLines(Leave::fault, "fault");
    leavesWithItsLines(Leave::fpe, "SIGFPE");
    leavesWithItsLines(Leave::ill, "SIGILL");
    aStalledWriterHoldsNoLock();
    aCrashWithStderrHeldUpStillLeaves();
    aCrashWaitsForAWriterThatWrites();
    aMemoryErrorThatNeedsNoActionStillKills();
    aLineWithNoThreadForItsWriterIsWritten();
    return 0;
} catch (std::exception & err) {
    static_cast<void>(std::fputs((std::string("logfmt-test: ") + err.what() + "\n").c_str(), stdout));
    return 1;
}
