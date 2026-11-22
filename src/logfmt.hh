#pragma once
// Minimal logfmt emitter for the daemon's access log. One key=value line per
// event on stderr, so journald/Loki can parse it and journalctl stays readable.

#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <deque>
#include <memory>
#include <mutex>
#include <optional>
#include <span>
#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <ctime>
#include <initializer_list>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <utility>

#include <poll.h>
#include <pthread.h>
#include <signal.h> // NOLINT(modernize-deprecated-headers): sigaction is POSIX, not in <csignal>.
#include <unistd.h>

#include <grpc/grpc_security_constants.h>
#include <grpcpp/grpcpp.h>

namespace nixgrpc {

// Quote a value if it contains characters that would break key=value parsing.
inline auto logfmtValue(std::string_view value) -> std::string
{
    bool needsQuotes = value.empty();
    for (char const chr : value) {
        if (chr == ' ' || chr == '"' || chr == '=' || chr == '\\' || chr == '\n') {
            needsQuotes = true;
            break;
        }
    }
    if (!needsQuotes) {
        return std::string(value);
    }
    std::string out;
    out.reserve(value.size() + 2);
    out.push_back('"');
    for (char const chr : value) {
        if (chr == '"' || chr == '\\') {
            out.push_back('\\');
            out.push_back(chr);
        } else if (chr == '\n') {
            out += "\\n";
        } else {
            out.push_back(chr);
        }
    }
    out.push_back('"');
    return out;
}

enum class LogLevel : std::uint8_t { info, debug };

using LogField = std::pair<std::string_view, std::string>;

// Append one logfmt line to `line`: ts=… level=… key=value …, with its
// newline.
inline void appendLogLine(std::string & line, LogLevel level, std::span<const LogField> fields)
{
    auto const now = std::chrono::system_clock::now();
    auto const secs = std::chrono::system_clock::to_time_t(now);
    std::tm utc{};
    gmtime_r(&secs, &utc);
    std::array<char, sizeof("2000-01-01T00:00:00Z")> timestamp{};
    static_cast<void>(std::strftime(timestamp.data(), timestamp.size(), "%Y-%m-%dT%H:%M:%SZ", &utc));

    line += "ts=";
    line += timestamp.data();
    line += level == LogLevel::debug ? " level=debug" : " level=info";
    for (const auto & [key, value] : fields) {
        line += ' ';
        line += key;
        line += '=';
        line += logfmtValue(value);
    }
    line += '\n';
}

// Our stderr, written by a thread of its own: a write there that its reader
// holds up (journald under systemd) holds up that thread, and no thread that
// logs. Lines go out in the order they were queued, so a thread's own lines
// in the order it logged them. The process's one writer of the lines it logs.
class StderrLines
{
public:
    // Never destroyed: a static destructor may still log.
    static auto get() -> StderrLines &
    {
        static auto * const lines = [] -> StderrLines * {
            auto * const made = new StderrLines; // NOLINT(cppcoreguidelines-owning-memory): never freed
            instance = made;
            return made;
        }();
        return *lines;
    }

    // `line` ends in its newline.
    void add(std::string line)
    {
        std::unique_lock lock(mutex);
        // While stopWriter lets the writer go, a line waits: one that started
        // a writer then would build it where the one being let go still is.
        // It sleeps on `retiring` itself, not the queue's lock, for as long
        // as the writer takes to write what is queued and end: while
        // stderr's reader holds stderr up, as long as that. The one caller
        // of stopWriter runs before the sandbox, at startup, with no other
        // thread logging. That it sleeps, rather than spins, is what lets a
        // waiter that will never be woken be told from one that is slow
        // (aChildForkedWhileTheWriterGoesLogs finds such a child asleep on a
        // futex); no test tells a sleeping waiter from a spinning one that
        // is woken in time.
        while (retiring) {
            lock.unlock();
            retiring.wait(true);
            lock.lock();
        }
        if (closed) {
            // Exiting: written here, after every line queued before.
            drained.wait(lock, [&] -> bool { return written >= pushed; });
            put(line);
            return;
        }
        if (writer == nullptr) {
            try {
                start();
            } catch (std::system_error &) {
                // No thread for a writer (EAGAIN, at a limit of threads): the
                // line is written by its caller, as each was before there was
                // a writer, and holds up its caller while stderr's reader
                // holds it up. None is queued: the writer goes only once what
                // was queued is written, and a child of fork has none of its
                // parent's.
                put(line);
                ++pushed;
                ++written;
                return;
            }
        }
        lines.push_back(std::move(line));
        ++pushed;
        lock.unlock();
        queued.notify_one();
    }

    // Returns once every line queued before it is written: however long our
    // stderr's reader holds it up.
    void flush() noexcept
    {
        std::unique_lock lock(mutex);
        auto const target = pushed.load();
        drained.wait(lock, [&] -> bool { return written >= target; });
    }

    // Writes what is queued and lets the writer go; the next line starts
    // another: for the sandbox, which wants the process to have one thread
    // when it applies, since a thread already running would escape it. A
    // line logged meanwhile waits for it to be done, then starts a writer.
    void stopWriter() noexcept
    {
        std::thread * running = nullptr;
        pid_t tid = 0;
        {
            std::unique_lock lock(mutex);
            drained.wait(lock, [&] -> bool { return written >= pushed; });
            running = std::exchange(writer, nullptr);
            tid = writerTid;
            retiring = true;
        }
        queued.notify_all();
        if (auto * const hook = whileWriterGoes.load()) {
            hook();
        }
        if (running != nullptr) {
            running->join();
            std::destroy_at(running);
            // A joined thread can still be listed while the kernel finishes
            // its exit, and the sandbox counts the listed ones.
            auto const task = "/proc/self/task/" + std::to_string(tid);
            while (tid != 0 && ::access(task.c_str(), F_OK) == 0) {
                std::this_thread::yield();
            }
        }
        {
            std::scoped_lock const lock(mutex);
            retiring = false;
        }
        retiring.notify_all();
    }

    // For tests only: called by stopWriter once it has told the writer to
    // go, before it joins it, where a line logged by another thread would
    // find no writer.
    static inline std::atomic<void (*)()> whileWriterGoes = nullptr;

    // How many polls of a crash's wait may pass with no line written
    // before the wait gives up: a choice, not a measurement. A test that
    // reads what a crash writes lengthens it, so that it is not failed by
    // being held off the CPU.
    static constexpr int defaultCrashPatience = 1000;
    static inline std::atomic<int> crashPatience = defaultCrashPatience;

    // On a fatal signal, from its handler: waits as flush does, for the
    // writer to write what was queued before, but only while it writes. A
    // crash must end the process: one whose stderr's reader holds it up for
    // good, or that left a lock the writer needs held for good (an abort
    // inside malloc keeps its arena's lock, and the writer frees lines the
    // crashing thread allocated), would otherwise wait for good, and never
    // dump its core nor be restarted. So the wait gives up after
    // crashPatience polls of a millisecond with no line written, and what is
    // left in the queue is lost. Only atomics and poll: what a signal
    // handler may use.
    static void flushOnCrash() noexcept
    {
        auto const * const self = instance.load();
        if (self == nullptr) {
            return;
        }
        auto const target = self->pushed.load();
        auto last = self->written.load();
        int const patience = crashPatience.load();
        for (int idle = 0; last < target && idle < patience;) {
            static_cast<void>(::poll(nullptr, 0, 1));
            auto const now = self->written.load();
            idle = now != last ? 0 : idle + 1;
            last = now;
        }
    }

private:
    StderrLines() = default;

    static inline std::atomic<StderrLines *> instance = nullptr;

    std::mutex mutex;
    std::condition_variable queued;
    std::condition_variable drained;
    std::deque<std::string> lines;
    std::atomic<uint64_t> pushed = 0;
    std::atomic<uint64_t> written = 0;
    std::atomic<pid_t> writerTid = 0;
    bool closed = false;
    std::atomic<bool> retiring = false;
    // Started with the first line, in writerBytes: a child of fork, which
    // has no such thread, drops it without destroying it (that would end the
    // process) and without leaking it.
    alignas(std::thread) std::array<std::byte, sizeof(std::thread)> writerBytes{};
    std::thread * writer = nullptr;

    // Under the lock.
    void start()
    {
        static bool const once = [] -> bool {
            // Normal exit (exit, or a return from main) writes what is queued.
            static_cast<void>(std::atexit([] -> void { get().close(); }));
            // A child of fork has neither the writer nor its lines: they are
            // its parent's. One that goes on logging starts its own.
            static_cast<void>(::pthread_atfork(
                [] -> void { get().mutex.lock(); },
                [] -> void { get().mutex.unlock(); },
                [] -> void { get().forked(); }));
            return true;
        }();
        static_cast<void>(once);
        // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast): storage for the one writer
        writer = std::construct_at(reinterpret_cast<std::thread *>(writerBytes.data()), [this] -> void { write(); });
    }

    // With write(2), not stdio: while our stderr's reader holds this up, it
    // holds no lock another writer of stderr would wait for, so a thread that
    // waits for stderr sleeps in a write of its own. A line that cannot be
    // written is lost, as fputs loses it.
    static void put(std::string_view line) noexcept
    {
        while (!line.empty()) {
            auto const wrote = ::write(STDERR_FILENO, line.data(), line.size());
            if (wrote < 0 && errno == EINTR) {
                continue;
            }
            if (wrote <= 0) {
                return;
            }
            line.remove_prefix(static_cast<size_t>(wrote));
        }
    }

    void write()
    {
        static_cast<void>(::pthread_setname_np(::pthread_self(), "log-writer"));
        writerTid = ::gettid();
        std::unique_lock lock(mutex);
        for (;;) {
            queued.wait(lock, [&] -> bool { return !lines.empty() || closed || retiring; });
            if (lines.empty()) {
                return;
            }
            std::string const line = std::move(lines.front());
            lines.pop_front();
            lock.unlock();
            put(line);
            lock.lock();
            ++written;
            drained.notify_all();
        }
    }

    void close()
    {
        std::thread * running = nullptr;
        {
            std::scoped_lock const lock(mutex);
            closed = true;
            running = writer;
        }
        queued.notify_all();
        if (running != nullptr) {
            running->join();
        }
    }

    // In the child of fork, whose one thread forked while it held the lock.
    void forked()
    {
        lines.clear();
        written = pushed.load();
        writer = nullptr;
        writerTid = 0;
        // A fork while stopWriter let the writer go: the child has no
        // writer to wait for, and its lines would wait for good
        // (logfmt-test's aChildForkedWhileTheWriterGoesLogs).
        retiring = false;
        // POSIX leaves the state of a mutex or condition variable undefined
        // in the child of fork. The mutex was locked by the fork's prepare
        // handler, in the thread that forked; the condition variables may
        // count the parent's waiters (its writer on `queued`, a flush on
        // `drained`), to whom glibc's group switching could give a child's
        // wakeup, or hold their internal lock as another thread left it.
        // All anew. No test fails with the condition variables kept
        // (aForkedChildsWriterWakes passes either way on this glibc).
        std::construct_at(&queued);
        std::construct_at(&drained);
        std::construct_at(&mutex);
    }
};

// Queues `line`, which ends in its newline, for our stderr.
inline void logText(std::string line)
{
    StderrLines::get().add(std::move(line));
}

// Emit one logfmt line on stderr. Formatted now, so its time is the event's.
inline void logLine(LogLevel level, std::span<const LogField> fields)
{
    std::string line;
    appendLogLine(line, level, fields);
    logText(std::move(line));
}

inline void logLine(LogLevel level, std::initializer_list<LogField> fields)
{
    logLine(level, std::span<const LogField>(fields.begin(), fields.size()));
}

// Writes what is queued and lets the log writer go, so that the process has
// no thread of its own but the caller's; the next line starts another writer.
inline void stopLogWriter() noexcept
{
    StderrLines::get().stopWriter();
}

// Returns once every line logged before it is on our stderr.
inline void flushLog() noexcept
{
    StderrLines::get().flush();
}

// Leaves the process at once, as std::_Exit does, once every line logged
// before is on our stderr.
[[noreturn]] inline void exitLogged(int status) noexcept
{
    flushLog();
    std::_Exit(status);
}

// On a fatal signal, the lines logged before it go out first: a crash leaves
// them, as it did while each line was written by its caller. The signal then
// takes its course: its previous action again, raised anew unless the fault
// itself raises it again: one the kernel raised (si_code > 0) recurs once the
// handler returns, with its own siginfo, but for a memory error that needs no
// action (BUS_MCEERR_AO), which is raised anew. The handler runs on the
// crashing thread's stack: a thread that overflowed its stack has none for
// it, and dies at once, without what is queued.
inline void flushLogOnCrash()
{
    static std::array<struct sigaction, NSIG> previous{}; // NOLINT(misc-include-cleaner): <signal.h>
    for (int const sig : {SIGABRT, SIGSEGV, SIGBUS, SIGFPE, SIGILL}) {
        struct sigaction act{};
        act.sa_flags = SA_SIGINFO;
        act.sa_sigaction = [](int sig, siginfo_t * info, void *) -> void {
            StderrLines::flushOnCrash();
            sigaction(sig, &previous.at(static_cast<size_t>(sig)), nullptr);
            if (info == nullptr || info->si_code <= 0 || (sig == SIGBUS && info->si_code == BUS_MCEERR_AO)) {
                static_cast<void>(::raise(sig));
            }
        };
        sigemptyset(&act.sa_mask);
        sigaction(sig, &act, &previous.at(static_cast<size_t>(sig)));
    }
}

// TLS client identity (x509 CN), or nullopt when the client did not present
// a certificate. Kept optional rather than a "-" sentinel so a certificate
// whose CN is literally "-" (or absent, SAN-only) cannot pose as anonymous.
inline auto clientCommonName(const grpc::ServerContextBase & context) -> std::optional<std::string>
{
    auto const auth = context.auth_context();
    if (!auth || auth->FindPropertyValues(GRPC_X509_PEM_CERT_PROPERTY_NAME).empty()) {
        return std::nullopt;
    }
    auto values = auth->FindPropertyValues(GRPC_X509_CN_PROPERTY_NAME);
    if (values.empty()) {
        return "";
    }
    return std::string{values.front().data(), values.front().size()};
}

// The client certificate's SPIFFE ID. gRPC sets the property only for a
// certificate whose single URI SAN is a well-formed SPIFFE ID.
inline auto clientSpiffeId(const grpc::ServerContextBase & context) -> std::optional<std::string>
{
    auto const auth = context.auth_context();
    if (!auth) {
        return std::nullopt;
    }
    auto const values = auth->FindPropertyValues(GRPC_PEER_SPIFFE_ID_PROPERTY_NAME);
    if (values.size() != 1) {
        return std::nullopt;
    }
    return std::string{values.front().data(), values.front().size()};
}

} // namespace nixgrpc
