#pragma once
// The long-running `niks3 push --stdin` child and acknowledged publishing through it.

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <exception>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <random>
#include <set>
#include <stop_token>
#include <string>
#include <string_view>
#include <thread>
#include <tuple>
#include <utility>
#include <vector>

#include <curl/curl.h>
#include <sys/ioctl.h>
#include <poll.h>
#include <unistd.h>
#include <nlohmann/json.hpp>

#include <nix/util/error.hh>
#include <nix/util/sync.hh>
#include <nix/util/file-descriptor.hh>
#include <nix/util/serialise.hh>
#include <nix/util/util.hh>
#include <nix/util/processes.hh>

#include "logfmt.hh"

namespace nixgrpc {

// Polled about once a second by blocking waits so a gone client or a
// daemon shutdown does not pin a handler thread forever.
using Cancelled = std::function<bool()>;
constexpr std::chrono::seconds cancelPoll{1};

struct CancelledWait : nix::Error
{
    using nix::Error::Error;
};


// One long-lived `niks3 push --stdin`. A request line names paths and blocks
// until every one was acked. If the child dies all waiters fail and the next
// request respawns it.
class PushProcess
{
public:
    using Argv = std::vector<std::string>;

    // For tests only: called by the reader once the child is reaped and
    // what its stderr's pipe holds is measured, before that is copied, where
    // a process left holding the pipe may write more. It runs under the
    // State lock: a hook that called pushWait or pushLog would wait for
    // that lock for good.
    static inline std::atomic<void (*)()> afterStderrMeasured = nullptr;

    explicit PushProcess(Argv argv)
        : argv(std::move(argv))
    {
    }

    PushProcess(const PushProcess &) = delete;
    PushProcess(PushProcess &&) = delete;
    auto operator=(const PushProcess &) -> PushProcess & = delete;
    auto operator=(PushProcess &&) -> PushProcess & = delete;

    ~PushProcess()
    {
        // Before stdin closes: the queued logs still go to the child.
        logWriter = {};
        {
            auto lck = state.lock();
            lck->stdinFd.reset(); // EOF: child drains and exits
        }
        if (reader.joinable()) {
            reader.join();
        }
    }

    // What niks3 signed each path with. Paths it already had come without.
    using Signatures = std::map<std::string, std::set<std::string>>;

    // Returns after niks3 committed all of `paths`.
    auto pushWait(const std::vector<std::string> & paths, const Cancelled & cancelled) -> Signatures
    {
        auto const pending = std::make_shared<Pending>(paths.size());
        uint64_t reqId = 0;
        std::shared_ptr<nix::AutoCloseFD> stdinFd;
        {
            auto lck = state.lock();
            ensureChild(lck);
            reqId = ++lck->lastId;
            lck->waiting[reqId] = pending;
            stdinFd = lck->stdinFd;
        }
        // Not under the State lock: a full stdin pipe must not keep readAcks
        // from draining the child's stdout.
        nlohmann::json const req{{"id", reqId}, {"paths", paths}};
        try {
            std::scoped_lock const one(writeMutex);
            nix::writeFull(stdinFd->get(), req.dump() + "\n", false);
        } catch (...) {
            state.lock()->waiting.erase(reqId);
            throw;
        }
        auto res = pending->wait(cancelled);
        if (res.status == "cancelled") {
            state.lock()->waiting.erase(reqId);
            throw CancelledWait("niks3 push: %s", res.message);
        }
        if (res.status != "ok") {
            throw nix::Error("niks3 push: %s", res.message);
        }
        return std::move(res.signatures);
    }

    // Uploads the log Nix kept of `drvPath`. Returns at once: a log must not
    // delay the result of its build. The outcome goes to our own log.
    void pushLog(std::string drvPath)
    {
        std::optional<std::string> dropped;
        {
            std::scoped_lock const lock(logsMutex);
            if (logs.size() >= maxQueuedLogs) {
                dropped = std::move(logs.front());
                logs.pop_front();
            }
            logs.push_back(std::move(drvPath));
            if (!logWriter.joinable()) {
                logWriter = std::jthread([this](const std::stop_token & stop) -> void { writeLogs(stop); });
            }
            logQueued.notify_one();
        }
        // Not under logsMutex: a write to our stderr that its reader holds up
        // must not hold up the log writer, nor another pushLog, with it.
        if (dropped) {
            logLine(LogLevel::info, {{"event", "build_log_dropped"}, {"drv", *dropped}});
        }
    }

private:
    // ack()/fail() run under the State lock, wait() under its own.
    struct Pending
    {
        void report()
        {
            if (log.empty()) {
                return;
            }
            // Every path was acked or failed: the wait returns at once.
            auto const res = wait([] -> bool { return false; });
            if (res.status == "ok") {
                logLine(LogLevel::info, {{"event", "build_log_pushed"}, {"drv", log}});
            } else {
                logLine(LogLevel::info, {{"event", "build_log_push_failed"}, {"drv", log}, {"why", res.message}});
            }
        }

        struct Result
        {
            std::string status = "ok";
            std::string message;
            Signatures signatures;
        };

        // `log` names the derivation of a build log, which nobody waits for.
        explicit Pending(size_t count, std::string log = {})
            : log(std::move(log))
            , sync(Inner{.left = count})
        {
        }

        // True once every path of the request was acked.
        struct Ack
        {
            std::string status;
            std::string message;
            std::string path;
            std::set<std::string> signatures;
        };

        auto ack(Ack msg) -> bool
        {
            auto inner = sync.lock();
            if (msg.status != "ok") {
                inner->res.status = std::move(msg.status);
                inner->res.message = std::move(msg.message);
            } else if (!msg.signatures.empty()) {
                inner->res.signatures[msg.path] = std::move(msg.signatures);
            }
            if (inner->left > 0) {
                inner->left--;
            }
            if (inner->left == 0) {
                done.notify_all();
            }
            return inner->left == 0;
        }

        void fail(const std::string & message)
        {
            auto inner = sync.lock();
            inner->res.status = "error";
            inner->res.message = message;
            inner->left = 0;
            done.notify_all();
        }

        auto wait(const Cancelled & cancelled) -> Result
        {
            auto inner = sync.lock();
            while (inner->left > 0) {
                if (cancelled()) {
                    return {.status = "cancelled", .message = "gave up waiting for niks3 push"};
                }
                inner.wait_for(done, cancelPoll);
            }
            return std::move(inner->res);
        }

    private:
        struct Inner
        {
            size_t left;
            Result res;
        };
        std::string log;
        nix::Sync<Inner> sync;
        std::condition_variable done;
    };

    struct State
    {
        nix::Pid pid;
        std::shared_ptr<nix::AutoCloseFD> stdinFd;
        uint64_t lastId = 0;
        std::map<uint64_t, std::shared_ptr<Pending>> waiting;
    };

    Argv argv;
    nix::Sync<State> state;
    std::mutex writeMutex;
    std::thread reader;

    // A child that reads slowly may hold up a write, so build logs go
    // through a thread of their own, and the oldest gives way.
    static constexpr size_t maxQueuedLogs = 64;
    std::mutex logsMutex;
    std::deque<std::string> logs;
    std::condition_variable_any logQueued;
    std::jthread logWriter;

    void writeLogs(const std::stop_token & stop)
    {
        for (;;) {
            std::string drvPath;
            {
                std::unique_lock lock(logsMutex);
                logQueued.wait(lock, stop, [&] -> bool { return !logs.empty(); });
                if (logs.empty()) {
                    return;
                }
                drvPath = std::move(logs.front());
                logs.pop_front();
            }
            // Any failure, not only Nix's: at a limit of threads the child's
            // reader cannot start (std::system_error), and an exception that
            // left this thread would end the process.
            try {
                sendLog(drvPath);
            } catch (std::exception & err) {
                logLine(LogLevel::info, {{"event", "build_log_push_failed"}, {"drv", drvPath}, {"why", err.what()}});
            }
        }
    }

    void sendLog(const std::string & drvPath)
    {
        auto const pending = std::make_shared<Pending>(1, drvPath);
        uint64_t reqId = 0;
        std::shared_ptr<nix::AutoCloseFD> stdinFd;
        {
            auto lck = state.lock();
            ensureChild(lck);
            reqId = ++lck->lastId;
            lck->waiting[reqId] = pending;
            stdinFd = lck->stdinFd;
        }
        nlohmann::json const req{{"id", reqId}, {"log", drvPath}};
        try {
            std::scoped_lock const one(writeMutex);
            nix::writeFull(stdinFd->get(), req.dump() + "\n", false);
        } catch (...) {
            state.lock()->waiting.erase(reqId);
            throw;
        }
    }

    void ensureChild(nix::Sync<State>::WriteLock & lck)
    {
        if (lck->stdinFd) {
            return;
        }
        if (reader.joinable()) {
            reader.join(); // previous child is gone, its reader returned
        }
        nix::Pipe toChild;
        nix::Pipe fromChild;
        nix::Pipe childErr;
        toChild.create();
        fromChild.create();
        childErr.create();
        // dieWithParent (PDEATHSIG) fires when the forking gRPC thread exits.
        lck->pid = nix::startProcess(
            [&] -> void {
                if (dup2(toChild.readSide.get(), STDIN_FILENO) == -1
                    || dup2(fromChild.writeSide.get(), STDOUT_FILENO) == -1
                    || dup2(childErr.writeSide.get(), STDERR_FILENO) == -1) {
                    throw nix::SysError("dup2");
                }
                nix::Strings const args(argv.begin(), argv.end());
                execvp(argv.front().c_str(), nix::stringsToCharPtrs(args).data());
                throw nix::SysError("exec %s", argv.front());
            },
            {.dieWithParent = false});
        // The reader before the child is published: at a limit of threads
        // (EAGAIN) it cannot start, and the child is reaped and the push
        // fails to its caller; the next push starts another child.
        try {
            reader = std::thread([this,
                                  acks = std::make_shared<nix::AutoCloseFD>(std::move(fromChild.readSide)),
                                  errs = std::make_shared<nix::AutoCloseFD>(std::move(childErr.readSide))] -> void {
                readChild(acks->get(), errs->get());
            });
        } catch (...) {
            lck->pid.kill();
            throw;
        }
        lck->stdinFd = std::make_shared<nix::AutoCloseFD>(std::move(toChild.writeSide));
        logLine(LogLevel::info, {{"event", "niks3_push_started"}, {"pid", std::to_string(static_cast<pid_t>(lck->pid))}});
    }

    enum class Copied : std::uint8_t { some, failed, ended };

    static constexpr size_t chunkBytes = 4096;

    // Reads what `errFd` has, at most `most` bytes, and logs each line it
    // completes; the rest of a line waits in `pending`. Called only where
    // the pipe has something or has ended (poll said so, or FIONREAD), so
    // the read does not block. A read that fails is said once, and what it
    // would have read is lost. `ended` once every holder of the pipe is
    // gone. `most` goes down by what was read.
    static auto copyAvailable(int errFd, std::string & pending, bool & lost, size_t & most) -> Copied
    {
        std::array<char, chunkBytes> chunk{};
        auto const got = ::read(errFd, chunk.data(), std::min(chunk.size(), most));
        if (got < 0) {
            if (errno == EINTR) {
                return Copied::some;
            }
            if (!lost) {
                lost = true;
                logLine(LogLevel::info, {{"event", "niks3_push_stderr_lost"}, {"why", nix::SysError("reading niks3 push's stderr").what()}});
            }
            return Copied::failed;
        }
        if (got == 0) {
            return Copied::ended;
        }
        most -= static_cast<size_t>(got);
        pending.append(chunk.data(), static_cast<size_t>(got));
        for (auto end = pending.find('\n'); end != std::string::npos; end = pending.find('\n')) {
            logText(pending.substr(0, end + 1));
            pending.erase(0, end + 1);
        }
        return Copied::some;
    }

    // The child's stdout and stderr, read by the one thread. Its stdout has
    // its acks. Its stderr (niks3 push: a line per path it uploads, before
    // its ack) goes to ours as our lines do, through logText: a held-up
    // stderr must hold up neither the child nor its acks; a last line
    // without its newline gets one. A read of stderr that fails is said once
    // and reading goes on, or the child would sleep once the pipe is full.
    // Once its stdout ends, the child is reaped: what it wrote to its stderr
    // is then in the pipe, and what the pipe holds then is copied, and no
    // more, under the State lock: a process it started and left running
    // (niks3's token command inherits its stderr) may hold the pipe for as
    // long as it runs, and write to it as fast as it is read. What such a
    // process writes after is lost, and once the reader is done, its writes
    // there fail (EPIPE, or SIGPIPE unless it ignores that).
    void readChild(int acksFd, int errFd)
    {
        std::string acked;
        std::string logged;
        bool lost = false;
        bool errOpen = true;
        std::string why;
        try {
            while (why.empty()) {
                std::array<pollfd, 2> fds{{{.fd = acksFd, .events = POLLIN, .revents = 0}, {.fd = errOpen ? errFd : -1, .events = POLLIN, .revents = 0}}};
                if (::poll(fds.data(), fds.size(), -1) < 0) {
                    if (errno == EINTR) {
                        continue;
                    }
                    throw nix::SysError("polling niks3 push's output");
                }
                if (fds.at(1).revents != 0) {
                    size_t most = chunkBytes;
                    errOpen = copyAvailable(errFd, logged, lost, most) != Copied::ended;
                }
                if (fds.at(0).revents != 0) {
                    why = readAcks(acksFd, acked);
                }
            }
        } catch (std::exception & err) {
            why = err.what();
        }
        auto lck = state.lock();
        lck->stdinFd.reset();
        int const status = lck->pid.kill();
        int held = 0;
        if (errOpen && ::ioctl(errFd, FIONREAD, &held) == 0) { // NOLINT(cppcoreguidelines-pro-type-vararg)
            if (auto * const hook = afterStderrMeasured.load()) {
                hook();
            }
            for (auto most = static_cast<size_t>(held); most > 0 && copyAvailable(errFd, logged, lost, most) == Copied::some;) {
            }
        }
        if (!logged.empty()) {
            logText(logged + "\n");
        }
        logLine(LogLevel::info, {{"event", "niks3_push_exited"}, {"status", nix::statusToString(status)}, {"why", why}});
        for (auto & [reqId, pending] : lck->waiting) {
            pending->fail(why);
            pending->report();
        }
        lck->waiting.clear();
    }

    // Reads what `acksFd` has and takes each ack it completes; the rest of
    // a line waits in `acked`. Why the child's stdout ended, once it has.
    auto readAcks(int acksFd, std::string & acked) -> std::string
    {
        constexpr size_t chunkBytes = 4096;
        std::array<char, chunkBytes> chunk{};
        auto const got = ::read(acksFd, chunk.data(), chunk.size());
        if (got < 0) {
            if (errno == EINTR) {
                return {};
            }
            throw nix::SysError("reading niks3 push's acks");
        }
        if (got == 0) {
            return "niks3 push exited";
        }
        acked.append(chunk.data(), static_cast<size_t>(got));
        for (auto end = acked.find('\n'); end != std::string::npos; end = acked.find('\n')) {
            takeAck(acked.substr(0, end));
            acked.erase(0, end + 1);
        }
        return {};
    }

    void takeAck(const std::string & line)
    {
        auto const ack = nlohmann::json::parse(line, nullptr, /*allow_exceptions=*/false);
        if (!ack.is_object()) {
            return;
        }
        auto lck = state.lock();
        auto const found = lck->waiting.find(ack.value("id", uint64_t{0}));
        if (found == lck->waiting.end()) {
            return;
        }
        if (found->second->ack(
                {.status = ack.value("status", "error"),
                 .message = ack.value("message", ""),
                 .path = ack.value("path", ""),
                 .signatures = ack.value("signatures", std::set<std::string>{})})) {
            found->second->report();
            lck->waiting.erase(found);
        }
    }
};

} // namespace nixgrpc
