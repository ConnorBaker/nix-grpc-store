#pragma once
// systemd socket activation. gRPC cannot adopt a listening socket, but it
// accepts already-connected fds with any ServerCredentials, so we accept()
// ourselves.

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <mutex>
#include <optional>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include <fcntl.h>
#include <poll.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/un.h>
#include <unistd.h>

#include <grpcpp/server_builder.h>

#include <nix/util/environment-variables.hh>
#include <nix/util/file-descriptor.hh>

#include "parse-int.hh"
#include "sd-notify.hh"

namespace nixgrpc {

template<typename T>
inline auto envInt(const char * name) -> std::optional<T>
{
    return parseInt<T>(nix::getEnv(name).value_or(""));
}

// sd_listen_fds(3) without libsystemd. Call before starting threads.
inline auto systemdListenFds() -> std::vector<int>
{
    constexpr int firstFd = 3;
    auto count = envInt<int>("LISTEN_FDS");
    if (!count || envInt<pid_t>("LISTEN_PID") != ::getpid()) {
        return {};
    }
    std::vector<int> fds;
    for (int sockFd = firstFd; sockFd < firstFd + *count; ++sockFd) {
        // NOLINTNEXTLINE(cppcoreguidelines-pro-type-vararg): C API.
        ::fcntl(sockFd, F_SETFD, FD_CLOEXEC);
        fds.push_back(sockFd);
    }
    for (const char * name : {"LISTEN_PID", "LISTEN_FDS", "LISTEN_FDNAMES"}) {
        // NOLINTNEXTLINE(concurrency-mt-unsafe): runs before any thread starts.
        ::unsetenv(name);
    }
    return fds;
}

// Half of WatchdogSec=, or zero if the watchdog is off.
inline auto sdWatchdogInterval() -> std::chrono::microseconds
{
    auto const usec = envInt<uint64_t>("WATCHDOG_USEC");
    return std::chrono::microseconds(usec.value_or(0) / 2);
}

// Accepts on the listen fds and hands the connections to gRPC, one thread per
// fd. The fds stay open: they belong to systemd, and after a takeover the
// successor accepts on them too. After Server::Shutdown() the acceptor drops
// new connections itself.
class Acceptor
{
public:
    using Grpc = grpc::experimental::ExternalConnectionAcceptor;

    Acceptor(std::vector<int> listenFds, std::shared_ptr<Grpc> acceptor)
        : listenFds(std::move(listenFds))
        , acceptor(std::move(acceptor))
    {
    }

    void start()
    {
        run = std::make_shared<Run>();
        run->wake.create();
        run->running = listenFds.size();
        for (int const listenFd : listenFds) {
            std::thread([listenFd, acceptor = acceptor, run = run] -> void { loop(listenFd, *acceptor, *run); })
                .detach();
        }
    }

    // For a successor to accept instead. True once no thread of ours accepts
    // any more. Only another process accepting on the same fds can leave one
    // blocked in accept() past `grace`, on the connection poll() announced:
    // it then hands the next one it gets to gRPC and leaves.
    auto stop(std::chrono::milliseconds grace) -> bool
    {
        auto const current = std::exchange(run, nullptr);
        if (!current) {
            return true;
        }
        current->stopping = true;
        char const wake = 0;
        static_cast<void>(::write(current->wake.writeSide.get(), &wake, 1));
        std::unique_lock lock(current->mutex);
        return current->stopped.wait_for(lock, grace, [&] -> bool { return current->running == 0; });
    }

private:
    struct Run
    {
        nix::Pipe wake;
        std::atomic<bool> stopping{false};
        std::mutex mutex;
        std::condition_variable stopped;
        size_t running = 0;
    };

    std::vector<int> listenFds;
    std::shared_ptr<Grpc> acceptor;
    std::shared_ptr<Run> run;

    static void loop(int listenFd, Grpc & acceptor, Run & run)
    {
        while (!run.stopping) {
            // poll() first, so stop() can wake us: the listen fds are shared,
            // so neither shutdown() nor O_NONBLOCK on them is ours to use.
            std::array<pollfd, 2> fds{{
                {.fd = listenFd, .events = POLLIN, .revents = 0},
                {.fd = run.wake.readSide.get(), .events = POLLIN, .revents = 0},
            }};
            if (::poll(fds.data(), fds.size(), -1) < 0) {
                if (errno == EINTR) {
                    continue;
                }
                break;
            }
            if (std::get<1>(fds).revents != 0) {
                break;
            }
            // No accept4 on macOS.
            int const conn = ::accept(listenFd, nullptr, nullptr);
            if (conn < 0) {
                if (errno == EINTR || errno == ECONNABORTED || errno == EMFILE || errno == ENFILE || errno == EAGAIN) {
                    continue;
                }
                break;
            }
            // NOLINTBEGIN(cppcoreguidelines-pro-type-vararg): C API.
            ::fcntl(conn, F_SETFD, FD_CLOEXEC);
            ::fcntl(conn, F_SETFL, static_cast<unsigned>(std::max(0, ::fcntl(conn, F_GETFL))) | unsigned{O_NONBLOCK});
            // NOLINTEND(cppcoreguidelines-pro-type-vararg)
            Grpc::NewConnectionParameters params;
            params.listener_fd = listenFd;
            params.fd = conn;
            acceptor.HandleNewConnection(&params);
        }
        const std::scoped_lock lock(run.mutex);
        --run.running;
        run.stopped.notify_all();
    }
};

} // namespace nixgrpc
