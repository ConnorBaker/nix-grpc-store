#include "takeover.hh"

#include <algorithm>
#include <array>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstring>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include <nix/util/environment-variables.hh>
#include <nix/util/error.hh>
#include <nix/util/file-descriptor.hh>
#include <nix/util/file-system.hh>

#include "logfmt.hh"

#ifdef __linux__
#  include <cstdlib>
#  include <iterator>
#  include <thread>
#  include <utility>

#  include <sys/poll.h>
#  include <sys/socket.h>
#  include <sys/types.h>
#  include <sys/un.h>
#  include <sys/wait.h>
#  include <unistd.h>

#  include "sd-notify.hh"
#endif

#include <stdlib.h> // NOLINT(modernize-deprecated-headers): setenv is POSIX, not in <cstdlib>.

namespace nixgrpc::takeover {

namespace {
constexpr std::string_view readyMsg = "ready";
constexpr std::string_view okMsg = "ok";
constexpr std::array<std::string_view, 2> notifyVars = {"NOTIFY_SOCKET", "WATCHDOG_USEC"};
} // namespace

auto notifyEnv() -> std::string
{
    std::string env;
    for (auto const name : notifyVars) {
        if (auto value = nix::getEnv(std::string(name))) {
            env.append(name).append("=").append(*value).append("\n");
        }
    }
    return env;
}

void applyNotifyEnv(std::string_view env)
{
    while (!env.empty()) {
        auto const end = std::min(env.find('\n'), env.size());
        auto const line = env.substr(0, end);
        env.remove_prefix(std::min(end + 1, env.size()));
        auto const sep = line.find('=');
        if (sep == std::string_view::npos || std::ranges::find(notifyVars, line.substr(0, sep)) == notifyVars.end()) {
            continue;
        }
        // NOLINTNEXTLINE(concurrency-mt-unsafe): before any thread starts.
        ::setenv(std::string(line.substr(0, sep)).c_str(), std::string(line.substr(sep + 1)).c_str(), 1);
    }
}

#ifdef __linux__

namespace {
// Every pending takeover is the reload systemd runs right now, and its
// timeout (TimeoutStartSec=, 90 s by default) kills the successor before
// this runs out, which ends the connection.
constexpr std::chrono::seconds readyTimeout{120};
// The steps of the other side that do not wait for a whole daemon start.
constexpr std::chrono::seconds stepTimeout{30};
constexpr size_t maxFds = 16;
// "ready" and "ok" fit, and so does the sd_notify environment.
constexpr size_t maxMsg = 64;
constexpr size_t maxEnv = 4096;

// In the abstract namespace: it goes away with the process, and the name
// says which main process listens. The peer checks below decide who may use it.
// NOLINTNEXTLINE(misc-include-cleaner): pid_t comes from <sys/types.h>.
auto address(pid_t pid) -> std::pair<sockaddr_un, socklen_t>
{
    sockaddr_un addr{};
    addr.sun_family = AF_UNIX;
    auto const name = "nix-grpc-daemon/takeover/" + std::to_string(pid);
    // sun_path[0] stays '\0'.
    std::ranges::copy(name, std::next(std::begin(addr.sun_path)));
    return {addr, static_cast<socklen_t>(offsetof(sockaddr_un, sun_path) + 1 + name.size())};
}

auto peerCred(int sock) -> ucred
{
    ucred cred{};
    socklen_t len = sizeof(cred);
    // NOLINTNEXTLINE(misc-include-cleaner): SOL_SOCKET and SO_PEERCRED come from <sys/socket.h>.
    if (::getsockopt(sock, SOL_SOCKET, SO_PEERCRED, &cred, &len) != 0) {
        throw nix::SysError("takeover: SO_PEERCRED");
    }
    return cred;
}

// Same unit: same cgroup. Our own uid could be another service's too.
auto sameCgroup(pid_t pid) -> bool
{
    try {
        return nix::readFile(std::string("/proc/self/cgroup"))
               == nix::readFile("/proc/" + std::to_string(pid) + "/cgroup");
    } catch (nix::Error &) {
        return false;
    }
}

// Waits for `sock` to become readable. False on timeout. Throws if `reload`,
// unless -1, hangs up first.
auto readable(int sock, std::chrono::milliseconds timeout, int reload = -1) -> bool
{
    // poll() skips a negative fd.
    std::array<pollfd, 2> fds{{
        {.fd = sock, .events = POLLIN, .revents = 0},
        {.fd = reload, .events = POLLIN, .revents = 0},
    }};
    for (;;) {
        int const res = ::poll(fds.data(), fds.size(), static_cast<int>(timeout.count()));
        if (res < 0 && errno == EINTR) {
            continue;
        }
        if (res < 0) {
            throw nix::SysError("takeover: poll");
        }
        if (std::get<1>(fds).revents != 0) {
            throw nix::Error("takeover: systemd gave up on the reload");
        }
        return res > 0;
    }
}

void sendAll(int sock, std::string_view msg)
{
    if (::send(sock, msg.data(), msg.size(), MSG_NOSIGNAL) != static_cast<ssize_t>(msg.size())) {
        throw nix::SysError("takeover: send");
    }
}

// One message, or nullopt if the peer has gone.
auto receive(int sock) -> std::optional<std::string>
{
    std::array<char, maxMsg> buf{};
    auto const len = ::recv(sock, buf.data(), buf.size(), MSG_DONTWAIT);
    if (len <= 0) {
        return std::nullopt;
    }
    return std::string(buf.data(), static_cast<size_t>(len));
}
} // namespace

Offer::Offer()
    : listener(::socket(AF_UNIX, SOCK_SEQPACKET | SOCK_CLOEXEC | SOCK_NONBLOCK, 0))
{
    if (!listener) {
        throw nix::SysError("takeover: socket");
    }
    auto const [addr, len] = address(::getpid());
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast): sockaddr API.
    if (::bind(listener.get(), reinterpret_cast<const sockaddr *>(&addr), len) != 0
        || ::listen(listener.get(), 1) != 0) {
        throw nix::SysError("takeover: listening");
    }
}

auto Offer::wait(std::chrono::milliseconds timeout) -> Event
{
    switch (state) {
    case State::idle: {
        if (!readable(listener.get(), timeout)) {
            return Event::none;
        }
        nix::AutoCloseFD fresh(::accept4(listener.get(), nullptr, nullptr, SOCK_CLOEXEC));
        if (!fresh) {
            return Event::none;
        }
        auto const cred = peerCred(fresh.get());
        if (cred.uid != ::geteuid() || !sameCgroup(cred.pid)) {
            logLine(
                LogLevel::info,
                {{"event", "takeover_refused"}, {"pid", std::to_string(cred.pid)}, {"uid", std::to_string(cred.uid)}});
            return Event::none;
        }
        conn = std::move(fresh);
        peerPid = cred.pid;
        state = State::asked;
        return Event::requested;
    }
    case State::sent: {
        auto const left = std::chrono::duration_cast<std::chrono::milliseconds>(deadline - std::chrono::steady_clock::now());
        if (left.count() <= 0) {
            return Event::gone;
        }
        if (!readable(conn.get(), std::min(timeout, left))) {
            return Event::none;
        }
        auto const msg = receive(conn.get());
        return msg == readyMsg ? Event::ready : Event::gone;
    }
    case State::asked:
    case State::done:
        readable(-1, timeout);
        return Event::none;
    }
    return Event::none;
}

void Offer::send(const std::vector<int> & fds, std::string_view env)
{
    if (fds.empty() || fds.size() > maxFds) {
        throw nix::Error("takeover: cannot pass %d sockets", fds.size());
    }
    // The env is never empty-length on the wire: a zero-length seqpacket reads as EOF.
    std::string const payload = "\n" + std::string(env);
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-const-cast,misc-include-cleaner): sendmsg only reads it; iovec comes from <sys/socket.h>.
    iovec iov{.iov_base = const_cast<char *>(payload.data()), .iov_len = payload.size()};
    std::vector<char> control(CMSG_SPACE(sizeof(int) * fds.size()));
    msghdr msg{};
    msg.msg_iov = &iov;
    msg.msg_iovlen = 1;
    msg.msg_control = control.data();
    msg.msg_controllen = control.size();
    cmsghdr * cmsg = CMSG_FIRSTHDR(&msg);
    cmsg->cmsg_level = SOL_SOCKET;
    cmsg->cmsg_type = SCM_RIGHTS;
    cmsg->cmsg_len = CMSG_LEN(sizeof(int) * fds.size());
    std::memcpy(CMSG_DATA(cmsg), fds.data(), sizeof(int) * fds.size());
    if (::sendmsg(conn.get(), &msg, MSG_NOSIGNAL) != static_cast<ssize_t>(payload.size())) {
        throw nix::SysError("takeover: sending the sockets");
    }
    state = State::sent;
    deadline = std::chrono::steady_clock::now() + readyTimeout;
}

auto Offer::peerGone() -> bool
{
    if (!readable(conn.get(), std::chrono::milliseconds(0))) {
        return false;
    }
    char byte = 0;
    auto const len = ::recv(conn.get(), &byte, 1, MSG_DONTWAIT | MSG_PEEK);
    return len == 0 || (len < 0 && errno != EAGAIN && errno != EINTR);
}

auto Offer::makeMain(std::chrono::milliseconds timeout) -> bool
{
    sdNotify("MAINPID=" + std::to_string(peerPid));
    return sdNotifyBarrier(timeout) && !peerGone();
}

void Offer::accept()
{
    sendAll(conn.get(), okMsg);
    state = State::done;
    listener.close();
}

void Offer::reject()
{
    conn.close();
    peerPid = 0;
    state = State::idle;
}

Predecessor::Predecessor(pid_t pid, const Detached * reload)
    : pid_(pid)
    , sock(::socket(AF_UNIX, SOCK_SEQPACKET | SOCK_CLOEXEC, 0))
{
    if (!sock) {
        throw nix::SysError("takeover: socket");
    }
    auto const [addr, len] = address(pid);
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast): sockaddr API.
    if (::connect(sock.get(), reinterpret_cast<const sockaddr *>(&addr), len) != 0) {
        throw nix::SysError(
            "process %d offers no takeover (not the main process of a socket-activated node that is its own "
            "scheduler's only builder, or an older nix-grpc-daemon)",
            pid);
    }
    // The name is no proof: anyone can listen on it while the real one is not.
    if (auto const cred = peerCred(sock.get()); cred.pid != pid || cred.uid != ::geteuid()) {
        throw nix::Error("takeover: process %d (uid %d) answered for process %d", cred.pid, cred.uid, pid);
    }
    if (!readable(sock.get(), stepTimeout, reload != nullptr ? reload->reloadFd() : -1)) {
        throw nix::Error("takeover: process %d sent no sockets", pid);
    }
    std::array<char, maxEnv> buf{};
    iovec iov{.iov_base = buf.data(), .iov_len = buf.size()};
    std::vector<char> control(CMSG_SPACE(sizeof(int) * maxFds));
    msghdr msg{};
    msg.msg_iov = &iov;
    msg.msg_iovlen = 1;
    msg.msg_control = control.data();
    msg.msg_controllen = control.size();
    auto const len2 = ::recvmsg(sock.get(), &msg, MSG_CMSG_CLOEXEC);
    if (len2 < 0) {
        throw nix::SysError("takeover: receiving the sockets");
    }
    for (cmsghdr * cmsg = CMSG_FIRSTHDR(&msg); cmsg != nullptr; cmsg = CMSG_NXTHDR(&msg, cmsg)) {
        if (cmsg->cmsg_level == SOL_SOCKET && cmsg->cmsg_type == SCM_RIGHTS) {
            auto const count = (cmsg->cmsg_len - CMSG_LEN(0)) / sizeof(int);
            fds_.resize(count);
            std::memcpy(fds_.data(), CMSG_DATA(cmsg), count * sizeof(int));
        }
    }
    if (len2 == 0 || fds_.empty() || (msg.msg_flags & (MSG_TRUNC | MSG_CTRUNC)) != 0) {
        throw nix::Error("takeover: process %d did not hand over its sockets", pid);
    }
    env_ = std::string_view(buf.data(), static_cast<size_t>(len2)).substr(1);
}

void Predecessor::ready()
{
    sendAll(sock.get(), readyMsg);
    if (!readable(sock.get(), stepTimeout) || receive(sock.get()) != okMsg) {
        throw nix::Error("takeover: process %d did not let go", pid_);
    }
}

auto Predecessor::alive() -> bool
{
    if (!sock) {
        return false;
    }
    if (readable(sock.get(), std::chrono::milliseconds(0))) {
        std::array<char, 1> byte{};
        auto const len = ::recv(sock.get(), byte.data(), byte.size(), MSG_DONTWAIT);
        if (len == 0 || (len < 0 && errno != EAGAIN && errno != EINTR)) {
            sock.close();
            return false;
        }
    }
    return true;
}

Detached::Detached()
{
    nix::Pipe verdictPipe;
    verdictPipe.create();
    // The control process holds the write side: its exit hangs it up.
    nix::Pipe reloadPipe;
    reloadPipe.create();
    auto const child = ::fork();
    if (child < 0) {
        throw nix::SysError("takeover: fork");
    }
    if (child > 0) {
        // systemd's control process.
        verdictPipe.writeSide.close();
        reloadPipe.readSide.close();
        int status = 0;
        while (::waitpid(child, &status, 0) < 0 && errno == EINTR) {
        }
        char byte = 1;
        for (;;) {
            auto const len = ::read(verdictPipe.readSide.get(), &byte, 1);
            if (len < 0 && errno == EINTR) {
                continue;
            }
            ::_exit(len == 1 && byte == 0 ? EXIT_SUCCESS : EXIT_FAILURE);
        }
    }
    // The child between them: forks the daemon and leaves, so the kernel gives
    // the daemon to the manager.
    middle = ::getpid();
    if (auto const daemon = ::fork(); daemon != 0) {
        ::_exit(daemon > 0 ? EXIT_SUCCESS : EXIT_FAILURE);
    }
    verdictPipe.readSide.close();
    reloadPipe.writeSide.close();
    verdict = std::move(verdictPipe.writeSide);
    reload = std::move(reloadPipe.readSide);
}

void Detached::adopted() const
{
    constexpr std::chrono::milliseconds step{10};
    auto const deadline = std::chrono::steady_clock::now() + stepTimeout;
    while (::getppid() == middle) {
        ensureReloadWaits();
        if (std::chrono::steady_clock::now() > deadline) {
            throw nix::Error("takeover: process %d never left", middle);
        }
        std::this_thread::sleep_for(step);
    }
}

void Detached::ensureReloadWaits() const
{
    if (readable(reload.get(), std::chrono::milliseconds(0))) {
        throw nix::Error("takeover: systemd gave up on the reload");
    }
}

void Detached::report(bool succeeded)
{
    char const byte = succeeded ? 0 : 1;
    static_cast<void>(::write(verdict.get(), &byte, 1));
    verdict.close();
}

#else

Offer::Offer()
{
    throw nix::Error("takeover needs Linux");
}

auto Offer::wait(std::chrono::milliseconds /*timeout*/) -> Event
{
    return Event::none;
}

void Offer::send(const std::vector<int> & /*fds*/, std::string_view /*env*/) {}

auto Offer::makeMain(std::chrono::milliseconds /*timeout*/) -> bool
{
    return false;
}

auto Offer::peerGone() -> bool
{
    return true;
}

void Offer::accept() {}

void Offer::reject() {}

Predecessor::Predecessor(pid_t pid, const Detached * /*reload*/)
    : pid_(pid)
{
    throw nix::Error("--take-over needs Linux");
}

void Predecessor::ready() {}

auto Predecessor::alive() -> bool
{
    return false;
}

Detached::Detached()
{
    throw nix::Error("--take-over needs Linux");
}

void Detached::adopted() const {}

void Detached::ensureReloadWaits() const {}

void Detached::report(bool /*succeeded*/) {}

#endif

} // namespace nixgrpc::takeover
