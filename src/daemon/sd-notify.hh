#pragma once
// sd_notify(3) and sd_notify_barrier(3) without libsystemd.

#include <algorithm>
#include <array>
#include <chrono>
#include <cstddef>
#include <cstring>
#include <iterator>
#include <string_view>

#include <poll.h>
#include <sys/socket.h>
#include <sys/uio.h>
#include <sys/un.h>

#include <nix/util/environment-variables.hh>
#include <nix/util/file-descriptor.hh>

namespace nixgrpc {

// Best effort, errors are ignored. `passFd`, if not -1, goes along as
// SCM_RIGHTS.
inline void sdNotify(std::string_view state, int passFd = -1)
{
    auto path = nix::getEnv("NOTIFY_SOCKET");
    if (!path || path->size() < 2 || path->size() >= sizeof(sockaddr_un::sun_path)) {
        return;
    }
    sockaddr_un addr{};
    addr.sun_family = AF_UNIX;
    std::ranges::copy(*path, std::begin(addr.sun_path));
    if (path->starts_with('@')) {
        addr.sun_path[0] = '\0'; // abstract namespace
    }
    auto const addrLen = static_cast<socklen_t>(offsetof(sockaddr_un, sun_path) + path->size());
    nix::AutoCloseFD const sock(::socket(AF_UNIX, SOCK_DGRAM, 0));
    if (!sock) {
        return;
    }
    iovec iov{.iov_base = const_cast<char *>(state.data()), .iov_len = state.size()}; // NOLINT(cppcoreguidelines-pro-type-const-cast): sendmsg only reads it.
    alignas(cmsghdr) std::array<char, CMSG_SPACE(sizeof(int))> control{};
    msghdr msg{};
    msg.msg_name = &addr;
    msg.msg_namelen = addrLen;
    msg.msg_iov = &iov;
    msg.msg_iovlen = 1;
    if (passFd != -1) {
        msg.msg_control = control.data();
        msg.msg_controllen = control.size();
        cmsghdr * cmsg = CMSG_FIRSTHDR(&msg);
        cmsg->cmsg_level = SOL_SOCKET;
        cmsg->cmsg_type = SCM_RIGHTS;
        cmsg->cmsg_len = CMSG_LEN(sizeof(int));
        std::memcpy(CMSG_DATA(cmsg), &passFd, sizeof(int));
    }
    ::sendmsg(sock.get(), &msg, 0);
}

// Returns once systemd has processed everything sent before, or after
// `timeout`. False on timeout or if there is no systemd.
inline auto sdNotifyBarrier(std::chrono::milliseconds timeout) -> bool
{
    if (!nix::getEnv("NOTIFY_SOCKET")) {
        return false;
    }
    nix::Pipe pipe;
    pipe.create();
    sdNotify("BARRIER=1", pipe.writeSide.get());
    pipe.writeSide.close();
    // systemd closes its copy of the write side, which hangs the read side up.
    pollfd pfd{.fd = pipe.readSide.get(), .events = 0, .revents = 0};
    return ::poll(&pfd, 1, static_cast<int>(timeout.count())) == 1;
}

} // namespace nixgrpc
