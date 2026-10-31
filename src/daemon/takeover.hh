#pragma once
// Hands a socket-activated daemon over to its next generation inside the
// same systemd unit, for a node that is its own scheduler's only builder:
// draining there would refuse every new build until the slowest running one
// is done, since nobody else takes them, and systemd starts no second process
// of the unit from the socket while the old one lives.
//
// `systemctl reload` runs the new generation as ExecReload with
// `--take-over $MAINPID`. It forks twice (Detached): the parent is systemd's
// control process and exits once the takeover is done, the grandchild goes on
// as the daemon, and the child between them leaves at once, so that the
// daemon becomes a child of systemd's manager. systemd treats a main process
// that is not its child as an alien: on stop it does not wait for it to exit
// but goes on to SIGKILL, which KillMode=mixed sends to the whole unit.
//
// The daemon connects to the main process (Predecessor -> Offer), which stops
// accepting and sends the listening sockets plus the sd_notify environment
// systemd gave it. When the daemon serves, it says so. The old process then
// makes it the unit's main process (MAINPID=), and once systemd has read that
// and the new one still lives, drains as an ordinary process of the unit;
// otherwise it serves on. The connection between the two stays open for as
// long as the old process lives, so the new one neither idles out nor stops
// before it is gone: systemd kills the rest of the unit (KillMode=mixed) as
// soon as the main process exits.

#include <chrono>
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include <sys/types.h>

#include <nix/util/file-descriptor.hh>

namespace nixgrpc::takeover {

class Detached;

#ifdef __linux__
constexpr bool supported = true;
#else
constexpr bool supported = false;
#endif

// The main process's side. The serve loop drives it, from one thread.
class Offer
{
public:
    // Listens where Predecessor looks for this process.
    Offer();

    enum class Event : uint8_t {
        none,      // nothing happened within the timeout
        requested, // a process of this unit asks: send() or reject()
        ready,     // it serves now: accept() or reject()
        gone,      // it left or did not get ready in time: reject() and serve again
    };

    auto wait(std::chrono::milliseconds timeout) -> Event;

    // Who asked.
    [[nodiscard]] auto peer() const -> pid_t
    {
        return peerPid;
    }

    void send(const std::vector<int> & fds, std::string_view env);
    // After `ready`: asks systemd to make the successor the unit's main
    // process and waits up to `timeout` until systemd has read that. True if
    // the successor lives then, so it lived when systemd checked it. False if
    // it is gone, which systemd refuses, so this one is still the main
    // process: reject() and serve on.
    auto makeMain(std::chrono::milliseconds timeout) -> bool;
    // Confirms to the successor that it is the main process now. The
    // connection stays open until this process exits.
    void accept();
    void reject();

private:
    auto peerGone() -> bool;

    enum class State : uint8_t {
        idle,
        asked,
        sent,
        done,
    };

    nix::AutoCloseFD listener;
    nix::AutoCloseFD conn;
    pid_t peerPid = 0;
    State state = State::idle;
    std::chrono::steady_clock::time_point deadline;
};

// The successor's side.
class Predecessor
{
public:
    // Connects to main process `pid` and receives what Offer::send() sent.
    // Throws if systemd gives up on `reload`, if any, meanwhile.
    explicit Predecessor(pid_t pid, const Detached * reload = nullptr);

    [[nodiscard]] auto fds() const -> const std::vector<int> &
    {
        return fds_;
    }

    [[nodiscard]] auto env() const -> const std::string &
    {
        return env_;
    }

    [[nodiscard]] auto pid() const -> pid_t
    {
        return pid_;
    }

    // We serve. Returns once the predecessor has made us the main process.
    void ready();
    // False once the predecessor has exited.
    auto alive() -> bool;

private:
    pid_t pid_;
    nix::AutoCloseFD sock;
    std::vector<int> fds_;
    std::string env_;
};

// The forks for ExecReload, before any thread exists. The parent is systemd's
// control process: it exits with what the daemon reports. The daemon, its
// grandchild, returns. systemd kills only the control process of a reload
// that runs out of time; the daemon then gives up. PR_SET_PDEATHSIG cannot
// tie it to the control process, whose child it no longer is. A thread
// watching for it would have to start after sandbox::apply, which refuses to
// confine threads that already run, and so after the wait for the sockets,
// where a reload most likely runs out of time. Instead that wait watches the
// control process, and the daemon checks again before it says it serves; a
// daemon that hangs in between is not ended.
class Detached
{
public:
    Detached();
    // Hangs up once the control process is gone: for Predecessor to watch
    // while it waits.
    [[nodiscard]] auto reloadFd() const -> int
    {
        return reload.get();
    }
    // Returns once the manager has adopted the daemon, which it supervises as
    // a main process only then (see above). Throws if the control process is
    // gone.
    void adopted() const;
    // Throws if the control process is gone, so a daemon whose reload systemd
    // gave up on does not take over.
    void ensureReloadWaits() const;
    void report(bool succeeded);

private:
    pid_t middle = 0;
    nix::AutoCloseFD verdict;
    nix::AutoCloseFD reload;
};

// What systemd told this process about sd_notify, as Offer::send() passes it.
auto notifyEnv() -> std::string;
// Sets what notifyEnv() returned, and nothing else. Before any thread exists.
void applyNotifyEnv(std::string_view env);

} // namespace nixgrpc::takeover
