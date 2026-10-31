// The handshake of takeover.hh between processes of one test, as the old
// and new generation of a unit would run it.

#include <algorithm>
#include <array>
#include <atomic>
#include <cassert>
#include <chrono>
#include <cstddef>
#include <cstring>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include <linux/prctl.h>
#include <sys/poll.h>
#include <sys/prctl.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <unistd.h>

#include <signal.h> // NOLINT(modernize-deprecated-headers): kill and SIGKILL are POSIX, not in <csignal>.
#include <stdlib.h> // NOLINT(modernize-deprecated-headers): WIFEXITED is POSIX.

#include <nix/util/environment-variables.hh>
#include <nix/util/error.hh>
#include <nix/util/file-descriptor.hh>

#include "takeover.hh"

using nixgrpc::takeover::Detached;
using nixgrpc::takeover::Offer;
using nixgrpc::takeover::Predecessor;
using Event = Offer::Event;

namespace {

constexpr std::chrono::milliseconds kStep{100};
constexpr int kSteps = 100;
// Longer than any sd_notify message these tests send.
constexpr size_t kMessage = 256;

// Runs `body` in a child; its exit status is whether it held.
// NOLINTNEXTLINE(misc-include-cleaner): pid_t comes from <unistd.h>.
auto spawn(const std::function<bool()> & body) -> pid_t
{
    auto const pid = ::fork();
    assert(pid >= 0);
    if (pid == 0) {
        bool held = false;
        try {
            held = body();
        } catch (...) { // NOLINT(bugprone-empty-catch): the exit status says it
        }
        ::_exit(held ? 0 : 1);
    }
    return pid;
}

auto exitStatus(pid_t pid) -> int
{
    int status = 0;
    assert(::waitpid(pid, &status, 0) == pid);
    return WIFEXITED(status) ? WEXITSTATUS(status) : -1;
}

auto next(Offer & offer) -> Event
{
    for (int i = 0; i < kSteps; ++i) {
        if (auto const event = offer.wait(kStep); event != Event::none) {
            return event;
        }
    }
    return Event::none;
}

void theSuccessorGetsTheSocketsAndSeesThePredecessorGo()
{
    auto offer = std::make_unique<Offer>();
    auto const self = ::getpid();
    // Stands in for a listening socket: whatever reads it reads what we write.
    nix::Pipe listening;
    listening.create();
    auto const child = spawn([&] -> bool {
        listening.writeSide.close();
        Predecessor pred(self);
        bool held = pred.pid() == self && pred.fds().size() == 1;
        char byte = 0;
        held = held && ::read(pred.fds().front(), &byte, 1) == 1 && byte == 'x';
        nixgrpc::takeover::applyNotifyEnv(pred.env());
        held = held && nix::getEnv("NOTIFY_SOCKET") == "/run/systemd/notify" && nix::getEnv("WATCHDOG_USEC") == "15000000"
               && !nix::getEnv("LD_PRELOAD");
        pred.ready();
        held = held && pred.alive();
        for (int i = 0; i < kSteps && pred.alive(); ++i) {
            std::this_thread::sleep_for(kStep);
        }
        return held && !pred.alive();
    });
    assert(next(*offer) == Event::requested);
    assert(offer->peer() == child);
    assert(::write(listening.writeSide.get(), "x", 1) == 1);
    offer->send({listening.readSide.get()}, "NOTIFY_SOCKET=/run/systemd/notify\nWATCHDOG_USEC=15000000\nLD_PRELOAD=/evil.so\n");
    assert(next(*offer) == Event::ready);
    offer->accept();
    // The successor still sees us.
    std::this_thread::sleep_for(3 * kStep);
    offer.reset(); // as when this process exits
    assert(exitStatus(child) == 0);
}

void aSuccessorThatDiesBeforeItServesGivesTheNodeBack()
{
    Offer offer;
    auto const self = ::getpid();
    nix::Pipe listening;
    listening.create();
    auto const child = spawn([&] -> bool {
        Predecessor const pred(self);
        return true; // and gone, without ready()
    });
    assert(next(offer) == Event::requested);
    offer.send({listening.readSide.get()}, "");
    assert(next(offer) == Event::gone);
    offer.reject();
    assert(exitStatus(child) == 0);
    // And the next one may try.
    auto const again = spawn([&] -> bool {
        Predecessor pred(self);
        pred.ready();
        return true;
    });
    assert(next(offer) == Event::requested);
    offer.send({listening.readSide.get()}, "");
    assert(next(offer) == Event::ready);
    offer.accept();
    assert(exitStatus(again) == 0);
}

void aRejectedSuccessorDoesNotTakeOver()
{
    Offer offer;
    auto const self = ::getpid();
    nix::Pipe listening;
    listening.create();
    auto const child = spawn([&] -> bool {
        Predecessor pred(self);
        try {
            pred.ready();
        } catch (nix::Error &) {
            return true;
        }
        return false;
    });
    assert(next(offer) == Event::requested);
    offer.send({listening.readSide.get()}, "");
    assert(next(offer) == Event::ready);
    offer.reject();
    assert(exitStatus(child) == 0);
}

void noTakeoverFromAProcessThatOffersNone()
{
    auto const sleeper = spawn([] -> bool {
        std::this_thread::sleep_for(kStep * kSteps);
        return true;
    });
    bool refused = false;
    try {
        Predecessor const pred(sleeper);
    } catch (nix::Error &) {
        refused = true;
    }
    assert(refused);
    ::kill(sleeper, SIGKILL);
    exitStatus(sleeper);
}

// The name of an offer is no proof of who listens on it.
void noTakeoverFromAnImpostor()
{
    auto const victim = spawn([] -> bool {
        std::this_thread::sleep_for(kStep * kSteps);
        return true;
    });
    // takeover.cc's name for the victim's offer, taken by us.
    nix::AutoCloseFD const impostor(::socket(AF_UNIX, SOCK_SEQPACKET | SOCK_CLOEXEC, 0));
    sockaddr_un addr{};
    addr.sun_family = AF_UNIX;
    auto const name = "nix-grpc-daemon/takeover/" + std::to_string(victim);
    name.copy(&addr.sun_path[1], name.size());
    auto const len = static_cast<socklen_t>(offsetof(sockaddr_un, sun_path) + 1 + name.size());
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast): sockaddr API.
    assert(::bind(impostor.get(), reinterpret_cast<const sockaddr *>(&addr), len) == 0);
    assert(::listen(impostor.get(), 1) == 0);
    bool refused = false;
    try {
        Predecessor const pred(victim);
    } catch (nix::Error & err) {
        refused = std::string(err.what()).contains("answered for");
    }
    assert(refused);
    ::kill(victim, SIGKILL);
    exitStatus(victim);
}

// What a unit's ExecReload sees: the status the child reports.
void theReloadExitsWithTheChildsVerdict()
{
    for (bool const succeeded : {true, false}) {
        auto const reload = spawn([succeeded] -> bool {
            Detached detached; // returns in the child only
            detached.report(succeeded);
            ::_exit(0);
        });
        assert(exitStatus(reload) == (succeeded ? 0 : 1));
    }
    auto const crashed = spawn([] -> bool {
        Detached const detached;
        ::_exit(0); // without a report
    });
    assert(exitStatus(crashed) == 1);
}

// What the daemon of a reload says once the test lets it go on.
auto verdictOf(nix::Pipe & said) -> char
{
    said.writeSide.close();
    pollfd pfd{.fd = said.readSide.get(), .events = POLLIN, .revents = 0};
    // Well within the 30 s it would wait on its own.
    assert(::poll(&pfd, 1, static_cast<int>((kStep * kSteps / 2).count())) == 1);
    char verdict = 0;
    assert(::read(said.readSide.get(), &verdict, 1) == 1);
    return verdict;
}

// A daemon whose reload systemd gave up on, and killed, does not take over.
void aDaemonWhoseReloadWasKilledGivesUp()
{
    for (bool const killed : {true, false}) {
        nix::Pipe daemonUp;
        daemonUp.create();
        nix::Pipe goOn;
        goOn.create();
        nix::Pipe said;
        said.create();
        auto const reload = spawn([&] -> bool {
            Detached detached;
            char byte = 0;
            assert(::write(daemonUp.writeSide.get(), &byte, 1) == 1);
            assert(::read(goOn.readSide.get(), &byte, 1) == 1);
            char verdict = 'k';
            try {
                detached.ensureReloadWaits();
            } catch (nix::Error &) {
                verdict = 'g';
            }
            assert(::write(said.writeSide.get(), &verdict, 1) == 1);
            detached.report(true);
            ::_exit(0);
        });
        goOn.readSide.close();
        // The daemon is there before its reload goes.
        char byte = 0;
        assert(::read(daemonUp.readSide.get(), &byte, 1) == 1);
        if (killed) {
            ::kill(reload, SIGKILL);
            exitStatus(reload);
        }
        assert(::write(goOn.writeSide.get(), &byte, 1) == 1);
        assert(verdictOf(said) == (killed ? 'g' : 'k'));
        if (!killed) {
            assert(exitStatus(reload) == 0);
        }
    }
}

// Nor does one still waiting for the sockets when systemd kills its reload.
void theWaitForTheSocketsEndsWithTheReload()
{
    Offer offer;
    auto const self = ::getpid();
    nix::Pipe said;
    said.create();
    auto const reload = spawn([&] -> bool {
        Detached const detached;
        char verdict = 'p';
        try {
            Predecessor const pred(self, &detached);
        } catch (nix::Error &) {
            verdict = 'g';
        }
        assert(::write(said.writeSide.get(), &verdict, 1) == 1);
        ::_exit(0);
    });
    assert(next(offer) == Event::requested);
    ::kill(reload, SIGKILL);
    exitStatus(reload);
    assert(verdictOf(said) == 'g');
    offer.reject();
}

// systemd supervises a main process that is not its child as an alien: it
// does not wait for it on stop. The daemon is the manager's child, which this
// test plays as a subreaper, as systemd's user managers are.
void theDaemonBecomesTheManagersChild()
{
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-vararg): prctl(2) is variadic.
    assert(::prctl(PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0) == 0);
    nix::Pipe pids;
    pids.create();
    auto const reload = spawn([&] -> bool {
        Detached detached;
        detached.adopted();
        std::array<pid_t, 2> const ours{::getpid(), ::getppid()};
        assert(::write(pids.writeSide.get(), ours.data(), sizeof(ours)) == sizeof(ours));
        detached.report(true);
        ::_exit(0);
    });
    pids.writeSide.close();
    std::array<pid_t, 2> theirs{};
    assert(::read(pids.readSide.get(), theirs.data(), sizeof(theirs)) == sizeof(theirs));
    assert(exitStatus(reload) == 0);
    auto const [daemon, parent] = theirs;
    assert(parent == ::getpid());
    assert(exitStatus(daemon) == 0);
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-vararg): prctl(2) is variadic.
    assert(::prctl(PR_SET_CHILD_SUBREAPER, 0, 0, 0, 0) == 0);
}

// What systemd's notify socket gets, and BARRIER=1 answered as systemd
// answers it.
class Manager
{
public:
    Manager()
        : sock(::socket(AF_UNIX, SOCK_DGRAM | SOCK_CLOEXEC, 0))
        , name("@nix-grpc-takeover-test/" + std::to_string(::getpid()))
    {
        sockaddr_un addr{};
        addr.sun_family = AF_UNIX;
        name.copy(&addr.sun_path[1], name.size() - 1, 1);
        auto const len = static_cast<socklen_t>(offsetof(sockaddr_un, sun_path) + name.size());
        // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast): sockaddr API.
        assert(::bind(sock.get(), reinterpret_cast<const sockaddr *>(&addr), len) == 0);
        // NOLINTNEXTLINE(concurrency-mt-unsafe): before this test's threads.
        assert(::setenv("NOTIFY_SOCKET", name.c_str(), 1) == 0);
        reader = std::thread([this] -> void { read(); });
    }

    Manager(const Manager &) = delete;
    Manager(Manager &&) = delete;
    auto operator=(const Manager &) -> Manager & = delete;
    auto operator=(Manager &&) -> Manager & = delete;

    ~Manager()
    {
        stop = true;
        reader.join();
        // NOLINTNEXTLINE(concurrency-mt-unsafe): its threads are gone.
        ::unsetenv("NOTIFY_SOCKET");
    }

    auto got(const std::string & message) -> bool
    {
        const std::scoped_lock lock(mutex);
        return std::ranges::find(messages, message) != messages.end();
    }

private:
    void read()
    {
        while (!stop) {
            pollfd pfd{.fd = sock.get(), .events = POLLIN, .revents = 0};
            if (::poll(&pfd, 1, static_cast<int>(kStep.count())) != 1) {
                continue;
            }
            std::array<char, kMessage> buf{};
            // NOLINTNEXTLINE(misc-include-cleaner): iovec comes from <sys/socket.h>.
            iovec iov{.iov_base = buf.data(), .iov_len = buf.size()};
            alignas(cmsghdr) std::array<char, CMSG_SPACE(sizeof(int))> control{};
            msghdr msg{};
            msg.msg_iov = &iov;
            msg.msg_iovlen = 1;
            msg.msg_control = control.data();
            msg.msg_controllen = control.size();
            auto const len = ::recvmsg(sock.get(), &msg, MSG_CMSG_CLOEXEC);
            if (len <= 0) {
                continue;
            }
            for (cmsghdr * cmsg = CMSG_FIRSTHDR(&msg); cmsg != nullptr; cmsg = CMSG_NXTHDR(&msg, cmsg)) {
                int passed = -1;
                std::memcpy(&passed, CMSG_DATA(cmsg), sizeof(passed));
                ::close(passed);
            }
            const std::scoped_lock lock(mutex);
            messages.emplace_back(buf.data(), static_cast<size_t>(len));
        }
    }

    nix::AutoCloseFD sock;
    std::string name;
    std::atomic<bool> stop{false};
    std::mutex mutex;
    std::vector<std::string> messages;
    std::thread reader;
};

// The successor becomes the main process only if it still lives once
// systemd has read MAINPID=: systemd refuses a process that is gone.
void theOldProcessKeepsTheUnitIfTheSuccessorDiesBeforeSystemdTakesIt()
{
    Manager manager;
    auto const self = ::getpid();
    nix::Pipe listening;
    listening.create();
    {
        Offer offer;
        auto const child = spawn([&] -> bool {
            Predecessor pred(self);
            pred.ready();
            return true;
        });
        assert(next(offer) == Event::requested);
        offer.send({listening.readSide.get()}, "");
        assert(next(offer) == Event::ready);
        assert(offer.makeMain(kStep * kSteps));
        assert(manager.got("MAINPID=" + std::to_string(child)));
        offer.accept();
        assert(exitStatus(child) == 0);
    }
    {
        Offer offer;
        auto const child = spawn([&] -> bool {
            Predecessor pred(self);
            // Says it serves, and dies before the answer.
            std::thread([&pred] -> void {
                try {
                    pred.ready();
                } catch (nix::Error &) { // NOLINT(bugprone-empty-catch): it is gone by then
                }
            }).detach();
            std::this_thread::sleep_for(3 * kStep);
            ::_exit(0);
        });
        assert(next(offer) == Event::requested);
        offer.send({listening.readSide.get()}, "");
        assert(next(offer) == Event::ready);
        assert(exitStatus(child) == 0);
        assert(!offer.makeMain(kStep * kSteps));
        offer.reject();
    }
}

} // namespace

auto main() -> int
{
    theSuccessorGetsTheSocketsAndSeesThePredecessorGo();
    aSuccessorThatDiesBeforeItServesGivesTheNodeBack();
    aRejectedSuccessorDoesNotTakeOver();
    noTakeoverFromAProcessThatOffersNone();
    noTakeoverFromAnImpostor();
    theReloadExitsWithTheChildsVerdict();
    aDaemonWhoseReloadWasKilledGivesUp();
    theWaitForTheSocketsEndsWithTheReload();
    theDaemonBecomesTheManagersChild();
    theOldProcessKeepsTheUnitIfTheSuccessorDiesBeforeSystemdTakesIt();
    return 0;
}
