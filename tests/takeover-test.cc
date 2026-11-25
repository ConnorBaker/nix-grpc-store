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
#include <string_view>
#include <thread>
#include <vector>

#include <linux/prctl.h>
#include <sys/poll.h>
#include <sys/prctl.h>
#include <sys/socket.h>
#include <sys/types.h>
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
// For Offer::makeMain: until the test's systemd has read MAINPID=.
constexpr std::chrono::milliseconds noLimit{-1};
// Longer than any sd_notify message these tests send.
constexpr size_t kMessage = 256;

// Runs `body` in a child; its exit status is whether it held.
// NOLINTNEXTLINE(misc-include-cleaner): pid_t comes from <unistd.h>.
auto spawn(const std::function<bool()> & body) -> pid_t
{
    auto const pid = ::fork();
    assert(pid >= 0);
    if (pid == 0) {
        // Dies with the test: a failed assert there ends the run, not waits on us.
        // NOLINTNEXTLINE(cppcoreguidelines-pro-type-vararg): prctl(2) is variadic.
        (void) ::prctl(PR_SET_PDEATHSIG, SIGKILL, 0, 0, 0);
        bool held = false;
        try {
            held = body();
        } catch (...) { // NOLINT(bugprone-empty-catch): the exit status says it
        }
        ::_exit(held ? 0 : 1);
    }
    return pid;
}

// Closes every descriptor above stderr but `keep`, as a process that has
// only what it was handed: none of this process's offer and links, which it
// would otherwise keep up.
void keepOnly(std::vector<int> keep)
{
    std::ranges::sort(keep);
    unsigned from = 3;
    for (int const kept : keep) {
        auto const num = static_cast<unsigned>(kept);
        if (num > from) {
            ::close_range(from, num - 1, 0);
        }
        from = std::max(from, num + 1);
    }
    ::close_range(from, ~0U, 0);
}

auto exitStatus(pid_t pid) -> int
{
    int status = 0;
    assert(::waitpid(pid, &status, 0) == pid);
    return WIFEXITED(status) ? WEXITSTATUS(status) : -1;
}

// The next event of `offer`. `peer` is to cause it: once it has exited and
// the offer still has nothing, nothing will come.
auto next(Offer & offer, pid_t peer) -> Event
{
    for (;;) {
        // <signal.h> and <sys/wait.h> provide these, through glibc's private
        // headers, which include-cleaner does not map.
        siginfo_t info{}; // NOLINT(misc-include-cleaner)
        bool const exited = ::waitid(P_PID, static_cast<id_t>(peer), &info, WEXITED | WNOHANG | WNOWAIT) == 0 // NOLINT(misc-include-cleaner)
                            && info.si_pid == peer; // NOLINT(misc-include-cleaner)
        if (auto const event = offer.wait(kStep); event != Event::none) {
            return event;
        }
        assert(!exited);
    }
}

void theSuccessorGetsTheSocketsAndSeesThePredecessorGo()
{
    auto offer = std::make_unique<Offer>();
    auto const self = ::getpid();
    // Stands in for a listening socket: whatever reads it reads what we write.
    nix::Pipe listening;
    listening.create();
    nix::Pipe sawUs;
    sawUs.create();
    auto const child = spawn([&] -> bool {
        listening.writeSide.close();
        Predecessor pred(self);
        bool held = pred.pid() == self && pred.fds().size() == 1;
        char byte = 0;
        held = held && ::read(pred.fds().front(), &byte, 1) == 1 && byte == 'x';
        nixgrpc::takeover::applyNotifyEnv(pred.env());
        held = held && nix::getEnv("NOTIFY_SOCKET") == "/run/systemd/notify" && nix::getEnv("WATCHDOG_USEC") == "15000000"
               && !nix::getEnv("LD_PRELOAD");
        auto links = pred.ready();
        held = held && links.size() == 1 && links.front().pid() == self && links.front().alive();
        bool const told = ::write(sawUs.writeSide.get(), "s", 1) == 1;
        held = held && told;
        // Nothing to wait on from outside the link: look until it is gone.
        while (links.front().alive()) {
            std::this_thread::sleep_for(kStep);
        }
        return held;
    });
    sawUs.writeSide.close();
    assert(next(*offer, child) == Event::requested);
    assert(offer->peer() == child);
    assert(::write(listening.writeSide.get(), "x", 1) == 1);
    offer->send({listening.readSide.get()}, "NOTIFY_SOCKET=/run/systemd/notify\nWATCHDOG_USEC=15000000\nLD_PRELOAD=/evil.so\n");
    assert(next(*offer, child) == Event::ready);
    assert(offer->peerWatchesOlder());
    offer->accept({});
    // The successor still sees us.
    char byte = 0;
    assert(::read(sawUs.readSide.get(), &byte, 1) == 1);
    offer.reset(); // as when this process exits
    assert(exitStatus(child) == 0);
}

// Three generations: this process, `second` that took over from it, and
// `third` that took over from `second`. With its confirmation `second` passes
// on its link to this one, so `third` sees each of the two go by itself:
// `second` first, while this one lives on.
void theSuccessorWatchesTheOlderGenerationsItIsPassed()
{
    auto first = std::make_unique<Offer>();
    auto const self = ::getpid();
    nix::Pipe listening;
    listening.create();
    nix::Pipe secondOffers; // `second` listens for its successor
    secondOffers.create();
    nix::Pipe secondGoes; // closed: `second` exits
    secondGoes.create();
    auto const second = spawn([&] -> bool {
        secondOffers.readSide.close();
        secondGoes.writeSide.close();
        Predecessor pred(self);
        auto links = pred.ready();
        Offer offer;
        nix::Pipe ours;
        ours.create();
        if (::write(secondOffers.writeSide.get(), "o", 1) != 1) {
            return false;
        }
        Event event = Event::none;
        while ((event = offer.wait(kStep)) == Event::none) {
        }
        if (event != Event::requested) {
            return false;
        }
        offer.send({ours.readSide.get()}, "");
        while ((event = offer.wait(kStep)) == Event::none) {
        }
        if (event != Event::ready) {
            return false;
        }
        std::vector<int> older;
        older.reserve(links.size());
        for (auto const & link : links) {
            older.push_back(link.fd());
        }
        offer.accept(older);
        char byte = 0;
        (void) ::read(secondGoes.readSide.get(), &byte, 1);
        return true;
    });
    secondOffers.writeSide.close();
    secondGoes.readSide.close();
    assert(next(*first, second) == Event::requested);
    first->send({listening.readSide.get()}, "");
    assert(next(*first, second) == Event::ready);
    first->accept({});
    char byte = 0;
    assert(::read(secondOffers.readSide.get(), &byte, 1) == 1);

    nix::Pipe report; // `third`: 'l' it has both links, 's' `second` went first
    report.create();
    auto const third = spawn([&] -> bool {
        // Only what a successor of its own has: not this process's side of
        // its link to `second`, which would keep that link up for it.
        keepOnly({report.writeSide.get()});
        Predecessor pred(second);
        auto links = pred.ready();
        if (links.size() != 2 || links.front().pid() != second || links.back().pid() != self) {
            return false;
        }
        if (::write(report.writeSide.get(), "l", 1) != 1) {
            return false;
        }
        while (links.front().alive()) {
            std::this_thread::sleep_for(kStep);
        }
        if (!links.back().alive() || ::write(report.writeSide.get(), "s", 1) != 1) {
            return false;
        }
        while (links.back().alive()) {
            std::this_thread::sleep_for(kStep);
        }
        return true;
    });
    report.writeSide.close();
    assert(::read(report.readSide.get(), &byte, 1) == 1 && byte == 'l');
    secondGoes.writeSide.close();
    assert(exitStatus(second) == 0);
    assert(::read(report.readSide.get(), &byte, 1) == 1 && byte == 's');
    first.reset(); // as when this process exits
    assert(exitStatus(third) == 0);
}

// A successor that predates links, as a rollback starts one: it says
// "ready" with nothing attached, as d279c64c's Predecessor::ready() did, and
// reads the answer with recv(). It does not watch the older generations.
void aSuccessorThatPredatesLinksDoesNotWatchTheOlderGenerations()
{
    Offer offer;
    auto const self = ::getpid();
    nix::Pipe listening;
    listening.create();
    auto const child = spawn([&] -> bool {
        nix::AutoCloseFD const sock(::socket(AF_UNIX, SOCK_SEQPACKET | SOCK_CLOEXEC, 0));
        sockaddr_un addr{};
        addr.sun_family = AF_UNIX;
        auto const name = "nix-grpc-daemon/takeover/" + std::to_string(self);
        name.copy(&addr.sun_path[1], name.size());
        auto const len = static_cast<socklen_t>(offsetof(sockaddr_un, sun_path) + 1 + name.size());
        // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast): sockaddr API.
        if (::connect(sock.get(), reinterpret_cast<const sockaddr *>(&addr), len) != 0) {
            return false;
        }
        std::array<char, kMessage> buf{};
        // The sockets: recv() closes them.
        constexpr std::string_view ready = "ready";
        if (::recv(sock.get(), buf.data(), buf.size(), 0) <= 0
            || ::send(sock.get(), ready.data(), ready.size(), MSG_NOSIGNAL) != static_cast<ssize_t>(ready.size())) {
            return false;
        }
        auto const got = ::recv(sock.get(), buf.data(), buf.size(), 0);
        return got == 2 && std::string(buf.data(), 2) == "ok";
    });
    assert(next(offer, child) == Event::requested);
    offer.send({listening.readSide.get()}, "");
    assert(next(offer, child) == Event::ready);
    assert(!offer.peerWatchesOlder());
    offer.accept({listening.readSide.get()});
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
    assert(next(offer, child) == Event::requested);
    offer.send({listening.readSide.get()}, "");
    assert(next(offer, child) == Event::gone);
    offer.reject();
    assert(exitStatus(child) == 0);
    // And the next one may try.
    auto const again = spawn([&] -> bool {
        Predecessor pred(self);
        pred.ready();
        return true;
    });
    assert(next(offer, again) == Event::requested);
    offer.send({listening.readSide.get()}, "");
    assert(next(offer, again) == Event::ready);
    offer.accept({});
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
    assert(next(offer, child) == Event::requested);
    offer.send({listening.readSide.get()}, "");
    assert(next(offer, child) == Event::ready);
    offer.reject();
    assert(exitStatus(child) == 0);
}

void noTakeoverFromAProcessThatOffersNone()
{
    auto const sleeper = spawn([] -> bool {
        ::pause(); // until killed
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
        ::pause(); // until killed
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

// Nor does one still waiting for the sockets when systemd kills its reload:
// the wait ends with the reload ('g'), not otherwise ('t'), as when its own
// time runs out. Its daemon, which nobody waits for, may outlive the test's
// offer; the next offer of this process listens even so.
void theWaitForTheSocketsEndsWithTheReload()
{
    auto offer = std::make_unique<Offer>();
    auto const self = ::getpid();
    nix::Pipe said;
    said.create();
    nix::Pipe goOn; // closed: the daemon exits
    goOn.create();
    auto const reload = spawn([&] -> bool {
        // Its daemon must see the test let it go, whatever else it holds.
        goOn.writeSide.close();
        // As ExecReload runs it: a process of its own, not a fork of the
        // main process holding that one's offer.
        keepOnly({said.writeSide.get(), goOn.readSide.get()});
        Detached const detached;
        char verdict = 'p';
        try {
            Predecessor const pred(self, &detached);
        } catch (nix::Error & err) {
            verdict = std::string(err.what()).contains("gave up on the reload") ? 'g' : 't';
        }
        assert(::write(said.writeSide.get(), &verdict, 1) == 1);
        char byte = 0;
        (void) ::read(goOn.readSide.get(), &byte, 1);
        ::_exit(0);
    });
    goOn.readSide.close();
    assert(next(*offer, reload) == Event::requested);
    ::kill(reload, SIGKILL);
    exitStatus(reload);
    assert(verdictOf(said) == 'g');
    offer->reject();
    offer.reset();
    offer = std::make_unique<Offer>(); // while that daemon lives
    goOn.writeSide.close();
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
        assert(next(offer, child) == Event::requested);
        offer.send({listening.readSide.get()}, "");
        assert(next(offer, child) == Event::ready);
        assert(offer.makeMain(noLimit));
        assert(manager.got("MAINPID=" + std::to_string(child)));
        offer.accept({});
        assert(exitStatus(child) == 0);
    }
    {
        Offer offer;
        nix::Pipe goOn;
        goOn.create();
        auto const child = spawn([&] -> bool {
            Predecessor pred(self);
            // Says it serves, and dies before the answer.
            std::thread([&pred] -> void {
                try {
                    pred.ready();
                } catch (nix::Error &) { // NOLINT(bugprone-empty-catch): it is gone by then
                }
            }).detach();
            char byte = 0;
            (void) ::read(goOn.readSide.get(), &byte, 1);
            ::_exit(0);
        });
        assert(next(offer, child) == Event::requested);
        offer.send({listening.readSide.get()}, "");
        assert(next(offer, child) == Event::ready);
        assert(::write(goOn.writeSide.get(), "x", 1) == 1);
        assert(exitStatus(child) == 0);
        assert(!offer.makeMain(noLimit));
        offer.reject();
    }
}

} // namespace

auto main() -> int
{
    theSuccessorGetsTheSocketsAndSeesThePredecessorGo();
    theSuccessorWatchesTheOlderGenerationsItIsPassed();
    aSuccessorThatPredatesLinksDoesNotWatchTheOlderGenerations();
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
