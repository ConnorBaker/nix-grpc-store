// A worker can have more than one reason to take no builds. It tells the
// scheduler it takes builds again only when the last one is gone, and its
// metric shows which ones hold.

#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <fstream>
#include <future>
#include <optional>
#include <ios>
#include <print>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <fcntl.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <unistd.h>

#include "builder.hh"
#include "logfmt.hh"
#include "metrics.hh"
#include "nix_remote.pb.h"
#include "scheduler.hh"

namespace {

using nix::remote::Done;
using nixgrpc::Builder;
using nixgrpc::Clock;

// A scheduler ends a session whose first message is not Hello.
auto greetsFirst() -> bool
{
    nixgrpc::Metrics metrics{""};
    Builder builder({.advertise = "w:0", .systems = {"x86_64-linux"}, .features = {}, .maxJobs = 1}, metrics);
    std::vector<bool> hellos;
    builder.setSend([&](const nix::remote::WorkerMsgs & msgs) -> bool {
        for (const auto & msg : msgs.msgs()) {
            hellos.push_back(msg.has_hello());
        }
        return true;
    });
    builder.setUnavailable(Builder::Unavailable::localDaemonDown, true);
    builder.greet();
    return hellos == std::vector<bool>{true, false};
}

// The scheduler hears of the builder's farm: in the Hello, and on a change,
// in a Load with max_jobs, which carries all of it.
auto tellsFarm() -> bool
{
    using Farm = std::vector<nixgrpc::sched::FarmMachine>;
    Farm const one{{.systems = {{"aarch64-linux", {}}}, .jobs = 1}};
    Farm const two{
        {.systems = {{"aarch64-linux", {}}}, .jobs = 2},
        {.systems = {{"x86_64-linux", {"kvm"}}, {"i686-linux", {}}}, .jobs = 3}};
    nixgrpc::Metrics metrics{""};
    Builder builder(
        {.advertise = "w:0", .systems = {"x86_64-linux", "aarch64-linux"}, .features = {}, .maxJobs = 4, .farm = one},
        metrics);
    std::vector<Farm> told;
    auto const farmOf = [](const auto & machines) -> Farm {
        Farm farm;
        for (const auto & machine : machines) {
            nixgrpc::sched::FarmMachine mach{.systems = {}, .jobs = machine.jobs()};
            for (const auto & [system, offered] : machine.systems()) {
                mach.systems.emplace(system, std::vector<std::string>(offered.features().begin(), offered.features().end()));
            }
            farm.push_back(std::move(mach));
        }
        return farm;
    };
    builder.setSend([&](const nix::remote::WorkerMsgs & msgs) -> bool {
        for (const auto & msg : msgs.msgs()) {
            if (msg.has_hello()) {
                told.push_back(farmOf(msg.hello().farm()));
            } else if (msg.has_load() && msg.load().has_max_jobs()) {
                told.push_back(farmOf(msg.load().farm()));
            }
        }
        return true;
    });
    builder.greet();
    builder.setFarm(one);
    builder.setFarm(two);
    builder.setFarm({});
    return told == std::vector<Farm>{one, two, {}};
}

auto expectOf(uint64_t assignId, const std::string & drv = "a.drv") -> nix::remote::Expect
{
    nix::remote::Expect exp;
    exp.set_drv_path(drv);
    exp.set_assign_id(assignId);
    return exp;
}

// The scheduler tells the worker and the client at the same time, on two
// streams. The client's call may be the first to arrive.
auto earlyCallWaits() -> bool
{
    nixgrpc::Metrics metrics{""};
    Builder builder({.advertise = "w:0", .systems = {"x86_64-linux"}, .features = {}, .maxJobs = 1}, metrics);
    auto call = std::async(std::launch::async, [&] -> bool { return builder.claim("a.drv", 2); });
    // Until the call waits; one that ended first did not.
    while (metrics.eventCounter("expect_wait").Value() < 1) {
        if (call.wait_for(std::chrono::milliseconds(1)) == std::future_status::ready) {
            return false;
        }
    }
    // The call waits Builder::expectLag (5 s) on the real clock, a product
    // constant: a stall that long between these two lines fails this test on
    // correct code. Kept, as the product's bound.
    builder.onExpect(expectOf(2));
    return call.get() && metrics.eventCounter("expect_late").Value() == 1;
}

// The worker read a later assignment already, so this one is not on its way:
// the call is refused without waiting for it.
auto staleCallIsRefusedAtOnce() -> bool
{
    nixgrpc::Metrics metrics{""};
    Builder builder({.advertise = "w:0", .systems = {"x86_64-linux"}, .features = {}, .maxJobs = 1}, metrics);
    builder.onExpect(expectOf(2));
    return !builder.claim("b.drv", 1) && metrics.eventCounter("expect_wait").Value() == 0;
}

constexpr auto past = std::chrono::seconds(61); // Builder's expectTimeout is 60 s
constexpr auto probeAfter = std::chrono::seconds(10); // Builder::probeAfter

// A builder on a clock the test moves, and the Dones it sends.
struct Quiet
{
    nixgrpc::Metrics metrics{""};
    Clock::time_point now = Clock::now();
    Builder builder{
        {.advertise = "w:0",
         .systems = {"x86_64-linux"},
         .features = {},
         .maxJobs = 2,
         .now = [this] -> Clock::time_point { return now; }},
        metrics};
    std::vector<Done> dones;

    Quiet()
    {
        builder.setSend([this](const nix::remote::WorkerMsgs & msgs) -> bool {
            for (const auto & msg : msgs.msgs()) {
                if (msg.has_done()) {
                    dones.push_back(msg.done());
                }
            }
            return true;
        });
        builder.greet();
    }
};

// A client that reached the builder for a drv and went away without building
// it (sent elsewhere, or to a successor that took the node over) left a
// claimed Expect nobody else ends. Once no call for it is in flight and
// expectTimeout has passed, it is given back; one with a call in flight
// stays.
auto quietClaimIsGivenBack() -> bool
{
    Quiet quiet;
    auto & builder = quiet.builder;
    builder.onExpect(expectOf(1, "a.drv"));
    builder.onExpect(expectOf(2, "b.drv"));
    builder.claim("a.drv");
    builder.callEnded("a.drv"); // say NOT_FOUND: the client was to upload and retry
    builder.claim("b.drv");     // still in its call
    constexpr auto halfway = std::chrono::seconds(30);
    quiet.now += halfway;
    builder.tick();
    if (!quiet.dones.empty() || builder.holding() != 2) {
        return false;
    }
    quiet.now += past;
    builder.tick();
    if (quiet.dones.size() != 1 || quiet.dones.at(0).drv_path() != "a.drv" || quiet.dones.at(0).assign_id() != 1
        || quiet.dones.at(0).outcome() != Done::GIVEN_BACK || quiet.dones.at(0).reason() != Done::UNREACHABLE
        || builder.holding() != 1) {
        return false;
    }
    builder.callEnded("b.drv");
    quiet.now += past;
    builder.tick();
    return quiet.dones.size() == 2 && builder.holding() == 0;
}

// A placement nobody claims, whose outputs this node's store has (a
// predecessor built them for a client that never calls here), is finished
// as BUILT with those outputs, not given back to be placed again. One a
// client claimed is its call's to finish, whatever the store says.
auto unclaimedBuiltHereIsFinished() -> bool
{
    Quiet quiet;
    auto & builder = quiet.builder;
    std::vector<std::string> asked;
    constexpr uint64_t narSize = 7;
    builder.checkBuiltWith([&](const std::string & drv) -> std::optional<std::vector<std::pair<std::string, uint64_t>>> {
        asked.push_back(drv);
        return std::vector<std::pair<std::string, uint64_t>>{{"/nix/store/" + drv + "-out", narSize}};
    });
    builder.onExpect(expectOf(1, "a.drv"));
    builder.onExpect(expectOf(2, "b.drv"));
    builder.claim("b.drv");
    builder.finishBuiltHere();
    // A client claims its placement within moments: not asked yet.
    if (!quiet.dones.empty() || !asked.empty()) {
        return false;
    }
    quiet.now += probeAfter;
    builder.finishBuiltHere();
    return asked == std::vector<std::string>{"a.drv"} && quiet.dones.size() == 1
           && quiet.dones.at(0).drv_path() == "a.drv" && quiet.dones.at(0).assign_id() == 1
           && quiet.dones.at(0).outcome() == Done::BUILT && quiet.dones.at(0).outputs_size() == 1
           && quiet.dones.at(0).outputs(0).nar_size() == narSize && builder.holding() == 1;
}

// The store is asked outside the builder's lock. A client that claims the
// drv meanwhile builds it: the probe's answer does not end its claim.
auto aClaimDuringTheProbeKeepsIt() -> bool
{
    Quiet quiet;
    auto & builder = quiet.builder;
    builder.checkBuiltWith([&](const std::string & drv) -> std::optional<std::vector<std::pair<std::string, uint64_t>>> {
        builder.claim(drv);
        return std::vector<std::pair<std::string, uint64_t>>{{"/nix/store/" + drv + "-out", 1}};
    });
    builder.onExpect(expectOf(1, "a.drv"));
    quiet.now += probeAfter;
    builder.finishBuiltHere();
    return quiet.dones.empty() && builder.holding() == 1;
}

// The drv is placed here again while the store is asked: the answer is for
// the placement asked about, and the newer one is finished in its own name.
auto aPlacementDuringTheProbeIsFinishedAsItself() -> bool
{
    Quiet quiet;
    auto & builder = quiet.builder;
    bool placedAgain = false;
    builder.checkBuiltWith([&](const std::string & drv) -> std::optional<std::vector<std::pair<std::string, uint64_t>>> {
        if (!placedAgain) {
            placedAgain = true;
            builder.onExpect(expectOf(2, drv));
        }
        return std::vector<std::pair<std::string, uint64_t>>{{"/nix/store/" + drv + "-out", 1}};
    });
    builder.onExpect(expectOf(1, "a.drv"));
    quiet.now += probeAfter;
    builder.finishBuiltHere();
    if (!quiet.dones.empty() || builder.holding() != 1) {
        return false;
    }
    quiet.now += probeAfter;
    builder.finishBuiltHere();
    return quiet.dones.size() == 1 && quiet.dones.at(0).assign_id() == 2 && builder.holding() == 0;
}

// The serve loop pets systemd's watchdog and ticks the builder: a tick must
// not wait on the node's nix-daemon, which may not answer. Only the probe
// thread (probeBuiltHere) asks the store.
auto tickAsksTheStoreNothing() -> bool
{
    Quiet quiet;
    auto & builder = quiet.builder;
    bool asked = false;
    builder.checkBuiltWith([&](const std::string &) -> std::optional<std::vector<std::pair<std::string, uint64_t>>> {
        asked = true;
        return std::nullopt;
    });
    builder.onExpect(expectOf(1, "a.drv"));
    // Due to be asked, and short of expectTimeout: the tick gives nothing
    // back first, wherever in it a call would go.
    quiet.now += probeAfter;
    builder.tick();
    return !asked && builder.holding() == 1;
}

// On the probe's thread an exception would end the process. A store that
// does not answer, or a Done that does not go out, costs that drv its turn
// only: no Done, the placement still held and asked about again next round,
// and the others settled meanwhile.
auto aFailedAskOrSendKeepsThePlacement() -> bool
{
    Quiet quiet;
    auto & builder = quiet.builder;
    int asks = 0;
    builder.checkBuiltWith([&](const std::string & drv) -> std::optional<std::vector<std::pair<std::string, uint64_t>>> {
        if (asks++ == 0) {
            throw std::runtime_error("the nix-daemon hung up");
        }
        return std::vector<std::pair<std::string, uint64_t>>{{"/nix/store/" + drv + "-out", 1}};
    });
    auto const round = [&] -> bool {
        try {
            builder.finishBuiltHere();
            return true;
        } catch (...) {
            return false;
        }
    };
    builder.onExpect(expectOf(1, "a.drv"));
    builder.onExpect(expectOf(2, "b.drv"));
    quiet.now += probeAfter;
    if (!round() || quiet.dones.size() != 1 || builder.holding() != 1) {
        return false;
    }
    if (!round() || quiet.dones.size() != 2 || builder.holding() != 0) {
        return false;
    }
    bool sendFails = true;
    builder.setSend([&](const nix::remote::WorkerMsgs & msgs) -> bool {
        for (const auto & msg : msgs.msgs()) {
            if (msg.has_done()) {
                if (std::exchange(sendFails, false)) {
                    throw std::runtime_error("the dispatcher threw");
                }
                quiet.dones.push_back(msg.done());
            }
        }
        return true;
    });
    builder.onExpect(expectOf(3, "c.drv"));
    quiet.now += probeAfter;
    if (!round() || quiet.dones.size() != 2 || builder.holding() != 1) {
        return false;
    }
    return round() && quiet.dones.size() == 3 && quiet.dones.back().drv_path() == "c.drv"
           && quiet.dones.back().assign_id() == 3 && builder.holding() == 0;
}

// Draining keeps a claimed Expect while its client may still upload, and
// gives it back once that client has gone quiet, so the drain can end.
auto drainingGivesBackAQuietClaim() -> bool
{
    Quiet quiet;
    auto & builder = quiet.builder;
    builder.onExpect(expectOf(1));
    builder.claim("a.drv");
    builder.callEnded("a.drv");
    builder.setUnavailable(Builder::Unavailable::stopping, true);
    builder.tick();
    if (!quiet.dones.empty() || builder.holding() != 1) {
        return false;
    }
    quiet.now += past;
    builder.tick();
    return quiet.dones.size() == 1 && quiet.dones.at(0).reason() == Done::DRAINING && builder.holding() == 0;
}

// A client claimed the drv (NOT_FOUND) and uploads its inputs to this builder
// (x-nix-drv, a Call) for longer than expectTimeout. The claim stays while
// the upload runs, and the end of the upload is the client's latest call:
// the next tick keeps the claim, and only expectTimeout after that does a
// quiet claim go.
auto uploadKeepsTheClaim() -> bool
{
    Quiet quiet;
    auto & builder = quiet.builder;
    builder.onExpect(expectOf(1));
    builder.claim("a.drv");
    builder.callEnded("a.drv");
    {
        Builder::Call const upload(builder, "a.drv");
        quiet.now += past;
        builder.tick();
        if (!quiet.dones.empty() || builder.holding() != 1) {
            return false;
        }
    }
    quiet.now += std::chrono::milliseconds(1);
    builder.tick();
    if (!quiet.dones.empty() || builder.holding() != 1) {
        return false;
    }
    quiet.now += past;
    builder.tick();
    return quiet.dones.size() == 1 && builder.holding() == 0;
}

// A builder with a.drv expected, and the Dones it sends.
struct Revokable
{
    nixgrpc::Metrics metrics{""};
    Builder builder{{.advertise = "w:0", .systems = {"x86_64-linux"}, .features = {}, .maxJobs = 1}, metrics};
    std::vector<Done> dones;

    Revokable()
    {
        builder.setSend([this](const nix::remote::WorkerMsgs & msgs) -> bool {
            for (const auto & msg : msgs.msgs()) {
                if (msg.has_done()) {
                    dones.push_back(msg.done());
                }
            }
            return true;
        });
        builder.onExpect(expectOf(1));
    }

    // From a scheduler handing over (Revoke.once_uncalled).
    void revokeOnceUncalled()
    {
        nix::remote::Revoke rev;
        rev.set_drv_path("a.drv");
        rev.set_once_uncalled(true);
        builder.onRevoke(rev);
    }

    // From a scheduler that supersedes it, refuses its Want or cancels it.
    void revokeOutright()
    {
        nix::remote::Revoke rev;
        rev.set_drv_path("a.drv");
        builder.onRevoke(rev);
    }
};

// A build that does not run yet has no call that could wait for it: a revoke
// once uncalled revokes it at once, as any Revoke does.
auto aRevokeOnceUncalledEndsABuildNotRunning() -> bool
{
    Revokable rev;
    rev.revokeOnceUncalled();
    return rev.dones.size() == 1 && rev.dones.front().outcome() == Done::CANCELLED && !rev.builder.admit("a.drv");
}

// A running build is only marked: it runs on while its owner's caller is
// there, and is to end once that caller is gone.
auto aRevokeOnceUncalledMarksARunningBuild() -> bool
{
    Revokable rev;
    auto const owner = rev.builder.claim("a.drv", 1) ? rev.builder.admit("a.drv") : std::nullopt;
    if (!owner) {
        return false;
    }
    rev.revokeOnceUncalled();
    return rev.dones.empty() && owner->shared->revokeOnceUncalled && !owner->shared->revoked
           && !owner->shared->uncalled(false) && owner->shared->uncalled(true);
}

// A call that attaches waits for the build from the moment it is admitted,
// before it gets to wait: a build revoked once uncalled whose owner's caller
// is gone must not end in between. Once the attached call is over, it ends.
auto anAttachedCallKeepsARevokedBuildFromItsAdmission() -> bool
{
    Revokable rev;
    auto const owner = rev.builder.claim("a.drv", 1) ? rev.builder.admit("a.drv") : std::nullopt;
    if (!owner) {
        return false;
    }
    rev.revokeOnceUncalled();
    auto attacher = rev.builder.admit("a.drv");
    if (!attacher || !attacher->attach || owner->shared->uncalled(true)) {
        return false;
    }
    attacher.reset();
    return owner->shared->uncalled(true);
}

// How a running build ended cancelled was counted: `revoked` events, and
// whether its caller heard it was superseded. Whether the owner's cancel poll
// ended it uncalled, and whether it is marked so.
struct RevokedEnd
{
    double revoked = 0;
    bool superseded = false;
    bool polledUncalled = false;
    bool markedUncalled = false;
};

// A running build revoked once uncalled, outright, or both, ends cancelled.
// Both: its owner's caller is gone, so the owner's cancel poll ends it
// uncalled; an outright Revoke for it then comes before the owner reports it
// ended. Without a revoke once uncalled the poll, its owner's caller gone
// too, does not end it uncalled. Each step runs in this order, on this thread.
auto revokedEnd(bool onceUncalled, bool outright) -> std::optional<RevokedEnd>
{
    Revokable rev;
    auto const owner = rev.builder.claim("a.drv", 1) ? rev.builder.admit("a.drv") : std::nullopt;
    if (!owner) {
        return std::nullopt;
    }
    if (onceUncalled) {
        rev.revokeOnceUncalled();
    }
    bool const polledUncalled = owner->shared->endsUncalled(true);
    if (outright) {
        rev.revokeOutright();
        if (!owner->shared->revoked) {
            return std::nullopt;
        }
    }
    rev.builder.finished("a.drv", Done::CANCELLED, {}, {});
    bool const superseded = rev.builder.endedCancelled(*owner->shared, "a.drv");
    return RevokedEnd{
        .revoked = rev.metrics.eventCounter("revoked").Value(),
        .superseded = superseded,
        .polledUncalled = polledUncalled,
        .markedUncalled = owner->shared->endedUncalled};
}

// One revoke ended the build, counted once however it came; a build no
// revoke ended counts none.
auto revokedEndsCountOnce() -> bool
{
    bool counted = true;
    for (bool const onceUncalled : {false, true}) {
        for (bool const outright : {false, true}) {
            auto const end = revokedEnd(onceUncalled, outright);
            double const want = onceUncalled || outright ? 1 : 0;
            if (!end || end->revoked != want || end->superseded != outright || end->polledUncalled != onceUncalled
                || end->markedUncalled != onceUncalled) {
                std::println(
                    "FAIL revoked once uncalled {} outright {}: counted {} revoked (want {}), superseded {}, "
                    "ended uncalled {}, marked uncalled {}",
                    onceUncalled, outright, end ? end->revoked : -1, want, end && end->superseded,
                    end && end->polledUncalled, end && end->markedUncalled);
                counted = false;
            }
        }
    }
    return counted;
}

constexpr uint64_t silent = 1800; // the build's --max-silent-time
constexpr uint64_t otherSilent = 1200;
constexpr uint64_t check = nixgrpc::Expected::Shared::checkMode;

// A second caller attaches to a running build, with the given mode and
// limits. Does it take the result of the build, run in `mode` and ended with
// `outcome`?
auto attacherTakes(nix::remote::Done::Outcome outcome, const std::array<uint64_t, 3> & want, uint64_t mode = 0) -> bool
{
    nixgrpc::Metrics metrics{""};
    Builder builder({.advertise = "w:0", .systems = {"x86_64-linux"}, .features = {}, .maxJobs = 1}, metrics);
    builder.onExpect(expectOf(1));
    auto const owner = builder.claim("a.drv", 1) ? builder.admit("a.drv") : std::nullopt;
    if (!owner) {
        return false;
    }
    owner->shared->settings = {mode, 0, silent};
    auto const attacher = builder.admit("a.drv");
    builder.finished("a.drv", outcome, {}, "result");
    return attacher && attacher->attach && attacher->shared->serves(want);
}

auto aStalledStderrHoldsUpNoPublish() -> bool;

auto run() -> int
{
    if (!attacherTakes(nix::remote::Done::BUILT, {0, 0, otherSilent})) {
        std::println("FAIL a caller with other limits does not take the outputs a build made");
        return EXIT_FAILURE;
    }
    if (attacherTakes(nix::remote::Done::FAILED, {0, 0, otherSilent})) {
        std::println("FAIL a caller with other limits takes a failure it might not have had");
        return EXIT_FAILURE;
    }
    if (!attacherTakes(nix::remote::Done::FAILED, {0, 0, silent})) {
        std::println("FAIL a caller with the same limits does not take the build's failure");
        return EXIT_FAILURE;
    }
    if (attacherTakes(nix::remote::Done::BUILT, {check, 0, silent})) {
        std::println("FAIL a caller asking for a check takes a build that checked nothing");
        return EXIT_FAILURE;
    }
    if (attacherTakes(nix::remote::Done::BUILT, {check, 0, otherSilent}, check)) {
        std::println("FAIL a caller asking for a check with other limits takes another caller's check");
        return EXIT_FAILURE;
    }
    if (!aRevokeOnceUncalledEndsABuildNotRunning()) {
        std::println("FAIL a revoke once uncalled of a build not running does not revoke it at once");
        return EXIT_FAILURE;
    }
    if (!aRevokeOnceUncalledMarksARunningBuild()) {
        std::println("FAIL a revoke once uncalled of a running build does not only mark it");
        return EXIT_FAILURE;
    }
    if (!anAttachedCallKeepsARevokedBuildFromItsAdmission()) {
        std::println("FAIL an attached call does not keep a build revoked once uncalled from its admission on");
        return EXIT_FAILURE;
    }
    if (!revokedEndsCountOnce()) {
        std::println("FAIL a build a revoke ended does not count one revoke");
        return EXIT_FAILURE;
    }
    if (!earlyCallWaits()) {
        std::println("FAIL a call that arrives before its assignment is not served once the assignment arrives");
        return EXIT_FAILURE;
    }
    if (!staleCallIsRefusedAtOnce()) {
        std::println("FAIL a call for an assignment that is over waits for it");
        return EXIT_FAILURE;
    }
    if (!greetsFirst()) {
        std::println("FAIL a worker that takes no builds from the start says so before its Hello");
        return EXIT_FAILURE;
    }
    if (!quietClaimIsGivenBack()) {
        std::println("FAIL a claimed Expect whose client went quiet is not given back after expectTimeout");
        return EXIT_FAILURE;
    }
    if (!unclaimedBuiltHereIsFinished()) {
        std::println("FAIL a placement nobody claimed whose outputs the node has is not finished as built");
        return EXIT_FAILURE;
    }
    if (!aClaimDuringTheProbeKeepsIt()) {
        std::println("FAIL a claim made while the store is asked is ended as built");
        return EXIT_FAILURE;
    }
    if (!aPlacementDuringTheProbeIsFinishedAsItself()) {
        std::println("FAIL a placement made while the store is asked is not finished in its own name");
        return EXIT_FAILURE;
    }
    if (!aFailedAskOrSendKeepsThePlacement()) {
        std::println("FAIL a store that does not answer or a Done that does not go out loses the placement or the round");
        return EXIT_FAILURE;
    }
    if (!tickAsksTheStoreNothing()) {
        std::println("FAIL a tick of the serve loop asks the node's store");
        return EXIT_FAILURE;
    }
    if (!drainingGivesBackAQuietClaim()) {
        std::println("FAIL a draining builder does not give back a claim whose client went quiet");
        return EXIT_FAILURE;
    }
    if (!tellsFarm()) {
        std::println("FAIL a builder does not tell the scheduler of its farm");
        return EXIT_FAILURE;
    }
    if (!uploadKeepsTheClaim()) {
        std::println("FAIL an upload for a claimed drv does not keep its claim");
        return EXIT_FAILURE;
    }
    if (!aStalledStderrHoldsUpNoPublish()) {
        std::println("FAIL a stalled stderr holds up a publish");
        return EXIT_FAILURE;
    }
    nixgrpc::Metrics metrics{""};
    Builder builder({.advertise = "w:0", .systems = {"x86_64-linux"}, .features = {}, .maxJobs = 1}, metrics);
    std::vector<bool> told;
    builder.setSend([&](const nix::remote::WorkerMsgs & msgs) -> bool {
        for (const auto & msg : msgs.msgs()) {
            if (msg.has_load()) {
                told.push_back(msg.load().draining());
            }
        }
        return true;
    });
    builder.greet();

    builder.setUnavailable(Builder::Unavailable::localDaemonDown, true);
    builder.setUnavailable(Builder::Unavailable::stopping, true);
    builder.setUnavailable(Builder::Unavailable::localDaemonDown, false);
    if (told != std::vector<bool>{true}) {
        std::println("FAIL a stopping worker takes builds again once its local nix-daemon answers");
        return EXIT_FAILURE;
    }
    if (metrics.unavailableNow("stopping") != 1 || metrics.unavailableNow("local_daemon") != 0) {
        std::println("FAIL the metric does not show stopping as the only reason");
        return EXIT_FAILURE;
    }
    builder.setUnavailable(Builder::Unavailable::stopping, false);
    if (told != std::vector<bool>{true, false}) {
        std::println("FAIL a worker with no reason left does not take builds again");
        return EXIT_FAILURE;
    }
    return EXIT_SUCCESS;
}

// The thread `tid` sleeps in a write to our stderr.
auto writesToStderr(pid_t tid) -> bool // NOLINT(misc-include-cleaner): pid_t comes from <sys/types.h>.
{
    std::ifstream stat("/proc/self/task/" + std::to_string(tid) + "/stat");
    std::string line;
    std::getline(stat, line);
    auto const end = line.rfind(')');
    if (end == std::string::npos || end + 2 >= line.size() || line.at(end + 2) != 'S') {
        return false;
    }
    std::ifstream syscall("/proc/self/task/" + std::to_string(tid) + "/syscall");
    long number = -1;
    long descriptor = -1;
    return syscall >> number >> std::hex >> descriptor && number == SYS_write && descriptor == STDERR_FILENO;
}

// Builder::published tells our stderr that publishing began failing, or works
// again, under the lock every publish takes. With our stderr a full pipe that
// nobody reads, a publish that turns it to failing, and then one that turns
// it back, must each return: one found asleep writing to stderr cannot, as
// nothing reads the pipe until the test is done. A step count, not a clock,
// bounds each wait; the first that does not return ends the steps. Then the
// pipe is read, which lets everything go.
auto aStalledStderrHoldsUpNoPublish() -> bool
{
    constexpr int steps = 1'000'000;
    nixgrpc::Metrics metrics{""};
    Builder builder({.advertise = "w:0", .systems = {"x86_64-linux"}, .features = {}, .maxJobs = 1}, metrics);
    nixgrpc::flushLog(); // what was logged before goes where stderr was
    int const ourStderr = fcntl(STDERR_FILENO, F_DUPFD_CLOEXEC, 0); // NOLINT(cppcoreguidelines-pro-type-vararg)
    std::array<int, 2> err{};
    if (ourStderr < 0 || pipe2(err.data(), O_CLOEXEC) != 0) {
        return false;
    }
    auto const [readSide, writeSide] = err;
    std::string const room(static_cast<size_t>(fcntl(writeSide, F_GETPIPE_SZ)), '\0'); // NOLINT(cppcoreguidelines-pro-type-vararg)
    // NOLINTNEXTLINE(misc-include-cleaner): ssize_t comes from <sys/types.h>.
    if (write(writeSide, room.data(), room.size()) != static_cast<ssize_t>(room.size()) || dup2(writeSide, STDERR_FILENO) < 0) {
        return false;
    }
    std::vector<std::thread> publishers;
    std::array<std::atomic<bool>, 2> returned{};
    bool allReturned = true;
    for (size_t i = 0; i < returned.size() && allReturned; i++) {
        std::atomic<pid_t> tid = 0;
        publishers.emplace_back([&, i] -> void {
            tid = gettid();
            builder.published(i == 1);
            returned.at(i) = true;
        });
        bool held = false;
        for (int step = 0; step < steps && !returned.at(i) && !held; step++) {
            held = tid != 0 && writesToStderr(tid);
            std::this_thread::yield();
        }
        allReturned = returned.at(i);
    }
    std::string said;
    std::thread reader([&] -> void {
        constexpr size_t chunkBytes = 4096;
        std::array<char, chunkBytes> chunk{};
        for (ssize_t got = 0; (got = read(readSide, chunk.data(), chunk.size())) > 0;) {
            said.append(chunk.data(), static_cast<size_t>(got));
        }
    });
    for (auto & publisher : publishers) {
        publisher.join();
    }
    nixgrpc::flushLog();
    dup2(ourStderr, STDERR_FILENO);
    close(writeSide);
    reader.join();
    close(readSide);
    close(ourStderr);
    if (!allReturned) {
        std::println(
            "a publish waited behind a stalled stderr write (returned {} {})", returned.at(0).load(), returned.at(1).load());
        return false;
    }
    if (!said.contains("event=publish_failing") || !said.contains("event=publishing")) {
        std::println("a change of publishing is missing from our stderr");
        return false;
    }
    return true;
}

} // namespace

auto main() -> int
{
    try {
        return run();
    } catch (...) {
        return EXIT_FAILURE;
    }
}
