// A worker can have more than one reason to take no builds. It tells the
// scheduler it takes builds again only when the last one is gone, and its
// metric shows which ones hold.

#include <array>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <future>
#include <optional>
#include <print>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "builder.hh"
#include "metrics.hh"
#include "nix_remote.pb.h"

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
    quiet.now += std::chrono::seconds(30);
    builder.tick();
    if (!quiet.dones.empty() || builder.holding() != 2) {
        return false;
    }
    quiet.now += past;
    builder.tick();
    if (quiet.dones.size() != 1 || quiet.dones[0].drv_path() != "a.drv" || quiet.dones[0].assign_id() != 1
        || quiet.dones[0].outcome() != Done::GIVEN_BACK || quiet.dones[0].reason() != Done::UNREACHABLE
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
    return quiet.dones.size() == 1 && quiet.dones[0].reason() == Done::DRAINING && builder.holding() == 0;
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
    if (!uploadKeepsTheClaim()) {
        std::println("FAIL an upload for a claimed drv does not keep its claim");
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

} // namespace

auto main() -> int
{
    try {
        return run();
    } catch (...) {
        return EXIT_FAILURE;
    }
}
