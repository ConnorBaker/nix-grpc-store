// The client's executor: Wants, placements, bounces and failures.

#include <cassert>
#include <chrono>
#include <exception>
#include <iostream>
#include <variant>
#include <vector>

#include "executor.hh"

namespace {

using nixgrpc::exec::Clock;
using nixgrpc::exec::Executor;
using nixgrpc::exec::JobId;
using nixgrpc::exec::Outcome;
using nixgrpc::exec::Placement;
namespace st = nixgrpc::exec::state;

auto outcome(const Executor & exe, JobId jid) -> Outcome
{
    return std::get<st::Done>(exe.at(jid)).outcome;
}

auto place(const char * addr) -> Placement
{
    return {.addr = addr, .assignId = 1, .system = {}};
}

// Wants plus placed jobs never exceed the build threads; the urgent go first.
void wantsFollowThreads()
{
    Executor exe({.threads = 2, .rescheduleRetries = Executor::defaultRescheduleRetries, .keepGoing = true});
    auto const low = exe.add(1, {});
    auto const high = exe.add(9, {});
    auto const mid = exe.add(5, {});
    assert(exe.takeWants().empty()); // no stream yet
    exe.connected();
    auto wants = exe.takeWants();
    assert((wants == std::vector<JobId>{high, mid}));
    exe.onAssigned(high, place("a"));
    assert(exe.takeWants().empty());
    exe.onCached(mid);
    assert((exe.takeWants() == std::vector<JobId>{low}));
}

// A dependant is Wanted once its inputs are done, and fails with them.
void dependantsWait()
{
    Executor exe({.threads = 4, .rescheduleRetries = Executor::defaultRescheduleRetries, .keepGoing = true});
    auto const input = exe.add(0, {});
    auto const top = exe.add(0, {input});
    exe.connected();
    assert((exe.takeWants() == std::vector<JobId>{input}));
    exe.onAssigned(input, place("a"));
    auto const job = exe.next();
    assert(job && job->first == input);
    exe.finished(input, false);
    assert(outcome(exe, top) == Outcome::dependencyFailed);
    assert(exe.remainingJobs() == 0);
}

// A bounce follows a redirect, else asks again, and gives up eventually.
void bounces()
{
    Executor exe({.threads = 1, .rescheduleRetries = 1, .keepGoing = true});
    auto const job = exe.add(0, {});
    exe.connected();
    exe.takeWants();
    exe.onAssigned(job, place("a"));
    exe.next();
    exe.onAssigned(job, place("b"));
    exe.bounced(job, Clock::now());
    auto again = exe.next();
    assert(again && again->second.addr == "b");
    exe.bounced(job, Clock::now());
    assert(std::holds_alternative<st::Wantable>(exe.at(job)));
    exe.takeWants();
    exe.onAssigned(job, place("c"));
    exe.next();
    exe.bounced(job, Clock::now());
    assert(outcome(exe, job) == Outcome::gaveUp);
}

// The first bounce asks again at once. Later ones wait, each longer than
// the one before, so a worker that is briefly out of reach can come back.
void laterBouncesWait()
{
    Executor exe({.threads = 1, .rescheduleRetries = Executor::defaultRescheduleRetries, .keepGoing = true});
    auto const job = exe.add(0, {});
    auto const grace = std::chrono::minutes(2);
    auto now = Clock::now();
    auto const bounce = [&] -> void {
        exe.takeWants();
        exe.onAssigned(job, place("a"));
        exe.next();
        exe.bounced(job, now);
    };
    exe.connected();
    bounce();
    assert(std::holds_alternative<st::Wantable>(exe.at(job)));
    bounce();
    assert(!std::holds_alternative<st::Wantable>(exe.at(job)));
    auto const first = exe.reap(now, grace) - now;
    assert(first < grace);
    now += first;
    exe.reap(now, grace);
    assert(std::holds_alternative<st::Wantable>(exe.at(job)));
    bounce();
    assert(exe.reap(now, grace) - now > first);
}

// Nothing wakes a waiting job once the scheduler is gone, so a bounce
// after that must end the job instead of making it wait.
void bounceAfterAbandonIsLost()
{
    Executor exe({.threads = 1, .rescheduleRetries = Executor::defaultRescheduleRetries, .keepGoing = true});
    auto const job = exe.add(0, {});
    exe.connected();
    exe.takeWants();
    exe.onAssigned(job, place("a"));
    exe.next();
    exe.bounced(job, Clock::now());
    exe.takeWants();
    exe.onAssigned(job, place("a"));
    exe.next();
    exe.abandon();
    exe.bounced(job, Clock::now());
    assert(outcome(exe, job) == Outcome::lost);
    assert(exe.remainingJobs() == 0);
}

// A worker that would not serve a job is named in the next Want, so the
// scheduler sends the job elsewhere. A stream that broke later is not.
void refusedPlacementIsNamedOnce()
{
    Executor exe({.threads = 1, .rescheduleRetries = Executor::defaultRescheduleRetries, .keepGoing = true});
    auto const job = exe.add(0, {});
    exe.connected();
    exe.takeWants();
    exe.onAssigned(job, place("a"));
    exe.next();
    exe.bounced(job, Clock::now(), {.refused = true, .left = false});
    assert(exe.refused(job) == place("a").assignId);
    exe.takeWants();
    exe.onAssigned(job, place("b"));
    exe.next();
    exe.bounced(job, Clock::now());
    assert(exe.refused(job) == 0);
}

// Without keep-going the first failure cancels what has not started, and
// the scheduler hears it.
void stopsWithoutKeepGoing()
{
    Executor exe({.threads = 3, .rescheduleRetries = Executor::defaultRescheduleRetries, .keepGoing = false});
    auto const bad = exe.add(0, {});
    auto const other = exe.add(0, {});
    auto const later = exe.add(0, {other});
    exe.connected();
    exe.takeWants();
    exe.onAssigned(bad, place("a"));
    exe.next();
    exe.finished(bad, false);
    assert(outcome(exe, other) == Outcome::cancelled && outcome(exe, later) == Outcome::cancelled);
    assert((exe.takeCancels() == std::vector<JobId>{bad, other}));
}

// Unplaceable fails a job only after the grace, Placeable resets it.
void unplaceableGrace()
{
    Executor exe({.threads = 1, .rescheduleRetries = Executor::defaultRescheduleRetries, .keepGoing = true});
    auto const job = exe.add(0, {});
    exe.connected();
    exe.takeWants();
    auto const now = Clock::now();
    assert(exe.onUnplaceable(job, "no kvm", now));
    assert(!exe.onUnplaceable(job, "no kvm", now));
    exe.onPlaceable(job);
    exe.reap(now + std::chrono::hours(1), std::chrono::minutes(2));
    assert(std::holds_alternative<st::Wanted>(exe.at(job)));
    exe.onUnplaceable(job, "no kvm", now);
    exe.reap(now + std::chrono::hours(1), std::chrono::minutes(2));
    assert(outcome(exe, job) == Outcome::unplaceable);
}

// A reconnect re-Wants placed jobs, and abandoning fails late releases too.
void reconnectAndAbandon()
{
    Executor exe({.threads = 2, .rescheduleRetries = Executor::defaultRescheduleRetries, .keepGoing = true});
    auto const placed = exe.add(0, {});
    auto const dependant = exe.add(0, {placed});
    exe.connected();
    exe.takeWants();
    exe.onAssigned(placed, place("a"));
    exe.next();
    exe.disconnected();
    exe.connected();
    assert((exe.takeWants() == std::vector<JobId>{placed}));
    exe.disconnected();
    exe.abandon();
    exe.finished(placed, true);
    assert(outcome(exe, dependant) == Outcome::lost);
}

// A missing input becomes a job that the build waits for.
void missingInput()
{
    Executor exe({.threads = 2, .rescheduleRetries = Executor::defaultRescheduleRetries, .keepGoing = true});
    auto const job = exe.add(0, {});
    exe.connected();
    exe.takeWants();
    exe.onAssigned(job, place("a"));
    exe.next();
    auto const input = exe.add(0, {});
    exe.blockOn(job, input);
    assert(std::holds_alternative<st::Blocked>(exe.at(job)));
    assert((exe.takeCancels() == std::vector<JobId>{job}));
    assert((exe.takeWants() == std::vector<JobId>{input}));
    exe.onCached(input);
    assert((exe.takeWants() == std::vector<JobId>{job}));
}

// Whatever ends a job we follow, the scheduler hears a Cancel for it, once.
void everyEndCancels()
{
    Executor exe({.threads = 4, .rescheduleRetries = 0, .keepGoing = true});
    auto const built = exe.add(0, {});
    auto const failed = exe.add(0, {});
    auto const cached = exe.add(0, {});
    auto const gaveUp = exe.add(0, {});
    exe.connected();
    exe.takeWants();
    for (auto const jid : {built, failed, gaveUp}) {
        exe.onAssigned(jid, place("a"));
        exe.next();
    }
    exe.finished(built, true);
    exe.finished(failed, false);
    exe.onCached(cached);
    exe.bounced(gaveUp, Clock::now());
    assert((exe.takeCancels() == std::vector<JobId>{built, failed, cached, gaveUp}));
    exe.finished(built, true);
    assert(exe.takeCancels().empty());
}

// A dropped stream ends the scheduler's follows: nothing left to cancel.
void disconnectForgetsFollows()
{
    Executor exe({.threads = 1, .rescheduleRetries = Executor::defaultRescheduleRetries, .keepGoing = true});
    auto const job = exe.add(0, {});
    exe.connected();
    exe.takeWants();
    exe.onAssigned(job, place("a"));
    exe.next();
    exe.disconnected();
    exe.finished(job, true);
    assert(exe.takeCancels().empty());
}

// A worker that went away, or did not take the job, costs no retry the
// first time, and the job is wanted again at once.
void freeBounces()
{
    Executor exe({.threads = 1, .rescheduleRetries = 0, .keepGoing = true});
    auto const job = exe.add(0, {});
    exe.connected();
    exe.takeWants();
    exe.onAssigned(job, {.addr = "a", .assignId = 2, .system = {}});
    exe.next();
    exe.bounced(job, Clock::now(), {.refused = true, .left = true});
    assert(exe.refused(job) == 2);
    assert((exe.takeWants() == std::vector<JobId>{job}));
    exe.onAssigned(job, {.addr = "b", .assignId = 3, .system = {}});
    exe.next();
    exe.bounced(job, Clock::now(), {.refused = false, .left = true});
    assert(exe.refused(job) == 0);
    assert((exe.takeWants() == std::vector<JobId>{job}));
    // Back on b: its second bounce counts, and there are no retries.
    exe.onAssigned(job, {.addr = "b", .assignId = 4, .system = {}});
    exe.next();
    exe.bounced(job, Clock::now(), {.refused = false, .left = true});
    assert(outcome(exe, job) == Outcome::gaveUp);
}

// A stream that broke under a build the worker keeps: the scheduler sends
// the job back to the same placement, and that bounce is refunded. Sent
// elsewhere, it counts.
void refundsABrokenStream()
{
    Executor exe({.threads = 1, .rescheduleRetries = 1, .keepGoing = true});
    auto const job = exe.add(0, {});
    exe.connected();
    exe.takeWants();
    exe.onAssigned(job, place("a"));
    exe.next();
    auto const grace = std::chrono::minutes(2);
    auto now = Clock::now();
    // Each bounce waits as any other (laterBouncesWait), then asks again.
    auto const bounce = [&] -> void {
        exe.bounced(job, now);
        now = exe.reap(now, grace);
        exe.reap(now, grace);
        assert((exe.takeWants() == std::vector<JobId>{job}));
    };
    for (int round = 0; round < 3; round++) {
        bounce();
        exe.onAssigned(job, place("a"));
        exe.next();
    }
    bounce();
    exe.onAssigned(job, place("b"));
    exe.next();
    exe.bounced(job, now);
    assert(outcome(exe, job) == Outcome::gaveUp);
}

// A refused job that the next Want does not ask for again is cancelled:
// the scheduler places a drv given back again at once, and a placement we
// do not take holds a slot until it expires, again and again, while the
// jobs we did ask for wait for one.
void bounceNotWantedAgainCancels()
{
    Executor exe({.threads = 1, .rescheduleRetries = Executor::defaultRescheduleRetries, .keepGoing = true});
    auto const bounced = exe.add(1, {});
    exe.connected();
    assert((exe.takeBatch().wants == std::vector<JobId>{bounced}));
    auto const urgent = exe.add(9, {});
    exe.onAssigned(bounced, place("a"));
    exe.next();
    exe.bounced(bounced, Clock::now(), {.refused = true, .left = true});
    assert(std::holds_alternative<st::Wantable>(exe.at(bounced)));
    auto const batch = exe.takeBatch();
    assert((batch.wants == std::vector<JobId>{urgent}));
    assert((batch.cancels == std::vector<JobId>{bounced}));
}

// A refused job that rests before it asks again is cancelled meanwhile.
void restingBounceCancels()
{
    Executor exe({.threads = 1, .rescheduleRetries = Executor::defaultRescheduleRetries, .keepGoing = true});
    auto const job = exe.add(0, {});
    exe.connected();
    for (int round = 0; round < 2; round++) {
        exe.takeBatch();
        exe.onAssigned(job, place("a"));
        exe.next();
        exe.bounced(job, Clock::now(), {.refused = true, .left = false});
    }
    assert(std::holds_alternative<st::Resting>(exe.at(job)));
    auto const batch = exe.takeBatch();
    assert(batch.wants.empty());
    assert((batch.cancels == std::vector<JobId>{job}));
}

// A job whose build goes on (its stream to the worker broke) is followed
// on, wanted again or not: a Cancel would have the scheduler revoke that
// build (farm::cutoffs, a build whose stream breaks).
void brokenStreamKeepsTheBuild()
{
    Executor exe({.threads = 1, .rescheduleRetries = Executor::defaultRescheduleRetries, .keepGoing = true});
    auto const job = exe.add(1, {});
    exe.connected();
    exe.takeBatch();
    auto const urgent = exe.add(9, {});
    exe.onAssigned(job, place("a"));
    exe.next();
    exe.bounced(job, Clock::now(), {.refused = false, .left = true, .held = true});
    auto const batch = exe.takeBatch();
    assert((batch.wants == std::vector<JobId>{urgent}));
    assert(batch.cancels.empty());
}

// A job not wanted again after a give-back the worker did not refuse is
// cancelled too: `bounce` as the worker answers (UNAVAILABLE, draining or
// out of space: left, not refused), or as Run::recover reports the upload
// of an input the worker lacked (neither).
void givenBackNotWantedAgainCancels(nixgrpc::exec::Bounce bounce)
{
    Executor exe({.threads = 1, .rescheduleRetries = Executor::defaultRescheduleRetries, .keepGoing = true});
    auto const job = exe.add(1, {});
    exe.connected();
    exe.takeBatch();
    auto const urgent = exe.add(9, {});
    exe.onAssigned(job, place("a"));
    exe.next();
    exe.bounced(job, Clock::now(), bounce);
    auto const batch = exe.takeBatch();
    assert((batch.wants == std::vector<JobId>{urgent}));
    assert((batch.cancels == std::vector<JobId>{job}));
}

// A job given back for an input this run already has done (a Cached answer
// whose output went later: Run::recover) is wanted again, and cancelled
// unless that is at once, as after any other give-back.
void doneInputNotWantedAgainCancels()
{
    Executor exe({.threads = 1, .rescheduleRetries = Executor::defaultRescheduleRetries, .keepGoing = true});
    auto const input = exe.add(0, {});
    exe.connected();
    assert((exe.takeBatch().wants == std::vector<JobId>{input}));
    exe.onCached(input);
    auto const job = exe.add(1, {});
    assert((exe.takeBatch().wants == std::vector<JobId>{job}));
    auto const urgent = exe.add(9, {});
    exe.onAssigned(job, place("a"));
    exe.next();
    exe.blockOn(job, input);
    assert(std::holds_alternative<st::Wantable>(exe.at(job)));
    auto const batch = exe.takeBatch();
    assert((batch.wants == std::vector<JobId>{urgent}));
    assert((batch.cancels == std::vector<JobId>{job}));
}

// A bounced job wanted again in the same batch is not cancelled: the
// scheduler keeps its entry, with the placement it may have made already.
void bounceWantedAgainKeepsTheEntry()
{
    Executor exe({.threads = 1, .rescheduleRetries = Executor::defaultRescheduleRetries, .keepGoing = true});
    auto const job = exe.add(0, {});
    exe.connected();
    exe.takeBatch();
    exe.onAssigned(job, place("a"));
    exe.next();
    exe.bounced(job, Clock::now(), {.refused = true, .left = true});
    auto const batch = exe.takeBatch();
    assert((batch.wants == std::vector<JobId>{job}));
    assert(batch.cancels.empty());
}

} // namespace

auto main() -> int
{
    try {
        wantsFollowThreads();
        dependantsWait();
        bounces();
        laterBouncesWait();
        bounceAfterAbandonIsLost();
        refusedPlacementIsNamedOnce();
        stopsWithoutKeepGoing();
        unplaceableGrace();
        reconnectAndAbandon();
        missingInput();
        everyEndCancels();
        disconnectForgetsFollows();
        freeBounces();
        refundsABrokenStream();
        bounceNotWantedAgainCancels();
        restingBounceCancels();
        brokenStreamKeepsTheBuild();
        givenBackNotWantedAgainCancels({.refused = false, .left = true, .held = false}); // draining
        givenBackNotWantedAgainCancels({});                                              // re-upload
        bounceWantedAgainKeepsTheEntry();
        doneInputNotWantedAgainCancels();
    } catch (const std::exception & err) {
        std::cerr << err.what() << "\n";
        return 1;
    }
    return 0;
}
