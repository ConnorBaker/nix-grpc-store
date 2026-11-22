// The Dispatcher driven through the calls its transports make, without gRPC.

#include <algorithm>
#include <atomic>
#include <cassert>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <initializer_list>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <utility>
#include <variant>
#include <vector>

#include <absl/base/thread_annotations.h>

#include "logfmt.hh"

#include "builder.hh"
#include "dispatcher.hh"
#include "executor.hh"
#include "metrics.hh"
#include "nix_remote.pb.h"
#include "scheduler.hh"

namespace {

using nix::remote::ClientMsgs;
using nix::remote::SchedCmd;
using nix::remote::SchedMsg;
using nix::remote::WorkerMsgs;
using nixgrpc::Dispatcher;
using nixgrpc::Metrics;

struct Farm
{
    nixgrpc::Metrics metrics{""};
    double nowMs = 0;
    Dispatcher disp{
        Dispatcher::Config{
            .defaultSystem = "x", .present = {}, .lookupThreads = 0, .nowMs = [this] -> double { return nowMs; }},
        metrics};
    std::vector<std::string> sentTo;
    bool unplaceable = false;
    Dispatcher::ClientPtr client = disp.connectClient(
        [this](const nix::remote::SchedMsg & msg) -> bool {
            if (msg.has_assigned()) {
                sentTo.push_back(msg.assigned().worker_addr());
            }
            unplaceable = unplaceable || msg.has_unplaceable();
            return true;
        },
        {});

    void hello(Dispatcher::Worker & session, const std::string & addr, uint32_t slots = 2)
    {
        nix::remote::WorkerMsgs msgs;
        auto * hel = msgs.add_msgs()->mutable_hello();
        hel->set_addr(addr);
        hel->add_systems("x");
        hel->set_max_jobs(slots);
        disp.workerMsgs(session, msgs);
    }

    // `older`: as a client that names the refused assignment in field 8.
    void want(const std::string & drv, uint64_t refused = 0, bool older = false)
    {
        nix::remote::ClientMsgs msgs;
        auto * wnt = msgs.add_msgs()->mutable_want();
        wnt->set_drv_path(drv);
        wnt->set_system("x");
        if (older) {
            wnt->set_bounced_assign_id(refused);
        } else {
            wnt->set_refused(refused);
        }
        disp.clientMsgs(client, msgs);
    }
};

// What a worker session was told to expect.
struct Session
{
    bool alive = true;
    bool ended = false;
    std::vector<std::string> expected;
    std::vector<std::string> revoked;
    uint64_t lastAssignId = 0;
    Dispatcher::Worker conn{
        .send = [this](const nix::remote::SchedCmd & cmd) -> bool {
            if (alive && cmd.has_revoke()) {
                revoked.push_back(cmd.revoke().drv_path());
            }
            if (alive && cmd.has_expect()) {
                expected.push_back(cmd.expect().drv_path());
                lastAssignId = cmd.expect().assign_id();
            }
            return alive;
        },
        .abort = [this] -> void { ended = true; },
        .id = {}};
};

// A session an Expect cannot be sent on is dropped and ended. Its transport
// reports the end later, after the worker has reconnected: that must not
// take the new session down.
void endOfADroppedSessionKeepsTheNewOne()
{
    Farm farm;
    Session dead;
    farm.hello(dead.conn, "w:1");
    dead.alive = false;
    farm.want("a.drv");

    Session fresh;
    farm.hello(fresh.conn, "w:1");
    assert(dead.ended);
    assert(fresh.expected == std::vector<std::string>{"a.drv"});

    farm.disp.workerGone(dead.conn);
    farm.want("b.drv");
    assert((fresh.expected == std::vector<std::string>{"a.drv", "b.drv"}));
    farm.disp.workerGone(fresh.conn);
}

// The derivation a dropped session was meant to get goes to another worker
// right away, not with the next event.
void dropPlacesItsWorkElsewhere()
{
    Farm farm;
    Session dead;
    Session other;
    constexpr uint32_t more = 4; // so the dead one is picked first
    farm.hello(dead.conn, "w:1", more);
    farm.hello(other.conn, "w:2");
    dead.alive = false;
    farm.want("a.drv");
    assert(other.expected == std::vector<std::string>{"a.drv"});
    farm.disp.workerGone(dead.conn);
    farm.disp.workerGone(other.conn);
}

// A worker in the scheduler's own process answers from inside the delivery
// of a command, here by giving an Expect back as a draining builder does.
void loopbackWorkerMayAnswerACommand()
{
    Farm farm;
    std::vector<std::string> expected;
    std::shared_ptr<Dispatcher::Worker> worker;
    worker = farm.disp.loopbackWorker([&](const nix::remote::SchedCmd & cmd) -> void {
        if (!cmd.has_expect()) {
            return;
        }
        expected.push_back(cmd.expect().drv_path());
        if (expected.size() == 1) {
            nix::remote::WorkerMsgs msgs;
            auto * done = msgs.add_msgs()->mutable_done();
            done->set_drv_path(cmd.expect().drv_path());
            done->set_assign_id(cmd.expect().assign_id());
            done->set_outcome(nix::remote::Done::GIVEN_BACK);
            farm.disp.workerMsgs(*worker, msgs);
        }
    });
    farm.hello(*worker, "w:1");
    farm.want("a.drv");
    // Given back once, then placed again on the only worker there is.
    assert((expected == std::vector<std::string>{"a.drv", "a.drv"}));
    farm.disp.workerGone(*worker);
}

// A worker that stops taking builds gives back what it was told to expect.
// That says nothing about the derivation, which may return there.
void aWorkerThatRestedGetsItsDerivationBack()
{
    Farm farm;
    Session rested;
    Session busy;
    farm.hello(rested.conn, "w:1", 1);
    farm.want("a.drv");
    farm.hello(busy.conn, "w:2", 1);
    farm.want("b.drv");
    assert(busy.expected == std::vector<std::string>{"b.drv"});

    nix::remote::WorkerMsgs msgs;
    msgs.add_msgs()->mutable_load()->set_draining(true);
    auto * done = msgs.add_msgs()->mutable_done();
    done->set_drv_path("a.drv");
    done->set_assign_id(rested.lastAssignId);
    done->set_outcome(nix::remote::Done::GIVEN_BACK);
    done->set_reason(nix::remote::Done::DRAINING);
    msgs.add_msgs()->mutable_load()->set_draining(false);
    farm.disp.workerMsgs(rested.conn, msgs);

    assert((rested.expected == std::vector<std::string>{"a.drv", "a.drv"}));
    farm.disp.workerGone(rested.conn);
    farm.disp.workerGone(busy.conn);
}

// A worker that gives back build after build for a failure of its own gets
// none for a while. Its client waits and is not told the build has no worker.
void aFailingWorkerIsRested()
{
    constexpr double rest = 30e3;
    Farm farm;
    Session sick;
    farm.hello(sick.conn, "w:1", 1);
    farm.want("a.drv");
    for (std::size_t round = 1; round <= 3; round++) {
        assert(sick.expected.size() == round);
        nix::remote::WorkerMsgs msgs;
        auto * done = msgs.add_msgs()->mutable_done();
        done->set_drv_path("a.drv");
        done->set_assign_id(sick.lastAssignId);
        done->set_outcome(nix::remote::Done::GIVEN_BACK);
        done->set_reason(nix::remote::Done::TRANSIENT_FAILURE);
        farm.disp.workerMsgs(sick.conn, msgs);
    }
    assert(sick.expected.size() == 3 && !farm.unplaceable);
    farm.nowMs = rest;
    farm.disp.tick();
    assert(sick.expected.size() == 4);
    farm.disp.workerGone(sick.conn);
}

// One worker gives a build back while the other is busy. True if the build
// goes back to the giver.
auto backOnGiverWhileOtherBusy(nix::remote::Done::Reason reason) -> bool
{
    Farm farm;
    Session giver;
    Session busy;
    farm.hello(giver.conn, "w:1", 1);
    farm.want("a.drv");
    farm.hello(busy.conn, "w:2", 1);
    farm.want("b.drv");
    assert(busy.expected == std::vector<std::string>{"b.drv"});

    nix::remote::WorkerMsgs msgs;
    auto * done = msgs.add_msgs()->mutable_done();
    done->set_drv_path("a.drv");
    done->set_assign_id(giver.lastAssignId);
    done->set_outcome(nix::remote::Done::GIVEN_BACK);
    done->set_reason(reason);
    farm.disp.workerMsgs(giver.conn, msgs);

    bool const back = giver.expected == std::vector<std::string>{"a.drv", "a.drv"};
    assert(back || giver.expected == std::vector<std::string>{"a.drv"});
    farm.disp.workerGone(giver.conn);
    farm.disp.workerGone(busy.conn);
    return back;
}

void aBuildGivenBackForAMissingInputDoesNotWaitForABusyWorker()
{
    assert(backOnGiverWhileOtherBusy(nix::remote::Done::INPUT_UNAVAILABLE));
}

void aBuildGivenBackForAFailureOfItsWorkerWaitsForTheOther()
{
    assert(!backOnGiverWhileOtherBusy(nix::remote::Done::TRANSIENT_FAILURE));
}

// A client that cannot reach the worker it was sent to is sent to another,
// also one that says so the older way.
void aClientThatCannotReachItsWorkerIsSentElsewhere(bool older)
{
    Farm farm;
    Session unreachable;
    Session other;
    constexpr uint32_t more = 4; // so it is picked first
    farm.hello(unreachable.conn, "w:1", more);
    farm.hello(other.conn, "w:2");
    farm.want("a.drv");
    assert(farm.sentTo == std::vector<std::string>{"w:1"});

    farm.want("a.drv", unreachable.lastAssignId, older);
    assert((farm.sentTo == std::vector<std::string>{"w:1", "w:2"}));
    assert(unreachable.revoked == std::vector<std::string>{"a.drv"});
    assert(other.expected == std::vector<std::string>{"a.drv"});
    farm.disp.workerGone(unreachable.conn);
    farm.disp.workerGone(other.conn);
}

// The balancer asks for the builders of a system nobody builds for.
// Queries and uploads work on any builder, so it gets all of them, once each.
void aSystemWithoutBuildersGetsEveryBuilder()
{
    const Dispatcher::Membership members{
        {"arm", {{.addr = "a:1", .draining = false}, {.addr = "both:1", .draining = true}}},
        {"x86", {{.addr = "both:1", .draining = true}, {.addr = "b:1", .draining = false}}},
    };
    auto const own = Dispatcher::membersFor(members, "arm");
    assert(own.size() == 2 && own.at(0).addr == "a:1");
    auto const all = Dispatcher::membersFor(members, "riscv");
    assert(all.size() == 3);
    assert(all.at(0).addr == "a:1" && all.at(1).addr == "both:1" && all.at(2).addr == "b:1");
    assert(all.at(1).draining);
    assert(Dispatcher::membersFor({}, "riscv").empty());
}

// Load.max_jobs changes a builder's slots without a reconnect, as after
// SIGHUP (Coordinator::reloadNixConfig); a Load without it, as older builders
// send, leaves them. The builder starts with none: older builders say Hello
// with max_jobs 0 while they drain.
void loadRestoresSlotsAfterDrainingHello()
{
    Farm farm;
    Session worker;
    farm.hello(worker.conn, "w:1", 0);
    farm.want("a.drv");

    nix::remote::WorkerMsgs older;
    older.add_msgs()->mutable_load()->set_draining(false);
    farm.disp.workerMsgs(worker.conn, older);
    assert(farm.sentTo.empty());
    assert(farm.disp.membership()->at("x").front().draining);

    nix::remote::WorkerMsgs load;
    auto * recovered = load.add_msgs()->mutable_load();
    recovered->set_draining(false);
    recovered->set_max_jobs(2);
    farm.disp.workerMsgs(worker.conn, load);
    assert(farm.sentTo == std::vector<std::string>{"w:1"});
    assert(!farm.disp.membership()->at("x").front().draining);
    farm.disp.workerGone(worker.conn);
}

// A worker session's send, which keeps what it is sent.
auto recordInto(std::vector<SchedCmd> & cmds) -> Dispatcher::WorkerSend
{
    return [&cmds](const SchedCmd & cmd) -> bool {
        cmds.push_back(cmd);
        return true;
    };
}

// A client's send, which keeps what it is sent.
auto recordMsgsInto(std::vector<SchedMsg> & msgs) -> Dispatcher::ClientSend
{
    return [&msgs](const SchedMsg & msg) -> bool {
        msgs.push_back(msg);
        return true;
    };
}

auto assigned(const std::vector<SchedMsg> & msgs) -> size_t
{
    return static_cast<size_t>(std::ranges::count_if(msgs, [](const SchedMsg & msg) -> bool { return msg.has_assigned(); }));
}

auto expectsIn(const std::vector<SchedCmd> & cmds) -> size_t
{
    return static_cast<size_t>(std::ranges::count_if(cmds, [](const SchedCmd & cmd) -> bool { return cmd.has_expect(); }));
}

// A builder's Hello (Builder::hello), for x86_64-linux.
auto helloFrom(const char * addr, unsigned maxJobs) -> WorkerMsgs
{
    WorkerMsgs msgs;
    auto * hel = msgs.add_msgs()->mutable_hello();
    hel->set_addr(addr);
    hel->add_systems("x86_64-linux");
    hel->set_max_jobs(maxJobs);
    return msgs;
}

auto wantOf(const char * drvPath, std::initializer_list<const char *> inputs = {}) -> ClientMsgs
{
    ClientMsgs msgs;
    auto * want = msgs.add_msgs()->mutable_want();
    want->set_drv_path(drvPath);
    for (const auto * input : inputs) {
        want->add_inputs(input);
    }
    return msgs;
}

auto doneOf(const char * drvPath, uint64_t assignId, nix::remote::Done::Outcome outcome) -> WorkerMsgs
{
    WorkerMsgs msgs;
    auto * done = msgs.add_msgs()->mutable_done();
    done->set_drv_path(drvPath);
    done->set_assign_id(assignId);
    done->set_outcome(outcome);
    return msgs;
}

auto loadOf(bool draining, unsigned maxJobs) -> WorkerMsgs
{
    WorkerMsgs msgs;
    auto * load = msgs.add_msgs()->mutable_load();
    load->set_draining(draining);
    load->set_max_jobs(maxJobs);
    return msgs;
}

// The x86_64-linux builders the scheduler counts as connected (and the
// balancer routes to, EDS).
auto connectedBuilders(const Dispatcher & disp) -> size_t
{
    auto const members = disp.membership();
    auto const iter = members->find("x86_64-linux");
    return iter == members->end() ? 0 : iter->second.size();
}

// What a session that is no longer the worker's says ends that stream
// (workerMsgs throws, its caller closes it), and changes nothing.
auto refused(Dispatcher & disp, Dispatcher::Worker & session, const WorkerMsgs & msgs) -> bool
{
    try {
        disp.workerMsgs(session, msgs);
    } catch (...) {
        return true;
    }
    return false;
}

auto debugConfig() -> Dispatcher::Config
{
    return {.defaultSystem = "x86_64-linux", .present = {}, .lookupThreads = 0, .logLevel = nixgrpc::LogLevel::debug};
}

// A builder that reconnects says Hello on its new session while the scheduler
// still holds the old one, which ends only once the scheduler notices: a
// partition the builder's keepalive caught first, a rebooted host, a balancer
// pod gone. The old session's end must leave the builder connected, its build
// and new ones on the new session, whose end takes it away.
void testOldSessionEndKeepsTheReconnectedWorker()
{
    constexpr auto drvA = "/nix/store/00000000000000000000000000000000-a.drv";
    constexpr auto drvB = "/nix/store/11111111111111111111111111111111-b.drv";
    Metrics metrics{""};
    double now = 0;
    auto cfg = debugConfig();
    cfg.nowMs = [&now] -> double { return now; };
    Dispatcher disp(cfg, metrics);
    std::vector<SchedCmd> toOld;
    std::vector<SchedCmd> toNew;
    std::vector<SchedCmd> toOther;
    std::vector<SchedMsg> toClient;
    Dispatcher::Worker oldSession{.send = recordInto(toOld), .abort = {}, .id = {}};
    disp.workerMsgs(oldSession, helloFrom("w1:50051", 3));
    auto client = disp.connectClient(recordMsgsInto(toClient), [] -> void {});
    disp.clientMsgs(client, wantOf(drvA));
    assert(expectsIn(toOld) == 1);
    // One free slot elsewhere, fewer than w1's: drvA must not land there.
    Dispatcher::Worker other{.send = recordInto(toOther), .abort = {}, .id = {}};
    disp.workerMsgs(other, helloFrom("w2:50051", 1));

    // w1 again, on a new session, still expecting drvA.
    auto reconnect = helloFrom("w1:50051", 3);
    auto * running = reconnect.mutable_msgs(0)->mutable_hello()->add_running();
    running->set_drv_path(drvA);
    running->set_assign_id(toOld.back().expect().assign_id());
    Dispatcher::Worker newSession{.send = recordInto(toNew), .abort = {}, .id = {}};
    disp.workerMsgs(newSession, reconnect);
    assert(connectedBuilders(disp) == 2);

    // Now the scheduler notices that the old session ended.
    disp.workerGone(oldSession);
    assert(connectedBuilders(disp) == 2);
    assert(expectsIn(toOther) == 0);
    // The client hears again where drvA runs (the Hello's reconciliation),
    // always the first placement.
    auto const placed = toOld.back().expect().assign_id();
    assert(std::ranges::all_of(toClient, [&](const SchedMsg & msg) -> bool {
        return !msg.has_assigned() || (msg.assigned().worker_addr() == "w1:50051" && msg.assigned().assign_id() == placed);
    }));
    // New work reaches w1, over the session it has now.
    auto const before = assigned(toClient);
    disp.clientMsgs(client, wantOf(drvB));
    assert(expectsIn(toNew) == 1);
    assert(assigned(toClient) == before + 1);

    // The current session's end still takes w1 away. Its builds are held for
    // its return (Core::holdMs); then one of them goes to w2's slot.
    disp.workerGone(newSession);
    assert(connectedBuilders(disp) == 1);
    assert(expectsIn(toOther) == 0);
    now = nixgrpc::sched::Core::holdMs;
    disp.tick();
    assert(expectsIn(toOther) == 1);

    disp.clientGone(client);
    disp.workerGone(other);
}

// What a builder sent on its old session can arrive after its Hello on a new
// one: a partition heals, and the old connection's unacknowledged bytes are
// sent again. The Hello is newer: it lists what the builder still runs and
// carries its load. So a late Done must not end the build's new placement,
// nor a late Load undo the Hello's state, while the new session's own Done
// and Load still count.
void testOldSessionMessagesAfterTheNewHello()
{
    constexpr auto drvA = "/nix/store/00000000000000000000000000000000-a.drv";
    Metrics metrics{""};
    Dispatcher disp(debugConfig(), metrics);
    std::vector<SchedCmd> toOld;
    std::vector<SchedCmd> toNew;
    std::vector<SchedMsg> toFirst;
    std::vector<SchedMsg> toSecond;
    std::vector<SchedMsg> toThird;
    Dispatcher::Worker oldSession{.send = recordInto(toOld), .abort = {}, .id = {}};
    disp.workerMsgs(oldSession, helloFrom("w1:50051", 2));
    auto first = disp.connectClient(recordMsgsInto(toFirst), [] -> void {});
    disp.clientMsgs(first, wantOf(drvA));
    assert(expectsIn(toOld) == 1);

    // w1 built drvA and said so on the old session, then that it drains;
    // neither got through. Its Hello on the new session omits drvA, which the
    // scheduler places on w1 again.
    Dispatcher::Worker newSession{.send = recordInto(toNew), .abort = {}, .id = {}};
    disp.workerMsgs(newSession, helloFrom("w1:50051", 2));
    assert(expectsIn(toNew) == 1);

    // Now the old session's messages arrive.
    auto late = doneOf(drvA, toOld.back().expect().assign_id(), nix::remote::Done::BUILT);
    late.MergeFrom(loadOf(true, 2));
    assert(refused(disp, oldSession, late));

    // w1 still takes builds, and another client wanting drvA is told where it
    // runs now, which takes no new placement.
    auto second = disp.connectClient(recordMsgsInto(toSecond), [] -> void {});
    disp.clientMsgs(second, wantOf(drvA));
    assert(!disp.membership()->at("x86_64-linux").front().draining);
    assert(expectsIn(toNew) == 1);
    assert(assigned(toSecond) == 1);

    // The new session's own Load and Done count.
    disp.workerMsgs(newSession, loadOf(true, 2));
    assert(disp.membership()->at("x86_64-linux").front().draining);
    disp.workerMsgs(newSession, loadOf(false, 2));
    disp.workerMsgs(newSession, doneOf(drvA, toNew.back().expect().assign_id(), nix::remote::Done::BUILT));
    auto third = disp.connectClient(recordMsgsInto(toThird), [] -> void {});
    disp.clientMsgs(third, wantOf(drvA));
    assert(expectsIn(toNew) == 2);

    disp.clientGone(first);
    disp.clientGone(second);
    disp.clientGone(third);
    disp.workerGone(oldSession);
    disp.workerGone(newSession);
}


// Once the worker's current session has ended as well, what an earlier
// session still says changes nothing, and its end takes away nothing more.
// The worker's next Hello brings it back as it says, and a drv still wanted
// goes to it.
void testReplacedSessionAfterTheWorkerLeft()
{
    constexpr auto drvA = "/nix/store/00000000000000000000000000000000-a.drv";
    Metrics metrics{""};
    Dispatcher disp(debugConfig(), metrics);
    std::vector<SchedCmd> toOld;
    std::vector<SchedCmd> toNew;
    std::vector<SchedCmd> toNext;
    std::vector<SchedMsg> toClient;
    Dispatcher::Worker oldSession{.send = recordInto(toOld), .abort = {}, .id = {}};
    disp.workerMsgs(oldSession, helloFrom("w1:50051", 2));
    Dispatcher::Worker newSession{.send = recordInto(toNew), .abort = {}, .id = {}};
    disp.workerMsgs(newSession, helloFrom("w1:50051", 2));
    auto client = disp.connectClient(recordMsgsInto(toClient), [] -> void {});
    disp.clientMsgs(client, wantOf(drvA));
    assert(expectsIn(toNew) == 1);
    disp.workerGone(newSession);
    assert(connectedBuilders(disp) == 0);

    auto late = loadOf(true, 0);
    late.MergeFrom(doneOf(drvA, toNew.back().expect().assign_id(), nix::remote::Done::BUILT));
    static_cast<void>(refused(disp, oldSession, late));
    disp.workerGone(oldSession);
    assert(connectedBuilders(disp) == 0);

    Dispatcher::Worker nextSession{.send = recordInto(toNext), .abort = {}, .id = {}};
    disp.workerMsgs(nextSession, helloFrom("w1:50051", 2));
    assert(connectedBuilders(disp) == 1);
    assert(!disp.membership()->at("x86_64-linux").front().draining);
    assert(expectsIn(toNext) == 1);
    assert(assigned(toClient) == 2);

    disp.clientGone(client);
    disp.workerGone(nextSession);
}

// A builder's first session can say Hello after its second one: behind a
// balancer, the first connection's bytes can be held up until the builder
// gave up on it and reconnected. The late Hello takes the worker and ends
// the live session (its abort), so the builder reconnects; that Hello, the
// newest, takes the worker back with the build it reports, and the dead
// sessions' ends take nothing away.
void testLateFirstHelloEndsInAReconnect()
{
    constexpr auto drvA = "/nix/store/00000000000000000000000000000000-a.drv";
    constexpr auto drvB = "/nix/store/11111111111111111111111111111111-b.drv";
    Metrics metrics{""};
    Dispatcher disp(debugConfig(), metrics);
    std::vector<SchedCmd> toDead;
    std::vector<SchedCmd> toLive;
    std::vector<SchedCmd> toNext;
    std::vector<SchedMsg> toClient;
    int deadAborted = 0;
    int liveAborted = 0;
    Dispatcher::Worker deadSession{.send = recordInto(toDead), .abort = [&] -> void { deadAborted++; }, .id = {}};
    Dispatcher::Worker liveSession{.send = recordInto(toLive), .abort = [&] -> void { liveAborted++; }, .id = {}};

    disp.workerMsgs(liveSession, helloFrom("w1:50051", 2));
    auto client = disp.connectClient(recordMsgsInto(toClient), [] -> void {});
    disp.clientMsgs(client, wantOf(drvA));
    assert(expectsIn(toLive) == 1);
    auto const placed = toLive.back().expect().assign_id();

    // The first session's Hello, sent before, arrives now.
    disp.workerMsgs(deadSession, helloFrom("w1:50051", 2));
    assert(liveAborted == 1);

    // The builder reconnects, still expecting drvA.
    auto reconnect = helloFrom("w1:50051", 2);
    auto * running = reconnect.mutable_msgs(0)->mutable_hello()->add_running();
    running->set_drv_path(drvA);
    running->set_assign_id(placed);
    Dispatcher::Worker nextSession{.send = recordInto(toNext), .abort = {}, .id = {}};
    disp.workerMsgs(nextSession, reconnect);
    assert(deadAborted == 1);
    disp.workerGone(liveSession);
    disp.workerGone(deadSession);
    assert(connectedBuilders(disp) == 1);
    disp.clientMsgs(client, wantOf(drvB));
    assert(expectsIn(toNext) == 1 && toNext.back().expect().drv_path() == drvB);

    disp.clientGone(client);
    disp.workerGone(nextSession);
}

// A client that will be back (Want.linger_s) has its build kept once it
// leaves, for no longer than the scheduler's cap: the stats round revokes
// it when the cap is over, not the 600 s asked for. A client that asked for
// nothing has its build revoked once its hold (Core::holdMs) is over.
void testLingerCappedThenRevoked()
{
    constexpr double holdMs = nixgrpc::sched::Core::holdMs;
    constexpr double capMs = 2 * holdMs; // past the hold, so the cap decides
    constexpr uint32_t askedS = 600;
    Metrics metrics{""};
    double now = 0;
    Dispatcher disp(
        Dispatcher::Config{
            .defaultSystem = "x86_64-linux",
            .present = {},
            .lookupThreads = 0,
            .nowMs = [&now] -> double { return now; },
            .maxLingerMs = capMs},
        metrics);
    std::atomic<int> revokes{0};
    Dispatcher::Worker worker{
        .send = [&revokes](const SchedCmd & cmd) -> bool {
            if (cmd.has_revoke()) {
                revokes++;
                revokes.notify_all();
            }
            return true;
        },
        .abort = {},
        .id = {}};
    WorkerMsgs hello;
    auto * hel = hello.add_msgs()->mutable_hello();
    hel->set_addr("w0:50051");
    hel->add_systems("x86_64-linux");
    hel->set_max_jobs(2);
    disp.workerMsgs(worker, hello);

    auto const follow = [&disp](const char * drv, uint32_t lingerS) -> Dispatcher::ClientPtr {
        auto client = disp.connectClient([](const SchedMsg &) -> bool { return true; }, [] -> void {});
        ClientMsgs want;
        auto * wnt = want.add_msgs()->mutable_want();
        wnt->set_drv_path(drv);
        wnt->set_linger_s(lingerS);
        disp.clientMsgs(client, want);
        return client;
    };
    auto const chain = follow("/nix/store/00000000000000000000000000000000-chain.drv", askedS);
    auto const plain = follow("/nix/store/11111111111111111111111111111111-plain.drv", 0);
    disp.clientGone(plain);
    now = holdMs - 1;
    disp.tick();
    assert(revokes == 0);
    now = holdMs;
    disp.tick();
    assert(revokes == 1);
    auto const left = now;
    disp.clientGone(chain);
    now = left + capMs - 1;
    disp.tick();
    assert(revokes == 1);
    now = left + capMs;
    disp.tick();
    assert(revokes == 2);
    disp.workerGone(worker);
}

// A chain's build kept for its lease (Want.lease_id) shows as the sixth
// series of its feature set, `lingering`. A Release from another stream
// ends the lease: answered with Released and the number of builds it held,
// the build revoked on its worker. Once nobody asks for or offers the
// feature set, its six series go, on the stats thread's next round
// (workerGone does not force an export).
void testReleaseAndTheStatsSeries()
{
    constexpr uint32_t lingerS = 600;
    constexpr double capMs = 600e3;
    // The stats thread exports a round at a time: nothing to wait on but the series.
    constexpr auto poll = std::chrono::milliseconds(100);
    Metrics metrics{""};
    Dispatcher disp(
        Dispatcher::Config{.defaultSystem = "x86_64-linux", .present = {}, .lookupThreads = 0, .maxLingerMs = capMs},
        metrics);
    std::vector<std::string> revoked;
    Dispatcher::Worker worker{
        .send = [&revoked](const SchedCmd & cmd) -> bool {
            if (cmd.has_revoke()) {
                revoked.push_back(cmd.revoke().drv_path());
            }
            return true;
        },
        .abort = {},
        .id = {}};
    WorkerMsgs hello;
    auto * hel = hello.add_msgs()->mutable_hello();
    hel->set_addr("w0:50051");
    hel->add_systems("x86_64-linux");
    hel->add_features("kvm");
    hel->set_max_jobs(1);
    disp.workerMsgs(worker, hello);
    auto const series = [&metrics](const char * kind) -> std::optional<double> {
        return metrics.schedSystemNow("x86_64-linux", "kvm", kind);
    };
    constexpr auto drv = "/nix/store/00000000000000000000000000000000-chain.drv";

    auto link = disp.connectClient([](const SchedMsg &) -> bool { return true; }, [] -> void {});
    ClientMsgs want;
    auto * wnt = want.add_msgs()->mutable_want();
    wnt->set_drv_path(drv);
    wnt->add_required_features("kvm");
    wnt->set_linger_s(lingerS);
    wnt->set_lease_id("chain-1");
    disp.clientMsgs(link, want);
    disp.clientGone(link);
    // Exported within a round of the stats thread (Dispatcher::exportStats).
    auto const settle = [&](const char * kind, double want) -> void {
        while (series(kind) != want) {
            std::this_thread::sleep_for(poll);
        }
    };
    settle("lingering", 1.0);
    assert(series("lingering") == 1.0 && series("running") == 1.0 && series("slots") == 1.0);
    assert(revoked.empty());

    std::vector<SchedMsg> answers;
    auto releaser = disp.connectClient(
        [&answers](const SchedMsg & msg) -> bool {
            answers.push_back(msg);
            return true;
        },
        [] -> void {});
    auto const release = [&](const char * lease) -> void {
        ClientMsgs msgs;
        msgs.add_msgs()->mutable_release()->set_lease_id(lease);
        disp.clientMsgs(releaser, msgs);
    };
    release("unknown");
    release("chain-1");
    assert(answers.size() == 2 && answers.at(0).has_released() && answers.at(1).has_released());
    assert(answers.at(0).released().lease_id() == "unknown" && answers.at(0).released().builds() == 0);
    assert(answers.at(1).released().lease_id() == "chain-1" && answers.at(1).released().builds() == 1);
    assert(revoked == std::vector<std::string>{drv});
    // Revoked, it holds its slot until its Done: running, no longer lingering.
    settle("lingering", 0.0);
    assert(series("lingering") == 0.0 && series("free") == 0.0 && series("running") == 1.0);
    disp.clientGone(releaser);

    disp.workerGone(worker);
    // A gone worker's system keeps its series, at 0 (running counts per
    // worker since upstream's "count running builds per worker").
    auto const kinds = {"queued", "unplaceable", "running", "slots", "free", "lingering"};
    while (!std::ranges::all_of(kinds, [&](const char * kind) -> bool { return series(kind).value_or(0) == 0; })) {
        std::this_thread::sleep_for(poll);
    }
}

// A chain's build kept in the scheduler's own queue (the one slot busy) is
// forgotten once its lease is over, and the round that forgets it (the stats
// thread's, here the test's tick) exports that as it happens: `lingering`
// drops to 0 with nothing else going on.
void testKeptQueuedExpiryExported()
{
    constexpr uint32_t lingerS = 600;
    constexpr double capMs = 3000;
    Metrics metrics{""};
    // The test's clock, and its rounds (tick): the lease is over only once
    // the test says so.
    std::atomic<double> nowMs{0};
    Dispatcher disp(
        Dispatcher::Config{
            .defaultSystem = "x86_64-linux",
            .present = {},
            .lookupThreads = 0,
            .nowMs = [&nowMs] -> double { return nowMs; },
            .maxLingerMs = capMs},
        metrics);
    Dispatcher::Worker worker{.send = [](const SchedCmd &) -> bool { return true; }, .abort = {}, .id = {}};
    WorkerMsgs hello;
    auto * hel = hello.add_msgs()->mutable_hello();
    hel->set_addr("w0:50051");
    hel->add_systems("x86_64-linux");
    hel->add_features("kvm");
    hel->set_max_jobs(1);
    disp.workerMsgs(worker, hello);
    auto const series = [&metrics](const char * kind) -> std::optional<double> {
        return metrics.schedSystemNow("x86_64-linux", "kvm", kind);
    };
    auto const follow = [&disp](const char * drv, uint32_t linger, const char * lease) -> Dispatcher::ClientPtr {
        auto client = disp.connectClient([](const SchedMsg &) -> bool { return true; }, [] -> void {});
        ClientMsgs want;
        auto * wnt = want.add_msgs()->mutable_want();
        wnt->set_drv_path(drv);
        wnt->add_required_features("kvm");
        wnt->set_linger_s(linger);
        wnt->set_lease_id(lease);
        disp.clientMsgs(client, want);
        return client;
    };
    auto const busy = follow("/nix/store/00000000000000000000000000000000-busy.drv", 0, "");
    auto const chain = follow("/nix/store/11111111111111111111111111111111-chain.drv", lingerS, "chain-1");
    disp.clientGone(chain);
    // Exported within a round (a tick), the cap not yet over.
    disp.tick();
    assert(series("lingering") == 1.0 && series("queued") == 0.0 && series("running") == 1.0);
    // The round in which the cap is over exports it, with nothing else going on.
    nowMs = capMs + 1;
    disp.tick();
    assert(series("lingering") == 0.0 && series("queued") == 0.0);
    disp.clientGone(busy);
    disp.workerGone(worker);
}

// A kept build's linger runs out with nothing else going on: the stats
// thread revokes it, and the Revoke must reach a worker whose send only
// queues the write for after the unlock, as WorkerSession's does
// (sched-reactor.hh: afterUnlock(writeNext)). The stats thread takes its
// Lock afresh each round, and that Lock's release runs the queue: with one
// Lock held across rounds the queued write waited for some other thread's
// unlock. Its client's stream ends while the build is assigned, so the
// build is also held for that client (Core::holdMs) and kept until the
// later of the two: the wait runs past the hold.
void testExpiryRevokeReachesAWorker()
{
    constexpr double capMs = 1000;
    constexpr uint32_t lingerS = 600;
    Metrics metrics{""};
    Dispatcher disp(
        Dispatcher::Config{.defaultSystem = "x86_64-linux", .present = {}, .lookupThreads = 0, .maxLingerMs = capMs},
        metrics);
    std::atomic<int> written{0};
    Dispatcher::Worker worker{
        .send = [&disp, &written](const SchedCmd & cmd) ABSL_EXCLUSIVE_LOCKS_REQUIRED(disp.sendLock()) -> bool {
            if (cmd.has_revoke()) {
                disp.afterUnlock([&written] -> void {
                    written++;
                    written.notify_all();
                });
            }
            return true;
        },
        .abort = {},
        .id = {}};
    WorkerMsgs hello;
    auto * hel = hello.add_msgs()->mutable_hello();
    hel->set_addr("w0:50051");
    hel->add_systems("x86_64-linux");
    hel->set_max_jobs(1);
    disp.workerMsgs(worker, hello);
    auto chain = disp.connectClient([](const SchedMsg &) -> bool { return true; }, [] -> void {});
    ClientMsgs want;
    auto * wnt = want.add_msgs()->mutable_want();
    wnt->set_drv_path("/nix/store/22222222222222222222222222222222-chain.drv");
    wnt->set_linger_s(lingerS);
    wnt->set_lease_id("chain-1");
    disp.clientMsgs(chain, want);
    disp.clientGone(chain);
    assert(written == 0);
    written.wait(0);
    assert(written == 1);
    disp.workerGone(worker);
}

// A client whose build runs on the only worker that can build it refuses
// that assignment (it could not reach the worker again). The scheduler
// revokes the build there and places it there again, with a new id. The
// builder, wired to the scheduler as coordinator.cc wires them, ends the
// revoked build with a Done that must name the placement the scheduler
// holds now: otherwise the Done ends nothing, and the worker's one slot
// stays taken once the client is gone.
void aBounceOffTheOnlyWorkerLeavesNoSlotTaken()
{
    constexpr auto drvA = "/nix/store/00000000000000000000000000000000-a.drv";
    constexpr auto drvB = "/nix/store/11111111111111111111111111111111-b.drv";
    Metrics metrics{""};
    Dispatcher disp(debugConfig(), metrics);
    nixgrpc::Builder builder({.advertise = "w1:50051", .systems = {"x86_64-linux"}, .features = {}, .maxJobs = 1}, metrics);
    auto const worker = disp.loopbackWorker([&](const SchedCmd & cmd) -> void {
        if (cmd.has_expect()) {
            builder.onExpect(cmd.expect());
        } else if (cmd.has_revoke()) {
            builder.onRevoke(cmd.revoke());
        }
    });
    builder.setSend([&](const WorkerMsgs & msgs) -> bool {
        disp.workerMsgs(*worker, msgs);
        return true;
    });
    builder.greet();
    std::vector<SchedMsg> toFirst;
    auto first = disp.connectClient(recordMsgsInto(toFirst), [] -> void {});
    disp.clientMsgs(first, wantOf(drvA));
    assert(assigned(toFirst) == 1);
    auto const firstId = toFirst.back().assigned().assign_id();
    bool const claimed = builder.claim(drvA, firstId);
    auto const admitted = builder.admit(drvA);
    assert(claimed && admitted);
    auto refuse = wantOf(drvA);
    refuse.mutable_msgs(0)->mutable_want()->set_refused(firstId);
    disp.clientMsgs(first, refuse);
    builder.finished(drvA, nix::remote::Done::CANCELLED, {}, "");
    builder.callEnded(drvA);
    ClientMsgs cancel;
    cancel.add_msgs()->mutable_cancel()->set_drv_path(drvA);
    disp.clientMsgs(first, cancel);
    disp.clientGone(first);
    std::vector<SchedMsg> toNext;
    auto next = disp.connectClient(recordMsgsInto(toNext), [] -> void {});
    disp.clientMsgs(next, wantOf(drvB));
    assert(assigned(toNext) == 1);
    disp.clientGone(next);
    disp.workerGone(*worker);
    builder.setSend(nullptr);
}

// A takeover, seen from the successor: the scheduler that took over from
// one that placed a.drv on node n:1, whose old process still builds it for
// the client. One slot on n:1; b.drv waits for it.
struct Successor
{
    nixgrpc::exec::Executor exe{
        {.threads = 2, .rescheduleRetries = nixgrpc::exec::Executor::defaultRescheduleRetries, .keepGoing = true}};
    nixgrpc::exec::JobId jobA = exe.add(2, {});
    nixgrpc::exec::JobId jobB = exe.add(1, {});
    std::vector<std::string> drvOf{"a.drv", "b.drv"};
    Metrics metrics{""};
    Dispatcher disp{[] -> Dispatcher::Config {
                        Dispatcher::Config cfg;
                        cfg.defaultSystem = "x";
                        cfg.lookupThreads = 0;
                        return cfg;
                    }(),
                    metrics};
    std::vector<SchedCmd> toNode;
    Dispatcher::Worker node{.send = recordInto(toNode), .abort = {}, .id = {}};
    Dispatcher::ClientPtr client;

    Successor()
    {
        exe.connected();
        assert((exe.takeWants() == std::vector<nixgrpc::exec::JobId>{jobA, jobB}));
        // The predecessor placed a.drv on n:1 and the client called it there.
        constexpr uint64_t predecessorsAssignId = 7;
        exe.onAssigned(jobA, {.addr = "n:1", .assignId = predecessorsAssignId, .system = {}});
        auto const call = exe.next();
        assert(call && call->first == jobA);
        // Its Restarting ended the stream; the client is back, on the successor.
        exe.disconnected();
        WorkerMsgs hello;
        auto * hel = hello.add_msgs()->mutable_hello();
        hel->set_addr("n:1");
        hel->add_systems("x");
        hel->set_max_jobs(1);
        disp.workerMsgs(node, hello);
        client = disp.connectClient(
            [this](const SchedMsg & msg) -> bool {
                if (msg.has_assigned()) {
                    exe.onAssigned(
                        msg.assigned().drv_path() == "a.drv" ? jobA : jobB,
                        {.addr = msg.assigned().worker_addr(), .assignId = msg.assigned().assign_id(), .system = {}});
                }
                return true;
            },
            {});
        exe.connected();
        flush();
    }

    Successor(const Successor &) = delete;
    Successor(Successor &&) = delete;
    auto operator=(const Successor &) -> Successor & = delete;
    auto operator=(Successor &&) -> Successor & = delete;

    ~Successor()
    {
        disp.clientGone(client);
        disp.workerGone(node);
    }

    // The client's Wants, as GrpcStore's flushLocked sends them.
    void flush()
    {
        ClientMsgs wants;
        for (auto const jid : exe.takeWants()) {
            auto * want = wants.add_msgs()->mutable_want();
            want->set_drv_path(drvOf.at(jid));
            want->set_system("x");
            want->set_refused(exe.refused(jid));
            want->set_bounced_assign_id(exe.refused(jid));
            want->set_running_on(exe.runningOn(jid));
        }
        if (wants.msgs_size() > 0) {
            disp.clientMsgs(client, wants);
        }
    }

    [[nodiscard]] auto expected() const -> std::vector<std::string>
    {
        std::vector<std::string> out;
        for (const auto & cmd : toNode) {
            if (cmd.has_expect()) {
                out.push_back(cmd.expect().drv_path());
            }
        }
        return out;
    }

    [[nodiscard]] auto assignIdOf(const std::string & drv) const -> uint64_t
    {
        uint64_t last = 0;
        for (const auto & cmd : toNode) {
            if (cmd.has_expect() && cmd.expect().drv_path() == drv) {
                last = cmd.expect().assign_id();
            }
        }
        return last;
    }
};

// The client Wants a.drv again (Executor::connected) and leaves alone an
// Assigned to the address it builds on already (Executor::onAssigned).
// Nobody would claim such a placement, so it must not take the node's one
// slot from b.drv.
void aBuildInFlightOnThePredecessorTakesNoSlotHere()
{
    Successor const suc;
    assert(std::holds_alternative<nixgrpc::exec::state::Running>(suc.exe.at(suc.jobA)));
    // The intended assertion: the node's slot goes to the build that can use it.
    assert(suc.expected() == std::vector<std::string>{"b.drv"});
}

// Its build on the predecessor is lost: the client gives that call up
// (Executor::bounced) and Wants a.drv again, now not running anywhere. It
// must be placed here once the node has a slot.
void aBuildLostWithThePredecessorIsPlacedHere()
{
    Successor suc;
    suc.exe.bounced(suc.jobA, nixgrpc::exec::Clock::now(), {.refused = false, .left = true});
    suc.flush();
    if (suc.assignIdOf("b.drv") != 0) {
        suc.disp.workerMsgs(suc.node, doneOf("b.drv", suc.assignIdOf("b.drv"), nix::remote::Done::BUILT));
    }
    auto const got = suc.expected();
    assert(std::ranges::contains(got, std::string("a.drv")));
}

// The phantom an old client leaves after a takeover, seen whole: it re-Wants
// a build it runs through the predecessor and never calls here, sending no
// Want.running_on. The node's one slot holds that placement; once the
// predecessor has built it into the node's store, the builder finishes it
// as BUILT (Builder::checkBuiltWith) and the slot goes to the next drv. Given
// back instead, it would only be placed again for its client.
void aPhantomWhoseOutputsTheNodeHasFreesItsSlot()
{
    constexpr auto drvA = "/nix/store/00000000000000000000000000000000-a.drv";
    constexpr auto drvB = "/nix/store/11111111111111111111111111111111-b.drv";
    Metrics metrics{""};
    Dispatcher disp(debugConfig(), metrics);
    auto now = std::chrono::steady_clock::now();
    nixgrpc::Builder builder(
        {.advertise = "w1:50051",
         .systems = {"x86_64-linux"},
         .features = {},
         .maxJobs = 1,
         .now = [&now] -> std::chrono::steady_clock::time_point { return now; }},
        metrics);
    bool aBuilt = false;
    builder.checkBuiltWith([&](const std::string & drv) -> std::optional<std::vector<std::pair<std::string, uint64_t>>> {
        if (drv == drvA && aBuilt) {
            return std::vector<std::pair<std::string, uint64_t>>{{"/nix/store/22222222222222222222222222222222-a", 1}};
        }
        return std::nullopt;
    });
    auto const worker = disp.loopbackWorker([&](const SchedCmd & cmd) -> void {
        if (cmd.has_expect()) {
            builder.onExpect(cmd.expect());
        } else if (cmd.has_revoke()) {
            builder.onRevoke(cmd.revoke());
        }
    });
    builder.setSend([&](const WorkerMsgs & msgs) -> bool {
        disp.workerMsgs(*worker, msgs);
        return true;
    });
    builder.greet();
    std::vector<SchedMsg> toClient;
    auto client = disp.connectClient(recordMsgsInto(toClient), [] -> void {});
    disp.clientMsgs(client, wantOf(drvA));
    disp.clientMsgs(client, wantOf(drvB));
    assert(assigned(toClient) == 1); // a, never claimed
    constexpr auto probeAfter = std::chrono::seconds(10); // Builder::probeAfter
    now += probeAfter;
    builder.finishBuiltHere();
    assert(assigned(toClient) == 1 && builder.holding() == 1); // still building there
    // The predecessor's build of a ends.
    aBuilt = true;
    now += probeAfter;
    builder.finishBuiltHere();
    assert(assigned(toClient) == 2 && toClient.back().assigned().drv_path() == drvB);
    disp.clientGone(client);
    disp.workerGone(*worker);
    builder.setSend(nullptr);
}

// The node's daemon starts while its nix-daemon hangs: the builder greets
// draining (Coordinator::start after askLocalDaemon), and builds are wanted.
// The scheduler in the same process must place none on it. Each would come
// straight back and be placed there again, inside greet(), which the
// daemon's readiness waits for. Past `cut` Expects that loop is cut here, so
// that it fails the count instead of hanging.
void aBuilderThatGreetsDrainingIsPlacedNothing()
{
    constexpr auto drvA = "/nix/store/00000000000000000000000000000000-a.drv";
    constexpr size_t cut = 100;
    Metrics metrics{""};
    Dispatcher disp(debugConfig(), metrics);
    nixgrpc::Builder builder({.advertise = "w1:50051", .systems = {"x86_64-linux"}, .features = {}, .maxJobs = 2}, metrics);
    size_t expects = 0;
    auto const worker = disp.loopbackWorker([&](const SchedCmd & cmd) -> void {
        if (cmd.has_expect() && ++expects <= cut) {
            builder.onExpect(cmd.expect());
        }
    });
    builder.setSend([&](const WorkerMsgs & msgs) -> bool {
        disp.workerMsgs(*worker, msgs);
        return true;
    });
    std::vector<SchedMsg> toClient;
    auto client = disp.connectClient(recordMsgsInto(toClient), [] -> void {});
    disp.clientMsgs(client, wantOf(drvA));
    builder.setUnavailable(nixgrpc::Builder::Unavailable::localDaemonDown, true);
    assert(builder.greet());
    assert(expects == 0 && assigned(toClient) == 0);
    // Its nix-daemon answers again: the build goes there.
    builder.setUnavailable(nixgrpc::Builder::Unavailable::localDaemonDown, false);
    assert(expects == 1 && assigned(toClient) == 1);
    disp.clientGone(client);
    disp.workerGone(*worker);
    builder.setSend(nullptr);
}

} // namespace

auto main() -> int
try {
    testOldSessionEndKeepsTheReconnectedWorker();
    testOldSessionMessagesAfterTheNewHello();
    testReplacedSessionAfterTheWorkerLeft();
    testLateFirstHelloEndsInAReconnect();
    loadRestoresSlotsAfterDrainingHello();
    aSystemWithoutBuildersGetsEveryBuilder();
    aClientThatCannotReachItsWorkerIsSentElsewhere(false);
    aClientThatCannotReachItsWorkerIsSentElsewhere(true);
    aWorkerThatRestedGetsItsDerivationBack();
    aFailingWorkerIsRested();
    aBuildGivenBackForAFailureOfItsWorkerWaitsForTheOther();
    aBuildGivenBackForAMissingInputDoesNotWaitForABusyWorker();
    loopbackWorkerMayAnswerACommand();
    endOfADroppedSessionKeepsTheNewOne();
    dropPlacesItsWorkElsewhere();
    testLingerCappedThenRevoked();
    testReleaseAndTheStatsSeries();
    testKeptQueuedExpiryExported();
    testExpiryRevokeReachesAWorker();
    aBounceOffTheOnlyWorkerLeavesNoSlotTaken();
    aBuildInFlightOnThePredecessorTakesNoSlotHere();
    aBuildLostWithThePredecessorIsPlacedHere();
    aPhantomWhoseOutputsTheNodeHasFreesItsSlot();
    aBuilderThatGreetsDrainingIsPlacedNothing();
    return 0;
} catch (std::exception &) {
    return 1;
}
