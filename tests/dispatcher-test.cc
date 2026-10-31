// The Dispatcher driven through the calls its transports make, without gRPC.

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <initializer_list>
#include <memory>
#include <string>
#include <vector>

#include "logfmt.hh"

#include "dispatcher.hh"
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

    void want(const std::string & drv, uint64_t refused = 0)
    {
        nix::remote::ClientMsgs msgs;
        auto * wnt = msgs.add_msgs()->mutable_want();
        wnt->set_drv_path(drv);
        wnt->set_system("x");
        wnt->set_refused(refused);
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

// A client that cannot reach the worker it was sent to is sent to another.
void aClientThatCannotReachItsWorkerIsSentElsewhere()
{
    Farm farm;
    Session unreachable;
    Session other;
    constexpr uint32_t more = 4; // so it is picked first
    farm.hello(unreachable.conn, "w:1", more);
    farm.hello(other.conn, "w:2");
    farm.want("a.drv");
    assert(farm.sentTo == std::vector<std::string>{"w:1"});

    farm.want("a.drv", unreachable.lastAssignId);
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

} // namespace

auto main() -> int
try {
    testOldSessionEndKeepsTheReconnectedWorker();
    testOldSessionMessagesAfterTheNewHello();
    testReplacedSessionAfterTheWorkerLeft();
    testLateFirstHelloEndsInAReconnect();
    loadRestoresSlotsAfterDrainingHello();
    aSystemWithoutBuildersGetsEveryBuilder();
    aClientThatCannotReachItsWorkerIsSentElsewhere();
    aWorkerThatRestedGetsItsDerivationBack();
    aFailingWorkerIsRested();
    loopbackWorkerMayAnswerACommand();
    endOfADroppedSessionKeepsTheNewOne();
    dropPlacesItsWorkElsewhere();
    return 0;
} catch (std::exception &) {
    return 1;
}
