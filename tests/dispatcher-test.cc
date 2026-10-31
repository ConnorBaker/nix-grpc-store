// Unit tests for the Dispatcher's worker messages, with in-memory peers (no gRPC).

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <initializer_list>
#include <vector>

#include "logfmt.hh"

#include "dispatcher.hh"
#include "metrics.hh"
#include "nix_remote.pb.h"

using nix::remote::ClientMsgs;
using nix::remote::SchedCmd;
using nix::remote::SchedMsg;
using nix::remote::WorkerMsgs;
using nixgrpc::Dispatcher;
using nixgrpc::Metrics;

namespace {

auto assigned(const std::vector<SchedMsg> & msgs) -> size_t
{
    return static_cast<size_t>(std::ranges::count_if(msgs, [](const SchedMsg & msg) -> bool { return msg.has_assigned(); }));
}

// Load.max_jobs changes a builder's slots without a reconnect, as after
// SIGHUP (Builder::reloadNixConfig); a Load without it, as older builders
// send, leaves them. The builder starts with none: older builders say Hello
// with max_jobs 0 while they drain.
void testLoadRestoresSlotsAfterDrainingHello()
{
    Metrics metrics{""};
    Dispatcher disp(Dispatcher::Config{.defaultSystem = "x86_64-linux", .present = {}, .lookupThreads = 0}, metrics);
    Dispatcher::Worker worker{.send = [](const SchedCmd &) -> bool { return true; }, .id = {}};
    WorkerMsgs hello;
    auto * hel = hello.add_msgs()->mutable_hello();
    hel->set_addr("w0:50051");
    hel->add_systems("x86_64-linux");
    hel->set_max_jobs(0);
    disp.workerMsgs(worker, hello);

    std::vector<SchedMsg> toClient;
    auto client = disp.connectClient(
        [&toClient](const SchedMsg & msg) -> bool {
            toClient.push_back(msg);
            return true;
        },
        [] -> void {});
    ClientMsgs want;
    want.add_msgs()->mutable_want()->set_drv_path("/nix/store/00000000000000000000000000000000-a.drv");
    disp.clientMsgs(client, want);

    WorkerMsgs older;
    older.add_msgs()->mutable_load()->set_draining(false);
    disp.workerMsgs(worker, older);
    assert(assigned(toClient) == 0);
    assert(disp.membership()->at("x86_64-linux").front().draining);

    WorkerMsgs load;
    auto * recovered = load.add_msgs()->mutable_load();
    recovered->set_draining(false);
    recovered->set_max_jobs(2);
    disp.workerMsgs(worker, load);
    assert(assigned(toClient) == 1);
    assert(!disp.membership()->at("x86_64-linux").front().draining);

    disp.clientGone(client);
    disp.workerGone(worker);
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

auto expectsIn(const std::vector<SchedCmd> & cmds) -> size_t
{
    return static_cast<size_t>(std::ranges::count_if(cmds, [](const SchedCmd & cmd) -> bool { return cmd.has_expect(); }));
}

// A builder's Hello (Builder::hello), for x86_64-linux. `namesPlacements`
// false: a builder from before Hello.names_placements.
auto helloFrom(const char * addr, unsigned maxJobs, bool namesPlacements = true) -> WorkerMsgs
{
    WorkerMsgs msgs;
    auto * hel = msgs.add_msgs()->mutable_hello();
    hel->set_addr(addr);
    hel->add_systems("x86_64-linux");
    hel->set_max_jobs(maxJobs);
    hel->set_names_placements(namesPlacements);
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
    Dispatcher disp(debugConfig(), metrics);
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

    // The current session's end still takes w1 away, and one of its builds
    // goes to w2's slot.
    disp.workerGone(newSession);
    assert(connectedBuilders(disp) == 1);
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

// A builder can send a Done on its new session after that session's Hello,
// for a drv the Hello left out: the Done was made first (the builder drops
// the drv from what it expects before it sends it), and the Hello was built
// in between. The Hello made the scheduler place the drv on the builder
// again. That Done names the earlier placement and must end nothing: the
// new placement keeps its slot until its own Done.
void testDoneEndsOnlyThePlacementItNames()
{
    constexpr auto drvA = "/nix/store/00000000000000000000000000000000-a.drv";
    constexpr auto drvB = "/nix/store/11111111111111111111111111111111-b.drv";
    Metrics metrics{""};
    Dispatcher disp(debugConfig(), metrics);
    std::vector<SchedCmd> toOld;
    std::vector<SchedCmd> toNew;
    std::vector<SchedMsg> toFirst;
    std::vector<SchedMsg> toSecond;
    Dispatcher::Worker oldSession{.send = recordInto(toOld), .abort = {}, .id = {}};
    disp.workerMsgs(oldSession, helloFrom("w1:50051", 1));
    auto first = disp.connectClient(recordMsgsInto(toFirst), [] -> void {});
    disp.clientMsgs(first, wantOf(drvA));
    assert(expectsIn(toOld) == 1);
    Dispatcher::Worker newSession{.send = recordInto(toNew), .abort = {}, .id = {}};
    disp.workerMsgs(newSession, helloFrom("w1:50051", 1));
    assert(expectsIn(toNew) == 1);

    // The first placement was given back (nobody came); that Done comes now.
    auto back = doneOf(drvA, toOld.back().expect().assign_id(), nix::remote::Done::GIVEN_BACK);
    back.mutable_msgs(0)->mutable_done()->set_reason(nix::remote::Done::UNREACHABLE);
    disp.workerMsgs(newSession, back);

    // drvA's new placement stands: another client is told where it runs,
    // and drvB finds w1's one slot taken.
    auto second = disp.connectClient(recordMsgsInto(toSecond), [] -> void {});
    disp.clientMsgs(second, wantOf(drvA));
    assert(expectsIn(toNew) == 1);
    assert(assigned(toSecond) == 1);
    assert(toSecond.back().assigned().assign_id() == toNew.back().expect().assign_id());
    disp.clientMsgs(second, wantOf(drvB));
    assert(expectsIn(toNew) == 1);

    // Its own Done ends it, and drvB takes the slot.
    disp.workerMsgs(newSession, doneOf(drvA, toNew.back().expect().assign_id(), nix::remote::Done::BUILT));
    assert(expectsIn(toNew) == 2);
    assert(toNew.back().expect().drv_path() == drvB);
    // The stale give-back did not go unnoticed.
    assert(metrics.eventCounter("done_other_placement").Value() == 1);

    disp.clientGone(first);
    disp.clientGone(second);
    disp.workerGone(oldSession);
    disp.workerGone(newSession);
}

// A client bounces off the builder that runs its drv (it could not reach
// it), and no other builder can run it: the scheduler places the drv there
// again, with a new assign id. The slot is free again after that build's
// Done if the Done names the placement the scheduler holds: the new one
// from a builder that names placements, which adopts the new id for the
// build it runs, or any one from a builder that predates that
// (Hello.names_placements unset), which keeps the first id. A Done naming
// the first placement from a builder that names placements ends nothing.
// NOLINTNEXTLINE(bugprone-easily-swappable-parameters): two flags, named at each call
auto slotFreeAfterBounce(bool namesPlacements, bool doneNamesFirst, Metrics & metrics) -> bool
{
    constexpr auto drvA = "/nix/store/00000000000000000000000000000000-a.drv";
    constexpr auto drvB = "/nix/store/11111111111111111111111111111111-b.drv";
    Dispatcher disp(debugConfig(), metrics);
    std::vector<SchedCmd> toBuilder;
    std::vector<SchedMsg> toFirst;
    std::vector<SchedMsg> toSecond;
    std::vector<SchedMsg> toThird;
    Dispatcher::Worker builder{.send = recordInto(toBuilder), .abort = {}, .id = {}};
    disp.workerMsgs(builder, helloFrom("w1:50051", 1, namesPlacements));
    auto first = disp.connectClient(recordMsgsInto(toFirst), [] -> void {});
    disp.clientMsgs(first, wantOf(drvA));
    assert(expectsIn(toBuilder) == 1);
    auto const firstId = toBuilder.back().expect().assign_id();
    auto second = disp.connectClient(recordMsgsInto(toSecond), [] -> void {});
    disp.clientMsgs(second, wantOf(drvA));
    assert(assigned(toSecond) == 1);
    auto bounce = wantOf(drvA);
    bounce.mutable_msgs(0)->mutable_want()->set_bounced_assign_id(firstId);
    disp.clientMsgs(second, bounce);
    assert(expectsIn(toBuilder) == 2 && toBuilder.back().expect().drv_path() == drvA);
    auto const againId = toBuilder.back().expect().assign_id();
    assert(againId != firstId);
    disp.workerMsgs(builder, doneOf(drvA, doneNamesFirst ? firstId : againId, nix::remote::Done::BUILT));
    auto const cancel = [](const char * drv) -> ClientMsgs {
        ClientMsgs msgs;
        msgs.add_msgs()->mutable_cancel()->set_drv_path(drv);
        return msgs;
    };
    disp.clientMsgs(first, cancel(drvA));
    disp.clientMsgs(second, cancel(drvA));
    auto third = disp.connectClient(recordMsgsInto(toThird), [] -> void {});
    disp.clientMsgs(third, wantOf(drvB));
    bool const freed = expectsIn(toBuilder) == 3;
    disp.clientGone(first);
    disp.clientGone(second);
    disp.clientGone(third);
    disp.workerGone(builder);
    return freed;
}

void testBounceOntoTheBuildingWorker()
{
    Metrics adopts{""};
    assert(slotFreeAfterBounce(true, false, adopts));
    assert(adopts.eventCounter("done_other_placement").Value() == 0);
    Metrics late{""};
    assert(!slotFreeAfterBounce(true, true, late));
    assert(late.eventCounter("done_other_placement").Value() == 1);
    Metrics older{""};
    assert(slotFreeAfterBounce(false, true, older));
    assert(older.eventCounter("done_other_placement").Value() == 0);
}

} // namespace

auto main() -> int
{
    testOldSessionEndKeepsTheReconnectedWorker();
    testOldSessionMessagesAfterTheNewHello();
    testReplacedSessionAfterTheWorkerLeft();
    testLateFirstHelloEndsInAReconnect();
    testDoneEndsOnlyThePlacementItNames();
    testBounceOntoTheBuildingWorker();
    testLoadRestoresSlotsAfterDrainingHello();
    return 0;
}
