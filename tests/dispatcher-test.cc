// Unit tests for the Dispatcher's worker messages, with in-memory peers (no gRPC).

#include <algorithm>
#include <atomic>
#include <cassert>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <initializer_list>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include <absl/base/thread_annotations.h>

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

// A client that will be back (Want.linger_s) has its build kept once it
// leaves, for no longer than the scheduler's cap: the stats thread revokes
// it when the cap is over, not the 600 s asked for. A client that asked for
// nothing has its build revoked once its reconnect grace is over.
void testLingerCappedThenRevoked()
{
    constexpr double capMs = 1000;
    constexpr uint32_t askedS = 600;
    constexpr auto patience = std::chrono::seconds(10);
    constexpr auto poll = std::chrono::milliseconds(100);
    Metrics metrics{""};
    constexpr double graceMs = 300;
    Dispatcher disp(
        Dispatcher::Config{
            .defaultSystem = "x86_64-linux",
            .present = {},
            .lookupThreads = 0,
            .maxLingerMs = capMs,
            .departedGraceMs = graceMs},
        metrics);
    std::atomic<int> revokes{0};
    Dispatcher::Worker worker{
        .send = [&revokes](const SchedCmd & cmd) -> bool {
            if (cmd.has_revoke()) {
                revokes++;
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
    auto const plainLeft = std::chrono::steady_clock::now();
    disp.clientGone(plain);
    assert(revokes == 0);
    while (revokes < 1 && std::chrono::steady_clock::now() - plainLeft < patience) {
        std::this_thread::sleep_for(poll);
    }
    assert(revokes == 1);
    assert(std::chrono::steady_clock::now() - plainLeft >= std::chrono::milliseconds(static_cast<int64_t>(graceMs)));
    auto const left = std::chrono::steady_clock::now();
    disp.clientGone(chain);
    assert(revokes == 1);
    while (revokes < 2 && std::chrono::steady_clock::now() - left < patience) {
        std::this_thread::sleep_for(poll);
    }
    assert(revokes == 2);
    assert(std::chrono::steady_clock::now() - left >= std::chrono::milliseconds(static_cast<int64_t>(capMs)));
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
    constexpr auto patience = std::chrono::seconds(10);
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
        auto const since = std::chrono::steady_clock::now();
        while (series(kind) != want && std::chrono::steady_clock::now() - since < patience) {
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
    auto const gone = std::chrono::steady_clock::now();
    while (series("slots") && std::chrono::steady_clock::now() - gone < patience) {
        std::this_thread::sleep_for(poll);
    }
    for (const auto * kind : {"queued", "unplaceable", "running", "slots", "free", "lingering"}) {
        assert(!series(kind));
    }
}

// A chain's build kept in the scheduler's own queue (the one slot busy) is
// forgotten once its lease is over, and the stats thread exports that as it
// happens: `lingering` drops to 0 with nothing else going on.
void testKeptQueuedExpiryExported()
{
    constexpr uint32_t lingerS = 600;
    constexpr double capMs = 3000;
    constexpr auto patience = std::chrono::seconds(10);
    constexpr auto poll = std::chrono::milliseconds(100);
    Metrics metrics{""};
    Dispatcher disp(
        Dispatcher::Config{.defaultSystem = "x86_64-linux", .present = {}, .lookupThreads = 0, .maxLingerMs = capMs},
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
    auto const left = std::chrono::steady_clock::now();
    disp.clientGone(chain);
    // Exported within a round of the stats thread, before the cap is over.
    while (series("lingering") != 1.0 && std::chrono::steady_clock::now() - left < patience) {
        std::this_thread::sleep_for(poll);
    }
    assert(series("lingering") == 1.0 && series("queued") == 0.0 && series("running") == 1.0);
    while (series("lingering") != 0.0 && std::chrono::steady_clock::now() - left < patience) {
        std::this_thread::sleep_for(poll);
    }
    assert(series("lingering") == 0.0 && series("queued") == 0.0);
    assert(std::chrono::steady_clock::now() - left >= std::chrono::milliseconds(static_cast<int64_t>(capMs)));
    disp.clientGone(busy);
    disp.workerGone(worker);
}

// A kept build's linger runs out with nothing else going on: the stats
// thread revokes it, and the Revoke must reach a worker whose send only
// queues the write for after the unlock, as WorkerSession's does
// (sched-reactor.hh: afterUnlock(writeNext)). The stats thread takes its
// Lock afresh each round, and that Lock's release runs the queue: with one
// Lock held across rounds the queued write waited for some other thread's
// unlock.
void testExpiryRevokeReachesAWorker()
{
    constexpr double capMs = 1000;
    constexpr uint32_t lingerS = 600;
    constexpr auto patience = std::chrono::seconds(10);
    constexpr auto poll = std::chrono::milliseconds(100);
    Metrics metrics{""};
    Dispatcher disp(
        Dispatcher::Config{.defaultSystem = "x86_64-linux", .present = {}, .lookupThreads = 0, .maxLingerMs = capMs},
        metrics);
    std::atomic<int> written{0};
    Dispatcher::Worker worker{
        .send = [&disp, &written](const SchedCmd & cmd) ABSL_EXCLUSIVE_LOCKS_REQUIRED(disp.sendLock()) -> bool {
            if (cmd.has_revoke()) {
                disp.afterUnlock([&written] -> void { written++; });
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
    auto const left = std::chrono::steady_clock::now();
    while (written < 1 && std::chrono::steady_clock::now() - left < patience) {
        std::this_thread::sleep_for(poll);
    }
    assert(written == 1);
    disp.workerGone(worker);
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
    testLingerCappedThenRevoked();
    testReleaseAndTheStatsSeries();
    testKeptQueuedExpiryExported();
    testExpiryRevokeReachesAWorker();
    return 0;
}
