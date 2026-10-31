// Unit tests for the placement core (no gRPC).

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <iostream>
#include <map>
#include <random>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <nix/util/error.hh>

#include "scheduler.hh"

using namespace nixgrpc::sched;

// NOLINTBEGIN(*-magic-numbers, *-avoid-unchecked-container-access, bugprone-unchecked-optional-access, readability-identifier-length): test vectors, asserted sizes.
namespace {

void testHeapAgainstMap()
{
    IndexedHeap heap;
    std::map<DrvId, double> ref;
    std::mt19937 rng(42); // NOLINT(cert-msc32-c,cert-msc51-cpp,bugprone-random-generator-seed): reproducible
    for (int step = 0; step < 20000; step++) {
        auto const id = static_cast<DrvId>(rng() % 200);
        switch (rng() % 3) {
        case 0:
        case 1: {
            auto const key = static_cast<double>(rng() % 1000);
            heap.set(id, key);
            ref[id] = key;
            break;
        }
        default:
            heap.erase(id);
            ref.erase(id);
        }
        assert(heap.size() == ref.size());
        if (!ref.empty()) {
            auto const top = *heap.top();
            double minKey = ref.begin()->second;
            for (auto & [k, v] : ref) {
                minKey = std::min(minKey, v);
            }
            assert(ref.at(top) == minKey);
        }
    }
}

void testFreeIndex()
{
    FreeIndex fi;
    for (WorkerId w = 0; w < 5; w++) {
        fi.push(w);
    }
    assert(!fi.emptiest());
    fi.set(2, 4);
    fi.set(3, 2);
    assert(fi.maxFree() == 4 && *fi.emptiest() == 2);
    fi.set(2, 1);
    assert(*fi.emptiest() == 3);
    fi.set(3, -5); // trimmed below running
    assert(fi.get(3) == -5 && *fi.emptiest() == 2);
    fi.set(2, 0);
    assert(!fi.emptiest());
    fi.set(4, 100); // clamped bucket, value kept
    assert(fi.get(4) == 100 && *fi.emptiest() == 4);
}

auto hello(Core & shard, std::string_view addr, uint32_t jobs, const std::vector<std::string_view> & running = {}, std::vector<std::string> features = {}, std::vector<Core::Superseded> * superseded = nullptr, bool faulty = false, bool namesPlacements = true) -> WorkerId
{
    std::vector<std::pair<std::string_view, uint64_t>> runs;
    runs.reserve(running.size());
    for (auto const drv : running) {
        runs.emplace_back(drv, 7);
    }
    std::vector<Core::Superseded> ignored;
    return shard.hello({.addr = addr, .systems = {"x"}, .features = std::move(features), .maxJobs = jobs, .running = std::move(runs), .faulty = faulty, .namesPlacements = namesPlacements}, superseded != nullptr ? *superseded : ignored);
}

void testAssignAndDedup()
{
    Core shard;
    std::vector<Assign> out;
    shard.want(1, {.drvPath = "a.drv", .inputs = {}, .system = "x", .features = {}, .cpHintMs = 0}, 0);
    shard.want(2, {.drvPath = "a.drv", .inputs = {}, .system = "x", .features = {}, .cpHintMs = 0}, 0);
    shard.dispatch(out);
    assert(out.empty()); // no workers
    auto const w0 = hello(shard, "10.0.0.1:1", 2);
    shard.dispatch(out);
    assert(out.size() == 1 && out[0].worker == w0 && out[0].clients.size() == 2);
    // third client after assignment: immediate
    auto res = shard.want(3, {.drvPath = "a.drv", .inputs = {}, .system = "x", .features = {}, .cpHintMs = 0}, 0);
    assert(res.assigned && res.assigned->first == w0 && res.assigned->second == out[0].assignId);
    shard.done(w0, "a.drv", {{"/nix/store/a-out", 100}});
    assert(!shard.entry(out[0].drv));
}

void testMostFreeAndLocality()
{
    Core shard;
    std::vector<Assign> out;
    auto const w0 = hello(shard, "w0", 4);
    auto const w1 = hello(shard, "w1", 4);
    // w1 produced a big input
    shard.done(w1, "x.drv", {{"/nix/store/big", 1'000'000'000}});
    shard.want(1, {.drvPath = "b.drv", .inputs = {"/nix/store/big"}, .system = "x", .features = {}, .cpHintMs = 0}, 0);
    shard.dispatch(out);
    assert(out.size() == 1 && out[0].worker == w1);
    // without locality: most free (w0 has 4, w1 has 3)
    shard.want(1, {.drvPath = "c.drv", .inputs = {}, .system = "x", .features = {}, .cpHintMs = 0}, 0);
    shard.dispatch(out);
    assert(out.size() == 2 && out[1].worker == w0);
}

void testPriority()
{
    Core shard;
    std::vector<Assign> out;
    shard.want(1, {.drvPath = "low.drv", .inputs = {}, .system = "x", .features = {}, .cpHintMs = 0}, 0);
    shard.want(1, {.drvPath = "high.drv", .inputs = {}, .system = "x", .features = {}, .cpHintMs = 100000}, 0);
    shard.want(2, {.drvPath = "fan.drv", .inputs = {}, .system = "x", .features = {}, .cpHintMs = 0}, 0);
    shard.want(3, {.drvPath = "fan.drv", .inputs = {}, .system = "x", .features = {}, .cpHintMs = 0}, 0);
    shard.want(4, {.drvPath = "fan.drv", .inputs = {}, .system = "x", .features = {}, .cpHintMs = 0}, 0);
    shard.want(5, {.drvPath = "fan.drv", .inputs = {}, .system = "x", .features = {}, .cpHintMs = 0}, 0);
    shard.want(6, {.drvPath = "fan.drv", .inputs = {}, .system = "x", .features = {}, .cpHintMs = 0}, 0); // 4 extra followers * 30e3 = 120e3 > 100e3
    hello(shard, "w", 1);
    shard.dispatch(out);
    assert(out.size() == 1 && shard.entry(out[0].drv)->drvPath == "fan.drv");
    shard.done(0, "fan.drv", {});
    shard.dispatch(out);
    assert(out.size() == 2 && shard.entry(out[1].drv)->drvPath == "high.drv");
}

void testWorkerGoneRequeues()
{
    Core shard;
    std::vector<Assign> out;
    auto const w0 = hello(shard, "w0", 1);
    shard.want(1, {.drvPath = "a.drv", .inputs = {}, .system = "x", .features = {}, .cpHintMs = 0}, 0);
    shard.dispatch(out);
    assert(out.size() == 1);
    shard.workerGone(w0);
    assert(shard.queued() == 1);
    auto const w1 = hello(shard, "w1", 1);
    shard.dispatch(out);
    assert(out.size() == 2 && out[1].worker == w1 && out[1].clients.size() == 1);
}

void testHelloReconciles()
{
    Core shard;
    std::vector<Assign> out;
    // scheduler restarted: worker says it is running a.drv
    auto const w0 = hello(shard, "w0", 2, {"a.drv"});
    auto res = shard.want(1, {.drvPath = "a.drv", .inputs = {}, .system = "x", .features = {}, .cpHintMs = 0}, 0);
    assert(res.assigned && res.assigned->first == w0);
    shard.want(1, {.drvPath = "b.drv", .inputs = {}, .system = "x", .features = {}, .cpHintMs = 0}, 0);
    shard.want(1, {.drvPath = "c.drv", .inputs = {}, .system = "x", .features = {}, .cpHintMs = 0}, 0);
    shard.dispatch(out);
    assert(out.size() == 1); // only one slot left
    shard.done(w0, "a.drv", {});
    shard.dispatch(out);
    assert(out.size() == 2);
}

// Scheduler restarted while w1 built a.drv. A client re-Wants before w1 is
// back, so it goes to w0. Then w1 says Hello: w1 keeps it, w0 is superseded
// and the client is a follower of w1's build.
void testHelloSupersedes()
{
    Core shard;
    std::vector<Assign> out;
    auto const w0 = hello(shard, "w0", 1);
    shard.want(1, {.drvPath = "a.drv", .inputs = {}, .system = "x", .features = {}, .cpHintMs = 0}, 0);
    shard.dispatch(out);
    assert(out.size() == 1 && out[0].worker == w0);
    std::vector<Core::Superseded> sup;
    auto const w1 = hello(shard, "w1", 1, {"a.drv"}, {}, &sup);
    assert(sup.size() == 1 && sup[0].loser == w0);
    const auto & ent = shard.entry(sup[0].drv);
    assert(ent && ent->worker == w1 && ent->assignId == 7);
    assert(ent->followers.size() == 1 && ent->followers[0] == 1);
    assert(shard.worker(w0).running.empty());
    // w0's slot is free again for something else.
    shard.want(2, {.drvPath = "b.drv", .inputs = {}, .system = "x", .features = {}, .cpHintMs = 0}, 0);
    out.clear();
    shard.dispatch(out);
    assert(out.size() == 1 && out[0].worker == w0);
}

void testCancelAndClientGone()
{
    Core shard;
    std::vector<Assign> out;
    shard.want(1, {.drvPath = "a.drv", .inputs = {}, .system = "x", .features = {}, .cpHintMs = 0}, 0);
    shard.want(2, {.drvPath = "a.drv", .inputs = {}, .system = "x", .features = {}, .cpHintMs = 0}, 0);
    assert(!shard.cancel(1, "a.drv", 0));
    assert(shard.queued() == 1);
    assert(!shard.cancel(2, "a.drv", 0));
    assert(shard.queued() == 0);
    shard.want(1, {.drvPath = "b.drv", .inputs = {}, .system = "x", .features = {}, .cpHintMs = 0}, 0);
    auto const w0 = hello(shard, "w0", 1);
    shard.dispatch(out);
    std::vector<std::pair<DrvId, WorkerId>> revokes;
    std::vector<DrvId> kept;
    shard.clientGone(1, 0, revokes, kept);
    assert(revokes.size() == 1 && revokes[0].second == w0 && kept.empty());
    assert(!shard.cancel(9, "nope.drv", 0));
}

// The client could not reach w0. The drv goes to w1 once w1 has a slot, and
// back to w0 only when nobody else can run it.
// A drv that a client bounced off w0, and then off the worker it went to
// next, is placed on the third, though w0 has more slots free: bounces
// feed the same list as give-backs.
void testBouncedOffTwiceTriesAThirdWorker()
{
    Core shard;
    std::vector<Assign> out;
    auto const w0 = hello(shard, "w0", 2);
    auto const w1 = hello(shard, "w1", 1);
    auto const w2 = hello(shard, "w2", 1);
    auto const want = [&](uint64_t bounced) -> Core::WantResult {
        return shard.want(
            1,
            {.drvPath = "a.drv", .inputs = {}, .system = "x", .features = {}, .cpHintMs = 0, .bouncedAssignId = bounced},
            0);
    };
    want(0);
    shard.dispatch(out);
    assert(out.size() == 1 && out[0].worker == w0);
    assert(want(out[0].assignId).bouncedOff == w0);
    shard.dispatch(out);
    assert(out.size() == 2 && (out[1].worker == w1 || out[1].worker == w2));
    auto const second = out[1].worker;
    assert(want(out[1].assignId).bouncedOff == second);
    shard.dispatch(out);
    assert(out.size() == 3 && out[2].worker != w0 && out[2].worker != second);
}

// Two workers, w0 with two slots and w1 with one, and a client that
// bounces off wherever the drv goes, five times: once both have, the one
// it bounced off last is passed over, so they take turns rather than w0
// getting it every time.
void testRepeatBouncesTakeTurns()
{
    Core shard;
    std::vector<Assign> out;
    auto const w0 = hello(shard, "w0", 2);
    auto const w1 = hello(shard, "w1", 1);
    auto const want = [&](uint64_t bounced) -> Core::WantResult {
        return shard.want(
            1,
            {.drvPath = "a.drv", .inputs = {}, .system = "x", .features = {}, .cpHintMs = 0, .bouncedAssignId = bounced},
            0);
    };
    want(0);
    shard.dispatch(out);
    std::vector<WorkerId> placed;
    for (int round = 0; round < 5; round++) {
        assert(out.size() == placed.size() + 1);
        placed.push_back(out.back().worker);
        assert(want(out.back().assignId).bouncedOff == placed.back());
        shard.dispatch(out);
    }
    placed.push_back(out.back().worker);
    assert((placed == std::vector<WorkerId>{w0, w1, w0, w1, w0, w1}));
}

void testBouncedOff()
{
    Core shard;
    std::vector<Assign> out;
    auto const w0 = hello(shard, "w0", 1);
    shard.want(1, {.drvPath = "a.drv", .inputs = {}, .system = "x", .features = {}, .cpHintMs = 0}, 0);
    shard.dispatch(out);
    auto const w1 = hello(shard, "w1", 1);
    shard.want(1, {.drvPath = "busy.drv", .inputs = {}, .system = "x", .features = {}, .cpHintMs = 0}, 0);
    shard.dispatch(out);
    assert(out.size() == 2 && out[0].worker == w0 && out[1].worker == w1);
    auto const asg = out[0];
    auto const busy = out[1];
    // An id that is not the drv's own changes nothing.
    auto res = shard.want(
        1,
        {.drvPath = "busy.drv", .inputs = {}, .system = "x", .features = {}, .cpHintMs = 0, .bouncedAssignId = asg.assignId},
        0);
    assert(res.assigned && res.assigned->first == w1 && !res.bouncedOff);
    res = shard.want(
        1,
        {.drvPath = "a.drv", .inputs = {}, .system = "x", .features = {}, .cpHintMs = 0, .bouncedAssignId = asg.assignId},
        0);
    assert(!res.assigned && res.bouncedOff == w0 && shard.worker(w0).running.empty());
    out.clear();
    shard.dispatch(out);
    assert(out.empty()); // w0 is free but did not take it, w1 is busy
    shard.done(w1, shard.entry(busy.drv)->drvPath, {});
    shard.dispatch(out);
    assert(out.size() == 1 && out[0].worker == w1 && out[0].assignId != asg.assignId);
    // Only w0 is left: it gets another try.
    res = shard.want(
        1,
        {.drvPath = "a.drv", .inputs = {}, .system = "x", .features = {}, .cpHintMs = 0, .bouncedAssignId = out[0].assignId},
        0);
    assert(!res.assigned && res.bouncedOff == w1);
    shard.workerGone(w1);
    out.clear();
    shard.dispatch(out);
    assert(out.size() == 1 && out[0].worker == w0);
}

// w0 built a.drv but could not publish it, and gave it back. It gets
// builds only while w1 is full, until it can publish again.
void testFaulty()
{
    Core shard;
    std::vector<Assign> out;
    auto const w0 = hello(shard, "w0", 2);
    shard.want(1, {.drvPath = "a.drv", .inputs = {}, .system = "x", .features = {}, .cpHintMs = 0}, 0);
    shard.dispatch(out);
    auto const w1 = hello(shard, "w1", 1);
    assert(out.size() == 1 && out[0].worker == w0);
    shard.setFaulty(w0, true);
    shard.giveBack(w0, "a.drv");
    assert(shard.entry(out[0].drv) && shard.worker(w0).running.empty());
    shard.dispatch(out);
    assert(out.size() == 2 && out[1].worker == w1 && out[1].clients == std::vector<ClientId>{1});
    shard.want(1, {.drvPath = "b.drv", .inputs = {}, .system = "x", .features = {}, .cpHintMs = 0}, 0);
    shard.dispatch(out);
    assert(out.size() == 3 && out[2].worker == w0); // w1 is full
    shard.done(w0, "b.drv", {});
    shard.done(w1, "a.drv", {});
    shard.want(1, {.drvPath = "c.drv", .inputs = {}, .system = "x", .features = {}, .cpHintMs = 0}, 0);
    shard.dispatch(out);
    assert(out.size() == 4 && out[3].worker == w1); // though w0 has more slots free
    shard.setFaulty(w0, false);
    shard.want(1, {.drvPath = "d.drv", .inputs = {}, .system = "x", .features = {}, .cpHintMs = 0}, 0);
    shard.done(w1, "c.drv", {});
    shard.dispatch(out);
    assert(out.size() == 5 && out[4].worker == w0);
    shard.done(w0, "d.drv", {});
    // A reconnect carries the state.
    hello(shard, "w0", 2, {}, {}, nullptr, true);
    assert(shard.worker(w0).faulty);
    // Given back with nobody waiting for it any more: forgotten.
    shard.want(1, {.drvPath = "e.drv", .inputs = {}, .system = "x", .features = {}, .cpHintMs = 0}, 0);
    shard.dispatch(out);
    assert(out.size() == 6 && out[5].worker == w1);
    assert(shard.cancel(1, "e.drv", 0) == w1);
    shard.giveBack(w1, "e.drv");
    assert(!shard.findDrv("e.drv"));
}

// A Done that names an earlier placement of its drv ends nothing, but a
// BUILT one's outputs are on that worker all the same: a build that needs
// one of them is placed there, not on a worker with more free slots.
void testOtherPlacementsDoneTellsWhereOutputsAre()
{
    Core shard;
    std::vector<Assign> out;
    auto const w0 = hello(shard, "w0", 4);
    shard.want(1, {.drvPath = "a.drv", .inputs = {}, .system = "x", .features = {}, .cpHintMs = 0}, 0);
    shard.dispatch(out);
    assert(out.size() == 1 && out[0].worker == w0);
    auto const first = out[0].assignId;
    // w0's next session's Hello leaves a.drv out: placed there again.
    hello(shard, "w0", 4);
    shard.dispatch(out);
    assert(out.size() == 2 && out[1].worker == w0 && out[1].assignId != first);
    auto const w1 = hello(shard, "w1", 4); // more free slots than w0 now
    assert(!shard.done(w0, "a.drv", {{"/nix/store/a-out", 1'000'000'000}}, first));
    assert(shard.entry(out[1].drv)->worker == w0 && shard.entry(out[1].drv)->assignId == out[1].assignId);
    shard.want(2, {.drvPath = "b.drv", .inputs = {"/nix/store/a-out"}, .system = "x", .features = {}, .cpHintMs = 0}, 0);
    shard.dispatch(out);
    assert(out.size() == 3 && out[2].worker == w0 && out[2].worker != w1);
}

void testDraining()
{
    Core shard;
    std::vector<Assign> out;
    auto const w0 = hello(shard, "w0", 2);
    shard.setDraining(w0, true);
    auto const res = shard.want(1, {.drvPath = "a.drv", .inputs = {}, .system = "x", .features = {}, .cpHintMs = 0}, 0);
    shard.dispatch(out);
    assert(out.empty());
    assert(!shard.placeable(res.drv));
    shard.setDraining(w0, false);
    assert(shard.placeable(res.drv));
    shard.dispatch(out);
    assert(out.size() == 1);
}

// Load.max_jobs: the worker's capacity changed while it stayed connected.
void testSetMaxJobs()
{
    Core shard;
    std::vector<Assign> out;
    auto const w0 = hello(shard, "w0", 1);
    for (auto const * drv : {"a.drv", "b.drv", "c.drv"}) {
        shard.want(1, {.drvPath = drv, .inputs = {}, .system = "x", .features = {}, .cpHintMs = 0}, 0);
    }
    shard.dispatch(out);
    assert(out.size() == 1);
    shard.setMaxJobs(w0, 3);
    shard.dispatch(out);
    assert(out.size() == 3 && shard.queued() == 0);
    // Shrunk below what runs: nothing new until the excess finishes.
    shard.setMaxJobs(w0, 1);
    shard.want(1, {.drvPath = "d.drv", .inputs = {}, .system = "x", .features = {}, .cpHintMs = 0}, 0);
    shard.done(w0, "a.drv", {});
    shard.done(w0, "b.drv", {});
    shard.dispatch(out);
    assert(out.size() == 3);
    shard.done(w0, "c.drv", {});
    shard.dispatch(out);
    assert(out.size() == 4 && out[3].worker == w0);
    // Clamped as Hello's is.
    shard.setMaxJobs(w0, 1000);
    auto sts = shard.stats();
    assert((sts[{.system = "x", .features = ""}].slots == FreeIndex::maxSlots));
}

void testFeatures()
{
    Core shard;
    std::vector<Assign> out;
    auto const plain = hello(shard, "plain", 4);
    auto const kvm = hello(shard, "kvm", 1, {}, {"kvm"});
    shard.want(1, {.drvPath = "vm.drv", .inputs = {}, .system = "x", .features = {"kvm"}, .cpHintMs = 9e9}, 0);
    shard.want(1, {.drvPath = "vm2.drv", .inputs = {}, .system = "x", .features = {"kvm"}, .cpHintMs = 9e9}, 0);
    shard.want(1, {.drvPath = "a.drv", .inputs = {}, .system = "x", .features = {}, .cpHintMs = 0}, 0);
    shard.want(1, {.drvPath = "gpu.drv", .inputs = {}, .system = "x", .features = {"cuda"}, .cpHintMs = 0}, 0);
    auto const gpu = *shard.findDrv("gpu.drv");
    assert(!shard.placeable(gpu));
    assert((shard.offeredFeatures("x") == std::vector<std::string>{"kvm"}));
    assert(shard.offeredFeatures("y").empty());
    shard.dispatch(out);
    // vm on kvm, a on plain; vm2 waits behind a despite higher priority; gpu unplaceable
    assert(out.size() == 2);
    assert(shard.entry(out[0].drv)->drvPath == "vm.drv" && out[0].worker == kvm);
    assert(shard.entry(out[1].drv)->drvPath == "a.drv" && out[1].worker == plain);
    assert(shard.queued() == 2);
    shard.done(kvm, "vm.drv", {});
    shard.dispatch(out);
    assert(out.size() == 3 && out[2].worker == kvm);
    hello(shard, "gpu", 1, {}, {"cuda", "kvm"});
    assert(shard.placeable(gpu));
    shard.dispatch(out);
    assert(out.size() == 4 && shard.entry(out[3].drv)->drvPath == "gpu.drv");
}

// Stats of the testSystems fixture after its first dispatch.
void checkStats(Core & shard)
{
    auto sts = shard.stats();
    auto const stat = [&](const char * sys, const char * feats = "") -> Core::Stats { return sts[{.system = sys, .features = feats}]; };
    assert(stat("aarch64").slots == 2 && stat("aarch64").free == 1 && stat("aarch64").running == 1);
    assert(stat("x86_64").slots == 1 && stat("x86_64").free == 0 && stat("i686").slots == 0);
    assert(stat("x86_64").queued + stat("i686").queued == 1 && stat("x86_64").running + stat("i686").running == 1);
    assert(stat("x86_64").unplaceable + stat("i686").unplaceable == 0);
    assert(stat("riscv").queued == 1 && stat("riscv").unplaceable == 1);
    // Required features are their own series.
    shard.want(1, {.drvPath = "e.drv", .inputs = {}, .system = "aarch64", .features = {"kvm", "big"}, .cpHintMs = 0}, 0);
    sts = shard.stats();
    assert(stat("aarch64", "big,kvm").queued == 1 && stat("aarch64", "big,kvm").unplaceable == 1);
    assert(stat("aarch64").queued == 0);
    shard.cancel(1, "e.drv", 0);
}

// Wants for systems no worker offers are kept, but only for a few systems,
// and a system is forgotten once nothing wants it.
void testUnofferedSystems()
{
    Core shard;
    for (size_t idx = 0; idx < Core::maxUnofferedSystems; idx++) {
        auto const name = "sys" + std::to_string(idx);
        assert(shard.admitsSystem(name));
        shard.want(1, {.drvPath = name + ".drv", .inputs = {}, .system = name, .features = {}, .cpHintMs = 0}, 0);
    }
    assert(!shard.admitsSystem("one-too-many"));
    assert(shard.admitsSystem("sys0"));
    shard.cancel(1, "sys0.drv", 0);
    assert(shard.admitsSystem("one-too-many"));
    assert(!shard.stats().contains({.system = "sys0", .features = ""}));
}

// A worker reporting a drv that was still queued takes it, and its
// followers must be told.
void testHelloAdoptsQueued()
{
    Core shard;
    std::vector<Core::Superseded> sup;
    shard.want(1, {.drvPath = "q.drv", .inputs = {}, .system = "x86_64", .features = {}, .cpHintMs = 0}, 0);
    auto const wkr = shard.hello(
        {.addr = "w", .systems = {"x86_64"}, .features = {}, .maxJobs = 1, .running = {{"q.drv", 7}}}, sup);
    assert(sup.size() == 1 && sup.at(0).loser == noWorker);
    assert(shard.entry(sup.at(0).drv)->worker == wkr && shard.entry(sup.at(0).drv)->assignId == 7);
}

// Followers of a queued entry hear when a capable worker comes or goes.
void testPlaceabilityFlips()
{
    Core shard;
    std::vector<Core::Superseded> sup;
    std::vector<DrvId> changed;
    auto const res = shard.want(1, {.drvPath = "k.drv", .inputs = {}, .system = "x86_64", .features = {"kvm"}, .cpHintMs = 0}, 0);
    shard.markUnplaceable(res.drv, true);
    shard.hello({.addr = "plain", .systems = {"x86_64"}, .features = {}, .maxJobs = 1, .running = {}}, sup);
    shard.placeabilityChanges(changed);
    assert(changed.empty());
    auto const kvm = shard.hello({.addr = "kvm", .systems = {"x86_64"}, .features = {"kvm"}, .maxJobs = 1, .running = {}}, sup);
    shard.placeabilityChanges(changed);
    assert(changed.size() == 1 && !shard.entry(res.drv)->unplaceable);
    changed.clear();
    shard.workerGone(kvm);
    shard.placeabilityChanges(changed);
    assert(changed.size() == 1 && shard.entry(res.drv)->unplaceable);
}

// A given-back drv goes to another worker, or back to the same one if
// nobody else can run it.
// A worker gives a drv back, then the next one it went to gives it back
// too: a third worker that was never tried gets it, not the first again,
// though that one has more slots free.
// Two workers, w0 with two slots and w1 with one, each giving the drv
// back as soon as it gets it, five times: once both have, the one that
// gave it back last is passed over, so they take turns rather than w0
// getting it every time.
void testRepeatGiveBacksTakeTurns()
{
    Core shard;
    std::vector<Assign> out;
    auto const w0 = hello(shard, "w0", 2);
    auto const w1 = hello(shard, "w1", 1);
    shard.want(1, {.drvPath = "a.drv", .inputs = {}, .system = "x", .features = {}, .cpHintMs = 0}, 0);
    shard.dispatch(out);
    std::vector<WorkerId> placed;
    for (int round = 0; round < 5; round++) {
        assert(out.size() == placed.size() + 1);
        placed.push_back(out.back().worker);
        shard.giveBack(placed.back(), "a.drv");
        shard.dispatch(out);
    }
    placed.push_back(out.back().worker);
    assert((placed == std::vector<WorkerId>{w0, w1, w0, w1, w0, w1}));
}

void testGiveBackPassesOverEveryWorkerThatGaveItBack()
{
    Core shard;
    std::vector<Assign> out;
    auto const w0 = hello(shard, "w0", 2);
    auto const w1 = hello(shard, "w1", 1);
    auto const w2 = hello(shard, "w2", 1);
    shard.want(1, {.drvPath = "a.drv", .inputs = {}, .system = "x", .features = {}, .cpHintMs = 0}, 0);
    shard.dispatch(out);
    assert(out.size() == 1 && out[0].worker == w0);
    shard.giveBack(w0, "a.drv");
    shard.dispatch(out);
    assert(out.size() == 2 && (out[1].worker == w1 || out[1].worker == w2));
    auto const second = out[1].worker;
    shard.giveBack(second, "a.drv");
    shard.dispatch(out);
    assert(out.size() == 3 && out[2].worker != w0 && out[2].worker != second);
}

void testGiveBack()
{
    Core shard;
    std::vector<Core::Superseded> sup;
    std::vector<Assign> out;
    auto const first = shard.hello({.addr = "a", .systems = {"x86_64"}, .features = {}, .maxJobs = 1, .running = {}}, sup);
    auto const res = shard.want(1, {.drvPath = "g.drv", .inputs = {}, .system = "x86_64", .features = {}, .cpHintMs = 0}, 0);
    shard.dispatch(out);
    assert(out.size() == 1 && out.at(0).worker == first);
    out.clear();
    shard.giveBack(first, "g.drv");
    shard.dispatch(out);
    assert(out.size() == 1 && out.at(0).worker == first); // sole worker
    out.clear();
    auto const second = shard.hello({.addr = "b", .systems = {"x86_64"}, .features = {}, .maxJobs = 1, .running = {}}, sup);
    shard.giveBack(first, "g.drv");
    shard.dispatch(out);
    assert(out.size() == 1 && out.at(0).worker == second);
    out.clear();
    shard.want(2, {.drvPath = "h.drv", .inputs = {}, .system = "x86_64", .features = {}, .cpHintMs = 0}, 0);
    shard.dispatch(out);
    assert(out.size() == 1 && out.at(0).worker == first);
    out.clear();
    // Another worker can run it, just not now: it waits rather than going back.
    shard.giveBack(second, "g.drv");
    shard.dispatch(out);
    assert(out.empty() && shard.entry(res.drv)->worker == noWorker);
}

// A worker serving two systems gives its slot to the more urgent drv.
void testCrossSystemPriority()
{
    for (auto const * urgent : {"x86_64", "i686"}) {
        Core shard;
        std::vector<Core::Superseded> sup;
        std::vector<Assign> out;
        shard.hello({.addr = "w", .systems = {"x86_64", "i686"}, .features = {}, .maxJobs = 1, .running = {}}, sup);
        for (auto const * system : {"x86_64", "i686"}) {
            auto const hint = std::string_view(system) == urgent ? 1000.0 : 0.0;
            shard.want(1, {.drvPath = std::string(system) + ".drv", .inputs = {}, .system = system, .features = {}, .cpHintMs = hint}, 0);
        }
        shard.dispatch(out);
        assert(out.size() == 1 && shard.entry(out.at(0).drv)->system == urgent);
    }
}

void testSystems()
{
    Core shard;
    std::vector<Assign> out;
    std::vector<Core::Superseded> sup;
    auto const arm = shard.hello({.addr = "arm", .systems = {"aarch64"}, .features = {}, .maxJobs = 2, .running = {}}, sup);
    auto const both = shard.hello({.addr = "both", .systems = {"x86_64", "i686"}, .features = {}, .maxJobs = 1, .running = {}}, sup);
    shard.want(1, {.drvPath = "a.drv", .inputs = {}, .system = "i686", .features = {}, .cpHintMs = 0}, 0);
    shard.want(1, {.drvPath = "b.drv", .inputs = {}, .system = "aarch64", .features = {}, .cpHintMs = 0}, 0);
    shard.want(1, {.drvPath = "c.drv", .inputs = {}, .system = "x86_64", .features = {}, .cpHintMs = 0}, 0);
    shard.want(1, {.drvPath = "d.drv", .inputs = {}, .system = "riscv", .features = {}, .cpHintMs = 0}, 0);
    shard.dispatch(out);
    // both has one slot: a or c, not both. b goes to arm. d has nobody.
    assert(out.size() == 2);
    int onBoth = 0;
    for (auto const & asg : out) {
        const auto & ent = shard.entry(asg.drv);
        if (ent->drvPath == "b.drv") {
            assert(asg.worker == arm);
        } else {
            assert(asg.worker == both);
            onBoth++;
        }
    }
    assert(onBoth == 1);
    assert(!shard.placeable(shard.drvId("d.drv")));
    assert(shard.queued() == 2);
    checkStats(shard);
    // Slot on `both` frees: the other x86-ish drv goes there.
    shard.done(both, shard.entry(out[0].worker == both ? out[0].drv : out[1].drv)->drvPath, {});
    out.clear();
    shard.dispatch(out);
    assert(out.size() == 1 && out[0].worker == both);
}

void testStaleDoneAndUnknown()
{
    Core shard;
    auto const w0 = hello(shard, "w0", 1);
    shard.done(w0, "never.drv", {});
    shard.done(w0, "never.drv", {});
    shard.workerGone(w0);
    shard.workerGone(w0);
    hello(shard, "w0", 1);
    hello(shard, "w0", 1); // duplicate hello
    assert(shard.workersUp() == 1);
    bool threw = false;
    try {
        hello(shard, "", 1);
    } catch (nix::Error &) {
        threw = true;
    }
    assert(threw);
}

// A restarted scheduler knows a running build from its worker's Hello alone:
// nobody follows it, yet it holds its slot until its Done. The stats count it
// as running, under its worker's system. A build whose last client cancelled
// it, revoked and not yet Done, holds its slot the same way.
void testReannouncedBuildRuns()
{
    Core shard;
    auto const w0 = hello(shard, "w0", 1, {"orphan.drv"});
    auto sts = shard.stats();
    assert(sts[{.system = "x", .features = ""}].slots == 1 && sts[{.system = "x", .features = ""}].free == 0);
    assert(sts[{.system = "x", .features = ""}].running == 1 && sts[{.system = "x", .features = ""}].queued == 0);
    shard.done(w0, "orphan.drv", {});
    sts = shard.stats();
    assert(sts[{.system = "x", .features = ""}].free == 1 && sts[{.system = "x", .features = ""}].running == 0);
    std::vector<Assign> out;
    shard.want(1, {.drvPath = "a.drv", .inputs = {}, .system = "x", .features = {}, .cpHintMs = 0}, 0);
    shard.dispatch(out);
    assert(out.size() == 1 && shard.cancel(1, "a.drv", 1000) == w0);
    sts = shard.stats();
    assert(sts[{.system = "x", .features = ""}].free == 0 && sts[{.system = "x", .features = ""}].running == 1);
}

// A linger without a lease id, as clients before Want.lease_id send it.
auto lingerWant(std::string_view drv, double lingerMs) -> Core::WantInfo
{
    return {.drvPath = drv, .inputs = {}, .system = "x", .features = {}, .cpHintMs = 0, .lingerMs = lingerMs};
}

auto leaseWant(std::string_view drv, double lingerMs, std::string_view lease) -> Core::WantInfo
{
    return {
        .drvPath = drv,
        .inputs = {},
        .system = "x",
        .features = {},
        .cpHintMs = 0,
        .lingerMs = lingerMs,
        .leaseId = lease};
}

struct Gone
{
    std::vector<std::pair<DrvId, WorkerId>> revokes;
    std::vector<DrvId> kept;
};

auto gone(Core & shard, ClientId client, double nowMs) -> Gone
{
    Gone left;
    shard.clientGone(client, nowMs, left.revokes, left.kept);
    return left;
}

// A chain of time-bounded clients asks that its build be kept 600 s once
// it leaves (Want.linger_s). Its first link goes while another drv waits for
// the slot: the build stays assigned, holding the slot, and the next link,
// 55 s on, follows the same assignment. Nothing is revoked while a follower
// is left. The last link goes and nobody comes back: revoked when its
// linger is over, not before, and the slot goes to the waiting drv.
void testLingerKeepsAChainsBuild()
{
    Core shard;
    std::vector<Assign> out;
    auto const w0 = hello(shard, "w0", 1);
    shard.want(1, lingerWant("chain.drv", 600e3), 0);
    shard.dispatch(out);
    assert(out.size() == 1 && out[0].worker == w0);
    shard.want(9, lingerWant("waiting.drv", 0), 0);
    std::vector<std::pair<DrvId, WorkerId>> revokes;
    std::vector<DrvId> kept;
    shard.clientGone(1, 1000, revokes, kept);
    assert(revokes.empty() && kept.size() == 1 && kept[0] == out[0].drv);
    shard.dispatch(out);
    assert(out.size() == 1); // the slot stays the chain's
    shard.expireLingers(56000, revokes);
    assert(revokes.empty());
    auto res = shard.want(2, lingerWant("chain.drv", 600e3), 56000);
    assert(res.assigned && res.assigned->first == w0 && res.assigned->second == out[0].assignId);
    assert(res.idleMs && *res.idleMs == 55000);
    shard.expireLingers(1e12, revokes);
    assert(revokes.empty()); // a follower again: nothing kept
    kept.clear();
    shard.clientGone(2, 100000, revokes, kept);
    assert(revokes.empty() && kept.size() == 1);
    shard.expireLingers(100000 + 600e3 - 1, revokes);
    assert(revokes.empty());
    shard.expireLingers(100000 + 600e3, revokes);
    assert(revokes.size() == 1 && revokes[0].first == out[0].drv && revokes[0].second == w0);
    shard.expireLingers(1e12, revokes);
    assert(revokes.size() == 1); // once
    // Revoked, it holds the slot until its Done.
    shard.dispatch(out);
    assert(out.size() == 1);
    shard.done(w0, "chain.drv", {});
    assert(!shard.findDrv("chain.drv"));
    shard.dispatch(out);
    assert(out.size() == 2 && shard.entry(out[1].drv)->drvPath == "waiting.drv");
}

// The longest linger any follower asked for counts, even when the last to
// go asked for none; a client that asked for none on its own is revoked as
// it goes (testCancelAndClientGone). A build that finishes while kept is
// forgotten, and its linger with it.
void testLingerLongestAndDone()
{
    Core shard;
    std::vector<Assign> out;
    auto const w0 = hello(shard, "w0", 2);
    shard.want(1, lingerWant("a.drv", 600e3), 0);
    shard.want(2, lingerWant("a.drv", 0), 0);
    shard.want(3, lingerWant("b.drv", 0), 0);
    shard.dispatch(out);
    assert(out.size() == 2);
    std::vector<std::pair<DrvId, WorkerId>> revokes;
    std::vector<DrvId> kept;
    shard.clientGone(1, 0, revokes, kept);
    assert(revokes.empty() && kept.empty()); // client 2 still follows it
    shard.clientGone(2, 1000, revokes, kept);
    assert(revokes.empty() && kept.size() == 1);
    shard.clientGone(3, 1000, revokes, kept);
    assert(revokes.size() == 1 && revokes[0].second == w0 && kept.size() == 1);
    shard.done(w0, "a.drv", {{"/nix/store/a-out", 1}});
    assert(!shard.findDrv("a.drv"));
    revokes.clear();
    shard.expireLingers(1e12, revokes);
    assert(revokes.empty());
}

// A chain's link leaves while its build still waits for the slot (a drv
// queued here, or Tribuchet's queue behind an assigned one). Kept, it holds
// its place but is not placed while nobody is there to take it: the slot
// that frees in the gap goes to the drv queued after it, and the next link,
// back 20 s on, gets the next slot ahead of a drv that came in the gap.
// Without a linger it is forgotten as its client goes
// (testCancelAndClientGone) and comes back behind that drv. A kept queued
// drv nobody comes back for is forgotten when its linger is over.
void testLingerKeepsAQueuedPlace()
{
    Core shard;
    std::vector<Assign> out;
    auto const w0 = hello(shard, "w0", 1);
    shard.want(7, lingerWant("busy.drv", 0), 0);
    shard.dispatch(out);
    assert(out.size() == 1);
    shard.want(1, lingerWant("chain.drv", 600e3), 1000);
    shard.want(8, lingerWant("after.drv", 0), 2000);
    shard.dispatch(out);
    assert(out.size() == 1); // w0 is full
    std::vector<std::pair<DrvId, WorkerId>> revokes;
    std::vector<DrvId> kept;
    shard.clientGone(1, 45000, revokes, kept);
    auto const chain = shard.findDrv("chain.drv");
    assert(chain && revokes.empty() && kept.size() == 1 && kept[0] == *chain);
    shard.done(w0, "busy.drv", {});
    shard.dispatch(out);
    assert(out.size() == 2 && shard.entry(out[1].drv)->drvPath == "after.drv");
    assert(shard.findDrv("chain.drv") == chain && shard.queued() == 1);
    shard.want(9, lingerWant("later.drv", 0), 50000);
    auto const res = shard.want(2, lingerWant("chain.drv", 600e3), 65000);
    assert(!res.assigned && res.idleMs && *res.idleMs == 20000);
    shard.done(w0, "after.drv", {});
    shard.dispatch(out);
    assert(out.size() == 3 && out[2].drv == *chain && out[2].clients == std::vector<ClientId>{2});
    shard.want(3, lingerWant("gone.drv", 1000), 70000);
    kept.clear();
    shard.clientGone(3, 71000, revokes, kept);
    assert(kept.size() == 1 && shard.findDrv("gone.drv"));
    assert(shard.expireLingers(71999, revokes).empty());
    assert(shard.findDrv("gone.drv"));
    // Forgotten, and said so: the dispatcher logs it and exports the stats.
    assert(shard.expireLingers(72000, revokes) == std::vector<std::string>{"gone.drv"});
    assert(revokes.empty() && !shard.findDrv("gone.drv"));
}

// Assigned and kept, a drv whose Expect runs out (Done NO_SHOW: its client
// left before sending the build) or whose worker goes goes back to its place
// in the queue, not away; a worker that comes back running it has it again,
// kept until its linger is over.
void testLingerOutlivesNoShowAndWorkerLoss()
{
    Core shard;
    std::vector<Assign> out;
    auto const w0 = hello(shard, "w0", 1);
    shard.want(1, lingerWant("chain.drv", 600e3), 0);
    shard.dispatch(out);
    assert(out.size() == 1 && out[0].worker == w0);
    std::vector<std::pair<DrvId, WorkerId>> revokes;
    std::vector<DrvId> kept;
    shard.clientGone(1, 1000, revokes, kept);
    assert(revokes.empty() && kept.size() == 1);
    shard.giveBack(w0, "chain.drv");
    auto const chain = shard.findDrv("chain.drv");
    assert(chain && shard.entry(*chain)->worker == noWorker && shard.queued() == 1);
    shard.dispatch(out);
    assert(out.size() == 1); // nobody to take it
    auto const res = shard.want(2, lingerWant("chain.drv", 600e3), 20000);
    assert(res.idleMs && !res.assigned);
    shard.dispatch(out);
    assert(out.size() == 2 && out[1].drv == *chain && out[1].clients == std::vector<ClientId>{2});
    kept.clear();
    shard.clientGone(2, 30000, revokes, kept);
    assert(revokes.empty() && kept.size() == 1);
    shard.workerGone(w0);
    assert(shard.findDrv("chain.drv") == chain && shard.queued() == 1);
    hello(shard, "w0", 1, {"chain.drv"});
    assert(shard.entry(*chain)->worker == w0 && shard.queued() == 0);
    shard.expireLingers(30000 + 600e3 - 1, revokes);
    assert(revokes.empty());
    shard.expireLingers(30000 + 600e3, revokes);
    assert(revokes.size() == 1 && revokes[0].first == *chain && revokes[0].second == w0);
}

// A kept build given back with no client to take it (a NO_SHOW, or a lost
// Done: the worker's next Hello leaves it out) waits in its place, and is
// placed again only for a client of it. With nobody back it is never
// placed, so it meets no further Expect and no further NO_SHOW: once
// requeued, it stays so until its lease ends, and is then forgotten.
void testKeptBuildPlacedOnlyForAClient()
{
    Core shard;
    std::vector<Assign> out;
    auto const w0 = hello(shard, "w0", 1);
    shard.want(1, leaseWant("chain.drv", 600e3, "chain"), 0);
    shard.dispatch(out);
    assert(out.size() == 1 && out[0].worker == w0);
    assert(gone(shard, 1, 1000).kept.size() == 1);
    // Its Done is lost; the worker reconnects and no longer runs it.
    hello(shard, "w0", 1);
    auto const chain = shard.findDrv("chain.drv");
    assert(chain && shard.entry(*chain)->worker == noWorker && shard.entry(*chain)->idleSinceMs);
    std::vector<std::pair<DrvId, WorkerId>> revokes;
    for (int tick = 0; tick < 60; tick++) {
        assert(shard.expireLingers(2000 + (tick * 10000.0), revokes).empty());
        shard.dispatch(out);
        assert(out.size() == 1 && shard.entry(*chain)->worker == noWorker);
    }
    assert(shard.expireLingers(1000 + 600e3, revokes) == std::vector<std::string>{"chain.drv"});
    assert(revokes.empty() && !shard.findDrv("chain.drv"));
}

// A kept build placed again keeps that placement against the earlier
// placement's late give-back, as any build does (a Done ends only the
// placement it names). The first placement's Done is lost (the worker's
// next Hello leaves the drv out) and the build waits, kept; the chain's
// next link is back, the build is placed on w0 again, and that link leaves
// too. The first placement's give-back arrives then and ends nothing: it
// would have put the kept build back in the queue and freed the slot the
// builder still holds it in. The second placement's own give-back puts it
// back, kept.
void testStaleDoneLeavesAKeptPlacement()
{
    Core shard;
    std::vector<Assign> out;
    auto const w0 = hello(shard, "w0", 1);
    shard.want(1, leaseWant("chain.drv", 600e3, "chain"), 0);
    shard.dispatch(out);
    assert(out.size() == 1 && out[0].worker == w0);
    auto const first = out[0].assignId;
    assert(gone(shard, 1, 1000).kept.size() == 1);
    hello(shard, "w0", 1);
    auto const chain = shard.findDrv("chain.drv");
    assert(chain && shard.entry(*chain)->worker == noWorker && shard.queued() == 1);
    shard.want(2, leaseWant("chain.drv", 600e3, "chain"), 20000);
    shard.dispatch(out);
    assert(out.size() == 2 && out[1].drv == *chain && out[1].worker == w0 && out[1].assignId != first);
    auto const second = out[1].assignId;
    assert(gone(shard, 2, 30000).kept.size() == 1);
    assert(!shard.giveBack(w0, "chain.drv", first));
    assert(shard.entry(*chain)->worker == w0 && shard.entry(*chain)->assignId == second && shard.queued() == 0);
    assert(shard.giveBack(w0, "chain.drv", second));
    assert(shard.entry(*chain)->worker == noWorker && shard.entry(*chain)->idleSinceMs && shard.queued() == 1);
}

// Two chains keep x.drv, each by its own lease. One stops and releases its
// lease: the build stays kept for the other, whose next link follows it The second chain's release lets it go.
void testReleaseEndsOnlyItsLease()
{
    Core shard;
    std::vector<Assign> out;
    auto const w0 = hello(shard, "w0", 1);
    shard.want(1, leaseWant("x.drv", 600e3, "chain1"), 0);
    shard.want(2, leaseWant("x.drv", 600e3, "chain2"), 0);
    shard.dispatch(out);
    assert(out.size() == 1);
    assert(gone(shard, 1, 1000).kept.empty()); // chain 2's link still follows
    auto const left = gone(shard, 2, 2000);
    assert(left.revokes.empty() && left.kept.size() == 1);
    std::vector<std::pair<DrvId, WorkerId>> revokes;
    assert(shard.release("chain1", 3000, 3000 + 600e3, revokes) == 1);
    assert(revokes.empty());
    auto const res = shard.want(4, leaseWant("x.drv", 600e3, "chain2"), 30000);
    assert(res.assigned && res.assigned->first == w0 && res.idleMs && *res.idleMs == 28000);
    assert(gone(shard, 4, 40000).kept.size() == 1);
    assert(shard.release("chain2", 41000, 41000 + 600e3, revokes) == 1);
    assert(revokes.size() == 1 && revokes[0].first == out[0].drv && revokes[0].second == w0);
    assert(shard.release("chain2", 42000, 42000 + 600e3, revokes) == 0); // once
}

// A lease id is one chain, so its latest link says how long the lease
// lasts, a shorter linger too; without an id the longest counts
// (testLingerLongestAndDone).
void testLeaseLatestLingerCounts()
{
    Core shard;
    std::vector<Assign> out;
    auto const w0 = hello(shard, "w0", 1);
    shard.want(1, leaseWant("x.drv", 600e3, "chain"), 0);
    shard.dispatch(out);
    assert(gone(shard, 1, 1000).kept.size() == 1);
    shard.want(2, leaseWant("x.drv", 30e3, "chain"), 2000);
    assert(gone(shard, 2, 3000).kept.size() == 1);
    std::vector<std::pair<DrvId, WorkerId>> revokes;
    shard.expireLingers(3000 + 30e3 - 1, revokes);
    assert(revokes.empty());
    shard.expireLingers(3000 + 30e3, revokes);
    assert(revokes.size() == 1 && revokes[0].first == out[0].drv && revokes[0].second == w0);
}

// A client without a lease (an interactive `nix build`: linger 0) asks for a
// build kept for a chain and is interrupted: the build stays kept until the
// chain's lease is over, counted from its own link's departure.
void testLeaselessFollowerKeepsTheLease()
{
    Core shard;
    std::vector<Assign> out;
    auto const w0 = hello(shard, "w0", 1);
    shard.want(1, leaseWant("x.drv", 600e3, "chain"), 0);
    shard.dispatch(out);
    assert(gone(shard, 1, 1000).kept.size() == 1);
    auto const res = shard.want(5, lingerWant("x.drv", 0), 5000);
    assert(res.assigned && res.idleMs && *res.idleMs == 4000);
    auto const left = gone(shard, 5, 8000);
    assert(left.revokes.empty() && left.kept.size() == 1);
    std::vector<std::pair<DrvId, WorkerId>> revokes;
    shard.expireLingers(1000 + 600e3 - 1, revokes);
    assert(revokes.empty());
    shard.expireLingers(1000 + 600e3, revokes);
    assert(revokes.size() == 1 && revokes[0].second == w0);
}

// The release reaches the scheduler before the stopped link has left it
// (its nix-daemon tears the link's hook down after the CLI exits): the
// build is let go as that link leaves, not kept for 600 s. A Want with the released lease, from a link on its way out, keeps
// nothing either.
void testReleaseBeforeTheLinkLeaves()
{
    Core shard;
    std::vector<Assign> out;
    auto const w0 = hello(shard, "w0", 2);
    shard.want(1, leaseWant("x.drv", 600e3, "chain"), 0);
    shard.dispatch(out);
    std::vector<std::pair<DrvId, WorkerId>> revokes;
    assert(shard.release("chain", 1000, 1000 + 600e3, revokes) == 1);
    assert(revokes.empty()); // link 1 still follows it
    auto const left = gone(shard, 1, 1100);
    assert(left.kept.empty() && left.revokes.size() == 1 && left.revokes[0].second == w0);
    shard.want(2, leaseWant("y.drv", 600e3, "chain"), 2000);
    shard.dispatch(out);
    auto const late = gone(shard, 2, 2100);
    assert(late.kept.empty() && late.revokes.size() == 1);
    // Once the lease could no longer be in any Want's linger, it is free.
    shard.expireLingers(1000 + 600e3, revokes);
    shard.want(3, leaseWant("z.drv", 600e3, "chain"), 1000 + 600e3);
    assert(gone(shard, 3, 1001 + 600e3).kept.size() == 1);
}

// A Release that reaches the scheduler before any Want of its lease (the
// chain stopped while its link's build hook was still on its way) is
// remembered all the same: that link's late Want keeps nothing once it
// leaves.
void testReleaseBeforeAnyWant()
{
    Core shard;
    std::vector<Assign> out;
    auto const w0 = hello(shard, "w0", 1);
    std::vector<std::pair<DrvId, WorkerId>> revokes;
    assert(shard.release("late", 0, 600e3, revokes) == 0 && revokes.empty());
    shard.want(1, leaseWant("x.drv", 600e3, "late"), 1000);
    shard.dispatch(out);
    assert(out.size() == 1 && out[0].worker == w0);
    auto const left = gone(shard, 1, 2000);
    assert(left.kept.empty() && left.revokes.size() == 1 && left.revokes[0].second == w0);
}

// A release is a message, not a client of the build: releasing a kept
// queued build forgets it, and never places it.
void testReleaseNeverPlaces()
{
    Core shard;
    std::vector<Assign> out;
    auto const w0 = hello(shard, "w0", 1);
    shard.want(9, lingerWant("busy.drv", 0), 0);
    shard.dispatch(out);
    shard.want(1, leaseWant("q.drv", 600e3, "chain"), 1000);
    shard.dispatch(out);
    assert(out.size() == 1);
    assert(gone(shard, 1, 2000).kept.size() == 1);
    shard.done(w0, "busy.drv", {});
    shard.dispatch(out);
    assert(out.size() == 1); // kept, not placed
    std::vector<std::pair<DrvId, WorkerId>> revokes;
    assert(shard.release("chain", 3000, 3000 + 600e3, revokes) == 1);
    shard.dispatch(out);
    assert(revokes.empty() && out.size() == 1 && !shard.findDrv("q.drv"));
}

// A Cancel ends the canceller's own lease on the drv, not another chain's;
// its sole follower's Cancel revokes it at once, whatever it declared.
void testCancelEndsOnlyItsLease()
{
    Core shard;
    std::vector<Assign> out;
    auto const w0 = hello(shard, "w0", 1);
    shard.want(1, leaseWant("x.drv", 600e3, "chain1"), 0);
    shard.want(2, leaseWant("x.drv", 600e3, "chain2"), 0);
    shard.dispatch(out);
    assert(!shard.cancel(1, "x.drv", 1000));
    assert(gone(shard, 2, 2000).kept.size() == 1);
    std::vector<std::pair<DrvId, WorkerId>> revokes;
    assert(shard.release("chain1", 3000, 3000 + 600e3, revokes) == 0); // the Cancel ended it
    assert(shard.release("chain2", 3000, 3000 + 600e3, revokes) == 1 && revokes.size() == 1);
    shard.done(w0, "x.drv", {});
    shard.want(3, leaseWant("y.drv", 600e3, "chain3"), 4000);
    shard.dispatch(out);
    assert(shard.cancel(3, "y.drv", 5000) == w0);
    revokes.clear();
    shard.expireLingers(1e12, revokes);
    assert(revokes.empty()); // nothing kept
}

// A kept running build holds its worker's slot: the stats count it as
// running, and as lingering. A kept queued build is lingering, not
// queued: nobody wants it placed until its client is back.
void testLingerStats()
{
    Core shard;
    std::vector<Assign> out;
    hello(shard, "w0", 1);
    shard.want(1, lingerWant("a.drv", 600e3), 0);
    shard.dispatch(out);
    shard.want(1, lingerWant("b.drv", 600e3), 0);
    std::vector<std::pair<DrvId, WorkerId>> revokes;
    std::vector<DrvId> kept;
    shard.clientGone(1, 1000, revokes, kept);
    assert(kept.size() == 2);
    auto sts = shard.stats();
    auto const & sst = sts[{.system = "x", .features = ""}];
    assert(sst.slots == 1 && sst.free == 0 && sst.running == 1 && sst.lingering == 2 && sst.queued == 0);
}

// A client whose stream breaks reconnects and Wants its builds again. Until
// departedGraceMs is over, an assigned build its last client left that way
// stays assigned without a lease, and the client back follows the same
// placement; one nobody comes back for is revoked then. A queued one is
// forgotten at once, and a Cancel revokes at once.
void testDepartedGraceKeepsAnAssignedBuild()
{
    Core shard(Config{.departedGraceMs = 10e3});
    std::vector<Assign> out;
    auto const w0 = hello(shard, "w0", 2);
    shard.want(1, lingerWant("a.drv", 0), 0);
    shard.want(1, lingerWant("b.drv", 0), 0);
    shard.dispatch(out);
    assert(out.size() == 2);
    shard.want(1, lingerWant("q.drv", 0), 0);
    auto left = gone(shard, 1, 1000);
    assert(left.revokes.empty() && left.kept.size() == 2 && !shard.findDrv("q.drv"));
    std::vector<std::pair<DrvId, WorkerId>> revokes;
    shard.expireLingers(1000 + 10e3 - 1, revokes);
    assert(revokes.empty());
    auto const res = shard.want(2, lingerWant("a.drv", 0), 5000);
    assert(res.assigned && res.assigned->first == w0 && res.assigned->second == out[0].assignId);
    shard.expireLingers(1000 + 10e3, revokes);
    assert(revokes.size() == 1 && shard.entry(revokes[0].first)->drvPath == "b.drv");
    assert(shard.cancel(2, "a.drv", 12000) == w0);
}

// A Release while a link still follows its build: once that link's stream
// ends, nobody of the chain comes back, so the build gets no reconnect
// grace. Another client's broken stream still gets one.
void testReleasedClientGetsNoGrace()
{
    Core shard(Config{.departedGraceMs = 10e3});
    std::vector<Assign> out;
    auto const w0 = hello(shard, "w0", 2);
    shard.want(1, leaseWant("x.drv", 600e3, "chain"), 0);
    shard.want(2, lingerWant("y.drv", 0), 0);
    shard.dispatch(out);
    assert(out.size() == 2);
    std::vector<std::pair<DrvId, WorkerId>> revokes;
    assert(shard.release("chain", 1000, 1000 + 600e3, revokes) == 1);
    assert(revokes.empty()); // link 1 still follows it
    auto const left = gone(shard, 1, 1100);
    assert(left.kept.empty() && left.revokes.size() == 1 && left.revokes[0].second == w0);
    auto const other = gone(shard, 2, 1100);
    assert(other.revokes.empty() && other.kept.size() == 1);
}

// A Release ends the reconnect grace of a client of its own lease only. A
// build whose last follower had no lease keeps that client's grace when
// another client's lease on it is released.
void testReleaseKeepsAnotherClientsGrace()
{
    Core shard(Config{.departedGraceMs = 10e3});
    std::vector<Assign> out;
    auto const w0 = hello(shard, "w0", 2);
    shard.want(1, leaseWant("x.drv", 600e3, "chain"), 0);
    shard.want(2, lingerWant("x.drv", 0), 0);
    shard.dispatch(out);
    assert(out.size() == 1);
    assert(gone(shard, 1, 1000).kept.empty()); // client 2 still follows
    assert(gone(shard, 2, 1100).kept.size() == 1);
    std::vector<std::pair<DrvId, WorkerId>> revokes;
    assert(shard.release("chain", 1200, 1200 + 600e3, revokes) == 1);
    assert(revokes.empty()); // client 2's grace still runs
    shard.expireLingers(1100 + 10e3 - 1, revokes);
    assert(revokes.empty());
    shard.expireLingers(1100 + 10e3, revokes);
    assert(revokes.size() == 1 && revokes[0].second == w0);
}

} // namespace
// NOLINTEND(*-magic-numbers, *-avoid-unchecked-container-access, bugprone-unchecked-optional-access, readability-identifier-length)

auto main() -> int
try {
    testHeapAgainstMap();
    testFreeIndex();
    testAssignAndDedup();
    testMostFreeAndLocality();
    testPriority();
    testWorkerGoneRequeues();
    testHelloReconciles();
    testHelloSupersedes();
    testCancelAndClientGone();
    testBouncedOff();
    testBouncedOffTwiceTriesAThirdWorker();
    testRepeatBouncesTakeTurns();
    testFaulty();
    testOtherPlacementsDoneTellsWhereOutputsAre();
    testReannouncedBuildRuns();
    testLingerKeepsAChainsBuild();
    testLingerLongestAndDone();
    testLingerKeepsAQueuedPlace();
    testLingerOutlivesNoShowAndWorkerLoss();
    testKeptBuildPlacedOnlyForAClient();
    testStaleDoneLeavesAKeptPlacement();
    testLingerStats();
    testReleaseEndsOnlyItsLease();
    testLeaseLatestLingerCounts();
    testLeaselessFollowerKeepsTheLease();
    testReleaseBeforeTheLinkLeaves();
    testReleaseBeforeAnyWant();
    testReleaseNeverPlaces();
    testCancelEndsOnlyItsLease();
    testDepartedGraceKeepsAnAssignedBuild();
    testReleasedClientGetsNoGrace();
    testReleaseKeepsAnotherClientsGrace();
    testDraining();
    testSetMaxJobs();
    testFeatures();
    testSystems();
    testUnofferedSystems();
    testHelloAdoptsQueued();
    testGiveBack();
    testGiveBackPassesOverEveryWorkerThatGaveItBack();
    testRepeatGiveBacksTakeTurns();
    testCrossSystemPriority();
    testPlaceabilityFlips();
    testStaleDoneAndUnknown();
    return 0;
} catch (std::exception & err) {
    std::cerr << err.what() << "\n";
    return 1;
}
