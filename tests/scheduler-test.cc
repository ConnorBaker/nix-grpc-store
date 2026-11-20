// Unit tests for the placement core (no gRPC).

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <iostream>
#include <map>
#include <optional>
#include <random>
#include <set>
#include <string>
#include <string_view>
#include <tuple>
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
    fi.set(4, 100);
    assert(fi.get(4) == 100 && *fi.emptiest() == 4);
    // Ties: the first in the count's bucket, a leaver's place taken by the last.
    fi.set(0, 100);
    fi.set(1, 100);
    assert(*fi.emptiest() == 4);
    fi.set(4, 99);
    assert(*fi.emptiest() == 1);
}

auto hello(Core & shard, std::string_view addr, uint32_t jobs, const std::vector<std::string_view> & running = {}, std::vector<std::string> features = {}, std::vector<Core::Superseded> * superseded = nullptr) -> WorkerId
{
    std::vector<std::pair<std::string_view, uint64_t>> runs;
    runs.reserve(running.size());
    for (auto const drv : running) {
        runs.emplace_back(drv, 7);
    }
    std::vector<Core::Superseded> ignored;
    return shard.hello({.addr = addr, .systems = {"x"}, .features = std::move(features), .maxJobs = jobs, .running = std::move(runs)}, superseded != nullptr ? *superseded : ignored);
}

// The drv's current assignment on `wid`.
auto lease(const Core & shard, WorkerId wid, std::string_view path) -> Core::Lease
{
    auto const drv = shard.findDrv(path);
    return {.worker = wid, .drvPath = path, .assignId = drv ? shard.entry(*drv)->assignId : 0};
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
    shard.done(lease(shard, w0, "a.drv"), {{"/nix/store/a-out", 100}});
    assert(!shard.entry(out[0].drv));
}

void testMostFreeAndLocality()
{
    Core shard;
    std::vector<Assign> out;
    auto const w0 = hello(shard, "w0", 4);
    auto const w1 = hello(shard, "w1", 4);
    // w1 produced a big input
    shard.done(lease(shard, w1, "x.drv"), {{"/nix/store/big", 1'000'000'000}});
    shard.want(1, {.drvPath = "b.drv", .inputs = {"/nix/store/big"}, .system = "x", .features = {}, .cpHintMs = 0}, 0);
    shard.dispatch(out);
    assert(out.size() == 1 && out[0].worker == w1);
    // without locality: most free (w0 has 4, w1 has 3)
    shard.want(1, {.drvPath = "c.drv", .inputs = {}, .system = "x", .features = {}, .cpHintMs = 0}, 0);
    shard.dispatch(out);
    assert(out.size() == 2 && out[1].worker == w0);
}

// By exact free count, with and without features alike, and past 2^31.
void testMostFreeIsExact()
{
    Core shard;
    std::vector<Assign> out;
    hello(shard, "small", 100, {}, {"kvm"});
    auto const big = hello(shard, "big", 1000, {}, {"kvm"});
    shard.want(1, {.drvPath = "p.drv", .inputs = {}, .system = "x", .features = {}, .cpHintMs = 0}, 0);
    shard.want(1, {.drvPath = "k.drv", .inputs = {}, .system = "x", .features = {"kvm"}, .cpHintMs = 0}, 0);
    shard.dispatch(out);
    assert(out.size() == 2 && out[0].worker == big && out[1].worker == big);
    auto const a = hello(shard, "a", UINT32_MAX);
    hello(shard, "b", UINT32_MAX);
    for (int i = 0; i < 64; i++) {
        shard.want(1, {.drvPath = std::to_string(i) + ".drv", .inputs = {}, .system = "x", .features = {}, .cpHintMs = 0}, 0);
    }
    shard.dispatch(out);
    assert(out.size() == 66 && std::ranges::count(out, a, &Assign::worker) == 32);
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
    shard.done(lease(shard, 0, "fan.drv"), {});
    shard.dispatch(out);
    assert(out.size() == 2 && shard.entry(out[1].drv)->drvPath == "high.drv");
}

using Revokes = std::vector<std::pair<WorkerId, std::string>>;

// A worker whose session broke may still be building. Its drv is not started
// a second time unless it stays away, returns without it, or a client says
// the worker is unreachable.
void testWorkerGoneHolds()
{
    const Core::WantInfo drv{.drvPath = "a.drv", .inputs = {}, .system = "x", .features = {}, .cpHintMs = 0};
    Revokes revokes;
    auto const placedOnW0 = [&](Core & shard, std::vector<Assign> & out) -> WorkerId {
        auto const w0 = hello(shard, "w0", 1);
        shard.want(1, drv, 0);
        shard.want(2, drv, 0);
        shard.dispatch(out);
        hello(shard, "w1", 1);
        shard.workerGone(w0, 0);
        shard.dispatch(out);
        assert(out.size() == 1 && shard.queued() == 0);
        return w0;
    };
    {
        Core shard;
        std::vector<Assign> out;
        placedOnW0(shard, out);
        std::vector<Core::Superseded> sup;
        shard.hello({.addr = "w0", .systems = {"x"}, .features = {}, .maxJobs = 1, .running = {{"a.drv", out[0].assignId}}}, sup);
        assert(!shard.expire(Core::holdMs, revokes) && shard.queued() == 0);
    }
    {
        Core shard;
        std::vector<Assign> out;
        placedOnW0(shard, out);
        hello(shard, "w0", 1);
        assert(shard.queued() == 1);
    }
    {
        Core shard;
        std::vector<Assign> out;
        placedOnW0(shard, out);
        assert(!shard.expire(Core::holdMs - 1, revokes) && shard.queued() == 0);
        assert(shard.expire(Core::holdMs, revokes) && revokes.empty());
        shard.dispatch(out);
        assert(out.size() == 2 && out[1].worker != out[0].worker && out[1].clients.size() == 2);
    }
    {
        Core shard;
        std::vector<Assign> out;
        auto const w0 = placedOnW0(shard, out);
        assert(shard.refuse(2, "a.drv", out[0].assignId) == w0);
        assert(shard.queued() == 1);
    }
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
    shard.done(lease(shard, w0, "a.drv"), {});
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
    // The build is kept for a client that left, counted from its last leave.
    std::vector<std::pair<DrvId, WorkerId>> gone;
    std::vector<DrvId> kept;
    shard.clientGone(1, 0, gone, kept);
    shard.want(1, {.drvPath = "b.drv", .inputs = {}, .system = "x", .features = {}, .cpHintMs = 0}, 0);
    shard.clientGone(1, 30, gone, kept);
    assert(gone.empty() && kept.empty());
    Revokes revokes;
    shard.expire(Core::holdMs + 29, revokes);
    assert(revokes.empty());
    shard.expire(Core::holdMs + 30, revokes);
    assert(revokes.size() == 1 && revokes[0].first == w0);
    assert(!shard.cancel(9, "nope.drv", 0));
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
    assert((shard.stats()[{.system = "x", .features = ""}].free == 0));
    shard.want(1, {.drvPath = "d.drv", .inputs = {}, .system = "x", .features = {}, .cpHintMs = 0}, 0);
    shard.done(lease(shard, w0, "a.drv"), {});
    shard.done(lease(shard, w0, "b.drv"), {});
    shard.dispatch(out);
    assert(out.size() == 3);
    shard.done(lease(shard, w0, "c.drv"), {});
    shard.dispatch(out);
    assert(out.size() == 4 && out[3].worker == w0);
    // As stated, past 63 and past int32.
    shard.setMaxJobs(w0, 0);
    auto const w1 = hello(shard, "w1", 100);
    for (int i = 0; i < 150; i++) {
        shard.want(1, {.drvPath = std::to_string(i) + ".drv", .inputs = {}, .system = "x", .features = {}, .cpHintMs = 0}, 0);
    }
    shard.dispatch(out);
    assert(out.size() == 104);
    shard.setMaxJobs(w1, UINT32_MAX);
    shard.dispatch(out);
    assert((out.size() == 154 && shard.stats()[{.system = "x", .features = ""}].slots == UINT32_MAX));
}

// Worker::farm: a hub forwards every build to a farm, whose machines differ
// in what they build: plain x86_64-linux on one kind, kvm tests on another,
// aarch64-linux on a third. Builds of a class the farm has little room for,
// even the oldest of the same client, hold no more of the hub's slots than
// that room, and plain x86_64-linux ones queued behind them still get slots.
// A class no machine of the farm builds is bounded by the hub's slots alone.
void testFarmClasses()
{
    Core shard;
    std::vector<Assign> out;
    std::vector<Core::Superseded> sup;
    auto const hub = shard.hello(
        {.addr = "hub",
         .systems = {"x86_64-linux", "aarch64-linux"},
         .features = {"big-parallel", "gpu", "kvm", "nixos-test"},
         .maxJobs = 9,
         .farm =
             {{.systems = {{"x86_64-linux", {"big-parallel"}}}, .jobs = 3},
              {.systems = {{"x86_64-linux", {"big-parallel", "kvm", "nixos-test"}}}, .jobs = 2},
              {.systems = {{"aarch64-linux", {"big-parallel"}}}, .jobs = 1}},
         .running = {}},
        sup);
    auto const want = [&](const std::string & drv, const char * system, std::vector<std::string> features, double atMs) -> void {
        shard.want(1, {.drvPath = drv, .inputs = {}, .system = system, .features = std::move(features), .cpHintMs = 0}, atMs);
    };
    for (int i = 1; i <= 3; i++) {
        want("a" + std::to_string(i) + ".drv", "aarch64-linux", {}, 0);
    }
    for (int i = 1; i <= 4; i++) {
        want("k" + std::to_string(i) + ".drv", "x86_64-linux", {"kvm", "nixos-test"}, 100);
    }
    for (int i = 1; i <= 6; i++) {
        want("p" + std::to_string(i) + ".drv", "x86_64-linux", {}, 200);
    }
    want("g1.drv", "x86_64-linux", {"gpu"}, 300);
    shard.dispatch(out);
    auto const placed = [&] -> std::vector<std::string> {
        std::vector<std::string> names;
        names.reserve(out.size());
        for (const auto & asg : out) {
            names.push_back(shard.entry(asg.drv)->drvPath);
        }
        std::ranges::sort(names);
        return names;
    };
    // aarch64-linux 1, kvm 2, plain 3 (the kvm machine's 2 slots are the kvm
    // tests'), and the gpu one, which no machine builds.
    assert((placed() == std::vector<std::string>{"a1.drv", "g1.drv", "k1.drv", "k2.drv", "p1.drv", "p2.drv", "p3.drv"}));
    assert(std::ranges::all_of(out, [&](const Assign & asg) -> bool { return asg.worker == hub; }));
    // A kvm test ends: its slot goes to the next kvm test, not to the older
    // aarch64-linux build, whose machine is still full.
    shard.done(lease(shard, hub, "k1.drv"), {});
    shard.dispatch(out);
    assert(out.size() == 8 && shard.entry(out.back().drv)->drvPath == "k3.drv");
    // Without a farm, the oldest wait goes first, whatever its class.
    shard.setFarm(hub, {});
    shard.done(lease(shard, hub, "p1.drv"), {});
    shard.dispatch(out);
    assert(out.size() > 8 && shard.entry(out.at(8).drv)->drvPath == "a2.drv");
}

auto farmHub(Core & shard, std::vector<FarmMachine> farm, uint32_t maxJobs = 10) -> WorkerId
{
    std::vector<Core::Superseded> sup;
    return shard.hello(
        {.addr = "hub", .systems = {"x", "y"}, .features = {"big-parallel", "kvm", "nixos-test"}, .maxJobs = maxJobs, .farm = std::move(farm), .running = {}},
        sup);
}

auto wantOn(Core & shard, const std::string & drv, const char * system, std::vector<std::string> features, double atMs) -> void
{
    shard.want(1, {.drvPath = drv, .inputs = {}, .system = system, .features = std::move(features), .cpHintMs = 0}, atMs);
}

// Classes whose machines overlap draw from the same slots: plain,
// big-parallel and nixos-test builds of one machine of 2 slots take 2 in
// all, not 2 each.
void testFarmClassesShareMachines()
{
    Core shard;
    std::vector<Assign> out;
    farmHub(shard, {{.systems = {{"x", {"big-parallel", "nixos-test"}}}, .jobs = 2}});
    for (int i = 0; i < 2; i++) {
        wantOn(shard, "p" + std::to_string(i) + ".drv", "x", {}, i);
        wantOn(shard, "b" + std::to_string(i) + ".drv", "x", {"big-parallel"}, i);
        wantOn(shard, "n" + std::to_string(i) + ".drv", "x", {"nixos-test"}, i);
    }
    shard.dispatch(out);
    assert(out.size() == 2);
}

// A build already out moves to another machine that builds it, so that a
// class only one machine builds gets that machine's slot.
void testFarmMovesBuildsAlong()
{
    Core shard;
    std::vector<Assign> out;
    auto const hub = farmHub(shard, {{.systems = {{"x", {"kvm"}}}, .jobs = 1}, {.systems = {{"x", {}}}, .jobs = 1}});
    wantOn(shard, "p1.drv", "x", {}, 0); // to the kvm machine, listed first
    shard.dispatch(out);
    wantOn(shard, "k1.drv", "x", {"kvm"}, 1);
    wantOn(shard, "p2.drv", "x", {}, 2);
    shard.dispatch(out);
    assert(out.size() == 2 && shard.entry(out.back().drv)->drvPath == "k1.drv");
    // Both machines' slots are out: p2 waits. One ends: p2 takes its slot.
    shard.done(lease(shard, hub, "p1.drv"), {});
    shard.dispatch(out);
    assert(out.size() == 3 && shard.entry(out.back().drv)->drvPath == "p2.drv");
}

// Builds out beyond the farm's slots (it shrank under them) take the first
// slot that frees, even one another class's build left: nothing new fits
// until they are all on slots.
void testFarmShrinksUnderItsBuilds()
{
    Core shard;
    std::vector<Assign> out;
    auto const hub = farmHub(shard, {{.systems = {{"x", {}}, {"y", {}}}, .jobs = 2}});
    wantOn(shard, "x1.drv", "x", {}, 0);
    wantOn(shard, "y1.drv", "y", {}, 1);
    shard.dispatch(out);
    assert(out.size() == 2);
    shard.setFarm(hub, {{.systems = {{"x", {}}, {"y", {}}}, .jobs = 1}});
    shard.done(lease(shard, hub, "x1.drv"), {});
    wantOn(shard, "x2.drv", "x", {}, 2);
    shard.dispatch(out);
    assert(out.size() == 2 && shard.queued() == 1);
}

// The same with one class: its build that ends frees no slot while another
// of it is out beyond the farm's.
void testFarmShrinksUnderOneClass()
{
    Core shard;
    std::vector<Assign> out;
    auto const hub = farmHub(shard, {{.systems = {{"x", {}}}, .jobs = 2}});
    wantOn(shard, "x1.drv", "x", {}, 0);
    wantOn(shard, "x2.drv", "x", {}, 1);
    shard.dispatch(out);
    assert(out.size() == 2);
    shard.setFarm(hub, {{.systems = {{"x", {}}}, .jobs = 1}});
    shard.done(lease(shard, hub, "x1.drv"), {});
    wantOn(shard, "x3.drv", "x", {}, 2);
    shard.dispatch(out);
    assert(out.size() == 2 && shard.queued() == 1);
}

// A machine of several systems has one set of slots for all of them.
void testFarmMachineOfSeveralSystems()
{
    Core shard;
    std::vector<Assign> out;
    farmHub(shard, {{.systems = {{"x", {}}, {"y", {}}}, .jobs = 1}});
    wantOn(shard, "x.drv", "x", {}, 0);
    wantOn(shard, "y.drv", "y", {}, 1);
    shard.dispatch(out);
    assert(out.size() == 1);
}

// A machine the farm lists for a class but with no room (jobs 0: away, or
// its build slots all down) is not "no machine builds it": its builds wait
// for it, as the farm keeps them waiting, and others go ahead.
void testFarmMachineWithoutRoom()
{
    Core shard;
    std::vector<Assign> out;
    farmHub(shard, {{.systems = {{"x", {}}}, .jobs = 4}, {.systems = {{"y", {}}}, .jobs = 0}}, 4);
    for (int i = 1; i <= 3; i++) {
        wantOn(shard, "a" + std::to_string(i) + ".drv", "y", {}, 0);
    }
    wantOn(shard, "p.drv", "x", {}, 100);
    shard.dispatch(out);
    assert(out.size() == 1 && shard.entry(out.front().drv)->drvPath == "p.drv");
}

// nix-daemon runs builtin:fetchurl itself: it takes none of the farm's slots.
void testFarmLeavesBuiltinsOut()
{
    Core shard;
    std::vector<Assign> out;
    farmHub(shard, {{.systems = {{"x", {}}}, .jobs = 1}});
    shard.want(1, {.drvPath = "fetch.drv", .inputs = {}, .system = "x", .features = {}, .cpHintMs = 0, .builtin = true}, 0);
    wantOn(shard, "p.drv", "x", {}, 1);
    shard.dispatch(out);
    assert(out.size() == 2);
}

// A drv a Hello reports before anyone says its system counts on the farm
// once a Want says it.
void testFarmOfAReportedDrv()
{
    Core shard;
    std::vector<Assign> out;
    std::vector<Core::Superseded> sup;
    shard.hello(
        {.addr = "hub",
         .systems = {"x86_64-linux", "aarch64-linux"},
         .features = {},
         .maxJobs = 4,
         .farm = {{.systems = {{"aarch64-linux", {}}}, .jobs = 1}},
         .running = {{"r.drv", 7}}},
        sup);
    wantOn(shard, "r.drv", "aarch64-linux", {}, 0);
    wantOn(shard, "a.drv", "aarch64-linux", {}, 0);
    shard.dispatch(out);
    assert(out.empty() && shard.queued() == 1);
}

// A builtin first known from a Hello is a class of its own once its Want
// says so: lost and queued again behind a plain build the full farm holds
// back, it is still placed.
void testFarmOfAReportedBuiltin()
{
    Core shard;
    std::vector<Assign> out;
    std::vector<Core::Superseded> sup;
    auto const farm = std::vector<FarmMachine>{{.systems = {{"x", {}}}, .jobs = 1}};
    shard.hello({.addr = "hub", .systems = {"x"}, .features = {}, .maxJobs = 4, .farm = farm, .running = {{"f.drv", 7}}},
                sup);
    shard.want(1, {.drvPath = "f.drv", .inputs = {}, .system = "x", .features = {}, .cpHintMs = 0, .builtin = true}, 0);
    wantOn(shard, "p1.drv", "x", {}, 0);
    shard.dispatch(out);
    assert(out.size() == 1 && shard.entry(out[0].drv)->drvPath == "p1.drv");
    // Ahead of f.drv, by its critical path.
    shard.want(1, {.drvPath = "p2.drv", .inputs = {}, .system = "x", .features = {}, .cpHintMs = 1e9}, 0);
    shard.hello(
        {.addr = "hub", .systems = {"x"}, .features = {}, .maxJobs = 4, .farm = farm,
         .running = {{"p1.drv", out[0].assignId}}},
        sup);
    out.clear();
    shard.dispatch(out);
    assert(out.size() == 1 && shard.entry(out[0].drv)->drvPath == "f.drv");
}

// The worker with the inputs is passed over while its farm has no room for
// the drv, however many slots it has itself.
void testFarmBeatsLocality()
{
    Core shard;
    std::vector<Assign> out;
    std::vector<Core::Superseded> sup;
    auto const w0 = shard.hello(
        {.addr = "w0", .systems = {"x", "y"}, .features = {}, .maxJobs = 10,
         .farm = {{.systems = {{"y", {}}}, .jobs = 1}}, .running = {}},
        sup);
    wantOn(shard, "b.drv", "y", {}, 0);
    shard.dispatch(out);
    shard.done(lease(shard, w0, "b.drv"), {{"b-out", 1'000'000'000'000}});
    wantOn(shard, "c.drv", "y", {}, 0);
    shard.dispatch(out);
    assert(out.size() == 2 && out[1].worker == w0);
    auto const w1 = shard.hello({.addr = "w1", .systems = {"x", "y"}, .features = {}, .maxJobs = 2, .running = {}}, sup);
    shard.want(1, {.drvPath = "d.drv", .inputs = {"b-out"}, .system = "y", .features = {}, .cpHintMs = 0}, 0);
    shard.dispatch(out);
    assert(out.size() == 3 && out[2].worker == w1);
}

// A drv given back is not placed on another worker whose farm has no room
// for it, however many slots that worker has itself (Core::sparing).
void testFarmOnAGiveBack()
{
    Core shard;
    std::vector<Assign> out;
    std::vector<Core::Superseded> sup;
    auto const giver = shard.hello({.addr = "giver", .systems = {"y"}, .features = {}, .maxJobs = 5, .running = {}}, sup);
    wantOn(shard, "d1.drv", "y", {}, 0);
    shard.dispatch(out);
    assert(out.size() == 1 && out[0].worker == giver);
    auto const full = shard.hello(
        {.addr = "full", .systems = {"y"}, .features = {}, .maxJobs = 10,
         .farm = {{.systems = {{"y", {}}}, .jobs = 1}}, .running = {}},
        sup);
    wantOn(shard, "d2.drv", "y", {}, 1);
    shard.setDraining(giver, true);
    shard.dispatch(out);
    assert(out.size() == 2 && out[1].worker == full);
    shard.setDraining(giver, false);
    shard.giveBack(lease(shard, giver, "d1.drv"), GaveBack::drv);
    shard.dispatch(out);
    // Not on `full`, whose farm has no room; and as another worker offers
    // it, not back on the giver either: it waits (sparing).
    assert(out.size() == 2 && shard.queued() == 1);
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
    shard.done(lease(shard, kvm, "vm.drv"), {});
    shard.dispatch(out);
    assert(out.size() == 3 && out[2].worker == kvm);
    hello(shard, "gpu", 1, {}, {"cuda", "kvm"});
    assert(shard.placeable(gpu));
    shard.dispatch(out);
    assert(out.size() == 4 && shard.entry(out[3].drv)->drvPath == "gpu.drv");
}

// A pass stops asking for a feature set once it found no worker for it.
// That must not hide work behind it, nor a sibling of an entry that failed
// only because it avoids the one free worker.
void testStuckFeatureSet()
{
    Core shard;
    std::vector<Assign> out;
    auto const plain = hello(shard, "plain", 1);
    auto const kvmA = hello(shard, "kvmA", 1, {}, {"kvm"});
    auto const kvmB = hello(shard, "kvmB", 1, {}, {"kvm"});
    auto const wantKvm = [&](std::string_view drv, double hint) -> void {
        shard.want(1, {.drvPath = drv, .inputs = {}, .system = "x", .features = {"kvm"}, .cpHintMs = hint}, 0);
    };
    wantKvm("vm1.drv", 9e9);
    wantKvm("vm2.drv", 8e9);
    shard.dispatch(out);
    assert(out.size() == 2);
    auto const holder = [&](std::string_view drv) -> WorkerId { return shard.entry(*shard.findDrv(drv))->worker; };

    // Both kvm workers busy, plain free: the plain drv behind two stuck ones is placed.
    wantKvm("vm3.drv", 7e9);
    wantKvm("vm4.drv", 6e9);
    shard.want(1, {.drvPath = "a.drv", .inputs = {}, .system = "x", .features = {}, .cpHintMs = 0}, 0);
    shard.dispatch(out);
    assert(out.size() == 3 && out[2].worker == plain);
    assert(shard.queued() == 2);

    // vm1's worker gives it back, so only the giver is free: vm1 avoids it while the other kvm worker exists, vm3 behind it takes the slot.
    auto const giver = holder("vm1.drv");
    shard.giveBack(lease(shard, giver, "vm1.drv"), GaveBack::drv);
    shard.dispatch(out);
    assert(out.size() == 4);
    assert(shard.entry(out[3].drv)->drvPath == "vm3.drv" && out[3].worker == giver);
    assert(giver == kvmA || giver == kvmB);
    assert(shard.queued() == 2);

    // Order within the set survives the early end of those passes.
    shard.done(lease(shard, holder("vm2.drv"), "vm2.drv"), {});
    shard.dispatch(out);
    assert(out.size() == 5 && shard.entry(out[4].drv)->drvPath == "vm1.drv");
}

// Feature names come from the network: a comma in one must not make its
// set look like another.
void testFeatureSetsWithCommas()
{
    Core shard;
    std::vector<Assign> out;
    hello(shard, "w", 1, {}, {"a", "b"});
    shard.want(1, {.drvPath = "odd.drv", .inputs = {}, .system = "x", .features = {"a,b"}, .cpHintMs = 9e9}, 0);
    shard.want(1, {.drvPath = "ok.drv", .inputs = {}, .system = "x", .features = {"a", "b"}, .cpHintMs = 0}, 0);
    shard.dispatch(out);
    assert(out.size() == 1 && shard.entry(out[0].drv)->drvPath == "ok.drv");
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
    shard.workerGone(kvm, 0);
    shard.placeabilityChanges(changed);
    assert(changed.size() == 1 && shard.entry(res.drv)->unplaceable);
}

// A given-back drv goes to another worker, or back to the same one if
// nobody else can run it.
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
    shard.giveBack(lease(shard, first, "g.drv"), GaveBack::drv);
    shard.dispatch(out);
    assert(out.size() == 1 && out.at(0).worker == first); // sole worker
    out.clear();
    auto const second = shard.hello({.addr = "b", .systems = {"x86_64"}, .features = {}, .maxJobs = 1, .running = {}}, sup);
    shard.giveBack(lease(shard, first, "g.drv"), GaveBack::drv);
    shard.dispatch(out);
    assert(out.size() == 1 && out.at(0).worker == second);
    out.clear();
    shard.want(2, {.drvPath = "h.drv", .inputs = {}, .system = "x86_64", .features = {}, .cpHintMs = 0}, 0);
    shard.dispatch(out);
    assert(out.size() == 1 && out.at(0).worker == first);
    out.clear();
    // Another worker can run it, just not now: it waits rather than going back.
    shard.giveBack(lease(shard, second, "g.drv"), GaveBack::drv);
    shard.dispatch(out);
    assert(out.empty() && shard.entry(res.drv)->worker == noWorker);
}

// Every worker that gave a drv back is passed over, not just the last one.
void testGiveBackRemembersEveryGiver()
{
    Core shard;
    std::vector<Assign> out;
    auto const big = hello(shard, "big", 3);
    auto const mid = hello(shard, "mid", 2);
    auto const small = hello(shard, "small", 1);
    shard.want(1, {.drvPath = "g.drv", .inputs = {}, .system = "x", .features = {}, .cpHintMs = 0}, 0);
    for (auto const giver : {big, mid}) {
        shard.dispatch(out);
        assert(out.size() == 1 && out.at(0).worker == giver);
        out.clear();
        shard.giveBack(lease(shard, giver, "g.drv"), GaveBack::drv);
    }
    shard.dispatch(out);
    assert(out.size() == 1 && out.at(0).worker == small);
}

// w0's last publish failed: it gets builds only while no other worker has a
// slot, its outputs do not draw builds to it, and passing over a giver does
// not pick it either, until it can publish again.
void testFaulty()
{
    Core shard;
    std::vector<Assign> out;
    auto const w0 = hello(shard, "w0", 3);
    auto const w1 = hello(shard, "w1", 2);
    shard.setFaulty(w0, true);
    shard.want(1, {.drvPath = "a.drv", .inputs = {}, .system = "x", .features = {}, .cpHintMs = 0}, 0);
    shard.want(1, {.drvPath = "b.drv", .inputs = {}, .system = "x", .features = {}, .cpHintMs = 0}, 0);
    shard.want(1, {.drvPath = "c.drv", .inputs = {}, .system = "x", .features = {}, .cpHintMs = 0}, 0);
    shard.dispatch(out);
    assert(out.size() == 3 && out.at(0).worker == w1 && out.at(1).worker == w1 && out.at(2).worker == w0); // w1 is full
    shard.done(lease(shard, w0, "c.drv"), {{"/nix/store/c-out", 1'000'000'000}});
    shard.done(lease(shard, w1, "a.drv"), {});
    shard.done(lease(shard, w1, "b.drv"), {});
    out.clear();
    shard.want(1, {.drvPath = "d.drv", .inputs = {"/nix/store/c-out"}, .system = "x", .features = {}, .cpHintMs = 0}, 0);
    shard.dispatch(out);
    assert(out.size() == 1 && out.at(0).worker == w1);
    auto const w2 = hello(shard, "w2", 1);
    shard.giveBack(lease(shard, w1, "d.drv"), GaveBack::drv);
    out.clear();
    shard.dispatch(out);
    assert(out.size() == 1 && out.at(0).worker == w2); // w0 has more slots and the input
    shard.done(lease(shard, w2, "d.drv"), {});
    shard.setFaulty(w0, false);
    out.clear();
    shard.want(1, {.drvPath = "e.drv", .inputs = {"/nix/store/c-out"}, .system = "x", .features = {}, .cpHintMs = 0}, 0);
    shard.dispatch(out);
    assert(out.size() == 1 && out.at(0).worker == w0);
    // A reconnect carries the state.
    std::vector<Core::Superseded> sup;
    shard.hello({.addr = "w0", .systems = {"x"}, .features = {}, .maxJobs = 3, .running = {}, .faulty = true}, sup);
    assert(shard.worker(w0).faulty);
}

// A worker that keeps failing for a reason of its own gets no work for a
// while, longer each time, until it finishes a build.
void testFailingWorkerRests()
{
    constexpr double firstRest = 30e3;
    Core shard;
    std::vector<Assign> out;
    auto const sick = hello(shard, "sick", 1);
    shard.want(1, {.drvPath = "r.drv", .inputs = {}, .system = "x", .features = {}, .cpHintMs = 0}, 0);
    auto const fail = [&](double now) -> bool {
        shard.dispatch(out);
        assert(out.size() == 1);
        out.clear();
        shard.giveBack(lease(shard, sick, "r.drv"), GaveBack::drv);
        return shard.failed(sick, now).has_value();
    };
    assert(!fail(0) && !fail(0) && fail(0));
    assert(!shard.failed(sick, 0)); // one more from before the rest
    shard.dispatch(out);
    assert(out.empty() && shard.placeable(*shard.findDrv("r.drv")));
    assert(!shard.wake(firstRest - 1) && shard.wake(firstRest));

    assert(fail(firstRest));
    assert(!shard.wake(2 * firstRest) && shard.wake(3 * firstRest));

    shard.dispatch(out);
    assert(out.size() == 1);
    out.clear();
    shard.done(lease(shard, sick, "r.drv"), {});
    shard.want(1, {.drvPath = "r.drv", .inputs = {}, .system = "x", .features = {}, .cpHintMs = 0}, 0);
    assert(!fail(0));
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
    shard.done(lease(shard, both, shard.entry(out[0].worker == both ? out[0].drv : out[1].drv)->drvPath), {});
    out.clear();
    shard.dispatch(out);
    assert(out.size() == 1 && out[0].worker == both);
}

// A worker with max_jobs 0 runs nothing and offers no features. A drv given
// back by the only other worker goes back there, and is unplaceable without it.
void testNoSlotsRunsNothing()
{
    Core shard;
    std::vector<Assign> out;
    auto const w0 = hello(shard, "w0", 1, {}, {"kvm"});
    hello(shard, "w1", 0, {}, {"f"});
    assert((shard.offeredFeatures("x") == std::vector<std::string>{"kvm"}));
    auto const res = shard.want(1, {.drvPath = "a.drv", .inputs = {}, .system = "x", .features = {}, .cpHintMs = 0}, 0);
    shard.dispatch(out);
    shard.giveBack(lease(shard, w0, "a.drv"), GaveBack::drv);
    shard.dispatch(out);
    assert(out.size() == 2 && out[1].worker == w0);
    shard.workerGone(w0, 0);
    Revokes revokes;
    shard.expire(Core::holdMs, revokes);
    assert(!shard.placeable(res.drv));
    std::vector<DrvId> changed;
    shard.placeabilityChanges(changed);
    assert(changed.size() == 1 && changed[0] == res.drv && shard.entry(res.drv)->unplaceable);
}

void testStaleDoneAndUnknown()
{
    Core shard;
    auto const w0 = hello(shard, "w0", 1);
    shard.done(lease(shard, w0, "never.drv"), {});
    shard.done(lease(shard, w0, "never.drv"), {});
    shard.workerGone(w0, 0);
    shard.workerGone(w0, 0);
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

// A build holds its worker's slot until its Done, followed or not: one known
// only from the worker's Hello after a scheduler restart, or one whose last
// client cancelled it.
void testRunningFollowsTheWorker()
{
    Core shard;
    auto const w0 = hello(shard, "w0", 1, {"orphan.drv"});
    auto const stat = [&] -> Core::Stats { return shard.stats()[{.system = "x", .features = ""}]; };
    assert(stat().slots == 1 && stat().free == 0 && stat().running == 1);
    shard.done(lease(shard, w0, "orphan.drv"), {});
    assert(stat().free == 1 && stat().running == 0);
    std::vector<Assign> out;
    shard.want(1, {.drvPath = "a.drv", .inputs = {}, .system = "x", .features = {}, .cpHintMs = 0}, 0);
    shard.dispatch(out);
    assert(out.size() == 1 && shard.cancel(1, "a.drv", 1000) == w0);
    assert(stat().free == 0 && stat().running == 1);
}

// A client with a long queue does not keep a later one waiting behind it.
void testLeastServedClientFirst()
{
    Core shard;
    std::vector<Assign> out;
    auto const wid = hello(shard, "w", 2);
    for (auto const * name : {"big0.drv", "big1.drv", "big2.drv", "big3.drv"}) {
        shard.want(1, {.drvPath = name, .inputs = {}, .system = "x", .features = {}, .cpHintMs = 0}, 0);
    }
    shard.dispatch(out);
    assert(out.size() == 2);
    shard.want(2, {.drvPath = "small.drv", .inputs = {}, .system = "x", .features = {}, .cpHintMs = 0}, 1000);
    shard.done(lease(shard, wid, shard.entry(out[0].drv)->drvPath), {});
    shard.dispatch(out);
    assert(out.size() == 3 && shard.entry(out[2].drv)->drvPath == "small.drv");

    // A drv two clients wait for counts for both: clients 1 and 2 have one
    // placed each, so client 3 goes first.
    shard.want(1, {.drvPath = "small.drv", .inputs = {}, .system = "x", .features = {}, .cpHintMs = 0}, 1000);
    shard.want(2, {.drvPath = "more.drv", .inputs = {}, .system = "x", .features = {}, .cpHintMs = 0}, 1000);
    shard.want(3, {.drvPath = "late.drv", .inputs = {}, .system = "x", .features = {}, .cpHintMs = 0}, 2000);
    shard.done(lease(shard, wid, shard.entry(out[1].drv)->drvPath), {});
    shard.dispatch(out);
    assert(out.size() == 4 && shard.entry(out[3].drv)->drvPath == "late.drv");
}

// The queue against a plain model of it, over random operations.
void testWaitQueueMatchesModel()
{
    struct Queued
    {
        double key = 0;
        std::set<ClientId> clients;
    };
    std::mt19937 rng(7); // NOLINT(cert-msc32-c,cert-msc51-cpp,bugprone-random-generator-seed): reproducible
    auto const upTo = [&](uint32_t max) -> uint32_t { return std::uniform_int_distribution<uint32_t>(0, max)(rng); };
    WaitQueue queue;
    std::map<ClientId, uint32_t> served;
    std::map<OwnerId, uint32_t> ownerServed;
    auto const shareOf = [&](ClientId client) -> Share {
        auto const owner = OwnerId{client % 3};
        return {.owner = owner, .ownerServed = ownerServed[owner], .served = served[client]};
    };
    std::map<DrvId, Queued> model;
    for (int step = 0; step < 20000; step++) {
        auto const drv = upTo(40);
        auto const client = ClientId{upTo(5)};
        switch (upTo(5)) {
        case 0:
        case 1: {
            std::set<ClientId> clients{client};
            if (upTo(3) == 0) {
                clients.insert(upTo(5));
            }
            auto const key = static_cast<double>(upTo(8));
            queue.set(drv, key, "", {clients.begin(), clients.end()}, shareOf);
            model[drv] = {.key = key, .clients = clients};
            break;
        }
        case 2:
        case 3:
            queue.erase(drv, "");
            model.erase(drv);
            break;
        case 4:
            served[client] = upTo(3);
            ownerServed[client % 3] = upTo(3);
            queue.serving(client, shareOf(client));
            break;
        default:
            queue.forget(client);
            for (auto & [id, entry] : model) {
                entry.clients.erase(client);
            }
        }
        // A client stands where its first drv does. An owner goes by its
        // clients' smallest key and sends its first client.
        std::map<OwnerId, std::tuple<uint32_t, double, ClientId, DrvId>> firstOf;
        std::map<OwnerId, double> keyOf;
        for (const auto & [id, entry] : model) {
            for (auto const who : entry.clients) {
                auto const rank = std::tuple(served[who], entry.key, who, id);
                auto const [iter, fresh] = firstOf.try_emplace(who % 3, rank);
                iter->second = std::min(iter->second, rank);
                auto const smallest = keyOf.try_emplace(who % 3, entry.key).first;
                smallest->second = std::min(smallest->second, entry.key);
            }
        }
        std::optional<std::tuple<uint32_t, double, OwnerId>> first;
        for (const auto & [owner, key] : keyOf) {
            auto const mine = std::tuple(ownerServed[owner], key, owner);
            if (!first || mine < *first) {
                first = mine;
            }
        }
        assert(queue.size() == model.size());
        assert(queue.top().has_value() == first.has_value());
        if (first) {
            assert(queue.topKey() == WaitQueue::Rank(std::get<0>(*first), std::get<1>(*first)));
            assert(queue.top() == std::get<3>(firstOf.at(std::get<2>(*first))));
        }
    }
}

// Three builds of one owner get together what one build of another gets.
void testOwnersShareTheirPlace()
{
    Core shard;
    std::vector<Assign> out;
    for (ClientId client = 1; client <= 3; client++) {
        shard.clientOwner(client, "ci");
    }
    shard.clientOwner(4, "dev");
    for (ClientId client = 1; client <= 4; client++) {
        for (auto const * name : {"a", "b", "c", "d"}) {
            shard.want(client, {.drvPath = std::to_string(client) + name, .inputs = {}, .system = "x", .features = {}, .cpHintMs = 0}, 0);
        }
    }
    hello(shard, "w", 4);
    shard.dispatch(out);
    assert(out.size() == 4);
    assert(std::ranges::count_if(out, [](const Assign & asg) -> bool { return asg.clients == std::vector<ClientId>{4}; }) == 2);

    // The owner's count ends with its clients.
    std::vector<std::pair<DrvId, WorkerId>> revokes;
    std::vector<DrvId> kept;
    for (ClientId client = 1; client <= 3; client++) {
        shard.clientGone(client, 0, revokes, kept);
    }
    shard.clientOwner(5, "ci");
    shard.want(5, {.drvPath = "5a", .inputs = {}, .system = "x", .features = {}, .cpHintMs = 0}, 0);
    shard.done(lease(shard, 0, "4a"), {});
    out.clear();
    shard.dispatch(out);
    assert(out.size() == 1 && shard.entry(out[0].drv)->drvPath == "5a");
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
    shard.done(lease(shard, w0, "chain.drv"), {});
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
    assert(revokes.empty() && kept.size() == 1); // b is held for client 3's return
    Revokes held;
    shard.expire(1000 + Core::holdMs, held);
    assert(held.size() == 1 && held[0].first == w0 && held[0].second == "b.drv");
    shard.done(lease(shard, w0, "a.drv"), {{"/nix/store/a-out", 1}});
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
    shard.done(lease(shard, w0, "busy.drv"), {});
    shard.dispatch(out);
    assert(out.size() == 2 && shard.entry(out[1].drv)->drvPath == "after.drv");
    assert(shard.findDrv("chain.drv") == chain && shard.queued() == 1);
    shard.want(9, lingerWant("later.drv", 0), 50000);
    auto const res = shard.want(2, lingerWant("chain.drv", 600e3), 65000);
    assert(!res.assigned && res.idleMs && *res.idleMs == 20000);
    shard.done(lease(shard, w0, "after.drv"), {});
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
// left before sending the build) goes back to its place in the queue, not
// away. One whose worker goes is held for it (Core::holdMs), not forgotten;
// a worker that comes back running it has it again, kept until its linger
// is over.
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
    shard.giveBack(lease(shard, w0, "chain.drv"), GaveBack::drv);
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
    shard.workerGone(w0, 30000);
    assert(shard.findDrv("chain.drv") == chain && shard.entry(*chain)->worker == w0 && shard.queued() == 0);
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
    assert(!shard.giveBack({.worker = w0, .drvPath = "chain.drv", .assignId = first}, GaveBack::drv));
    assert(shard.entry(*chain)->worker == w0 && shard.entry(*chain)->assignId == second && shard.queued() == 0);
    assert(shard.giveBack({.worker = w0, .drvPath = "chain.drv", .assignId = second}, GaveBack::drv));
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
    // The latest link's 30 s, not the first's 600 s, but no sooner than
    // the hold for client 2's return.
    static_assert(30e3 < Core::holdMs && Core::holdMs < 600e3);
    shard.expireLingers(3000 + Core::holdMs - 1, revokes);
    assert(revokes.empty());
    shard.expireLingers(3000 + Core::holdMs, revokes);
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
    shard.done(lease(shard, w0, "busy.drv"), {});
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
    shard.done(lease(shard, w0, "x.drv"), {});
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
// its hold (Core::holdMs) is over, an assigned build its last client left that way
// stays assigned without a lease, and the client back follows the same
// placement; one nobody comes back for is revoked then. A queued one is
// forgotten at once, and a Cancel revokes at once.
// A client's return to a build kept for its lease ends the hold for it, not
// the hold for a worker that is down: the drv is queued again when that ends.
void testAReturningClientKeepsTheWorkersHold()
{
    Core shard;
    std::vector<Assign> out;
    auto const w0 = hello(shard, "w0", 1);
    shard.want(1, leaseWant("a.drv", 600e3, "chain"), 0);
    shard.dispatch(out);
    assert(out.size() == 1);
    shard.workerGone(w0, 1000);
    auto left = gone(shard, 1, 2000);
    assert(left.revokes.empty() && left.kept.size() == 1);
    shard.want(2, lingerWant("a.drv", 0), 3000);
    Revokes revokes;
    assert(shard.expire(2000 + Core::holdMs, revokes) && revokes.empty());
    assert(shard.queued() == 1);
}

void testHoldKeepsAnAssignedBuild()
{
    Core shard;
    std::vector<Assign> out;
    auto const w0 = hello(shard, "w0", 2);
    shard.want(1, lingerWant("a.drv", 0), 0);
    shard.want(1, lingerWant("b.drv", 0), 0);
    shard.dispatch(out);
    assert(out.size() == 2);
    shard.want(1, lingerWant("q.drv", 0), 0);
    auto left = gone(shard, 1, 1000);
    // Held, not kept: only a lease keeps a build.
    assert(left.revokes.empty() && left.kept.empty() && !shard.findDrv("q.drv"));
    Revokes revokes;
    shard.expire(1000 + Core::holdMs - 1, revokes);
    assert(revokes.empty());
    auto const res = shard.want(2, lingerWant("a.drv", 0), 5000);
    assert(res.assigned && res.assigned->first == w0 && res.assigned->second == out[0].assignId);
    shard.expire(1000 + Core::holdMs, revokes);
    assert(revokes.size() == 1 && revokes[0].first == w0 && revokes[0].second == "b.drv");
    assert(shard.cancel(2, "a.drv", 1000 + Core::holdMs + 1) == w0);
}

// A Release while a link still follows its build: once that link's stream
// ends, nobody of the chain comes back, so the build gets no reconnect
// grace. Another client's broken stream still gets one.
void testReleasedClientGetsNoGrace()
{
    Core shard;
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
    assert(other.revokes.empty() && other.kept.empty()); // held
}

// A Release ends the reconnect grace of a client of its own lease only. A
// build whose last follower had no lease keeps that client's grace when
// another client's lease on it is released.
void testReleaseKeepsAnotherClientsGrace()
{
    Core shard;
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
    assert(revokes.empty()); // client 2's hold still runs
    Revokes held;
    shard.expire(1100 + Core::holdMs - 1, held);
    assert(held.empty());
    shard.expire(1100 + Core::holdMs, held);
    assert(held.size() == 1 && held[0].first == w0);
}

} // namespace
// NOLINTEND(*-magic-numbers, *-avoid-unchecked-container-access, bugprone-unchecked-optional-access, readability-identifier-length)

auto main() -> int
try {
    testLeastServedClientFirst();
    testWaitQueueMatchesModel();
    testOwnersShareTheirPlace();
    testHeapAgainstMap();
    testFreeIndex();
    testAssignAndDedup();
    testMostFreeAndLocality();
    testPriority();
    testWorkerGoneHolds();
    testHelloReconciles();
    testHelloSupersedes();
    testCancelAndClientGone();
    testRunningFollowsTheWorker();
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
    testHoldKeepsAnAssignedBuild();
    testAReturningClientKeepsTheWorkersHold();
    testReleasedClientGetsNoGrace();
    testReleaseKeepsAnotherClientsGrace();
    testDraining();
    testSetMaxJobs();
    testMostFreeIsExact();
    testFarmClasses();
    testFarmClassesShareMachines();
    testFarmMovesBuildsAlong();
    testFarmMachineOfSeveralSystems();
    testFarmShrinksUnderItsBuilds();
    testFarmShrinksUnderOneClass();
    testFarmMachineWithoutRoom();
    testFarmLeavesBuiltinsOut();
    testFarmOfAReportedDrv();
    testFarmOfAReportedBuiltin();
    testFarmBeatsLocality();
    testFarmOnAGiveBack();
    testFeatures();
    testStuckFeatureSet();
    testFeatureSetsWithCommas();
    testSystems();
    testUnofferedSystems();
    testHelloAdoptsQueued();
    testGiveBack();
    testGiveBackRemembersEveryGiver();
    testFaulty();
    testFailingWorkerRests();
    testCrossSystemPriority();
    testPlaceabilityFlips();
    testNoSlotsRunsNothing();
    testStaleDoneAndUnknown();
    return 0;
} catch (std::exception & err) {
    std::cerr << err.what() << "\n";
    return 1;
}
