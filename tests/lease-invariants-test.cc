// The placement core's lease bookkeeping against itself. Random runs of Want
// (with and without a lease or linger), stream end, Cancel, Release,
// placement, Done and give-back naming any placement, linger and grace
// expiry, and a worker's loss and Hello; after every step the bookkeeping
// must agree with itself, and once every client, lease and worker is gone
// nothing may be left. It catches leaks scheduler-test does not, such as a
// lease index entry left behind as a lease is dropped, or a holder left
// behind by a Release.

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <iostream>
#include <map>
#include <random>
#include <set>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <nix/util/error.hh>

#include "scheduler.hh"

// NOLINTBEGIN(*-magic-numbers, bugprone-unchecked-optional-access, readability-identifier-length): random test vectors, as scheduler-test.
namespace nixgrpc::sched {

// A friend of Core: reads its private lease state.
struct LeaseInvariants // NOLINT(misc-use-internal-linkage): Core names it as its friend
{
    static void check(const Core & core)
    {
        auto const fail = [](const char * what) -> void { throw nix::Error("lease invariant: %s", what); };
        std::map<std::string, std::set<DrvId>> named;
        size_t kept = 0;
        for (size_t idx = 0; idx < core.entries.size(); idx++) {
            auto const drv = static_cast<DrvId>(idx);
            auto const & slot = core.entries.at(idx);
            if (!slot) {
                continue;
            }
            if (slot->idleSinceMs) {
                kept++;
                if (!slot->followers.empty()) {
                    fail("kept with a follower");
                }
                if (!core.lingering.contains({slot->keptUntilMs, drv})) {
                    fail("kept, not in lingering");
                }
                if (slot->leases.empty() && slot->graceUntilMs == 0) {
                    fail("kept without a lease or a reconnect grace");
                }
                if (slot->keptUntilMs < slot->graceUntilMs) {
                    fail("kept for less than its grace");
                }
            }
            for (auto const & [client, lease] : slot->holders) {
                if (std::ranges::find(slot->followers, client) == slot->followers.end()) {
                    fail("a holder is no follower");
                }
                if (std::ranges::find(slot->leases, lease, &Lease::id) == slot->leases.end()) {
                    fail("a holder's lease is gone from the drv");
                }
                if (std::ranges::count_if(slot->holders, [&](auto const & other) -> bool { return other.first == client; }) != 1) {
                    fail("a client holds twice");
                }
            }
            for (auto const & lse : slot->leases) {
                if (std::ranges::count(slot->leases, lse.id, &Lease::id) != 1) {
                    fail("a lease twice on one drv");
                }
                auto const holding =
                    std::ranges::count_if(slot->holders, [&](auto const & holder) -> bool { return holder.second == lse.id; });
                if (std::cmp_not_equal(holding, lse.links)) {
                    fail("links disagree with holders");
                }
                if (!lse.id.empty()) {
                    named[lse.id].insert(drv);
                }
            }
        }
        for (auto const & [id, drvs] : core.leaseDrvs) {
            std::set<DrvId> const seen(drvs.begin(), drvs.end());
            if (seen.size() != drvs.size()) {
                fail("a drv twice in leaseDrvs");
            }
            auto const found = named.find(id);
            if (found == named.end() || found->second != seen) {
                fail("leaseDrvs disagrees with the entries");
            }
        }
        if (named.size() != core.leaseDrvs.size()) {
            fail("a named lease is missing from leaseDrvs");
        }
        // A stale key (one a drv was kept under before) may stay until it is due.
        size_t current = 0;
        for (auto const & [until, drv] : core.lingering) {
            if (drv < core.entries.size() && core.entries.at(drv) && core.entries.at(drv)->idleSinceMs
                && core.entries.at(drv)->keptUntilMs == until) {
                current++;
            }
        }
        if (current != kept) {
            fail("lingering disagrees with the kept entries");
        }
        if (core.released.size() != core.releasedBy.size() || core.released.size() > Core::maxReleased) {
            fail("the released index");
        }
    }

    // What a Done naming another placement must leave alone.
    // NOLINTNEXTLINE(bugprone-easily-swappable-parameters): a drv and its worker, named where it is called
    [[nodiscard]] static auto snapshot(const Core & core, DrvId drv, WorkerId wid) -> std::string
    {
        auto const & slot = core.entries.at(drv);
        if (!slot) {
            return "retired";
        }
        auto const & wkr = core.workers.at(wid);
        std::string avoid;
        for (auto const passed : slot->avoid) {
            avoid += " " + std::to_string(passed);
        }
        return "worker " + std::to_string(slot->worker) + " assign " + std::to_string(slot->assignId) + " kept "
               + std::to_string(static_cast<int>(slot->idleSinceMs.has_value())) + " until "
               + std::to_string(slot->keptUntilMs) + " followers " + std::to_string(slot->followers.size()) + " leases "
               + std::to_string(slot->leases.size()) + " holders " + std::to_string(slot->holders.size()) + " queued "
               + std::to_string(core.queued()) + " on-worker " + std::to_string(std::ranges::count(wkr.running, drv))
               + " avoid" + avoid;
    }

    // Once every client, lease and worker is gone: what is left, "" if nothing.
    [[nodiscard]] static auto leftOver(const Core & core) -> std::string
    {
        auto const live = std::ranges::count_if(core.entries, [](auto const & slot) -> bool { return slot.has_value(); });
        if (core.lingering.empty() && core.leaseDrvs.empty() && core.released.empty() && live == 0 && core.follows.empty()) {
            return "";
        }
        return "lingering " + std::to_string(core.lingering.size()) + ", leaseDrvs " + std::to_string(core.leaseDrvs.size())
               + ", released " + std::to_string(core.released.size()) + ", entries " + std::to_string(live) + ", follows "
               + std::to_string(core.follows.size());
    }
};

} // namespace nixgrpc::sched

using namespace nixgrpc::sched;

namespace {

auto want(std::string_view drv, double lingerMs, std::string_view lease) -> Core::WantInfo
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

auto hello(Core & shard, std::string_view addr, uint32_t jobs, std::vector<std::pair<std::string_view, uint64_t>> running)
    -> WorkerId
{
    std::vector<Core::Superseded> ignored;
    return shard.hello(
        {.addr = addr,
         .systems = {"x"},
         .features = {},
         .maxJobs = jobs,
         .running = std::move(running),
         .faulty = false,
         .namesPlacements = true},
        ignored);
}

struct Counts
{
    size_t steps = 0;
    size_t kept = 0;     // drvs kept as a stream ended
    size_t released = 0; // leases a Release ended on a drv
    size_t revokes = 0;
    size_t staleDones = 0;  // Dones naming another placement
    size_t staleOnKept = 0; // of those, while a lease kept the drv
};

// Where a run failed, for the message.
struct Where
{
    uint32_t seed = 0;
    size_t step = 0;
    const char * op = "setup";
};

// `steps` random steps from `seed`, the bookkeeping checked after each, then
// every client, lease and worker gone, and nothing may be left.
void run(size_t steps, Where & where, Counts & counts)
{
    const std::array<std::string_view, 6> drvs{"d0.drv", "d1.drv", "d2.drv", "d3.drv", "d4.drv", "d5.drv"};
    const std::array<std::string_view, 4> leases{"", "a", "b", "c"};
    const std::array<std::string_view, 2> addrs{"w0", "w1"};
    const std::array<uint32_t, 2> jobs{2, 1};
    const std::array<double, 3> lingers{0, 30e3, 600e3};
    std::mt19937 rng(where.seed); // NOLINT(cert-msc32-c,cert-msc51-cpp,bugprone-random-generator-seed): reproducible
    auto const pick = [&rng](size_t bound) -> size_t { return static_cast<size_t>(rng() % bound); };
    // Half the runs with a reconnect grace, as the dispatcher sets it.
    Core shard(Config{.departedGraceMs = pick(2) == 0 ? 0 : 10e3});
    std::array<WorkerId, 2> wids{hello(shard, addrs.at(0), jobs.at(0), {}), hello(shard, addrs.at(1), jobs.at(1), {})};
    std::array<bool, 2> up{true, true};
    std::vector<ClientId> live;
    ClientId next = 1;
    double now = 0;
    std::vector<std::pair<DrvId, WorkerId>> revokes;
    std::vector<Assign> out;
    for (where.step = 0; where.step < steps; where.step++) {
        now += static_cast<double>(pick(90)) * 1000.0;
        switch (pick(10)) {
        case 0:
        case 1:
        case 2: {
            where.op = "want";
            ClientId const client = (live.empty() || pick(3) == 0) ? next++ : live.at(pick(live.size()));
            if (std::ranges::find(live, client) == live.end()) {
                live.push_back(client);
            }
            shard.want(
                client,
                want(drvs.at(pick(drvs.size())), lingers.at(pick(lingers.size())), leases.at(pick(leases.size()))),
                now);
            break;
        }
        case 3: {
            where.op = "clientGone";
            if (live.empty()) {
                break;
            }
            auto const gone = pick(live.size());
            std::vector<DrvId> kept;
            shard.clientGone(live.at(gone), now, revokes, kept);
            counts.kept += kept.size();
            live.erase(live.begin() + static_cast<std::ptrdiff_t>(gone));
            break;
        }
        case 4:
            where.op = "cancel";
            if (!live.empty()) {
                static_cast<void>(shard.cancel(live.at(pick(live.size())), drvs.at(pick(drvs.size())), now));
            }
            break;
        case 5:
            where.op = "release";
            counts.released += shard.release(leases.at(pick(leases.size())), now, now + 600e3, revokes);
            break;
        case 6:
            where.op = "dispatch";
            shard.dispatch(out);
            break;
        case 7: {
            where.op = "done";
            auto const name = drvs.at(pick(drvs.size()));
            auto const drv = shard.findDrv(name);
            if (drv && shard.entry(*drv) && shard.entry(*drv)->worker != noWorker) {
                // The Done or give-back names no placement (0), the current
                // one, or another one, earlier or later: one naming another
                // placement ends nothing, kept or not.
                auto const wid = shard.entry(*drv)->worker;
                auto const cur = shard.entry(*drv)->assignId;
                bool const kept = shard.entry(*drv)->idleSinceMs.has_value();
                uint64_t named = 0;
                switch (pick(3)) {
                case 0:
                    break;
                case 1:
                    named = cur;
                    break;
                default:
                    named = (cur > 1 && pick(2) == 0) ? 1 + pick(cur - 1) : cur + 1 + pick(5);
                    break;
                }
                bool const other = named != 0 && named != cur;
                auto const before = other ? LeaseInvariants::snapshot(shard, *drv, wid) : std::string();
                bool const taken = pick(2) == 0 ? shard.done(wid, name, {}, named) : shard.giveBack(wid, name, named);
                if (other) {
                    counts.staleDones++;
                    counts.staleOnKept += kept ? 1 : 0;
                    if (taken || LeaseInvariants::snapshot(shard, *drv, wid) != before) {
                        throw nix::Error(
                            "a Done naming another placement ended something: %s -> %s",
                            before,
                            LeaseInvariants::snapshot(shard, *drv, wid));
                    }
                } else if (!taken) {
                    throw nix::Error("a Done naming its placement, or none, ended nothing");
                }
            }
            break;
        }
        case 8:
            where.op = "expireLingers";
            static_cast<void>(shard.expireLingers(now, revokes));
            static_cast<void>(shard.stats());
            break;
        default: {
            auto const wkr = pick(2);
            if (up.at(wkr)) {
                where.op = "workerGone";
                shard.workerGone(wids.at(wkr));
                up.at(wkr) = false;
            } else {
                where.op = "hello";
                std::vector<std::pair<std::string_view, uint64_t>> running;
                if (pick(2) == 0) {
                    running.emplace_back(drvs.at(pick(drvs.size())), 7);
                }
                wids.at(wkr) = hello(shard, addrs.at(wkr), jobs.at(wkr), std::move(running));
                up.at(wkr) = true;
            }
            break;
        }
        }
        LeaseInvariants::check(shard);
        counts.steps++;
    }
    counts.revokes += revokes.size();
    where.op = "teardown";
    for (auto const client : live) {
        std::vector<DrvId> kept;
        shard.clientGone(client, now, revokes, kept);
        LeaseInvariants::check(shard);
    }
    now += 1e9;
    static_cast<void>(shard.expireLingers(now, revokes));
    LeaseInvariants::check(shard);
    for (size_t wkr = 0; wkr < up.size(); wkr++) {
        if (up.at(wkr)) {
            shard.workerGone(wids.at(wkr));
        }
    }
    static_cast<void>(shard.expireLingers(now, revokes));
    LeaseInvariants::check(shard);
    if (auto const left = LeaseInvariants::leftOver(shard); !left.empty()) {
        throw nix::Error("left over after teardown: %s", left);
    }
}

} // namespace
// NOLINTEND(*-magic-numbers, bugprone-unchecked-optional-access, readability-identifier-length)

auto main() -> int
{
    constexpr uint32_t seeds = 500;
    constexpr size_t steps = 300;
    Where where;
    Counts counts;
    try {
        for (where.seed = 0; where.seed < seeds; where.seed++) {
            run(steps, where, counts);
        }
        std::cout << seeds << " seeds, " << counts.steps << " steps checked: " << counts.kept << " kept as a stream ended, "
                  << counts.released << " leases released on a drv, " << counts.revokes << " revokes, " << counts.staleDones
                  << " Dones naming another placement (" << counts.staleOnKept << " on a kept drv)\n";
        return 0;
    } catch (std::exception & err) {
        std::cerr << "seed " << where.seed << " step " << where.step << " after " << where.op << ": " << err.what() << "\n";
        return 1;
    }
}
