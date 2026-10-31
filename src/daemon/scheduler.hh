#pragma once
// Placement core. No I/O, no locking; ported from sim/src/sim.rs.
// Queue and free-slot index are kept per system so a full system costs O(1)
// per dispatch; a worker offering several systems sits in each index.
// Inputs arrive from the network, so every id is range-checked and every
// state transition tolerates duplicates and stale messages.

#include <algorithm>
#include <array>
#include <bit>
#include <compare>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <map>
#include <optional>
#include <set>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <absl/container/flat_hash_map.h>


#include <nix/util/error.hh>

namespace nixgrpc::sched {

using ClientId = uint64_t;
using WorkerId = uint32_t;
using DrvId = uint32_t;
constexpr WorkerId noWorker = std::numeric_limits<WorkerId>::max();

// Everything here runs under the dispatcher's one mutex, so worst case per
// call counts. Store paths are keyed by their 32-char hash part held inline
// in an open-addressing table: no allocation per key, and growth is a flat
// copy of 40-byte slots. std::unordered_map (node re-bucketing) or heap
// string keys each stalled dispatcher-bench for 20-40 ms per rehash.
struct PathKey
{
    static constexpr size_t len = 32;
    std::array<char, len> hash{};

    // `/nix/store/<hash>-name`, or a bare name in tests.
    explicit PathKey(std::string_view path)
    {
        if (auto const slash = path.rfind('/'); slash != std::string_view::npos) {
            path.remove_prefix(slash + 1);
        }
        path = path.substr(0, len);
        std::ranges::copy(path, hash.begin());
    }
    auto operator==(const PathKey &) const -> bool = default;
    template<typename H>
    friend auto AbslHashValue(H state, const PathKey & key) -> H
    {
        return H::combine_contiguous(std::move(state), key.hash.data(), len);
    }
};
template<typename V>
using PathMap = absl::flat_hash_map<PathKey, V>;

// Min-heap of (key, id) with update-key and erase by id.
class IndexedHeap
{
    std::vector<std::pair<double, DrvId>> heap;
    std::vector<uint32_t> pos;
    static constexpr uint32_t npos = std::numeric_limits<uint32_t>::max();

    void place(size_t idx, std::pair<double, DrvId> item)
    {
        pos.at(item.second) = static_cast<uint32_t>(idx);
        heap.at(idx) = item;
    }

    void siftUp(size_t idx)
    {
        auto item = heap.at(idx);
        while (idx > 0) {
            const size_t parent = (idx - 1) / 4;
            if (heap.at(parent) <= item) {
                break;
            }
            place(idx, heap.at(parent));
            idx = parent;
        }
        place(idx, item);
    }

    void siftDown(size_t idx)
    {
        auto item = heap.at(idx);
        const size_t len = heap.size();
        for (;;) {
            const size_t first = (4 * idx) + 1;
            if (first >= len) {
                break;
            }
            size_t best = first;
            for (size_t child = first + 1; child < std::min(first + 4, len); child++) {
                if (heap.at(child) < heap.at(best)) {
                    best = child;
                }
            }
            if (item <= heap.at(best)) {
                break;
            }
            place(idx, heap.at(best));
            idx = best;
        }
        place(idx, item);
    }

public:
    [[nodiscard]] auto size() const -> size_t
    {
        return heap.size();
    }

    [[nodiscard]] auto contains(DrvId drv) const -> bool
    {
        return drv < pos.size() && pos.at(drv) != npos;
    }

    [[nodiscard]] auto top() const -> std::optional<DrvId>
    {
        return heap.empty() ? std::nullopt : std::optional(heap.front().second);
    }

    [[nodiscard]] auto topKey() const -> std::optional<double>
    {
        return heap.empty() ? std::nullopt : std::optional(heap.front().first);
    }

    void set(DrvId drv, double key)
    {
        if (drv == npos) {
            throw nix::Error("IndexedHeap: id out of range");
        }
        if (drv >= pos.size()) {
            pos.resize(size_t{drv} + 1, npos);
        }
        if (pos.at(drv) == npos) {
            pos.at(drv) = static_cast<uint32_t>(heap.size());
            heap.emplace_back(key, drv);
            siftUp(heap.size() - 1);
            return;
        }
        auto const idx = pos.at(drv);
        auto const old = heap.at(idx).first;
        heap.at(idx).first = key;
        if (key < old) {
            siftUp(idx);
        } else {
            siftDown(idx);
        }
    }

    void erase(DrvId drv)
    {
        if (!contains(drv)) {
            return;
        }
        auto const idx = pos.at(drv);
        pos.at(drv) = npos;
        auto last = heap.back();
        heap.pop_back();
        if (idx >= heap.size()) {
            return;
        }
        place(idx, last);
        siftUp(idx);
        siftDown(pos.at(last.second));
    }
};

// Workers bucketed by free slot count: O(1) "most free" and O(1) update.
class FreeIndex
{
public:
    static constexpr int32_t maxSlots = 63;

private:
    std::vector<int32_t> free; // may go negative (draining below running)
    std::vector<uint32_t> pos;
    std::vector<bool> tracked;
    std::vector<std::vector<WorkerId>> byFree = std::vector<std::vector<WorkerId>>(maxSlots + 1);
    uint64_t nonempty = 0;

    [[nodiscard]] static auto bucket(int32_t val) -> size_t
    {
        return static_cast<size_t>(std::clamp<int32_t>(val, 0, maxSlots));
    }

    void check(WorkerId wid) const
    {
        if (wid >= free.size() || !tracked.at(wid)) {
            throw nix::Error("FreeIndex: unknown worker %d", wid);
        }
    }

public:
    // Make `wid` known with zero free slots. Ids not pushed read as zero.
    void push(WorkerId wid)
    {
        if (wid < free.size() && tracked.at(wid)) {
            return;
        }
        if (wid >= free.size()) {
            free.resize(wid + 1, 0);
            pos.resize(wid + 1, 0);
            tracked.resize(wid + 1, false);
        }
        tracked.at(wid) = true;
        pos.at(wid) = static_cast<uint32_t>(byFree.at(0).size());
        byFree.at(0).push_back(wid);
        nonempty |= uint64_t{1};
    }

    [[nodiscard]] auto get(WorkerId wid) const -> int32_t
    {
        return wid < free.size() ? free.at(wid) : 0;
    }

    void set(WorkerId wid, int32_t val)
    {
        check(wid);
        auto const from = bucket(free.at(wid));
        auto const dest = bucket(val);
        free.at(wid) = val;
        if (from == dest) {
            return;
        }
        auto & src = byFree.at(from);
        auto const idx = pos.at(wid);
        if (idx >= src.size() || src.at(idx) != wid) {
            throw nix::Error("FreeIndex: corrupt bucket for worker %d", wid);
        }
        auto const last = src.back();
        src.at(idx) = last;
        pos.at(last) = idx;
        src.pop_back();
        if (src.empty()) {
            nonempty &= ~(uint64_t{1} << from);
        }
        pos.at(wid) = static_cast<uint32_t>(byFree.at(dest).size());
        byFree.at(dest).push_back(wid);
        nonempty |= uint64_t{1} << dest;
    }

    [[nodiscard]] auto maxFree() const -> int32_t
    {
        return nonempty == 0 ? 0 : maxSlots - std::countl_zero(nonempty);
    }

    [[nodiscard]] auto emptiest() const -> std::optional<WorkerId>
    {
        auto const top = maxFree();
        if (top <= 0 || byFree.at(static_cast<size_t>(top)).empty()) {
            return std::nullopt;
        }
        return byFree.at(static_cast<size_t>(top)).front();
    }
};

struct Config
{
    static constexpr double defaultAgeWeight = 4.0;
    static constexpr double defaultFaninWeight = 30e3;
    static constexpr double defaultLocLambda = 256e6;
    double ageWeight = defaultAgeWeight;     // cp_hint ms per ms waited
    double faninWeight = defaultFaninWeight; // cp_hint ms per extra follower
    double locLambda = defaultLocLambda;     // input bytes a fully free worker is worth
    // How long an assigned build is kept once its last client's stream ended
    // without a lease on it, for that client to reconnect and follow it again.
    double departedGraceMs = 0;
};

struct Assign
{
    DrvId drv = 0;
    WorkerId worker = 0;
    uint64_t assignId = 0;
    std::vector<ClientId> clients;
};

// A chain's promise to come back for a build (Want.lease_id, Want.linger_s):
// live while a client holding it follows the build, and for termMs once the
// last such client left.
struct Lease
{
    std::string id;     // "" for clients that declared a linger without an id
    double termMs = 0;  // an id's latest declaration; without one, the longest
    uint32_t links = 0; // followers holding it
    double leftMs = 0;  // when its last holder left (links == 0)
};

struct Entry
{
    std::string drvPath;
    std::vector<PathKey> inputs;       // for locality only
    std::string system;
    std::vector<std::string> features; // required
    std::vector<ClientId> followers;
    // Followers holding a lease on it (at most one each), and which.
    std::vector<std::pair<ClientId, std::string>> holders;
    std::vector<Lease> leases;
    WorkerId worker = noWorker;
    uint64_t assignId = 0;
    double enqMs = 0;
    double cpHint = 0;
    bool unplaceable = false; // as last told to the followers
    // Gave it back, or a client bounced off it (Want.bounced_assign_id):
    // each used only if no other worker can run it.
    std::vector<WorkerId> avoid;
    // Its last follower gone at this time while a lease on it was live, or
    // (assigned) its stream ended within Config::departedGraceMs: kept, queued
    // or assigned, until keptUntilMs. Queued, it keeps its place but is not
    // placed: no client would take the assignment.
    std::optional<double> idleSinceMs;
    double keptUntilMs = 0;  // its key in Core::lingering while idleSinceMs
    double graceUntilMs = 0; // its last client's stream ended: kept until then
    std::optional<std::string> graceLease; // the lease that client held, if any: its Release ends the grace
};

struct Worker
{
    std::string addr;
    std::vector<std::string> systems;
    std::vector<std::string> features;
    int32_t maxJobs = 0;
    bool up = false;
    bool draining = false;
    // Its last publish to the cache failed: it gets builds only while no
    // other worker has a slot, until it reports that publishing works again.
    bool faulty = false;
    bool namesPlacements = false; // see Hello.names_placements
    std::vector<DrvId> running;
};

class Core
{
public:
    explicit Core(Config cfg = {})
        : cfg(cfg)
    {
        drvIds.reserve(initialEntries);
        entries.reserve(initialEntries);
        // To its cap so it never grows: even a flat rehash of 2^18 slots is
        // ~10 ms. 12 MB of address space, untouched until that many outputs.
        lastBuilder.reserve(maxLastBuilder);
    }

    auto drvId(std::string_view path) -> DrvId
    {
        const PathKey key(path);
        if (auto const iter = drvIds.find(key); iter != drvIds.end()) {
            return iter->second;
        }
        DrvId drv = 0;
        if (!freeIds.empty()) {
            drv = freeIds.back();
            freeIds.pop_back();
        } else {
            if (entries.size() >= maxEntries) {
                throw nix::Error("scheduler: derivation table full");
            }
            drv = static_cast<DrvId>(entries.size());
            entries.emplace_back();
        }
        drvIds.emplace(key, drv);
        return drv;
    }

    [[nodiscard]] auto findDrv(std::string_view path) const -> std::optional<DrvId>
    {
        auto const iter = drvIds.find(PathKey(path));
        return iter == drvIds.end() ? std::nullopt : std::optional(iter->second);
    }

    auto workerId(std::string_view addr) -> WorkerId
    {
        if (auto const iter = workerIds.find(std::string(addr)); iter != workerIds.end()) {
            return iter->second;
        }
        auto const wid = static_cast<WorkerId>(workers.size());
        workers.push_back(Worker{.addr = std::string(addr)});
        workerIds.emplace(std::string(addr), wid);
        return wid;
    }

    [[nodiscard]] auto entry(DrvId drv) const -> const std::optional<Entry> &
    {
        return entries.at(drv);
    }

    [[nodiscard]] auto worker(WorkerId wid) const -> const Worker &
    {
        return workers.at(wid);
    }

    [[nodiscard]] auto queued() const -> size_t
    {
        size_t total = 0;
        for (const auto & [name, state] : systems) {
            total += state.queue.size();
        }
        return total;
    }

    [[nodiscard]] auto workersUp() const -> size_t
    {
        return workersUp_;
    }

    [[nodiscard]] auto workerCount() const -> size_t
    {
        return workers.size();
    }

    struct WantResult
    {
        DrvId drv = 0;
        std::optional<std::pair<WorkerId, uint64_t>> assigned;
        std::optional<WorkerId> bouncedOff; // the worker the drv was taken back from
        // It was kept with no follower for this long, until this Want.
        std::optional<double> idleMs;
    };

    struct WantInfo
    {
        std::string_view drvPath;
        std::vector<std::string_view> inputs;
        std::string_view system;
        std::vector<std::string> features;
        double cpHintMs = 0;
        uint64_t bouncedAssignId = 0; // see Want.bounced_assign_id
        double lingerMs = 0;      // see Want.linger_s, capped by the caller
        std::string_view leaseId; // see Want.lease_id
    };

    // Caller handled the cache-hit case already.
    auto want(ClientId client, WantInfo info, double nowMs) -> WantResult
    {
        auto const drv = drvId(info.drvPath);
        auto const cpHintMs = info.cpHintMs;
        auto & slot = entries.at(drv);
        if (!slot) {
            slot = Entry{
                .drvPath = std::string(info.drvPath),
                .system = std::string(info.system),
                .features = std::move(info.features),
                .enqMs = nowMs};
            slot->inputs.reserve(info.inputs.size());
            for (auto const input : info.inputs) {
                slot->inputs.emplace_back(input);
            }
        } else if (slot->system.empty()) {
            // Known only from a worker's Hello so far.
            slot->system = std::string(info.system);
            slot->features = std::move(info.features);
        }
        // Followers of an assigned drv are told again if its worker vanishes.
        if (std::ranges::find(slot->followers, client) == slot->followers.end()) {
            slot->followers.push_back(client);
            follows[client].push_back(drv);
        }
        // A client back for a build kept for it: it follows the build again.
        std::optional<double> idleMs;
        if (slot->idleSinceMs) {
            idleMs = nowMs - *slot->idleSinceMs;
            unkeep(drv, *slot);
        }
        // A lease released meanwhile keeps nothing: this client is on its
        // way out, or its chain was stopped.
        if (info.lingerMs > 0 && !wasReleased(info.leaseId, nowMs)) {
            hold(drv, *slot, client, info.leaseId, info.lingerMs);
        }
        slot->cpHint = std::max(slot->cpHint, cpHintMs);
        std::optional<WorkerId> bouncedOff;
        if (slot->worker != noWorker && info.bouncedAssignId != 0 && slot->assignId == info.bouncedAssignId) {
            // That worker was not reached or did not take it, so there is
            // nothing to revoke: it gives back an Expect left there on its own.
            bouncedOff = slot->worker;
            giveBack(slot->worker, drv);
        }
        if (slot->worker != noWorker) {
            return {.drv = drv, .assigned = std::pair(slot->worker, slot->assignId), .bouncedOff = {}, .idleMs = idleMs};
        }
        sys(slot->system).queue.set(drv, prio(*slot));
        return {.drv = drv, .assigned = {}, .bouncedOff = bouncedOff, .idleMs = idleMs};
    }

    // Worker to Revoke on, if the last follower cancelled an assigned drv
    // that no lease keeps. A Cancel says the client no longer wants it: the
    // lease it held on it ends, unless another of its clients still holds it.
    auto cancel(ClientId client, std::string_view drvPath, double nowMs) -> std::optional<WorkerId>
    {
        auto found = findDrv(drvPath);
        if (!found) {
            return std::nullopt;
        }
        return leave(client, *found, nowMs, Departure::cancelled).revoke;
    }

    // A client's stream ended. `kept`: drvs whose last follower this was,
    // kept for a lease or for departedGraceMs; `revokes`: assigned drvs whose
    // last follower this was, not kept.
    // NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
    void clientGone(ClientId client, double nowMs, std::vector<std::pair<DrvId, WorkerId>> & revokes, std::vector<DrvId> & kept)
    {
        auto const iter = follows.find(client);
        if (iter == follows.end()) {
            releasedClients.erase(client);
            return;
        }
        auto const drvs = std::move(iter->second);
        follows.erase(iter);
        for (auto const drv : drvs) {
            auto const left = leave(client, drv, nowMs, Departure::gone);
            if (left.revoke) {
                revokes.emplace_back(drv, *left.revoke);
            } else if (left.kept) {
                kept.push_back(drv);
            }
        }
        releasedClients.erase(client);
    }

    // Drvs kept with no follower whose last lease and grace are over by
    // `nowMs`: a queued one is forgotten (returned, by path), an assigned one
    // gets the worker to Revoke it on and stays assigned until its Done.
    auto expireLingers(double nowMs, std::vector<std::pair<DrvId, WorkerId>> & revokes) -> std::vector<std::string>
    {
        std::vector<std::string> forgotten;
        while (!lingering.empty() && lingering.begin()->first <= nowMs) {
            auto const [until, drv] = *lingering.begin();
            auto & slot = entries.at(drv);
            if (!slot || !slot->idleSinceMs || slot->keptUntilMs != until) {
                lingering.erase(lingering.begin()); // stale
                continue;
            }
            if (!keep(drv, *slot, nowMs)) {
                if (slot->worker == noWorker) {
                    forgotten.push_back(slot->drvPath);
                }
                letGo(drv, revokes);
            }
        }
        while (!releasedBy.empty() && releasedBy.begin()->first <= nowMs) {
            released.erase(releasedBy.begin()->second);
            releasedBy.erase(releasedBy.begin());
        }
        return forgotten;
    }

    // A chain is over (Release): lease `lease` ends on every build. One it
    // alone kept is forgotten if queued and revoked (into `revokes`) if
    // assigned, its reconnect grace ended too, as nobody of the chain comes
    // back for it; one followed by a client of it runs on for that client but
    // is no longer kept for the lease once it leaves, nor for a reconnect
    // grace. Until `untilMs` a Want
    // with the lease keeps nothing, so a link still on its way out cannot
    // renew it. The number of builds that held it.
    // NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
    auto release(std::string_view lease, double nowMs, double untilMs, std::vector<std::pair<DrvId, WorkerId>> & revokes) -> size_t
    {
        if (lease.empty()) {
            return 0; // a linger without an id: nobody can name it
        }
        rememberReleased(std::string(lease), untilMs);
        auto const found = leaseDrvs.find(std::string(lease));
        if (found == leaseDrvs.end()) {
            return 0;
        }
        auto const drvs = std::move(found->second);
        leaseDrvs.erase(found);
        size_t held = 0;
        for (auto const drv : drvs) {
            auto & slot = entries.at(drv);
            if (!slot) {
                continue;
            }
            auto const lse = std::ranges::find(slot->leases, lease, &Lease::id);
            if (lse == slot->leases.end()) {
                continue;
            }
            held++;
            std::erase_if(slot->holders, [&](const auto & holder) -> bool {
                if (holder.second != lease) {
                    return false;
                }
                releasedClients.insert(holder.first);
                return true;
            });
            slot->leases.erase(lse); // unindexed above
            if (slot->graceLease == lease) {
                slot->graceUntilMs = 0; // the grace of a client of this lease
            }
            if (slot->followers.empty() && slot->idleSinceMs && !keep(drv, *slot, nowMs)) {
                letGo(drv, revokes);
            }
        }
        return held;
    }

    struct HelloInfo
    {
        std::string_view addr;
        std::vector<std::string> systems; // first is the native one
        std::vector<std::string> features;
        uint32_t maxJobs = 0;
        std::vector<std::pair<std::string_view, uint64_t>> running; // running + expecting, with assignId
        bool faulty = false;
        bool namesPlacements = false; // Hello.names_placements
    };

    // A reported drv that followers wait for: the caller sends them a fresh
    // Assigned. If this scheduler had meanwhile placed it on `loser`, the
    // reporter keeps it and the caller revokes `loser`.
    struct Superseded
    {
        DrvId drv;
        WorkerId loser = noWorker;
    };

    auto hello(const HelloInfo & info, std::vector<Superseded> & superseded) -> WorkerId
    {
        if (info.addr.empty()) {
            throw nix::Error("scheduler: Hello without addr");
        }
        auto const wid = workerId(info.addr);
        if (workers.at(wid).up) {
            workerGone(wid);
        }
        auto & wkr = workers.at(wid);
        wkr.up = true;
        workersUp_++;
        wkr.draining = false;
        setFaulty(wid, info.faulty);
        wkr.namesPlacements = info.namesPlacements;
        wkr.systems = info.systems;
        for (const auto & name : wkr.systems) {
            sys(name).free.push(wid);
        }
        wkr.features = info.features;
        wkr.maxJobs = static_cast<int32_t>(std::min<uint32_t>(info.maxJobs, FreeIndex::maxSlots));
        for (auto [path, assignId] : info.running) {
            auto const drv = drvId(path);
            auto & slot = entries.at(drv);
            if (!slot) {
                slot = Entry{.drvPath = std::string(path)};
            }
            if (slot->worker != noWorker && slot->worker != wid) {
                // Placed since our restart, seconds old. The reporter is further along.
                superseded.push_back({.drv = drv, .loser = slot->worker});
                std::erase(workers.at(slot->worker).running, drv);
                refreshFree(slot->worker);
            } else if (slot->worker == noWorker && !slot->followers.empty()) {
                superseded.push_back({.drv = drv});
            }
            slot->worker = wid;
            slot->assignId = assignId;
            sys(slot->system).queue.erase(drv);
            if (std::ranges::find(wkr.running, drv) == wkr.running.end()) {
                wkr.running.push_back(drv);
            }
        }
        refreshFree(wid);
        return wid;
    }

    // `assignId`: the placement the Done ends. One that names an earlier
    // placement of the drv on this worker ends nothing, that placement's own
    // Done follows; 0 (not known) ends the current one. False if it ended
    // nothing for that. The outputs are on the worker either way.
    auto done(
        WorkerId wid,
        std::string_view drvPath,
        const std::vector<std::pair<std::string_view, uint64_t>> & outputs,
        uint64_t assignId = 0) -> bool
    {
        auto & wkr = workers.at(wid);
        for (auto [path, size] : outputs) {
            rememberBuilder(path, wid, size);
        }
        auto found = findDrv(drvPath);
        if (!found) {
            return true;
        }
        if (namesAnother(wid, *found, assignId)) {
            return false;
        }
        std::erase(wkr.running, *found);
        refreshFree(wid);
        auto & slot = entries.at(*found);
        if (slot && slot->worker == wid) {
            retire(*found);
        }
        return true;
    }

    // The worker can't run it after all: place it elsewhere. `assignId` as
    // for done().
    auto giveBack(WorkerId wid, std::string_view drvPath, uint64_t assignId = 0) -> bool
    {
        auto found = findDrv(drvPath);
        if (!found) {
            return true;
        }
        if (namesAnother(wid, *found, assignId)) {
            return false;
        }
        giveBack(wid, *found);
        return true;
    }

    void giveBack(WorkerId wid, DrvId drv)
    {
        std::erase(workers.at(wid).running, drv);
        refreshFree(wid);
        auto & slot = entries.at(drv);
        if (!slot || slot->worker != wid) {
            return;
        }
        slot->worker = noWorker;
        // Latest last: a worker that gives it back again is the one passed over.
        std::erase(slot->avoid, wid);
        slot->avoid.push_back(wid);
        // Kept for a client that will be back: in its place for it.
        if (slot->followers.empty() && !slot->idleSinceMs) {
            retire(drv);
        } else {
            sys(slot->system).queue.set(drv, prio(*slot));
        }
    }

    void setDraining(WorkerId wid, bool draining)
    {
        workers.at(wid).draining = draining;
        refreshFree(wid);
    }

    void setFaulty(WorkerId wid, bool faulty)
    {
        auto & wkr = workers.at(wid);
        if (std::exchange(wkr.faulty, faulty) == faulty) {
            return;
        }
        if (faulty) {
            faultyCount++;
        } else {
            faultyCount--;
        }
    }

    // Capacity changed while connected. Below what runs, free goes negative
    // and nothing new is placed until enough finish.
    void setMaxJobs(WorkerId wid, uint32_t maxJobs)
    {
        workers.at(wid).maxJobs = static_cast<int32_t>(std::min<uint32_t>(maxJobs, FreeIndex::maxSlots));
        refreshFree(wid);
    }

    // Requeues what still has followers or is kept for one; a reconnecting
    // worker's Hello reconciles.
    void workerGone(WorkerId wid)
    {
        auto & wkr = workers.at(wid);
        if (wkr.up) {
            workersUp_--;
        }
        wkr.up = false;
        refreshFree(wid);
        for (auto const drv : std::exchange(wkr.running, {})) {
            if (drv >= entries.size()) {
                continue;
            }
            auto & slot = entries.at(drv);
            if (!slot || slot->worker != wid) {
                continue;
            }
            slot->worker = noWorker;
            if (slot->followers.empty() && !slot->idleSinceMs) {
                retire(drv);
            } else {
                sys(slot->system).queue.set(drv, prio(*slot));
            }
        }
    }

    // Across systems, so the most urgent drv wins.
    void dispatch(std::vector<Assign> & out)
    {
        std::vector<Skipped> skipped;
        for (;;) {
            std::string const * best = nullptr;
            double bestKey = 0;
            for (const auto & [name, state] : systems) {
                auto const key = state.queue.topKey();
                if (key && state.free.maxFree() > 0 && (best == nullptr || *key < bestKey)) {
                    best = &name;
                    bestKey = *key;
                }
            }
            if (best == nullptr) {
                break;
            }
            dispatchHead(std::string(*best), out, skipped);
        }
        for (const auto & skip : skipped) {
            sys(skip.system).queue.set(skip.drv, skip.key);
        }
    }

    // Some connected, non-draining worker could ever run it.
    [[nodiscard]] auto placeable(const Entry & ent) const -> bool
    {
        return std::ranges::any_of(
            workers, [&](const Worker & wkr) -> bool { return wkr.up && !wkr.draining && offers(wkr, ent); });
    }

    // Features of the connected, non-draining workers that serve the system.
    [[nodiscard]] auto offeredFeatures(const std::string & system) const -> std::vector<std::string>
    {
        std::set<std::string> feats;
        for (const auto & wkr : workers) {
            if (wkr.up && !wkr.draining && std::ranges::find(wkr.systems, system) != wkr.systems.end()) {
                feats.insert(wkr.features.begin(), wkr.features.end());
            }
        }
        return {feats.begin(), feats.end()};
    }

    [[nodiscard]] auto placeable(DrvId drv) const -> bool
    {
        const auto & slot = entries.at(drv);
        return slot && placeable(*slot);
    }

    // What a follower was last told about a queued entry.
    void markUnplaceable(DrvId drv, bool unplaceable)
    {
        if (auto & slot = entries.at(drv)) {
            slot->unplaceable = unplaceable;
        }
    }

    // Queued entries whose placeability flipped since the followers were
    // last told. After Hello, workerGone and Load: O(entries), memoised per
    // (system, features).
    void placeabilityChanges(std::vector<DrvId> & out)
    {
        std::map<std::pair<std::string, std::vector<std::string>>, bool> byKey;
        for (size_t idx = 0; idx < entries.size(); idx++) {
            auto & slot = entries.at(idx);
            if (!slot || slot->worker != noWorker || slot->followers.empty()) {
                continue;
            }
            auto [iter, fresh] = byKey.try_emplace({slot->system, slot->features}, false);
            if (fresh) {
                iter->second = placeable(*slot);
            }
            if (slot->unplaceable == iter->second) {
                slot->unplaceable = !iter->second;
                out.push_back(static_cast<DrvId>(idx));
            }
        }
    }

    struct StatsKey
    {
        std::string system;
        std::string features; // sorted, comma-joined
        auto operator<=>(const StatsKey &) const = default;
    };

    struct Stats
    {
        // Keyed by what the derivation requires.
        size_t queued = 0;      // wanted, not placed
        size_t unplaceable = 0; // of those, no connected worker could ever run it
        // Assigned to a worker, so holding its slot, whether a client follows
        // it or not: also one a worker's Hello re-announced to a restarted
        // scheduler, which knows it from that alone, one whose clients left
        // and that is kept or revoked, until its Done.
        size_t running = 0;
        // Kept with no follower for a lease or a reconnect: an assigned one is
        // in `running` too, it holds its slot; a queued one is not in
        // `queued`, nobody wants it placed until its client is back.
        size_t lingering = 0;
        // Keyed by what the worker offers (native system).
        size_t slots = 0; // maxJobs of up, non-draining workers
        size_t free = 0;
    };

    [[nodiscard]] static auto featureKey(std::vector<std::string> feats) -> std::string
    {
        std::ranges::sort(feats);
        std::string key;
        for (const auto & feat : feats) {
            key += key.empty() ? "" : ",";
            key += feat;
        }
        return key;
    }

    // O(entries + workers), for metrics.
    [[nodiscard]] auto stats() const -> std::map<StatsKey, Stats>
    {
        std::map<StatsKey, Stats> out;
        for (const auto & [name, state] : systems) {
            out[{.system = name, .features = ""}];
        }
        for (const auto & wkr : workers) {
            if (!wkr.up || wkr.draining || wkr.systems.empty()) {
                continue;
            }
            auto & sst = out[{.system = wkr.systems.front(), .features = featureKey(wkr.features)}];
            sst.slots += static_cast<size_t>(wkr.maxJobs);
            sst.free += static_cast<size_t>(std::max(0, wkr.maxJobs - static_cast<int32_t>(wkr.running.size())));
        }
        // placeable() is O(workers). Entries sharing system+features share the answer.
        std::map<StatsKey, bool> memo;
        for (const auto & slot : entries) {
            if (!slot || (slot->followers.empty() && !slot->idleSinceMs && slot->worker == noWorker)) {
                continue;
            }
            // A build known from a Hello alone has no system of its own yet:
            // it counts under its worker's.
            std::string_view system = slot->system;
            if (system.empty() && slot->worker != noWorker && !workers.at(slot->worker).systems.empty()) {
                system = workers.at(slot->worker).systems.front();
            }
            const StatsKey key{.system = std::string(system), .features = featureKey(slot->features)};
            auto & sst = out[key];
            if (slot->idleSinceMs) {
                sst.lingering++;
            }
            if (slot->worker != noWorker) {
                sst.running++;
                continue;
            }
            if (slot->idleSinceMs) {
                continue;
            }
            sst.queued++;
            auto [iter, fresh] = memo.try_emplace(key, false);
            if (fresh) {
                iter->second = placeable(*slot);
            }
            if (!iter->second) {
                sst.unplaceable++;
            }
        }
        return out;
    }

    // Right after a restart no worker has said Hello yet, so a Want for a
    // system nobody offers is kept, but only for a few such systems.
    [[nodiscard]] auto admitsSystem(const std::string & system) const -> bool
    {
        if (systems.contains(system)) {
            return true;
        }
        return std::ranges::count_if(systems, [&](const auto & named) -> bool { return !offered(named.first); })
            < static_cast<std::ptrdiff_t>(maxUnofferedSystems);
    }

    // Bounds on soft state; inputs come from the network.
    static constexpr size_t maxUnofferedSystems = 16;
    static constexpr size_t maxEntries = 1U << 22U;
    static constexpr size_t maxLastBuilder = 1U << 18U;
    static constexpr size_t initialEntries = 1U << 16U;

private:
    // tests/lease-invariants-test.cc checks the lease bookkeeping below
    // against itself.
    friend struct LeaseInvariants;

    Config cfg;
    PathMap<DrvId> drvIds; // live entries only
    std::vector<DrvId> freeIds;
    std::unordered_map<std::string, WorkerId> workerIds;
    std::vector<std::optional<Entry>> entries;
    std::vector<Worker> workers;
    size_t workersUp_ = 0;
    struct SystemState
    {
        IndexedHeap queue;
        FreeIndex free; // a worker sits in each of its systems' index with the same count
    };
    std::unordered_map<std::string, SystemState> systems;
    std::unordered_map<ClientId, std::vector<DrvId>> follows;
    // Drvs kept with no follower, by when their last lease ends (keptUntilMs).
    std::set<std::pair<double, DrvId>> lingering;
    // Drvs each named lease is on, for Release.
    std::unordered_map<std::string, std::vector<DrvId>> leaseDrvs;
    // Released leases, until when a Want with one keeps nothing; and by that time.
    std::unordered_map<std::string, double> released;
    std::set<std::pair<double, std::string>> releasedBy;
    // Clients that held a lease when it was released: nobody of their chain
    // comes back, so their stream's end gets no reconnect grace.
    std::unordered_set<ClientId> releasedClients;
    // Ids come from the network: past this many, the oldest is forgotten
    // early, which only lets a stray Want of its chain keep a build again.
    static constexpr size_t maxReleased = 1U << 16U;
    // Output path -> who built it last, for input locality. A tie-breaker
    // only, so on overflow the table is simply dropped.
    PathMap<std::pair<WorkerId, uint64_t>> lastBuilder;
    uint64_t lastAssignId = 0;
    size_t faultyCount = 0; // workers with `faulty` set

    struct Leave
    {
        std::optional<WorkerId> revoke; // its last follower left: stop it there
        bool kept = false;              // its last follower left: kept for a lease or a reconnect
    };

    // Why a client no longer follows a drv.
    enum class Departure : uint8_t {
        gone,      // its stream ended: its lease counts from now, and it may reconnect
        cancelled, // it said it no longer wants the drv (Cancel): its lease there ends
    };

    // `client` no longer follows `drv`. A drv whose last follower that was
    // is forgotten if queued and revoked if assigned, unless a lease on it
    // is live, or its stream ended while it was assigned (departedGraceMs):
    // then it stays as it is until the last one is over.
    // `follows[client]` keeps the id until clientGone; a stale id there is
    // harmless because followers is checked, and cheaper than a linear erase per
    // cancel.
    // NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
    auto leave(ClientId client, DrvId drv, double nowMs, Departure why) -> Leave
    {
        if (drv >= entries.size()) {
            return {};
        }
        auto & slot = entries.at(drv);
        if (!slot || std::erase(slot->followers, client) == 0) {
            return {};
        }
        auto held = unhold(drv, *slot, client, nowMs, why);
        if (!slot->followers.empty()) {
            if (slot->worker == noWorker) {
                sys(slot->system).queue.set(drv, prio(*slot));
            }
            return {};
        }
        // Not for a client of a released lease: its chain said nobody comes back.
        if (why == Departure::gone && slot->worker != noWorker && !releasedClients.contains(client)) {
            slot->graceUntilMs = nowMs + cfg.departedGraceMs;
            slot->graceLease = std::move(held);
        }
        if (keep(drv, *slot, nowMs)) {
            return {.kept = true};
        }
        if (slot->worker == noWorker) {
            retire(drv);
            return {};
        }
        return {.revoke = slot->worker};
    }

    // `client` holds lease `lease` on `drv` from its Want: one lease per
    // client, its first.
    // NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
    void hold(DrvId drv, Entry & ent, ClientId client, std::string_view lease, double termMs)
    {
        if (std::ranges::any_of(ent.holders, [&](const auto & holder) -> bool { return holder.first == client; })) {
            return;
        }
        auto lse = std::ranges::find(ent.leases, lease, &Lease::id);
        if (lse == ent.leases.end()) {
            ent.leases.push_back(Lease{.id = std::string(lease)});
            lse = std::prev(ent.leases.end());
            if (!lease.empty()) {
                leaseDrvs[std::string(lease)].push_back(drv);
            }
        }
        // An id is one chain, whose latest link says how long; without one
        // there is nobody to say less, so the longest counts.
        lse->termMs = lease.empty() ? std::max(lse->termMs, termMs) : termMs;
        lse->links++;
        ent.holders.emplace_back(client, std::string(lease));
    }

    // `client` left `drv`: the lease it held there counts from now, or ends
    // there if it cancelled and none of its other clients holds it.
    // The lease it held there, if any.
    // NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
    auto unhold(DrvId drv, Entry & ent, ClientId client, double nowMs, Departure why) -> std::optional<std::string>
    {
        auto const holder =
            std::ranges::find_if(ent.holders, [&](const auto & held) -> bool { return held.first == client; });
        if (holder == ent.holders.end()) {
            return std::nullopt;
        }
        auto lease = std::move(holder->second);
        ent.holders.erase(holder);
        auto const lse = std::ranges::find(ent.leases, lease, &Lease::id);
        if (lse == ent.leases.end() || --lse->links > 0) {
            return lease;
        }
        lse->leftMs = nowMs;
        if (why == Departure::cancelled) {
            dropLease(drv, ent, lse);
        }
        return lease;
    }

    // With no follower: kept, or kept on, until its last live lease and its
    // grace are over. Leases over by `nowMs` are dropped. False if nothing
    // keeps it any more.
    auto keep(DrvId drv, Entry & ent, double nowMs) -> bool
    {
        if (ent.idleSinceMs) {
            lingering.erase({ent.keptUntilMs, drv});
        }
        double until = ent.graceUntilMs > nowMs ? ent.graceUntilMs : 0;
        for (auto lse = ent.leases.begin(); lse != ent.leases.end();) {
            if (lse->links == 0 && lse->leftMs + lse->termMs <= nowMs) {
                lse = dropLease(drv, ent, lse);
            } else {
                until = std::max(until, lse->leftMs + lse->termMs);
                ++lse;
            }
        }
        if (until <= nowMs) {
            ent.idleSinceMs.reset();
            return false;
        }
        if (!ent.idleSinceMs) {
            ent.idleSinceMs = nowMs;
        }
        ent.keptUntilMs = until;
        lingering.emplace(until, drv);
        return true;
    }

    // It has a follower again, or is gone: no longer kept for its leases,
    // which it still carries.
    void unkeep(DrvId drv, Entry & ent)
    {
        ent.graceUntilMs = 0;
        ent.graceLease.reset();
        if (ent.idleSinceMs) {
            lingering.erase({ent.keptUntilMs, drv});
            ent.idleSinceMs.reset();
        }
    }

    // No follower and nothing keeps it: forget it if queued, revoke it if assigned.
    void letGo(DrvId drv, std::vector<std::pair<DrvId, WorkerId>> & revokes)
    {
        auto & slot = entries.at(drv);
        if (!slot) {
            return;
        }
        if (slot->worker == noWorker) {
            retire(drv);
            return;
        }
        revokes.emplace_back(drv, slot->worker);
    }

    auto dropLease(DrvId drv, Entry & ent, std::vector<Lease>::iterator lse) -> std::vector<Lease>::iterator
    {
        if (!lse->id.empty()) {
            if (auto const found = leaseDrvs.find(lse->id); found != leaseDrvs.end()) {
                std::erase(found->second, drv);
                if (found->second.empty()) {
                    leaseDrvs.erase(found);
                }
            }
        }
        return ent.leases.erase(lse);
    }

    void rememberReleased(std::string lease, double untilMs)
    {
        if (auto const old = released.find(lease); old != released.end()) {
            releasedBy.erase({old->second, lease});
            released.erase(old);
        } else if (released.size() >= maxReleased) {
            released.erase(releasedBy.begin()->second);
            releasedBy.erase(releasedBy.begin());
        }
        releasedBy.emplace(untilMs, lease);
        released.emplace(std::move(lease), untilMs);
    }

    [[nodiscard]] auto wasReleased(std::string_view lease, double nowMs) const -> bool
    {
        if (lease.empty()) {
            return false;
        }
        auto const found = released.find(std::string(lease));
        return found != released.end() && found->second > nowMs;
    }

    // A Done names an earlier placement of `drv` on `wid` than the current
    // one: the worker's Hello left the drv out after the Done was made, and the
    // scheduler placed it there again.
    // NOLINTNEXTLINE(bugprone-easily-swappable-parameters): a worker and a drv, named where it is called
    [[nodiscard]] auto namesAnother(WorkerId wid, DrvId drv, uint64_t assignId) const -> bool
    {
        auto const & slot = entries.at(drv);
        return slot && slot->worker == wid && workers.at(wid).namesPlacements && assignId != 0
               && slot->assignId != assignId;
    }

    // Entry is finished or abandoned: free the slot and its id for reuse.
    void retire(DrvId drv)
    {
        auto & slot = entries.at(drv);
        if (!slot) {
            return;
        }
        unkeep(drv, *slot);
        while (!slot->leases.empty()) {
            dropLease(drv, *slot, slot->leases.begin());
        }
        auto const iter = systems.find(slot->system);
        if (iter != systems.end()) {
            iter->second.queue.erase(drv);
            if (iter->second.queue.size() == 0 && !offered(iter->first)) {
                systems.erase(iter);
            }
        }
        drvIds.erase(PathKey(slot->drvPath));
        slot.reset();
        freeIds.push_back(drv);
    }

    [[nodiscard]] auto offered(const std::string & system) const -> bool
    {
        return std::ranges::any_of(
            workers, [&](const Worker & wkr) -> bool { return std::ranges::find(wkr.systems, system) != wkr.systems.end(); });
    }

    void rememberBuilder(std::string_view path, WorkerId wid, uint64_t size)
    {
        const PathKey key(path);
        if (auto const iter = lastBuilder.find(key); iter != lastBuilder.end()) {
            iter->second = {wid, size};
            return;
        }
        if (lastBuilder.size() >= maxLastBuilder) {
            lastBuilder.clear();
        }
        lastBuilder.emplace(key, std::pair(wid, size));
    }

    auto sys(const std::string & name) -> SystemState &
    {
        return systems.try_emplace(name).first->second;
    }

    struct Skipped
    {
        std::string system;
        DrvId drv;
        double key;
    };

    void dispatchHead(const std::string & system, std::vector<Assign> & out, std::vector<Skipped> & skipped)
    {
        auto & queue = sys(system).queue;
        auto const drv = queue.top().value_or(0);
        queue.erase(drv);
        auto & slot = entries.at(drv);
        if (slot && slot->followers.empty() && slot->worker == noWorker && slot->idleSinceMs) {
            // Kept for a client that will be back: in its place, not placed.
            skipped.push_back({.system = system, .drv = drv, .key = prio(*slot)});
            return;
        }
        if (!slot || slot->followers.empty() || slot->worker != noWorker) {
            if (slot && slot->followers.empty() && slot->worker == noWorker) {
                retire(drv); // stale
            }
            return;
        }
        auto wid = pick(*slot);
        if (!wid) {
            skipped.push_back({.system = system, .drv = drv, .key = prio(*slot)});
            return;
        }
        slot->worker = *wid;
        slot->assignId = ++lastAssignId;
        workers.at(*wid).running.push_back(drv);
        refreshFree(*wid);
        out.push_back(Assign{.drv = drv, .worker = *wid, .assignId = slot->assignId, .clients = slot->followers});
    }

    [[nodiscard]] auto freeOf(WorkerId wid) const -> int32_t
    {
        const auto & wkr = workers.at(wid);
        if (wkr.systems.empty()) {
            return 0;
        }
        auto const iter = systems.find(wkr.systems.front());
        return iter == systems.end() ? 0 : iter->second.free.get(wid);
    }

    void refreshFree(WorkerId wid)
    {
        auto const & wkr = workers.at(wid);
        auto const running = static_cast<int32_t>(std::min<size_t>(wkr.running.size(), FreeIndex::maxSlots));
        auto const val = (wkr.up && !wkr.draining) ? wkr.maxJobs - running : 0;
        for (const auto & name : wkr.systems) {
            sys(name).free.set(wid, val);
        }
    }

    [[nodiscard]] auto prio(const Entry & ent) const -> double
    {
        auto const extra = ent.followers.empty() ? 0.0 : static_cast<double>(ent.followers.size() - 1);
        return -(ent.cpHint + (cfg.faninWeight * extra) - (cfg.ageWeight * ent.enqMs));
    }

    [[nodiscard]] static auto offers(const Worker & wkr, const Entry & ent) -> bool
    {
        return std::ranges::find(wkr.systems, ent.system) != wkr.systems.end()
               && std::ranges::all_of(ent.features, [&](const std::string & feat) -> bool {
                      return std::ranges::find(wkr.features, feat) != wkr.features.end();
                  });
    }

    // Most free among workers offering system and features, a faulty one
    // only if no other has a slot. O(1) without features while no worker is
    // faulty, O(workers) otherwise.
    [[nodiscard]] auto emptiestFor(const Entry & ent) const -> std::optional<WorkerId>
    {
        auto const iter = systems.find(ent.system);
        if (iter == systems.end()) {
            return std::nullopt;
        }
        const auto & fidx = iter->second.free;
        if (ent.features.empty() && faultyCount == 0) {
            return fidx.emptiest();
        }
        std::optional<WorkerId> best;
        std::optional<WorkerId> faulty;
        for (WorkerId wid = 0; wid < workers.size(); wid++) {
            const auto & wkr = workers.at(wid);
            auto & pick = wkr.faulty ? faulty : best;
            if (wkr.up && fidx.get(wid) > 0 && offers(wkr, ent) && (!pick || fidx.get(wid) > fidx.get(*pick))) {
                pick = wid;
            }
        }
        return best ? best : faulty;
    }

    [[nodiscard]] static auto avoided(const Entry & ent, WorkerId wid) -> bool
    {
        return std::ranges::find(ent.avoid, wid) != ent.avoid.end();
    }

    // The emptiest worker it was not given back by. If each one that can run
    // it gave it back, the emptiest but the last of them; `fallback` (one it
    // was given back by) if nobody else can run it. A faulty one only if no
    // other has a slot. None while those it may go to have no slot.
    [[nodiscard]] auto avoiding(const Entry & ent, WorkerId fallback) const -> std::optional<WorkerId>
    {
        auto const among = [&](auto passed) -> std::optional<std::optional<WorkerId>> {
            std::optional<WorkerId> best;
            std::optional<WorkerId> faulty;
            bool others = false;
            for (WorkerId wid = 0; wid < workers.size(); wid++) {
                const auto & wkr = workers.at(wid);
                if (passed(wid) || !wkr.up || wkr.draining || !offers(wkr, ent)) {
                    continue;
                }
                others = true;
                auto & pick = wkr.faulty ? faulty : best;
                if (freeOf(wid) > 0 && (!pick || freeOf(wid) > freeOf(*pick))) {
                    pick = wid;
                }
            }
            if (!others) {
                return std::nullopt;
            }
            return best ? best : faulty;
        };
        if (auto const placed = among([&](WorkerId wid) -> bool { return avoided(ent, wid); })) {
            return *placed;
        }
        auto const last = [&](WorkerId wid) -> bool { return !ent.avoid.empty() && wid == ent.avoid.back(); };
        if (auto const placed = among(last)) {
            return *placed;
        }
        return fallback;
    }

    [[nodiscard]] auto pick(const Entry & ent) const -> std::optional<WorkerId>
    {
        auto best = emptiestFor(ent);
        if (best && avoided(ent, *best)) {
            best = avoiding(ent, *best);
        }
        if (!best) {
            return std::nullopt;
        }
        const auto topFree = std::max(1.0, static_cast<double>(freeOf(*best)));
        auto const score = [&](WorkerId wid, double bytes) -> double {
            return bytes + (cfg.locLambda * freeOf(wid) / topFree);
        };
        std::vector<std::pair<WorkerId, double>> cands;
        for (const auto & input : ent.inputs) {
            auto const iter = lastBuilder.find(input);
            if (iter == lastBuilder.end()) {
                continue;
            }
            auto [wid, size] = iter->second;
            if (wid >= workers.size() || avoided(ent, wid) || !workers.at(wid).up || workers.at(wid).faulty
                || freeOf(wid) <= 0 || !offers(workers.at(wid), ent)) {
                continue;
            }
            auto const cand = std::ranges::find_if(cands, [&](auto & cnd) -> bool { return cnd.first == wid; });
            if (cand == cands.end()) {
                cands.emplace_back(wid, static_cast<double>(size));
            } else {
                cand->second += static_cast<double>(size);
            }
        }
        WorkerId picked = *best;
        double bestScore = score(*best, 0);
        for (auto [wid, bytes] : cands) {
            if (auto const scr = score(wid, bytes); scr > bestScore) {
                bestScore = scr;
                picked = wid;
            }
        }
        return picked;
    }
};

} // namespace nixgrpc::sched
