#include "builder.hh"

#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <stop_token>
#include <string>
#include <utility>
#include <vector>
#include <google/protobuf/repeated_ptr_field.h>

#include "logfmt.hh"
#include "metrics.hh"
#include "nix_remote.pb.h"
#include "scheduler.hh"

namespace nixgrpc {

using nix::remote::WorkerMsg;
using nix::remote::WorkerMsgs;

namespace {
void addFarm(google::protobuf::RepeatedPtrField<nix::remote::FarmMachine> & into, const std::vector<sched::FarmMachine> & farm)
{
    for (const auto & machine : farm) {
        auto * out = into.Add();
        for (const auto & [system, offered] : machine.systems) {
            nix::remote::FarmFeatures features;
            features.mutable_features()->Add(offered.begin(), offered.end());
            out->mutable_systems()->emplace(system, std::move(features));
        }
        out->set_jobs(machine.jobs);
    }
}
} // namespace

Builder::Builder(Config config, Metrics & metrics)
    : config(std::move(config))
    , metrics(metrics)
    , maxJobs_(this->config.maxJobs)
    , farm_(this->config.farm)
{
    for (auto const why : {Unavailable::stopping, Unavailable::localDaemonDown}) {
        metrics.unavailable(name(why), false);
    }
}

void Builder::onExpect(const nix::remote::Expect & exp)
{
    if (exp.drv_path().empty()) {
        return;
    }
    auto const arrived = [&](State & sta) -> void {
        sta.newestExpect = std::max(sta.newestExpect, exp.assign_id());
        expectArrived.notify_all();
    };
    if (draining()) {
        arrived(*state.lock());
        giveBack({{exp.drv_path(), exp.assign_id()}}, nix::remote::Done::DRAINING);
        return;
    }
    auto lck = state.lock();
    arrived(*lck);
    auto & ent = lck->expected[exp.drv_path()];
    // A claimed one runs here already: placed here again (a client bounced
    // off it), or re-announced across a scheduler restart. Its Done names
    // the placement the scheduler holds now, the latest.
    ent.assignId = exp.assign_id();
    if (ent.claimed) {
        return;
    }
    ent.since = config.now();
    metrics.event("expect");
}

auto Builder::claim(const std::string & drvPath, uint64_t assignId) -> bool
{
    auto const deadline = Clock::now() + expectLag;
    auto lck = state.lock();
    for (bool waited = false;; waited = true) {
        if (auto const found = lck->expected.find(drvPath); found != lck->expected.end()) {
            found->second.claimed = true;
            found->second.calls++;
            found->second.since = config.now();
            if (waited) {
                metrics.event("expect_late");
            }
            return true;
        }
        // The scheduler numbers its assignments in the order it sends them,
        // so only a newer one than the last we read can still arrive.
        if (assignId <= lck->newestExpect) {
            return false;
        }
        metrics.event("expect_wait");
        if (lck.wait_until(expectArrived, deadline) == std::cv_status::timeout) {
            return false;
        }
    }
}

void Builder::callEnded(const std::string & drvPath)
{
    auto lck = state.lock();
    if (auto const found = lck->expected.find(drvPath); found != lck->expected.end() && found->second.calls > 0) {
        found->second.calls--;
        found->second.since = config.now();
    }
}

auto Builder::holding() const -> size_t
{
    return state.lock()->expected.size();
}

void Builder::giveBack(const std::vector<std::pair<std::string, uint64_t>> & drvs, nix::remote::Done::Reason reason)
{
    for (const auto & [drv, assignId] : drvs) {
        metrics.event("give_back");
        sendDone(drv, assignId, nix::remote::Done::GIVEN_BACK, {}, reason);
    }
}

void Builder::onRevoke(const nix::remote::Revoke & rev)
{
    uint64_t assignId = 0;
    {
        auto lck = state.lock();
        auto const found = lck->expected.find(rev.drv_path());
        if (found == lck->expected.end()) {
            return;
        }
        if (found->second.running && rev.once_uncalled()) {
            // The build RPC kills the build once no call waits for it.
            found->second.shared->revokeOnceUncalled = true;
            logLine(LogLevel::info, {{"event", "revoke_once_uncalled"}, {"drv", rev.drv_path()}});
            return;
        }
        if (found->second.running) {
            // The build RPC notices, kills the build and reports Done itself.
            found->second.shared->revoked = true;
            logLine(LogLevel::info, {{"event", "revoked_running"}, {"drv", rev.drv_path()}});
            return;
        }
        assignId = found->second.assignId;
        lck->expected.erase(found);
    }
    sendDone(rev.drv_path(), assignId, nix::remote::Done::CANCELLED, {});
}

auto Builder::admit(const std::string & drvPath) -> std::optional<Admission>
{
    auto lck = state.lock();
    auto const found = lck->expected.find(drvPath);
    if (found == lck->expected.end()) {
        return std::nullopt;
    }
    auto & ent = found->second;
    Admission adm{
        .shared = ent.shared,
        .assignId = ent.assignId,
        .attach = ent.running,
        .waiting = ent.running ? Expected::Waiting(ent.shared) : Expected::Waiting()};
    ent.claimed = true;
    ent.running = true;
    return adm;
}

void Builder::finished(
    const std::string & drvPath,
    nix::remote::Done::Outcome outcome,
    const std::vector<std::pair<std::string, uint64_t>> & outputs,
    std::string resultWire,
    nix::remote::Done::Reason reason)
{
    uint64_t assignId = 0;
    std::shared_ptr<Expected::Shared> shared;
    {
        auto lck = state.lock();
        auto const found = lck->expected.find(drvPath);
        if (found != lck->expected.end()) {
            assignId = found->second.assignId;
            shared = found->second.shared;
            lck->expected.erase(found);
        }
    }
    if (shared) {
        const std::scoped_lock lock(shared->mutex);
        shared->finished = true;
        shared->resultWire = std::move(resultWire);
        shared->built = outcome == nix::remote::Done::BUILT;
        shared->cv.notify_all();
    }
    sendDone(drvPath, assignId, outcome, outputs, reason);
}

void Builder::sendDone(
    const std::string & drvPath,
    uint64_t assignId,
    nix::remote::Done::Outcome outcome,
    const std::vector<std::pair<std::string, uint64_t>> & outputs,
    nix::remote::Done::Reason reason)
{
    WorkerMsg msg;
    auto * done = msg.mutable_done();
    done->set_drv_path(drvPath);
    done->set_assign_id(assignId);
    done->set_outcome(outcome);
    done->set_reason(reason);
    for (const auto & [path, size] : outputs) {
        auto * out = done->add_outputs();
        out->set_path(path);
        out->set_nar_size(size);
    }
    if (!send(msg)) {
        // Session down: the next Hello omits this drv, which frees the slot.
        metrics.event("done_unsent");
    }
}

auto Builder::hello() -> WorkerMsg
{
    WorkerMsg msg;
    auto * hel = msg.mutable_hello();
    hel->set_addr(config.advertise);
    for (const auto & system : config.systems) {
        hel->add_systems(system);
    }
    for (const auto & feat : config.features) {
        hel->add_features(feat);
    }
    hel->set_max_jobs(maxJobs_);
    addFarm(*hel->mutable_farm(), farm_);
    hel->set_publish_failing(publishFailing_);
    auto lck = state.lock();
    lck->newestExpect = 0; // a new session, maybe of a new scheduler, counts anew
    // A slot nobody claimed is dropped, not reported: the scheduler would
    // take it for a build and revoke the one its client started elsewhere.
    std::erase_if(lck->expected, [](const auto & item) -> bool { return !item.second.claimed; });
    for (const auto & [drv, ent] : lck->expected) {
        auto * run = hel->add_running();
        run->set_drv_path(drv);
        run->set_assign_id(ent.assignId);
    }
    return msg;
}

auto Builder::greet() -> bool
{
    std::scoped_lock const lock(unavailableMutex);
    // With its drain in the same message: a draining builder is never
    // offered builds on its Hello alone, which it would each give back.
    WorkerMsgs msgs;
    *msgs.add_msgs() = hello();
    if (draining()) {
        *msgs.add_msgs() = load();
    }
    greeted_ = send(msgs);
    return greeted_;
}

auto Builder::load() const -> WorkerMsg
{
    WorkerMsg msg;
    msg.mutable_load()->set_draining(draining());
    msg.mutable_load()->set_max_jobs(maxJobs_);
    addFarm(*msg.mutable_load()->mutable_farm(), farm_);
    msg.mutable_load()->set_publish_failing(publishFailing_);
    return msg;
}

void Builder::published(bool works)
{
    std::scoped_lock const lock(unavailableMutex);
    if (publishFailing_.exchange(!works) == !works) {
        return;
    }
    logLine(LogLevel::info, {{"event", works ? "publishing" : "publish_failing"}});
    if (greeted_) {
        send(load());
    }
}

void Builder::setMaxJobs(uint32_t maxJobs)
{
    std::scoped_lock const lock(unavailableMutex);
    if (maxJobs_.exchange(maxJobs) == maxJobs || !greeted_) {
        return;
    }
    send(load());
}

void Builder::setFarm(std::vector<sched::FarmMachine> farm)
{
    std::scoped_lock const lock(unavailableMutex);
    if (farm == farm_) {
        return;
    }
    farm_ = std::move(farm);
    if (greeted_) {
        send(load());
    }
}

void Builder::onAvailability(std::function<void(bool)> changed)
{
    std::scoped_lock const lock(unavailableMutex);
    availability_ = std::move(changed);
    if (availability_) {
        availability_(!draining());
    }
}

void Builder::setUnavailable(Unavailable why, bool holds)
{
    std::scoped_lock const lock(unavailableMutex);
    auto const bit = std::to_underlying(why);
    bool const was = draining();
    if (holds) {
        unavailable_ |= bit;
    } else {
        unavailable_ &= static_cast<uint8_t>(~bit);
    }
    metrics.unavailable(name(why), holds);
    bool const draining = this->draining();
    if (was != draining && availability_) {
        availability_(!draining);
    }
    if (was == draining || !greeted_) {
        return;
    }
    send(load());
    if (draining) {
        giveBack(take([](const Expected & ent) -> bool { return !ent.claimed; }), nix::remote::Done::DRAINING);
    }
}

auto Builder::take(const std::function<bool(const Expected &)> & pred) -> std::vector<std::pair<std::string, uint64_t>>
{
    std::vector<std::pair<std::string, uint64_t>> taken;
    auto lck = state.lock();
    for (auto iter = lck->expected.begin(); iter != lck->expected.end();) {
        if (pred(iter->second)) {
            taken.emplace_back(iter->first, iter->second.assignId);
            iter = lck->expected.erase(iter);
        } else {
            ++iter;
        }
    }
    return taken;
}

void Builder::setSend(SendFn send)
{
    std::scoped_lock const lock(unavailableMutex);
    greeted_ = false;
    *sendFn.lock() = std::move(send);
}

auto Builder::send(const WorkerMsg & msg) const -> bool
{
    WorkerMsgs one;
    *one.add_msgs() = msg;
    return send(one);
}

auto Builder::send(const WorkerMsgs & msgs) const -> bool
{
    const SendFn cur = *sendFn.lock(); // copy: do not hold the lock across stream I/O
    return cur && cur(msgs);
}

void Builder::checkBuiltWith(BuiltHere built)
{
    builtHere = std::move(built);
}

// A placement nobody claims whose outputs this node's store has, all of
// them, is built: its build ran on this node outside this process, as one
// of a predecessor's for a client that never calls here. Finished as BUILT,
// it is neither given back nor placed again, and it frees its slot. Outputs
// the node had before the placement read the same: a real client calling
// more than probeAfter late (--rebuild, saturated handlers) finds its
// placement already finished, and its call is turned down as for any drv
// not expected here, a bounce (docs/farm.md, Upgrade a single node).
void Builder::probeBuiltHere(const std::stop_token & stop)
{
    std::mutex sleepMutex;
    std::condition_variable_any wake;
    while (!stop.stop_requested()) {
        finishBuiltHere();
        std::unique_lock lock(sleepMutex);
        wake.wait_for(lock, stop, std::chrono::seconds(1), [] -> bool { return false; });
    }
}

void Builder::finishBuiltHere()
{
    if (!builtHere) {
        return;
    }
    auto const now = config.now();
    std::vector<std::pair<std::string, uint64_t>> due;
    {
        auto lck = state.lock();
        for (auto & [drv, ent] : lck->expected) {
            if (!ent.claimed && now - ent.since >= probeAfter) {
                due.emplace_back(drv, ent.assignId);
            }
        }
    }
    // On this thread an exception would end the process, and every build on
    // the node with it. One drv's failure is its own: the others are asked.
    for (const auto & [drv, assignId] : due) {
        try {
            settleBuiltHere(drv, assignId);
        } catch (const std::exception & err) {
            logLine(LogLevel::info, {{"event", "built_here_failed"}, {"drv", drv}, {"error", err.what()}});
        }
    }
}

void Builder::settleBuiltHere(const std::string & drv, uint64_t assignId)
{
    auto const outputs = builtHere(drv);
    if (!outputs) {
        return;
    }
    decltype(State::expected)::node_type held;
    {
        auto lck = state.lock();
        auto const found = lck->expected.find(drv);
        if (found == lck->expected.end() || found->second.claimed || found->second.assignId != assignId) {
            return;
        }
        held = lck->expected.extract(found);
    }
    try {
        sendDone(drv, assignId, nix::remote::Done::BUILT, *outputs);
    } catch (...) {
        // Not sent: held as before, for the next round, unless placed anew.
        state.lock()->expected.insert(std::move(held));
        throw;
    }
    metrics.event("expect_built_here");
}

void Builder::tick()
{
    const auto now = config.now();
    // While draining, also what an Expect racing setUnavailable() left behind.
    bool const draining = this->draining();
    // A claimed one whose client went quiet goes too: no call for it here
    // (BuildDerivation, or an upload of its inputs) for expectTimeout. Its
    // client was sent elsewhere or reached a successor that took this node
    // over, and nobody else ends it.
    giveBack(
        take([&](const Expected & ent) -> bool {
            if (ent.running) {
                return false;
            }
            if (!ent.claimed) {
                return draining || now - ent.since > expectTimeout;
            }
            return ent.calls == 0 && now - ent.since > expectTimeout;
        }),
        draining ? nix::remote::Done::DRAINING : nix::remote::Done::UNREACHABLE);
}

} // namespace nixgrpc
