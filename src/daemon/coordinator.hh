#pragma once
// Scheduler gRPC service, the builder's session to it (in-process or remote),
// and the Expect table that gates BuildDerivation.

#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include <grpcpp/grpcpp.h>
#include <grpcpp/health_check_service_interface.h>
#include <grpcpp/support/sync_stream.h>

#include <nix/store/store-api.hh>
#include <nix/util/sync.hh>
#include <nix/util/types.hh>

#include "auth.hh"
#include "cache.hh"
#include "dispatcher.hh"
#include "eds.hh"
#include "elector.hh"
#include "idle.hh"
#include "log-spill.hh"
#include "metrics.hh"
#include "nix_remote.grpc.pb.h"
#include "nix_remote.pb.h"
#include "options.hh"

namespace nixgrpc {

using Clock = std::chrono::steady_clock;

// One assignment the builder accepted from its scheduler.
struct Expected
{
    uint64_t assignId = 0;
    Clock::time_point since;
    // A client reached us for it. The worker then holds the lease until Done,
    // a give-back or a Revoke: the scheduler revokes it if its client leaves.
    bool claimed = false;
    bool running = false;
    uint32_t calls = 0; // calls for it in flight (Call); `since` is when the last one began or ended

    // Second BuildDerivation for the same drv attaches here.
    struct Shared
    {
        std::mutex mutex;
        std::condition_variable cv;
        std::atomic<bool> revoked{false}; // scheduler moved the drv to a worker further along, or nobody wants it
        bool finished = false;
        std::string resultWire; // BuildDerivationDone serialised
        // Written before `finished`.
        std::array<uint64_t, 3> settings{};
        LogSpill spill; // the owner's build log, replayed to whoever attaches

        // Owner only. Waking under the lock keeps an attacher that just
        // checked `written()` from sleeping through this record.
        void logged(SpillKind kind, std::string_view text)
        {
            spill.append(kind, text);
            const std::scoped_lock lock(mutex);
            cv.notify_all();
        }
    };

    std::shared_ptr<Shared> shared = std::make_shared<Shared>();
};

// Builder side: Expect table + outgoing session.
class Builder
{
public:
    // Delivers a message to the scheduler. False if no session is up.
    using SendFn = std::function<bool(const nix::remote::WorkerMsg &)>;

    Builder(const Options & options, Metrics & metrics);

    // From the scheduler.
    void onExpect(const nix::remote::Expect & exp);
    void onRevoke(const nix::remote::Revoke & rev);

    // BuildDerivation entry: nullopt => FAILED_PRECONDITION. `attach` set if
    // the drv already runs. Caller then waits on it instead of building.
    struct Admission
    {
        std::shared_ptr<Expected::Shared> shared;
        uint64_t assignId = 0;
        bool attach = false;
    };

    // A call for the drv: any BuildDerivation, also one answered NOT_FOUND,
    // and an upload of its inputs (x-nix-drv).
    void claim(const std::string & drvPath);
    auto admit(const std::string & drvPath) -> std::optional<Admission>;
    void finished(
        const std::string & drvPath,
        nix::remote::Done::Outcome outcome,
        const std::vector<std::pair<std::string, uint64_t>> & outputs,
        std::string resultWire,
        nix::remote::Done::Reason reason = nix::remote::Done::REASON_UNSPECIFIED);
    [[nodiscard]] auto holding() const -> size_t;

    // Hello reflecting current state. Sent on every (re)connect.
    auto hello() const -> nix::remote::WorkerMsg;
    // False if the session is gone.
    auto greet() const -> bool;
    void setDraining(bool draining);
    // Every publish to the cache reports how it went. The scheduler hears
    // when that changes, see Load.publish_failing.
    void published(bool works);

    [[nodiscard]] auto draining() const -> bool
    {
        return draining_;
    }

    // Builds taken at once: --max-jobs, else nix's max-jobs as last read.
    [[nodiscard]] auto maxJobs() const -> unsigned
    {
        return maxJobs_;
    }

    // SIGHUP: without --max-jobs, read nix's max-jobs anew and hand the
    // scheduler the new capacity. Called from the serve loop.
    void reloadNixConfig();

    // Give back Expects nobody came for, or whose client went quiet. Called
    // from the serve loop; `now` for tests.
    void tick();
    void tick(Clock::time_point now);
    // A call that claimed `drvPath` returns, however it went.
    void callEnded(const std::string & drvPath);

    // A call for a drv, from claim() to callEnded().
    class Call
    {
        Builder & builder;
        std::string drvPath;

    public:
        Call(Builder & builder, std::string drvPath)
            : builder(builder)
            , drvPath(std::move(drvPath))
        {
            builder.claim(this->drvPath);
        }

        Call(const Call &) = delete;
        Call(Call &&) = delete;
        auto operator=(const Call &) -> Call & = delete;
        auto operator=(Call &&) -> Call & = delete;

        ~Call()
        {
            builder.callEnded(drvPath);
        }
    };

    // Swapped by the session thread on every (re)connect.
    void setSend(SendFn send);
    auto send(const nix::remote::WorkerMsg & msg) const -> bool;

private:
    const Options & options;
    Metrics & metrics;

    struct State
    {
        std::unordered_map<std::string, Expected> expected;
    };

    mutable nix::Sync<State> state;
    mutable nix::Sync<SendFn> sendFn;
    std::atomic<bool> draining_{false};
    std::atomic<bool> publishFailing{false};
    std::atomic<unsigned> maxJobs_;
    static constexpr std::chrono::seconds expectTimeout{60};

    // Draining and capacity, after Hello. False if the session is gone.
    auto sendLoad() const -> bool;
    void sendDone(
        const std::string & drvPath,
        uint64_t assignId,
        nix::remote::Done::Outcome outcome,
        const std::vector<std::pair<std::string, uint64_t>> & outputs,
        nix::remote::Done::Reason reason = nix::remote::Done::REASON_UNSPECIFIED);
    void giveBack(const std::vector<std::pair<std::string, uint64_t>> & drvs, nix::remote::Done::Reason reason);
    auto take(const std::function<bool(const Expected &)> & pred) -> std::vector<std::pair<std::string, uint64_t>>;
};

// gRPC front for the Dispatcher: auth, then pump the stream into it.
class SchedulerService final : public nix::remote::Scheduler::CallbackService
{
public:
    SchedulerService(Dispatcher & dispatcher, Auth & auth, IdleTracker & idle);

    auto Schedule(grpc::CallbackServerContext * context)
        -> grpc::ServerBidiReactor<nix::remote::ClientMsgs, nix::remote::SchedMsgs> * override;
    auto WorkerSession(grpc::CallbackServerContext * context)
        -> grpc::ServerBidiReactor<nix::remote::WorkerMsgs, nix::remote::SchedCmds> * override;

    void setActive(bool active)
    {
        active_ = active;
    }
    static auto passive() -> grpc::Status
    {
        return {grpc::StatusCode::UNAVAILABLE, "scheduler passive, another node serves"};
    }

private:
    Dispatcher * dispatcher;
    Auth * auth;
    IdleTracker * idle;
    std::atomic<bool> active_{true};
};

// Owns the pieces and wires loopback vs. remote session.
class Coordinator
{
public:
    Coordinator(const Options & options, Auth & auth, Metrics & metrics, IdleTracker & idle);
    ~Coordinator();
    Coordinator(const Coordinator &) = delete;
    Coordinator(Coordinator &&) = delete;
    auto operator=(const Coordinator &) -> Coordinator & = delete;
    auto operator=(Coordinator &&) -> Coordinator & = delete;

    // NOLINTBEGIN(*-non-private-member-variables-in-classes): the RPC handlers
    // use these directly.
    Cache cache;
    std::optional<Builder> builder;
    std::optional<Dispatcher> dispatcher;
    std::unique_ptr<SchedulerService> scheduler;
    std::unique_ptr<EdsService> eds;
    // NOLINTEND(*-non-private-member-variables-in-classes)

    void start(grpc::Server & server);
    // From the serve loop, ~1 Hz. `stopping`: after a SIGTERM or a takeover,
    // when the builder drains for good and --min-free no longer moves it.
    void tick(bool stopping);
    // Before Shutdown(): peers of the scheduler are told to reconnect rather
    // than left to find a dead stream.
    void restarting();

private:
    const Options & options;
    std::jthread sessionThread; // remote WorkerSession client
    std::optional<Elector> elector;
    grpc::HealthCheckServiceInterface * health = nullptr;
    std::atomic<bool> lastHealthy{true};
    void setSchedulerActive(bool active);
    void runRemoteSession(Builder & bld, const std::stop_token & stop);
};

} // namespace nixgrpc
