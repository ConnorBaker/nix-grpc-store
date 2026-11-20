#pragma once
// The worker's side of the scheduler protocol: the Expect table that gates
// BuildDerivation and the messages it sends back. No gRPC here, so tests
// drive the same code as the session does.

#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <stop_token>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include <nix/util/sync.hh>

#include "log-spill.hh"
#include "metrics.hh"
#include "nix_remote.pb.h"

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
        static constexpr uint64_t checkMode = 2; // nix::bmCheck

        // The build mode and limits it ran with, and whether it built. Written before `finished`.
        std::array<uint64_t, 3> settings{};
        bool built = false;
        LogSpill spill; // the owner's build log, replayed to whoever attaches

        // Whether a caller asking with `want` may take this result. Outputs are
        // valid whatever limits built them, and Nix reuses them too, except in
        // check mode, where a caller with other limits runs its own check. A
        // failure (a timeout) under other limits, or another mode's result, is
        // not that caller's: it retries and builds with its own settings.
        [[nodiscard]] auto serves(const std::array<uint64_t, 3> & want) const -> bool
        {
            auto const mode = std::get<0>(want);
            return want == settings || (built && mode == std::get<0>(settings) && mode != checkMode);
        }

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
    // Delivers messages to the scheduler, which takes them in one go: it
    // places nothing between them. False if no session is up.
    using SendFn = std::function<bool(const nix::remote::WorkerMsgs &)>;

    // What the worker tells the scheduler about itself.
    struct Config
    {
        std::string advertise;
        std::vector<std::string> systems; // first is the native one
        std::vector<std::string> features;
        uint32_t maxJobs = 0;
        std::function<Clock::time_point()> now = Clock::now;
    };

    Builder(Config config, Metrics & metrics);

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
    // and an upload of its inputs (x-nix-drv); see Call, which pairs it with
    // callEnded(). False if the worker does not expect the drv. `assignId` names the
    // caller's assignment, 0 if it does not say. Waits a short while for an
    // assignment that is still on its way.
    auto claim(const std::string & drvPath, uint64_t assignId = 0) -> bool;
    auto admit(const std::string & drvPath) -> std::optional<Admission>;
    void finished(
        const std::string & drvPath,
        nix::remote::Done::Outcome outcome,
        const std::vector<std::pair<std::string, uint64_t>> & outputs,
        std::string resultWire,
        nix::remote::Done::Reason reason = nix::remote::Done::REASON_UNSPECIFIED);
    [[nodiscard]] auto holding() const -> size_t;

    // Sent on every (re)connect. Reports the builds a client came for and
    // forgets the Expects nobody claimed.
    auto hello() -> nix::remote::WorkerMsg;
    // False if the session is gone.
    auto greet() -> bool;
    // Why this worker takes no new builds. It takes none while any holds.
    enum class Unavailable : uint8_t {
        stopping = 1U << 0U,
        localDaemonDown = 1U << 1U,
    };
    void setUnavailable(Unavailable why, bool holds);
    // `changed` hears whether the worker takes builds, now and on every change.
    void onAvailability(std::function<void(bool)> changed);
    // The `reason` label of nix_grpc_unavailable.
    [[nodiscard]] static constexpr auto name(Unavailable why) -> std::string_view
    {
        switch (why) {
        case Unavailable::stopping:
            return "stopping";
        case Unavailable::localDaemonDown:
            return "local_daemon";
        }
        return "";
    }

    [[nodiscard]] auto draining() const -> bool
    {
        return unavailable_ != 0;
    }

    // Builds taken at once: Config::maxJobs, then the last setMaxJobs().
    [[nodiscard]] auto maxJobs() const -> uint32_t
    {
        return maxJobs_;
    }
    // The capacity changed (nix's max-jobs read anew): tell the scheduler.
    void setMaxJobs(uint32_t maxJobs);
    // Every publish to the cache reports how it went. The scheduler hears
    // when that changes, see Load.publish_failing.
    void published(bool works);

    // Give back Expects nobody came for, or whose client went quiet. Called
    // from the serve loop.
    void tick();

    // The outputs of a drv, all valid in this node's store, with their NAR
    // sizes; nullopt if any is not. It asks the node's nix-daemon, which may
    // not answer: never from the serve loop, which pets the watchdog.
    using BuiltHere = std::function<std::optional<std::vector<std::pair<std::string, uint64_t>>>(const std::string &)>;
    // Before the builder serves.
    void checkBuiltWith(BuiltHere built);
    // Finishes the Expects nobody claimed whose outputs this node's store
    // has (see builder.cc), until `stop`. On a thread of its own.
    void probeBuiltHere(const std::stop_token & stop);
    // One round of that, at the builder's now(): for tests.
    void finishBuiltHere();
    // A call that claimed `drvPath` returns, however it went.
    void callEnded(const std::string & drvPath);

    // A call for a drv, from claim() to callEnded().
    class Call
    {
        Builder & builder;
        std::string drvPath;
        bool expected_;

    public:
        Call(Builder & builder, std::string drvPath, uint64_t assignId = 0)
            : builder(builder)
            , drvPath(std::move(drvPath))
            , expected_(builder.claim(this->drvPath, assignId))
        {
        }

        // The drv was expected here when the call began.
        [[nodiscard]] auto expected() const -> bool
        {
            return expected_;
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
    auto send(const nix::remote::WorkerMsgs & msgs) const -> bool;

private:
    Config config;
    Metrics & metrics;

    struct State
    {
        std::unordered_map<std::string, Expected> expected;
        uint64_t newestExpect = 0; // highest assign_id read in this session
    };

    mutable nix::Sync<State> state;
    mutable nix::Sync<SendFn> sendFn;
    std::atomic<uint8_t> unavailable_{0};
    std::atomic<uint32_t> maxJobs_;
    std::atomic<bool> publishFailing_{false};
    // Orders the Load messages of concurrent setUnavailable() and
    // setMaxJobs() calls, and keeps them behind the Hello of their session.
    std::mutex unavailableMutex;
    std::function<void(bool)> availability_;
    bool greeted_ = false;
    static constexpr std::chrono::seconds expectTimeout{60};
    // How long a call waits for an Expect the scheduler has sent but this
    // worker has not read yet.
    static constexpr std::chrono::seconds expectLag{5};
    std::condition_variable expectArrived;
    BuiltHere builtHere;
    void settleBuiltHere(const std::string & drv, uint64_t assignId);
    // An Expect nobody claimed for this long is asked about (builtHere): a
    // client's own call claims it sooner.
    static constexpr std::chrono::seconds probeAfter{10};

    // Draining and capacity. Under unavailableMutex.
    [[nodiscard]] auto load() const -> nix::remote::WorkerMsg;
    void sendDone(
        const std::string & drvPath,
        uint64_t assignId,
        nix::remote::Done::Outcome outcome,
        const std::vector<std::pair<std::string, uint64_t>> & outputs,
        nix::remote::Done::Reason reason = nix::remote::Done::REASON_UNSPECIFIED);
    void giveBack(const std::vector<std::pair<std::string, uint64_t>> & drvs, nix::remote::Done::Reason reason);
    auto take(const std::function<bool(const Expected &)> & pred) -> std::vector<std::pair<std::string, uint64_t>>;
};

} // namespace nixgrpc
