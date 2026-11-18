#include "coordinator.hh"
#include "dispatcher.hh"
#include "idle.hh"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <initializer_list>
#include <memory>
#include <mutex>
#include <stop_token>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_set>
#include <utility>
#include <vector>

#include <sys/statvfs.h>

#include <grpcpp/client_context.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/health_check_service_interface.h>
#include <grpcpp/security/credentials.h>
#include <grpcpp/security/tls_certificate_provider.h>
#include <grpcpp/security/tls_credentials_options.h>
#include <grpcpp/server.h>
#include <grpcpp/server_context.h>
#include <grpcpp/support/server_callback.h>
#include <grpcpp/support/channel_arguments.h>
#include <grpcpp/support/status.h>
#include <grpcpp/support/sync_stream.h>

#include <nix/store/globals.hh>
#include <nix/util/error.hh>
#include <nix/util/file-system.hh>

#include "acl.hh"
#include "auth.hh"
#include "builder.hh"
#include "cache.hh"
#include "client/channel.hh"
#include "client/retry-pause.hh"
#include "daemon-probe.hh"
#include "logfmt.hh"
#include "metrics.hh"
#include "nix-compat.hh"
#include "nix_remote.grpc.pb.h"
#include "nix_remote.pb.h"
#include "options.hh"
#include "sched-reactor.hh"
#include "scheduler.hh"

namespace nixgrpc {

using nix::remote::ClientMsgs;
using nix::remote::SchedCmd;
using nix::remote::SchedCmds;
using nix::remote::SchedMsgs;
using nix::remote::WorkerMsg;
using nix::remote::WorkerMsgs;

namespace {
// 0 (nix: no local builds) still takes one.
auto nixMaxJobs() -> unsigned
{
    return std::clamp(nixcompat::configuredMaxBuildJobs(), 1U, static_cast<unsigned>(sched::FreeIndex::maxSlots));
}
} // namespace

// ---------------------------------------------------------- SchedulerService

SchedulerService::SchedulerService(Dispatcher & dispatcher, Auth & auth, IdleTracker & idle)
    : dispatcher(&dispatcher)
    , auth(&auth)
    , idle(&idle)
{
}


auto SchedulerService::Schedule(grpc::CallbackServerContext * context)
    -> grpc::ServerBidiReactor<ClientMsgs, SchedMsgs> *
{
    if (!active_) {
        return new RejectReactor<ClientMsgs, SchedMsgs>(passive()); // NOLINT(cppcoreguidelines-owning-memory): deletes itself in OnDone
    }
    auto const caller = auth->identify(*context);
    if (auto const status = auth->authorize(caller, "Schedule", Role::write); !status.ok()) {
        return new RejectReactor<ClientMsgs, SchedMsgs>(status); // NOLINT(cppcoreguidelines-owning-memory)
    }
    return new ScheduleReactor(context, *dispatcher, *idle, caller.name); // NOLINT(cppcoreguidelines-owning-memory)
}

auto SchedulerService::WorkerSession(grpc::CallbackServerContext * context)
    -> grpc::ServerBidiReactor<WorkerMsgs, SchedCmds> *
{
    if (!active_) {
        return new RejectReactor<WorkerMsgs, SchedCmds>(passive()); // NOLINT(cppcoreguidelines-owning-memory)
    }
    auto caller = auth->identify(*context);
    if (auto const status = auth->authorize(caller, "WorkerSession", Role::trusted); !status.ok()) {
        return new RejectReactor<WorkerMsgs, SchedCmds>(status); // NOLINT(cppcoreguidelines-owning-memory)
    }
    logLine(LogLevel::info, {{"event", "worker_session_open"}, {"cn", caller.name}, {"peer", context->peer()}});
    return new WorkerSessionReactor(context, *dispatcher, *idle); // NOLINT(cppcoreguidelines-owning-memory)
}

// -------------------------------------------------------------- Coordinator

namespace {
// TLS unless the URL says http://. Our server cert doubles as client cert,
// --scheduler-token-file adds a bearer: either, both or neither. The address
// is often the balancer with a public certificate, so trust the system
// bundle as well as the farm CA.
auto schedulerCreds(const Options & options) -> std::shared_ptr<grpc::ChannelCredentials>
{
    if (options.schedulerAddr.starts_with(plaintextScheme)) {
        return grpc::InsecureChannelCredentials();
    }
    std::string roots;
    if (auto const bundle = defaultCaCert(); !bundle.empty()) {
        roots = nix::readFile(bundle);
    }
    if (!options.clientCA.empty()) {
        roots += "\n" + nix::readFile(options.clientCA);
    }
    grpc::experimental::TlsChannelCredentialsOptions tls;
    if (!roots.empty()) {
        auto const rootProvider = std::make_shared<grpc::experimental::InMemoryCertificateProvider>();
        if (auto const status = rootProvider->UpdateRoot(roots); !status.ok()) {
            throw nix::Error("loading CA certificates: %s", status.ToString());
        }
        tls.set_root_certificate_provider(rootProvider);
    }
    if (!options.tlsCert.empty()) {
        tls.set_identity_certificate_provider(std::make_shared<grpc::experimental::FileWatcherCertificateProvider>(
            options.tlsKey, options.tlsCert, certRefreshSeconds));
    }
    std::shared_ptr<grpc::ChannelCredentials> creds = grpc::experimental::TlsCredentials(tls);
    if (!options.schedulerTokenFile.empty()) {
        creds = grpc::CompositeChannelCredentials(
            creds,
            grpc::MetadataCredentialsFromPlugin(std::make_unique<TokenFileCredentials>(options.schedulerTokenFile)));
    }
    return creds;
}
} // namespace


void Coordinator::setSchedulerActive(bool active)
{
    if (health != nullptr) {
        health->SetServingStatus("nix.scheduler", active);
    }
    scheduler->setActive(active);
    eds->setActive(active);
    if (active) {
        dispatcher->serving(); // NOLINT(bugprone-unchecked-optional-access): set iff scheduler
    } else {
        dispatcher->restarting(); // NOLINT(bugprone-unchecked-optional-access)
    }
}

void Coordinator::restarting()
{
    if (!dispatcher) {
        return;
    }
    elector.reset();
    setSchedulerActive(false);
    logLine(LogLevel::info, {{"event", "scheduler_restarting"}});
    // No wait for the writes to flush: Shutdown() lets in-flight writes
    // finish, and a peer that misses the message just reconnects the old way.
}

Coordinator::Coordinator(const Options & options, Auth & auth, Metrics & metrics, IdleTracker & idle)
    : cache(options.niks3)
    , options(options)
    , metrics(metrics)
{
    if (options.builder) {
        Builder::Config self{
            .advertise = options.advertise,
            .systems = {nix::settings.thisSystem.get()},
            .features = {},
            .maxJobs = options.maxJobs != 0 ? options.maxJobs : nixMaxJobs()};
        self.systems.append_range(nix::settings.extraPlatforms.get());
        self.systems.append_range(options.extraSystems);
        self.features.append_range(nix::settings.systemFeatures.get());
        builder.emplace(std::move(self), metrics);
        metrics.buildSlots(builder->maxJobs());
    }
    if (options.scheduler) {
        Dispatcher::PresentFn present;
        if (cache.hasRemote()) {
            present = [this](const std::vector<std::string> & keys) -> std::unordered_set<std::string> {
                return cache.holds(keys);
            };
        }
        dispatcher.emplace(
            Dispatcher::Config{
                .defaultSystem = nix::settings.thisSystem.get(),
                .present = std::move(present),
                .lookupThreads =
                    options.lookupThreads != 0 ? options.lookupThreads : Dispatcher::Config::defaultLookupThreads,
                .logLevel = options.logLevel,
                .maxLingerMs = static_cast<double>(std::chrono::milliseconds(options.maxLinger).count())},
            metrics);
        scheduler = std::make_unique<SchedulerService>(*dispatcher, auth, idle);
        eds = std::make_unique<EdsService>(*dispatcher, auth, idle);
    }
    if (builder && dispatcher && options.schedulerAddr.empty()) {
        auto const worker = dispatcher->loopbackWorker([this](const SchedCmd & cmd) -> void {
            if (cmd.has_expect()) {
                builder->onExpect(cmd.expect());
            } else if (cmd.has_revoke()) {
                builder->onRevoke(cmd.revoke());
            }
        });
        builder->setSend([this, worker](const WorkerMsg & msg) -> bool {
            WorkerMsgs one;
            *one.add_msgs() = msg;
            dispatcher->workerMsgs(*worker, one);
            return true;
        });
    }
}

// Members would go in reverse declaration order: session thread and
// dispatcher after the Builder they call into. Cut the edges first.
Coordinator::~Coordinator()
{
    elector.reset();
    if (probeThread.joinable()) {
        probeThread.request_stop();
        probeThread.join();
    }
    if (sessionThread.joinable()) {
        sessionThread.request_stop();
        sessionThread.join();
    }
    if (builder) {
        builder->setSend(nullptr);
    }
    eds.reset();
    scheduler.reset();
    dispatcher.reset();
}

void Coordinator::start(grpc::Server & server)
{
    health = server.GetHealthCheckService();
    if (scheduler) {
        // A node that schedules only onto itself has no peers to elect among.
        const bool elect = options.niks3.enabled() && (!builder || !options.schedulerAddr.empty());
        setSchedulerActive(!elect);
        if (elect) {
            // The leader gauge is sampled, so a lead that moves and moves back
            // between two scrapes would leave no trace.
            metrics.declareEvent("scheduler_take_over");
            metrics.declareEvent("scheduler_yield");
            elector.emplace(*cache.client(), [this](bool lead) -> void {
                metrics.event(lead ? "scheduler_take_over" : "scheduler_yield");
                setSchedulerActive(lead);
            });
        }
    }
    if (!builder) {
        return;
    }
    // The first answer comes before the worker offers itself to a scheduler.
    auto & bld = *builder;
    askLocalDaemon(bld);
    probeThread = std::jthread([this, &bld](const std::stop_token & stop) -> void { watchLocalDaemon(bld, stop); });
    if (options.schedulerAddr.empty()) {
        bld.greet();
    } else {
        sessionThread = std::jthread([this, &bld](const std::stop_token & stop) -> void { runRemoteSession(bld, stop); });
    }
}

void Coordinator::runRemoteSession(Builder & bld, const std::stop_token & stop)
{
    auto const creds = schedulerCreds(options);
    auto const args = daemonChannelArgs();
    auto const target = options.schedulerAddr.starts_with(plaintextScheme) ? options.schedulerAddr.substr(plaintextScheme.size())
                                                                    : options.schedulerAddr;
    auto const channel = grpc::CreateCustomChannel(target, creds, args);
    auto stub = nix::remote::Scheduler::NewStub(channel);
    unsigned retry = 1;
    while (!stop.stop_requested()) {
        grpc::ClientContext ctx;
        auto stream = stub->WorkerSession(&ctx);
        auto const writeMutex = std::make_shared<std::mutex>();
        auto const live = std::make_shared<std::atomic<bool>>(true);
        auto * raw = stream.get();
        // Builders send a Done every few seconds at most. No batching needed.
        bld.setSend([raw, writeMutex, live](const WorkerMsg & msg) -> bool {
            WorkerMsgs one;
            *one.add_msgs() = msg;
            const std::scoped_lock lock(*writeMutex);
            return *live && raw->Write(one);
        });
        const bool helloed = bld.greet();
        if (helloed) {
            logLine(LogLevel::info, {{"event", "scheduler_connected"}, {"addr", options.schedulerAddr}});
            retry = 1;
        }
        const std::stop_callback onStop(stop, [&ctx] -> void { ctx.TryCancel(); });
        SchedCmds cmds;
        bool restarting = false;
        while (helloed && stream->Read(&cmds)) {
            for (const auto & cmd : cmds.msgs()) {
                if (cmd.has_expect()) {
                    bld.onExpect(cmd.expect());
                } else if (cmd.has_revoke()) {
                    bld.onRevoke(cmd.revoke());
                } else if (cmd.has_restarting()) {
                    restarting = true;
                }
            }
            if (restarting) {
                // Leave now. A yielding scheduler keeps the connection open.
                ctx.TryCancel();
            }
        }
        *live = false;
        auto const status = stream->Finish();
        if (stop.stop_requested()) {
            return;
        }
        if (restarting) {
            // Announced: go straight back, the successor is (about to be) up.
            logLine(LogLevel::info, {{"event", "scheduler_restarting"}, {"addr", options.schedulerAddr}});
            retry = 1;
        } else {
            logLine(
                LogLevel::info,
                {{"event", "scheduler_disconnected"}, {"addr", options.schedulerAddr}, {"error", status.error_message()}});
        }
        std::this_thread::sleep_for(retryPause(retry++));
    }
}

// A nix-daemon that stops answering blocks every call that reaches it, and
// no error tells the scheduler. So ask it, and take no builds meanwhile.
void Coordinator::askLocalDaemon(Builder & bld)
{
    constexpr std::chrono::seconds patience{5};
    bool const answers = nixDaemonAnswers(options.socketPath, patience);
    if (answers == daemonAnswered) {
        return;
    }
    daemonAnswered = answers;
    logLine(
        LogLevel::info,
        {{"event", answers ? "healthy" : "unhealthy"},
         {"reason", std::string(Builder::name(Builder::Unavailable::localDaemonDown))}});
    bld.setUnavailable(Builder::Unavailable::localDaemonDown, !answers);
}

void Coordinator::watchLocalDaemon(Builder & bld, const std::stop_token & stop)
{
    constexpr std::chrono::seconds every{10};
    std::mutex mutex;
    std::condition_variable_any wake;
    std::unique_lock lock(mutex);
    while (!wake.wait_for(lock, stop, every, [&] -> bool { return stop.stop_requested(); })) {
        askLocalDaemon(bld);
    }
}

// As an unprivileged user sees it, which runs out before root's reserve. A
// filesystem that reports no blocks or inodes at all (ramfs) has no limit.
auto Coordinator::outOfSpace(uint64_t need) const -> bool
{
    struct statvfs vfs{};
    if (statvfs(options.storeDir.c_str(), &vfs) != 0) {
        return false;
    }
    auto const avail = static_cast<uint64_t>(vfs.f_bavail) * vfs.f_frsize;
    return (vfs.f_blocks > 0 && need >= avail) || (vfs.f_files > 0 && vfs.f_favail == 0);
}

void Coordinator::tick()
{
    if (!builder) {
        return;
    }
    builder->tick();
}

// Whoever rewrites nix's configuration signals us; a Tribuchet hub rewrites
// max-jobs, in a file nix.conf includes, whenever a worker joins or leaves.
void Coordinator::reloadNixConfig()
{
    if (!builder || options.maxJobs != 0) {
        return;
    }
    unsigned jobs = 0;
    try {
        jobs = nixMaxJobs();
    } catch (nix::Error & err) {
        logLine(LogLevel::info, {{"event", "max_jobs_unreadable"}, {"error", err.msg()}});
        return;
    }
    auto const was = builder->maxJobs();
    logLine(LogLevel::info, {{"event", "max_jobs"}, {"from", std::to_string(was)}, {"to", std::to_string(jobs)}});
    if (was != jobs) {
        metrics.buildSlots(jobs);
        builder->setMaxJobs(jobs);
    }
}

} // namespace nixgrpc
