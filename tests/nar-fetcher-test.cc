// NarFetcher against in-process servers whose FetchNars fails in scripted ways.

#include <atomic>
#include <cassert>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <latch>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include <grpcpp/channel.h>
#include <grpcpp/grpcpp.h>
#include <grpcpp/security/credentials.h>
#include <grpcpp/security/server_credentials.h>
#include <grpcpp/support/status.h>
#include <zstd.h>

#include <nix/store/path.hh>
#include <nix/util/error.hh>
#include <nix/util/logging.hh>
#include <nix/util/serialise.hh>

#include "nar-fetcher.hh"
#include "retry.hh"
#include "nix_remote.grpc.pb.h"
#include "nix_remote.pb.h"

namespace {

using Handler = std::function<grpc::Status(int call, const nix::remote::FetchNarsRequest &,
                                           grpc::ServerWriter<nix::remote::NarFrame> &)>;

class ScriptedService final : public nix::remote::NixRemote::Service
{
public:
    explicit ScriptedService(Handler handler) : handler(std::move(handler)) {}

    [[nodiscard]] auto calls() const -> int { return callCount; }

    [[nodiscard]] auto requests() -> std::vector<std::vector<std::string>>
    {
        std::scoped_lock const lock(mutex);
        return seen;
    }

    auto FetchNars(
        grpc::ServerContext * /*ctx*/,
        const nix::remote::FetchNarsRequest * request,
        grpc::ServerWriter<nix::remote::NarFrame> * writer) -> grpc::Status override
    {
        {
            std::scoped_lock const lock(mutex);
            seen.emplace_back(request->paths().begin(), request->paths().end());
        }
        return handler(callCount++, *request, *writer);
    }

private:
    Handler handler;
    std::atomic<int> callCount = 0;
    std::mutex mutex;
    std::vector<std::vector<std::string>> seen;
};

struct Fixture
{
    ScriptedService service;
    std::unique_ptr<grpc::Server> server;
    std::shared_ptr<grpc::Channel> channel;

    explicit Fixture(Handler handler) : service(std::move(handler))
    {
        grpc::ServerBuilder builder;
        int port = 0;
        builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(), &port);
        builder.RegisterService(&service);
        server = builder.BuildAndStart();
        assert(server);
        channel = grpc::CreateChannel("127.0.0.1:" + std::to_string(port), grpc::InsecureChannelCredentials());
    }

    auto fetcher(nixgrpc::RetryPolicy retry = {}) -> std::unique_ptr<nixgrpc::NarFetcher>
    {
        return std::make_unique<nixgrpc::NarFetcher>(
            [this] -> std::shared_ptr<grpc::Channel> { return channel; }, "test", 1, std::move(retry));
    }
};

// Without eof, the start of a NAR the stream then breaks off.
auto sendNar(grpc::ServerWriter<nix::remote::NarFrame> & writer, uint32_t index, const std::string & nar, bool eof = true)
    -> void
{
    std::string compressed(ZSTD_compressBound(nar.size()), '\0');
    auto const size = ZSTD_compress(compressed.data(), compressed.size(), nar.data(), nar.size(), 1);
    assert(!ZSTD_isError(size));
    compressed.resize(size);
    nix::remote::NarFrame frame;
    frame.set_path_index(index);
    frame.set_data(compressed);
    frame.set_eof(eof);
    frame.set_nar_hash("sha256-one");
    writer.Write(frame);
}

// The first frame of a stream that resumes, from the NAR `hash`.
auto resumeAt(const nix::remote::FetchNarsRequest & request, grpc::ServerWriter<nix::remote::NarFrame> & writer,
              const std::string & hash = "sha256-one") -> void
{
    nix::remote::NarFrame resumed;
    resumed.set_offset(request.offset());
    resumed.set_nar_hash(hash);
    writer.Write(resumed);
}

auto narOf(const std::string & path) -> std::string { return "nar of " + path; }

// What the build hook sees when the balancer does not finish the TLS
// handshake in time.
auto connectFailed() -> grpc::Status
{
    return {grpc::StatusCode::UNAVAILABLE,
            "failed to connect to all addresses; last error: UNKNOWN: ipv4:127.0.0.1:1: handshaker shutdown"};
}

constexpr std::chrono::seconds retryBudget{10};

auto anyUnavailable() -> nixgrpc::RetryPolicy
{
    return {
        .retryable = [](const grpc::Status & status) -> bool {
            return status.error_code() == grpc::StatusCode::UNAVAILABLE;
        },
        .budget = [] -> std::chrono::milliseconds { return retryBudget; },
    };
}

auto fetch(nixgrpc::NarFetcher & fetcher, const nix::StorePath & path) -> std::string
{
    nix::StringSink sink;
    fetcher.fetchInto(path, sink);
    return sink.s;
}

auto fetchFails(nixgrpc::NarFetcher & fetcher, const nix::StorePath & path) -> bool
{
    try {
        fetch(fetcher, path);
    } catch (nix::Error &) {
        return true;
    }
    return false;
}

auto pathA() -> nix::StorePath { return {"00000000000000000000000000000000-a"}; }
auto pathB() -> nix::StorePath { return {"11111111111111111111111111111111-b"}; }


// A starved client misses the connect deadline once; the copy must survive.
void connectFailureIsRetried()
{
    Fixture fix([](int call, const auto & request, auto & writer) -> grpc::Status {
        if (call < 2) {
            return connectFailed();
        }
        sendNar(writer, 0, narOf(request.paths(0)));
        return grpc::Status::OK;
    });
    auto fetcher = fix.fetcher(anyUnavailable());

    assert(fetch(*fetcher, pathA()) == narOf(std::string(pathA().to_string())));
    assert(fix.service.calls() == 3);
}

// After a mid-stream break, finished paths are kept and only the paths with
// nothing received yet are asked for again.
void midStreamBreakRefetchesUntouchedPaths()
{
    Fixture fix([](int call, const auto & request, auto & writer) -> grpc::Status {
        for (int i = 0; i < request.paths_size(); ++i) {
            sendNar(writer, static_cast<uint32_t>(i), narOf(request.paths(i)));
            if (call == 0) {
                return {grpc::StatusCode::UNAVAILABLE, "Stream removed (Socket closed)"};
            }
        }
        return grpc::Status::OK;
    });
    auto fetcher = fix.fetcher(anyUnavailable());
    fetcher->recordOrder({pathA(), pathB()}, {{pathA(), 2}, {pathB(), 1}});

    assert(fetch(*fetcher, pathA()) == narOf(std::string(pathA().to_string())));
    assert(fetch(*fetcher, pathB()) == narOf(std::string(pathB().to_string())));
    auto const requests = fix.service.requests();
    assert(requests.size() == 2);
    assert(requests.at(1) == std::vector<std::string>{std::string(pathB().to_string())});
}

// Half of a NAR, then the stream breaks; `rest` answers the next call.
auto cutHalfway(const std::string & nar, std::function<void(const nix::remote::FetchNarsRequest &, grpc::ServerWriter<nix::remote::NarFrame> &)> rest) -> Handler
{
    return [nar, rest = std::move(rest)](int call, const auto & request, auto & writer) -> grpc::Status {
        if (call == 0) {
            sendNar(writer, 0, nar.substr(0, nar.size() / 2), false);
            return {grpc::StatusCode::UNAVAILABLE, "Stream removed (Socket closed)"};
        }
        rest(request, writer);
        return grpc::Status::OK;
    };
}

// A path cut off mid-NAR goes on where it ended.
void midPathBreakResumes()
{
    auto const nar = narOf(std::string(pathA().to_string()));
    std::atomic<uint64_t> asked = 0;
    Fixture fix(cutHalfway(nar, [&](const auto & request, auto & writer) -> void {
        asked = request.offset();
        resumeAt(request, writer);
        sendNar(writer, 0, nar.substr(request.offset()));
    }));
    auto fetcher = fix.fetcher(anyUnavailable());

    assert(fetch(*fetcher, pathA()) == nar);
    assert(asked == nar.size() / 2);
}

// The retry may reach another builder, with another NAR of the path, or not
// say which it has: its rest must not be joined to the start.
void otherNarIsRefused()
{
    auto const nar = narOf(std::string(pathA().to_string()));
    for (std::string const hash : {"sha256-two", ""}) {
        Fixture fix(cutHalfway(nar, [&](const auto & request, auto & writer) -> void {
            resumeAt(request, writer, hash);
            sendNar(writer, 0, nar.substr(request.offset()));
        }));
        auto fetcher = fix.fetcher(anyUnavailable());

        assert(fetchFails(*fetcher, pathA()));
        assert(fix.service.calls() == 2);
    }
}

// A stream cut off after longer than the retry budget, twice, still resumes:
// each cut-off attempt that delivered bytes gets the budget anew.
void lateCutsResume()
{
    auto const nar = narOf(std::string(pathA().to_string()));
    constexpr std::chrono::seconds budget{1};
    Fixture fix([&](int call, const auto & request, auto & writer) -> grpc::Status {
        resumeAt(request, writer);
        if (call == 2) {
            sendNar(writer, 0, nar.substr(request.offset()));
            return grpc::Status::OK;
        }
        sendNar(writer, 0, nar.substr(request.offset(), 1), false);
        std::this_thread::sleep_for(budget * 3 / 2);
        return {grpc::StatusCode::UNAVAILABLE, "Stream removed (Socket closed)"};
    });
    auto fetcher = fix.fetcher({.retryable = anyUnavailable().retryable,
                                .budget = [=] -> std::chrono::milliseconds { return budget; }});

    assert(fetch(*fetcher, pathA()) == nar);
    assert(fix.service.calls() == 3);
}

// A server that does not say it resumes, as one from before offsets, would
// send the start again.
void serverThatStartsOverIsRefused()
{
    auto const nar = narOf(std::string(pathA().to_string()));
    Fixture fix(cutHalfway(nar, [&](const auto &, auto & writer) -> void { sendNar(writer, 0, nar); }));
    auto fetcher = fix.fetcher(anyUnavailable());

    assert(fetchFails(*fetcher, pathA()));
    assert(fix.service.calls() == 2);
}

void nonRetryableFailsAtOnce()
{
    Fixture fix([](int, const auto &, auto &) -> grpc::Status {
        return {grpc::StatusCode::PERMISSION_DENIED, "no"};
    });
    auto fetcher = fix.fetcher(anyUnavailable());

    assert(fetchFails(*fetcher, pathA()));
    assert(fix.service.calls() == 1);
}

constexpr size_t siblings = 20000;

// One session for A, many siblings and B. Its first call breaks off all but
// B mid-NAR if `cutOff` and ends with `first`; later calls deliver. Once A
// failed, path `next` of that order must be fetched anew, not get the failure
// that was pending for it: the siblings keep the session failing its paths
// long enough for it to be asked for meanwhile.
void freshAfterFailure(const grpc::Status & first, bool cutOff, nixgrpc::RetryPolicy retry, size_t next)
{
    Fixture fix([&](int call, const auto & request, auto & writer) -> grpc::Status {
        for (int i = 0; i < request.paths_size(); ++i) {
            if (call > 0) {
                sendNar(writer, static_cast<uint32_t>(i), narOf(request.paths(i)));
            } else if (cutOff && i + 1 < request.paths_size()) {
                sendNar(writer, static_cast<uint32_t>(i), "part", false);
            }
        }
        return call > 0 ? grpc::Status::OK : first;
    });
    auto fetcher = fix.fetcher(std::move(retry));
    std::vector<nix::StorePath> order{pathA()};
    for (size_t i = 0; i < siblings; ++i) {
        auto const hash = std::to_string(i);
        order.emplace_back(std::string(pathA().hashPart().size() - hash.size(), '0') + hash + "-sibling");
    }
    order.push_back(pathB());
    fetcher->recordOrder(order, {});

    assert(fetchFails(*fetcher, pathA()));
    assert(!fetchFails(*fetcher, order.at(next)));
}

// A failed fetch is not cached: nix repl keeps the store after ^C and asks
// for the same path again. Nor are the paths a failed session never
// delivered, also those it cut off, and also when it retries the rest.
void failedFetchIsRetried()
{
    grpc::Status const denied{grpc::StatusCode::PERMISSION_DENIED, "no"};
    freshAfterFailure(denied, false, {}, 0);
    freshAfterFailure(denied, false, {}, siblings + 1);
    freshAfterFailure(denied, true, {}, siblings + 1);
    freshAfterFailure({grpc::StatusCode::UNAVAILABLE, "Stream removed (Socket closed)"}, true, anyUnavailable(), siblings);
}

// Each unannounced path gets a session; finished ones must not pile up.
void finishedSessionsGo()
{
    Fixture fix([](int, const auto & request, auto & writer) -> grpc::Status {
        sendNar(writer, 0, narOf(request.paths(0)));
        return grpc::Status::OK;
    });
    auto fetcher = fix.fetcher();
    for (int i = 0; i < 3; ++i) {
        assert(fetch(*fetcher, pathA()) == narOf(std::string(pathA().to_string())));
    }
    // The last thread may still be on its way out, and says so to nobody.
    constexpr std::chrono::milliseconds poll{10};
    while (fetcher->sessionCount() > 0) {
        std::this_thread::sleep_for(poll);
    }
}

// What the fetcher logs.
class Recorder final : public nix::Logger
{
public:
    void log(nix::Verbosity /*lvl*/, std::string_view msg) noexcept override
    {
        std::scoped_lock const lock(mutex);
        lines.emplace_back(msg);
    }

    void logEI(const nix::ErrorInfo & /*info*/) noexcept override {}

    [[nodiscard]] auto logged() -> std::vector<std::string>
    {
        std::scoped_lock const lock(mutex);
        return lines;
    }

private:
    std::mutex mutex;
    std::vector<std::string> lines;
};

// A fetcher destroyed while a session still waits for a path cancels that
// call itself. That ends it CANCELLED "Stream removed (CANCELLED)", which
// the store's policy takes for a broken stream, or with a bare "CANCELLED"
// before the stream started, which it takes for a daemon gone away
// (retry.hh). The session gives up without retrying, or saying it does.
void destroyedFetcherGivesUpQuietly()
{
    std::latch released{1};
    Fixture fix([&](int, const auto & request, auto & writer) -> grpc::Status {
        sendNar(writer, 0, narOf(request.paths(0)));
        released.wait();
        return grpc::Status::OK;
    });
    Recorder recorder;
    auto * const saved = nix::logger;
    nix::logger = &recorder;
    auto fetcher = fix.fetcher({
        // As the store's (store.cc).
        .retryable = [](const grpc::Status & status) -> bool {
            return nixgrpc::goneAway(status) || nixgrpc::streamBroken(status);
        },
        .budget = [] -> std::chrono::milliseconds { return retryBudget; },
    });
    fetcher->recordOrder({pathA(), pathB()}, {});
    assert(fetch(*fetcher, pathA()) == narOf(std::string(pathA().to_string())));
    fetcher.reset();
    released.count_down();
    nix::logger = saved;
    assert(recorder.logged().empty());
    assert(fix.service.calls() == 1);
}

} // namespace

auto main() -> int
try {
    failedFetchIsRetried();
    connectFailureIsRetried();
    midStreamBreakRefetchesUntouchedPaths();
    midPathBreakResumes();
    lateCutsResume();
    otherNarIsRefused();
    serverThatStartsOverIsRefused();
    nonRetryableFailsAtOnce();
    finishedSessionsGo();
    destroyedFetcherGivesUpQuietly();
    return 0;
} catch (...) {
    return 1;
}
