// Which failures the client retries, and the backoff under a deadline.

#include <cassert>
#include <chrono>
#include <iostream>
#include <latch>
#include <string>
#include <thread>

#include <grpcpp/grpcpp.h>
#include <grpcpp/security/credentials.h>
#include <grpcpp/security/server_credentials.h>
#include <grpcpp/support/status.h>

#include "nix_remote.grpc.pb.h"
#include "nix_remote.pb.h"
#include "retry.hh"

namespace {

auto unavailable(const char * msg) -> grpc::Status { return {grpc::StatusCode::UNAVAILABLE, msg}; }

// Messages as seen in nixbot build logs.
void classifiesFarmErrors()
{
    assert(nixgrpc::goneAway(unavailable(
        "failed to connect to all addresses; last error: UNKNOWN: ipv4:65.21.215.17:50051: handshaker shutdown")));
    assert(nixgrpc::goneAway(unavailable(
        "failed to connect to all addresses; last error: UNKNOWN: ipv4:65.21.215.17:50051: tcp handshaker shutdown")));
    assert(nixgrpc::goneAway(unavailable("failed to connect to all addresses; last error: UNKNOWN: "
                                         "ipv4:65.21.215.17:50051: connection attempt timed out before "
                                         "receiving SETTINGS frame")));
    assert(!nixgrpc::goneAway(unavailable("failed to connect to all addresses; last error: UNKNOWN: "
                                          "ipv4:65.21.215.17:50051: Ssl handshake failed: certificate verify failed")));
    assert(!nixgrpc::goneAway({grpc::StatusCode::PERMISSION_DENIED, "Connection reset"}));
    // Whatever the daemon says with UNAVAILABLE is worth a retry.
    assert(nixgrpc::goneAway(unavailable("publish failed: niks3 push exited with 1")));
    assert(nixgrpc::goneAway(unavailable("no healthy upstream")));
    // A call that reached a daemon as it exited idle, before any handler took it.
    assert(nixgrpc::goneAway({grpc::StatusCode::CANCELLED, ""}));
    assert(!nixgrpc::goneAway({grpc::StatusCode::CANCELLED, "Cancelled by build hook"}));
    assert(nixgrpc::streamBroken({grpc::StatusCode::UNKNOWN, "Stream removed (Socket closed)"}));
}

// What the store calls and FetchNars retry. Schedule asks none of these:
// schedulerGone (build.cc) reconnects on any UNAVAILABLE, UNKNOWN or
// INTERNAL but a TLS failure.
void classifiesWhatAWorkerAnswersApartFromCutOffs()
{
    // As main.cc answers an upload or query whose publish failed.
    auto const publishFailed = unavailable("publish failed: niks3 push exited with status 1");
    assert(nixgrpc::goneAway(publishFailed));
    assert(!nixgrpc::cutOff(publishFailed));
    // Still an answer when the publish error's own text quotes a broken
    // stream: main.cc puts the cache client's message after "publish failed: ".
    assert(!nixgrpc::cutOff(unavailable("node-a: publish failed: niks3 push: Stream removed")));
    assert(nixgrpc::streamBroken({grpc::StatusCode::INTERNAL, "Received RST_STREAM with error code 2"}));
    assert(nixgrpc::streamBroken({grpc::StatusCode::UNKNOWN, "Socket closed"}));
    assert(nixgrpc::cutOff({grpc::StatusCode::UNKNOWN, "Stream removed (Socket closed)"}));
    // As a call ends once the client's own pings go unanswered
    // (client-keepalive-test.py): a store query tries again.
    assert(nixgrpc::goneAway(unavailable("Stream removed (ping timeout)")));
    // The same once gRPC arms its keepalive timeout, which 1.84 never does
    // (writing.cc:797-801, chttp2_transport.cc:2059-2060).
    assert(nixgrpc::goneAway(unavailable("Stream removed (keepalive timeout)")));
    // When the ping's packets are lost, the kernel drops the connection
    // (TCP_USER_TIMEOUT) and the call ends with the socket's error, as gRPC
    // 1.84.0 ended it in a partition that dropped packets; "sendmsg: ..."
    // when a write gets the error first (posix_endpoint.cc:372-375, 1056).
    assert(nixgrpc::goneAway(unavailable("Stream removed (recvmsg:Connection timed out (110))")));
    // A TLS rejection while connecting is no broken stream.
    assert(!nixgrpc::streamBroken(unavailable("failed to connect to all addresses; last error: UNAVAILABLE: "
                                              "ipv4:127.0.0.1:50051: Socket closed")));
    assert(!nixgrpc::cutOff({grpc::StatusCode::PERMISSION_DENIED, "Socket closed"}));
}

// A store query stuck on the server the way the daemon's are when nix does
// not answer: nothing but the test lets it return.
class StuckQuery final : public nix::remote::NixRemote::Service
{
public:
    auto QueryValidPaths(grpc::ServerContext * /*ctx*/, const nix::remote::QueryValidPathsRequest * /*request*/,
                         nix::remote::QueryValidPathsReply * /*reply*/) -> grpc::Status override
    {
        arrived.count_down();
        released.wait();
        return grpc::Status::OK;
    }

    void waitArrived() { arrived.wait(); }
    void release() { released.count_down(); }

private:
    std::latch arrived{1};
    std::latch released{1};
};

// nix-grpc-daemon gives the calls it still serves this long once it stops.
constexpr std::chrono::milliseconds shutdownGrace{100};
// Only so that a call gRPC does not cut off fails the test instead of hanging it.
constexpr std::chrono::seconds callDeadline{10};

// What the client gets from a server that stops while a call of its own is
// stuck in a handler: the server's real answer, not one written down here.
auto callCutOffByAStoppingServer() -> grpc::Status
{
    StuckQuery service;
    grpc::ServerBuilder builder;
    int port = 0;
    builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(), &port);
    builder.RegisterService(&service);
    auto server = builder.BuildAndStart();
    assert(server);
    auto const stub = nix::remote::NixRemote::NewStub(
        grpc::CreateChannel("127.0.0.1:" + std::to_string(port), grpc::InsecureChannelCredentials()));

    grpc::Status status;
    std::jthread client([&] -> void {
        grpc::ClientContext ctx;
        ctx.set_deadline(std::chrono::system_clock::now() + callDeadline);
        nix::remote::QueryValidPathsRequest const request;
        nix::remote::QueryValidPathsReply reply;
        status = stub->QueryValidPaths(&ctx, request, &reply);
    });
    service.waitArrived();
    // As the daemon stops. Shutdown() returns only once the stuck handler has.
    std::jthread stop([&] -> void { server->Shutdown(std::chrono::system_clock::now() + shutdownGrace); });
    client.join();
    service.release();
    stop.join();
    return status;
}

// A worker leaving the farm cuts off the store calls it still serves.
// The client must try again, on another worker, instead of failing.
void callCutOffByALeavingWorkerIsRetried()
{
    constexpr int rounds = 5;
    for (int i = 0; i < rounds; ++i) {
        auto const status = callCutOffByAStoppingServer();
        std::cerr << "cut off: " << status.error_code() << " " << status.error_message() << '\n';
        // The leaving-worker case itself, not some other way the call failed.
        assert(status.error_code() == grpc::StatusCode::UNAVAILABLE);
        assert(status.error_message().contains("Cancelling all calls"));
        assert(nixgrpc::goneAway(status));
        // Also a cut-off, not an answer. That restarts the retry window only
        // for an upload the worker had taken: retrying() never sets
        // `progressed`, so a store query retries within its first window.
        assert(nixgrpc::cutOff(status));
    }
}

// An upload to the assigned worker that ends with one of these sends the
// build to the scheduler again (buildAssigned, recover), as the same answer
// to BuildDerivation does; NOT_FOUND and the like stay errors.
void classifiesAWorkerThatLeft()
{
    for (auto const code :
         {grpc::StatusCode::FAILED_PRECONDITION,
          grpc::StatusCode::ABORTED,
          grpc::StatusCode::UNAVAILABLE,
          grpc::StatusCode::CANCELLED,
          grpc::StatusCode::UNKNOWN,
          grpc::StatusCode::INTERNAL}) {
        assert(nixgrpc::workerLeft({code, "x"}));
    }
    for (auto const code :
         {grpc::StatusCode::NOT_FOUND,
          grpc::StatusCode::PERMISSION_DENIED,
          grpc::StatusCode::UNAUTHENTICATED,
          grpc::StatusCode::INVALID_ARGUMENT,
          grpc::StatusCode::RESOURCE_EXHAUSTED}) {
        assert(!nixgrpc::workerLeft({code, "x"}));
    }
}

} // namespace

auto main() -> int
try {
    classifiesFarmErrors();
    classifiesWhatAWorkerAnswersApartFromCutOffs();
    callCutOffByALeavingWorkerIsRetried();
    classifiesAWorkerThatLeft();
    return 0;
} catch (...) {
    return 1;
}
