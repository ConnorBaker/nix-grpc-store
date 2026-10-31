#pragma once
// Command line of nix-grpc-daemon.

#include <chrono>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include <sys/types.h>

#include <grpcpp/security/server_credentials.h>

#include "acl.hh"
#include "niks3-client.hh"
#include "logfmt.hh"
#include "xfcc.hh"

namespace nixgrpc {

struct Options
{
    std::string listen = "0.0.0.0:50051";
    std::optional<std::chrono::seconds> idleTimeout;
    std::string socketPath = "/nix/var/nix/daemon-socket/socket";
    // Store URI for the native bulk RPCs. Defaults to the proxy socket.
    std::string storeUri;
    std::string tlsCert;
    std::string tlsKey;
    std::string clientCA;
    std::string metricsListen;
    std::string workerName;
    LogLevel logLevel = LogLevel::info;
    Acl acl;
    xfcc::TrustedProxies proxies;
    std::string oidcConfig;
    Niks3Config niks3;

    bool builder = true;
    bool scheduler = true;
    // Empty: in-process scheduler. Else the balancer URL (host:port) to dial.
    std::string schedulerAddr;
    std::string schedulerTokenFile; // bearer towards a remote scheduler, optional
    // How the balancer reaches this worker. Goes into Assigned.worker_addr.
    std::string advertise;
    unsigned maxJobs = 0; // 0 = nix's max-jobs, read at start and on SIGHUP (Builder::reloadNixConfig)
    uint64_t minFree = 0;
    std::string storeDir;
    // --take-over: start as the successor of this main process, see takeover.hh.
    std::optional<pid_t> takeOverFrom;
    bool sandbox = false;
    // Extra subtrees the sandbox lets the daemon write.
    std::vector<std::string> sandboxWrite;

    // Its own scheduler's only builder: nobody else takes new builds while
    // it drains, so a successor takes over instead (takeover.hh).
    [[nodiscard]] auto handsOver() const -> bool
    {
        return builder && scheduler && schedulerAddr.empty();
    }
};

auto parseOptions(const std::vector<std::string_view> & args) -> Options;

// --scheduler http://host:port is dialed without TLS.
constexpr std::string_view plaintextScheme = "http://";
constexpr unsigned certRefreshSeconds = 10;

auto makeServerCredentials(const Options & options) -> std::shared_ptr<grpc::ServerCredentials>;

} // namespace nixgrpc
