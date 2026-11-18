#pragma once
// The grpc:// store's configuration and its global settings: everything Nix
// needs to know about the store type before one is opened. The plugin the
// loader opens (store-config.cc) holds this alone and links no gRPC; opening
// a store loads the rest (nix-grpc-store-impl, store.hh), so that a process
// that never opens a grpc:// store never maps gRPC. Nix execs a daemon worker
// per connection, and each of them loads plugin-files.

#include <cstddef>
#include <memory>
#include <string>

#include <nix/store/remote-store.hh>
#include <nix/store/store-api.hh>
#include <nix/store/store-reference.hh>
#include <nix/util/configuration.hh>
#include <nix/util/environment-variables.hh>
#include <nix/util/ref.hh>
#include <nix/util/types.hh>
#include <nix/util/url.hh>

#include "nix-compat.hh"

namespace nixgrpc {
// The `grpc-linger` setting (store-config.cc): seconds every Want asks the scheduler
// to keep its build once this client leaves (Want.linger_s).
auto lingerSeconds() -> unsigned;
// The `grpc-lease` setting (store-config.cc): whose promise that is (Want.lease_id).
auto leaseId() -> std::string;
// How much of a lease id a notice shows: whoever knows the id can end that
// lease, and a notice ends up in logs others read (the scheduler's journal
// shows as much, dispatcher.cc's leaseIdLogged).
constexpr size_t leaseIdShown = 8;
} // namespace nixgrpc

namespace nix {
struct GrpcStoreConfig;
} // namespace nix

// The one way into nix-grpc-store-impl: defined there (store.cc), looked up
// with dlsym by GrpcStoreConfig::openStore (store-config.cc). Declared here
// for both, so that the two cannot disagree on its type.
extern "C" void nix_grpc_store_open(const nix::GrpcStoreConfig & config, std::shared_ptr<nix::Store> & store);

namespace nixgrpc {
using OpenGrpcStore = decltype(&nix_grpc_store_open);
} // namespace nixgrpc

namespace nix {

// Follows Nix's own StoreConfig pattern (e.g. DummyStoreConfig).
// NOLINTNEXTLINE(misc-multiple-inheritance,misc-use-internal-linkage)
struct GrpcStoreConfig : std::enable_shared_from_this<GrpcStoreConfig>, virtual RemoteStoreConfig
{
private:
    friend struct GrpcStore;
    friend void ::nix_grpc_store_open(const GrpcStoreConfig & config, std::shared_ptr<Store> & store);

    ParsedURL::Authority authority;

    Setting<bool> insecure{this, false, "insecure", "Use plaintext instead of TLS. Only for local testing."};

    static constexpr unsigned defaultConnectTimeout = 30;
    Setting<unsigned int> connectTimeout{this, defaultConnectTimeout, "connect-timeout",
        "Seconds the first call waits for the server to become reachable before failing."};

    static constexpr unsigned defaultRestartGrace = 120;
    Setting<unsigned int> restartGrace{this, defaultRestartGrace, "restart-grace",
        "Seconds to keep retrying once the server has answered before: across a worker or "
        "scheduler restart, and while the scheduler says no worker can take a build."};

    Setting<std::string> routeSystem{this, "", "system",
        "Send `x-nix-system: VALUE` on every call, not only on builds. Use one "
        "`nix.buildMachines` entry per system behind a balancer so input uploads "
        "and substitution land on a worker of that system."};

    Setting<bool> debug{this, nix::getEnv("NIX_GRPC_DEBUG").value_or("") == "1", "debug",
        "Log TLS setup and enable gRPC handshake tracing on stderr. Also `NIX_GRPC_DEBUG=1`."};

    Setting<std::string> caCert{
        this,
        "",
        "ca-cert",
        "Path to a PEM file with the CA certificate used to verify the server. "
        "Defaults to `$NIX_SSL_CERT_FILE`, `$SSL_CERT_FILE` or the system CA bundle."};

    Setting<std::string> clientCert{
        this,
        "",
        "client-cert",
        "Path to a PEM client certificate chain to present for mTLS. Defaults to "
        "`$NIX_GRPC_CLIENT_CERT`, then `client.crt` in `$XDG_DATA_HOME/nix-grpc-store`, "
        "`/run/nix-grpc-store` or `/var/lib/nix-grpc-store`."};

    Setting<std::string> clientKey{
        this,
        "",
        "client-key",
        "Path to the PEM private key for `client-cert`. Defaults to "
        "`$NIX_GRPC_CLIENT_KEY`, then `client.key` next to the default `client-cert`."};

    Setting<std::string> tokenFile{
        this,
        "",
        "token-file",
        "File holding an OIDC bearer token, re-read for every call so it may be "
        "rotated in place. Defaults to `$NIX_GRPC_TOKEN_FILE`, then `token` next to "
        "the default `client-cert`. Requires TLS."};

    static constexpr unsigned defaultMaxBuilds = 64;

    static constexpr unsigned defaultRescheduleRetries = 8;
    Setting<unsigned> rescheduleRetries{this, defaultRescheduleRetries, "reschedule-retries",
        "How often a derivation bounced by its assigned worker is handed back to the scheduler."};
    Setting<unsigned> maxBuilds{
        this,
        defaultMaxBuilds,
        "max-builds",
        "Number of concurrent BuildDerivation streams."};

    Setting<unsigned> narConnections{
        this,
        4,
        "nar-connections",
        "Number of parallel TCP connections used for NAR downloads."};

    Setting<bool> releaseLease{this, false, "release-lease",
        "End the lease the `grpc-lease` setting names on the scheduler when the store connects, as "
        "`nix store info` does: builds kept only for it are let go. Fails unless `grpc-lease` is set "
        "and the scheduler answers."};

public:
    explicit GrpcStoreConfig(const Params & params)
        : StoreConfig(NIX_COMPAT_STORE_CONFIG_ARGS(params))
        , RemoteStoreConfig(NIX_COMPAT_STORE_CONFIG_ARGS(params))
    {
    }

    GrpcStoreConfig(ParsedURL::Authority authority, const Params &params)
        : StoreConfig(NIX_COMPAT_STORE_CONFIG_ARGS(params)),
          RemoteStoreConfig(NIX_COMPAT_STORE_CONFIG_ARGS(params)),
          authority(std::move(authority)) {}

    // NOLINTNEXTLINE(modernize-use-string-view): nix::StoreConfig's signature
    static auto name() -> std::string { return "gRPC Store"; }

    static auto uriSchemes() -> StringSet { return {"grpc"}; }

    // NOLINTNEXTLINE(modernize-use-string-view)
    static auto doc() -> std::string {
      return "Connects to a `nix-grpc-daemon`: native RPCs for queries, copies and "
             "scheduled builds, the tunnelled worker protocol for the rest.";
    }

    auto getReference() const -> StoreReference override {
      return {
          .variant =
              StoreReference::Specified{
                  .scheme = *uriSchemes().begin(),
                  .authority = authority.to_string(),
              },
          .params = getQueryParams(),
      };
    }

    auto openStore() const -> ref<Store> override;
};

} // namespace nix
