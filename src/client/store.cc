// grpc:// Store: channel setup, error mapping, trust probe, worker-protocol tunnel, and the
// entry GrpcStoreConfig::openStore (store-config.cc) loads this library for.

#include "store.hh"

#include <chrono>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include <grpc/impl/channel_arg_names.h>
#include <grpcpp/grpcpp.h>
#include <grpcpp/security/credentials.h>
#include <grpcpp/support/channel_arguments.h>
#include <grpcpp/support/client_interceptor.h>
#include <grpcpp/support/status.h>
#include <grpcpp/support/sync_stream.h>

#include <nix/store/derivations.hh>
#include <nix/store/path-info.hh>
#include <nix/store/remote-store.hh>
#include <nix/store/store-api.hh>
#include <nix/store/worker-protocol.hh>
#include <nix/util/error.hh>
#include <nix/util/file-system.hh>
#include <nix/util/logging.hh>
#include <nix/util/ref.hh>
#include <nix/util/serialise.hh>
#include <nix/util/util.hh>

#include "channel.hh"
#include "nix_remote.grpc.pb.h"
#include "nix_remote.pb.h"
#include "pump.hh"
#include "retry.hh"

namespace nix {

auto GrpcStore::sslOptions() -> grpc::SslCredentialsOptions {
  grpc::SslCredentialsOptions ssl;
  auto const caCert = config->caCert.get().empty() ? nixgrpc::defaultCaCert() : config->caCert.get();
  if (!caCert.empty()) {
    ssl.pem_root_certs = readFile(caCert);
  }
  auto clientCert = config->clientCert.get();
  auto clientKey = config->clientKey.get();
  if (clientCert.empty() != clientKey.empty()) {
    throw Error("gRPC store '%s': client-cert and client-key must be set together",
                config->authority.to_string());
  }
  // A stray default cert from another CA would abort the handshake before the token counts.
  if (clientCert.empty() && config->tokenFile.get().empty()) {
    clientCert = nixgrpc::defaultClientCred("NIX_GRPC_CLIENT_CERT", "client.crt");
    clientKey = nixgrpc::defaultClientCred("NIX_GRPC_CLIENT_KEY", "client.key");
  }
  if (!clientCert.empty() && !clientKey.empty()) {
    ssl.pem_cert_chain = nixgrpc::readValidCert(clientCert);
    ssl.pem_private_key = readFile(clientKey);
    haveClientCert = true;
  }
  if (config->debug) {
    warn("grpc-store %s: ca=%s (%d bytes) client-cert=%s client-key=%s",
         config->authority.to_string(), caCert.empty() ? "<grpc builtin>" : caCert,
         ssl.pem_root_certs.size(), clientCert.empty() ? "<none>" : clientCert,
         clientKey.empty() ? "<none>" : clientKey);
  }
  return ssl;
}

auto GrpcStore::makeChannel(bool ownConnection) -> std::shared_ptr<grpc::Channel>
{
  grpc::ChannelArguments args = nixgrpc::daemonChannelArgs();
  // The worker protocol streams NARs; do not cap message size.
  args.SetMaxReceiveMessageSize(-1);
  args.SetMaxSendMessageSize(-1);
  if (ownConnection) {
    // Private subchannel pool = own TCP connection.
    args.SetInt(GRPC_ARG_USE_LOCAL_SUBCHANNEL_POOL, 1);
  }
  std::vector<std::unique_ptr<grpc::experimental::ClientInterceptorFactoryInterface>> interceptors;
  if (!config->routeSystem.get().empty()) {
    interceptors.push_back(std::make_unique<nixgrpc::StaticHeaders::Factory>(
        nixgrpc::StaticHeaders::Headers{{"x-nix-system", config->routeSystem.get()}}));
  }
  auto channel = grpc::experimental::CreateCustomChannelWithInterceptors(
      config->authority.to_string(), creds, args, std::move(interceptors));
  // Start TCP+TLS setup now instead of stalling the first RPC.
  channel->GetState(true);
  return channel;
}

GrpcStore::GrpcStore(const ref<const Config> &config)
    : Store{*config}, RemoteStore{*config}, config{config},
      narFetcher{
          [this] -> std::shared_ptr<grpc::Channel> { return makeChannel(true); },
          config->authority.to_string(), config->narConnections,
          // A path cut off resumes where it ended, so a broken stream is as
          // safe to retry as a failed connect.
          nixgrpc::RetryPolicy{
              .retryable = [](const grpc::Status & status) -> bool {
                return nixgrpc::goneAway(status) || nixgrpc::streamBroken(status);
              },
              .budget = [this] -> std::chrono::milliseconds { return retryBudget(); }}} {
  if (config->insecure) {
    if (!config->tokenFile.get().empty()) {
      throw Error("gRPC store '%s': token-file needs TLS", config->authority.to_string());
    }
    creds = grpc::InsecureChannelCredentials();
  } else {
    creds = grpc::SslCredentials(sslOptions());
    auto tokenFile = config->tokenFile.get();
    if (tokenFile.empty()) {
      tokenFile = nixgrpc::defaultClientCred("NIX_GRPC_TOKEN_FILE", "token");
    }
    if (!tokenFile.empty()) {
      creds = grpc::CompositeChannelCredentials(
          creds, grpc::MetadataCredentialsFromPlugin(std::make_unique<nixgrpc::TokenFileCredentials>(tokenFile)));
      haveClientCert = true;
    }
  }

  auto const channel = makeChannel(false);
  stub = remote::NixRemote::NewStub(channel);
  sched = remote::Scheduler::NewStub(channel);
}

auto GrpcStore::connectHint(const std::string & msg) const -> std::string
{
  auto const has = [&](std::string_view needle) -> bool { return msg.contains(needle); };
  std::string const more = config->debug ? "" : " Set NIX_GRPC_DEBUG=1 for a handshake trace.";
  if (nixgrpc::transportError(msg)) {
    return "\nhint: could not reach the server (TCP/DNS), not a certificate problem." + more;
  }
  if (has("Handshake") || has("SSL") || has("TLS") ||
      has("certificate") || has("Socket closed")) {
    if (has("verif") || has("issuer") || has("self-signed") || has("self signed")) {
      return "\nhint: the server certificate is not trusted. Set 'ca-cert' to the CA bundle that "
             "signed it." + more;
    }
    if (!haveClientCert) {
      return "\nhint: TLS handshake failed and no client certificate was presented. If the "
             "server requires mTLS set 'client-cert'/'client-key' or install client.crt/"
             "client.key in $XDG_DATA_HOME/nix-grpc-store or /var/lib/nix-grpc-store." + more;
    }
    return "\nhint: TLS handshake failed: server certificate not trusted by our CA bundle, "
           "server rejected our client certificate, or the peer is not speaking TLS." + more;
  }
  return more.empty() ? "" : "\nhint:" + more;
}

auto GrpcStore::statusError(const grpc::Status & status, const char * opName) const -> GrpcStatusError
{
  if (auto const detail = nixgrpc::credentialsFailed(status)) {
    // NOLINTNEXTLINE(modernize-return-braced-init-list): Error ctor is explicit
    return GrpcStatusError(status, "gRPC %s on '%s' failed: %s", opName, config->authority.to_string(), *detail);
  }
  std::string hint;
  auto const code = status.error_code();
  if (!config->insecure && code == grpc::StatusCode::UNAVAILABLE) {
    hint = connectHint(status.error_message());
  } else if (!config->insecure && code == grpc::StatusCode::UNAUTHENTICATED && !haveClientCert) {
    // The handshake went through, the server wants to know who we are.
    hint = "\nhint: no client certificate or token was sent. Set 'client-cert'/'client-key' or "
           "'token-file', or install client.crt/client.key or token in "
           "$XDG_DATA_HOME/nix-grpc-store or /var/lib/nix-grpc-store.";
  }
  // NOLINTNEXTLINE(modernize-return-braced-init-list): Error ctor is explicit
  return GrpcStatusError(status, "gRPC %s on '%s' failed: %s%s", opName, config->authority.to_string(),
                         status.error_message(), hint);
}

auto GrpcStore::isTrustedClient() -> std::optional<TrustedFlag> {
  std::call_once(trustedOnce, [&] -> void {
    remote::StoreInfoRequest const request;
    remote::StoreInfoReply reply;
    retrying("StoreInfo", [&] -> grpc::Status {
      nixgrpc::Call call;
      return stub->StoreInfo(&call.ctx(), request, &reply);
    });
    if (reply.has_trusted()) {
      trusted = reply.trusted() ? Trusted : NotTrusted;
    }
  });
  return trusted;
}

auto GrpcStore::openConnection() -> ref<RemoteStore::Connection> {
  auto const conn = make_ref<Connection>();

  conn->stream = stub->Connect(&conn->call.ctx());
  if (!conn->stream) {
    throw Error("failed to open gRPC stream to '%s'",
                config->authority.to_string());
  }

  conn->toRemote.create();
  conn->fromRemote.create();
  nixgrpc::growPipe(conn->toRemote);
  nixgrpc::growPipe(conn->fromRemote);

  conn->reader = std::jthread([connPtr = &*conn] -> void {
    try {
      nixgrpc::pumpFdToStream(connPtr->toRemote.readSide.get(), *connPtr->stream);
    } catch (...) {
      ignoreExceptionInDestructor();
    }
    connPtr->stream->WritesDone();
    // If the stream broke, RemoteStore may be blocked in FdSink writing to
    // a full pipe with no drainer. Closing the read side turns that into
    // EPIPE so the error surfaces instead of hanging.
    connPtr->toRemote.readSide.close();
  });

  conn->writer = std::jthread([this, connPtr = &*conn] -> void {
    try {
      nixgrpc::pumpStreamToFd(*connPtr->stream, connPtr->fromRemote.writeSide.get());
    } catch (...) {
      ignoreExceptionInDestructor();
    }
    // RemoteStore only sees EOF on the pipe, so surface the real status here.
    auto const status = connPtr->stream->Finish();
    connPtr->finished = true;
    if (!status.ok() && status.error_code() != grpc::StatusCode::CANCELLED) {
      logError(statusError(status, "Connect").info());
    }
    // Propagate EOF / error to the worker-protocol reader.
    connPtr->fromRemote.writeSide.close();
  });

  conn->to = FdSink(conn->toRemote.writeSide.get());
  conn->from = FdSource(conn->fromRemote.readSide.get());
  return conn;
}

} // namespace nix

// Declared in store-config.hh.
extern "C" void nix_grpc_store_open(const nix::GrpcStoreConfig & config, std::shared_ptr<nix::Store> & store) {
  // gRPC reads its trace settings in grpc_init().
  if (config.debug) {
    nixgrpc::enableGrpcTracing();
  }
  nixgrpc::retainGrpcRuntime();
  store = std::make_shared<nix::GrpcStore>(nix::ref{config.shared_from_this()});
}
