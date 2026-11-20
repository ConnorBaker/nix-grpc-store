#pragma once
// One worker-protocol connection to the local nix-daemon for a build, with the
// stderr stream relayed to the gRPC client.

#include <algorithm>
#include <csignal>
#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>


#include <nix/store/build-result.hh>
#include <nix/store/derivations.hh>
#include <nix/store/derived-path.hh>
#include <nix/store/path.hh>
#include <nix/store/store-api.hh>
#include <nix/store/worker-protocol-connection.hh>
#include <nix/util/file-descriptor.hh>

#include "build-log.hh"
#include "nix_remote.pb.h"

namespace nixgrpc {

using Cancelled = std::function<bool()>;

// SIGTERM count: 1 drains (no new builds), 2 cancels running ones.
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables): signal handler, defined in main.
extern volatile std::sig_atomic_t stopSignal;
constexpr int kCancelBuilds = 2;

// Relays the raw worker-protocol stderr stream of one build to the
// client, which replays it through its own protocol code. Output path
// infos ride on the final message.
struct Backend
{
    struct Conn : nix::WorkerProto::BasicClientConnection
    {
        void closeWrite() override {}
    };
    nix::AutoCloseFD sock;
    Conn conn;
    nix::WorkerProto::ClientHandshakeInfo info;
    // Declared last so it joins before sock closes.
    std::jthread canceller;

    // A silent build sends nothing to notice a cancelled RPC on, so poll
    // and drop the connection, which makes nix-daemon kill the build.
    void cancelWith(Cancelled cancelled);
};

struct Backends
{
    std::string socketPath;

    [[nodiscard]] auto connect(nix::Store & store) const -> std::unique_ptr<Backend>;
    // Build-capable connection: cancellable, protocol checked.
    [[nodiscard]] auto forBuild(Cancelled cancelled, nix::Store & localStore) const -> std::unique_ptr<Backend>;

    // By path, not inline: nix-daemon recomputes output paths from the stored
    // closure, so a forged drv cannot claim foreign paths. No build hook.
    struct Limits
    {
        uint64_t buildTimeout = 0;
        uint64_t maxSilentTime = 0;
        // Who the build is for (the caller's session, NixRemoteService's
        // clientOf): Nix's `build-client`, which an external builder reads
        // as build.json's `client`.
        std::string client;

        // The settings SetOptions overrides by name: `extra`, and over it
        // these.
        [[nodiscard]] auto overrides(std::map<std::string, std::string> extra = {}) const
            -> std::map<std::string, std::string>
        {
            extra.insert_or_assign("builders", "");
            extra.insert_or_assign("timeout", std::to_string(buildTimeout));
            if (!client.empty()) {
                extra.insert_or_assign("build-client", client);
            }
            return extra;
        }
    };

    [[nodiscard]] auto storedBuild(
        Cancelled cancelled,
        nix::Store & localStore,
        const nix::StorePath & drvPath,
        nix::BuildMode mode,
        const Limits & limits,
        const BuildEventSink & sendLogLine) const -> nix::BuildResult;

    // EnsurePath with narinfo-cache-positive-ttl 0. Throws as ensurePath does.
    void ensureUncached(nix::Store & localStore, const nix::StorePath & path) const;
};

// Who a build is for, as Nix's `build-client`: the caller's session
// (x-nix-session, 32 lowercase hex digits as the plugin makes it), under
// the caller's authenticated name where it has one. Any other session names
// no client, so that a caller cannot pass for another name, an
// authenticated one, or one of another kind (Tribuchet's boot/pid/start).
[[nodiscard]] inline auto buildClient(std::optional<std::string_view> name, std::string_view session) -> std::string
{
    constexpr size_t sessionLength = 32;
    auto const lowerHex = [](char chr) -> bool { return (chr >= '0' && chr <= '9') || (chr >= 'a' && chr <= 'f'); };
    if (session.size() != sessionLength || !std::ranges::all_of(session, lowerHex)) {
        return {};
    }
    return name ? std::string(*name) + "/" + std::string(session) : std::string(session);
}

template<typename Chunk>
auto toChunk(BuildEvent event) -> Chunk
{
    Chunk chunk;
    if (event.kind == BuildEvent::Kind::phase) {
        *chunk.mutable_phase() = std::move(event.text);
    } else {
        *chunk.mutable_log_line() = std::move(event.text);
    }
    return chunk;
}

// Wire BuildResult plus PathInfo of every valid output.
void encodeResult(nix::Store & localStore, nix::BuildResult & res, nix::remote::BuildDerivationDone * done);

} // namespace nixgrpc
