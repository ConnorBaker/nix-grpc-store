#pragma once
// Local store plus, if configured, a niks3 binary cache. Same calls in every topology.

#include <cstdint>
#include <memory>
#include <optional>
#include <ranges>
#include <string>
#include <string_view>
#include <unordered_set>
#include <utility>
#include <vector>

#include <curl/curl.h>

#include <nlohmann/json.hpp>

#include <nix/store/path-info.hh>
#include <nix/store/path.hh>
#include <nix/store/store-api.hh>
#include <nix/util/environment-variables.hh>
#include <nix/util/error.hh>

#include "http.hh"
#include "nix-compat.hh"
#include "push.hh"
#include "niks3-client.hh"

namespace nixgrpc {

class Cache
{
    struct Remote
    {
        Niks3Client client;
        PushProcess push;
        // For publish(verify); its child starts on first use (ensureChild).
        PushProcess verifyingPush;
        std::string cacheUrl;

        explicit Remote(const Niks3Config & cfg)
            : client(cfg)
            , push(cfg.pushArgv)
            , verifyingPush(withVerify(cfg.pushArgv))
            , cacheUrl(cfg.cacheUrl)
        {
            while (cacheUrl.ends_with('/')) {
                cacheUrl.pop_back();
            }
        }

        static auto withVerify(PushProcess::Argv argv) -> PushProcess::Argv
        {
            argv.emplace_back("--verify-s3-integrity");
            return argv;
        }
    };
    std::optional<Remote> remote;

public:
    explicit Cache(const Niks3Config & cfg)
    {
        if (cfg.enabled()) {
            remote.emplace(cfg);
        }
    }

    [[nodiscard]] auto hasRemote() const -> bool
    {
        return remote.has_value();
    }

    [[nodiscard]] auto client() const -> const Niks3Client *
    {
        return remote ? &remote->client : nullptr;
    }

    // Subset of `keys` ("<hash>.narinfo") the binary cache has. Empty without niks3.
    [[nodiscard]] auto present(const std::vector<std::string> & keys) -> std::unordered_set<std::string>
    {
        std::unordered_set<std::string> res;
        if (!remote || keys.empty()) {
            return res;
        }
        auto call = remote->client.post("/api/objects/present", nlohmann::json{{"keys", keys}});
        auto status = call->perform();
        if (!http::ok(status)) {
            throw nix::Error("niks3 present: HTTP %d: %s", status, call->body());
        }
        auto reply = nlohmann::json::parse(call->body(), nullptr, /*allow_exceptions=*/false);
        auto const found = reply.is_object() ? reply.find("present") : reply.end();
        // Go encodes an empty slice as null.
        if (found == reply.end() || !(found->is_array() || found->is_null())) {
            throw nix::Error("niks3 present: malformed reply: %s", call->body());
        }
        for (const auto & key : *found) {
            if (key.is_string()) {
                res.insert(key.get<std::string>());
            }
        }
        return res;
    }

    // Make locally valid paths visible to other nodes. No-op without niks3.
    // `verify`: niks3 skips what its database lists, so for paths whose
    // objects the bucket may have lost it checks the bucket instead, at an S3
    // lookup per object of their closures.
    auto publish(const std::vector<std::string> & paths, const Cancelled & cancelled = never, bool verify = false)
        -> PushProcess::Signatures
    {
        if (!remote || paths.empty()) {
            return {};
        }
        return (verify ? remote->verifyingPush : remote->push).pushWait(paths, cancelled);
    }

    // Also puts the cache's signatures on the local paths: built here they are
    // only "ultimate", and a client checking signatures would refuse them.
    void publish(
        nix::Store & store, const std::vector<std::string> & paths, const Cancelled & cancelled = never, bool verify = false)
    {
        for (const auto & [path, sigs] : publish(paths, cancelled, verify)) {
            nixcompat::addSignatures(store, store.parseStorePath(path), sigs);
        }
    }

    // Whether the cache lost the path whose hash part is `hashPart`: niks3
    // lists it as a closure root, and the bucket has no narinfo for it, or
    // the narinfo's NAR answers 404. False for a path niks3 does not list,
    // such as one only another substituter offers, when the cache did not
    // answer either way, and without --niks3-cache-url. Throws on transport
    // errors.
    [[nodiscard]] auto lost(std::string_view hashPart) -> bool
    {
        constexpr long notFound = 404;
        if (!remote || remote->cacheUrl.empty()) {
            return false;
        }
        auto const cacheUrl = remote->cacheUrl;
        auto const key = std::string(hashPart) + ".narinfo";
        if (!present({key}).contains(key)) {
            return false;
        }
        http::Call narinfo(cacheUrl + "/" + key, "", std::nullopt);
        auto const status = narinfo.perform();
        if (status == notFound) {
            return true;
        }
        if (!http::ok(status)) {
            return false;
        }
        constexpr std::string_view urlField = "URL: ";
        for (const auto & line : std::views::split(std::string_view(narinfo.body()), '\n')) {
            auto const field = std::string_view(line.begin(), line.end());
            if (field.starts_with(urlField)) {
                http::Call nar(cacheUrl + "/" + std::string(field.substr(urlField.size())), "", std::nullopt);
                nar.opt(CURLOPT_NOBODY, 1L);
                return nar.perform() == notFound;
            }
        }
        return false;
    }

    // Path valid locally afterwards, or throws.
    static void substitute(nix::Store & store, const nix::StorePath & path)
    {
        if (!store.isValidPath(path)) {
            nixcompat::ensurePath(store, path);
        }
    }

    // References the client skipped as present may live only in the cache.
    void completeRefs(nix::Store & store, const nix::ValidPathInfo & info) const
    {
        if (!remote) {
            return;
        }
        for (const auto & ref : info.references) {
            if (ref != info.path) {
                substitute(store, ref);
            }
        }
    }
};

} // namespace nixgrpc
