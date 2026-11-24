#pragma once
// Temp roots per client session (x-nix-session), on a nix-daemon connection
// per session that is closed after `ttl` without calls.

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <absl/base/thread_annotations.h>
#include <absl/synchronization/mutex.h>
#include <nix/store/path.hh>
#include <nix/store/store-api.hh>
#include <nix/util/error.hh>
#include <nix/util/ref.hh>

#include "logfmt.hh"

namespace nixgrpc {

class SessionRoots
{
public:
    using Clock = std::chrono::steady_clock;
    using Open = std::function<nix::ref<nix::Store>()>;

    static constexpr size_t defaultCap = 256;

    // What a hold is for. A session that held only what queries answered
    // present is evicted before one that holds uploads or outputs, so
    // clients that only ask cannot push out the paths of one that builds
    // while there is a query-only session to evict. When every session
    // holds uploads or outputs, a new one evicts the least recently used,
    // and that is logged. A session that held an upload or an output once
    // stays one that does: its later queries do not make it evictable
    // first, as its uploads and outputs are still held.
    enum class Hold : uint8_t { queried, kept };

    SessionRoots(Open open, std::chrono::seconds ttl, size_t cap = defaultCap)
        : open_(std::move(open))
        , ttl_(ttl)
        , cap_(cap)
    {
    }

    // Also refreshes the session. No-op without a session or paths. Throws
    // if the session's nix-daemon connection cannot be opened: answering
    // without holding would let the garbage collector take what was
    // answered present.
    void hold(
        std::string_view session,
        const std::vector<nix::StorePath> & paths,
        Hold why = Hold::kept,
        Clock::time_point now = Clock::now())
    {
        // Nothing to hold: no nix-daemon connection for a query that
        // found nothing present.
        if (session.empty() || paths.empty()) {
            return;
        }
        auto const add = [&](nix::Store & store) -> void {
            for (const auto & path : paths) {
                store.addTempRoot(path);
            }
        };
        try {
            add(*roots(session, why, now));
        } catch (nix::Error &) {
            // Its nix-daemon connection is gone, and the roots with it.
            drop(session);
            add(*roots(session, why, now));
        }
    }

    [[nodiscard]] auto holds(std::string_view session) -> bool
    {
        absl::MutexLock const lock(mutex_);
        return held_.contains(session);
    }

private:
    auto roots(std::string_view session, Hold why, Clock::time_point now) -> std::shared_ptr<nix::Store>
    {
        {
            absl::MutexLock const lock(mutex_);
            if (auto const found = held_.find(session); found != held_.end()) {
                touch(found->second, why, now);
                return found->second.store;
            }
        }
        // A new session: its connection first, outside the lock. One that
        // cannot be opened evicts no other session.
        std::shared_ptr<nix::Store> opened = open_().get_ptr();
        std::shared_ptr<nix::Store> store;
        std::shared_ptr<nix::Store> evicted; // closed outside the lock
        std::string holder;                  // an evicted session that held uploads or outputs
        {
            absl::MutexLock const lock(mutex_);
            auto found = held_.find(session);
            if (found == held_.end()) { // nobody opened it meanwhile
                if (held_.size() >= cap_) {
                    // The least recently used, of those that only queried
                    // if there are any.
                    auto oldest = held_.begin();
                    for (auto it = held_.begin(); it != held_.end(); ++it) {
                        if (std::pair(it->second.keeps, it->second.last)
                            < std::pair(oldest->second.keeps, oldest->second.last)) {
                            oldest = it;
                        }
                    }
                    evicted = std::move(oldest->second.store);
                    if (oldest->second.keeps) {
                        holder = oldest->first;
                        ++holdersEvicted_;
                    }
                    held_.erase(oldest);
                }
                found = held_.emplace(std::string(session), Held{.store = std::move(opened), .last = now}).first;
            }
            touch(found->second, why, now);
            store = found->second.store;
        }
        if (!holder.empty()) {
            // Its uploads and outputs are no longer held: as many sessions
            // as the cap all held some within the expiry.
            logLine(LogLevel::info, {{"event", "session_evicted"}, {"session", holder}, {"held", "uploads_or_outputs"}});
        }
        return store;
    }

    void drop(std::string_view session)
    {
        std::shared_ptr<nix::Store> store;
        absl::MutexLock const lock(mutex_);
        if (auto const found = held_.find(session); found != held_.end()) {
            store = std::move(found->second.store);
            held_.erase(found);
        }
    }

public:

    auto expire(Clock::time_point now = Clock::now()) -> size_t
    {
        std::vector<std::shared_ptr<nix::Store>> ended;
        {
            absl::MutexLock const lock(mutex_);
            for (auto it = held_.begin(); it != held_.end();) {
                if (now - it->second.last >= ttl_) {
                    ended.push_back(std::move(it->second.store));
                    it = held_.erase(it);
                } else {
                    ++it;
                }
            }
        }
        return ended.size();
    }

    // Sessions evicted although they held uploads or outputs.
    [[nodiscard]] auto holdersEvicted() -> size_t
    {
        absl::MutexLock const lock(mutex_);
        return holdersEvicted_;
    }

    [[nodiscard]] auto size() -> size_t
    {
        absl::MutexLock const lock(mutex_);
        return held_.size();
    }

private:
    struct Held
    {
        std::shared_ptr<nix::Store> store;
        Clock::time_point last;
        bool keeps = false; // held an upload or an output, not only queried paths
    };

    static void touch(Held & held, Hold why, Clock::time_point now)
    {
        held.last = now;
        held.keeps = held.keeps || why == Hold::kept;
    }

    Open open_;
    std::chrono::seconds ttl_;
    size_t cap_;
    absl::Mutex mutex_;
    std::map<std::string, Held, std::less<>> held_ ABSL_GUARDED_BY(mutex_);
    size_t holdersEvicted_ ABSL_GUARDED_BY(mutex_) = 0;
};

} // namespace nixgrpc
