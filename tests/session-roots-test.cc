// SessionRoots: sessions that only queried are evicted before one that
// keeps an upload or an output, so clients that only ask (nix path-info,
// validity checks in a loop) cannot drop a building client's paths.

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdio>
#include <exception>
#include <functional>
#include <print>
#include <stdexcept>
#include <string>
#include <vector>

#include <sys/types.h>
#include <unistd.h>

#include <nix/store/globals.hh>
#include <nix/store/path.hh>
#include <nix/store/store-api.hh>
#include <nix/store/store-open.hh>
#include <nix/util/error.hh>
#include <nix/util/ref.hh>

#include "session-roots.hh"

namespace {

using Roots = nixgrpc::SessionRoots;

struct Checks
{
    int failures = 0;

    void operator()(bool passed, const std::string & what)
    {
        if (!passed) {
            std::println(stderr, "FAIL: {}", what);
            ++failures;
        }
    }
};

// What `body` writes to stderr, through a pipe: a few log lines fit in
// its buffer.
auto stderrOf(const std::function<void()> & body) -> std::string
{
    std::array<int, 2> fds{};
    if (::pipe(fds.data()) != 0) {
        throw std::runtime_error("pipe");
    }
    (void)std::fflush(stderr);
    int const saved = ::dup(STDERR_FILENO);
    (void)::dup2(std::get<1>(fds), STDERR_FILENO);
    (void)::close(std::get<1>(fds));
    body();
    (void)std::fflush(stderr);
    (void)::dup2(saved, STDERR_FILENO); // closes the pipe's last write end
    (void)::close(saved);
    std::string out;
    constexpr size_t chunk = 4096;
    std::array<char, chunk> buf{};
    for (ssize_t got = 0; (got = ::read(std::get<0>(fds), buf.data(), buf.size())) > 0;) {
        out.append(buf.data(), static_cast<size_t>(got));
    }
    (void)::close(std::get<0>(fds));
    return out;
}

auto run() -> int
{
    nix::initLibStore();
    // A store without a garbage collector: its temp roots are no-ops.
    auto const open = [] -> nix::ref<nix::Store> { return nix::openStore("dummy://"); };
    std::vector<nix::StorePath> const paths{nix::StorePath("00000000000000000000000000000000-x")};
    constexpr size_t cap = 4;
    constexpr int queries = 100;
    auto const start = Roots::Clock::now();
    auto const after = [&](int seconds) -> Roots::Clock::time_point { return start + std::chrono::seconds(seconds); };
    Checks check;

    {
        Roots roots(open, std::chrono::hours(1), cap);
        roots.hold("build", paths, Roots::Hold::queried, after(0));
        roots.hold("build", paths, Roots::Hold::kept, after(1));
        for (int i = 0; i < queries; ++i) {
            roots.hold("query-" + std::to_string(i), paths, Roots::Hold::queried, after(2 + i));
        }
        check(roots.holds("build"), "100 query-only sessions evicted the session that keeps an upload");
        check(roots.holdersEvicted() == 0, "only query-only sessions were evicted");
        check(roots.size() == cap, "the cap holds");
        check(roots.holds("query-" + std::to_string(queries - 1)), "the newest query-only session is held");
        check(!roots.holds("query-0"), "the oldest query-only session went first");
    }
    {
        // Among sessions that all keep paths, the least recently used goes,
        // loudly.
        Roots roots(open, std::chrono::hours(1), 2);
        roots.hold("a", paths, Roots::Hold::kept, after(0));
        roots.hold("b", paths, Roots::Hold::kept, after(1));
        auto const logged = stderrOf([&] -> void { roots.hold("c", paths, Roots::Hold::kept, after(2)); });
        check(logged.contains("event=session_evicted session=a"), "the eviction of a session that kept paths is logged: " + logged);
        check(!roots.holds("a") && roots.holds("b") && roots.holds("c"), "least recently used among keepers");
        check(roots.holdersEvicted() == 1, "the evicted session that kept paths is counted");
    }
    {
        // Only queried so far: as evictable as any other query-only session.
        Roots roots(open, std::chrono::hours(1), 2);
        roots.hold("planning", paths, Roots::Hold::queried, after(0));
        roots.hold("q1", paths, Roots::Hold::queried, after(1));
        roots.hold("q2", paths, Roots::Hold::queried, after(2));
        check(!roots.holds("planning"), "a session that only queried is evicted by age");
    }
    {
        // Every session holds uploads or outputs: a new query-only one
        // evicts the least recently used of them, loudly.
        Roots roots(open, std::chrono::hours(1), 2);
        roots.hold("a", paths, Roots::Hold::kept, after(0));
        roots.hold("b", paths, Roots::Hold::kept, after(1));
        auto const logged = stderrOf([&] -> void { roots.hold("q", paths, Roots::Hold::queried, after(2)); });
        check(!roots.holds("a") && roots.holds("b") && roots.holds("q"), "a full set of keepers loses its oldest");
        check(roots.holdersEvicted() == 1, "that eviction is counted");
        check(logged.contains("event=session_evicted session=a"), "that eviction is logged: " + logged);
    }
    {
        // Once it kept paths, a session's later queries leave it a keeper:
        // its uploads and outputs are still held.
        Roots roots(open, std::chrono::hours(1), 2);
        roots.hold("build", paths, Roots::Hold::kept, after(0));
        roots.hold("build", paths, Roots::Hold::queried, after(1));
        roots.hold("q1", paths, Roots::Hold::queried, after(2));
        roots.hold("q2", paths, Roots::Hold::queried, after(3));
        check(roots.holds("build"), "a later query made the session that keeps an upload evictable first");
    }
    {
        // A query that found nothing present opens no session.
        Roots roots(open, std::chrono::hours(1), 2);
        roots.hold("empty", {}, Roots::Hold::queried, after(0));
        check(!roots.holds("empty") && roots.size() == 0, "an empty hold opened a session");
    }
    {
        // A session whose nix-daemon connection cannot be opened fails the
        // hold instead of holding nothing.
        Roots roots([] -> nix::ref<nix::Store> { throw nix::Error("cannot connect to socket at 'test'"); },
                    std::chrono::hours(1), 2);
        bool threw = false;
        try {
            roots.hold("x", paths, Roots::Hold::queried, after(0));
        } catch (nix::Error &) {
            threw = true;
        }
        check(threw, "a session that could not open its connection held nothing, silently");
    }
    {
        // All slots held by keepers, and the next session's connection
        // cannot be opened: no session is evicted for it.
        int opens = 0;
        Roots roots(
            [&] -> nix::ref<nix::Store> {
                if (++opens > 2) {
                    throw nix::Error("cannot connect to socket at 'test'");
                }
                return open();
            },
            std::chrono::hours(1), 2);
        roots.hold("a", paths, Roots::Hold::kept, after(0));
        roots.hold("b", paths, Roots::Hold::kept, after(1));
        bool threw = false;
        auto const logged = stderrOf([&] -> void {
            try {
                roots.hold("q", paths, Roots::Hold::queried, after(2));
            } catch (nix::Error &) {
                threw = true;
            }
        });
        check(threw, "the hold whose connection could not be opened did not fail");
        check(roots.holds("a") && roots.holds("b"), "a session was evicted for one whose connection could not be opened");
        check(roots.holdersEvicted() == 0 && !logged.contains("session_evicted"), "an eviction happened: " + logged);
    }
    {
        // A held session's later holds open no other connection.
        int opens = 0;
        Roots roots(
            [&] -> nix::ref<nix::Store> {
                ++opens;
                return open();
            },
            std::chrono::hours(1), 2);
        roots.hold("s", paths, Roots::Hold::queried, after(0));
        roots.hold("s", paths, Roots::Hold::kept, after(1));
        roots.hold("s", paths, Roots::Hold::queried, after(2));
        check(opens == 1, "a held session opened " + std::to_string(opens) + " connections");
    }
    {
        // Two first holds of one session at once: the one whose connection
        // opened second finds the session the other just added and evicts
        // nothing. The other's hold is nested in the first's open here, so
        // the order is fixed.
        Roots * self = nullptr;
        bool nested = false;
        Roots roots(
            [&] -> nix::ref<nix::Store> {
                if (!nested && self != nullptr && self->holds("a")) {
                    nested = true;
                    self->hold("s", paths, Roots::Hold::kept, after(1));
                }
                return open();
            },
            std::chrono::hours(1), 2);
        self = &roots;
        roots.hold("a", paths, Roots::Hold::kept, after(3));
        roots.hold("s", paths, Roots::Hold::kept, after(4));
        check(nested, "the nested hold did not run");
        check(roots.holds("a") && roots.holds("s"), "a session was evicted for one that was already held");
        check(roots.holdersEvicted() == 0, "the losing open evicted a session");
    }
    if (check.failures == 0) {
        std::println("session-roots: ok");
    }
    return check.failures == 0 ? 0 : 1;
}

} // namespace

auto main() -> int
{
    try {
        return run();
    } catch (std::exception & err) {
        (void)std::fputs(err.what(), stderr);
        return 1;
    }
}
