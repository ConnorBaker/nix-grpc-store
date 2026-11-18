// A worker can have more than one reason to take no builds. It tells the
// scheduler it takes builds again only when the last one is gone, and its
// metric shows which ones hold.

#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <future>
#include <print>
#include <string>
#include <thread>
#include <vector>

#include "builder.hh"
#include "metrics.hh"
#include "nix_remote.pb.h"

namespace {

using nix::remote::Done;
using nixgrpc::Builder;
using nixgrpc::Clock;

// A scheduler ends a session whose first message is not Hello.
auto greetsFirst() -> bool
{
    nixgrpc::Metrics metrics{""};
    Builder builder({.advertise = "w:0", .systems = {"x86_64-linux"}, .features = {}, .maxJobs = 1}, metrics);
    std::vector<bool> hellos;
    builder.setSend([&](const nix::remote::WorkerMsg & msg) -> bool {
        hellos.push_back(msg.has_hello());
        return true;
    });
    builder.setUnavailable(Builder::Unavailable::localDaemonDown, true);
    builder.greet();
    return hellos == std::vector<bool>{true, false};
}

constexpr std::chrono::seconds hung{20};
constexpr std::chrono::seconds atOnce{2}; // well below the wait for a late assignment
constexpr std::chrono::milliseconds headStart{100};

auto expectOf(uint64_t assignId, const std::string & drv = "a.drv") -> nix::remote::Expect
{
    nix::remote::Expect exp;
    exp.set_drv_path(drv);
    exp.set_assign_id(assignId);
    return exp;
}

// The scheduler tells the worker and the client at the same time, on two
// streams. The client's call may be the first to arrive.
auto earlyCallWaits() -> bool
{
    nixgrpc::Metrics metrics{""};
    Builder builder({.advertise = "w:0", .systems = {"x86_64-linux"}, .features = {}, .maxJobs = 1}, metrics);
    auto call = std::async(std::launch::async, [&] -> bool { return builder.claim("a.drv", 2); });
    std::this_thread::sleep_for(headStart);
    builder.onExpect(expectOf(2));
    return call.wait_for(hung) == std::future_status::ready && call.get();
}

// The worker read a later assignment already, so this one is not on its way.
auto staleCallIsRefusedAtOnce() -> bool
{
    nixgrpc::Metrics metrics{""};
    Builder builder({.advertise = "w:0", .systems = {"x86_64-linux"}, .features = {}, .maxJobs = 1}, metrics);
    builder.onExpect(expectOf(2));
    auto call = std::async(std::launch::async, [&] -> bool { return builder.claim("b.drv", 1); });
    return call.wait_for(atOnce) == std::future_status::ready && !call.get();
}

constexpr auto past = std::chrono::seconds(61); // Builder's expectTimeout is 60 s

// A builder on a clock the test moves, and the Dones it sends.
struct Quiet
{
    nixgrpc::Metrics metrics{""};
    Clock::time_point now = Clock::now();
    Builder builder{
        {.advertise = "w:0",
         .systems = {"x86_64-linux"},
         .features = {},
         .maxJobs = 2,
         .now = [this] -> Clock::time_point { return now; }},
        metrics};
    std::vector<Done> dones;

    Quiet()
    {
        builder.setSend([this](const nix::remote::WorkerMsg & msg) -> bool {
            if (msg.has_done()) {
                dones.push_back(msg.done());
            }
            return true;
        });
        builder.greet();
    }
};

// A client that reached the builder for a drv and went away without building
// it (sent elsewhere, or to a successor that took the node over) left a
// claimed Expect nobody else ends. Once no call for it is in flight and
// expectTimeout has passed, it is given back; one with a call in flight
// stays.
auto quietClaimIsGivenBack() -> bool
{
    Quiet quiet;
    auto & builder = quiet.builder;
    builder.onExpect(expectOf(1, "a.drv"));
    builder.onExpect(expectOf(2, "b.drv"));
    builder.claim("a.drv");
    builder.callEnded("a.drv"); // say NOT_FOUND: the client was to upload and retry
    builder.claim("b.drv");     // still in its call
    constexpr auto halfway = std::chrono::seconds(30);
    quiet.now += halfway;
    builder.tick();
    if (!quiet.dones.empty() || builder.holding() != 2) {
        return false;
    }
    quiet.now += past;
    builder.tick();
    if (quiet.dones.size() != 1 || quiet.dones.at(0).drv_path() != "a.drv" || quiet.dones.at(0).assign_id() != 1
        || quiet.dones.at(0).outcome() != Done::GIVEN_BACK || quiet.dones.at(0).reason() != Done::UNREACHABLE
        || builder.holding() != 1) {
        return false;
    }
    builder.callEnded("b.drv");
    quiet.now += past;
    builder.tick();
    return quiet.dones.size() == 2 && builder.holding() == 0;
}

// Draining keeps a claimed Expect while its client may still upload, and
// gives it back once that client has gone quiet, so the drain can end.
auto drainingGivesBackAQuietClaim() -> bool
{
    Quiet quiet;
    auto & builder = quiet.builder;
    builder.onExpect(expectOf(1));
    builder.claim("a.drv");
    builder.callEnded("a.drv");
    builder.setUnavailable(Builder::Unavailable::stopping, true);
    builder.tick();
    if (!quiet.dones.empty() || builder.holding() != 1) {
        return false;
    }
    quiet.now += past;
    builder.tick();
    return quiet.dones.size() == 1 && quiet.dones.at(0).reason() == Done::DRAINING && builder.holding() == 0;
}

// A client claimed the drv (NOT_FOUND) and uploads its inputs to this builder
// (x-nix-drv, a Call) for longer than expectTimeout. The claim stays while
// the upload runs, and the end of the upload is the client's latest call:
// the next tick keeps the claim, and only expectTimeout after that does a
// quiet claim go.
auto uploadKeepsTheClaim() -> bool
{
    Quiet quiet;
    auto & builder = quiet.builder;
    builder.onExpect(expectOf(1));
    builder.claim("a.drv");
    builder.callEnded("a.drv");
    {
        Builder::Call const upload(builder, "a.drv");
        quiet.now += past;
        builder.tick();
        if (!quiet.dones.empty() || builder.holding() != 1) {
            return false;
        }
    }
    quiet.now += std::chrono::milliseconds(1);
    builder.tick();
    if (!quiet.dones.empty() || builder.holding() != 1) {
        return false;
    }
    quiet.now += past;
    builder.tick();
    return quiet.dones.size() == 1 && builder.holding() == 0;
}

auto run() -> int
{
    if (!earlyCallWaits()) {
        std::println("FAIL a call that arrives before its assignment is not served once the assignment arrives");
        return EXIT_FAILURE;
    }
    if (!staleCallIsRefusedAtOnce()) {
        std::println("FAIL a call for an assignment that is over waits for it");
        return EXIT_FAILURE;
    }
    if (!greetsFirst()) {
        std::println("FAIL a worker that takes no builds from the start says so before its Hello");
        return EXIT_FAILURE;
    }
    if (!quietClaimIsGivenBack()) {
        std::println("FAIL a claimed Expect whose client went quiet is not given back after expectTimeout");
        return EXIT_FAILURE;
    }
    if (!drainingGivesBackAQuietClaim()) {
        std::println("FAIL a draining builder does not give back a claim whose client went quiet");
        return EXIT_FAILURE;
    }
    if (!uploadKeepsTheClaim()) {
        std::println("FAIL an upload for a claimed drv does not keep its claim");
        return EXIT_FAILURE;
    }
    nixgrpc::Metrics metrics{""};
    Builder builder({.advertise = "w:0", .systems = {"x86_64-linux"}, .features = {}, .maxJobs = 1}, metrics);
    std::vector<bool> told;
    builder.setSend([&](const nix::remote::WorkerMsg & msg) -> bool {
        if (msg.has_load()) {
            told.push_back(msg.load().draining());
        }
        return true;
    });
    builder.greet();

    builder.setUnavailable(Builder::Unavailable::localDaemonDown, true);
    builder.setUnavailable(Builder::Unavailable::stopping, true);
    builder.setUnavailable(Builder::Unavailable::localDaemonDown, false);
    if (told != std::vector<bool>{true}) {
        std::println("FAIL a stopping worker takes builds again once its local nix-daemon answers");
        return EXIT_FAILURE;
    }
    if (metrics.unavailableNow("stopping") != 1 || metrics.unavailableNow("local_daemon") != 0) {
        std::println("FAIL the metric does not show stopping as the only reason");
        return EXIT_FAILURE;
    }
    builder.setUnavailable(Builder::Unavailable::stopping, false);
    if (told != std::vector<bool>{true, false}) {
        std::println("FAIL a worker with no reason left does not take builds again");
        return EXIT_FAILURE;
    }
    return EXIT_SUCCESS;
}

} // namespace

auto main() -> int
{
    try {
        return run();
    } catch (...) {
        return EXIT_FAILURE;
    }
}
