// Unit tests for the Builder's side of the scheduler protocol, with an
// in-memory session (no gRPC).

#include <cassert>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <exception>
#include <iostream>
#include <vector>

#include "backend.hh"
#include "coordinator.hh"
#include "metrics.hh"
#include "nix_remote.pb.h"
#include "options.hh"

// main.cc's, which this test does not link.
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables): set by its signal handler there.
volatile std::sig_atomic_t nixgrpc::stopSignal = 0;

using nix::remote::Done;
using nix::remote::WorkerMsg;
using nixgrpc::Builder;
using nixgrpc::Clock;
using nixgrpc::Metrics;
using nixgrpc::Options;

// NOLINTBEGIN(*-magic-numbers, *-avoid-unchecked-container-access): test vectors, asserted sizes.
namespace {

constexpr auto drvA = "/nix/store/00000000000000000000000000000000-a.drv";
constexpr auto drvB = "/nix/store/11111111111111111111111111111111-b.drv";
constexpr auto past = std::chrono::seconds(61); // Builder's expectTimeout is 60 s

auto expect(const char * drv, uint64_t assignId) -> nix::remote::Expect
{
    nix::remote::Expect exp;
    exp.set_drv_path(drv);
    exp.set_assign_id(assignId);
    return exp;
}

// What the builder tells its scheduler. `inside` is set while the scheduler
// calls into the builder: on the in-process path that is under the
// dispatcher's lock, which a message sent then would take again.
struct Session
{
    std::vector<WorkerMsg> sent;
    bool inside = false;

    void attach(Builder & builder)
    {
        builder.setSend([this](const WorkerMsg & msg) -> bool {
            assert(!inside);
            sent.push_back(msg);
            return true;
        });
    }

    [[nodiscard]] auto dones() const -> std::vector<Done>
    {
        std::vector<Done> out;
        for (const auto & msg : sent) {
            if (msg.has_done()) {
                out.push_back(msg.done());
            }
        }
        return out;
    }
};

auto options() -> Options
{
    Options opts;
    opts.advertise = "w1:50051";
    opts.maxJobs = 1;
    return opts;
}

// An Expect that reaches a draining builder is given back, but not from
// within onExpect: the in-process scheduler calls onExpect under its lock.
void testExpectWhileDrainingIsGivenBackLater()
{
    auto const opts = options();
    Metrics metrics{""};
    Builder builder(opts, metrics);
    Session session;
    session.attach(builder);
    builder.setDraining(true);
    session.inside = true;
    builder.onExpect(expect(drvA, 1));
    session.inside = false;
    assert(session.dones().empty());
    builder.tick();
    auto const dones = session.dones();
    assert(dones.size() == 1 && dones[0].drv_path() == drvA && dones[0].assign_id() == 1);
    assert(dones[0].outcome() == Done::GIVEN_BACK && dones[0].reason() == Done::DRAINING);
    assert(builder.holding() == 0);
}

// The scheduler can place a drv that this builder already runs on it once
// more, with a new assign id (a client bounced, and no other worker can run
// it). The running build then stands for the new placement: clients attach
// to it, and the next Hello and its Done name the new id, which Core::done
// needs to end that placement.
void testRunningBuildTakesTheNewPlacement()
{
    auto const opts = options();
    Metrics metrics{""};
    Builder builder(opts, metrics);
    Session session;
    session.attach(builder);
    builder.onExpect(expect(drvA, 1));
    auto const building = builder.admit(drvA);
    assert(building && !building->attach && building->assignId == 1);
    builder.onExpect(expect(drvA, 2));
    auto const attached = builder.admit(drvA);
    assert(attached && attached->attach);
    auto const hello = builder.hello();
    assert(hello.hello().names_placements());
    assert(hello.hello().running_size() == 1 && hello.hello().running(0).assign_id() == 2);
    builder.finished(drvA, Done::BUILT, {}, "");
    auto const dones = session.dones();
    assert(dones.size() == 1 && dones[0].assign_id() == 2);
}

// A client that reached the builder for a drv and went away without building
// it (bounced elsewhere, or sent to a successor that took the node over) left
// a claimed Expect nobody else ends. Once no call for it is in flight and
// expectTimeout has passed, it is given back; one with a call in flight
// stays.
void testQuietClaimIsGivenBack()
{
    auto const opts = options();
    Metrics metrics{""};
    Builder builder(opts, metrics);
    Session session;
    session.attach(builder);
    builder.onExpect(expect(drvA, 1));
    builder.onExpect(expect(drvB, 2));
    builder.claim(drvA);
    builder.callEnded(drvA); // say NOT_FOUND: the client was to upload and retry
    builder.claim(drvB);     // still in its call
    auto const now = Clock::now();
    builder.tick(now + std::chrono::seconds(30));
    assert(session.dones().empty() && builder.holding() == 2);
    builder.tick(now + past);
    auto const dones = session.dones();
    assert(dones.size() == 1 && dones[0].drv_path() == drvA && dones[0].assign_id() == 1);
    assert(dones[0].outcome() == Done::GIVEN_BACK && dones[0].reason() == Done::UNREACHABLE);
    assert(builder.holding() == 1);
    builder.callEnded(drvB);
    builder.tick(Clock::now() + past);
    assert(session.dones().size() == 2 && builder.holding() == 0);
}

// Draining keeps a claimed Expect while its client may still upload, and
// gives it back once that client has gone quiet, so the drain can end.
void testDrainingGivesBackAQuietClaim()
{
    auto const opts = options();
    Metrics metrics{""};
    Builder builder(opts, metrics);
    Session session;
    session.attach(builder);
    builder.onExpect(expect(drvA, 1));
    builder.claim(drvA);
    builder.callEnded(drvA);
    builder.setDraining(true);
    builder.tick();
    assert(session.dones().empty() && builder.holding() == 1);
    builder.tick(Clock::now() + past);
    auto const dones = session.dones();
    assert(dones.size() == 1 && dones[0].reason() == Done::DRAINING);
    assert(builder.holding() == 0);
}

// A client claimed drvA (NOT_FOUND) and uploads its inputs to this builder
// (x-nix-drv, a Call) for longer than expectTimeout. The claim stays while
// the upload runs, and the end of the upload is the client's latest call:
// the next tick keeps the claim, and only expectTimeout after that does a
// quiet claim go.
void testUploadKeepsTheClaim()
{
    auto const opts = options();
    Metrics metrics{""};
    Builder builder(opts, metrics);
    Session session;
    session.attach(builder);
    builder.onExpect(expect(drvA, 1));
    builder.claim(drvA);
    builder.callEnded(drvA);
    {
        Builder::Call const upload(builder, drvA);
        builder.tick(Clock::now() + past);
        assert(session.dones().empty() && builder.holding() == 1);
    }
    builder.tick(Clock::now() + std::chrono::milliseconds(1));
    assert(session.dones().empty() && builder.holding() == 1);
    builder.tick(Clock::now() + past);
    assert(session.dones().size() == 1 && builder.holding() == 0);
}

} // namespace
// NOLINTEND(*-magic-numbers, *-avoid-unchecked-container-access)

auto main() -> int
try {
    testExpectWhileDrainingIsGivenBackLater();
    testRunningBuildTakesTheNewPlacement();
    testQuietClaimIsGivenBack();
    testDrainingGivesBackAQuietClaim();
    testUploadKeepsTheClaim();
    return 0;
} catch (std::exception & err) {
    std::cerr << err.what() << '\n';
    return 1;
}
