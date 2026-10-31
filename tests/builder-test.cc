// Unit tests for the Builder's side of the scheduler protocol, with an
// in-memory session (no gRPC).

#include <cassert>
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
using nixgrpc::Metrics;
using nixgrpc::Options;

// NOLINTBEGIN(*-magic-numbers, *-avoid-unchecked-container-access): test vectors, asserted sizes.
namespace {

constexpr auto drvA = "/nix/store/00000000000000000000000000000000-a.drv";

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

} // namespace
// NOLINTEND(*-magic-numbers, *-avoid-unchecked-container-access)

auto main() -> int
try {
    testExpectWhileDrainingIsGivenBackLater();
    return 0;
} catch (std::exception & err) {
    std::cerr << err.what() << '\n';
    return 1;
}
