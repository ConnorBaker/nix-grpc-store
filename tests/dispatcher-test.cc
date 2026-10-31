// Unit tests for the Dispatcher's worker messages, with in-memory peers (no gRPC).

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <vector>

#include "dispatcher.hh"
#include "metrics.hh"
#include "nix_remote.pb.h"

using nix::remote::ClientMsgs;
using nix::remote::SchedCmd;
using nix::remote::SchedMsg;
using nix::remote::WorkerMsgs;
using nixgrpc::Dispatcher;
using nixgrpc::Metrics;

namespace {

auto assigned(const std::vector<SchedMsg> & msgs) -> size_t
{
    return static_cast<size_t>(std::ranges::count_if(msgs, [](const SchedMsg & msg) -> bool { return msg.has_assigned(); }));
}

// Load.max_jobs changes a builder's slots without a reconnect, as after
// SIGHUP (Builder::reloadNixConfig); a Load without it, as older builders
// send, leaves them. The builder starts with none: older builders say Hello
// with max_jobs 0 while they drain.
void testLoadRestoresSlotsAfterDrainingHello()
{
    Metrics metrics{""};
    Dispatcher disp(Dispatcher::Config{.defaultSystem = "x86_64-linux", .present = {}, .lookupThreads = 0}, metrics);
    Dispatcher::Worker worker{.send = [](const SchedCmd &) -> bool { return true; }, .id = {}};
    WorkerMsgs hello;
    auto * hel = hello.add_msgs()->mutable_hello();
    hel->set_addr("w0:50051");
    hel->add_systems("x86_64-linux");
    hel->set_max_jobs(0);
    disp.workerMsgs(worker, hello);

    std::vector<SchedMsg> toClient;
    auto client = disp.connectClient(
        [&toClient](const SchedMsg & msg) -> bool {
            toClient.push_back(msg);
            return true;
        },
        [] -> void {});
    ClientMsgs want;
    want.add_msgs()->mutable_want()->set_drv_path("/nix/store/00000000000000000000000000000000-a.drv");
    disp.clientMsgs(client, want);

    WorkerMsgs older;
    older.add_msgs()->mutable_load()->set_draining(false);
    disp.workerMsgs(worker, older);
    assert(assigned(toClient) == 0);
    assert(disp.membership()->at("x86_64-linux").front().draining);

    WorkerMsgs load;
    auto * recovered = load.add_msgs()->mutable_load();
    recovered->set_draining(false);
    recovered->set_max_jobs(2);
    disp.workerMsgs(worker, load);
    assert(assigned(toClient) == 1);
    assert(!disp.membership()->at("x86_64-linux").front().draining);

    disp.clientGone(client);
    disp.workerGone(worker);
}

} // namespace

auto main() -> int
{
    testLoadRestoresSlotsAfterDrainingHello();
    return 0;
}
