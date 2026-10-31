// A Schedule client as the plugin's (build.cc: scheduleUntilDone, readSchedule):
// the channel arguments of every client of the daemon, a Call, one Want, and
// then a wait on the stream until it ends, with nothing more to send. One line
// per event on stdout, for client-keepalive-test.py. With a system no worker
// offers, the Want waits unplaced: one Unplaceable, then nothing either way.
//
// usage: keepalive-client <host:port> <drv path> [system]

#include <cstddef>
#include <exception>
#include <iostream>
#include <iterator>
#include <span>
#include <string>

#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>
#include <grpcpp/support/status.h>
#include <grpcpp/support/sync_stream.h>

#include "channel.hh"
#include "nix_remote.grpc.pb.h"
#include "nix_remote.pb.h"

auto main(int argc, char ** argv) -> int
try {
    std::span const args(argv, static_cast<size_t>(argc));
    if (args.size() != 3 && args.size() != 4) {
        std::cerr << "usage: keepalive-client <host:port> <drv path> [system]\n";
        return 2;
    }
    std::string const target = *std::next(args.begin());
    std::string const drvPath = *std::next(args.begin(), 2);
    // The test reads each line as it comes.
    std::cout << std::unitbuf;
    auto const sched = nix::remote::Scheduler::NewStub(
        grpc::CreateCustomChannel(target, grpc::InsecureChannelCredentials(), nixgrpc::daemonChannelArgs()));
    nixgrpc::Call call;
    auto stream = sched->Schedule(&call.ctx());
    nix::remote::ClientMsgs want;
    auto & wanted = *want.add_msgs()->mutable_want();
    wanted.set_drv_path(drvPath);
    if (args.size() == 4) {
        wanted.set_system(*std::next(args.begin(), 3));
    }
    if (!stream->Write(want)) {
        std::cout << "write failed\n";
    }
    nix::remote::SchedMsgs msgs;
    while (stream->Read(&msgs)) {
        for (const auto & msg : msgs.msgs()) {
            if (msg.has_assigned()) {
                std::cout << "assigned " << msg.assigned().drv_path() << "\n";
            } else if (msg.has_unplaceable()) {
                std::cout << "unplaceable " << msg.unplaceable().drv_path() << "\n";
            } else {
                std::cout << "other message\n";
            }
        }
    }
    auto const status = stream->Finish();
    std::cout << "ended " << static_cast<int>(status.error_code()) << " " << status.error_message() << "\n";
    return 0;
} catch (const std::exception & err) {
    std::cerr << "error: " << err.what() << "\n";
    return 1;
}
