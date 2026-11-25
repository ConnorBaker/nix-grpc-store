#pragma once
// What a bounce of a build means for its job (exec::Bounce), read off how
// the call to the assigned worker ended: its status (build.cc
// buildAssigned), or the recovery of an input it lacked (Run::recover).

#include <grpcpp/support/status.h>

#include "executor.hh"
#include "retry.hh"

namespace nixgrpc {

// The worker does not hold the build: it never got the call (the balancer
// could not connect to it) or turned it down.
inline auto notTaken(const grpc::Status & status) -> bool
{
    auto const & msg = status.error_message();
    switch (status.error_code()) {
    case grpc::StatusCode::FAILED_PRECONDITION: // not scheduled there
        return true;
    case grpc::StatusCode::UNAVAILABLE:
        return msg.contains("connection failure") || msg.contains("connection timeout");
    default:
        return false;
    }
}

// Of what workerLeft lists, what may leave the build running for us there:
// the stream to the worker broke under the call, whatever code gRPC gives
// that (UNKNOWN, INTERNAL, or UNAVAILABLE "Stream removed"), or the worker
// shut down or handed over under it (CANCELLED); the build goes on, and
// the scheduler sends us back to it. Or another call already builds the
// drv (ABORTED), and the scheduler sends us there. After any other end the
// worker does not build it for us: it was not reachable, refused the call,
// or answered that it gave the build back (draining, out of space, a
// transient failure, an input it could not get), and the scheduler places
// the drv again at once.
// `answered`: the worker's handler returned the status (build-answer.hh),
// so the stream did not break under the call, whatever its message quotes
// (a gRPC error the worker wraps, a build's log). Without the mark, from a
// worker that predates it, only the message tells an answer.
inline auto buildGoesOn(const grpc::Status & status, bool answered) -> bool
{
    // Never with `refused`: the next Want names a refused placement, and
    // the scheduler revokes it, so no build goes on there for us.
    if (notTaken(status)) {
        return false;
    }
    switch (status.error_code()) {
    case grpc::StatusCode::ABORTED:
        return true;
    case grpc::StatusCode::CANCELLED:
    case grpc::StatusCode::UNKNOWN:
    case grpc::StatusCode::INTERNAL:
        return !answered;
    case grpc::StatusCode::UNAVAILABLE:
        // Not an answer of the worker's that quotes a broken stream.
        return !answered && streamBroken(status) && cutOff(status);
    default:
        return false;
    }
}

// A call to the assigned worker that ended with `status`, one workerLeft
// lists. `refused` names the assignment in the next Want, so the scheduler
// places it elsewhere and revokes it there: only for a worker that does not
// hold the build. `left` makes the first such bounce per worker free.
// `held` keeps following a build that goes on (Executor::bounced).
// `answered` as buildGoesOn takes it.
inline auto bounceOf(const grpc::Status & status, bool answered) -> exec::Bounce
{
    return {
        .refused = notTaken(status),
        .left = notTaken(status) || goneAway(status),
        .held = buildGoesOn(status, answered)};
}

// The worker gave the build back for an input it lacked, which Run::recover
// uploaded again or found still missing: it builds nothing for us, and the
// scheduler places the drv again at once.
inline auto inputGivenBack() -> exec::Bounce
{
    return {};
}

} // namespace nixgrpc
