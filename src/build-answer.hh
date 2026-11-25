#pragma once
// Whether a BuildDerivation status is the worker's own answer. The worker
// marks it, the client reads it; both live here so the wire name has one
// owner.

#include <string>
#include <string_view>

#include <grpcpp/client_context.h>
#include <grpcpp/server_context.h>
#include <grpcpp/support/string_ref.h>

namespace nixgrpc {

// Trailing metadata on every status the worker's BuildDerivation handler
// returns. A status without it the handler did not return: the stream
// broke, or gRPC cut the call off, under it; or the worker predates the
// mark.
inline constexpr std::string_view answeredKey = "x-nix-answered";

// As the handler returns: the status it returns is an answer.
inline void markAnswered(grpc::ServerContext & context)
{
    context.AddTrailingMetadata(std::string(answeredKey), "1");
}

// After the call's Finish.
inline auto answered(const grpc::ClientContext & context) -> bool
{
    return context.GetServerTrailingMetadata().contains(grpc::string_ref(answeredKey.data(), answeredKey.size()));
}

} // namespace nixgrpc
