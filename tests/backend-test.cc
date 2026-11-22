// The settings a build's nix-daemon connection overrides: the client's
// session reaches Nix as `build-client`, and no well-formed session names
// no client.

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <iostream>
#include <map>
#include <optional>
#include <string>

#include "backend.hh"

namespace {

using Limits = nixgrpc::Backends::Limits;
using Overrides = std::map<std::string, std::string>;

void sessionNamesTheClient()
{
    constexpr uint64_t timeout = 7;
    assert((Limits{.buildTimeout = timeout, .client = "c0ffee"}.overrides()
            == Overrides{{"build-client", "c0ffee"}, {"builders", ""}, {"timeout", "7"}}));
    assert((Limits{}.overrides() == Overrides{{"builders", ""}, {"timeout", "0"}}));
    // A caller's own settings stay, under the build's.
    assert((Limits{.client = "c0ffee"}.overrides({{"narinfo-cache-positive-ttl", "0"}, {"timeout", "9"}})
            == Overrides{
                {"build-client", "c0ffee"}, {"builders", ""}, {"narinfo-cache-positive-ttl", "0"}, {"timeout", "0"}}));
}

// A well-formed session names the client, under an authenticated name;
// any other names none.
void onlyAWellFormedSessionNamesAClient()
{
    using nixgrpc::buildClient;
    constexpr std::size_t sessionLength = 32;
    std::string const session(sessionLength, 'a');
    assert(buildClient(std::nullopt, session) == session);
    assert(buildClient("a", session) == "a/" + session);
    // An anonymous caller passing for a named one.
    assert(buildClient(std::nullopt, "victim/" + session).empty());
    // A named caller passing for another: a's "b/s" would read as a/b's "s".
    assert(buildClient("a", "b/" + session).empty());
    // Tribuchet's own names for a daemon connection: boot/pid/start.
    assert(buildClient(std::nullopt, "0e9a3bfd-2c5f-4777-9191-15325dfac465/1016/1623").empty());
    assert(buildClient(std::nullopt, std::string(sessionLength, 'A')).empty());
    assert(buildClient(std::nullopt, "").empty());
}

} // namespace

auto main() -> int
{
    try {
        sessionNamesTheClient();
        onlyAWellFormedSessionNamesAClient();
    } catch (std::exception & err) {
        std::cerr << "error: " << err.what() << "\n";
        return 1;
    }
    return 0;
}
