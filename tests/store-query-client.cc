// A store query as nix makes it through the grpc:// store: loads the plugin
// as nix loads plugin-files, opens grpc://<host:port>?insecure=true and asks
// QueryValidPaths, then again after each line on stdin. One line per query
// on stdout, written once the query returned, for client-keepalive-test.py.
//
// usage: store-query-client <nix-grpc-store plugin> <host:port>

#include <cstddef>
#include <exception>
#include <iostream>
#include <iterator>
#include <span>
#include <string>

#include <nix/store/globals.hh>
#include <nix/store/path.hh>
#include <nix/store/store-api.hh>
#include <nix/store/store-open.hh>
#include <nix/util/error.hh>

#include "load-plugin.hh"

namespace {

// The answer or the error, in one write: the plugin logs its retries to
// stderr meanwhile, which the test reads from the same pipe.
void ask(nix::Store & store, const nix::StorePath & path)
{
    std::string line;
    try {
        line = "answered " + std::to_string(store.queryValidPaths({path}).size());
    } catch (const nix::Error & err) {
        line = "failed " + err.msg();
    }
    std::cout << (line + "\n");
}

} // namespace

auto main(int argc, char ** argv) -> int
try {
    std::span const args(argv, static_cast<size_t>(argc));
    if (args.size() != 3) {
        std::cerr << "usage: store-query-client <nix-grpc-store plugin> <host:port>\n";
        return 2;
    }
    std::string const plugin = *std::next(args.begin());
    std::string const target = *std::next(args.begin(), 2);
    // The test reads each line as it comes.
    std::cout << std::unitbuf;
    nix::initLibStore();
    loadPlugin(plugin);
    auto const store = nix::openStore("grpc://" + target + "?insecure=true");
    auto const path = store->parseStorePath("/nix/store/00000000000000000000000000000000-store-query");
    ask(*store, path);
    std::string request;
    while (std::getline(std::cin, request)) {
        ask(*store, path);
    }
    return 0;
} catch (const std::exception & err) {
    std::cerr << "error: " << err.what() << "\n";
    return 1;
}
