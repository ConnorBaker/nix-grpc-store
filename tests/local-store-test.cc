// What nix throws when its daemon cannot be reached must count as a lost
// daemon. The errors come from nix itself, talking to sockets that are
// missing, refuse, or hang up, so a nix that words them differently fails
// here.

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstdlib>
#include <exception>
#include <filesystem>
#include <print>
#include <set>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include <signal.h> // NOLINT(modernize-deprecated-headers): SIGPIPE is POSIX, not in <csignal>.
#include <sys/socket.h>
#include <sys/stat.h> // NOLINT(misc-include-cleaner): which header has S_IRWXU differs per platform.
#include <unistd.h>

#include <nix/store/derivations.hh>
#include <nix/store/globals.hh>
#include <nix/store/path-info.hh>
#include <nix/store/path.hh>
#include <nix/store/store-api.hh>
#include <nix/util/archive.hh>
#include <nix/util/hash.hh>
#include <nix/util/repair-flag.hh>
#include <nix/util/error.hh> // NOLINT(misc-include-cleaner): which header has SysError differs per Nix version.
#include <nix/util/serialise.hh>
#include <nix/util/terminal.hh>
#include <nix/util/unix-domain-socket.hh>

#include "client-stream.hh"
#include "local-store.hh"
#include "nix-compat.hh"
#include "nix_remote.pb.h"
#include "pump.hh"
#include "upload-spool.hh"

namespace {

using nixgrpc::LocalStore;

// The errors of two calls in a row: a store that failed to connect answers
// the second one differently.
auto errorsOf(const std::filesystem::path & socket) -> std::vector<std::string>
{
    std::vector<std::string> seen;
    LocalStore local("unix://" + socket.string());
    for (int call = 0; call < 2; call++) {
        try {
            local.shared()->isValidPath(nix::StorePath("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-x"));
            seen.emplace_back("no error");
        } catch (const std::exception & err) {
            seen.push_back(LocalStore::lost(err) ? err.what() : std::string("not lost: ") + err.what());
        }
    }
    return seen;
}

// A client's stream that has ended, as the upload handler sees it.
struct EndedStream
{
    static auto Read(nix::remote::AddMultipleChunk * /*msg*/) -> bool { return false; }
};

auto endedUploadIsLost() -> bool
{
    EndedStream ended;
    nixgrpc::ClientStream stream(ended);
    nixgrpc::ZstdReaderSource<nixgrpc::ClientStream<EndedStream>, nix::remote::AddMultipleChunk> source(stream, "");
    try {
        std::array<char, 1> byte{};
        source.read(byte.data(), byte.size());
    } catch (const std::exception & err) {
        return LocalStore::lost(err);
    }
    return false;
}

// nix parses the NAR it stores; one that goes on after its declared size
// makes it read past what was spooled.
auto longNarIsLost() -> bool
{
    nixgrpc::NarSpool spool;
    std::string const declared = "nar";
    nix::StringSource upload(declared);
    auto reader = spool.reader(spool.add(upload, declared.size()));
    try {
        std::array<char, 2> bytes{};
        while (true) {
            reader.read(bytes.data(), bytes.size());
        }
    } catch (const std::exception & err) {
        return LocalStore::lost(err);
    }
}

// A drv with these outputs, written to `store`, and its path as clients name
// it (StorePath::to_string); none of the outputs valid yet.
auto writeDrv(nix::Store & store, const std::vector<std::string> & outputs) -> std::pair<nix::Derivation, std::string>
{
    nix::Derivation drv;
    drv.name = "built-here";
    drv.platform = "x86_64-linux";
    drv.builder = "/bin/sh";
    for (const auto & output : outputs) {
        drv.outputs.emplace(output, nix::DerivationOutput::Deferred{});
        // As the evaluator does: each output's variable, empty until its path is in.
        drv.env.emplace(output, "");
    }
    nixcompat::fillInOutputPaths(drv, store);
    auto const path = std::string(store.writeDerivation(drv).to_string());
    return {std::move(drv), path};
}

// One of the drv's outputs made valid, a file holding its name: its path and
// NAR size.
auto addOutput(nix::Store & store, const nix::Derivation & drv, const std::string & output) -> LocalStore::Outputs::value_type
{
    nix::StringSink nar;
    nix::dumpString(output, nar);
    auto const path = std::get<nix::DerivationOutput::InputAddressed>(drv.outputs.at(output).raw).path;
    nix::ValidPathInfo info(path, nix::UnkeyedValidPathInfo(store, nix::hashString(nix::HashAlgorithm::SHA256, nar.s)));
    info.narSize = nar.s.size();
    nix::StringSource source(nar.s);
    store.addToStore(info, source, nix::NoRepair, nix::NoCheckSigs);
    return {store.printStorePath(path), info.narSize};
}

// The check that settles a placement nobody claims (Builder::checkBuiltWith),
// against a real store: a drv the store lacks is not built here, and one with
// two outputs only once both are valid, and then with their NAR sizes.
auto builtHereNeedsEveryOutput(const std::filesystem::path & root) -> bool
{
    LocalStore local("local?root=" + root.string());
    auto const store = local.scoped();
    if (LocalStore::builtHereIn(*store, "00000000000000000000000000000000-absent.drv")) {
        return false;
    }
    auto const [drv, drvPath] = writeDrv(*store, {"out", "dev"});
    if (LocalStore::builtHereIn(*store, drvPath)) {
        return false;
    }
    auto const out = addOutput(*store, drv, "out");
    if (LocalStore::builtHereIn(*store, drvPath)) {
        return false;
    }
    auto const dev = addOutput(*store, drv, "dev");
    auto built = LocalStore::builtHereIn(*store, drvPath);
    if (!built) {
        return false;
    }
    std::ranges::sort(*built);
    auto want = LocalStore::Outputs{out, dev};
    std::ranges::sort(want);
    return *built == want;
}

// A store that failed to connect refuses every later attempt (errorsOf): a
// lost nix-daemon must leave the next ask a fresh pool.
auto aLostDaemonDropsThePool(const std::filesystem::path & socket) -> bool
{
    LocalStore local("unix://" + socket.string());
    auto const first = local.shared();
    try {
        local.builtHere("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-x.drv");
        return false;
    } catch (const std::exception & err) {
        if (!LocalStore::lost(err)) {
            return false;
        }
    }
    return local.shared().get() != first.get();
}

auto run() -> int
{
    nix::initLibStore();
    // As the daemon does. NOLINTNEXTLINE(misc-include-cleaner): which header has SIGPIPE differs per platform.
    (void) ::signal(SIGPIPE, SIG_IGN);
    auto const dir = std::filesystem::temp_directory_path() / ("local-store-test-" + std::to_string(::getpid()));
    std::filesystem::create_directory(dir);

    // Bound and closed: the file is there, nobody listens.
    // NOLINTNEXTLINE(misc-include-cleaner): which header has S_IRWXU differs per platform.
    nix::createUnixDomainSocket(dir / "refused", S_IRWXU).close();
    // NOLINTNEXTLINE(misc-include-cleaner)
    auto listener = nix::createUnixDomainSocket(dir / "hangup", S_IRWXU);
    std::thread closer([hangup = listener.get()] -> void {
        for (;;) {
            int const conn = ::accept(hangup, nullptr, nullptr);
            if (conn < 0) {
                return;
            }
            ::close(conn);
        }
    });

    int status = EXIT_SUCCESS;
    std::set<std::string_view> used;
    for (const auto * name : {"missing", "refused", "hangup"}) {
        for (const auto & what : errorsOf(dir / name)) {
            if (what.starts_with("no") || what.starts_with("not lost")) {
                std::println("FAIL {}: {}", name, what);
                status = EXIT_FAILURE;
            }
            auto const plain = nix::filterANSIEscapes(what, true);
            for (auto const needle : LocalStore::cannotOpen) {
                if (plain.contains(needle)) {
                    used.insert(needle);
                }
            }
        }
    }
    for (auto const needle : LocalStore::cannotOpen) {
        if (!used.contains(needle)) {
            std::println("FAIL no error of nix says \"{}\"", needle);
            status = EXIT_FAILURE;
        }
    }
    if (LocalStore::lost(nix::SysError(ENOENT, "opening file '/nix/var/log/nix/drvs/x'"))) {
        std::println("FAIL a missing file counts as a lost nix-daemon");
        status = EXIT_FAILURE;
    }

    bool needsEvery = false;
    try {
        needsEvery = builtHereNeedsEveryOutput(dir / "root");
    } catch (const std::exception & err) {
        std::println("FAIL a real store: {}", err.what());
    }
    if (!needsEvery) {
        std::println("FAIL a drv counts as built here with an output missing, or not with all of them");
        status = EXIT_FAILURE;
    }
    if (!aLostDaemonDropsThePool(dir / "missing")) {
        std::println("FAIL asking a lost nix-daemon whether a drv was built keeps the dead pool");
        status = EXIT_FAILURE;
    }

    if (endedUploadIsLost()) {
        std::println("FAIL an upload that ends early counts as a lost nix-daemon");
        status = EXIT_FAILURE;
    }

    if (longNarIsLost()) {
        std::println("FAIL a NAR longer than its declared size counts as a lost nix-daemon");
        status = EXIT_FAILURE;
    }

    // Linux wakes the accept on shutdown, macOS on close.
    ::shutdown(listener.get(), SHUT_RDWR);
    listener.close();
    closer.join();
    std::filesystem::remove_all(dir);
    return status;
}

// For built-here-test.py: a drv whose one output is valid in the store at
// `root`, as one a predecessor built; prints the drv's path.
auto builtDrv(const std::filesystem::path & root) -> int
{
    nix::initLibStore();
    LocalStore local("local?root=" + root.string());
    auto const store = local.scoped();
    auto const [drv, drvPath] = writeDrv(*store, {"out"});
    addOutput(*store, drv, "out");
    std::println("{}", drvPath);
    return EXIT_SUCCESS;
}

} // namespace

auto main(int argc, char ** argv) -> int
{
    try {
        auto const args = std::span(argv, static_cast<size_t>(argc)).subspan(1);
        if (args.size() == 2 && std::string_view(args.front()) == "--built-drv") {
            return builtDrv(args.back());
        }
        return run();
    } catch (...) {
        return EXIT_FAILURE;
    }
}
