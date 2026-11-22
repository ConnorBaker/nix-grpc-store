// queryMissing with an evaluation store, as `nix build --store grpc://…
// --eval-store auto` asks it before building: loads the plugin as nix loads
// plugin-files, writes a graph of derivations to a local evaluation store,
// asks grpc://<host:port>?insecure=true, and asks Nix's own walk over the
// local store the daemon serves (--proxy-store) for the answer to compare.
// For query-missing-test.py, which counts the QueryValidPaths RPCs.
//
//   chain N: N derivations in a chain, none of their outputs anywhere.
//            Once queryMissing returned, the top output is added, and the
//            grpc store must say it is valid: nothing it asked for the
//            walk may answer for it later.
//   built N: N derivations whose outputs the build store has.
//   mixed:   outputs valid on the build store, missing, substitutable (from
//            the file:// cache that is the only substituter), a derivation
//            with two outputs wanted apart, floating content-addressed and
//            deferred derivations, store paths, targets nobody has, and
//            derivations only the build store has.
//   nosubst: a derivation that disallows substitutes, whose output's
//            narinfo in the substituter is corrupt. Nix never asks for it.
//   probe:   a derivation nobody built. While the grpc store's
//            queryMissing runs, its output is added; another grpc store,
//            and the same one on another thread, must see it valid.
//   opaque N: N store paths as targets, as `nix path-info` gives its
//            arguments, every other one valid on the build store.
//   deep D W: a target only that substituter has, over D levels of W
//            derivations nobody has, each depending on all of the level
//            below.
//   toposort N: `nix path-info`, and printMissing's next step: the grpc
//            store's topoSortPaths() of N derivations it does not have
//            (only the evaluation store has them) and two paths it has,
//            then a lookup of each; no queryMissing.
//   toposort-probe: a path the sort found absent is added while the sort
//            runs; the same grpc store must find it on another thread,
//            and on this one once the sort is over.
//
// usage: query-missing-client <nix-grpc-store plugin> <host:port> <build> <dir> chain <N> | built <N> | opaque <N> | mixed | deep <depth> <width> | nosubst | probe | toposort <N> | toposort-probe
// <build> is the root of the daemon's local store, <dir>/eval of the
// evaluation store, and <build>-cache the substituter NIX_CONFIG must name,
// for the client and the daemon alike: a Nix that leaves what the build
// store can see to its queryMissing asks the daemon about substitutes.
// exit 0: the answers match, 1: they do not (or a stale validity), 3: the
// client could not run the scenario, 77: this Nix has no queryMissing with
// an evaluation store.

#include <cstddef>
#include <exception>
#include <iostream>
#include <iterator>
#include <span>
#include <string>
#include <vector>

#include <nix/store/globals.hh>

#include "load-plugin.hh"
#include "nix-compat.hh"

#if NIX_COMPAT_HAS_QUERY_MISSING_EVAL_STORE && NIX_COMPAT_TEMPLATED_DRV
#include <algorithm>
#include <fstream>
#include <functional>
#include <string_view>
#include <thread>
#include <utility>

#include <nix/store/content-address.hh>
#include <nix/store/derivations.hh>
#include <nix/store/derived-path.hh>
#include <nix/store/outputs-spec.hh>
#include <nix/store/path-info.hh>
#include <nix/store/path.hh>
#include <nix/store/store-api.hh>
#include <nix/store/store-open.hh>
#include <nix/util/archive.hh>
#include <nix/util/error.hh>
#include <nix/util/finally.hh>
#include <nix/util/hash.hh>
#include <nix/util/logging.hh>
#include <nix/util/repair-flag.hh>
#include <nix/util/serialise.hh>
#include <nix/util/types.hh>
#endif

namespace {

constexpr int broken = 3;

// <plugin> <host:port> <build> <dir> <scenario> [N]
struct ClientArgs
{
    std::string target;
    std::string build;
    std::string dir;
    std::string scenario;
    std::vector<std::string> rest;
};

#if NIX_COMPAT_HAS_QUERY_MISSING_EVAL_STORE && NIX_COMPAT_TEMPLATED_DRV

using namespace nix;

constexpr int differ = 1;

auto built(const StorePath & drvPath, OutputsSpec outputs) -> DerivedPath
{
    return DerivedPath::Built{.drvPath = makeConstantStorePathRef(drvPath), .outputs = std::move(outputs)};
}

auto out(const StorePath & drvPath) -> DerivedPath
{
    return built(drvPath, OutputsSpec::Names{"out"});
}

struct Graph
{
    Store * eval;

    // Input-addressed unless floating; inputs are (derivation, output).
    [[nodiscard]] auto drv(const std::string & name, const std::vector<std::pair<StorePath, std::string>> & inputs,
                           const StringSet & outputs = {"out"}, bool floating = false, const StringMap & env = {}) const
        -> StorePath
    {
        Derivation drv;
        drv.name = name;
        drv.env = env;
        drv.platform = "x86_64-linux";
        drv.builder = "/bin/sh";
        for (const auto & output : outputs) {
            if (floating) {
                drv.outputs.insert_or_assign(
                    output,
                    DerivationOutput::CAFloating{
                        .method = ContentAddressMethod::Raw::NixArchive, .hashAlgo = HashAlgorithm::SHA256});
                drv.env[output] = hashPlaceholder(output);
            } else {
                drv.outputs.insert_or_assign(output, DerivationOutput::Deferred{});
                drv.env[output] = "";
            }
        }
        for (const auto & [input, output] : inputs) {
            drv.inputs.insert(SingleDerivedPath::Built{.drvPath = makeConstantStorePathRef(input), .output = output});
        }
        derivation::fillInOutputPaths(drv, *eval);
        return eval->writeDerivation(drv);
    }

    [[nodiscard]] auto output(const StorePath & drvPath, const std::string & name) const -> StorePath
    {
        auto const path = eval->queryStaticPartialDerivationOutputMap(drvPath).at(name);
        if (!path) {
            throw Error("output '%s' of '%s' has no static path", name, eval->printStorePath(drvPath));
        }
        return *path;
    }
};

// Registers path in store with a one-file NAR, claiming references.
void add(Store & store, const StorePath & path, const StorePathSet & references = {})
{
    StringSink nar;
    dumpString(std::string(path.name()), nar);
    ValidPathInfo info{path, UnkeyedValidPathInfo{store, hashString(HashAlgorithm::SHA256, nar.s)}};
    info.narSize = nar.s.size();
    info.references = references;
    StringSource source{nar.s};
    store.addToStore(info, source, NoRepair, NoCheckSigs);
}

auto chain(Graph & graph, std::size_t length) -> std::vector<DerivedPath>
{
    auto top = graph.drv("chain-0", {});
    for (std::size_t idx = 1; idx < length; ++idx) {
        top = graph.drv("chain-" + std::to_string(idx), {{top, "out"}});
    }
    return {out(top)};
}

auto builtAlready(Graph & graph, Store & build, std::size_t count) -> std::vector<DerivedPath>
{
    std::vector<DerivedPath> targets;
    for (std::size_t idx = 0; idx < count; ++idx) {
        auto const drvPath = graph.drv("built-" + std::to_string(idx), {});
        add(build, graph.output(drvPath, "out"));
        targets.push_back(out(drvPath));
    }
    return targets;
}

auto mixed(Graph & graph, Store & build, Store & cache) -> std::vector<DerivedPath>
{
    auto const drvA = graph.drv("a", {});
    auto const drvB = graph.drv("b", {{drvA, "out"}});
    auto const drvC = graph.drv("c", {{drvB, "out"}}, {"out", "dev"});
    auto const drvD = graph.drv("d", {{drvC, "dev"}, {drvB, "out"}});
    auto const drvE = graph.drv("e", {});
    auto const drvF = graph.drv("f", {{drvB, "out"}}, {"out"}, true);
    auto const drvG = graph.drv("g", {{drvF, "out"}});
    auto const drvH = graph.drv("h", {});
    // Valid on the build store: a's output (also in the cache, under c's) and e's.
    add(build, graph.output(drvA, "out"));
    add(build, graph.output(drvE, "out"));
    add(cache, graph.output(drvA, "out"));
    // Substitutable: c's out but not its dev, h's out, and a store path.
    // h's refers to a path the build store has (and the cache), which only
    // the walk reaches.
    add(cache, graph.output(drvC, "out"), {graph.output(drvA, "out")});
    StorePath const held{"00000000000000000000000000000004-held"};
    add(build, held);
    add(cache, held);
    add(cache, graph.output(drvH, "out"), {held});
    StorePath const fetched{"00000000000000000000000000000001-fetched"};
    add(cache, fetched, {graph.output(drvA, "out")});
    // Derivations only the build store has: one to build, one built, one
    // the cache has (substitutable only with the client's substituter).
    Graph const farm{&build};
    auto const farmOnly = farm.drv("farm-only", {});
    auto const farmBuilt = farm.drv("farm-built", {});
    auto const farmCached = farm.drv("farm-cached", {});
    add(build, farm.output(farmBuilt, "out"));
    add(cache, farm.output(farmCached, "out"));
    return {
        out(farmOnly),
        out(farmBuilt),
        out(farmCached),
        out(drvD),
        out(drvC),
        built(drvC, OutputsSpec::All{}),
        out(drvE),
        out(drvG),
        out(drvH),
        DerivedPath::Opaque{graph.output(drvA, "out")},
        DerivedPath::Opaque{fetched},
        DerivedPath::Opaque{StorePath{"00000000000000000000000000000002-absent"}},
        out(StorePath{"00000000000000000000000000000003-absent.drv"}),
    };
}

// COUNT store paths given as such (as `nix path-info` gives its arguments),
// every other one valid on the build store.
auto opaquePaths(Store & build, std::size_t count) -> std::vector<DerivedPath>
{
    std::vector<DerivedPath> targets;
    for (std::size_t idx = 0; idx < count; ++idx) {
        auto const path = StorePath::random("opaque-" + std::to_string(idx));
        if (idx % 2 == 0) {
            add(build, path);
        }
        targets.push_back(DerivedPath::Opaque{path});
    }
    return targets;
}

// A target only the client's substituter has, over DEPTH levels of WIDTH
// derivations that nobody has: Nix's walk stops at the target.
// NOLINTNEXTLINE(bugprone-easily-swappable-parameters): depth, then width, as the scenario's arguments
auto deep(Graph & graph, Store & cache, std::size_t depth, std::size_t width) -> std::vector<DerivedPath>
{
    std::vector<std::pair<StorePath, std::string>> below;
    for (std::size_t lvl = 0; lvl < depth; ++lvl) {
        std::vector<std::pair<StorePath, std::string>> level;
        level.reserve(width);
        for (std::size_t idx = 0; idx < width; ++idx) {
            level.emplace_back(graph.drv("deep-" + std::to_string(lvl) + "-" + std::to_string(idx), below), "out");
        }
        below = std::move(level);
    }
    auto const top = graph.drv("deep-top", below);
    add(cache, graph.output(top, "out"));
    return {out(top)};
}

// A target that disallows substitutes, nobody has its output, and the
// substituter's narinfo for that output is corrupt.
auto noSubst(Graph & graph, const std::string & buildRoot) -> std::vector<DerivedPath>
{
    auto const drvPath = graph.drv("nosubst", {}, {"out"}, false, {{"allowSubstitutes", ""}});
    std::ofstream(buildRoot + "-cache/" + std::string(graph.output(drvPath, "out").hashPart()) + ".narinfo")
        << "garbage\n";
    return {out(drvPath)};
}

// Runs probe() at the first message containing `at`. For queryMissing, a
// substituter's check: by then it has asked ahead, and is still running.
class ProbeLogger : public Logger
{
public:
    std::function<void()> probe;
    std::string_view at = "checking substituter";

    void log(Verbosity /*lvl*/, std::string_view msg) noexcept override
    {
        if (probe && msg.contains(at)) {
            std::exchange(probe, nullptr)();
        }
    }

    void logEI(const ErrorInfo & /*info*/) noexcept override {}
};

// While queryMissing runs with the answers it asked ahead (top invalid), top
// becomes valid. Another grpc store, and this one on another thread, must
// ask the daemon, not read those answers.
// NOLINTNEXTLINE(bugprone-easily-swappable-parameters): the grpc store, then the build store
auto probeOthers(const ClientArgs & args, Store & grpc, Store & build, const StorePath & top) -> std::string
{
    add(build, top);
    std::string seen;
    try {
        auto const other = openStore("grpc://" + args.target + "?insecure=true");
        seen += other->queryValidPaths({top}).contains(top) ? "other store: live" : "other store: STALE";
        bool fromThread = false;
        std::thread thread([&] -> void { fromThread = grpc.queryValidPaths({top}).contains(top); });
        thread.join();
        seen += fromThread ? ", other thread: live" : ", other thread: STALE";
    } catch (const std::exception & err) {
        seen += std::string(" ERROR ") + err.what();
    }
    return seen;
}

void print(const char * label, const MissingPaths & missing)
{
    auto const paths = [](const StorePathSet & set) -> std::string {
        std::string res;
        for (const auto & path : set) {
            res += " " + std::string(path.to_string());
        }
        return res;
    };
    std::cout << label << " willBuild:" << paths(missing.willBuild) << "\n"
              << label << " willSubstitute:" << paths(missing.willSubstitute) << "\n"
              << label << " unknown:" << paths(missing.unknown) << "\n"
              << label << " downloadSize: " << missing.downloadSize << " narSize: " << missing.narSize << "\n";
}

// Whether the store finds the path.
auto finds(Store & store, const StorePath & path) -> bool
{
    try {
        static_cast<void>(store.queryPathInfo(path));
        return true;
    } catch (InvalidPath &) {
        return false;
    }
}

// What `nix path-info` does through the grpc store: sort `count`
// derivations only the evaluation store has, and two paths the farm has
// (one referring to the other), then look up each path. Present paths keep
// their references and infos, absent ones are InvalidPath.
auto topoSortAbsent(const ClientArgs & args, Graph & graph, Store & build, std::size_t count) -> int
{
    StorePathSet asked;
    for (std::size_t idx = 0; idx < count; ++idx) {
        asked.insert(graph.drv("toposort-" + std::to_string(idx), {}));
    }
    StorePath const high{"00000000000000000000000000000007-toposort-high"};
    StorePath const low{"00000000000000000000000000000008-toposort-low"};
    add(build, low);
    add(build, high, {low});
    asked.insert(high);
    asked.insert(low);
    auto const grpc = openStore("grpc://" + args.target + "?insecure=true");
    auto const sorted = grpc->topoSortPaths(asked);
    auto const found = std::ranges::count_if(asked, [&grpc](const StorePath & path) -> bool { return finds(*grpc, path); });
    auto const at = [&sorted](const StorePath & path) -> StorePaths::const_iterator { return std::ranges::find(sorted, path); };
    std::cout << "SORTED " << sorted.size() << " of " << asked.size() << ", FOUND " << found << "\n";
    return StorePathSet(sorted.begin(), sorted.end()) == asked && found == 2 && at(high) < at(low) ? 0 : differ;
}

// While the grpc store sorts a path its prefetch found absent, the path
// becomes valid: the same store on another thread must ask the daemon, not
// read the sort's answer, and so must this thread once the sort is over.
auto topoSortProbe(const ClientArgs & args, Store & build) -> int
{
    StorePath const late{"00000000000000000000000000000009-toposort-late"};
    auto const grpc = openStore("grpc://" + args.target + "?insecure=true");
    auto const seen = [&grpc, &late]() -> std::string {
        grpc->clearPathInfoCache();
        return finds(*grpc, late) ? "live" : "STALE";
    };
    std::string during = "never ran";
    ProbeLogger probeLogger;
    probeLogger.at = "sorting";
    probeLogger.probe = [&] -> void {
        add(build, late);
        std::thread thread([&] -> void { during = seen(); });
        thread.join();
    };
    auto * const saved = logger;
    auto const verbose = verbosity;
    Finally const restore([&] -> void {
        logger = saved;
        verbosity = verbose;
    });
    verbosity = lvlDebug;
    logger = &probeLogger;
    static_cast<void>(grpc->topoSortPaths({late}));
    auto const after = seen();
    std::cout << "SORT-PROBE other thread: " << during << ", after: " << after << "\n";
    if (during == "never ran") {
        return broken;
    }
    return during == "live" && after == "live" ? 0 : differ;
}

auto run(const ClientArgs & args) -> int
{
    auto const & scenario = args.scenario;
    auto const eval = openStore("local?root=" + args.dir + "/eval");
    auto const build = openStore("local?root=" + args.build);
    auto const cache = openStore("file://" + args.build + "-cache?compression=none");
    Graph graph{&*eval};
    if (scenario == "toposort-probe" && args.rest.empty()) {
        return topoSortProbe(args, *build);
    }
    if (scenario == "toposort" && args.rest.size() == 1) {
        return topoSortAbsent(args, graph, *build, std::stoul(args.rest.front()));
    }
    std::vector<DerivedPath> targets;
    if (scenario == "chain" && args.rest.size() == 1) {
        targets = chain(graph, std::stoul(args.rest.front()));
    } else if (scenario == "built" && args.rest.size() == 1) {
        targets = builtAlready(graph, *build, std::stoul(args.rest.front()));
    } else if (scenario == "opaque" && args.rest.size() == 1) {
        targets = opaquePaths(*build, std::stoul(args.rest.front()));
    } else if (scenario == "deep" && args.rest.size() == 2) {
        targets = deep(graph, *cache, std::stoul(args.rest.at(0)), std::stoul(args.rest.at(1)));
    } else if (scenario == "mixed" && args.rest.empty()) {
        targets = mixed(graph, *build, *cache);
    } else if (scenario == "nosubst" && args.rest.empty()) {
        targets = noSubst(graph, args.build);
    } else if (scenario == "probe" && args.rest.empty()) {
        targets = {out(graph.drv("probe", {}))};
    } else {
        std::cerr << "unknown scenario '" << scenario << "'\n";
        return broken;
    }
    auto const want = build->queryMissing(targets, &*eval);
    auto const grpc = openStore("grpc://" + args.target + "?insecure=true");
    ProbeLogger probeLogger;
    auto * const saved = logger;
    auto const verbose = verbosity;
    Finally const restore([&] -> void {
        logger = saved;
        verbosity = verbose;
    });
    std::string probed;
    if (scenario == "probe") {
        auto const & drvPath = std::get<DerivedPath::Built>(targets.front().raw()).drvPath->getBaseStorePath();
        probeLogger.probe = [&, top = graph.output(drvPath, "out")] -> void {
            probed = probeOthers(args, *grpc, *build, top);
        };
        verbosity = lvlDebug;
        logger = &probeLogger;
    }
    MissingPaths got;
    try {
        got = grpc->queryMissing(targets, &*eval);
    } catch (const std::exception & err) {
        std::cout << "GRPC FAILED: " << err.what() << "\n";
        return differ;
    }
    if (scenario == "probe") {
        logger = saved;
        verbosity = verbose;
        std::cout << "PROBE " << (probed.empty() ? "never ran" : probed) << "\n";
        if (probed != "other store: live, other thread: live") {
            return probed.empty() ? broken : differ;
        }
    }
    print("grpc", got);
    print("nix", want);
    // Paths to substitute only with a substituter (not in mixed-own).
    if (scenario == "mixed"
        && (want.willBuild.empty() || want.willSubstitute.empty() != getDefaultSubstituters().empty()
            || want.unknown.empty())) {
        std::cerr << "the mixed graph must have paths to build, to substitute (with a substituter) and unknown ones\n";
        return broken;
    }
    if (got.willBuild != want.willBuild || got.willSubstitute != want.willSubstitute || got.unknown != want.unknown
        || got.downloadSize != want.downloadSize || got.narSize != want.narSize) {
        std::cout << "MISMATCH\n";
        return differ;
    }
    std::cout << "MATCH\n";
    if (scenario == "chain") {
        auto const & topDrv = std::get<DerivedPath::Built>(targets.front().raw()).drvPath->getBaseStorePath();
        auto const top = graph.output(topDrv, "out");
        add(*build, top);
        if (!grpc->isValidPath(top)) {
            std::cout << "STALE: added " << top.to_string() << " after queryMissing, the grpc store still says invalid\n";
            return differ;
        }
        std::cout << "LIVE\n";
    }
    return 0;
}

#else

// meson's exit status for a skipped test.
constexpr int skipped = 77;

auto run(const ClientArgs & /*args*/) -> int
{
    std::cout << "this Nix has no queryMissing with an evaluation store\n";
    return skipped;
}

#endif

} // namespace

auto main(int argc, char ** argv) -> int
try {
    std::span const argSpan(argv, static_cast<size_t>(argc));
    std::vector<std::string> const args(std::next(argSpan.begin()), argSpan.end());
    constexpr std::size_t fixed = 5;
    if (args.size() < fixed) {
        std::cerr << "usage: query-missing-client <nix-grpc-store plugin> <host:port> <build> <dir> chain <N> | built <N> | opaque <N> | mixed | deep <depth> <width> | nosubst | probe | toposort <N> | toposort-probe\n";
        return broken;
    }
    auto arg = args.begin();
    std::string const & plugin = *arg++;
    ClientArgs const parsed{
        .target = *arg++, .build = *arg++, .dir = *arg++, .scenario = *arg++, .rest = {arg, args.end()}};
    nix::initLibStore();
    loadPlugin(plugin);
    return run(parsed);
} catch (const std::exception & err) {
    std::cerr << "error: " << err.what() << "\n";
    return broken;
}
