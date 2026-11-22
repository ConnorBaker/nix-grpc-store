// grpc:// Store: batched path-info queries and resumable NAR upload.

#include "store.hh"

#include <array>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <ranges>
#include <stop_token>
#include <string>
#include <thread>
#include <utility>
#include <variant>
#include <vector>

#include <grpcpp/grpcpp.h>
#include <grpcpp/support/status.h>
#include <grpcpp/support/sync_stream.h>

#include <nix/store/derivations.hh>
#include <nix/store/derived-path.hh>
#include <nix/store/path-info.hh>
#include <nix/store/path.hh>
#include <nix/store/store-api.hh>
#include <nix/store/worker-protocol.hh>
#include <nix/util/error.hh>
#include <nix/util/finally.hh>
#include <nix/util/logging.hh>
#include <nix/util/repair-flag.hh>
#include <nix/util/serialise.hh>
#include <nix/util/types.hh>
#include <nix/util/util.hh>

#include "channel.hh"
#include "nix-compat.hh"
#include "nix_remote.grpc.pb.h"
#include "nix_remote.pb.h"
#include "path-info-wire.hh"
#include "pump.hh"
#include "top-down.hh"

namespace nix {

namespace {

// Store's own queryMissing, which walks the graph with this store's path
// queries, given the evaluation store where this Nix takes one.
auto walkMissing(Store & store, const std::vector<DerivedPath> & targets,
                 [[maybe_unused]] Store * evalStore) -> MissingPaths {
#if NIX_COMPAT_HAS_QUERY_MISSING_EVAL_STORE
  return store.Store::queryMissing(targets, evalStore);
#else
  return store.Store::queryMissing(targets);
#endif
}

} // namespace

auto GrpcStore::queryValidPathsRouted(const StorePathSet & paths, SubstituteFlag maybeSubstitute,
                           const Metadata & headers) -> StorePathSet {
  remote::QueryValidPathsRequest request;
  request.set_substitute(maybeSubstitute == Substitute);
  for (const auto &path : paths) {
    request.add_paths(std::string(path.to_string()));
  }
  remote::QueryValidPathsReply reply;
  retrying("QueryValidPaths", [&] -> grpc::Status {
    nixgrpc::Call call;
    addHeaders(call.ctx(), headers);
    reply.Clear();
    return stub->QueryValidPaths(&call.ctx(), request, &reply);
  });
  StorePathSet res;
  for (const auto &path : reply.paths()) {
    res.insert(StorePath(path));
  }
  return res;
}

namespace {

// What Nix's queryMissing walk visits: a derivation and the outputs wanted of
// it. Reads each derivation from the evaluation store once, when asked; one
// it cannot read has no outputs or inputs here, and is left to the walk.
class WalkGraph {
public:
  using Node = DerivedPath::Built;

  explicit WalkGraph(Store & evalStore) : evalStore(&evalStore) {}

  // The wanted outputs whose paths are known, each with its derivation.
  auto keys(const Node & node) -> std::vector<StorePath> {
    std::vector<StorePath> wanted;
    auto const & drvPath = node.drvPath->getBaseStorePath();
    for (const auto & [name, path] : outputs(drvPath)) {
      if (path && node.outputs.contains(name)) {
        wanted.push_back(*path);
        derivers.insert_or_assign(*path, drvPath);
      }
    }
    return wanted;
  }

  // The input derivations, each with the outputs this one uses.
  auto children(const Node & node) -> std::vector<Node> {
    std::map<StorePath, StringSet> used;
    if (const auto * drv = read(node.drvPath->getBaseStorePath())) {
      nixcompat::forInputOutputs(*drv, [&](const StorePath & input, const std::string & name) -> void {
        used[input].insert(name);
      });
    }
    std::vector<Node> below;
    below.reserve(used.size());
    for (auto & [input, names] : used) {
      below.push_back(
          Node{.drvPath = makeConstantStorePathRef(input), .outputs = OutputsSpec::Names{std::move(names)}});
    }
    return below;
  }

  // Of these outputs, those the client's substituters have, asked only for
  // a derivation that allows substitutes, as Nix's walk asks.
  auto substitutable(const StorePathSet & paths) -> StorePathSet {
    StorePathCAMap query;
    for (const auto & path : paths) {
      const auto * drv = read(derivers.at(path));
      if (drv != nullptr && nixcompat::substitutesAllowed(*evalStore, *drv)) {
        query.emplace(path, std::nullopt);
      }
    }
    SubstitutablePathInfos infos;
    evalStore->Store::querySubstitutablePathInfos(query, infos);
    StorePathSet found;
    for (const auto & [path, _info] : infos) {
      found.insert(path);
    }
    return found;
  }

private:
  Store * evalStore;
  std::map<StorePath, std::map<std::string, std::optional<StorePath>>> outputMaps;
  std::map<StorePath, std::optional<Derivation>> drvs;
  std::map<StorePath, StorePath> derivers;

  auto outputs(const StorePath & drvPath) -> const std::map<std::string, std::optional<StorePath>> & {
    auto found = outputMaps.find(drvPath);
    if (found == outputMaps.end()) {
      std::map<std::string, std::optional<StorePath>> known;
      try {
        known = evalStore->queryStaticPartialDerivationOutputMap(drvPath);
      } catch (Error & err) {
        debug("not asking ahead for '%s': %s", evalStore->printStorePath(drvPath), err.what());
      }
      found = outputMaps.emplace(drvPath, std::move(known)).first;
    }
    return found->second;
  }

  auto read(const StorePath & drvPath) -> const Derivation * {
    auto found = drvs.find(drvPath);
    if (found == drvs.end()) {
      std::optional<Derivation> drv;
      try {
        drv = evalStore->readDerivation(drvPath);
      } catch (Error & err) {
        debug("not asking ahead below '%s': %s", evalStore->printStorePath(drvPath), err.what());
      }
      found = drvs.emplace(drvPath, std::move(drv)).first;
    }
    return found->second ? &*found->second : nullptr;
  }
};

} // namespace

auto GrpcStore::queryValidPaths(const StorePathSet & paths, SubstituteFlag maybeSubstitute) -> StorePathSet {
  if (askedAhead.store != this || askedAhead.valid == nullptr || maybeSubstitute != NoSubstitute) {
    return queryValidPathsRouted(paths, maybeSubstitute, {});
  }
  if (auto * ask = askedAhead.ask; ask != nullptr && *ask) {
    std::exchange(*ask, nullptr)();
  }
  StorePathSet held;
  StorePathSet unasked;
  for (const auto & path : paths) {
    if (auto const known = askedAhead.valid->find(path); known == askedAhead.valid->end()) {
      unasked.insert(path);
    } else if (known->second) {
      held.insert(path);
    }
  }
  if (!unasked.empty()) {
    held.merge(queryValidPathsRouted(unasked, NoSubstitute, {}));
  }
  return held;
}

// Nix's walk asks about validity one path at a time, or (on a Nix that
// batches it) one level of the graph at a time, a round trip each. So at its
// first question, ask ahead in batches, top-down as the walk goes: the
// targets, then the inputs of the derivations whose wanted outputs the farm
// lacks and the client's substituters cannot provide, as the walk goes below
// only those. Each call takes a level and the levels below it that fit in
// topDownBudget, so it may cover derivations the walk never reaches. Only
// validity is recorded: the walk still decides everything, and asks what the
// batches did not reach (a derivation the evaluation store cannot read, a
// reference of a substitutable output) itself.
void GrpcStore::askAhead(const std::vector<DerivedPath> & targets, Store & evalStore,
                         std::map<StorePath, bool> & valid) {
  auto const ask = [&](const StorePathSet & paths) -> StorePathSet {
    auto held = queryValidPathsRouted(paths, NoSubstitute, {});
    for (const auto & path : paths) {
      valid.insert_or_assign(path, held.contains(path));
    }
    return held;
  };
  WalkGraph graph(evalStore);
  std::vector<WalkGraph::Node> roots;
  // Store paths given as targets (`nix path-info` passes each of its
  // arguments so): the walk asks each one's validity on its own.
  StorePathSet opaque;
  for (const auto & target : targets) {
    if (const auto * path = std::get_if<DerivedPath::Opaque>(&target.raw())) {
      opaque.insert(path->path);
    }
    const auto * built = std::get_if<DerivedPath::Built>(&target.raw());
    if (built != nullptr && std::holds_alternative<SingleDerivedPath::Opaque>(built->drvPath->raw())) {
      roots.push_back(*built);
    }
  }
  if (!opaque.empty()) {
    ask(opaque);
  }
  nixgrpc::missingTopDown(
      std::move(roots), [&](const WalkGraph::Node & node) -> std::vector<StorePath> { return graph.keys(node); },
      [&](const WalkGraph::Node & node) -> std::vector<WalkGraph::Node> { return graph.children(node); }, ask,
      nixgrpc::topDownBudget, [&](const StorePathSet & paths) -> StorePathSet { return graph.substitutable(paths); });
}

auto GrpcStore::walkMissingAskedAhead(const std::vector<DerivedPath> & targets, Store & evalStore)
    -> MissingPaths {
  std::map<StorePath, bool> valid;
  std::function<void()> ask = [&] -> void { askAhead(targets, evalStore, valid); };
  auto const outer = std::exchange(askedAhead, {.store = this, .valid = &valid, .ask = &ask});
  Finally const restore([&] -> void { askedAhead = outer; });
  return walkMissing(*this, targets, &evalStore);
}

#if NIX_COMPAT_HAS_QUERY_MISSING_EVAL_STORE
auto GrpcStore::queryMissing(const std::vector<DerivedPath> & targets, Store * evalStore)
    -> MissingPaths {
#else
auto GrpcStore::queryMissing(const std::vector<DerivedPath> & targets) -> MissingPaths {
  Store * const evalStore = nullptr; // this Nix passes none (Nix pull request 16433)
#endif
  if (evalStore != nullptr && evalStore != this) {
    // The remote daemon cannot read the caller's evaluation store. Walk the
    // graph client-side, using native RPCs for build-store path queries.
    return walkMissingAskedAhead(targets, *evalStore);
  }

  remote::QueryMissingRequest request;
  for (const auto & target : targets) {
    request.add_targets(target.to_string(*this));
  }
  remote::QueryMissingReply reply;
  retrying("QueryMissing", [&] -> grpc::Status {
    nixgrpc::Call call;
    reply.Clear();
    return stub->QueryMissing(&call.ctx(), request, &reply);
  });
  MissingPaths res;
  for (const auto & path : reply.will_build()) {
    res.willBuild.insert(StorePath(path));
  }
  for (const auto & path : reply.will_substitute()) {
    res.willSubstitute.insert(StorePath(path));
  }
  for (const auto & path : reply.unknown()) {
    res.unknown.insert(StorePath(path));
  }
  res.downloadSize = reply.download_size();
  res.narSize = reply.nar_size();
  return res;
}

void GrpcStore::querySubstitutablePathInfos(const StorePathCAMap & paths,
                                            SubstitutablePathInfos & infos) {
  std::vector<DerivedPath> targets;
  targets.reserve(paths.size());
  for (const auto & [path, _ca] : paths) {
    targets.emplace_back(DerivedPath::Opaque{path});
  }
  for (const auto & path : queryMissing(targets).willSubstitute) {
    infos.insert_or_assign(path, SubstitutablePathInfo{});
  }
}

auto GrpcStore::queryPathInfosNative(const StorePathSet &paths) -> PathInfoMap {
  remote::QueryPathInfosRequest request;
  for (const auto &path : paths) {
    request.add_paths(std::string(path.to_string()));
  }
  remote::QueryPathInfosReply reply;
  retrying("QueryPathInfos", [&] -> grpc::Status {
    nixgrpc::Call call;
    reply.Clear();
    return stub->QueryPathInfos(&call.ctx(), request, &reply);
  });

  PathInfoMap res;
  for (const auto &entry : reply.infos()) {
    res.insert(nixgrpc::decodePathInfo(*this, entry));
  }
  return res;
}

auto GrpcStore::getBuildLogExact(const StorePath & path) -> std::optional<std::string> {
  remote::GetBuildLogRequest request;
  request.set_drv_path(std::string(path.to_string()));
  StringSink log;
  bool none = false;
  retrying("GetBuildLog", [&] -> grpc::Status {
    nixgrpc::Call call;
    log.s.clear();
    auto reader = stub->GetBuildLog(&call.ctx(), request);
    nixgrpc::ZstdReaderSource<grpc::ClientReader<remote::GetBuildLogChunk>, remote::GetBuildLogChunk> source(
        *reader, "");
    source.drainInto(log);
    auto const status = reader->Finish();
    // UNIMPLEMENTED: the store keeps no logs, or the server predates GetBuildLog.
    none = status.error_code() == grpc::StatusCode::NOT_FOUND
           || status.error_code() == grpc::StatusCode::UNIMPLEMENTED;
    return none ? grpc::Status::OK : status;
  });
  if (none) {
    return std::nullopt;
  }
  return std::move(log.s);
}

#if NIX_COMPAT_AT_LEAST(2, 35)
auto GrpcStore::topoSortPaths(const StorePathSet &paths) -> StorePaths {
  // copyPaths() hands the source store the full set of paths to copy
  // here, right before querying their info one by one; prefetch them in
  // a single RPC.
  std::map<StorePath, uint64_t> sizes;
  StorePathSet absent = paths;
  if (!paths.empty()) {
    auto infos = queryPathInfosNative(paths);
    for (const auto & [infoPath, info] : infos) {
      sizes.emplace(infoPath, info->narSize);
      absent.erase(infoPath);
    }
    std::scoped_lock const lock(prefetchMutex);
    prefetchedInfos.merge(infos);
  }
  // The reply answers for every path asked: one it leaves out is absent
  // (printMissing sorts derivations the farm does not have yet), and the
  // sort's question about it needs no round trip of its own.
  auto const outer = std::exchange(askedAhead, {.store = this, .absent = &absent});
  Finally const restore([&] -> void { askedAhead = outer; });
  debug("sorting %d paths, %d of them absent", paths.size(), absent.size());
  // copyPaths() consumes the sources in reverse topological order.
  // Record it so narFromPath() can pipeline its NAR requests.
  auto sorted = Store::topoSortPaths(paths);
  narFetcher.recordOrder({sorted.rbegin(), sorted.rend()}, std::move(sizes));
  return sorted;
}
#endif

void GrpcStore::runInfoBatches(const std::stop_token & stop)
{
    while (true) {
        std::vector<std::pair<StorePath, InfoCallback>> batch;
        {
            std::unique_lock lock(infoBatchMutex);
            if (!infoBatchWakeup.wait(lock, stop, [&] -> bool { return !infoBatch.empty(); })) {
                return;
            }
            batch.swap(infoBatch);
        }
        StorePathSet paths;
        for (const auto & [path, callback] : batch) {
            paths.insert(path);
        }
        PathInfoMap infos;
        try {
            infos = queryPathInfosNative(paths);
        } catch (...) {
            for (auto & [path, callback] : batch) {
                callback.rethrow();
            }
            continue;
        }
        // Each callback fires exactly once, even if one throws.
        for (auto & [path, callback] : batch) {
            auto const found = infos.find(path);
            try {
                callback(found == infos.end() ? nullptr : found->second);
            } catch (...) {
                ignoreExceptionExceptInterrupt();
            }
        }
    }
}

void GrpcStore::queryPathInfoUncached(const StorePath & path, InfoCallback callback) noexcept {
    try {
        if (askedAhead.store == this && askedAhead.absent != nullptr && askedAhead.absent->contains(path)) {
          callback(nullptr);
          return;
        }
        {
          std::scoped_lock const lock(prefetchMutex);
          if (auto const found = prefetchedInfos.find(path);
              found != prefetchedInfos.end()) {
            auto info = std::move(found->second);
            prefetchedInfos.erase(found);
            callback(std::move(info));
            return;
          }
        }

        {
          std::scoped_lock const lock(infoBatchMutex);
          infoBatch.emplace_back(path, std::move(callback));
          if (!infoBatchWorker.joinable()) {
            infoBatchWorker = std::jthread([this](const std::stop_token & stop) -> void { runInfoBatches(stop); });
          }
        }
        infoBatchWakeup.notify_one();
    } catch (...) {
        callback.rethrow();
    }
}

void GrpcStore::addMultipleToStoreRouted(
    PathsSource pathsToCopy, Activity & act, RepairFlag repair, CheckSigsFlag checkSigs, const Metadata & headers)
{
    auto const spool = std::make_shared<UploadSpool>();
    std::vector<std::pair<ValidPathInfo, ReplayableNar>> paths;
    paths.reserve(pathsToCopy.size());
    for (auto & [pathInfo, pathSource] : pathsToCopy) {
      paths.emplace_back(pathInfo, ReplayableNar(std::move(pathSource), spool, pathInfo.narSize));
    }
    // A worker that took the paths may leave before they reach the cache,
    // after minutes under a busy cache. That was no failure to reach the farm.
    bool again = false;
    retryingWithProgress("AddMultipleToStore", [&](bool & accepted) -> grpc::Status {
      // A worker whose call was cut off goes on importing and publishing,
      // so some of the paths may be in the farm by now.
      if (std::exchange(again, true) && repair == NoRepair) {
        StorePathSet names;
        for (auto & [pathInfo, nar] : paths) {
          names.insert(pathInfo.path);
        }
        auto const landed = queryValidPathsRouted(names, NoSubstitute, headers);
        std::erase_if(paths, [&](const auto & item) -> bool { return landed.contains(item.first.path); });
        if (paths.empty()) {
          return grpc::Status::OK;
        }
      }
      return addMultipleToStoreOnce(paths, act, repair, checkSigs, headers, accepted);
    });
}

auto GrpcStore::addMultipleToStoreOnce(
    std::vector<std::pair<ValidPathInfo, ReplayableNar>> & paths, Activity & act, RepairFlag repair,
    CheckSigsFlag checkSigs, const Metadata & headers, bool & accepted) -> grpc::Status
{
    uint64_t bytesExpected = 0;
    for (auto & [pathInfo, nar] : paths) {
      bytesExpected += pathInfo.narSize;
    }
    act.setExpected(actCopyPath, bytesExpected);

    nixgrpc::Call call;
    addHeaders(call.ctx(), headers);
    remote::AddMultipleReply reply;
    auto writer = stub->AddMultipleToStore(&call.ctx(), &reply);

    try {
        // Flags travel on the first message; everything after is one zstd
        // stream in worker-protocol AddMultipleToStore framing.
        remote::AddMultipleChunk flags;
        flags.set_repair(repair == Repair);
        flags.set_check_sigs(checkSigs == CheckSigs);
        if (!writer->Write(flags)) {
          throw nixgrpc::StreamClosed("gRPC stream closed by peer");
        }

        nixgrpc::ZstdWriterSink<grpc::ClientWriter<remote::AddMultipleChunk>, remote::AddMultipleChunk> sink(
            *writer);
        sink << paths.size();
        size_t done = 0;
        for (auto & [pathInfo, nar] : paths) {
            act.progress(done, paths.size(), static_cast<size_t>(1), static_cast<size_t>(0));
            WorkerProto::Serialise<ValidPathInfo>::write(
                *this, WorkerProto::WriteConn{.to = sink, .version = nixcompat::infoProtocolVersion()}, pathInfo);
            nar.drainInto(sink);
            act.progress(++done, paths.size(), static_cast<size_t>(0), static_cast<size_t>(0));
        }
        sink.finish();
        writer->WritesDone();
    } catch (nixgrpc::StreamClosed &) {
        // No cancel: it could replace the server's status with CANCELLED.
        auto status = writer->Finish();
        if (status.ok()) {
          throw;
        }
        accepted = call.ctx().GetServerInitialMetadata().contains("x-nix-accepted");
        return status;
    } catch (...) {
        // The server may still be reading. A status other than our cancel
        // explains the failure better.
        call.ctx().TryCancel();
        auto status = writer->Finish();
        if (status.ok() || status.error_code() == grpc::StatusCode::CANCELLED) {
          throw;
        }
        return status;
    }
    auto status = writer->Finish();
    accepted = call.ctx().GetServerInitialMetadata().contains("x-nix-accepted");
    return status;
}

} // namespace nix
