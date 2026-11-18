#pragma once
// NOLINTBEGIN(cppcoreguidelines-macro-usage) -- version detection needs macros
// Compatibility helpers for building against multiple Nix versions.
// NIX_COMPAT_VERSION_{MAJOR,MINOR} come from meson (version of the
// `nix-store` pkg-config dependency).

#include <cerrno>
#include <cstdint>
#include <filesystem>
#include <map>
#include <optional>
#include <set>
#include <string>
#include <type_traits>
#include <utility>
#include <variant>

// Must be a macro: it is evaluated in #if directives below and in the sources.
#define NIX_COMPAT_AT_LEAST(major, minor)                                     \
  (NIX_COMPAT_VERSION_MAJOR > (major) ||                                      \
   (NIX_COMPAT_VERSION_MAJOR == (major) && NIX_COMPAT_VERSION_MINOR >= (minor)))

#include <string_view>

#include <nix/store/build-result.hh>
#include <nix/store/derivations.hh>
#include <nix/store/globals.hh>
#include <nix/store/realisation.hh>
#include <nix/store/store-api.hh>
#include <nix/store/worker-protocol.hh>
#include <nix/util/file-descriptor.hh>
#include <nix/util/file-system.hh>
#include <nix/util/serialise.hh>

// Late 2.36 moved the build operations from Store into a Builder interface.
#if __has_include(<nix/store/build.hh>)
#include <nix/store/build.hh>
#define NIX_COMPAT_HAS_BUILDER 1
#else
#define NIX_COMPAT_HAS_BUILDER 0
#endif

// Late 2.36 templated Derivation over its inputs: `inputs` replaces
// `inputSrcs`/`inputDrvs` and tryResolve became a free function.
#if __has_include(<nix/store/derivation/resolution.hh>)
#define NIX_COMPAT_TEMPLATED_DRV 1
#else
#define NIX_COMPAT_TEMPLATED_DRV 0
#endif
#include <nix/store/derivation-options.hh>

#if __has_include(<nix/store/substituter.hh>)
#include <nix/store/substituter.hh> // IWYU pragma: export
#endif

// Newer Nix declares DerivationOutput in a header of its own.
#if __has_include(<nix/store/derivation/output.hh>)
#include <nix/store/derivation/output.hh> // IWYU pragma: export
#endif

#if __has_include(<nix/store/derivation/aterm.hh>)
#include <nix/store/derivation/aterm.hh>
#define NIX_COMPAT_HAS_DERIVATION_ATERM 1
#else
#define NIX_COMPAT_HAS_DERIVATION_ATERM 0
#endif

// Nix 2.35 added a FilePathType argument to the StoreConfig /
// RemoteStoreConfig constructors.
// Must be a macro: it expands inside constructor member-initializer lists.
#if NIX_COMPAT_AT_LEAST(2, 35)
#define NIX_COMPAT_STORE_CONFIG_ARGS(params) params, FilePathType::Unix
#else
#define NIX_COMPAT_STORE_CONFIG_ARGS(params) params
#endif

namespace nixcompat {

constexpr auto makeProtocolVersion(unsigned int major, uint8_t minor) -> nix::WorkerProto::Version {
  return {.number = {.major = major, .minor = minor}};
}

// Worker-protocol version 1.16: the ValidPathInfo framing used by the bulk
// RPCs.
inline auto infoProtocolVersion() -> nix::WorkerProto::Version {
  constexpr unsigned int major = 1;
  constexpr uint8_t minor = 16;
  return makeProtocolVersion(major, minor);
}

// Worker-protocol version 1.37: BasicDerivation, BuildResult and the stderr
// stream framing used by the native BuildDerivation RPC.
constexpr uint32_t kBuildProtocolWire = (1U << 8U) | 37U;

constexpr auto protocolWire(const nix::WorkerProto::Version &version) -> uint32_t {
  constexpr unsigned int minorBits = 8;
  return (version.number.major << minorBits) | version.number.minor;
}

template <typename C>
inline auto handshakeCompat(C &conn, const nix::WorkerProto::Version &version)
    -> nix::WorkerProto::Version {
  return C::handshake(conn.to, conn.from, version);
}

template <typename F>
inline void forBuiltOutputs(nix::BuildResult &res, const F &fun) {
  if (const auto *success = res.tryGetSuccess()) {
    for (const auto &[name, realisation] : success->builtOutputs) {
      fun(realisation.outPath);
    }
  }
}

inline auto buildFailureMsg(nix::BuildResult &res) -> std::optional<std::string> {
  if (const auto *failure = res.tryGetFailure()) {
    return std::string(failure->what());
  }
  return std::nullopt;
}

inline void setAlreadyValid(nix::BuildResult &res) {
  res.inner = nix::BuildResult::Success{.status = nix::BuildResult::Success::AlreadyValid};
}

inline auto buildProtocolVersion() -> nix::WorkerProto::Version {
  constexpr unsigned int major = 1;
  constexpr uint8_t minor = 37;
  return makeProtocolVersion(major, minor);
}

// Late 2.36 moved the drv wire serialisation into namespace derivation.
#if NIX_COMPAT_HAS_DERIVATION_ATERM
inline void writeDrv(nix::Sink &sink, const nix::StoreDirConfig &store,
                     const nix::BasicDerivation &drv) {
  nix::derivation::write(sink, store, drv);
}
inline void readDrv(nix::Source &source, const nix::StoreDirConfig &store,
                    nix::BasicDerivation &drv, std::string_view name) {
  nix::derivation::read(source, store, drv, name);
}
#else
inline void writeDrv(nix::Sink &sink, const nix::StoreDirConfig &store,
                     const nix::BasicDerivation &drv) {
  nix::writeDerivation(sink, store, drv);
}
inline void readDrv(nix::Source &source, const nix::StoreDirConfig &store,
                    nix::BasicDerivation &drv, std::string_view name) {
  nix::readDerivation(source, store, drv, name);
}
#endif

// Late 2.36 made the derivation type a free function, found by ADL.
template<typename D>
auto isSandboxed(const D & drv) -> bool
{
    if constexpr (requires { drv.type(); }) {
        return drv.type().isSandboxed();
    } else {
        return type(drv).isSandboxed();
    }
}

template<typename D>
auto isImpure(const D & drv) -> bool
{
    if constexpr (requires { drv.type(); }) {
        return drv.type().isImpure();
    } else {
        return type(drv).isImpure();
    }
}

inline auto succeeded(nix::BuildResult &res) -> bool {
  return res.tryGetSuccess() != nullptr;
}

inline void addSignatures(nix::Store &store, const nix::StorePath &path,
                          const std::set<std::string> &sigs) {
  std::set<nix::Signature> parsed;
  for (const auto &sig : sigs) {
    parsed.insert(nix::Signature::parse(sig));
  }
  store.addSignatures(path, parsed);
}

using FailureStatus = nix::BuildResult::Failure::Status;

using RealisationCallback = nix::Callback<std::shared_ptr<const nix::UnkeyedRealisation>>;

inline auto failed(FailureStatus status, const std::string &msg) -> nix::BuildResult {
  nix::BuildResult res;
  nix::BuildResult::Failure failure(status, "%s", msg);
  res.inner = std::move(failure);
  return res;
}

// AlreadyValid with builtOutputs filled in, which `nix build` reads to map
// results to paths. Only outPath matters to it. The DrvOutput key changed
// from a hash to the drv path in 2.35.
inline auto alreadyValid(const nix::StorePath &drvPath,
                         std::map<std::string, nix::StorePath> outputs) -> nix::BuildResult {
  nix::BuildResult res;
  setAlreadyValid(res);
  auto &built = res.tryGetSuccess()->builtOutputs;
  for (auto &[name, path] : outputs) {
#if NIX_COMPAT_AT_LEAST(2, 35)
    nix::DrvOutput key{.drvPath = drvPath, .outputName = name};
#else
    (void)drvPath;
    nix::DrvOutput key{.drvHash = nix::Hash(nix::HashAlgorithm::SHA256), .outputName = name};
#endif
    built.emplace(name, nix::Realisation{{.outPath = std::move(path)}, std::move(key)});
  }
  return res;
}

// Failure kinds worth telling other workers' waiters about.
inline auto failureStatus(nix::BuildResult &res) -> std::optional<FailureStatus> {
  if (const auto *failure = res.tryGetFailure()) {
    return failure->status;
  }
  return std::nullopt;
}

// Metric label: the enum name only, never the message, which is unbounded.
inline auto failureStatusName(FailureStatus status) -> std::string_view {
  using S = FailureStatus;
  switch (status) {
  case S::HashMismatch:
    return "HashMismatch";
  case S::PermanentFailure:
    return "PermanentFailure";
  case S::InputRejected:
    return "InputRejected";
  case S::OutputRejected:
    return "OutputRejected";
  case S::TransientFailure:
    return "TransientFailure";
  case S::CachedFailure:
    return "CachedFailure";
  case S::TimedOut:
    return "TimedOut";
  case S::MiscFailure:
    return "MiscFailure";
  case S::DependencyFailed:
    return "DependencyFailed";
  case S::LogLimitExceeded:
    return "LogLimitExceeded";
  case S::NotDeterministic:
    return "NotDeterministic";
  case S::NoSubstituters:
    return "NoSubstituters";
  default:
    return "Unknown";
  }
}

// Plain store path inputs of a BasicDerivation.
inline auto drvInputs(const nix::BasicDerivation &drv) -> const nix::StorePathSet & {
#if NIX_COMPAT_TEMPLATED_DRV
  return drv.inputs;
#else
  return drv.inputSrcs;
#endif
}

// What nix's DerivationGoal hands a builder: the BasicDerivation with each
// input derivation output replaced by its store path. `outputOf(drvPath,
// outputName)` supplies those. Returns nullopt for dynamic derivation inputs.
template <typename OutputOf>
auto toBasicDrv(const nix::Derivation &drv, const OutputOf &outputOf)
    -> std::optional<nix::BasicDerivation> {
  bool dynamic = false;
#if NIX_COMPAT_TEMPLATED_DRV
  auto basic = drv.mapInputs([&](const std::set<nix::SingleDerivedPath> &inputs) -> nix::StorePathSet {
    nix::StorePathSet srcs;
    for (const auto &input : inputs) {
      if (const auto *opaque = std::get_if<nix::SingleDerivedPath::Opaque>(&input.raw())) {
        srcs.insert(opaque->path);
        continue;
      }
      const auto &built = std::get<nix::SingleDerivedPath::Built>(input.raw());
      const auto *dep = std::get_if<nix::SingleDerivedPath::Opaque>(&built.drvPath->raw());
      if (dep == nullptr) {
        dynamic = true;
        continue;
      }
      srcs.insert(outputOf(dep->path, built.output));
    }
    return srcs;
  });
#else
  nix::BasicDerivation basic(static_cast<const nix::BasicDerivation &>(drv));
  for (const auto &[depPath, node] : drv.inputDrvs.map) {
    if (!node.childMap.empty()) {
      dynamic = true;
      continue;
    }
    for (const auto &output : node.value) {
      basic.inputSrcs.insert(outputOf(depPath, output));
    }
  }
#endif
  if (dynamic) {
    return std::nullopt;
  }
  return basic;
}

// Every input derivation path (static ones only, see toBasicDrv).
template <typename F> void forInputDrvs(const nix::Derivation &drv, const F &visit) {
#if NIX_COMPAT_TEMPLATED_DRV
  for (const auto &input : drv.inputs) {
    if (const auto *built = std::get_if<nix::SingleDerivedPath::Built>(&input.raw())) {
      visit(built->drvPath->getBaseStorePath());
    }
  }
#else
  for (const auto &[drvPath, node] : drv.inputDrvs.map) {
    (void)node;
    visit(drvPath);
  }
#endif
}

// Calls visit(drvPath, outputName) for each output of a static input
// derivation that `drv` uses.
template <typename F> void forInputOutputs(const nix::Derivation &drv, const F &visit) {
#if NIX_COMPAT_TEMPLATED_DRV
  for (const auto &input : drv.inputs) {
    if (const auto *built = std::get_if<nix::SingleDerivedPath::Built>(&input.raw())) {
      if (const auto *dep = std::get_if<nix::SingleDerivedPath::Opaque>(&built->drvPath->raw())) {
        visit(dep->path, built->output);
      }
    }
  }
#else
  for (const auto &[drvPath, node] : drv.inputDrvs.map) {
    for (const auto &output : node.value) {
      visit(drvPath, output);
    }
  }
#endif
}

// drv: a BasicDerivation, or a Derivation.
template <typename Drv> auto derivationOptions(const nix::StoreDirConfig &store, const Drv &drv) {
  const auto *attrs = drv.structuredAttrs ? &*drv.structuredAttrs : nullptr;
  return nix::derivationOptionsFromStructuredAttrs(store, drv.env, attrs, /*shouldWarn=*/false);
}

inline auto requiredSystemFeatures(const nix::StoreDirConfig &store,
                                   const nix::BasicDerivation &drv) -> nix::StringSet {
  return derivationOptions(store, drv).getRequiredSystemFeatures(drv);
}

// Whether Nix may substitute drv's outputs: its allowSubstitutes, or
// always-allow-substitutes.
template <typename Drv> auto substitutesAllowed(const nix::StoreDirConfig &store, const Drv &drv) -> bool {
  return derivationOptions(store, drv).substitutesAllowed(nix::settings.getWorkerSettings());
}

// Substitute `path` if it is not valid.
inline void ensurePath(nix::Store &store, const nix::StorePath &path) {
#if NIX_COMPAT_HAS_BUILDER
  store.getBuilder()->ensurePath(path);
#else
  store.ensurePath(path);
#endif
}

// Replaces a valid path with the copy a substituter of `store` offers.
inline void substituteOver(nix::Store &store, const nix::StorePath &path) {
  const std::vector<nix::DerivedPath> target{nix::DerivedPath::Opaque{path}};
#if NIX_COMPAT_HAS_BUILDER
  store.getBuilder()->buildPaths(target, nix::bmRepair);
#else
  store.buildPaths(target, nix::bmRepair);
#endif
}

// Nix 2.35 added EnsureRead, which turns a short NAR read into an error
// instead of silently truncating. Older versions read the NAR unguarded.
#if NIX_COMPAT_AT_LEAST(2, 35)
using nix::EnsureRead;
#else
class EnsureRead : public nix::Source {
  nix::Source &inner;

public:
  EnsureRead(nix::Source &source, uint64_t /*bytesExpected*/) : inner(source) {}
  auto read(char *data, size_t len) -> size_t override {
    return inner.read(data, len);
  }
  void finish() {}
};
#endif


// max-jobs as Nix's configuration says now, not as nix::settings read it at
// startup: fresh settings filled by Nix's own loader (nix.conf and its
// includes, the user files, NIX_CONFIG). Only the WorkerSettings base is
// instantiated, so no other setting's parser runs (or warns).
inline auto configuredMaxBuildJobs() -> unsigned {
  using Worker = std::remove_cvref_t<decltype(nix::settings.getWorkerSettings())>;
  // WorkerSettings' constructor is protected. Its bases are Nix's.
  struct Fresh : Worker {}; // NOLINT(misc-multiple-inheritance)
  Fresh fresh;
  nix::loadConfFile(fresh);
  return fresh.maxBuildJobs;
}

// The worker's own `cores`.
inline auto buildCores() -> unsigned { return nix::settings.getLocalSettings().buildCores; }

inline auto keepGoing() -> bool { return nix::settings.getWorkerSettings().keepGoing; }

inline auto buildTimeout() -> time_t { return nix::settings.getWorkerSettings().buildTimeout; }

inline auto maxSilentTime() -> time_t { return nix::settings.getWorkerSettings().maxSilentTime; }

// Late 2.36 made openFileReadonly() return a Result. Before, a failure
// was an invalid fd with errno set, which is what this keeps.
inline auto openFileReadonly(const std::filesystem::path &path) -> nix::AutoCloseFD {
  constexpr auto unwrap = []<typename F>(F file) -> nix::AutoCloseFD {
    if constexpr (requires { file.error(); }) {
      if (!file) {
        errno = file.error();
        return {};
      }
      return std::move(file.value());
    } else {
      return file;
    }
  };
  return unwrap(nix::openFileReadonly(path));
}

} // namespace nixcompat
// NOLINTEND(cppcoreguidelines-macro-usage)
