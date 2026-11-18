// The plugin the loader (plugin-loader.cc) opens for the running Nix: registers the
// grpc:// store type and the global settings, and loads the gRPC client
// (nix-grpc-store-impl: store.cc and the rest) only once a grpc:// store is
// opened. See store-config.hh.

#include "store-config.hh"

#include <filesystem>
#include <memory>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>

#include <dlfcn.h>

#include <nix/store/globals.hh>
#include <nix/store/store-api.hh>
#include <nix/store/store-registration.hh>
#include <nix/util/configuration.hh>
#include <nix/util/environment-variables.hh>
#include <nix/util/error.hh>
#include <nix/util/logging.hh>
#include <nix/util/ref.hh>

#include "parse-int.hh"

namespace nix {

namespace {

using nixgrpc::OpenGrpcStore;

// The gRPC client, set by nix_plugin_entry: next to this plugin's file with
// symlinks resolved, in every install a merged one included. That is the
// output this process mapped the plugin from, which it keeps alive, whatever
// the path it was opened by points at by the time a store is opened.
auto grpcClientPath() -> std::filesystem::path & {
  static std::filesystem::path path;
  return path;
}

void locateGrpcClient() {
  Dl_info info{};
  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast): dladdr needs a data pointer
  if (dladdr(reinterpret_cast<void *>(&locateGrpcClient), &info) == 0 || info.dli_fname == nullptr) {
    return;
  }
  std::error_code error;
  auto const self = std::filesystem::canonical(info.dli_fname, error);
  if (!error) {
    grpcClientPath() = self.parent_path() / ("nix-grpc-store-impl." NIX_GRPC_MODULE_SUFFIX);
  }
}

auto loadGrpcClient() -> OpenGrpcStore {
  auto const & client = grpcClientPath();
  if (client.empty()) {
    throw Error("nix-grpc-store: cannot find its own plugin file");
  }
  // Leaked on purpose, as Nix leaks its plugins: the stores it makes live
  // until the process exits. It links this plugin, which the loader opened
  // RTLD_LOCAL, for nixgrpc::lingerSeconds and nixgrpc::leaseId alone, and
  // binds them to this copy.
  void * handle = dlopen(client.c_str(), RTLD_NOW | RTLD_LOCAL);
  if (handle == nullptr) {
    // NOLINTNEXTLINE(concurrency-mt-unsafe): read before any other dlopen of this thread
    throw Error("nix-grpc-store: could not load '%s': %s", client.string(), dlerror());
  }
  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast): dlsym returns void*
  auto * open = reinterpret_cast<OpenGrpcStore>(dlsym(handle, "nix_grpc_store_open"));
  if (open == nullptr) {
    throw Error("nix-grpc-store: no nix_grpc_store_open in '%s'", client.string());
  }
  return open;
}

} // namespace

auto GrpcStoreConfig::openStore() const -> ref<Store> {
  // Loaded once; a failed load throws and is tried again next time.
  static OpenGrpcStore const open = loadGrpcClient();
  std::shared_ptr<Store> store;
  open(*this, store);
  return ref{std::move(store)};
}

} // namespace nix


namespace nixgrpc {

namespace {

constexpr auto lingerEnv = "NIX_GRPC_LINGER";
constexpr auto leaseEnv = "NIX_GRPC_LEASE";

// `grpc-linger`: seconds the scheduler is asked to keep a build of this
// client's once the client leaves, because it will ask for it again
// (Want.linger_s): a chain of time-bounded `nix build`s, say. It lives in
// nix::settings, not in the store URL, which `builders` shares with every
// build on the machine: it is set per invocation (`--option grpc-linger
// 600`), sent to the daemon with the other settings, and applied there for
// a trusted user. The build hook the daemon starts reads its parent's
// settings before it loads plugins (`nix __build-remote`) and so drops this
// one; the value also goes to the environment, which the hook inherits and
// takes the default from.
struct LingerSetting : nix::Setting<unsigned> {
  using nix::Setting<unsigned>::Setting;

protected:
  void appendOrSet(unsigned value, bool append) override {
    nix::Setting<unsigned>::appendOrSet(value, append);
    nix::setEnv(lingerEnv, std::to_string(get()).c_str());
  }
};

auto lingerSetting() -> LingerSetting & {
  // Never destroyed: nix::settings keeps a pointer to it until the process
  // exits, and may outlive a static of this plugin's.
  static auto * const setting = new LingerSetting{ // NOLINT(cppcoreguidelines-owning-memory): never freed, see above
      &nix::settings, parseInt<unsigned>(nix::getEnv(lingerEnv).value_or("")).value_or(0), "grpc-linger",
      "Seconds a `grpc://` scheduler is asked to keep a build once this client leaves, because it will "
      "ask for it again (a chain of time-bounded clients). The scheduler caps it (`--max-linger`). "
      "Reaches the build hook only from a trusted user."};
  return *setting;
}

// `grpc-lease`: the name of this client's chain, so that the scheduler keeps
// a build while any chain's lease on it is live, and Release ends one chain's
// alone (Want.lease_id). It travels as grpc-linger does.
struct LeaseSetting : nix::Setting<std::string> {
  using nix::Setting<std::string>::Setting;

protected:
  void appendOrSet(std::string value, bool append) override {
    nix::Setting<std::string>::appendOrSet(std::move(value), append);
    nix::setEnv(leaseEnv, get().c_str());
  }
};

auto leaseSetting() -> LeaseSetting & {
  // Never destroyed, as lingerSetting.
  static auto * const setting = new LeaseSetting{ // NOLINT(cppcoreguidelines-owning-memory): never freed, see above
      &nix::settings, nix::getEnv(leaseEnv).value_or(""), "grpc-lease",
      "A name for this client's chain of time-bounded clients, the same for each of them: the "
      "`grpc-linger` they declare is its lease, which keeps a build until it is over or ended "
      "(the store's `release-lease`). Reaches the build hook only from a trusted user."};
  return *setting;
}

} // namespace

auto lingerSeconds() -> unsigned { return lingerSetting().get(); }

auto leaseId() -> std::string { return leaseSetting().get(); }

} // namespace nixgrpc

namespace {

// "2.31.5" / "2.35pre20260619_f8bb823a" -> "2.31" / "2.35"
auto majorMinor(std::string_view version) -> std::string_view {
  auto const firstDot = version.find('.');
  auto const end = version.find_first_not_of("0123456789", firstDot + 1);
  return version.substr(0, end);
}

} // namespace

// Called right after dlopen(), by the loader (or by Nix when this file is in
// plugin-files itself). Registering here instead of in a static
// initializer lets a version mismatch degrade to a warning.
extern "C" void nix_plugin_entry() {
  auto const running = majorMinor(nix::nixVersion);
  auto const builtAgainst = majorMinor(NIX_GRPC_BUILT_AGAINST_NIX);
  if (running != builtAgainst) {
    nix::warn(
        "nix-grpc-store plugin was built against Nix %s but is being loaded by Nix %s. "
        "grpc:// stores are unavailable",
        std::string(builtAgainst), std::string(running));
    return;
  }
  // Before any thread can open a store.
  nix::locateGrpcClient();
  static const nix::RegisterStoreImplementation<nix::GrpcStoreConfig> regGrpcStore;
  // Registers grpc-linger and grpc-lease before Nix applies the nix.conf and
  // --option values it did not know yet (initPlugins).
  static_cast<void>(nixgrpc::lingerSeconds());
  static_cast<void>(nixgrpc::leaseId());
}
