// The plugin as Nix loads it (libmain/plugin.cc initPlugins: dlopen
// RTLD_LAZY | RTLD_LOCAL, nix_plugin_entry, reapply the settings it did not
// know yet), through the loader, must not map gRPC into a process that never
// opens a grpc:// store: since Nix execs a fresh daemon worker per
// connection, every one of them would pay for loading it. The store type,
// its URL parameters and the global settings must still be there, and
// opening a store must load the gRPC client of the output the plugin was
// mapped from, hand it the parameters and bind it to that plugin's settings.
//
// usage: lazy-plugin-test <nix-grpc-store-loader> [<link> <target>]
// With <link> and <target>, the directory symlink <link> is pointed at
// <target> once the plugin is loaded, as a profile switch would.

#include <cassert>
#include <cstddef>
#include <filesystem>
#include <iostream>
#include <map>
#include <span>
#include <string>
#include <string_view>

#include <dlfcn.h>
#include <link.h>

#include <nix/store/globals.hh>
#include <nix/store/store-api.hh>
#include <nix/store/store-open.hh>
#include <nix/store/store-reference.hh>
#include <nix/util/config-global.hh>
#include <nix/util/configuration.hh>
#include <nix/util/environment-variables.hh>
#include <nix/util/error.hh>

namespace {

// The last mapped object whose path contains `part`, or "".
auto mapped(std::string_view part) -> std::string
{
    std::pair<std::string_view, std::string> search{part, ""};
    dl_iterate_phdr(
        [](dl_phdr_info * info, size_t /*size*/, void * data) -> int {
            auto & [part, found] = *static_cast<std::pair<std::string_view, std::string> *>(data);
            if (info->dlpi_name != nullptr && std::string_view(info->dlpi_name).contains(part)) {
                found = info->dlpi_name;
            }
            return 0;
        },
        &search);
    return search.second;
}

auto settingValue(const nix::AbstractConfig & config, const std::string & name) -> std::string
{
    std::map<std::string, nix::AbstractConfig::SettingInfo> settings;
    config.getSettings(settings);
    return settings.contains(name) ? settings.at(name).value : "<unknown>";
}

// nixgrpc::lingerSeconds as the scope of the loaded `object` binds it.
auto lingerSecondsOf(const std::string & object) -> void *
{
    void * handle = dlopen(object.c_str(), RTLD_NOW | RTLD_NOLOAD);
    assert(handle != nullptr);
    return dlsym(handle, "_ZN7nixgrpc13lingerSecondsEv");
}

} // namespace

auto main(int argc, char ** argv) -> int
{
    std::span const args(argv, static_cast<size_t>(argc));
    assert(args.size() == 2 || args.size() == 4);
    nix::initLibStore();
    // Set before the plugin is loaded, as nix.conf and --option are.
    assert(!nix::globalConfig.set("grpc-linger", "600"));

    // Never closed, as Nix never closes it.
    void * loader = dlopen(args[1], RTLD_LAZY | RTLD_LOCAL);
    assert(loader != nullptr);
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast): dlsym returns void*
    reinterpret_cast<void (*)()>(dlsym(loader, "nix_plugin_entry"))();
    nix::globalConfig.reapplyUnknownSettings();
    auto const plugin = mapped("/nix-grpc-store.so");
    assert(!plugin.empty());
    if (args.size() == 4) {
        std::filesystem::remove(args[2]);
        std::filesystem::create_directory_symlink(args[3], args[2]);
    }

    // The settings and the store's parameters apply without gRPC.
    assert(nix::getEnv("NIX_GRPC_LINGER") == "600");
    assert(nix::globalConfig.set("grpc-lease", "chain") && nix::getEnv("NIX_GRPC_LEASE") == "chain");
    auto const config = nix::resolveStoreConfig(
        nix::StoreReference::parse("grpc://127.0.0.1:1?insecure=true&max-builds=3&token-file=/nonexistent"));
    assert(settingValue(*config, "max-builds") == "3");
    for (std::string_view const part : {"libgrpc", "libprotobuf", "nix-grpc-store-impl"}) {
        if (!mapped(part).empty()) {
            std::cerr << "lazy-plugin-test: " << part << " is mapped before a grpc:// store is opened\n";
            return 1;
        }
    }

    // Opening loads the client, which gets the parameters (insecure with a
    // token-file is its error) and the settings of the plugin loaded.
    try {
        static_cast<void>(config->openStore());
        assert(false);
    } catch (const nix::Error & err) {
        if (!err.msg().contains("token-file needs TLS")) {
            std::cerr << "lazy-plugin-test: " << err.msg() << "\n";
            return 1;
        }
    }
    auto const client = mapped("nix-grpc-store-impl");
    if (client.empty()) {
        std::cerr << "lazy-plugin-test: opening a store did not map the client\n";
        return 1;
    }
    if (lingerSecondsOf(client) != lingerSecondsOf(plugin)) {
        std::cerr << "lazy-plugin-test: the client binds another copy of nixgrpc::lingerSeconds\n";
        return 1;
    }
    assert(settingValue(nix::openStore("grpc://127.0.0.1:1?insecure=true&max-builds=3")->config, "max-builds") == "3");
    std::cout << "lazy-plugin-test: ok\n";
    return 0;
}
