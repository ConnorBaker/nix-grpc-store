// Loads the nix-grpc-store plugin as nix loads plugin-files. Never closed:
// the store stays registered for the process lifetime.
#pragma once

#include <string>

#include <dlfcn.h>

#include <nix/util/error.hh>

inline void loadPlugin(const std::string & plugin)
{
    void * handle = dlopen(plugin.c_str(), RTLD_NOW | RTLD_LOCAL);
    if (handle == nullptr) {
        // NOLINTNEXTLINE(concurrency-mt-unsafe): no other thread loads libraries yet
        throw nix::Error("could not load '%s': %s", plugin, dlerror());
    }
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast): dlsym returns void*
    auto entry = reinterpret_cast<void (*)()>(dlsym(handle, "nix_plugin_entry"));
    if (entry == nullptr) {
        throw nix::Error("no nix_plugin_entry in '%s'", plugin);
    }
    entry();
}
