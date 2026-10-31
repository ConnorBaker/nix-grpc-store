// nixcompat::configuredMaxBuildJobs() reads nix.conf and its includes as they
// are now, which is what a builder does on SIGHUP after, e.g., a Tribuchet hub
// rewrote its fragment. meson points NIX_CONF_DIR at a scratch directory.

#include <algorithm>
#include <cassert>
#include <exception>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>
#include <thread>

#include <nix/util/environment-variables.hh>
#include <nix/util/error.hh>

#include "nix-compat.hh"

namespace {

void write(const std::filesystem::path & path, const std::string & text)
{
    auto tmp = path;
    tmp += ".tmp";
    std::ofstream(tmp) << text;
    std::filesystem::rename(tmp, path);
}

} // namespace

auto main() -> int
try {
    auto const confDir = nix::getEnv("NIX_CONF_DIR");
    if (!confDir) {
        std::cerr << "max-jobs-test: NIX_CONF_DIR is not set\n";
        return 1;
    }
    const std::filesystem::path dir = *confDir;
    std::filesystem::create_directories(dir);
    auto const fragment = dir / "fragment.conf";
    std::filesystem::remove(fragment);
    write(dir / "nix.conf", "max-jobs = 2\n!include fragment.conf\n");
    assert(nixcompat::configuredMaxBuildJobs() == 2);

    write(fragment, "max-jobs = 7\n");
    assert(nixcompat::configuredMaxBuildJobs() == 7);
    write(fragment, "build-max-jobs = 5\n");
    assert(nixcompat::configuredMaxBuildJobs() == 5);
    write(fragment, "max-jobs = auto\n");
    assert(nixcompat::configuredMaxBuildJobs() == std::max(1U, std::thread::hardware_concurrency()));

    // The builder keeps its last value on this.
    write(fragment, "max-jobs = lots\n");
    bool threw = false;
    try {
        nixcompat::configuredMaxBuildJobs();
    } catch (nix::Error &) {
        threw = true;
    }
    assert(threw);

    std::filesystem::remove(fragment);
    assert(nixcompat::configuredMaxBuildJobs() == 2);
    return 0;
} catch (std::exception & err) {
    std::cerr << err.what() << "\n";
    return 1;
}
