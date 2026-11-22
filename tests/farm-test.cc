// parseFarm and readFarmText: what a builder takes from --farm-file.

#include <cassert>
#include <exception>
#include <string>
#include <string_view>
#include <variant>
#include <vector>

#include <nix/util/error.hh>

#include "farm.hh"
#include "scheduler.hh"

namespace {

using nixgrpc::sched::FarmMachine;

auto refused(std::string_view text) -> bool
{
    try {
        nixgrpc::parseFarm(text);
    } catch (nix::Error &) {
        return true;
    }
    return false;
}

} // namespace

auto main() -> int
try {
    auto const farm = nixgrpc::parseFarm(R"({"machines": [
        {"systems": {"x86_64-linux": ["kvm", "nixos-test"], "i686-linux": []}, "jobs": 8},
        {"systems": {"aarch64-linux": []}, "jobs": 0},
        {"systems": {"x86_64-linux": []}, "jobs": 4294967295}]})");
    assert((farm
            == std::vector<FarmMachine>{
                {.systems = {{"i686-linux", {}}, {"x86_64-linux", {"kvm", "nixos-test"}}}, .jobs = 8},
                {.systems = {{"aarch64-linux", {}}}, .jobs = 0},
                {.systems = {{"x86_64-linux", {}}}, .jobs = 4294967295U}}));
    assert(nixgrpc::describeFarm(farm) == "i686-linux[]+x86_64-linux[kvm,nixos-test]=8 aarch64-linux[]=0 x86_64-linux[]=4294967295");
    assert(nixgrpc::parseFarm(R"({"machines": []})").empty());
    assert(refused(R"({"machines": [{"systems": {"x": []}, "jobs": 4294967296}]})"));
    assert(refused(R"({"machines": [{"systems": {"x": []}, "jobs": -1}]})"));
    assert(refused(R"({"machines": [{"systems": {"x": []}, "jobs": 1.5}]})"));
    assert(refused(R"({"machines": [{"systems": {"x": []}, "jobs": "4"}]})"));
    assert(refused(R"({"machines": [{"systems": ["x"], "jobs": 4}]})"));
    assert(refused(R"({"machines": [{"systems": {"x": "kvm"}, "jobs": 4}]})"));
    assert(refused(R"({"machines": [{"systems": {"x": [1]}, "jobs": 4}]})"));
    assert(refused(R"({"machines": [{"jobs": 4}]})"));
    assert(refused(R"({"machines": [{"systems": {"x": []}}]})"));
    assert(refused(R"({"workers": []})"));
    assert(refused(R"([])"));
    assert(refused(R"({"machines": [)"));
    assert(refused(""));

    // Absent or not a farm: no farm, whatever was there before.
    auto const absent = nixgrpc::readFarmText(std::monostate{});
    assert(absent.farm.empty() && absent.event == "farm_absent");
    auto const denied = nixgrpc::readFarmText(nixgrpc::FarmUnreadable{"Permission denied"});
    assert(denied.farm.empty() && denied.event == "farm_unreadable" && denied.error == "Permission denied");
    auto const unreadable = nixgrpc::readFarmText(std::string(R"({"machines": [{"systems": {"x": []}}]})"));
    assert(unreadable.farm.empty() && unreadable.event == "farm_unreadable" && !unreadable.error.empty());
    auto const read = nixgrpc::readFarmText(std::string(R"({"machines": [{"systems": {"x": []}, "jobs": 2}]})"));
    assert((read.event == "farm" && read.farm == std::vector<FarmMachine>{{.systems = {{"x", {}}}, .jobs = 2}}));
    return 0;
} catch (std::exception &) {
    return 1;
}
