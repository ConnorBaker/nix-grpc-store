#pragma once
// Options::farmFile: the farm a builder hands its builds to (Hello.farm), as
// whoever runs the farm writes it:
//
//   {"machines": [{"systems": {"x86_64-linux": ["kvm", "nixos-test"]}, "jobs": 8}, ...]}
//
// One machine per set of build slots: the features it offers for each system
// it builds, and how many builds the farm takes for it at once, of all its
// systems together (sched::FarmFlow). A machine that is known but away
// takes 0.

#include <cstdint>
#include <limits>
#include <optional>
#include <string>
#include <string_view>
#include <variant>
#include <vector>

#include <nlohmann/json.hpp>

#include <nix/util/error.hh>

#include "scheduler.hh"

namespace nixgrpc {

namespace detail {
inline auto strings(const nlohmann::json & doc) -> std::vector<std::string>
{
    if (!doc.is_array()) {
        throw nix::Error("farm: features are not a list");
    }
    std::vector<std::string> out;
    for (const auto & item : doc) {
        if (!item.is_string()) {
            throw nix::Error("farm: a feature is not a string");
        }
        out.push_back(item.get<std::string>());
    }
    return out;
}
} // namespace detail

// Throws nix::Error unless `text` is a farm as above.
inline auto parseFarm(std::string_view text) -> std::vector<sched::FarmMachine>
{
    auto const doc = nlohmann::json::parse(text, nullptr, false);
    if (!doc.is_object() || !doc.contains("machines") || !doc.at("machines").is_array()) {
        throw nix::Error("farm: not an object with a list of machines");
    }
    std::vector<sched::FarmMachine> farm;
    for (const auto & machine : doc.at("machines")) {
        if (!machine.is_object() || !machine.contains("systems") || !machine.at("systems").is_object()
            || !machine.contains("jobs")) {
            throw nix::Error("farm: a machine without systems or jobs");
        }
        const auto & jobs = machine.at("jobs");
        if (!jobs.is_number_unsigned() || jobs.get<uint64_t>() > std::numeric_limits<uint32_t>::max()) {
            throw nix::Error("farm: jobs is not a count of builds");
        }
        sched::FarmMachine mach{.systems = {}, .jobs = jobs.get<uint32_t>()};
        for (const auto & [system, features] : machine.at("systems").items()) {
            mach.systems.emplace(system, detail::strings(features));
        }
        farm.push_back(std::move(mach));
    }
    return farm;
}

// Why a farm file's text could not be had.
struct FarmUnreadable
{
    std::string error;
};
// None: absent.
using FarmText = std::variant<std::monostate, std::string, FarmUnreadable>;

// The farm a builder takes from its farm file, and why. A file that is
// absent or not a farm gives no farm: the builder admits builds as it would
// without one, never by a farm nobody stated.
struct FarmRead
{
    std::vector<sched::FarmMachine> farm;
    std::string event; // "farm", "farm_absent" or "farm_unreadable"
    std::string error;
};

inline auto readFarmText(const FarmText & text) -> FarmRead
{
    if (std::holds_alternative<std::monostate>(text)) {
        return {.farm = {}, .event = "farm_absent", .error = ""};
    }
    if (const auto * why = std::get_if<FarmUnreadable>(&text)) {
        return {.farm = {}, .event = "farm_unreadable", .error = why->error};
    }
    try {
        return {.farm = parseFarm(std::get<std::string>(text)), .event = "farm", .error = ""};
    } catch (nix::Error & err) {
        return {.farm = {}, .event = "farm_unreadable", .error = err.msg()};
    }
}

// For the log: "x86_64-linux[kvm,nixos-test]=8 aarch64-linux[]=4".
inline auto describeFarm(const std::vector<sched::FarmMachine> & farm) -> std::string
{
    std::string out;
    for (const auto & machine : farm) {
        out += out.empty() ? "" : " ";
        bool first = true;
        for (const auto & [system, features] : machine.systems) {
            out += (first ? "" : "+") + system + "[";
            first = false;
            for (size_t i = 0; i < features.size(); i++) {
                out += (i == 0 ? "" : ",") + features.at(i);
            }
            out += ']';
        }
        out += "=" + std::to_string(machine.jobs);
    }
    return out;
}

} // namespace nixgrpc
