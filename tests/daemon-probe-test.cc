// The probe must tell a nix-daemon that greets a new connection from one
// that is missing, too slow, or not a nix-daemon.

#include <array>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <functional>
#include <latch>
#include <print>
#include <string>
#include <string_view>
#include <thread>
#include <utility>

#include <signal.h> // NOLINT(modernize-deprecated-headers): SIGPIPE is POSIX, not in <csignal>.
#include <sys/socket.h>
#include <sys/stat.h> // NOLINT(misc-include-cleaner): which header has S_IRWXU differs per platform.
#include <unistd.h>

#include <nix/util/file-descriptor.hh>
#include <nix/util/unix-domain-socket.hh>

#include "daemon-probe.hh"

namespace {

// The late nix-daemon greets only after the probe returned: one that gives
// up passes, one that waits for the greeting hangs the test (its liveness
// failure). The patience only keeps the test short.
constexpr std::chrono::milliseconds shortPatience{200};

// Serves every connection to `path` with `serve` until destroyed.
class Listener
{
public:
    Listener(const std::filesystem::path & path, std::function<void(int)> serve)
        // NOLINTNEXTLINE(misc-include-cleaner)
        : sock_(nix::createUnixDomainSocket(path, S_IRWXU))
    {
        thread_ = std::thread([sock = sock_.get(), serve = std::move(serve)] -> void {
            for (;;) {
                int const conn = ::accept(sock, nullptr, nullptr);
                if (conn < 0) {
                    return;
                }
                serve(conn);
                ::close(conn);
            }
        });
    }
    Listener(const Listener &) = delete;
    Listener(Listener &&) = delete;
    auto operator=(const Listener &) -> Listener & = delete;
    auto operator=(Listener &&) -> Listener & = delete;
    ~Listener()
    {
        // Linux wakes the accept on shutdown, macOS on close.
        ::shutdown(sock_.get(), SHUT_RDWR);
        sock_.close();
        thread_.join();
    }

private:
    nix::AutoCloseFD sock_;
    std::thread thread_;
};

void readGreeting(int conn)
{
    std::array<unsigned char, nixgrpc::probe::hello.size()> greeting{};
    (void) ::read(conn, greeting.data(), greeting.size());
}

auto run() -> int
{
    // The probe hangs up on a listener that still writes.
    // NOLINTNEXTLINE(misc-include-cleaner): which header has SIGPIPE differs per platform.
    (void) ::signal(SIGPIPE, SIG_IGN);
    auto const dir = std::filesystem::temp_directory_path() / ("daemon-probe-test-" + std::to_string(::getpid()));
    std::filesystem::create_directory(dir);
    int status = EXIT_SUCCESS;
    auto const expect = [&](std::string_view name, bool answers, std::chrono::milliseconds within = nixgrpc::daemonPatience) -> void {
        if (nixgrpc::nixDaemonAnswers(dir / name, within) != answers) {
            std::println("FAIL a nix-daemon that {} {}", name, answers ? "is taken for dead" : "is taken for alive");
            status = EXIT_FAILURE;
        }
    };
    std::latch gaveUp{1};
    {
        Listener const greets(dir / "greets", [](int conn) -> void {
            readGreeting(conn);
            (void) ::write(conn, nixgrpc::probe::daemonMagic.data(), nixgrpc::probe::daemonMagic.size());
        });
        Listener const late(dir / "greets too late", [&gaveUp](int conn) -> void {
            readGreeting(conn);
            gaveUp.wait();
            (void) ::write(conn, nixgrpc::probe::daemonMagic.data(), nixgrpc::probe::daemonMagic.size());
        });
        Listener const other(dir / "answers something else", [](int conn) -> void {
            readGreeting(conn);
            (void) ::write(conn, nixgrpc::probe::clientMagic.data(), nixgrpc::probe::clientMagic.size());
        });
        expect("greets", true);
        expect("greets too late", false, shortPatience);
        gaveUp.count_down();
        expect("answers something else", false);
        expect("is missing", false);
    }
    std::filesystem::remove_all(dir);
    return status;
}

} // namespace

auto main() -> int
{
    try {
        return run();
    } catch (...) {
        return EXIT_FAILURE;
    }
}
