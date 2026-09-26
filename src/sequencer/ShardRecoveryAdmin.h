#pragma once

#include <condition_variable>
#include <filesystem>
#include <map>
#include <mutex>
#include <spawn.h>
#include <sys/wait.h>
#include <thread>
#include <vector>
#include <string>

extern char **environ;

namespace Sequencer
{
    // Runs inside the sequencer. SSH is only a process launcher; admission and
    // fencing remain the sequencer's existing lease/configuration protocol.
    class ShardRecoveryAdmin
    {
    public:
        struct Settings
        {
            bool enabled = true;
            std::string user = "root";
            std::uint16_t port = 22;
            std::string directory = std::filesystem::current_path().string();
            std::string executable = (std::filesystem::read_symlink("/proc/self/exe").parent_path() / "server").string();
            std::vector<std::string> ssh_options;
            std::string library_path = std::getenv("LD_LIBRARY_PATH") ? std::getenv("LD_LIBRARY_PATH") : "";
        };
    private:
        Settings settings_;
        std::vector<std::string> addresses_;
        std::mutex mutex_;
        std::condition_variable changed_;
        std::map<std::size_t, std::uint64_t> pending_;
        bool stopping_ = false;
        std::thread thread_;

        static std::string quote(const std::string &value)
        {
            std::string result = "'";
            for (char c : value)
                result += c == '\'' ? "'\\''" : std::string(1, c);
            return result + "'";
        }
        void launch(std::size_t shard)
        {
            const auto address = shard < addresses_.size() ? addresses_[shard] :
                "192.168.100." + std::to_string(shard + 3);
            const auto id = std::to_string(shard);
            // The lock is held by the server after exec, including during slow
            // recovery. Repeated launch attempts cannot start two incarnations.
            // Background only the launcher, not the cd/launch AND-list: a
            // waiting shell for that list would retain the SSH channel pipes.
            const auto command = "cd " + quote(settings_.directory) + " && { nohup env " +
                quote("LD_LIBRARY_PATH=" + settings_.library_path) + " flock --nonblock --no-fork " +
                quote("rhodes-shard-" + id + ".lock") + " " + quote(settings_.executable) + " " + id +
                " --recover >> " + quote("recovery-shard-" + id + ".log") + " 2>&1 < /dev/null & }";
            std::vector<std::string> arguments{"ssh", "-n", "-oBatchMode=yes", "-oConnectTimeout=2",
                "-oServerAliveInterval=2", "-oServerAliveCountMax=2", "-p", std::to_string(settings_.port)};
            arguments.insert(arguments.end(), settings_.ssh_options.begin(), settings_.ssh_options.end());
            arguments.push_back(settings_.user + "@" + address);
            arguments.push_back(command);
            std::vector<char *> argv;
            for (auto &argument : arguments)
                argv.push_back(argument.data());
            argv.push_back(nullptr);
            pid_t pid;
            const int error = posix_spawnp(&pid, "ssh", nullptr, nullptr, argv.data(), environ);
            if (error)
            {
                LOG(WARNING) << "Cannot start shard recovery SSH: " << strerror(error);
                return;
            }
            int status;
            while (waitpid(pid, &status, 0) < 0 && errno == EINTR) {}
        }
    public:
        void start(Settings settings, std::vector<std::string> addresses)
        {
            settings_ = std::move(settings);
            addresses_ = std::move(addresses);
            if (!settings_.enabled)
                return;
            thread_ = std::thread([this]
            {
                std::unique_lock lock(mutex_);
                while (!stopping_)
                {
                    changed_.wait(lock, [&] { return stopping_ || !pending_.empty(); });
                    if (stopping_)
                        break;
                    const auto pending = pending_;
                    lock.unlock();
                    for (const auto &[shard, epoch] : pending)
                        launch(shard);
                    lock.lock();
                    changed_.wait_for(lock, std::chrono::seconds(1), [&] { return stopping_ || pending_.empty(); });
                }
            });
        }
        void schedule(std::uint64_t shards, std::uint64_t epoch)
        {
            if (!settings_.enabled)
                return;
            std::lock_guard lock(mutex_);
            for (std::size_t shard = 0; shard < SharedData::max_cluster_shards; ++shard)
                if (shards & (std::uint64_t{1} << shard))
                    pending_[shard] = epoch;
            changed_.notify_one();
        }
        void cancel(std::size_t shard)
        {
            std::lock_guard lock(mutex_);
            pending_.erase(shard);
            changed_.notify_one();
        }
        ~ShardRecoveryAdmin()
        {
            {
                std::lock_guard lock(mutex_);
                stopping_ = true;
                changed_.notify_one();
            }
            if (thread_.joinable())
                thread_.join();
        }
    };
}
