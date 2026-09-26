#pragma once

#include "ReconfigControl.h"
#include <functional>
#include <chrono>

namespace SharedData
{
    // These messages use separate control sockets, never the request queues.
    enum MaintenanceCommand : std::uint32_t
    {
        trim_request = 3,
        delete_request = 4,
        degrade_backup_request = 5,
        rebuild_backup_request = 6,
        finish_backup_request = 7,
        maintenance_status = 8,
        pause_ingress = 16,
        arm_readers = 17,
        wait_readers = 18,
        stop_writers = 19,
        reload_shard = 20,
        resume_shard = 21,
        drop_backup = 22,
        copy_backup = 23,
        backup_ready = 24,
        catchup_backup = 25,
        cancel_backup = 26,
    };

    class MaintenanceServer
    {
        int listener_ = -1;
        std::thread thread_;
        std::atomic<bool> stopping_{false};
    public:
        void start(std::uint16_t port, std::function<JoinReply(JoinRequest)> handle)
        {
            listener_ = socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
            int reuse = 1;
            setsockopt(listener_, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));
            sockaddr_in address{};
            address.sin_family = AF_INET;
            address.sin_addr.s_addr = htonl(INADDR_ANY);
            address.sin_port = htons(port);
            if (listener_ < 0 || bind(listener_, reinterpret_cast<sockaddr *>(&address), sizeof(address)) ||
                listen(listener_, 8))
                throw std::runtime_error("cannot listen for shard maintenance");
            thread_ = std::thread([this, handle = std::move(handle)]
            {
                while (!stopping_.load(std::memory_order_relaxed))
                {
                    const int fd = accept4(listener_, nullptr, nullptr, SOCK_CLOEXEC);
                    if (fd < 0)
                        continue;
                    control_timeout(fd);
                    JoinRequest request;
                    JoinReply reply;
                    if (control_transfer(fd, &request, sizeof(request), false))
                    {
                        try { reply = handle(request); }
                        catch (const std::exception &error) { LOG(ERROR) << error.what(); }
                        control_transfer(fd, &reply, sizeof(reply), true);
                    }
                    close(fd);
                }
            });
        }
        ~MaintenanceServer()
        {
            stopping_.store(true, std::memory_order_relaxed);
            if (listener_ >= 0)
            {
                shutdown(listener_, SHUT_RDWR);
                close(listener_);
            }
            if (thread_.joinable())
                thread_.join();
        }
    };

    // Private DRAM; only the control path and epoch transitions access it.
    class MaintenanceBarrier
    {
        std::mutex mutex_;
        std::condition_variable changed_;
        std::uint64_t epoch_ = 0;
        std::size_t parked_ = 0;
    public:
        void arm(std::uint64_t epoch)
        {
            std::lock_guard lock(mutex_);
            if (epoch_ != 0 || epoch == 0)
                throw std::runtime_error("maintenance barrier is already armed");
            epoch_ = epoch;
            parked_ = 0;
        }
        bool requested(std::uint64_t epoch)
        {
            std::lock_guard lock(mutex_);
            return epoch_ == epoch;
        }
        void park()
        {
            std::unique_lock lock(mutex_);
            ++parked_;
            changed_.notify_all();
            changed_.wait(lock, [&] { return epoch_ == 0; });
            --parked_;
            changed_.notify_all();
        }
        void wait(std::size_t workers)
        {
            std::unique_lock lock(mutex_);
            if (!changed_.wait_for(lock, std::chrono::seconds(110), [&] { return parked_ == workers; }))
                throw std::runtime_error("maintenance workers did not reach their barrier");
        }
        void resume()
        {
            std::unique_lock lock(mutex_);
            epoch_ = 0;
            changed_.notify_all();
            changed_.wait(lock, [&] { return parked_ == 0; });
        }
    };
}
