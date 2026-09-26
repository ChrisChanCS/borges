#pragma once

#include "ClusterConfig.h"
#include <arpa/inet.h>
#include <condition_variable>
#include <mutex>
#include <sys/socket.h>
#include <unistd.h>
#include <string>
#include <utility>

namespace SharedData
{
    struct JoinRequest
    {
        std::uint32_t format = 1;
        std::uint32_t shard = 0;
        std::uint64_t settings = 0;
    };
    struct JoinReply
    {
        std::uint64_t status = 1;
        std::uint64_t epoch = 0;
    };

    inline bool control_transfer(int fd, void *data, std::size_t bytes, bool sending)
    {
        auto *cursor = static_cast<char *>(data);
        while (bytes)
        {
            const auto n = sending ? send(fd, cursor, bytes, MSG_NOSIGNAL) : recv(fd, cursor, bytes, 0);
            if (n < 0 && errno == EINTR)
                continue;
            if (n <= 0)
                return false;
            cursor += n;
            bytes -= n;
        }
        return true;
    }

    inline void control_timeout(int fd)
    {
        timeval timeout{120, 0};
        setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
        setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));
    }

    // TCP serializes admission; no joining hosts contend on a non-coherent
    // CXL fetch_add or a preallocated per-shard request array.
    class ReconfigInbox
    {
    public:
        struct Operation
        {
            JoinRequest request;
            JoinReply reply;
            std::mutex mutex;
            std::condition_variable done;
            bool complete = false;
        };

    private:
        int listener_ = -1;
        std::thread thread_;
        std::atomic<bool> stopping_{false};
        std::mutex mutex_;
        std::shared_ptr<Operation> pending_;

    public:
        void start(std::uint16_t port, std::atomic<std::uint64_t> &notifications)
        {
            listener_ = socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
            int reuse = 1;
            setsockopt(listener_, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));
            sockaddr_in address{};
            address.sin_family = AF_INET;
            address.sin_addr.s_addr = htonl(INADDR_ANY);
            address.sin_port = htons(port);
            if (listener_ < 0 || bind(listener_, reinterpret_cast<sockaddr *>(&address), sizeof(address)) != 0 ||
                listen(listener_, 8) != 0)
                throw std::runtime_error("cannot listen for shard joins");
            thread_ = std::thread([this, &notifications]
            {
                while (!stopping_.load(std::memory_order_relaxed))
                {
                    const int fd = accept4(listener_, nullptr, nullptr, SOCK_CLOEXEC);
                    if (fd < 0)
                        continue;
                    control_timeout(fd);
                    auto operation = std::make_shared<Operation>();
                    if (control_transfer(fd, &operation->request, sizeof(JoinRequest), false))
                    {
                        {
                            std::lock_guard lock(mutex_);
                            pending_ = operation;
                        }
                        notifications.fetch_or(reconfiguration_pending, std::memory_order_release);
                        std::unique_lock lock(operation->mutex);
                        operation->done.wait(lock, [&] { return operation->complete; });
                        control_transfer(fd, &operation->reply, sizeof(JoinReply), true);
                    }
                    close(fd);
                }
            });
        }

        std::shared_ptr<Operation> take()
        {
            std::lock_guard lock(mutex_);
            return std::exchange(pending_, nullptr);
        }

        static void finish(const std::shared_ptr<Operation> &operation, JoinReply reply)
        {
            std::lock_guard lock(operation->mutex);
            operation->reply = reply;
            operation->complete = true;
            operation->done.notify_one();
        }

        ~ReconfigInbox()
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

    inline JoinReply request_join(const std::string &address, std::uint16_t port, JoinRequest request)
    {
        const int fd = socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
        sockaddr_in endpoint{};
        endpoint.sin_family = AF_INET;
        endpoint.sin_port = htons(port);
        control_timeout(fd);
        JoinReply reply;
        const bool success = fd >= 0 && inet_pton(AF_INET, address.c_str(), &endpoint.sin_addr) == 1 &&
            connect(fd, reinterpret_cast<sockaddr *>(&endpoint), sizeof(endpoint)) == 0 &&
            control_transfer(fd, &request, sizeof(request), true) &&
            control_transfer(fd, &reply, sizeof(reply), false);
        if (fd >= 0)
            close(fd);
        if (!success || reply.status != 0)
            throw std::runtime_error("shard join rejected or unavailable; check shard ID and cluster settings");
        return reply;
    }
}
