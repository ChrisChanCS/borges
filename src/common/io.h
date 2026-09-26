#pragma once

#include <unistd.h>
#include <cerrno>
#include <cassert>
#include <absl/base/macros.h>
#include <span>
#include <sys/types.h>
#include <fcntl.h>
#include "absl/log/check.h"
#include "absl/log/log.h"
#include "Protocol.h"
#include "absl/synchronization/mutex.h"
#include <atomic>
#include <string>
#include <utility>

namespace io_utils
{
    // Connection state shared by the I/O and response workers.
    struct ClientConnection
    {
        int fd;
        std::string ip;
        absl::Mutex send_mu_;

        // I/O worker assigned to this connection.
        std::uint32_t assigned_worker = 0;
        // Prevent scheduling the same descriptor more than once.
        std::atomic<bool> scheduled{false};

        // Default constructor
        ClientConnection() : fd(-1), ip("") {}

        // Initialize a connection from its descriptor and address.
        ClientConnection(int f, const std::string &i) : fd(f), ip(i) {}

        // CopyConstructor
        ClientConnection(const ClientConnection &other) : fd(other.fd), ip(other.ip), assigned_worker(other.assigned_worker)
        {
            // A copied connection starts unscheduled.
            scheduled.store(false, std::memory_order_relaxed);
        }

        // Move constructor
        ClientConnection(ClientConnection &&other) noexcept : fd(other.fd), ip(std::move(other.ip)), assigned_worker(other.assigned_worker)
        {
            other.fd = -1;
            scheduled.store(other.scheduled.load(std::memory_order_relaxed), std::memory_order_relaxed);
            other.scheduled.store(false, std::memory_order_relaxed);
        }

        // Copy assignment
        ClientConnection &operator=(const ClientConnection &other)
        {
            if (this != &other)
            {
                fd = other.fd;
                ip = other.ip;
                assigned_worker = other.assigned_worker;
                scheduled.store(false, std::memory_order_relaxed);
            }
            return *this;
        }

        // Move assignment
        ClientConnection &operator=(ClientConnection &&other) noexcept
        {
            if (this != &other)
            {
                fd = other.fd;
                ip = std::move(other.ip);
                assigned_worker = other.assigned_worker;
                other.fd = -1;
                scheduled.store(other.scheduled.load(std::memory_order_relaxed), std::memory_order_relaxed);
                other.scheduled.store(false, std::memory_order_relaxed);
            }
            return *this;
        }
    };

    void FdSetNonblocking(int fd)
    {
        int flags = fcntl(fd, F_GETFL, 0);
        PCHECK(flags != -1) << "fcntl F_GETFL failed";
        PCHECK(fcntl(fd, F_SETFL, flags | O_NONBLOCK) == 0)
            << "fcntl F_SETFL failed";
    }

    template <class T>
    bool SendMessage(int fd, const T &message)
    {
        const char *buffer = reinterpret_cast<const char *>(&message);
        size_t pos = 0;
        while (pos < sizeof(T))
        {
            ssize_t nwrite = write(fd, buffer + pos, sizeof(T) - pos);
            DCHECK(nwrite != 0) << "write() returns 0";
            if (nwrite < 0)
            {
                if (errno == EAGAIN || errno == EINTR)
                {
                    continue;
                }
                return false;
            }
            pos += static_cast<size_t>(nwrite);
        }
        return true;
    }

    template <class T>
    bool RecvMessage(int fd, T *message, bool *eof)
    {
        char *buffer = reinterpret_cast<char *>(message);
        size_t pos = 0;
        if (eof != nullptr)
        {
            *eof = false;
        }
        while (pos < sizeof(T))
        {
            ssize_t nread = read(fd, buffer + pos, sizeof(T) - pos);
            if (nread == 0)
            {
                if (eof != nullptr)
                {
                    *eof = true;
                }
                return false;
            }
            if (nread < 0)
            {
                if (errno == EAGAIN || errno == EWOULDBLOCK)
                {
                    // Return false when no more data is currently available.
                    return false;
                }
                if (errno == EINTR)
                {
                    continue;
                }
                return false;
            }
            pos += static_cast<size_t>(nread);
        }
        return true;
    }

    inline bool SendData(int fd, const char *data, size_t size)
    {
        size_t pos = 0;
        while (pos < size)
        {
            ssize_t nwrite = write(fd, data + pos, size - pos);
            CHECK(nwrite != 0) << "write() returns 0";
            if (nwrite < 0)
            {
                if (errno == EAGAIN || errno == EINTR)
                {
                    continue;
                }
                return false;
            }
            pos += static_cast<size_t>(nwrite);
        }
        return true;
    }

    inline bool SendData(int fd, std::span<const char> data)
    {
        return SendData(fd, data.data(), data.size());
    }

    inline bool RecvData(int fd, char *buffer, size_t size, bool *eof)
    {
        size_t pos = 0;
        if (eof != nullptr)
        {
            *eof = false;
        }
        while (pos < size)
        {
            ssize_t nread = read(fd, buffer + pos, size - pos);
            if (nread == 0)
            {
                if (eof != nullptr)
                {
                    *eof = true;
                }
                return false;
            }
            if (nread < 0)
            {
                if (errno == EAGAIN || errno == EWOULDBLOCK)
                {
                    // Return false when no more data is currently available.
                    return false;
                }
                if (errno == EINTR)
                {
                    continue;
                }
                return false;
            }
            pos += static_cast<size_t>(nread);
        }
        return true;
    }

} // namespace io_utils
