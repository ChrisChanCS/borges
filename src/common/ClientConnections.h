#pragma once

#include "io.h"
#include <absl/container/flat_hash_map.h>

namespace io_utils
{
    using ClientConnectionMap = absl::flat_hash_map<std::uint32_t, ClientConnection *>;

    // Lock order is always directory, then connection. Deletion takes both
    // locks too, so the connection remains alive after releasing the directory.
    // No hash-table reference or iterator escapes the directory lock.
    class LockedClientConnection
    {
        ClientConnection *connection_ = nullptr;

    public:
        LockedClientConnection(ClientConnectionMap &connections, absl::Mutex &mutex, std::uint32_t id)
        {
            absl::MutexLock lock(&mutex);
            const auto found = connections.find(id);
            if (found != connections.end())
            {
                connection_ = found->second;
                connection_->send_mu_.Lock();
            }
        }

        ~LockedClientConnection()
        {
            if (connection_)
                connection_->send_mu_.Unlock();
        }

        LockedClientConnection(const LockedClientConnection &) = delete;
        LockedClientConnection &operator=(const LockedClientConnection &) = delete;
        explicit operator bool() const { return connection_ != nullptr; }
        int fd() const { return connection_->fd; }
    };
}
