#pragma once

#include <cstdint>
#include <string_view>

namespace io_utils
{
    // Listen on an already bound socket.
    bool SocketListen(int sockfd, int backlog);

    // Return the listening socket, or -1 on failure.
    int TcpSocketBindAndListen(std::string_view ip, uint16_t port, int backlog = 4);
} // namespace io_utils
