#include "Socket.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <string>
#include <sys/socket.h>
#include <unistd.h>

#include <glog/logging.h>
#include "absl/log/check.h"

namespace io_utils
{
    namespace
    {
        static bool SetSocketOption(int sockfd, int option, int value)
        {
            if (setsockopt(sockfd, SOL_SOCKET, option, (const void *)&value, sizeof(int)) != 0)
            {
                PLOG(ERROR) << "setsockopt failed";
                return false;
            }
            return true;
        }

        static bool FillTcpSocketAddr(struct sockaddr_in *addr,
                                      std::string_view ip, uint16_t port)
        {
            addr->sin_family = AF_INET;
            addr->sin_port = htons(port);
            if (inet_aton(std::string(ip).c_str(), &addr->sin_addr) != 1)
            {
                return false;
            }
            return true;
        }
    } // namespace

    bool SocketListen(int sockfd, int backlog)
    {
        if (listen(sockfd, backlog) != 0)
        {
            PLOG(ERROR) << "Failed to listen with backlog " << backlog;
            return false;
        }
        return true;
    }

    int TcpSocketBindAndListen(std::string_view ip, uint16_t port, int backlog)
    {
        struct sockaddr_in sockaddr;
        if (!FillTcpSocketAddr(&sockaddr, ip, port))
        {
            LOG(ERROR) << "Failed to fill socket addr: " << ip << ":" << port;
            return -1;
        }
        int fd = socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
        if (fd == -1)
        {
            PLOG(ERROR) << "Failed to create AF_INET socket";
            return -1;
        }
        CHECK(SetSocketOption(fd, SO_REUSEPORT, 1));
        if (bind(fd, (struct sockaddr *)&sockaddr, sizeof(sockaddr)) != 0)
        {
            PLOG(ERROR) << "Failed to bind to " << ip << ":" << port;
            close(fd);
            return -1;
        }
        if (!SocketListen(fd, backlog))
        {
            close(fd);
            return -1;
        }
        return fd;
    }
} // namespace io_utils
