#pragma once

#include <vector>
#include <thread>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <netinet/tcp.h>
#include <functional>
#include <atomic>
#include <string>
#include <span>
#include <memory>
#include <arpa/inet.h>
#include "../common/io.h"
#include "../common/Socket.h"
#include <chrono>
#include <roaring/roaring.hh>
#include <sys/epoll.h>
#include <poll.h>
#include <unordered_map>
#include <optional>
#include <sys/stat.h>
#include <cassert>
#include <glog/logging.h>
#include "../common/Macro.h"
#include "Shard.h"
#include <cstdint>
#include "../common/flags.h"
#include <absl/container/flat_hash_map.h>
#include <absl/synchronization/mutex.h>

namespace io_utils
{

// Declare accept4 if the system headers do not expose it.
#ifndef SOCK_NONBLOCK
#define SOCK_NONBLOCK O_NONBLOCK
#endif

#ifndef SOCK_CLOEXEC
#define SOCK_CLOEXEC O_CLOEXEC
#endif

    // TCP listener wrapper.
    class TcpServer
    {
    private:
        int fd_;

    public:
        TcpServer(const std::string &ip, uint16_t port, int backlog = absl::GetFlag(FLAGS_socket_listen_backlog))
            : fd_(TcpSocketBindAndListen(ip, port, backlog))
        {
            if (fd_ != -1)
            {
                FdSetNonblocking(fd_);
            }
        }

        ~TcpServer()
        {
            if (fd_ != -1)
            {
                close(fd_);
            }
        }

        // Accept a connection and return its descriptor and peer address.
        struct AcceptResult
        {
            int client_fd;
            std::string client_ip;
            uint16_t client_port;

            AcceptResult() : client_fd(-1), client_ip(""), client_port(0) {}
        };

        AcceptResult Accept()
        {
            AcceptResult result;
            pollfd ready{fd_, POLLIN, 0};
            if (poll(&ready, 1, 100) <= 0)
            {
                errno = EAGAIN;
                return result;
            }
            struct sockaddr_in addr;
            socklen_t addrlen = sizeof(addr);
            result.client_fd = accept4(fd_, (struct sockaddr *)&addr, &addrlen, SOCK_NONBLOCK);

            if (result.client_fd != -1)
            {
                int flag = 1;
                setsockopt(result.client_fd, IPPROTO_TCP, TCP_NODELAY, (char *)&flag, sizeof(int));
                result.client_ip = inet_ntoa(addr.sin_addr);
                result.client_port = ntohs(addr.sin_port);
            }
            return result;
        }

        bool IsValid() const { return fd_ != -1; }
    };

    // TCP request server.
    template <typename T, typename F>
    class ServerBase
    {
    private:
        // Task passed to an I/O worker.
        struct Task
        {
            int fd;
            std::uint32_t client_id;
        };

        absl::flat_hash_map<std::uint32_t, io_utils::ClientConnection *> client_connections_;
        absl::Mutex connections_mutex_;

        absl::Mutex processing_fds_mutex_;

        TcpServer server_;
        Shard::Shard<T, F> *shard_;
        Responser<T, F> *responser_;
        std::thread accept_thread_;
        std::thread listen_thread_;
        std::vector<std::thread> io_workers_;
        // Preserve the existing per-dispatch state load; 2 parks I/O workers
        // only for an explicit control operation.
        std::atomic<unsigned> running_{1};
#ifndef USE_RDMA
        SharedData::MaintenanceBarrier maintenance_io_;
#endif

        // Per-worker task queues.
        std::vector<std::queue<Task>> worker_queues_;
        std::vector<std::unique_ptr<std::mutex>> worker_queue_mutexes_;
        std::vector<std::unique_ptr<std::condition_variable>> worker_cvs_;

        // epoll state.
        int epoll_fd_;
        static constexpr int MAX_EVENTS = 16;
        int IO_WORKER_COUNT = IO_WORKER_NUM; // I/O worker count.

        // Next available client ID.
        std::atomic<std::uint32_t> next_client_id_{0};

        // Allocate a client ID.
        inline std::uint32_t AllocateClientId()
        {
            return next_client_id_.fetch_add(1, std::memory_order_seq_cst);
        }

        // Connection-accepting thread.
        void AcceptThread()
        {
            while (running_)
            {
                auto accept_result = server_.Accept();

                if (accept_result.client_fd != -1)
                {
                    // Allocate a client ID.
                    std::uint32_t client_id = AllocateClientId();

                    {
                        absl::MutexLock lock(&connections_mutex_);
                        // Store the connection under its client ID.
                        client_connections_[client_id] = new io_utils::ClientConnection(
                            accept_result.client_fd,
                            accept_result.client_ip);
                        // Assign the connection to a fixed I/O worker using client_id modulo the worker count.
                        client_connections_[client_id]->assigned_worker = client_id % IO_WORKER_COUNT;
                        client_connections_[client_id]->scheduled.store(false, std::memory_order_relaxed);
                    }

                    // Register the descriptor with epoll.
                    struct epoll_event ev;
                    ev.events = EPOLLIN | EPOLLRDHUP; // Use level-triggered readiness.
                    ev.data.fd = accept_result.client_fd;

                    if (epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, accept_result.client_fd, &ev) == -1)
                    {
                        LOG(ERROR) << "Failed to register fd with epoll: " << strerror(errno);
                        absl::MutexLock lock(&connections_mutex_);
                        delete client_connections_.at(client_id);
                        client_connections_.erase(client_id);
                        close(accept_result.client_fd);
                        continue;
                    }

                }
                else if (errno != EAGAIN && errno != EWOULDBLOCK)
                {
                    LOG(ERROR) << "AcceptError: " << strerror(errno);
                }
                else
                {
                }
            }
        }

        // Dispatch readable descriptors to their assigned I/O workers.
        void ListenThread()
        {
            struct epoll_event events[MAX_EVENTS];

            while (running_)
            {
                // Wait for events on registered client descriptors.
                int nfds = epoll_wait(epoll_fd_, events, MAX_EVENTS, 100);

                if (nfds == -1)
                {
                    if (errno != EINTR)
                    {
                        LOG(ERROR) << "epoll_waitError: " << strerror(errno);
                    }
                    continue;
                }

                for (int i = 0; i < nfds; i++)
                {
                    int fd = events[i].data.fd;

                    // Handle readable client descriptors.
                    if ((events[i].events & EPOLLIN) && !(events[i].events & (EPOLLERR | EPOLLHUP | EPOLLRDHUP)))
                    {
                        std::uint32_t client_id = 0;
                        io_utils::ClientConnection *conn = nullptr;
                        {
                            absl::MutexLock lock(&connections_mutex_);
                            for (const auto &pair : client_connections_)
                            {
                                if (pair.second->fd == fd)
                                {
                                    client_id = pair.first;
                                    conn = pair.second;
                                    break;
                                }
                            }
                        }

                        if (conn != nullptr)
                        {
                            // Skip an already scheduled descriptor until its worker finishes.
                            bool expected = false;
                            if (conn->scheduled.compare_exchange_strong(expected, true, std::memory_order_acq_rel))
                            {
                                // Enqueue on the connection's assigned worker.
                                std::uint32_t worker_idx = conn->assigned_worker % IO_WORKER_COUNT;
                                {
                                    std::lock_guard<std::mutex> lock(*worker_queue_mutexes_[worker_idx]);
                                    worker_queues_[worker_idx].push({fd, client_id});
                                }
                                worker_cvs_[worker_idx]->notify_one();
                            }
                        }
                        else
                        {
                            LOG(WARNING) << "No client ID found for fd " << fd << "; the connection may have been removed";
                        }
                    }

                    // Handle connection errors or closure.
                    if (events[i].events & (EPOLLERR | EPOLLHUP | EPOLLRDHUP))
                    {
                        LOG(INFO) << "Connection closed or failed, fd: " << fd;

                        {
                            absl::MutexLock lock(&connections_mutex_);
                            for (auto it = client_connections_.begin(); it != client_connections_.end();)
                            {
                                if (it->second->fd == fd)
                                {
                                    // A queued I/O worker still owns this descriptor.
                                    // Keep it alive until that worker releases it.
                                    if (it->second->scheduled.load(std::memory_order_acquire))
                                    {
                                        ++it;
                                        continue;
                                    }
                                    LOG(INFO) << "Removing client " << it->first << " connection";
                                    {
                                        // Finish an in-flight response before closing
                                        // the descriptor or destroying its send lock.
                                        absl::MutexLock send_lock(&it->second->send_mu_);
                                        epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd, nullptr);
                                        close(fd);
                                    }
                                    delete it->second;
                                    it->second = nullptr;
                                    client_connections_.erase(it++);
                                }
                                else
                                {
                                    ++it;
                                }
                            }
                        }

                    }
                }
            }
        }

        // I/O worker entry point.
        void IOWorker(std::uint32_t worker_index)
        {
            Task task;
            while (running_)
            {
                {
                    std::unique_lock<std::mutex> lock(*worker_queue_mutexes_[worker_index]);
                    while (worker_queues_[worker_index].empty() && running_ == 1)
                    {
                        worker_cvs_[worker_index]->wait(lock, [this, worker_index]
                                                        { return !worker_queues_[worker_index].empty() || running_ != 1; });
                    }

                    if (running_ != 1)
                    {
                        if (!running_)
                            return;
#ifndef USE_RDMA
                        lock.unlock();
                        maintenance_io_.park();
                        continue;
#endif
                    }

                    if (!worker_queues_[worker_index].empty())
                    {
                        task = worker_queues_[worker_index].front();
                        worker_queues_[worker_index].pop();
                    }
                    else
                    {
                        continue;
                    }
                }
                // Process the request.
                // Process one request per dispatch to avoid monopolizing the worker.
                shard_->process_request_once(task.fd, task.client_id);

                // Clear the scheduled flag so the next readiness event can enqueue this connection.
                {
                    absl::MutexLock lock(&connections_mutex_);
                    auto it = client_connections_.find(task.client_id);
                    if (it != client_connections_.end())
                    {
                        it->second->scheduled.store(false, std::memory_order_release);
                    }
                }
            }
        }

    public:
#ifndef USE_RDMA
        void pause_ingress()
        {
            maintenance_io_.arm(1);
            running_.store(2);
            for (std::size_t i = 0; i < worker_cvs_.size(); ++i)
            {
                std::lock_guard lock(*worker_queue_mutexes_[i]);
                worker_cvs_[i]->notify_all();
            }
            maintenance_io_.wait(IO_WORKER_COUNT);
        }
        void resume_ingress()
        {
            running_.store(1);
            maintenance_io_.resume();
        }
#endif
        ServerBase(const std::string &ip, uint16_t port, Shard::Shard<T, F> *shard, Responser<T, F> *responser)
            : server_(ip, port), shard_(shard), responser_(responser)
        {
            responser_->set_client_connections(&client_connections_);
            responser_->set_connections_mutex(&connections_mutex_);
            responser_->run();

            // Create the epoll instance.
            epoll_fd_ = epoll_create1(0);
            if (epoll_fd_ == -1)
            {
                LOG(FATAL) << "Failed to create epoll instance: " << strerror(errno);
                return;
            }

            // Start the I/O workers.
            worker_queues_.resize(IO_WORKER_COUNT);
            worker_queue_mutexes_.reserve(IO_WORKER_COUNT);
            worker_cvs_.reserve(IO_WORKER_COUNT);
            for (int i = 0; i < IO_WORKER_COUNT; ++i)
            {
                worker_queue_mutexes_.emplace_back(std::make_unique<std::mutex>());
                worker_cvs_.emplace_back(std::make_unique<std::condition_variable>());
            }
            for (int i = 0; i < IO_WORKER_COUNT; i++)
            {
                io_workers_.emplace_back(&ServerBase::IOWorker, this, static_cast<std::uint32_t>(i));
            }

            // Start the event-dispatch thread.
            listen_thread_ = std::thread(&ServerBase::ListenThread, this);

            // Start the accept thread.
            accept_thread_ = std::thread(&ServerBase::AcceptThread, this);

            LOG(INFO) << "Server listening on " << ip << ":" << port << ", I/O workers: " << IO_WORKER_COUNT;
        }

        ~ServerBase()
        {
            Stop();

            // Close all connections.
            {
                absl::MutexLock lock(&connections_mutex_);
                for (auto &conn : client_connections_)
                {
                    if (conn.second)
                    {
                        close(conn.second->fd);
                        delete conn.second;
                        conn.second = nullptr;
                    }
                }
                client_connections_.clear();
            }

            // Close epoll.
            if (epoll_fd_ != -1)
            {
                close(epoll_fd_);
            }
        }

        void Stop()
        {
            if (running_)
            {
                running_ = false;
#ifndef USE_RDMA
                maintenance_io_.resume();
#endif
                for (auto &cv : worker_cvs_)
                {
                    cv->notify_all(); // Wake all waiting workers.
                }

                if (accept_thread_.joinable())
                {
                    accept_thread_.join();
                }

                if (listen_thread_.joinable())
                {
                    listen_thread_.join();
                }

                for (auto &worker : io_workers_)
                {
                    if (worker.joinable())
                    {
                        worker.join();
                    }
                }
            }
        }
    };

} // namespace io_utils
