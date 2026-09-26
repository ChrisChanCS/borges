#pragma once

#include <cstdint>
#include <vector>
#include <atomic>
#include <mutex>
#include "../common/Macro.h"
#include "Batch.h"
#include "Replicator.h"
#include "Reader.h"
#include "absl/container/flat_hash_map.h"
#include "absl/synchronization/mutex.h"
#include "BatchManager.h"
#include "absl/flags/flag.h"
#include "absl/flags/parse.h"
#include "absl/time/time.h"
#include "absl/log/log.h"
#include "absl/log/check.h"
#include <memory>
#include <unistd.h>
#include "../common/io.h"
#include "Committer.h"
#include "Response.h"
#include "../common/Protocol.h"

// const size_t CXL_CAPACITY = size_t(1024) * 1024 * 1024 * 100; // 100GB

namespace Shard
{
    enum class WORKLOAD_TYPE
    {
        TEST
    };

    template <typename T, typename F>
    class Shard
    {
    private:
        std::uint32_t shard_id_;
        std::shared_ptr<BatchManager<T, F>> batch_manager_;
        Replicator *replicator_[REPLICATOR_NUM];
        Committer *committer_;

        enum State
        {
            kRunning,
            kStopping,
            kStopped
        };
        std::atomic<State> state_{kRunning};

    public:
        Shard(std::uint32_t shard_id, std::shared_ptr<BatchManager<T, F>> batch_manager, Committer *committer)
            : shard_id_(shard_id), batch_manager_(batch_manager), committer_(committer) {}

        void init_replicator(Replicator *replicator, int id)
        {
            replicator_[id] = replicator;
        }
        Committer *get_committer()
        {
            return committer_;
        }
#ifndef USE_RDMA
        std::uint32_t admitted_lsn() const { return batch_manager_->admitted_lsn(); }
        void stop_writers() { batch_manager_->stop_writers(committer_); }
        void drop_backup() { batch_manager_->drop_backup(shard_id_); }
        void restart_writers() { batch_manager_->restart_writers(shard_id_, committer_); }
#endif

        std::uint32_t get_id()
        {
            return shard_id_;
        }

        void send_response(std::uint32_t &client_id, int fd)
        {
            char buffer[64];
            ResHeader header;
            header.is_write = true;
            header.completed_req_cnt = 1;
            std::uint32_t request_id = 0;
            memcpy(buffer, &header, sizeof(ResHeader));
            memcpy(buffer + sizeof(ResHeader), &request_id, sizeof(std::uint32_t));

            io_utils::SendData(fd, buffer, sizeof(ResHeader) + sizeof(std::uint32_t));
        }

        void process_request(int fd, std::uint32_t client_id)
        {
            while (true)
            {
                std::shared_ptr<ReqHeader> header = std::make_shared<ReqHeader>();
                bool eof = false;
                bool ret = io_utils::RecvMessage(fd, header.get(), &eof);
                if (eof)
                {
                    break;
                }
                // count++;

                // No more data to read or an error occurred
                if (!ret)
                {
                    if (errno == EAGAIN || errno == EWOULDBLOCK)
                    {
                        // All data has been read; exit normally
                        break;
                    }
                    else
                    {
                        // Read error
                        LOG(ERROR) << "Error reading request header: " << strerror(errno);
                        break;
                    }
                }

                // logAppend log
                LOG_CLASS("Shard {}", shard_id_, "receive request, request id: {}", header->request_id);

                append_record(header, client_id, fd);
            }
        }

        // Process one request per call from the connection's assigned worker.
        void process_request_once(int fd, std::uint32_t client_id)
        {
            std::shared_ptr<ReqHeader> header = std::make_shared<ReqHeader>();
            bool eof = false;
            bool ret = io_utils::RecvMessage(fd, header.get(), &eof);
            if (eof)
            {
                // Return on EOF or peer closure; the caller handles cleanup.
                return;
            }
            if (!ret)
            {
                if (errno == EAGAIN || errno == EWOULDBLOCK)
                {
                    // No more data is currently available.
                    return;
                }
                else
                {
                    LOG(ERROR) << "Error reading request header: " << strerror(errno);
                    return;
                }
            }
            LOG_CLASS("Shard {}", shard_id_, "receive request, request id: {}", header->request_id);
            append_record(header, client_id, fd);
        }

        void append_record(std::shared_ptr<ReqHeader> header, std::uint32_t &client_id, const int &socket_fd)
        {
            if (WORKLOAD == 2 || WORKLOAD == 3)
            {
                batch_manager_->append_lock_record_with_scaling(header, client_id, socket_fd);
            }
            else if (WORKLOAD == 0 || WORKLOAD == 1)
            {
                if ((WORKLOAD == 1 && YCSB_OPTION == 3) || (WORKLOAD == 0 && SCALE_OUT_STREAM))
                {
                    batch_manager_->append_record_with_scaling_and_new_key(header, client_id, socket_fd);
                }
                else
                {
                    batch_manager_->append_record_with_scaling(header, client_id, socket_fd);
                }
            }
            else if (WORKLOAD == 4)
            {
                batch_manager_->append_record_with_scaling_and_new_key(header, client_id, socket_fd);
            }
            else
            {
                LOG(ERROR) << "invalid workload: " << WORKLOAD;
                exit(-1);
            }
        }

        void append_write_record(std::shared_ptr<ReqHeader> header, std::uint32_t &client_id, const int &socket_fd)
        {
            batch_manager_->append_record_with_scaling(header, client_id, socket_fd);
        }

        void append_read_record(std::shared_ptr<ReqHeader> header, std::uint32_t &client_id, const int &socket_fd)
        {
            batch_manager_->append_record_with_scaling(header, client_id, socket_fd);
        }

        Stream *get_stream(const StateKey &state_key, int replicator_id)
        {
            return replicator_[replicator_id]->get_stream(state_key);
        }
    };
} // namespace Shard
