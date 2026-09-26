#pragma once

#include "../common/Cacheline.h"
#include <atomic>
#include <thread>
#include <vector>
#include <cstdint>
#include <mutex>
#include <cstring>
#include <queue>
#include <condition_variable>
#include <roaring/roaring.hh>
#include "absl/synchronization/mutex.h"
#include "absl/container/flat_hash_map.h"
#include "BufferPool.h"

std::atomic<std::uint64_t> batch_id_counter(0);

// Batch states.
enum class BatchState
{
    WAITING,     // Waiting for entries
    COLLECTING,  // Collecting entries
    READY,       // Collection complete
    REPLICATING, // Replicating
    COMPLETED    // Replication complete
};

int batch_cnt = 0;

// State shared by the producer and consumers of one batch.
class Batch
{
private:
    std::uint64_t batch_id_;
    std::uint64_t lsn_{0};
    absl::Mutex mu_;
    absl::Mutex entry_count_mu_;
    absl::flat_hash_map<StateKey, std::uint64_t /*offset*/> new_state_key_;
    bool consumed_replicator_[REPLICATOR_NUM]{false};   // Replicators that have consumed this batch.
    std::atomic<uint32_t> finished_replicators_{0};     // Number of replicators that have finished copying.
    std::atomic<BatchState> state{BatchState::WAITING}; // batchStatus

    std::atomic<std::uint64_t> entry_count_active_writers_{0}; // Upper 32 bits: entry count; lower 32 bits: active writers.

    Pending_area_state_pool *pending_area_state_pool_;
    absl::Mutex read_buffer_mu_;
    char *read_buffer_;
    std::uint32_t read_buffer_offset_{0};
    absl::flat_hash_map<StateKey, Pending_area_state *> pending_area_by_state_;

    absl::Mutex need_scaling_mu_;
    // Keys requiring another segment and the byte offset at which it is needed.
    absl::flat_hash_map<StateKey, std::uint32_t> need_scaling_by_state_;

public:
    Batch()
    {
        pending_area_state_pool_ = new Pending_area_state_pool();
        pending_area_by_state_.reserve(MAX_ENTRIES_PER_BATCH);
        need_scaling_by_state_.reserve(MAX_ENTRIES_PER_BATCH);
        batch_id_ = batch_id_counter.fetch_add(1, std::memory_order_seq_cst);
        read_buffer_ = new char[MAX_ENTRIES_PER_BATCH * ENTRY_MAX_SIZE];
    }
    ~Batch()
    {
        delete[] read_buffer_;
    }
    BatchState get_state() const
    {
        return state.load(std::memory_order_acquire);
    }
    void mark_consumed_by_replicator(std::uint32_t &replicator_id)
    {
        consumed_replicator_[replicator_id] = true;
    }
    bool is_consumed_by_replicator(std::uint32_t &replicator_id) const
    {
        return consumed_replicator_[replicator_id];
    }
    // Return true if this entry fills the batch.
    bool receive_entry_no_lock(std::uint32_t &lsn)
    {
        lsn_ = lsn;
        std::uint64_t old_entry_count_active_writers = entry_count_active_writers_.fetch_add(0x100000001ULL, std::memory_order_acq_rel);
        std::uint32_t entry_count = old_entry_count_active_writers >> 32;
        if (entry_count == MAX_ENTRIES_PER_BATCH - 1)
        {
            state.store(BatchState::COLLECTING, std::memory_order_release);
            return true;
        }
        else
        {
            return false;
        }
    }
    void set_lsn(std::uint32_t &lsn)
    {
        lsn_ = lsn;
    }
    bool decrement_active_writers_no_lock()
    {
        std::uint64_t old_entry_count_active_writers = entry_count_active_writers_.fetch_sub(1ULL, std::memory_order_acq_rel);
        if (old_entry_count_active_writers == ((static_cast<std::uint64_t>(MAX_ENTRIES_PER_BATCH) << 32) | 1ULL))
        {
            state.store(BatchState::READY, std::memory_order_release);
            return true;
        }
        return false;
    }
    void record_new_state_key(const StateKey &state_key, const std::uint64_t &offset)
    {
        absl::MutexLock lock(&mu_);
        new_state_key_[state_key] = offset;
    }
    void increment_finished_replicators()
    {
        // Consumers include the committer and all replicators.
        if (finished_replicators_.fetch_add(1, std::memory_order_seq_cst) == REPLICATOR_NUM)
        {
            reset_batch();
        }
    }
    inline absl::flat_hash_map<StateKey, std::uint64_t /*offset*/> &get_new_state_key()
    {
        return new_state_key_;
    }
    std::uint64_t get_lsn() const
    {
        return lsn_;
    }
    void record_need_scaling(const StateKey &state_key, const std::uint32_t &offset)
    {
        absl::MutexLock lock(&need_scaling_mu_);
        need_scaling_by_state_[state_key] = offset;
    }
    inline bool is_need_scaling(const StateKey &state_key, std::uint32_t &offset)
    {
        if (need_scaling_by_state_.contains(state_key))
        {
            offset = need_scaling_by_state_[state_key];
            return true;
        }
        return false;
    }
    void reset_batch()
    {
        // Only the last consumer resets the batch.
        entry_count_active_writers_.store(0);
        finished_replicators_.store(0);
        state.store(BatchState::WAITING, std::memory_order_release);
        new_state_key_.clear();
        need_scaling_by_state_.clear();
        read_buffer_offset_ = 0;
        for (auto &[state_key, pending_area_state] : pending_area_by_state_)
        {
            pending_area_state_pool_->Return(pending_area_state);
        }
        pending_area_by_state_.clear();
    }

    Pending_area_state *reserve_offset(const StateKey &state_key, const std::uint32_t &payload_size, bool is_write, std::uint32_t &reserved_offset)
    {
        Pending_area_state *pending_area_state;
        {
            // The caller holds the shared lock.
            if (pending_area_by_state_.contains(state_key))
            {
                pending_area_state = pending_area_by_state_[state_key];
            }
            else
            {
                pending_area_state_pool_->Get(&pending_area_state);
                pending_area_by_state_[state_key] = pending_area_state;
            }
        }
        pending_area_state->write_buffer.reserve_offset(payload_size, reserved_offset);
        return pending_area_state;
    }
    void copy_to_write_buffer(const StateKey &state_key, std::uint32_t &offset, const std::uint32_t &lsn, const std::uint32_t &payload_size, const std::uint64_t &socket_fd)
    {
        // Look up the state key in the pending-area map.
        if (!pending_area_by_state_.contains(state_key))
        {
            LOG(ERROR) << "state_key " << state_key << " not found in pending_area_by_state_";
            exit(-1);
        }

        Pending_area_state *pending_area_state = pending_area_by_state_[state_key];
        if (pending_area_state == nullptr)
        {
            LOG(ERROR) << "pending_area_state is nullptr";
            exit(-1);
        }
        pending_area_state->write_buffer.copy_to_buffer(lsn, offset, socket_fd, payload_size);
    }
    void copy_to_read_buffer(const std::uint32_t &lsn, const std::uint32_t &payload_size, const std::uint64_t &socket_fd)
    {
        std::uint32_t reserved_offset;
        {
            absl::MutexLock lock(&read_buffer_mu_);
            reserved_offset = read_buffer_offset_;
            read_buffer_offset_ += sizeof(std::uint64_t) + payload_size;
        }
        std::uint64_t combined_lsn = (static_cast<std::uint64_t>(lsn) << 32) | static_cast<std::uint64_t>(payload_size);
        std::memcpy(read_buffer_ + reserved_offset, &combined_lsn, sizeof(std::uint64_t));
        bool eof = false;
        if (io_utils::RecvData(socket_fd, read_buffer_ + reserved_offset + sizeof(std::uint64_t), payload_size, &eof))
        {
        }
        else if (eof)
        {
            LOG(ERROR) << "Connection closed by peer, fd=" << socket_fd;
            exit(-1);
        }
        else
        {
            LOG(ERROR) << "Failed to read from fd " << socket_fd;
            exit(-1);
        }
    }
    void copy_to_read_buffer(const std::uint32_t &lsn, const std::uint32_t &payload_size, char *data)
    {
        std::uint32_t reserved_offset;
        {
            absl::MutexLock lock(&read_buffer_mu_);
            reserved_offset = read_buffer_offset_;
            read_buffer_offset_ += sizeof(std::uint64_t) + payload_size;
        }
        *reinterpret_cast<std::uint64_t *>(read_buffer_ + reserved_offset) = (static_cast<std::uint64_t>(lsn) << 32) | static_cast<std::uint64_t>(payload_size);
        std::memcpy(read_buffer_ + reserved_offset + sizeof(std::uint64_t), data, payload_size);
    }
#ifdef USE_RDMA
    std::uint32_t copy_to_cxl_read_buffer(const std::uint32_t &cxl_offset, uint64_t cxl_read_buffer_offset)
    {
        if (read_buffer_offset_ == 0)
        {
            return 0;
        }
        void *staging = g_rdma.get_staging_buf();
        std::memcpy(staging, read_buffer_, read_buffer_offset_);
        g_rdma.write(staging, cxl_read_buffer_offset + cxl_offset, read_buffer_offset_);
        // Validate: read back first entry header and verify lsn+payload_size
        // Read buffer entry format: [8 bytes: (lsn<<32)|payload_size] [payload_size bytes: data]
        if (read_buffer_offset_ >= sizeof(std::uint64_t))
        {
            std::uint64_t local_hdr;
            std::memcpy(&local_hdr, read_buffer_, sizeof(std::uint64_t));
            std::uint64_t remote_hdr;
            g_rdma.read(staging, cxl_read_buffer_offset + cxl_offset, sizeof(std::uint64_t));
            std::memcpy(&remote_hdr, staging, sizeof(std::uint64_t));
            CHECK(remote_hdr == local_hdr)
                << "[RDMA-CHECK] read_buffer entry header mismatch:"
                << " local_lsn=" << (local_hdr >> 32) << " local_psz=" << (local_hdr & 0xFFFFFFFF)
                << " remote_lsn=" << (remote_hdr >> 32) << " remote_psz=" << (remote_hdr & 0xFFFFFFFF)
                << " lsn=" << lsn_;
        }
        return read_buffer_offset_;
    }
#else
    std::uint32_t copy_to_cxl_read_buffer(const std::uint32_t &cxl_offset, char *cxl_read_buffer)
    {
        if (read_buffer_offset_ == 0)
        {
            return 0;
        }
        std::memcpy(cxl_read_buffer + cxl_offset, read_buffer_, read_buffer_offset_);
        clflushopt(cxl_read_buffer + cxl_offset, read_buffer_offset_);
        return read_buffer_offset_;
    }
#endif
    absl::flat_hash_map<StateKey, Pending_area_state *> *get_pending_area_by_state()
    {
        return &pending_area_by_state_;
    }
};
