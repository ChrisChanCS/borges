#pragma once

#include "../common/SharedData.h"
#include <memory>
#ifndef USE_RDMA
#include <cxlalloc.h>
#else
#include "rdma/RdmaRegion.h"
#endif
#include "SPMCBuffer.h"
#include "../common/Macro.h"

class Committer
{
private:
    // Consumer IDs 0 through REPLICATOR_NUM-1 belong to replicators; REPLICATOR_NUM belongs to the committer.
    std::uint32_t committer_id_{REPLICATOR_NUM};
    SPMCBuffer *batch_buffer_;

    absl::CondVar *cv_;
    absl::Mutex *cv_mu_;
    bool *task_ready_;
    bool stopping_ = false; // Guarded by the existing batch wakeup mutex.

    std::uint32_t shard_id_;

    class CommitManager
    {
    public:
#ifndef USE_RDMA
        SharedData::CommitRegion commit_region_;
        std::uint64_t write_position_ = 0;
        std::uint64_t cached_reclaimed_ = 0;
        std::uint32_t last_lsn_ = 0;
        // Allocated at startup; only a batch straddling the end uses it.
        const std::size_t wrap_slots_ = 3 * MAX_ENTRIES_PER_BATCH;
        std::unique_ptr<std::uint64_t[]> wrap_buffer_{
            new std::uint64_t[wrap_slots_] {}};
#else
        uint64_t commit_buffer_offset_;
        uint64_t commit_metadata_offset_;
        std::vector<std::uint64_t> local_commit_buf_;  // local staging for commit entries
#endif
        std::uint32_t local_commit_buffer_key_num_;
        CommitManager(std::uint32_t committer_id, std::uint32_t shard_id)
        {
            local_commit_buffer_key_num_ = 0;
#ifdef USE_RDMA
            commit_buffer_offset_ = g_rdma.get_root(COMMIT_RING_BUFFER_ROOT_INDEX + shard_id);
            commit_metadata_offset_ = g_rdma.get_root(COMMIT_BUFFER_METADATA_ROOT_INDEX + shard_id);
            local_commit_buf_.resize(COMMIT_RING_BUFFER_CAPACITY);
#else
            commit_region_.commit_buffer = SharedData::shared_pointer<std::uint64_t>(SharedData::shard_resources(shard_id).commit_buffer);
            commit_region_.commit_buffer_metadata = SharedData::shared_pointer<SharedData::CommitBufferMetadata>(SharedData::shard_resources(shard_id).commit_metadata);
            if (SharedData::recovered_shard)
            {
                local_commit_buffer_key_num_ = static_cast<std::uint32_t>(SharedData::recovered_shard->progress);
                last_lsn_ = SharedData::recovered_shard->progress >> 32;
                write_position_ = SharedData::DeltaRing::restore_position(
                    SharedData::read_cxl_word(commit_region_.commit_buffer_metadata->write_position),
                    local_commit_buffer_key_num_);
                cached_reclaimed_ = SharedData::read_cxl_word(commit_region_.commit_buffer_metadata->reclaimed);
            }
            else
                commit_region_.commit_buffer_metadata->key_num.store(0, std::memory_order_relaxed);
#endif
        }
#ifndef USE_RDMA
        void publish()
        {
            auto *metadata = commit_region_.commit_buffer_metadata;
            metadata->write_position.store(write_position_, std::memory_order_relaxed);
            metadata->key_num.store(local_commit_buffer_key_num_, std::memory_order_release);
            clwb(&metadata->key_num, sizeof(metadata->key_num));
            sfence();
        }

        [[gnu::noinline]] void wait_for_space(std::uint64_t count)
        {
            CHECK_LE(count, SharedData::DeltaRing::capacity - SharedData::DeltaRing::entries_per_line);
            auto *metadata = commit_region_.commit_buffer_metadata;
            cached_reclaimed_ = SharedData::read_cxl_word(metadata->reclaimed);
            if (SharedData::DeltaRing::fits(write_position_, cached_reclaimed_, count))
                return;
            // Publish already flushed batches before waiting. Replicator cuts
            // can include later batches whose deltas do not fit in the ring.
            metadata->blocked_cut.store((std::uint64_t(last_lsn_) << 32) |
                local_commit_buffer_key_num_, std::memory_order_release);
            publish();
            do
            {
                SharedData::writer_backoff();
                cached_reclaimed_ = SharedData::read_cxl_word(metadata->reclaimed);
            } while (!SharedData::DeltaRing::fits(write_position_, cached_reclaimed_, count));
            // The next ordinary producer publication flushes this same line.
            metadata->blocked_cut.store(0, std::memory_order_relaxed);
        }

        std::uint64_t *prepare(std::uint64_t upper_bound)
        {
            if (!SharedData::DeltaRing::fits(write_position_, cached_reclaimed_, upper_bound)) [[unlikely]]
                wait_for_space(upper_bound);
            const auto offset = SharedData::DeltaRing::offset(write_position_);
            if (offset + upper_bound > SharedData::DeltaRing::capacity) [[unlikely]]
            {
                CHECK_LE(upper_bound, wrap_slots_);
                return wrap_buffer_.get();
            }
            return commit_region_.commit_buffer + offset;
        }

        void finish(std::uint64_t *begin, std::uint64_t *end, std::uint32_t lsn)
        {
            const auto count = static_cast<std::uint64_t>(end - begin);
            if (begin == wrap_buffer_.get()) [[unlikely]]
            {
                SharedData::DeltaRing::copy_in(commit_region_.commit_buffer, write_position_, count, begin);
                SharedData::DeltaRing::flush(commit_region_.commit_buffer, write_position_, count);
            }
            else if (count)
                clflushopt(begin, count * sizeof(*begin));
            write_position_ += count;
            local_commit_buffer_key_num_ = static_cast<std::uint32_t>(write_position_);
            last_lsn_ = lsn;
        }
#endif
    };

    CommitManager commit_manager_;
    std::vector<std::uint64_t> key_tail_; // Batch deltas: upper 32 bits hold the state key; lower 32 hold pending write bytes.
    std::thread worker_thread_;           // Worker thread

    // Worker thread's main loop.
    void run()
    {
        init_shard_cxlalloc(REPLICATOR_NUM + 1, shard_id_);
        while (true)
        {
            {
                absl::MutexLock lock(cv_mu_);
                while (!*task_ready_)
                {
                    cv_->Wait(cv_mu_);
                }
                if (stopping_)
                    return;
                *task_ready_ = false;
            }
            if (WORKLOAD == 4 || (WORKLOAD == 1 && YCSB_OPTION == 3) || (WORKLOAD == 0 && SCALE_OUT_STREAM))
            {
                commit_with_new_state_key();
            }
            else
            {
                commit_directly();
            }
        }
    }

public:
    Committer(SPMCBuffer *batch_buffer, absl::CondVar *cv, absl::Mutex *cv_mu, bool *task_ready, std::uint32_t shard_id)
        : batch_buffer_(batch_buffer), cv_(cv), cv_mu_(cv_mu), task_ready_(task_ready), shard_id_(shard_id), commit_manager_(REPLICATOR_NUM, shard_id)
    {
        key_tail_.reserve(REPORT_RESERVED_SIZE);
        worker_thread_ = std::thread(&Committer::run, this);
    }

    ~Committer()
    {
        {
            absl::MutexLock lock(cv_mu_);
            stopping_ = true;
            *task_ready_ = true;
            cv_->SignalAll();
        }
        if (worker_thread_.joinable())
            worker_thread_.join();
    }

    void commit_directly()
    {
        std::shared_ptr<Batch> batch;
        std::uint32_t start_key_num = commit_manager_.local_commit_buffer_key_num_;
        while (batch_buffer_->can_consume(committer_id_))
        {
            batch = batch_buffer_->get_consumer_batch_with_state(committer_id_);
            while (batch == nullptr)
            {
                std::this_thread::yield();
                batch = batch_buffer_->get_consumer_batch_with_state(committer_id_);
            }
            absl::flat_hash_map<StateKey, Pending_area_state *> *pending_area_by_state = batch->get_pending_area_by_state();
            std::uint32_t last_key_num = commit_manager_.local_commit_buffer_key_num_;
#ifndef USE_RDMA
            auto *begin = commit_manager_.prepare(pending_area_by_state->size());
            auto *destination = begin;
#endif
            for (auto &[state_key, pending_area_state] : *pending_area_by_state)
            {
                if (pending_area_state->write_buffer.tail_offset > 0)
                {
                    // This state key has a write entry in the batch.
#ifdef USE_RDMA
                    commit_manager_.local_commit_buf_[commit_manager_.local_commit_buffer_key_num_] = (static_cast<std::uint64_t>(state_key) << 32) | pending_area_state->write_buffer.tail_offset;
#else
                    *destination++ = (static_cast<std::uint64_t>(state_key) << 32) | pending_area_state->write_buffer.tail_offset;
#endif
#ifdef USE_RDMA
                    commit_manager_.local_commit_buffer_key_num_++;
#endif
                }
            }
#ifndef USE_RDMA
            commit_manager_.finish(begin, destination, batch->get_lsn());
#endif
            LOG_CLASS("Committer {}", committer_id_, "write index update, request id: {}", batch->get_lsn());
            batch->increment_finished_replicators();
            batch_buffer_->advance_consumer(committer_id_);
        }
#ifdef USE_RDMA
        // RDMA WRITE only the newly appended entries (delta), not the full buffer
        if (start_key_num < commit_manager_.local_commit_buffer_key_num_)
        {
            void* staging = g_rdma.get_staging_buf();
            size_t start_offset = static_cast<size_t>(start_key_num) * sizeof(std::uint64_t);
            size_t write_bytes = (commit_manager_.local_commit_buffer_key_num_ - start_key_num) * sizeof(std::uint64_t);
            size_t offset = start_offset;
            char* src = reinterpret_cast<char*>(commit_manager_.local_commit_buf_.data()) + start_offset;
            while (write_bytes > 0) {
                size_t chunk = std::min(write_bytes, (size_t)4096);
                memcpy(staging, src, chunk);
                g_rdma.write(staging, commit_manager_.commit_buffer_offset_ + offset, chunk);
                src += chunk; offset += chunk; write_bytes -= chunk;
            }
            // Write metadata (key_num)
            uint32_t key_num_val = commit_manager_.local_commit_buffer_key_num_;
            memcpy(staging, &key_num_val, sizeof(uint32_t));
            g_rdma.write(staging, commit_manager_.commit_metadata_offset_, sizeof(uint32_t));
            // Validate: read back metadata and first/last written entry
            {
                uint32_t meta_rb;
                g_rdma.read(staging, commit_manager_.commit_metadata_offset_, sizeof(uint32_t));
                std::memcpy(&meta_rb, staging, sizeof(uint32_t));
                CHECK(meta_rb == key_num_val)
                    << "[RDMA-CHECK] commit metadata readback mismatch: wrote=" << key_num_val
                    << " read=" << meta_rb << " shard=" << shard_id_;
                // Verify the first new entry was written correctly
                uint64_t entry_rb;
                g_rdma.read(staging, commit_manager_.commit_buffer_offset_ + start_offset, sizeof(uint64_t));
                std::memcpy(&entry_rb, staging, sizeof(uint64_t));
                uint64_t expected = commit_manager_.local_commit_buf_[start_key_num];
                CHECK(entry_rb == expected)
                    << "[RDMA-CHECK] commit_buf[" << start_key_num << "] readback mismatch: wrote=0x"
                    << std::hex << expected << " read=0x" << entry_rb << std::dec
                    << " shard=" << shard_id_;
            }
        }
#else
        commit_manager_.publish();
#endif
    }

    void commit_with_new_state_key()
    {
        std::shared_ptr<Batch> batch;
        std::uint32_t start_key_num = commit_manager_.local_commit_buffer_key_num_;
        while (batch_buffer_->can_consume(committer_id_))
        {
            batch = batch_buffer_->get_consumer_batch_with_state(committer_id_);
            while (batch == nullptr)
            {
                std::this_thread::yield();
                batch = batch_buffer_->get_consumer_batch_with_state(committer_id_);
            }
            // Publish new keys first so the sequencer allocates their storage before processing batch deltas.
            std::uint32_t last_key_num = commit_manager_.local_commit_buffer_key_num_;
            absl::Time start_time = absl::Now();
            auto new_state_key = batch->get_new_state_key();
#ifndef USE_RDMA
            auto *begin = commit_manager_.prepare(
                batch->get_pending_area_by_state()->size() + 2 * new_state_key.size());
            auto *destination = begin;
#endif
            for (auto &[state_key, offset] : new_state_key)
            {
#ifdef USE_RDMA
                commit_manager_.local_commit_buf_[commit_manager_.local_commit_buffer_key_num_] = (static_cast<std::uint64_t>(state_key) << 32) | 0x00000000;
                commit_manager_.local_commit_buf_[commit_manager_.local_commit_buffer_key_num_ + 1] = offset;
#else
                *destination++ = (static_cast<std::uint64_t>(state_key) << 32) | 0xFFFFFFFF;
                *destination++ = offset;
#endif
#ifdef USE_RDMA
                commit_manager_.local_commit_buffer_key_num_ += 2;
#endif
            }
            absl::Duration duration = absl::Now() - start_time;
            LOG_CLASS("Committer", std::to_string(committer_id_), "commit new state key latency {}", absl::ToInt64Microseconds(duration));

            start_time = absl::Now();
            absl::flat_hash_map<StateKey, Pending_area_state *> *pending_area_by_state = batch->get_pending_area_by_state();
            for (auto &[state_key, pending_area_state] : *pending_area_by_state)
            {
                if (pending_area_state->write_buffer.tail_offset > 0)
                {
#ifdef USE_RDMA
                    commit_manager_.local_commit_buf_[commit_manager_.local_commit_buffer_key_num_] = (static_cast<std::uint64_t>(state_key) << 32) | pending_area_state->write_buffer.tail_offset;
#else
                    *destination++ = (static_cast<std::uint64_t>(state_key) << 32) | pending_area_state->write_buffer.tail_offset;
#endif
#ifdef USE_RDMA
                    commit_manager_.local_commit_buffer_key_num_++;
#endif
                }
            }
#ifdef USE_RDMA
            if (commit_manager_.local_commit_buffer_key_num_ * sizeof(std::uint64_t) >= COMMIT_RING_BUFFER_CAPACITY)
            {
                LOG(ERROR) << "COMMIT_RING_BUFFER_CAPACITY is not enough";
                exit(-1);
            }
#endif
#ifndef USE_RDMA
            commit_manager_.finish(begin, destination, batch->get_lsn());
#endif
            batch->increment_finished_replicators();
            batch_buffer_->advance_consumer(committer_id_);
        }
#ifdef USE_RDMA
        // RDMA WRITE only the newly appended entries (delta), not the full buffer
        if (start_key_num < commit_manager_.local_commit_buffer_key_num_)
        {
            void* staging = g_rdma.get_staging_buf();
            size_t start_offset = static_cast<size_t>(start_key_num) * sizeof(std::uint64_t);
            size_t write_bytes = (commit_manager_.local_commit_buffer_key_num_ - start_key_num) * sizeof(std::uint64_t);
            size_t offset = start_offset;
            char* src = reinterpret_cast<char*>(commit_manager_.local_commit_buf_.data()) + start_offset;
            while (write_bytes > 0) {
                size_t chunk = std::min(write_bytes, (size_t)4096);
                memcpy(staging, src, chunk);
                g_rdma.write(staging, commit_manager_.commit_buffer_offset_ + offset, chunk);
                src += chunk; offset += chunk; write_bytes -= chunk;
            }
            // Write metadata (key_num)
            uint32_t key_num_val = commit_manager_.local_commit_buffer_key_num_;
            memcpy(staging, &key_num_val, sizeof(uint32_t));
            g_rdma.write(staging, commit_manager_.commit_metadata_offset_, sizeof(uint32_t));
            // Validate: read back metadata and first written entry
            {
                uint32_t meta_rb;
                g_rdma.read(staging, commit_manager_.commit_metadata_offset_, sizeof(uint32_t));
                std::memcpy(&meta_rb, staging, sizeof(uint32_t));
                CHECK(meta_rb == key_num_val)
                    << "[RDMA-CHECK] commit_new_key metadata readback: wrote=" << key_num_val
                    << " read=" << meta_rb << " shard=" << shard_id_;
                uint64_t entry_rb;
                g_rdma.read(staging, commit_manager_.commit_buffer_offset_ + start_offset, sizeof(uint64_t));
                std::memcpy(&entry_rb, staging, sizeof(uint64_t));
                uint64_t expected = commit_manager_.local_commit_buf_[start_key_num];
                CHECK(entry_rb == expected)
                    << "[RDMA-CHECK] commit_buf_new_key[" << start_key_num << "] readback: wrote=0x"
                    << std::hex << expected << " read=0x" << entry_rb << std::dec
                    << " shard=" << shard_id_;
            }
        }
#else
        commit_manager_.publish();
#endif
    }
};
