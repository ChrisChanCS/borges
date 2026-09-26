#pragma once
#include "absl/log/check.h"
#include "absl/synchronization/mutex.h"
#include "absl/container/flat_hash_map.h"
#include "absl/time/time.h"
#include <thread>
#include <atomic>
#include <vector>
#include "Stream.h"
#include "Message.h"
#include <coroutine>
#include <condition_variable>
#include <mutex>
#include <cstring>
#include "Reader.h"
#include "../common/Macro.h"
#include "SPMCBuffer.h"

static int batch_dequeue_count = 0;

// Replicator states.
enum class ReplicatorState
{
    IDLE,    // Idle
    RUNNING, // Running
    STOPPED  // Stopped
};

// One shard replica writer.
class Replicator
{
private:
    std::uint32_t shard_id_;
    std::uint32_t replicator_id_; // Unique replicator ID.
    std::thread worker_thread_;          // Worker thread
    std::atomic<ReplicatorState> state_; // Current state.
#ifdef USE_RDMA
    uint64_t cxl_region_write_;
    uint64_t cxl_region_read_;
#else
    char *cxl_region_write_;
    char *cxl_region_read_;
    SharedData::StreamCatalogEntry *catalog_ = nullptr;
    std::size_t catalog_cursor_ = 0;
#endif
    std::uint32_t cxl_region_read_offset_;
    std::uint32_t cxl_region_read_max_offset_;
    SPMCBuffer *batch_buffer_;

    absl::CondVar *cv_;
    absl::Mutex *cv_mu_;
    bool *task_ready_;

    absl::Mutex map_mu_;
    absl::flat_hash_map<StateKey, Stream *> stream_by_state_key_ ABSL_GUARDED_BY(map_mu_);
    absl::flat_hash_map<StateKey, StreamWithScaling *> stream_with_scaling_by_state_key_ ABSL_GUARDED_BY(map_mu_);
    Message::Report report_;
    bool dirty_ = false;

    void send_report()
    {
        if (dirty_)
        {
            report_.send_report();
            dirty_ = false;
        }
    }
    // Worker thread's main loop.
    void run()
    {
        init_shard_cxlalloc(replicator_id_, shard_id_);
#ifndef USE_RDMA
        if (!(SharedData::startup_configuration.header.replica_mask & (1u << replicator_id_)))
        {
            drain_failed_replica();
            return;
        }
#endif
        // Initialize this replicator's stream map.
        // 1. Locate this replicator's CXL region.
#ifdef USE_RDMA
        cxl_region_write_ = g_rdma.get_root(STREAM_ROOT_INDEX + shard_id_ * REPLICATOR_NUM + replicator_id_);
#else
        cxl_region_write_ = SharedData::write_stream(shard_id_, replicator_id_);
        const auto &recovered = replicator_id_ == 0 ?
            SharedData::recovered_shard : SharedData::recovered_backup;
        if (replicator_id_ == 1)
        {
            catalog_ = SharedData::shared_pointer<SharedData::StreamCatalogEntry>(
                SharedData::shard_resources(shard_id_).stream_catalog);
            if (recovered)
                catalog_cursor_ = recovered->catalog_cursor;
        }
#endif
        // 2. Initialize its read and write streams.
#ifndef USE_RDMA
        if (recovered)
        {
            for (const auto &[key, stream] : recovered->streams)
                if (stream.initial_segments != 0)
                    stream_with_scaling_by_state_key_[key] = new StreamWithScaling(cxl_region_write_, stream);
        }
        else
#endif
        if (WORKLOAD == 1 || WORKLOAD == 2 || WORKLOAD == 3)
        {
            init_ycsb_segment();
        }
        else if (WORKLOAD == 4)
        {
            init_retwis_segment();
        }
        else if (WORKLOAD == 0)
        {
            init_append_only_segment();
        }
        else
        {
            LOG(ERROR) << "Invalid workload type: " << WORKLOAD;
            exit(-1);
        }
#ifdef USE_RDMA
        cxl_region_read_ = g_rdma.get_root(READ_STREAM_ROOT_INDEX + shard_id_ * REPLICATOR_NUM + replicator_id_);
#else
        cxl_region_read_ = SharedData::shared_pointer<char>(SharedData::shard_resources(shard_id_).read_stream[replicator_id_]);
#endif
        cxl_region_read_max_offset_ = READ_STREAM_SIZE;
        cxl_region_read_offset_ = 0;
#ifndef USE_RDMA
        if (recovered)
            cxl_region_read_offset_ = recovered->read_tail;
#endif
        auto last_report_time = absl::Now();

        while (true)
        {
            {
                absl::MutexLock lock(cv_mu_);
                while (!*task_ready_)
                {
                    cv_->Wait(cv_mu_);
                }
                if (state_.load() == ReplicatorState::STOPPED)
                    return;
                *task_ready_ = false;
            }
            VLOG(1) << "replicator " << replicator_id_ << " (shard " << shard_id_ << ") woke up";
            if (WORKLOAD == 4 || (WORKLOAD == 1 && YCSB_OPTION == 3) || (WORKLOAD == 0 && SCALE_OUT_STREAM))
            {
                replicate_batch_buffer_with_scaling_and_new_key();
            }
            else
            {
                replicate_batch_buffer_with_scaling();
            }
            send_report();
            VLOG(1) << "replicator " << replicator_id_ << " (shard " << shard_id_ << ") sent report, lsn=" << report_.get_lsn();
        }
    }

#ifndef USE_RDMA
    void drain_failed_replica()
    {
        // The failed replica's local cut is an immutable maximum sentinel.
        // Keep the existing SPMC consumer count, without touching that device.
        while (true)
        {
            {
                absl::MutexLock lock(cv_mu_);
                while (!*task_ready_)
                    cv_->Wait(cv_mu_);
                if (state_.load() == ReplicatorState::STOPPED)
                    return;
                *task_ready_ = false;
            }
            while (batch_buffer_->can_consume(replicator_id_))
            {
                auto batch = batch_buffer_->get_consumer_batch_with_state(replicator_id_);
                if (!batch)
                {
                    std::this_thread::yield();
                    continue;
                }
                batch->increment_finished_replicators();
                batch_buffer_->advance_consumer(replicator_id_);
            }
        }
    }
#endif

    void init_ycsb_segment()
    {
        if (WORKLOAD == 1 && YCSB_OPTION == 3)
        {
            return;
        }
        if (std::uint64_t(HOTTER_SEG_NUM) * SEGMENT_SIZE * YCSB_HOTTER_KEY_CNT + std::uint64_t(HOT_SEG_NUM) * SEGMENT_SIZE * YCSB_HOT_KEY_CNT + std::uint64_t(COLD_SEG_NUM) * SEGMENT_SIZE * (KEY_CNT_OF_SHARD - YCSB_HOT_KEY_CNT - YCSB_HOTTER_KEY_CNT) > WRITE_STREAM_SIZE)
        {
            LOG(ERROR) << "not enough cxl region for all state keys";
            exit(-1);
        }
        for (std::uint32_t i = 0; i < KEY_CNT_OF_SHARD; i++)
        {
            if (i < YCSB_HOTTER_KEY_CNT)
            {
                std::uint64_t write_stream_offset = std::uint64_t(i) * HOTTER_SEG_NUM * SEGMENT_SIZE;
                stream_with_scaling_by_state_key_[i] = new StreamWithScaling(cxl_region_write_ + write_stream_offset, HOTTER_SEG_NUM);
            }
            else if (i < YCSB_HOT_KEY_CNT + YCSB_HOTTER_KEY_CNT)
            {
                std::uint64_t write_stream_offset = std::uint64_t(i - YCSB_HOTTER_KEY_CNT) * HOT_SEG_NUM * SEGMENT_SIZE + std::uint64_t(YCSB_HOTTER_KEY_CNT) * HOTTER_SEG_NUM * SEGMENT_SIZE;
                stream_with_scaling_by_state_key_[i] = new StreamWithScaling(cxl_region_write_ + write_stream_offset, HOT_SEG_NUM);
            }
            else
            {
                std::uint64_t write_stream_offset = std::uint64_t(i - YCSB_HOT_KEY_CNT - YCSB_HOTTER_KEY_CNT) * COLD_SEG_NUM * SEGMENT_SIZE + std::uint64_t(YCSB_HOT_KEY_CNT) * HOT_SEG_NUM * SEGMENT_SIZE + std::uint64_t(YCSB_HOTTER_KEY_CNT) * HOTTER_SEG_NUM * SEGMENT_SIZE;
                stream_with_scaling_by_state_key_[i] = new StreamWithScaling(cxl_region_write_ + write_stream_offset, COLD_SEG_NUM);
            }
        }
    }

    void init_retwis_segment()
    {
        if (std::uint64_t(USER_CNT) * RETWIS_SEGMENT_SIZE * TIMELINE_SEG_NUM > WRITE_STREAM_SIZE)
        {
            LOG(ERROR) << "not enough cxl region for all state keys";
            exit(-1);
        }
        // Only timelines need initialized streams.
        for (std::uint32_t i = 0; i < USER_CNT; i++)
        {
            std::uint64_t write_stream_offset = std::uint64_t(i) * RETWIS_SEGMENT_SIZE * TIMELINE_SEG_NUM;
            stream_with_scaling_by_state_key_[i + USER_CNT] = new StreamWithScaling(cxl_region_write_ + write_stream_offset, TIMELINE_SEG_NUM);
        }
    }

    void init_append_only_segment()
    {
        // Only one key
        if (SCALE_OUT_STREAM)
        {
            stream_with_scaling_by_state_key_[0] = new StreamWithScaling(cxl_region_write_, 1024 * 100);
        }
        else
        {
            stream_with_scaling_by_state_key_[0] = new StreamWithScaling(cxl_region_write_, 1024 * 500);
        }
    }

public:
    Replicator(std::uint32_t shard_id, std::uint32_t replicator_id, SPMCBuffer *batch_buffer, absl::CondVar *cv,
               absl::Mutex *cv_mu, bool *task_ready)
        : shard_id_(shard_id),
          replicator_id_(replicator_id),
          state_(ReplicatorState::IDLE),
          batch_buffer_(batch_buffer),
          cv_(cv),
          cv_mu_(cv_mu),
          task_ready_(task_ready),
          report_(shard_id_, replicator_id_)
    {
        // Start the worker thread.
        worker_thread_ = std::thread(&Replicator::run, this);
    }

    ~Replicator()
    {
        stop();
        if (worker_thread_.joinable())
        {
            worker_thread_.join();
        }
    }

    // Start replication.
    void start()
    {
        state_.store(ReplicatorState::RUNNING);
    }

    // Stop replication.
    void stop()
    {
        absl::MutexLock lock(cv_mu_);
        state_.store(ReplicatorState::STOPPED);
        *task_ready_ = true;
        cv_->SignalAll();
    }

    // Pause replication.
    void pause()
    {
        state_.store(ReplicatorState::IDLE);
    }

    // Return the replicator ID.
    uint32_t get_id() const
    {
        return replicator_id_;
    }

    Stream *get_stream(const StateKey &state_key)
    {
        absl::MutexLock lock(&map_mu_);
        if (!stream_by_state_key_.contains(state_key))
        {
            return nullptr;
        }
        return stream_by_state_key_[state_key];
    }

    StreamWithScaling *get_stream_with_scaling(const StateKey &state_key)
    {
        absl::MutexLock lock(&map_mu_);
        if (!stream_with_scaling_by_state_key_.contains(state_key))
        {
            return nullptr;
        }
        return stream_with_scaling_by_state_key_[state_key];
    }

    template <bool IsHead = false>
    void scale_segment(std::uint32_t &segment_index, StateKey &state_key)
    {
#ifndef USE_RDMA
        if (!(SharedData::startup_configuration.header.replica_mask & (1u << replicator_id_)))
            return;
#endif
        std::uint32_t segment_size;
        if (WORKLOAD == 4)
        {
            segment_size = RETWIS_SEGMENT_SIZE;
        }
        else
        {
            if (YCSB_OPTION == 3 && WORKLOAD == 1)
            {
                segment_size = YCSB_NEW_KEY_SEGMENT_SIZE;
            }
            else
            {
                segment_size = SEGMENT_SIZE;
            }
        }
#ifdef USE_RDMA
        uint64_t cxl_begin_offset = cxl_region_write_ + std::uint64_t(segment_index) * segment_size;
#else
        char *cxl_begin_ptr = cxl_region_write_ + std::uint64_t(segment_index) * segment_size;
#endif
        StreamWithScaling *stream = nullptr;

        // Look up the state key in the growing-stream map.
        {
            absl::MutexLock lock(&map_mu_);
            if (!stream_with_scaling_by_state_key_.contains(state_key))
            {
                std::uint64_t write_stream_offset = std::uint64_t(segment_index) * segment_size;
                stream_with_scaling_by_state_key_[state_key] = new StreamWithScaling(cxl_region_write_ + write_stream_offset, 1);
                // An expansion may arrive before the head. Its registration
                // stays provisional until the head's batch becomes READY.
                return;
            }
            stream = stream_with_scaling_by_state_key_[state_key];
        }
        if (stream != nullptr)
        {
#ifdef USE_RDMA
            stream->scale_segment<IsHead>(cxl_begin_offset);
#else
            stream->scale_segment<IsHead>(cxl_begin_ptr);
#endif
        }
        else
        {
            LOG(ERROR) << "stream is nullptr, state_key: " << state_key << " during scale_segment";
            exit(-1);
        }
    }

    void replicate_batch_buffer_with_scaling()
    {
        std::shared_ptr<Batch> batch;
        while (batch_buffer_->can_consume(replicator_id_))
        {
            batch = batch_buffer_->get_consumer_batch_with_state(replicator_id_);
            while (batch == nullptr)
            {
                // Back off and retry when no batch is available.
                std::this_thread::yield();
                batch = batch_buffer_->get_consumer_batch_with_state(replicator_id_);
            }
            dirty_ = true;
            std::uint32_t key_num = 0;

            // Copywrite entry
            absl::flat_hash_map<StateKey, Pending_area_state *> *pending_area_by_state = batch->get_pending_area_by_state();
            for (auto &[state_key, pending_area_state] : *pending_area_by_state)
            {
                if (pending_area_state->write_buffer.tail_offset > 0)
                {
                    key_num++;
                }
                CHECK(pending_area_state->write_buffer.tail_offset > 0);

                // Look up the state key in the growing-stream map.
                if (!stream_with_scaling_by_state_key_.contains(state_key))
                {
                    LOG(ERROR) << "state_key " << state_key << " not found in stream_with_scaling_by_state_key_";
                    exit(-1);
                }

                StreamWithScaling *stream = stream_with_scaling_by_state_key_[state_key];

                // Check whether the stream exists.
                if (stream == nullptr)
                {
                    LOG(ERROR) << "stream is nullptr, state_key: " << state_key;
                    exit(-1);
                }
                // available_offset is the writable space remaining in the current segment.
                std::uint32_t availabale_offset;
                bool is_scale = batch->is_need_scaling(state_key, availabale_offset);
                stream->copy_to_cxl(pending_area_state->write_buffer.tail_offset, pending_area_state->write_buffer.buffer, availabale_offset, is_scale);
            }
#ifdef USE_RDMA
            // Validate: for each state_key, read back the entry header from the commit buffer
            // and verify the tail_offset matches what was written to the stream.
            // The pending_buffer for each state_key has format:
            //   [(lsn<<32)|payload_size][data][(lsn<<32)|payload_size][data]...
            // Verify the FIRST entry header matches batch->get_lsn()
            for (auto &[state_key, pending_area_state] : *pending_area_by_state)
            {
                if (pending_area_state->write_buffer.tail_offset >= sizeof(std::uint64_t))
                {
                    std::uint64_t local_first_entry;
                    std::memcpy(&local_first_entry, pending_area_state->write_buffer.buffer, sizeof(std::uint64_t));
                    std::uint32_t entry_lsn = local_first_entry >> 32;
                    std::uint32_t entry_psz = local_first_entry & 0xFFFFFFFF;
                    CHECK(entry_lsn == batch->get_lsn())
                        << "[RDMA-CHECK] replicator " << replicator_id_
                        << " entry lsn mismatch: entry_lsn=" << entry_lsn
                        << " batch_lsn=" << batch->get_lsn()
                        << " state_key=" << state_key
                        << " payload_size=" << entry_psz
                        << " tail_offset=" << pending_area_state->write_buffer.tail_offset;
                    CHECK(entry_psz > 0 && entry_psz <= ENTRY_MAX_SIZE)
                        << "[RDMA-CHECK] replicator " << replicator_id_
                        << " invalid payload_size=" << entry_psz
                        << " for state_key=" << state_key << " lsn=" << entry_lsn;
                }
            }
#endif

            // Copyread entry
            std::uint32_t offset = batch->copy_to_cxl_read_buffer(cxl_region_read_offset_, cxl_region_read_);
            cxl_region_read_offset_ += offset;
            if (cxl_region_read_offset_ > cxl_region_read_max_offset_)
            {
                LOG(ERROR) << "cxl_region_read_ overflow";
                exit(-1);
            }
            // logAppend log
            LOG_CLASS("replicator {}", replicator_id_, "copy_to_cxl_buffer, request id: {}", batch->get_lsn());

            // Update the report and determine whether the batch can be reset.
            {
                report_.set_lsn(batch->get_lsn());
                report_.increment_commit_buffer_key_num(key_num);
                batch->increment_finished_replicators();
            }
            batch_buffer_->advance_consumer(replicator_id_);
        }
    }

    void replicate_batch_buffer_with_scaling_and_new_key()
    {
        std::shared_ptr<Batch> batch;
        while (batch_buffer_->can_consume(replicator_id_))
        {
            batch = batch_buffer_->get_consumer_batch_with_state(replicator_id_);
            while (batch == nullptr)
            {
                // Back off and retry when no batch is available.
                std::this_thread::yield();
                batch = batch_buffer_->get_consumer_batch_with_state(replicator_id_);
            }
            dirty_ = true;
            std::uint32_t key_num = 0;

            auto new_state_key = batch->get_new_state_key();
#ifndef USE_RDMA
            const auto catalog_begin = catalog_cursor_;
#endif
            for (auto &[state_key, offset] : new_state_key)
            {
                key_num += 2;
#ifndef USE_RDMA
                if (catalog_)
                    SharedData::append_stream_catalog(catalog_, catalog_cursor_, state_key, offset, batch->get_lsn());
#endif
            }
#ifndef USE_RDMA
            if (catalog_cursor_ != catalog_begin)
                clflushopt(catalog_ + catalog_begin,
                    (catalog_cursor_ - catalog_begin) * sizeof(SharedData::StreamCatalogEntry));
#endif

            // Copywrite entry
            absl::flat_hash_map<StateKey, Pending_area_state *> *pending_area_by_state = batch->get_pending_area_by_state();
            for (auto &[state_key, pending_area_state] : *pending_area_by_state)
            {
                if (pending_area_state->write_buffer.tail_offset > 0)
                {
                    key_num++;
                }
                CHECK(pending_area_state->write_buffer.tail_offset > 0);

                StreamWithScaling *stream;
                {
                    absl::MutexLock lock(&map_mu_);
                    if (!stream_with_scaling_by_state_key_.contains(state_key))
                    {
                        // The replicator has allocated the new state key's segments by this point.
                        LOG(ERROR) << "replicator " << replicator_id_ << " state_key " << state_key << " not found in stream_with_scaling_by_state_key_";
                        exit(-1);
                    }
                    stream = stream_with_scaling_by_state_key_[state_key];
                }

                // Check whether the stream exists.
                if (stream == nullptr)
                {
                    LOG(ERROR) << "stream is nullptr, state_key: " << state_key;
                    exit(-1);
                }
                // available_offset is the writable space remaining in the current segment.
                std::uint32_t availabale_offset;
                bool is_scale = batch->is_need_scaling(state_key, availabale_offset);
                stream->copy_to_cxl(pending_area_state->write_buffer.tail_offset, pending_area_state->write_buffer.buffer, availabale_offset, is_scale);
            }
#ifdef USE_RDMA
            // Validate entry headers for new-key path
            for (auto &[state_key, pending_area_state] : *pending_area_by_state)
            {
                if (pending_area_state->write_buffer.tail_offset >= sizeof(std::uint64_t))
                {
                    std::uint64_t local_first_entry;
                    std::memcpy(&local_first_entry, pending_area_state->write_buffer.buffer, sizeof(std::uint64_t));
                    std::uint32_t entry_lsn = local_first_entry >> 32;
                    std::uint32_t entry_psz = local_first_entry & 0xFFFFFFFF;
                    CHECK(entry_lsn == batch->get_lsn())
                        << "[RDMA-CHECK] replicator_new_key " << replicator_id_
                        << " entry lsn=" << entry_lsn << " != batch_lsn=" << batch->get_lsn()
                        << " state_key=" << state_key;
                    CHECK(entry_psz > 0 && entry_psz <= ENTRY_MAX_SIZE)
                        << "[RDMA-CHECK] replicator_new_key " << replicator_id_
                        << " invalid payload_size=" << entry_psz
                        << " state_key=" << state_key << " lsn=" << entry_lsn;
                }
            }
#endif

            // Copyread entry
            std::uint32_t offset = batch->copy_to_cxl_read_buffer(cxl_region_read_offset_, cxl_region_read_);
            cxl_region_read_offset_ += offset;
            if (cxl_region_read_offset_ > cxl_region_read_max_offset_)
            {
                LOG(ERROR) << "cxl_region_read_ overflow";
                exit(-1);
            }

            // Update the report and determine whether the batch can be reset.
            {
                report_.set_lsn(batch->get_lsn());
                report_.increment_commit_buffer_key_num(key_num);
                batch->increment_finished_replicators();
            }
            batch_buffer_->advance_consumer(replicator_id_);
        }
    }
};
