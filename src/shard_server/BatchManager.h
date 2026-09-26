#pragma once
#include "Batch.h"
#include "Response.h"
#include "SPMCBuffer.h"
#include "Committer.h"
#include <roaring/roaring.hh>

int append_thread_cnt = 0;

struct StreamInfo
{
    std::uint32_t cur_segment_offset_ = 0;   // Offset of the segment currently receiving the entry
    std::uint32_t cur_segment_capacity_ = 0; // Capacity of the segment currently receiving the entry
    std::uint32_t cur_segment_index_ = 0;    // Index of the segment currently receiving the entry
    std::uint32_t total_segment_cnt_ = 0;    // Total segment count of the current stream, used to decide whether a new segment is needed
};

template <typename T, typename F>
class BatchManager
{
private:
    SPMCBuffer *batch_buffer_; // Use SPMCBuffer to store batches
    absl::CondVar *cv_;
    absl::Mutex *cv_mu_;

    absl::Mutex mu_;
    std::uint64_t lsn_{0};
    Responser<T, F> *responser_;
    Replicator *replicator_[REPLICATOR_NUM];
    roaring::Roaring key_bitmap_;
    absl::Mutex segment_bitmap_mu_;
    roaring::Roaring segment_bitmap_;
    absl::flat_hash_map<StateKey, StreamInfo> stream_info_;
    bool *task_ready_[REPLICATOR_NUM + 1]; // 0~REPLICATOR_NUM-1: replicator, REPLICATOR_NUM: committer

    std::map<StateKey, std::uint32_t> key_segment_cnt_;

    std::queue<std::shared_ptr<WrapperHeader>> single_thread_request_queue_;
    absl::Mutex single_thread_request_queue_mu_;
    absl::CondVar single_thread_request_queue_cv_;

    std::vector<std::thread> append_record_threads_;

public:
    BatchManager(SPMCBuffer *batch_buffer, absl::CondVar *cv, absl::Mutex *cv_mu, Responser<T, F> *responser)
        : batch_buffer_(batch_buffer), cv_(cv), cv_mu_(cv_mu), responser_(responser), key_bitmap_(roaring::Roaring())
    {
        if (WORKLOAD == 1 || WORKLOAD == 2 || WORKLOAD == 3)
        {
            stream_info_.reserve(KEY_CNT_OF_SHARD);
            init_ycsb_segment();
        }
        else if (WORKLOAD == 4)
        {
            stream_info_.reserve(3 * USER_CNT);
            init_retwis_segment();
        }
        else if (WORKLOAD == 0)
        {
            stream_info_.reserve(1);
            init_append_only_segment();
        }
        else
        {
            LOG(ERROR) << "invalid workload: " << WORKLOAD;
            exit(-1);
        }
#ifndef USE_RDMA
        if (SharedData::recovered_shard)
            restore_stream_state(*SharedData::recovered_shard);
#endif
        for (std::uint32_t i = 0; i < append_thread_cnt; i++)
        {
            append_record_threads_.push_back(std::thread(&BatchManager::append_record_with_scaling_thread, this));
        }
    }

    ~BatchManager()
    {
    }

#ifndef USE_RDMA
    std::uint32_t admitted_lsn() const { return lsn_; }

    void drop_backup(std::uint32_t shard)
    {
        // Ingress is already parked, so segment allocation cannot race this
        // change. The surviving writer and committer continue draining batches.
        delete replicator_[1];
        SharedData::startup_configuration.header.replica_mask = 1;
        auto *cut = SharedData::shared_pointer<SharedData::CutRegion>(
            SharedData::shard_resources(shard).local_cuts) + 1;
        cut->value.store(std::numeric_limits<std::uint64_t>::max(), std::memory_order_release);
        clwb(cut, sizeof(*cut));
        sfence();
        *task_ready_[1] = true;
        replicator_[1] = new Replicator(shard, 1, batch_buffer_, cv_, cv_mu_, task_ready_[1]);
    }

    void stop_writers(Committer *&committer)
    {
        for (unsigned i = 0; i < REPLICATOR_NUM; ++i)
        {
            delete replicator_[i];
            replicator_[i] = nullptr;
        }
        delete committer;
        committer = nullptr;
    }

    void restart_writers(std::uint32_t shard, Committer *&committer)
    {
        restore_stream_state(*SharedData::recovered_shard);
        for (unsigned i = 0; i < REPLICATOR_NUM + 1; ++i)
            *task_ready_[i] = false;
        committer = new Committer(batch_buffer_, cv_, cv_mu_, task_ready_[REPLICATOR_NUM], shard);
        for (unsigned i = 0; i < REPLICATOR_NUM; ++i)
            replicator_[i] = new Replicator(shard, i, batch_buffer_, cv_, cv_mu_, task_ready_[i]);
    }

    void restore_stream_state(const SharedData::RecoveredShard &state)
    {
        lsn_ = state.progress >> 32;
        stream_info_.clear();
        key_segment_cnt_.clear();
        segment_bitmap_ = roaring::Roaring();
        key_bitmap_ = roaring::Roaring();
        const auto allocation_unit = SharedData::stream_segment_size();
        auto reserve_segments = [&](const SharedData::RecoveredStream &stream)
        {
            if (stream.initial_segments == 0)
                return;
            const auto end = std::uint64_t(stream.head) +
                (stream.first_capacity ? stream.first_capacity : stream.segment_size) +
                std::uint64_t(stream.initial_segments - 1) * stream.segment_size;
            segment_bitmap_.addRange(stream.head / allocation_unit, (end + allocation_unit - 1) / allocation_unit);
            for (const auto &segment : stream.segments)
                segment_bitmap_.addRange(segment.offset / allocation_unit,
                    (std::uint64_t(segment.offset) + segment.capacity + allocation_unit - 1) / allocation_unit);
        };
        for (const auto &[key, stream] : state.streams)
        {
            key_bitmap_.add(key);
            if (stream.initial_segments == 0)
            {
                stream_info_[key] = {};
                continue;
            }
            const auto used = stream.segments.size();
            const auto count = std::max<std::size_t>(stream.initial_segments, used);
            stream_info_[key] = StreamInfo{used ? stream.segments.back().used : 0,
                used ? stream.segments.back().capacity : stream.segment_size, used ? static_cast<std::uint32_t>(used - 1) : 0,
                static_cast<std::uint32_t>(count)};
            key_segment_cnt_[key] = count;
            reserve_segments(stream);
        }
        // A segment remains allocated if either live replica retains it.
        if (SharedData::recovered_backup)
            for (const auto &[key, stream] : SharedData::recovered_backup->streams)
                reserve_segments(stream);
    }
#endif

    void init_append_only_segment()
    {
        if (SCALE_OUT_STREAM)
        {
            // For the scale-out stream experiment, initially there is only one key, and that key has a 100 MB stream
            // should be segment size * 1024 * 100
            stream_info_[0] = StreamInfo{0, 1024 * 1024 * 100, 0, 1};
            segment_bitmap_.addRange(0, 1024 * 100);
        }
        else
        {
            stream_info_[0] = StreamInfo{0, 1024 * 1024 * 500, 0, 1};
            segment_bitmap_.addRange(0, 1024 * 500);
        }
        key_bitmap_.add(0);
    }

    void init_ycsb_segment()
    {
        if (WORKLOAD == 1 && YCSB_OPTION == 3)
        {
            // ycsb_d only has insert and read, so each key does not need a segment; it only needs to record that the key exists
            for (std::uint32_t i = 0; i < KEY_CNT_OF_SHARD; i++)
            {
                stream_info_[i] = StreamInfo{0, 0, 0, 0};
                key_bitmap_.add(i);
            }
            return;
        }
        for (std::uint32_t i = 0; i < KEY_CNT_OF_SHARD; i++)
        {
            if (i < YCSB_HOTTER_KEY_CNT)
            {
                stream_info_[i] = StreamInfo{0, SEGMENT_SIZE, 0, HOTTER_SEG_NUM};
                segment_bitmap_.addRange(i * HOTTER_SEG_NUM, (i + 1) * HOTTER_SEG_NUM);
                key_segment_cnt_[i] = HOTTER_SEG_NUM;
            }
            else if (i < YCSB_HOT_KEY_CNT + YCSB_HOTTER_KEY_CNT)
            {
                stream_info_[i] = StreamInfo{0, SEGMENT_SIZE, 0, HOT_SEG_NUM};
                segment_bitmap_.addRange(HOTTER_SEG_NUM * YCSB_HOTTER_KEY_CNT + (i - YCSB_HOTTER_KEY_CNT) * HOT_SEG_NUM, HOTTER_SEG_NUM * YCSB_HOTTER_KEY_CNT + (i - YCSB_HOTTER_KEY_CNT + 1) * HOT_SEG_NUM);
                key_segment_cnt_[i] = HOT_SEG_NUM;
            }
            else
            {
                stream_info_[i] = StreamInfo{0, SEGMENT_SIZE, 0, COLD_SEG_NUM};
                segment_bitmap_.addRange(HOT_SEG_NUM * YCSB_HOT_KEY_CNT + HOTTER_SEG_NUM * YCSB_HOTTER_KEY_CNT + (i - YCSB_HOT_KEY_CNT - YCSB_HOTTER_KEY_CNT) * COLD_SEG_NUM, HOT_SEG_NUM * YCSB_HOT_KEY_CNT + HOTTER_SEG_NUM * YCSB_HOTTER_KEY_CNT + (i - YCSB_HOT_KEY_CNT - YCSB_HOTTER_KEY_CNT + 1) * COLD_SEG_NUM);
                key_segment_cnt_[i] = COLD_SEG_NUM;
            }
            key_bitmap_.add(i);
        }
    }

    void init_retwis_segment()
    {
        for (std::uint32_t i = USER_CNT; i < 2 * USER_CNT; i++)
        {
            // Only timelines need segments
            stream_info_[i] = StreamInfo{0, RETWIS_SEGMENT_SIZE, 0, TIMELINE_SEG_NUM};
            segment_bitmap_.addRange((i - USER_CNT) * TIMELINE_SEG_NUM, (i - USER_CNT + 1) * TIMELINE_SEG_NUM);
            key_bitmap_.add(i);
        }
    }

    void init_replicator(Replicator *replicator, int id)
    {
        replicator_[id] = replicator;
    }

    void init_task_ready(bool *task_ready, int id)
    {
        task_ready_[id] = task_ready;
    }

    // Send an entry to the batch:
    // 1. Get the current batch
    // 2. Increment the active_writers_ count
    // 3. Check whether the batch is full; if it is, insert a new batch
    std::shared_ptr<Batch> send_entry_to_batch_buffer(std::uint32_t &lsn)
    {
        std::shared_ptr<Batch> batch = batch_buffer_->get_producer_batch();
        if (batch->receive_entry_no_lock(lsn))
        {
            batch_buffer_->advance_producer_index();
        }
        return batch;
    }

    // Return value: need cross segment and need new segment
    // 1. The preallocated segment is sufficient but data must cross segments; 2. a new segment must be allocated
    std::pair<bool /*need cross segment*/, bool /*need new segment*/> check_for_scaling(std::uint32_t &payload_size, StateKey &state_key)
    {
        // Only non-insert workloads reach here, so new state_key cases do not need handling
        // Check whether state_key exists in stream_info_
        if (!stream_info_.contains(state_key))
        {
            LOG(ERROR) << "state_key " << state_key << " not found in stream_info_";
            exit(-1);
        }

        // 1. First check whether the current segment is sufficient
        if (stream_info_[state_key].cur_segment_capacity_ >= stream_info_[state_key].cur_segment_offset_ + payload_size + sizeof(std::uint64_t) + sizeof(LinkPointer))
        {
            stream_info_[state_key].cur_segment_offset_ += payload_size + sizeof(std::uint64_t);
            return std::make_pair(false, false);
        }
        // 2. Check whether remaining segments are sufficient
        if (stream_info_[state_key].cur_segment_index_ < stream_info_[state_key].total_segment_cnt_ - 1)
        {
            stream_info_[state_key].cur_segment_index_++;
            stream_info_[state_key].cur_segment_offset_ = payload_size + sizeof(std::uint64_t);
            return std::make_pair(true, false);
        }
        stream_info_[state_key].cur_segment_index_++;
        stream_info_[state_key].cur_segment_offset_ = payload_size + sizeof(std::uint64_t);
#ifndef USE_RDMA
        stream_info_[state_key].cur_segment_capacity_ = SharedData::stream_segment_size();
#endif
        stream_info_[state_key].total_segment_cnt_++;
        return std::make_pair(true, true);
    }

    std::tuple<bool /*need cross segment*/, bool /*need new segment*/, bool /*new key*/> check_for_scaling_and_new_key(std::uint32_t &payload_size, StateKey &state_key)
    {
        // Check whether state_key exists in stream_info_
        if (!stream_info_.contains(state_key))
        {
            std::uint32_t to_write_size = payload_size + sizeof(std::uint64_t);
            if (WORKLOAD == 4)
            {
                stream_info_[state_key] = StreamInfo{to_write_size, RETWIS_SEGMENT_SIZE, 0, 1};
            }
            else
            {
                if (WORKLOAD == 1 && YCSB_OPTION == 3)
                {
                    stream_info_[state_key] = StreamInfo{to_write_size, YCSB_NEW_KEY_SEGMENT_SIZE, 0, 1};
                }
                else
                {
                    stream_info_[state_key] = StreamInfo{to_write_size, SEGMENT_SIZE, 0, 1};
                }
            }
            // ***note***: For a newly inserted key, is_scale is false because segment scaling writes a link pointer, while inserting a new key does not need one
            return std::make_tuple(false, true, true);
        }

        if (stream_info_[state_key].cur_segment_capacity_ >= stream_info_[state_key].cur_segment_offset_ + payload_size + sizeof(std::uint64_t) + sizeof(LinkPointer))
        {
            // Enough space; nothing else is needed
            stream_info_[state_key].cur_segment_offset_ += payload_size + sizeof(std::uint64_t);
            return std::make_tuple(false, false, false);
        }
        // 2. Check whether remaining segments are sufficient
        if (stream_info_[state_key].cur_segment_index_ < stream_info_[state_key].total_segment_cnt_ - 1)
        {
            // Enough space, but data crosses segments and a link pointer must be written
            stream_info_[state_key].cur_segment_index_++;
            stream_info_[state_key].cur_segment_offset_ = payload_size + sizeof(std::uint64_t);
            return std::make_tuple(true, false, false);
        }
        // 3. No remaining segment; allocate a new segment
        stream_info_[state_key].cur_segment_index_++;
        stream_info_[state_key].cur_segment_offset_ = payload_size + sizeof(std::uint64_t);
#ifndef USE_RDMA
        stream_info_[state_key].cur_segment_capacity_ = SharedData::stream_segment_size();
#endif
        stream_info_[state_key].total_segment_cnt_++;
        return std::make_tuple(true, true, false);
    }

    void scale_stream(StateKey &state_key)
    {
        std::uint32_t low = 0;
        // TODO: This could search for a segment adjacent to the last segment of this state's write stream to maximize data locality
        // Currently this searches for the first free segment
        {
            absl::MutexLock lock(&segment_bitmap_mu_);
            uint32_t high = segment_bitmap_.maximum() + 2; // +2 avoid the maximum value already being contiguous

            while (low < high)
            {
                uint32_t mid = low + (high - low) / 2;
                uint64_t r = segment_bitmap_.rank(mid); // number of values <= mid

                if (r == mid + 1)
                {
                    low = mid + 1;
                }
                else
                {
                    high = mid;
                }
            }
            segment_bitmap_.add(low);
        }
        if (low <= WRITE_STREAM_SIZE / SEGMENT_SIZE)
        {
            for (std::uint32_t i = 0; i < REPLICATOR_NUM; i++)
            {
                replicator_[i]->scale_segment(low, state_key);
            }
        }
        else
        {
            LOG(ERROR) << "not enough cxl region for new segment";
            exit(-1);
        }
    }

    // Return the new segment offset
    template <bool IsHead = false>
    inline std::uint64_t scale_stream_with_new_key(StateKey &state_key)
    {
        std::uint32_t low = 0;
        // TODO: This could search for a segment adjacent to the last segment of this state's write stream to maximize data locality
        // Currently this searches for the first free segment
        {
            absl::MutexLock lock(&segment_bitmap_mu_);
            uint32_t high = segment_bitmap_.maximum() + 2; // +2 avoid the maximum value already being contiguous

            while (low < high)
            {
                uint32_t mid = low + (high - low) / 2;
                uint64_t r = segment_bitmap_.rank(mid); // number of values <= mid

                if (r == mid + 1)
                {
                    low = mid + 1;
                }
                else
                {
                    high = mid;
                }
            }
            segment_bitmap_.add(low);
        }
        std::uint32_t segment_size;
        if (WORKLOAD == 4)
        {
            segment_size = RETWIS_SEGMENT_SIZE;
        }
        else
        {
            if (WORKLOAD == 1 && YCSB_OPTION == 3)
            {
                segment_size = YCSB_NEW_KEY_SEGMENT_SIZE;
            }
            else
            {
                segment_size = SEGMENT_SIZE;
            }
        }
        if (low <= WRITE_STREAM_SIZE / segment_size)
        {
            for (std::uint32_t i = 0; i < REPLICATOR_NUM; i++)
            {
                replicator_[i]->template scale_segment<IsHead>(low, state_key);
            }
        }
        else
        {
            LOG(ERROR) << "not enough cxl region for new segment";
            exit(-1);
        }
        return low * segment_size;
    }

    void push_request_to_queue(std::shared_ptr<WrapperHeader> wrapper_header)
    {
        {
            absl::MutexLock lock(&single_thread_request_queue_mu_);
            single_thread_request_queue_.push(wrapper_header);
        }
        single_thread_request_queue_cv_.Signal();
    }

    void append_record_with_scaling_thread()
    {

    }

    void append_record_with_scaling(std::shared_ptr<ReqHeader> header, std::uint32_t &client_id, const int &socket_fd)
    {
        header->client_id = client_id;
        VLOG(1) << "append_record_with_scaling: is_write=" << header->is_write << " state_key=" << header->state_key << " payload_size=" << header->payload_size;

        if (!header->is_write)
        {
            // Read requests do not need to be written to CXL and can be handled directly by the responser
            responser_->check_and_create_ack_client_thread(client_id, socket_fd);
            header->lsn = 0;
            responser_->receive_request(header->lsn, client_id, header);
            return;
        }

        std::uint32_t lsn;
        std::shared_ptr<Batch> batch;
        std::uint32_t reserved_offset;
        bool is_new_state_key = false;
        bool need_new_segment = false;
        bool need_cross_segment = false;
        Pending_area_state *pending_area_state;
        {
            absl::MutexLock lock(&mu_);
            lsn = ++lsn_;
            header->lsn = lsn;
            // The read entry must also be written into the batch here, so send_entry_to_batch_buffer is called to increase the writer count
            batch = send_entry_to_batch_buffer(lsn);
            // reserve offset is the starting write address in the batch pending_buffer and represents the amount of data already written before this entry
            if (header->is_write)
            {
                pending_area_state = batch->reserve_offset(header->state_key, header->payload_size, header->is_write, reserved_offset);
                std::tie(need_cross_segment, need_new_segment) = check_for_scaling(header->payload_size, header->state_key);
            }
            responser_->receive_request(lsn, client_id, header);
        }
        if (need_cross_segment)
        {
            batch->record_need_scaling(header->state_key, reserved_offset);
            // 2. Insert the new segment into every replicator stream
            if (need_new_segment)
            {
                scale_stream(header->state_key);
            }
        }
        if (header->is_write)
        {
            pending_area_state->write_buffer.copy_to_buffer(lsn, reserved_offset, socket_fd, header->payload_size);
        }
        else
        {
            batch->copy_to_read_buffer(lsn, header->payload_size, socket_fd);
            if (WORKLOAD == 4)
            {
                responser_->receive_reader(lsn, client_id, header);
            }
        }
        if (batch->decrement_active_writers_no_lock())
        {
            VLOG(1) << "append_record_with_scaling: batch READY, lsn=" << lsn << ", signaling replicators";
            {
                absl::MutexLock lock(cv_mu_);
                for (std::uint32_t i = 0; i < REPLICATOR_NUM + 1; i++)
                {
                    *task_ready_[i] = true;
                }
            }
            cv_->SignalAll();
        }
        else
        {
            VLOG(1) << "append_record_with_scaling: decrement did NOT return true, lsn=" << lsn;
        }
    }

    void append_lock_record_with_scaling(std::shared_ptr<ReqHeader> lock_app_header, std::uint32_t &client_id, const int &socket_fd)
    {
        lock_app_header->client_id = client_id;
        std::shared_ptr<ReqHeader> lock_read_header = std::make_shared<ReqHeader>(*lock_app_header);
        std::shared_ptr<Batch> batch;
        std::uint32_t reserved_offset;
        bool need_cross_segment = false;
        bool need_new_segment = false;
        Pending_area_state *pending_area_state;
        {
            absl::MutexLock lock(&mu_);
            lock_app_header->lsn = ++lsn_;
            lock_read_header->lsn = ++lsn_;
            // 1.Update the batch lsn using the read header lsn
            batch = send_entry_to_batch_buffer(lock_read_header->lsn);
            // reserve offset is the starting write address in the batch pending_buffer and represents the amount of data already written before this entry
            pending_area_state = batch->reserve_offset(lock_app_header->state_key, lock_app_header->payload_size, lock_app_header->is_write, reserved_offset);
            std::tie(need_cross_segment, need_new_segment) = check_for_scaling(lock_app_header->payload_size, lock_app_header->state_key);

            // 2. Only the read request needs to be recorded. Write requests update status, and read requests check whether write (lock/unlock) requests succeeded
            responser_->receive_request(lock_read_header->lsn, client_id, lock_read_header);
        }
        if (need_cross_segment)
        {
            batch->record_need_scaling(lock_app_header->state_key, reserved_offset);
            // 2. Insert the new segment into every replicator stream
            if (need_new_segment)
            {
                scale_stream(lock_app_header->state_key);
            }
        }
        // Only record write entries. Read entries are logical and do not need to be written into the batch
        pending_area_state->write_buffer.copy_to_buffer(lock_app_header->lsn, reserved_offset, socket_fd, lock_app_header->payload_size);
        if (batch->decrement_active_writers_no_lock())
        {
            {
                absl::MutexLock lock(cv_mu_);
                for (std::uint32_t i = 0; i < REPLICATOR_NUM + 1; i++)
                {
                    *task_ready_[i] = true;
                }
            }
            cv_->SignalAll();
        }
    }

    void append_record_with_scaling_and_new_key(std::shared_ptr<ReqHeader> header, std::uint32_t &client_id, const int &socket_fd)
    {
        header->client_id = client_id;

        if (WORKLOAD == 1 && YCSB_OPTION == 3 && !header->is_write)
        {
            // YCSB D reads can bypass the CXL log; Retwis reads require logging for exactly-once semantics.
            responser_->check_and_create_ack_client_thread(client_id, socket_fd);
            header->lsn = 0;
            responser_->receive_request(header->lsn, client_id, header);
            return;
        }

        std::uint32_t lsn;
        std::shared_ptr<Batch> batch;
        std::uint32_t reserved_offset;
        bool is_new_state_key = false;
        bool need_cross_segment = false;
        bool need_new_segment = false;
        Pending_area_state *pending_area_state;
        {
            absl::MutexLock lock(&mu_);
            lsn = ++lsn_;
            header->lsn = lsn;
            // The read entry must also be written into the batch here, so send_entry_to_batch_buffer is called to increase the writer count
            batch = send_entry_to_batch_buffer(lsn);
            // reserve offset is the starting write address in the batch pending_buffer and represents the amount of data already written before this entry
            if (header->is_write)
            {
                // Reading a new key will not enter this path
                pending_area_state = batch->reserve_offset(header->state_key, header->payload_size, header->is_write, reserved_offset);
                std::tie(need_cross_segment, need_new_segment, is_new_state_key) = check_for_scaling_and_new_key(header->payload_size, header->state_key);
            }
            responser_->receive_request(lsn, client_id, header);
        }

        if (is_new_state_key)
        {
            absl::Time start_time = absl::Now();
            // A new key scales the segment directly without crossing segments
            std::uint64_t new_segment_offset = scale_stream_with_new_key<true>(header->state_key);
            absl::Duration duration = absl::Now() - start_time;
            LOG_CLASS("BatchManager", "", "allocate segment latency {}", absl::ToInt64Microseconds(duration));
            batch->record_new_state_key(header->state_key, new_segment_offset);
            if (WORKLOAD != 0)
            {
                responser_->create_view_for_new_state_key(header->state_key, new_segment_offset);
            }
        }

        if (need_cross_segment)
        {
            batch->record_need_scaling(header->state_key, reserved_offset);
            // 2. Insert the new segment into every replicator stream
            if (need_new_segment)
            {
                scale_stream_with_new_key(header->state_key);
            }
        }

        if (header->is_write)
        {
            pending_area_state->write_buffer.copy_to_buffer(lsn, reserved_offset, socket_fd, header->payload_size);
        }
        else
        {
            batch->copy_to_read_buffer(lsn, header->payload_size, socket_fd);
            if (WORKLOAD == 4)
            {
                responser_->receive_reader(lsn, client_id, header);
            }
        }
        if (batch->decrement_active_writers_no_lock())
        {
            {
                absl::MutexLock lock(cv_mu_);
                for (std::uint32_t i = 0; i < REPLICATOR_NUM + 1; i++)
                {
                    *task_ready_[i] = true;
                }
            }
            cv_->SignalAll();
        }
    }
};
