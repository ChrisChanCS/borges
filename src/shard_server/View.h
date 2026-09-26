#pragma once

#include "absl/container/flat_hash_map.h"
#include "../common/Macro.h"
#include "../common/CCHashTable.h"
#include <latch>
#include <condition_variable>
#include <barrier>
#include <roaring/roaring.hh>
#include <absl/synchronization/notification.h>
#include <absl/synchronization/mutex.h>
#include "absl/synchronization/barrier.h"
#include "../benchmark/ycsb/Workload.h"
#include "../benchmark/lock/Workload.h"
#include "../benchmark/counter/Workload.h"
#include "../benchmark/retwis/Workload.h"
#include <immintrin.h>
#include "Collector.h"
#include "ViewPool.h"
#include <pthread.h>

int process_entry_count = 0;

struct RoundPosition
{
    std::uint32_t start_offset;
    std::uint32_t end_offset;
    std::uint32_t shard_id;
};

struct Task
{
    std::shared_ptr<absl::InlinedVector<std::pair<OperationId, std::uint32_t>, 64>> opId_lsn_pairs;
};

namespace View
{
    template <typename T>
    class View
    {
    public:
        // state key is also used as the key index
        StateKey state_key_;
        // roaring::Roaring writer_bitmap_;
        std::uint32_t view_lsn_{0};
        std::uint32_t shard_id_{0};
        // YCSB requests for this key always run on key % request_worker_num.
        // The same worker owns both replay state and its publication version.
        std::uint64_t publication_version_ = 0;

        std::shared_ptr<T> value_;

        // ViewOfGSN *view_of_GSN;
        absl::Mutex mu_;

        roaring::Roaring writer_bitmap_;

#ifdef USE_RDMA
        ViewOfShard view_of_shard_[SHARD_SERVER_NUM];
#else
        star::CCHashTable::LookupCache index_lookup_[COMMIT_TAIL_HASH_TABLE_CANDIDATE_NUM];
        absl::InlinedVector<ViewOfShard, SHARD_SERVER_NUM> view_of_shard_ =
            absl::InlinedVector<ViewOfShard, SHARD_SERVER_NUM>(SharedData::startup_configuration.shards.size());
#endif

        std::size_t shard_count() const { return std::size(view_of_shard_); }

        View(StateKey state_key, std::uint32_t shard_id) : state_key_(state_key), shard_id_(shard_id)
        {
            if (WORKLOAD == 0 || WORKLOAD == 1 || WORKLOAD == 2 || WORKLOAD == 3)
            {
                if (YCSB_OPTION == 3 && WORKLOAD == 1)
                {
                    for (std::uint32_t i = 0; i < shard_count(); i++)
                    {
                        // Because there are no writes, ycsb_d does not need to allocate segments for keys
#ifdef USE_RDMA
                        uint64_t cxl_region = g_rdma.get_root(STREAM_ROOT_INDEX + i * REPLICATOR_NUM);
                        view_of_shard_[i].last_tail_offset_ = 0;
                        view_of_shard_[i].cxl_cur_offset_ = cxl_region;
#else
                        char *cxl_region = SharedData::write_stream(i, 0);
                        view_of_shard_[i].last_tail_offset_ = 0;
                        view_of_shard_[i].cxl_cur_ptr_ = cxl_region;
#endif
                    }
                    return;
                }
                for (std::uint32_t i = 0; i < shard_count(); i++)
                {
                    // Use replicator 0 as the primary replica
#ifdef USE_RDMA
                    uint64_t cxl_region = g_rdma.get_root(STREAM_ROOT_INDEX + i * REPLICATOR_NUM);
#else
                    char *cxl_region = SharedData::write_stream(i, 0);
#endif
                    // note: Only the stream begin pointer is recorded here because the hash table stores the write-stream offset; get_new_entry reads data based on that offset
                    view_of_shard_[i].last_tail_offset_ = 0;
                    if (state_key_ < YCSB_HOTTER_KEY_CNT)
                    {
#ifdef USE_RDMA
                        view_of_shard_[i].cxl_cur_offset_ = cxl_region + std::uint64_t(state_key_) * HOTTER_SEG_NUM * SEGMENT_SIZE;
#else
                        view_of_shard_[i].cxl_cur_ptr_ = cxl_region + std::uint64_t(state_key_) * HOTTER_SEG_NUM * SEGMENT_SIZE;
#endif
                    }
                    else if (state_key_ < YCSB_HOT_KEY_CNT + YCSB_HOTTER_KEY_CNT)
                    {
#ifdef USE_RDMA
                        view_of_shard_[i].cxl_cur_offset_ = cxl_region + std::uint64_t(state_key_ - YCSB_HOTTER_KEY_CNT) * HOT_SEG_NUM * SEGMENT_SIZE + std::uint64_t(YCSB_HOTTER_KEY_CNT) * HOTTER_SEG_NUM * SEGMENT_SIZE;
#else
                        view_of_shard_[i].cxl_cur_ptr_ = cxl_region + std::uint64_t(state_key_ - YCSB_HOTTER_KEY_CNT) * HOT_SEG_NUM * SEGMENT_SIZE + std::uint64_t(YCSB_HOTTER_KEY_CNT) * HOTTER_SEG_NUM * SEGMENT_SIZE;
#endif
                    }
                    else
                    {
#ifdef USE_RDMA
                        view_of_shard_[i].cxl_cur_offset_ = cxl_region + std::uint64_t(state_key_ - YCSB_HOT_KEY_CNT - YCSB_HOTTER_KEY_CNT) * COLD_SEG_NUM * SEGMENT_SIZE + std::uint64_t(YCSB_HOT_KEY_CNT) * HOT_SEG_NUM * SEGMENT_SIZE + std::uint64_t(YCSB_HOTTER_KEY_CNT) * HOTTER_SEG_NUM * SEGMENT_SIZE;
#else
                        view_of_shard_[i].cxl_cur_ptr_ = cxl_region + std::uint64_t(state_key_ - YCSB_HOT_KEY_CNT - YCSB_HOTTER_KEY_CNT) * COLD_SEG_NUM * SEGMENT_SIZE + std::uint64_t(YCSB_HOT_KEY_CNT) * HOT_SEG_NUM * SEGMENT_SIZE + std::uint64_t(YCSB_HOTTER_KEY_CNT) * HOTTER_SEG_NUM * SEGMENT_SIZE;
#endif
                    }
                }
            }
            else if (WORKLOAD == 4)
            {
                for (std::uint32_t i = 0; i < shard_count(); i++)
                {
#ifdef USE_RDMA
                    uint64_t cxl_region = g_rdma.get_root(STREAM_ROOT_INDEX + i * REPLICATOR_NUM);
#else
                    char *cxl_region = SharedData::write_stream(i, 0);
#endif
                    view_of_shard_[i].last_tail_offset_ = 0;
                    if (state_key_ < USER_CNT)
                    {
                        // user is read-only
                        continue;
                    }
                    else if (state_key_ < 2 * USER_CNT)
                    {
                        // timeline, one retwis_segment per timeline
#ifdef USE_RDMA
                        view_of_shard_[i].cxl_cur_offset_ = cxl_region + std::uint64_t(state_key_ - USER_CNT) * RETWIS_SEGMENT_SIZE * TIMELINE_SEG_NUM;
#else
                        view_of_shard_[i].cxl_cur_ptr_ = cxl_region + std::uint64_t(state_key_ - USER_CNT) * RETWIS_SEGMENT_SIZE * TIMELINE_SEG_NUM;
#endif
                    }
                    else
                    {
                        // post is not updated, only read and append
                        continue;
                    }
                }
            }
            else
            {
                LOG(ERROR) << "Invalid workload type: " << WORKLOAD;
                exit(-1);
            }
#ifndef USE_RDMA
            for (const auto &[identity, entry] : SharedData::startup_lifecycles)
                if (entry.key == state_key_)
                    view_of_shard_[entry.shard].cxl_cur_ptr_ =
                        SharedData::write_stream(entry.shard, 0) + entry.head;
#endif
        }

#ifdef USE_RDMA
        View(StateKey state_key, uint64_t begin_cxl_offset, std::uint32_t shard_id) : state_key_(state_key), shard_id_(shard_id)
        {
            view_of_shard_[shard_id].cxl_cur_offset_ = begin_cxl_offset;
        }
#else
        View(StateKey state_key, char *begin_cxl_region, std::uint32_t shard_id) : state_key_(state_key), shard_id_(shard_id)
        {
            view_of_shard_[shard_id].cxl_cur_ptr_ = begin_cxl_region;
        }
#endif

#ifndef USE_RDMA
        void ensure_configuration(const SharedData::ClusterConfiguration &configuration)
        {
            const auto old_count = view_of_shard_.size();
            if (old_count >= configuration.shards.size())
                return;
            view_of_shard_.resize(configuration.shards.size());
            for (std::size_t shard = old_count; shard < view_of_shard_.size(); ++shard)
            {
                auto *base = SharedData::shared_pointer<char>(configuration.shards[shard].write_stream[0]);
                auto &cursor = view_of_shard_[shard];
                cursor.cxl_cur_ptr_ = base;
                if (WORKLOAD == 4)
                {
                    if (state_key_ >= USER_CNT && state_key_ < 2 * USER_CNT)
                        cursor.cxl_cur_ptr_ += std::uint64_t(state_key_ - USER_CNT) * RETWIS_SEGMENT_SIZE * TIMELINE_SEG_NUM;
                }
                else if (!(WORKLOAD == 1 && YCSB_OPTION == 3))
                {
                    if (state_key_ < YCSB_HOTTER_KEY_CNT)
                        cursor.cxl_cur_ptr_ += std::uint64_t(state_key_) * HOTTER_SEG_NUM * SEGMENT_SIZE;
                    else if (state_key_ < YCSB_HOTTER_KEY_CNT + YCSB_HOT_KEY_CNT)
                        cursor.cxl_cur_ptr_ += std::uint64_t(state_key_ - YCSB_HOTTER_KEY_CNT) * HOT_SEG_NUM * SEGMENT_SIZE +
                            std::uint64_t(YCSB_HOTTER_KEY_CNT) * HOTTER_SEG_NUM * SEGMENT_SIZE;
                    else
                        cursor.cxl_cur_ptr_ += std::uint64_t(state_key_ - YCSB_HOTTER_KEY_CNT - YCSB_HOT_KEY_CNT) * COLD_SEG_NUM * SEGMENT_SIZE +
                            std::uint64_t(YCSB_HOT_KEY_CNT) * HOT_SEG_NUM * SEGMENT_SIZE +
                            std::uint64_t(YCSB_HOTTER_KEY_CNT) * HOTTER_SEG_NUM * SEGMENT_SIZE;
                }
            }
        }

        void read_index(star::CCHashTable *index, const SharedData::ClusterConfiguration &configuration)
        {
            ensure_configuration(configuration);
            if constexpr (std::is_same_v<T, star::retwis::State>)
                if (state_key_ >= 2 * USER_CNT)
                {
                    index->search_with_heads(state_key_, view_of_shard_.data(), shard_count(),
                        [&](std::size_t shard, std::uint64_t head)
                        {
                            return SharedData::shared_pointer<char>(configuration.shards[shard].write_stream[0]) + head;
                        });
                    return;
                }
            index->search(state_key_, view_of_shard_.data(), shard_count());
        }
#endif

        void lock()
        {
            mu_.Lock();
        }
        void unlock()
        {
            mu_.Unlock();
        }
        void set_value(std::shared_ptr<T> value)
        {
            value_ = value;
        }
        bool is_operation_id_exist(std::uint32_t operation_id)
        {
            if (writer_bitmap_.contains(operation_id))
            {
                return true;
            }
            else
            {
                writer_bitmap_.add(operation_id);
                return false;
            }
        }
    };

    template <typename T>
    class ViewManager
    {
    private:
        std::uint32_t shard_id_;
        absl::Mutex mu_;
        WorkloadType workload_type_;
        absl::flat_hash_map<StateKey, View<T> *> state_key_to_view_;
#ifdef USE_RDMA
        uint64_t cxl_write_region_[SHARD_SERVER_NUM];
#else
        std::vector<char *> cxl_write_region_ = std::vector<char *>(SharedData::startup_configuration.shards.size());
#endif

    public:
        ViewManager(std::uint32_t shard_id, WorkloadType workload_type) : shard_id_(shard_id), workload_type_(workload_type)
        {
            for (std::uint32_t i = 0; i < std::size(cxl_write_region_); i++)
            {
                // Use replicator 0 as the primary replica
#ifdef USE_RDMA
                cxl_write_region_[i] = g_rdma.get_root(STREAM_ROOT_INDEX + i * REPLICATOR_NUM);
#else
                cxl_write_region_[i] = SharedData::write_stream(i, 0);
#endif
            }
            if constexpr (std::is_same_v<T, star::ycsb::State>)
            {
                init_ycsb_view();
            }
            else if constexpr (std::is_same_v<T, star::lock::State>)
            {
                init_lock_view();
            }
            else if constexpr (std::is_same_v<T, star::counter::State>)
            {
                init_counter_view();
            }
            else if constexpr (std::is_same_v<T, star::retwis::State>)
            {
                init_retwis_view();
            }
            else
            {
                LOG(ERROR) << "Invalid workload type: " << WORKLOAD;
                exit(-1);
            }
#ifndef USE_RDMA
            if (SharedData::recovered_shard)
                for (const auto &[key, stream] : SharedData::recovered_shard->streams)
                    if (stream.tail != 0 && !state_key_to_view_.contains(key))
                    {
                        auto state_key = key;
                        std::uint64_t offset = stream.head;
                        create_view_for_new_state_key(state_key, offset);
                    }
#endif
        }

        void init_ycsb_view()
        {
            for (std::uint32_t i = 0; i < KEY_CNT_OF_SHARD; i++)
            {
                state_key_to_view_[i] = new View<T>(i, shard_id_);
                state_key_to_view_[i]->set_value(star::ycsb::init_value());
            }
            LOG(INFO) << "init ycsb with " << KEY_CNT_OF_SHARD << " state keys";
        }

        void init_lock_view()
        {
            for (std::uint32_t i = 0; i < KEY_CNT_OF_SHARD; i++)
            {
                state_key_to_view_[i] = new View<T>(i, shard_id_);
                state_key_to_view_[i]->set_value(star::lock::init_value());
            }
            LOG(INFO) << "init lock with " << KEY_CNT_OF_SHARD << " state keys";
        }

        void init_counter_view()
        {
            for (std::uint32_t i = 0; i < KEY_CNT_OF_SHARD; i++)
            {
                state_key_to_view_[i] = new View<T>(i, shard_id_);
                state_key_to_view_[i]->set_value(star::counter::init_value());
            }
            LOG(INFO) << "init counter with " << KEY_CNT_OF_SHARD << " state keys";
        }

        void init_retwis_view()
        {
            // init user
            for (std::uint32_t i = 0; i < USER_CNT; i++)
            {
                state_key_to_view_[i] = new View<T>(i, shard_id_);
                state_key_to_view_[i]->set_value(star::retwis::init_user(i));
            }

            std::map<std::uint32_t, std::uint32_t> initial_posts;
            for (std::uint32_t i = 0; i < MAX_RETURN_POST_CNT; i++)
            {
                initial_posts[i] = star::retwis::get_timestamp_sec();
                state_key_to_view_[i + 2 * USER_CNT] = new View<T>(i + 2 * USER_CNT, shard_id_);
                state_key_to_view_[i + 2 * USER_CNT]->set_value(star::retwis::init_post(i));
            }

            // init timeline
            for (std::uint32_t i = USER_CNT; i < 2 * USER_CNT; i++)
            {
                state_key_to_view_[i] = new View<T>(i, shard_id_);
                state_key_to_view_[i]->set_value(star::retwis::init_timeline(i - USER_CNT, initial_posts));
            }
        }

        View<T> *get_view(const StateKey &state_key)
        {
            View<T> *view = nullptr;
            {
                // If a new key is read here, create a view and initialize the value
                absl::MutexLock lock(&mu_);
                auto it = state_key_to_view_.find(state_key);
                if (it == state_key_to_view_.end())
                {
#ifdef USE_RDMA
                    view = new View<T>(state_key, uint64_t(0), shard_id_);
#else
                    view = new View<T>(state_key, nullptr, shard_id_);
#endif
                    state_key_to_view_[state_key] = view;
                    // TODO: This can be optimized: value does not need to be initialized, but currently skipping initialization leaves view->value as a null pointer
                    if constexpr (std::is_same_v<T, star::ycsb::State>)
                    {
                        state_key_to_view_[state_key]->value_ = star::ycsb::init_value();
                    }
                    else if constexpr (std::is_same_v<T, star::lock::State>)
                    {
                        state_key_to_view_[state_key]->value_ = star::lock::init_value();
                    }
                    else if constexpr (std::is_same_v<T, star::counter::State>)
                    {
                        state_key_to_view_[state_key]->value_ = star::counter::init_value();
                    }
                    else if constexpr (std::is_same_v<T, star::retwis::State>)
                    {
                        CHECK_GE(state_key, 2 * USER_CNT);
                        view->value_ = star::retwis::init_post(state_key - 2 * USER_CNT);
                    }
                    else
                    {
                        LOG(ERROR) << "Invalid workload type in get_view, state_key: " << state_key;
                        exit(-1);
                    }
                }
                else
                {
                    view = it->second;
                }
            }
            return view;
        }

        // The maintenance reader barrier owns all views while this runs.
        void invalidate_stream(StateKey key)
        {
            auto it = state_key_to_view_.find(key);
            if (it != state_key_to_view_.end())
            {
                delete it->second;
                auto *view = new View<T>(key, shard_id_);
                if constexpr (std::is_same_v<T, star::ycsb::State>)
                    view->value_ = star::ycsb::init_value();
                else if constexpr (std::is_same_v<T, star::lock::State>)
                    view->value_ = star::lock::init_value();
                else if constexpr (std::is_same_v<T, star::counter::State>)
                    view->value_ = star::counter::init_value();
                else if constexpr (std::is_same_v<T, star::retwis::State>)
                {
                    CHECK_GE(key, 2 * USER_CNT);
                    view->value_ = star::retwis::init_post(key - 2 * USER_CNT);
                }
                it->second = view;
            }
        }

        void create_view_for_new_state_key(StateKey &state_key, std::uint64_t &offset)
        {
            View<T> *view = new View<T>(state_key, cxl_write_region_[shard_id_] + offset, shard_id_);
            {
                absl::MutexLock lock(&mu_);
                if (state_key_to_view_.contains(state_key))
                {
                    // This new key may have been read before: for example, another shard inserted a key, then this shard read it and created a view, but no segment exists
                    return;
                }
                state_key_to_view_[state_key] = view;
                // TODO: This can be optimized: value does not need to be initialized, but currently skipping initialization leaves view->value as a null pointer
                if constexpr (std::is_same_v<T, star::ycsb::State>)
                {
                    state_key_to_view_[state_key]->value_ = nullptr;
                }
                else if constexpr (std::is_same_v<T, star::lock::State>)
                {
                    state_key_to_view_[state_key]->value_ = star::lock::init_value();
                }
                else if constexpr (std::is_same_v<T, star::counter::State>)
                {
                    state_key_to_view_[state_key]->value_ = star::counter::init_value();
                }
                else if constexpr (std::is_same_v<T, star::retwis::State>)
                {
                    state_key_to_view_[state_key]->value_ = star::retwis::init_post();
                }
                else
                {
                    LOG(ERROR) << "Invalid workload type in create_view_for_new_state_key";
                    exit(-1);
                }
            }
        }
    };

    //                 // Process the current entry.
    //                 process_entry(view_buffer_->buffer_[min_cursor.shard_id] + min_cursor.offset,
    //                               min_cursor.shard_id, min_cursor.round);

}
