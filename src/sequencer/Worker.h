#pragma once

#include "../common/CCHashTable.h"
#include <cstring>
#include <thread>
#include <atomic>
#include <glog/logging.h>
#include <absl/container/flat_hash_map.h>
#include <roaring/roaring.h>
#include "../common/SharedData.h"
#include "../benchmark/retwis/Query.h"
#include "../common/Cacheline.h"

// Give each shard's GSN a separate cache line to avoid false sharing.
#ifdef USE_RDMA
alignas(64) SharedData::GSN gsn[SHARD_SERVER_NUM];
#else
std::vector<SharedData::GSN> gsn(SHARD_SERVER_NUM);
#endif
std::atomic<bool> gsn_dirty = false;
std::atomic<bool> hash_table_dirty = false;
std::uint8_t switch_flag_value = 1;

// UpdateKeyList update_key_list[SHARD_SERVER_NUM];

// Give each pointer a separate cache line to avoid false sharing.
struct alignas(64) CommitTailPtr
{
    star::CCHashTable *ptr{nullptr};
    char padding[64 - sizeof(star::CCHashTable *)]; // Pad to a full cache line.
};
alignas(64) CommitTailPtr commit_tail_hash_table_ptr_[SHARD_SERVER_NUM];

namespace Sequencer
{
    class Worker
    {
        // Keep one round of updates while alternating the two commit-tail indexes.
    public:
        std::thread thread; // Worker thread
        star::CCHashTable *commit_tail_hash_table_candidate_[COMMIT_TAIL_HASH_TABLE_CANDIDATE_NUM];
        // Start at index 1 so the first increment selects candidate index 0.
        std::uint32_t current_candidate_index_{1};
        SharedData::CutRegion *cut_region_;
        SharedData::CommitRegion commit_region_;
        std::uint64_t *local_commit_buffer_;
        // Track the consumed commit-buffer offset separately for each index.
#ifdef USE_RDMA
        std::uint32_t last_key_num_[2] = {0};
#else
        std::uint64_t last_key_num_[2] = {0};
        std::uint64_t sealed_position_ = 0;
        std::uint64_t reclaimed_position_ = 0;
#endif
        // Diagnostic state belongs to this worker, just like its shard LSN.
        std::uint32_t last_logged_lsn_ = std::numeric_limits<std::uint32_t>::max();
#ifndef USE_RDMA
        // Last prefix whose deltas were consumed successfully. Once its owner
        // expires, both index copies may catch up to this prefix but no farther.
        std::uint64_t sealed_cut_ = 0;
        unsigned consumed_slot_ = 0;
#endif

        absl::flat_hash_map<StateKey, std::uint64_t> update_map_;
        std::uint32_t key_cnt_of_shard_;

        std::uint32_t shard_id_;
        std::size_t shard_count_ = SHARD_SERVER_NUM;
        std::atomic<std::uint32_t> *done_worker_;

        std::atomic<bool> busy; // Whether the worker is busy
        std::atomic<bool> stop; // Stop flag

#ifdef USE_RDMA
        uint64_t cut_region_offset_;        // base remote offset for cut regions
        uint64_t commit_buffer_offset_;     // remote offset of commit ring buffer
        uint64_t commit_metadata_offset_;   // remote offset of commit buffer metadata
        uint64_t flat_table_offset_[COMMIT_TAIL_HASH_TABLE_CANDIDATE_NUM]; // flat AllShardInfo table
        uint64_t stream_base_offset_;       // base offset of shard's replicator-0 stream (for validation)
#endif

        Worker(std::uint32_t shard_id, std::atomic<std::uint32_t> *done_worker, bool restoring = false, std::uint64_t sealed = 0) : shard_id_(shard_id), done_worker_(done_worker)
        {
#ifndef USE_RDMA
            if (restoring)
            {
                shard_count_ = SharedData::startup_configuration.shards.size();
                for (std::size_t i = 0; i < 2; ++i)
                    commit_tail_hash_table_candidate_[i] = SharedData::shared_pointer<star::CCHashTable>(
                        SharedData::startup_configuration.header.index[i]);
                sealed_cut_ = sealed;
            }
            else
#endif
            if (WORKLOAD == 1 || WORKLOAD == 2 || WORKLOAD == 3)
            {
                init_ycsb_hash_table();
            }
            else if (WORKLOAD == 4)
            {
                init_retwis_hash_table();
            }
            else if (WORKLOAD == 0)
            {
                init_append_only_hash_table();
            }
            else
            {
                LOG(ERROR) << "invalid workload: " << WORKLOAD;
                exit(-1);
            }

            // 1b. Under RDMA, read flat hash table root offsets
#ifdef USE_RDMA
            for (std::uint32_t i = 0; i < COMMIT_TAIL_HASH_TABLE_CANDIDATE_NUM; i++)
            {
                flat_table_offset_[i] = g_rdma.get_root(TAIL_HASH_TABLE_ROOT_INDEX + i);
            }
#endif

            // 2. Locate this shard's commit buffer.
#ifdef USE_RDMA
            commit_buffer_offset_ = g_rdma.get_root(COMMIT_RING_BUFFER_ROOT_INDEX + shard_id_);
            commit_metadata_offset_ = g_rdma.get_root(COMMIT_BUFFER_METADATA_ROOT_INDEX + shard_id_);
#else
            commit_region_.commit_buffer = SharedData::shared_pointer<std::uint64_t>(SharedData::shard_resources(shard_id_).commit_buffer);
            commit_region_.commit_buffer_metadata = SharedData::shared_pointer<SharedData::CommitBufferMetadata>(SharedData::shard_resources(shard_id_).commit_metadata);
            if (!restoring)
                commit_region_.commit_buffer_metadata->key_num.store(0, std::memory_order_relaxed);
            else
            {
                const auto positions = commit_region_.commit_buffer_metadata->read_consumed();
                CHECK(positions[0] == sealed || positions[1] == sealed);
                consumed_slot_ = positions[0] == sealed ? 0 : 1;
                sealed_position_ = SharedData::DeltaRing::restore_position(
                    SharedData::read_cxl_word(commit_region_.commit_buffer_metadata->write_position),
                    static_cast<std::uint32_t>(sealed));
                last_key_num_[0] = last_key_num_[1] = sealed_position_;
                // Restored indexes already contain the same durable prefix.
                // Release it even if no later delta arrives to wake the writer.
                auto &reclaimed = commit_region_.commit_buffer_metadata->reclaimed;
                reclaimed_position_ = SharedData::DeltaRing::reclaim_before(
                    sealed_position_, sealed_position_, sealed_position_);
                reclaimed.store(reclaimed_position_, std::memory_order_release);
                clwb(&reclaimed, sizeof(reclaimed));
                sfence();
            }
#endif

            // 3. Locate every replicator's cut region for this shard.
#ifdef USE_RDMA
            cut_region_offset_ = g_rdma.get_root(CUT_REGION_ROOT_INDEX + shard_id * REPLICATOR_NUM);
            // Cache stream base offset for replicator 0 (used for validation)
            stream_base_offset_ = g_rdma.get_root(STREAM_ROOT_INDEX + shard_id * REPLICATOR_NUM);
#else
            cut_region_ = SharedData::shared_pointer<SharedData::CutRegion>(SharedData::shard_resources(shard_id_).local_cuts);
#endif

            // 4. Allocate the local commit buffer.
            local_commit_buffer_ = new std::uint64_t[LOCAL_CUT_BUFFER_SIZE];

            update_map_.reserve(MAX_ENTRIES_PER_BATCH);
            busy.store(false);
            stop.store(false);
        }

        ~Worker() { delete[] local_commit_buffer_; }

        void init_append_only_hash_table()
        {
#ifndef USE_RDMA
            for (std::uint32_t i = 0; i < COMMIT_TAIL_HASH_TABLE_CANDIDATE_NUM; i++)
            {
                commit_tail_hash_table_candidate_[i] = reinterpret_cast<star::CCHashTable *>(cxlalloc_get_root(TAIL_HASH_TABLE_ROOT_INDEX + i));
                commit_tail_hash_table_candidate_[i]->insert(0);
                for (std::uint32_t j = 0; j < SHARD_SERVER_NUM; j++)
                {
                    commit_tail_hash_table_candidate_[i]->update(0, j, 0, 0);
                }
            }
#endif
        }

        void init_ycsb_hash_table()
        {
#ifndef USE_RDMA
            if (YCSB_OPTION == 3 && WORKLOAD == 1)
            {
                for (std::uint32_t i = 0; i < COMMIT_TAIL_HASH_TABLE_CANDIDATE_NUM; i++)
                {
                    commit_tail_hash_table_candidate_[i] = reinterpret_cast<star::CCHashTable *>(cxlalloc_get_root(TAIL_HASH_TABLE_ROOT_INDEX + i));
                    for (std::uint32_t j = 0; j < KEY_CNT_OF_SHARD; j++)
                    {
                        commit_tail_hash_table_candidate_[i]->insert(j);
                        for (std::uint32_t k = 0; k < SHARD_SERVER_NUM; k++)
                        {
                            // YCSB D needs no per-key segments; initialize stream offsets to zero.
                            commit_tail_hash_table_candidate_[i]->update(j, k, 0, 0);
                        }
                    }
                }
                return;
            }
            for (std::uint32_t i = 0; i < COMMIT_TAIL_HASH_TABLE_CANDIDATE_NUM; i++)
            {
                commit_tail_hash_table_candidate_[i] = reinterpret_cast<star::CCHashTable *>(cxlalloc_get_root(TAIL_HASH_TABLE_ROOT_INDEX + i));
                for (std::uint32_t j = 0; j < KEY_CNT_OF_SHARD; j++)
                {
                    std::uint64_t write_stream_offset;
                    if (j < YCSB_HOTTER_KEY_CNT)
                    {
                        write_stream_offset = std::uint64_t(j) * HOTTER_SEG_NUM * SEGMENT_SIZE;
                    }
                    else if (j < YCSB_HOT_KEY_CNT + YCSB_HOTTER_KEY_CNT)
                    {
                        write_stream_offset = std::uint64_t(j - YCSB_HOTTER_KEY_CNT) * HOT_SEG_NUM * SEGMENT_SIZE + std::uint64_t(YCSB_HOTTER_KEY_CNT) * HOTTER_SEG_NUM * SEGMENT_SIZE;
                    }
                    else
                    {
                        write_stream_offset = std::uint64_t(j - YCSB_HOT_KEY_CNT - YCSB_HOTTER_KEY_CNT) * COLD_SEG_NUM * SEGMENT_SIZE + std::uint64_t(YCSB_HOT_KEY_CNT) * HOT_SEG_NUM * SEGMENT_SIZE + std::uint64_t(YCSB_HOTTER_KEY_CNT) * HOTTER_SEG_NUM * SEGMENT_SIZE;
                    }
                    commit_tail_hash_table_candidate_[i]->insert(j);
                    for (std::uint32_t k = 0; k < SHARD_SERVER_NUM; k++)
                    {
                        commit_tail_hash_table_candidate_[i]->restore(j, k, star::ShardInfo(0, write_stream_offset));
                    }
                }
            }
#endif
        }

        void init_retwis_hash_table()
        {
#ifndef USE_RDMA
            for (std::uint32_t i = 0; i < COMMIT_TAIL_HASH_TABLE_CANDIDATE_NUM; i++)
            {
                commit_tail_hash_table_candidate_[i] = reinterpret_cast<star::CCHashTable *>(cxlalloc_get_root(TAIL_HASH_TABLE_ROOT_INDEX + i));
                for (std::uint32_t j = USER_CNT; j < 2 * USER_CNT; j++)
                {
                    // Initialize timelines.
                    std::uint64_t write_stream_offset = std::uint64_t(j - USER_CNT) * RETWIS_SEGMENT_SIZE * TIMELINE_SEG_NUM;
                    commit_tail_hash_table_candidate_[i]->insert(j);
                    for (std::uint32_t k = 0; k < SHARD_SERVER_NUM; k++)
                    {
                        commit_tail_hash_table_candidate_[i]->restore(j, k, star::ShardInfo(0, write_stream_offset));
                    }
                }
                for (std::uint32_t j = 2 * USER_CNT; j < 2 * USER_CNT + MAX_RETURN_POST_CNT; j++)
                {
                    // Initialize posts; they do not append entries and need no write stream.
                    commit_tail_hash_table_candidate_[i]->insert(j);
                    for (std::uint32_t k = 0; k < SHARD_SERVER_NUM; k++)
                    {
                        commit_tail_hash_table_candidate_[i]->update(j, k, 0, 0);
                    }
                }
            }
#endif
        }

        // #pragma clang optimize off
        void collect_report()
        {
            current_candidate_index_ = 1 - switch_flag_value;
            // Read each replicator's cut from its cut region first.
            std::uint64_t commit_cut = std::numeric_limits<std::uint64_t>::max();
            get_commit_cut(commit_cut);
            std::uint32_t lsn = commit_cut >> 32;
            if (VLOG_IS_ON(1) && lsn != last_logged_lsn_) {
                VLOG(1) << "worker " << shard_id_ << " collect_report: lsn=" << lsn << " gsn=" << gsn[shard_id_].value;
                last_logged_lsn_ = lsn;
            }
            std::uint32_t replicated_key_num = commit_cut & 0xFFFFFFFF;

            // for debug
#ifdef USE_RDMA
            if (replicated_key_num <= last_key_num_[current_candidate_index_] && lsn <= gsn[shard_id_].value)
#else
            if (replicated_key_num == static_cast<std::uint32_t>(last_key_num_[current_candidate_index_]) &&
                lsn <= gsn[shard_id_].value)
#endif
            {
                sfence(); // complete the local-cut post-read invalidations
                return;
            }

            std::uint32_t delta_key_num = replicated_key_num - static_cast<std::uint32_t>(last_key_num_[current_candidate_index_]);
            CHECK(delta_key_num >= 0) << "delta_key_num should be at least 0";

            if (lsn < gsn[shard_id_].value)
            {
                // A decreasing LSN is stale; RDMA completion races can produce such reads.
                LOG(WARNING) << "find a smaller lsn: " << lsn << " gsn: " << gsn[shard_id_].value << " (ignoring stale read)";
                sfence();
                return;
            }
#ifdef USE_RDMA
            {
                void* staging = g_rdma.get_staging_buf();
                std::uint32_t remote_key_num = 0;
                g_rdma.read(staging, commit_metadata_offset_, sizeof(std::uint32_t));
                std::memcpy(&remote_key_num, staging, sizeof(std::uint32_t));
                while (remote_key_num < replicated_key_num)
                {
                    SharedData::reader_backoff();
                    g_rdma.read(staging, commit_metadata_offset_, sizeof(std::uint32_t));
                    std::memcpy(&remote_key_num, staging, sizeof(std::uint32_t));
                }
            }
#else
            // Reused physical lines may still contain a previous lap in this
            // host's cache. The existing metadata-read fence completes these
            // invalidations; the first linear pass needs no additional flush.
            if (last_key_num_[current_candidate_index_] + delta_key_num > SharedData::DeltaRing::capacity) [[unlikely]]
                SharedData::DeltaRing::flush(commit_region_.commit_buffer,
                    last_key_num_[current_candidate_index_], std::min<std::uint64_t>(delta_key_num, SharedData::DeltaRing::capacity));
            auto *metadata = commit_region_.commit_buffer_metadata;
            auto published = SharedData::read_cxl_word(metadata->key_num);
            while (!SharedData::DeltaRing::reached(published, replicated_key_num))
            {
                if (SharedData::node_leases.expired_shards.load(std::memory_order_acquire) &
                    (std::uint64_t{1} << shard_id_))
                {
                    gsn[shard_id_].value = sealed_cut_ >> 32;
                    return;
                }
                // A full writer cannot supply the replicators' entire cut yet.
                // Drain its last complete batch, which both replicas have
                // already covered, and let the other index catch up next round.
                const auto blocked = SharedData::read_cxl_word(metadata->blocked_cut);
                if (blocked && blocked <= commit_cut && (blocked >> 32) >= gsn[shard_id_].value &&
                    SharedData::DeltaRing::reached(published, static_cast<std::uint32_t>(blocked)))
                {
                    commit_cut = blocked;
                    lsn = commit_cut >> 32;
                    replicated_key_num = static_cast<std::uint32_t>(commit_cut);
                    delta_key_num = replicated_key_num - static_cast<std::uint32_t>(last_key_num_[current_candidate_index_]);
                    break;
                }
                SharedData::reader_backoff();
                published = SharedData::read_cxl_word(metadata->key_num);
            }
            CHECK_LE(delta_key_num, SharedData::DeltaRing::capacity);
#endif
            if (lsn > gsn[shard_id_].value)
            {
                gsn[shard_id_].value = lsn;
                gsn_dirty.store(true, std::memory_order_release);
                LOG_CLASS("Worker {}", shard_id_, "receive local cut of request id: {}", lsn);
            }
            // The other index may need to catch up even when the LSN has not advanced.
            if (delta_key_num > 0)
            {
                hash_table_dirty.store(true, std::memory_order_release);
#ifdef USE_RDMA
                // Read the commit buffer entries from remote into local_commit_buffer_ via staging buffer
                {
                    uint64_t base_offset = commit_buffer_offset_ + static_cast<uint64_t>(last_key_num_[current_candidate_index_]) * sizeof(std::uint64_t);
                    uint64_t total_len = static_cast<uint64_t>(delta_key_num) * sizeof(std::uint64_t);
                    void* staging = g_rdma.get_staging_buf();
                    constexpr uint64_t chunk_size = RdmaRegion::STAGING_BUF_SIZE;
                    std::uint64_t *dst = local_commit_buffer_ + last_key_num_[current_candidate_index_];
                    uint64_t bytes_read = 0;
                    while (bytes_read < total_len)
                    {
                        uint64_t this_len = std::min(chunk_size, total_len - bytes_read);
                        g_rdma.read(staging, base_offset + bytes_read, this_len);
                        std::memcpy(reinterpret_cast<char*>(dst) + bytes_read, staging, this_len);
                        bytes_read += this_len;
                    }
                    // Validate: check that commit buffer entries are well-formed
                    // Each entry is (state_key << 32) | tail_offset; state_key must be < KEY_CNT_OF_SHARD
                    for (std::uint32_t vi = 0; vi < delta_key_num; vi++)
                    {
                        std::uint64_t entry = dst[vi];
                        std::uint32_t sk = entry >> 32;
                        std::uint32_t tail = entry & 0xFFFFFFFF;
                        if (tail == 0 && (WORKLOAD == 4 || (WORKLOAD == 1 && YCSB_OPTION == 3) || (WORKLOAD == 0 && SCALE_OUT_STREAM)))
                        {
                            vi++; // next entry is write_stream_offset for new key, skip validation
                            continue;
                        }
                        CHECK(entry != 0)
                            << "[RDMA-CHECK] worker " << shard_id_
                            << " commit_buf entry[" << (last_key_num_[current_candidate_index_] + vi)
                            << "] is zero (stale/unwritten RDMA region)";
                        CHECK(sk < KEY_CNT_OF_SHARD)
                            << "[RDMA-CHECK] worker " << shard_id_
                            << " commit_buf entry[" << (last_key_num_[current_candidate_index_] + vi)
                            << "] has invalid state_key=" << sk << " (max=" << KEY_CNT_OF_SHARD << ")"
                            << " raw=0x" << std::hex << entry << std::dec;
                    }
                }
#endif
                parse_commit_buffer(delta_key_num, commit_cut);
                last_key_num_[current_candidate_index_] += delta_key_num;
                // logAppend log
                LOG_CLASS("Worker {}", shard_id_, "update hash table, request id: {}", lsn);
            }
#ifndef USE_RDMA
            if (delta_key_num == 0 && commit_cut != sealed_cut_)
            {
                record_consumed_position(commit_cut);
                sfence();
            }
            sealed_cut_ = commit_cut;
            sealed_position_ = last_key_num_[current_candidate_index_];
#endif
        }
        // #pragma clang optimize on

#ifdef USE_RDMA
        void get_commit_cut(std::uint64_t &commit_cut)
        {
            void* staging = g_rdma.get_staging_buf();
            for (std::uint32_t i = 0; i < REPLICATOR_NUM; ++i)
            {
                g_rdma.read(staging, cut_region_offset_ + i * 64, sizeof(std::uint64_t));
                std::uint64_t cut_value;
                std::memcpy(&cut_value, staging, sizeof(std::uint64_t));
                std::uint32_t r_lsn = cut_value >> 32;
                std::uint32_t r_key = cut_value & 0xFFFFFFFF;
                static std::uint32_t last_rep_lsn[REPLICATOR_NUM] = {};
                if (r_lsn != last_rep_lsn[i]) {
                    LOG(INFO) << "[CUT] shard=" << shard_id_ << " rep=" << i
                              << " lsn=" << r_lsn << " key=" << r_key;
                    last_rep_lsn[i] = r_lsn;
                }
                commit_cut = std::min(commit_cut, cut_value);
            }
        }
#else
        void get_commit_cut(std::uint64_t &commit_cut)
        {
            if (SharedData::node_leases.expired_shards.load(std::memory_order_acquire) &
                (std::uint64_t{1} << shard_id_))
            {
                commit_cut = sealed_cut_;
                return;
            }
            clflushopt(cut_region_, sizeof(SharedData::CutRegion) * REPLICATOR_NUM);
            sfence();
            for (std::uint32_t i = 0; i < REPLICATOR_NUM; ++i)
            {
                // Load each replicator's cut region.
                std::uint64_t current_value = cut_region_[i].value.load(std::memory_order_acquire);
                commit_cut = std::min(commit_cut, current_value);
            }
            clflushopt(cut_region_, sizeof(SharedData::CutRegion) * REPLICATOR_NUM);
            // collect_report completes this post-read batch with the fence
            // before loading commit metadata, or before an early return.
        }
#endif
        // Decode key/tail deltas and update the candidate index.
        // delta_key_num is the number of key/tail entries consumed this round.
#ifndef USE_RDMA
        void record_consumed_position(std::uint64_t progress)
        {
            auto *metadata = commit_region_.commit_buffer_metadata;
            bool dirty = progress != sealed_cut_;
            if (dirty)
            {
                consumed_slot_ ^= 1;
                metadata->consumed[consumed_slot_].store(progress, std::memory_order_release);
            }
            const auto next = last_key_num_[current_candidate_index_] +
                static_cast<std::uint32_t>(static_cast<std::uint32_t>(progress) -
                                           static_cast<std::uint32_t>(last_key_num_[current_candidate_index_]));
            const auto reclaimed = SharedData::DeltaRing::reclaim_before(
                next, last_key_num_[1 - current_candidate_index_], sealed_position_);
            if (reclaimed != reclaimed_position_)
            {
                metadata->reclaimed.store(reclaimed, std::memory_order_release);
                reclaimed_position_ = reclaimed;
                dirty = true;
            }
            // Reclamation and the two recovery snapshots have one writer and
            // share its existing flush/fence, including index-only catch-up.
            if (dirty)
                clwb(metadata->consumed, CACHELINE_SIZE);
        }
#endif
        void parse_commit_buffer(std::uint32_t &delta_key_num, std::uint64_t progress)
        {
            CHECK(delta_key_num >= 0) << "delta_key_num should be at least 0";
#ifdef USE_RDMA
            // Under RDMA, data has already been read into local_commit_buffer_
            std::uint64_t *data_ptr = local_commit_buffer_ + last_key_num_[current_candidate_index_];
#else
            const auto position = last_key_num_[current_candidate_index_];
            const auto offset = SharedData::DeltaRing::offset(position);
            std::uint64_t *data_ptr = commit_region_.commit_buffer + offset;
            if (offset + delta_key_num > SharedData::DeltaRing::capacity) [[unlikely]]
            {
                // Preserve one parser invocation: a new-stream marker and its
                // address may occupy opposite ends, and aggregation spans both.
                SharedData::DeltaRing::copy_out(commit_region_.commit_buffer, position,
                                                delta_key_num, local_commit_buffer_);
                data_ptr = local_commit_buffer_;
            }
#endif
            if (WORKLOAD == 4 || (WORKLOAD == 1 && YCSB_OPTION == 3) || (WORKLOAD == 0 && SCALE_OUT_STREAM))
            {
                update_next_candidate_hash_table_aggregate_with_new_state_key(data_ptr, delta_key_num);
            }
            else
            {
                update_next_candidate_hash_table_aggregate(data_ptr, delta_key_num);
            }
#ifndef USE_RDMA
            // The existing index fence also completes the consumer position.
            // No cut can be published until every worker passes this fence.
            record_consumed_position(progress);
#endif
            sfence();
        }
        void update_next_candidate_hash_table_aggregate(std::uint64_t *data, std::uint32_t &key_num)
        {
            for (std::uint64_t i = 0; i < key_num; i++)
            {
#ifndef USE_RDMA
                if (static_cast<std::uint32_t>(data[i]) == 0)
                {
                    const StateKey key = data[i] >> 32;
                    update_map_.erase(key);
                    commit_tail_hash_table_candidate_[current_candidate_index_]->remove(key, shard_id_);
                    continue;
                }
#endif
                update_map_[data[i] >> 32] += data[i] & 0xFFFFFFFF;
            }
            for (auto &[state_key, tail] : update_map_)
            {
#ifdef USE_RDMA
                // Flat table: entry at base + state_key*sizeof(AllShardInfo) + shard_id*sizeof(ShardInfo)
                uint64_t entry_off = flat_table_offset_[current_candidate_index_]
                    + static_cast<uint64_t>(state_key) * sizeof(star::AllShardInfo)
                    + shard_id_ * sizeof(star::ShardInfo);
                void* staging = g_rdma.get_staging_buf();
                // Read current ShardInfo for this shard
                g_rdma.read(staging, entry_off, sizeof(star::ShardInfo));
                star::ShardInfo si;
                std::memcpy(&si, staging, sizeof(star::ShardInfo));
                // Apply delta (same semantics as CXL: cur_offset_ += delta)
                std::uint32_t old_offset = si.cur_offset_;
                si.cur_offset_ += tail;
                // Write back
                std::memcpy(staging, &si, sizeof(star::ShardInfo));
                g_rdma.write(staging, entry_off, sizeof(star::ShardInfo));
                // Validate end-to-end: use the updated index to locate the entry in
                // the RDMA stream and verify the entry header is valid.
                // Stream entry at stream_base + write_stream_begin_offset + old_offset
                // should contain a valid header: (lsn<<32)|payload_size
                {
                    // 1. Verify index readback
                    star::ShardInfo si_rb;
                    g_rdma.read(staging, entry_off, sizeof(star::ShardInfo));
                    std::memcpy(&si_rb, staging, sizeof(star::ShardInfo));
                    CHECK(si_rb.cur_offset_ == si.cur_offset_)
                        << "[RDMA-CHECK] worker " << shard_id_ << " index readback mismatch:"
                        << " key=" << state_key << " expected=" << si.cur_offset_
                        << " read=" << si_rb.cur_offset_;
                    // 2. Read entry header from stream at position where new data starts
                    // The entry is at: stream_base + write_stream_begin_offset + old_offset
                    uint64_t entry_addr = stream_base_offset_
                        + si.write_stream_begin_offset_
                        + old_offset;
                    std::uint64_t entry_hdr;
                    g_rdma.read(staging, entry_addr, sizeof(std::uint64_t));
                    std::memcpy(&entry_hdr, staging, sizeof(std::uint64_t));
                    std::uint32_t entry_lsn = entry_hdr >> 32;
                    std::uint32_t entry_psz = entry_hdr & 0xFFFFFFFF;
                    // If entry_hdr is the magic LINK_POINTER, the replicator crossed a
                    // segment boundary — skip validation for this entry.
                    if (entry_hdr != LINK_POINTER_MAGIC)
                    {
                        CHECK(entry_lsn > 0)
                            << "[RDMA-CHECK] worker " << shard_id_
                            << " stream entry at index has lsn=0 (stale/unwritten):"
                            << " key=" << state_key << " stream_off=0x" << std::hex << entry_addr
                            << " hdr=0x" << entry_hdr << std::dec
                            << " old_off=" << old_offset << " new_off=" << si.cur_offset_;
                        CHECK(entry_psz > 0 && entry_psz <= ENTRY_MAX_SIZE)
                            << "[RDMA-CHECK] worker " << shard_id_
                            << " stream entry has invalid payload_size=" << entry_psz
                            << " key=" << state_key << " lsn=" << entry_lsn
                            << " stream_off=0x" << std::hex << entry_addr << std::dec;
                    }
                }
#else
                commit_tail_hash_table_candidate_[current_candidate_index_]->update(state_key, shard_id_, 0, tail, shard_count_);
#endif
            }
            update_map_.clear();
        }
        void update_next_candidate_hash_table_aggregate_with_new_state_key(std::uint64_t *data, std::uint32_t &key_num)
        {
            StateKey state_key;
            std::uint32_t tail;
            std::uint64_t write_stream_offset;
            absl::flat_hash_map<StateKey, std::uint64_t> new_state_key_map;
            for (std::uint64_t i = 0; i < key_num; i++)
            {
                state_key = data[i] >> 32;
                tail = data[i] & 0xFFFFFFFF;
                if (tail ==
#ifdef USE_RDMA
                    0
#else
                    std::numeric_limits<std::uint32_t>::max() || tail == 0
#endif
                )
                {
#ifndef USE_RDMA
                    if (tail == 0)
                    {
                        update_map_.erase(state_key);
                        new_state_key_map.erase(state_key);
                        commit_tail_hash_table_candidate_[current_candidate_index_]->remove(state_key, shard_id_);
                        continue;
                    }
#endif
                    // This is a new state key.
                    i++;
                    write_stream_offset = data[i];
                    new_state_key_map[state_key] = write_stream_offset;
                }
                else
                {
                    update_map_[state_key] += tail;
                }
            }
            for (auto &[state_key, tail] : update_map_)
            {
#ifdef USE_RDMA
                uint64_t entry_off = flat_table_offset_[current_candidate_index_]
                    + static_cast<uint64_t>(state_key) * sizeof(star::AllShardInfo)
                    + shard_id_ * sizeof(star::ShardInfo);
                void* staging = g_rdma.get_staging_buf();
                g_rdma.read(staging, entry_off, sizeof(star::ShardInfo));
                star::ShardInfo si;
                std::memcpy(&si, staging, sizeof(star::ShardInfo));
                std::uint32_t old_offset = si.cur_offset_;
                if (new_state_key_map.contains(state_key))
                {
                    si.write_stream_begin_offset_ = new_state_key_map[state_key];
                }
                si.cur_offset_ += tail;
                std::memcpy(staging, &si, sizeof(star::ShardInfo));
                g_rdma.write(staging, entry_off, sizeof(star::ShardInfo));
                // Validate: use index to locate entry in stream and verify header
                {
                    uint64_t entry_addr = stream_base_offset_
                        + si.write_stream_begin_offset_
                        + old_offset;
                    std::uint64_t entry_hdr;
                    g_rdma.read(staging, entry_addr, sizeof(std::uint64_t));
                    std::memcpy(&entry_hdr, staging, sizeof(std::uint64_t));
                    std::uint32_t entry_lsn = entry_hdr >> 32;
                    std::uint32_t entry_psz = entry_hdr & 0xFFFFFFFF;
                    if (entry_hdr != LINK_POINTER_MAGIC)
                    {
                        CHECK(entry_lsn > 0)
                            << "[RDMA-CHECK] worker " << shard_id_
                            << " stream entry (new_key path) has lsn=0:"
                            << " key=" << state_key << " off=0x" << std::hex << entry_addr << std::dec;
                        CHECK(entry_psz > 0 && entry_psz <= ENTRY_MAX_SIZE)
                            << "[RDMA-CHECK] worker " << shard_id_
                            << " stream entry (new_key) invalid psz=" << entry_psz
                            << " key=" << state_key << " lsn=" << entry_lsn;
                    }
                }
#else
                if (new_state_key_map.contains(state_key))
                {
                    commit_tail_hash_table_candidate_[current_candidate_index_]->insert_with_new_state_key(state_key, shard_id_, new_state_key_map[state_key], tail, shard_count_);
                }
                else
                {
                    commit_tail_hash_table_candidate_[current_candidate_index_]->update(state_key, shard_id_, 0, tail, shard_count_);
                }
#endif
            }
            update_map_.clear();
        }
        void update_next_candidate_hash_table_one_by_one(std::uint64_t *data, std::uint32_t &key_num)
        {
            for (std::uint64_t i = 0; i < key_num; i++)
            {
                StateKey state_key = data[i] >> 32;
                std::uint32_t tail = data[i] & 0xFFFFFFFF;
                commit_tail_hash_table_candidate_[current_candidate_index_]->update(state_key, shard_id_, 0, tail, shard_count_);
            }
        }
    };
}
