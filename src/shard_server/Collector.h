#pragma once
#include <cstdint>
#include <pthread.h>
#include <vector>
#include "../common/SharedData.h"
#include "View.h"

struct alignas(64) GSNRecord
{
    std::vector<std::uint32_t> gsn_list_;
    pthread_spinlock_t latch_;
    GSNRecord()
    {
        gsn_list_.reserve(1024);
        pthread_spin_init(&latch_, PTHREAD_PROCESS_PRIVATE);
    }
};

class GSNCollector
{
public:
#ifdef USE_RDMA
    uint64_t gsn_set_offset_;
    uint64_t round_number_offset_;
#else
    SharedData::GSNSet *gsn_set_;
    SharedData::GSNSet *backup_gsn_set_;
    SharedData::RoundNumber *round_number_;
    SharedData::GlobalCutConsumer *consumer_;
    std::uint64_t index_offsets_[COMMIT_TAIL_HASH_TABLE_CANDIDATE_NUM]{};
    std::uint64_t epoch_ = 0;
    SharedData::ClusterRegion *cluster_region_ = nullptr;
    std::vector<SharedData::ClusterConfiguration> configurations_;
    std::vector<std::array<star::CCHashTable *, 2>> indexes_;
    std::vector<std::unique_ptr<GSNRecord>> additional_records_;
    std::unique_ptr<SharedData::RecordBuffer> configured_snapshots_;
    std::size_t snapshot_stride_ = 0;
    std::function<void(GSNCollector &, const SharedData::ClusterConfiguration &)> configuration_observer_;
    // Reuse startup-allocated private snapshots and amortize the two required
    // CXL read fences when several published cuts arrive together.
    SharedData::GSNSet cut_snapshots_[16];
#endif
    std::uint32_t round_;
    std::uint32_t shard_id_;
    std::uint32_t thread_id_;
    GSNRecord gsn_record_[SHARD_SERVER_NUM];
    absl::Mutex gsn_record_mutex_;
    absl::flat_hash_map<std::uint32_t /*state_key*/, std::vector<std::uint32_t> /*start_round of each shard*/> round_map_;
    absl::Mutex round_map_mutex_;

    GSNCollector(std::uint32_t shard_id, std::uint32_t thread_id) : shard_id_(shard_id), thread_id_(thread_id)
    {
        init_shard_cxlalloc(REPLICATOR_NUM + 2, shard_id);
#ifdef USE_RDMA
        gsn_set_offset_ = g_rdma.get_root(GSN_BUFFER_ROOT_INDEX);
        round_number_offset_ = g_rdma.get_root(ROUND_NUMBER_ROOT_INDEX);
#else
        gsn_set_ = reinterpret_cast<SharedData::GSNSet *>(cxlalloc_get_root(GSN_BUFFER_ROOT_INDEX));
        backup_gsn_set_ = reinterpret_cast<SharedData::GSNSet *>(cxlalloc_get_root(GSN_BUFFER_ROOT_INDEX + 1));
        round_number_ = reinterpret_cast<SharedData::RoundNumber *>(cxlalloc_get_root(ROUND_NUMBER_ROOT_INDEX));
        cluster_region_ = static_cast<SharedData::ClusterRegion *>(cxlalloc_get_root(CLUSTER_ROOT_INDEX));
        CHECK(load_configurations(0)) << "initial cluster configuration is unavailable";
        CHECK_GE(thread_id_, REPLICATOR_NUM);
        CHECK_LT(thread_id_ - REPLICATOR_NUM, static_cast<std::uint32_t>(REQUEST_WORKER_NUM));
        consumer_ = SharedData::shared_pointer<SharedData::GlobalCutConsumer>(
            SharedData::shard_resources(shard_id_).global_cut_consumers) + thread_id_ - REPLICATOR_NUM;
        for (std::size_t i = 0; i < COMMIT_TAIL_HASH_TABLE_CANDIDATE_NUM; ++i)
            CHECK(cxlalloc_pointer_to_offset(cxlalloc_get_root(TAIL_HASH_TABLE_ROOT_INDEX + i), &index_offsets_[i]));
#endif
        round_ = 0;
        round_map_.reserve(KEY_CNT_OF_SHARD);
        for (std::uint32_t i = 0; i < KEY_CNT_OF_SHARD; i++)
        {
            round_map_[i] = std::vector<std::uint32_t>(SHARD_SERVER_NUM, 0);
        }
        LOG(INFO) << "init GSNCollector";
    }

    ~GSNCollector()
    {
    }

    GSNRecord &record(std::size_t shard)
    {
#ifndef USE_RDMA
        if (shard >= SHARD_SERVER_NUM)
            return *additional_records_.at(shard - SHARD_SERVER_NUM);
#endif
        return gsn_record_[shard];
    }

#ifndef USE_RDMA
    bool load_configurations(std::uint64_t through_epoch)
    {
        if (through_epoch > std::numeric_limits<std::uint32_t>::max())
            return false;
        while (configurations_.size() <= through_epoch)
        {
            SharedData::ClusterConfiguration next;
            if (!SharedData::load_configuration(cluster_region_, configurations_.size(), next))
                return false;
            if (next.header.request_workers != static_cast<std::uint64_t>(REQUEST_WORKER_NUM))
                return false;
            if (!configurations_.empty())
            {
                const auto &previous = configurations_.back().header;
                if (next.header.first_round < previous.first_round ||
                    next.header.shard_count < previous.shard_count ||
                    next.header.shard_count > previous.shard_count + 1 ||
                    next.header.cut_offset != previous.cut_offset +
                        (next.header.first_round - previous.first_round) * SharedData::global_cut_bytes(previous.shard_count))
                    return false;
            }
            while (SHARD_SERVER_NUM + additional_records_.size() < next.shards.size())
                additional_records_.push_back(std::make_unique<GSNRecord>());
            indexes_.push_back({SharedData::shared_pointer<star::CCHashTable>(next.header.index[0]),
                                SharedData::shared_pointer<star::CCHashTable>(next.header.index[1])});
            configurations_.push_back(std::move(next));
            // Replica addresses change only at an epoch boundary. In degraded
            // mode a checksum fallback must not touch the failed device.
            const auto primary = SharedData::read_cxl_word(cluster_region_->heads[0].cut_log);
            const auto backup = SharedData::read_cxl_word(cluster_region_->heads[1].cut_log);
            gsn_set_ = primary ? SharedData::shared_pointer<SharedData::GSNSet>(primary) :
                static_cast<SharedData::GSNSet *>(cxlalloc_get_root(GSN_BUFFER_ROOT_INDEX));
            backup_gsn_set_ = (configurations_.back().header.replica_mask & 2) ?
                (backup ? SharedData::shared_pointer<SharedData::GSNSet>(backup) :
                 static_cast<SharedData::GSNSet *>(cxlalloc_get_root(GSN_BUFFER_ROOT_INDEX + 1))) : gsn_set_;
            if (configuration_observer_)
                configuration_observer_(*this, configurations_.back());
        }
        return true;
    }

    const SharedData::ClusterConfiguration &configuration_for_switch(std::uint64_t value)
    {
        const auto epoch = value >> 32;
        if (epoch >= configurations_.size())
            CHECK(load_configurations(epoch)) << "index names an unavailable cluster configuration";
        return configurations_[epoch];
    }

    star::CCHashTable *index_for_switch(std::uint64_t value)
    {
        const auto epoch = value >> 32;
        if (epoch >= indexes_.size())
            CHECK(load_configurations(epoch));
        return indexes_[epoch][value & 1];
    }

    std::uint32_t local_lsn()
    {
        const auto &history = record(shard_id_).gsn_list_;
        return history.empty() ? 0 : history.back();
    }

    std::uint32_t collect_configured(std::uint64_t current_round)
    {
        CHECK_GE(current_round, round_);
        const auto before = round_;
        while (round_ < current_round)
        {
            const auto first = std::uint64_t(round_) + 1;
            auto config_epoch = epoch_;
            while (config_epoch + 1 < configurations_.size() &&
                   configurations_[config_epoch + 1].header.first_round <= first)
                ++config_epoch;
            const auto &config = configurations_[config_epoch];
            const auto stride = SharedData::global_cut_bytes(config.shards.size());
            if (stride != snapshot_stride_)
            {
                configured_snapshots_ = std::make_unique<SharedData::RecordBuffer>(16 * stride);
                snapshot_stride_ = stride;
            }
            const auto end = config_epoch + 1 < configurations_.size() ?
                std::min(current_round + 1, configurations_[config_epoch + 1].header.first_round) : current_round + 1;
            const auto count = std::min<std::uint64_t>(16, end - first);
            const auto offset = config.header.cut_offset + (first - config.header.first_round) * stride;
            if (offset + count * stride > std::uint64_t(GSN_SET_CNT) * sizeof(SharedData::GSNSet))
                break;
            auto *source = reinterpret_cast<const std::byte *>(gsn_set_) + offset;
            clflushopt(source, count * stride);
            sfence();
            for (std::size_t i = 0; i < count; ++i)
            {
                auto *target = configured_snapshots_->data() + i * stride;
                std::memcpy(target, source + i * stride, stride - sizeof(std::uint64_t));
                const auto *marker = reinterpret_cast<const std::uint64_t *>(source + (i + 1) * stride - sizeof(std::uint64_t));
                const auto checksum = std::atomic_ref<const std::uint64_t>(*marker).load(std::memory_order_acquire);
                std::memcpy(target + stride - sizeof(checksum), &checksum, sizeof(checksum));
            }
            clflushopt(source, count * stride);
            sfence();
            for (std::size_t i = 0; i < count; ++i)
            {
                auto *snapshot = configured_snapshots_->data() + i * stride;
                const auto round = first + i;
                if (!SharedData::valid_dynamic_cut(snapshot, stride, round, config.shards.size()))
                {
                    SharedData::read_record(reinterpret_cast<const std::byte *>(backup_gsn_set_) + offset + i * stride,
                                            snapshot, stride);
                    if (!SharedData::valid_dynamic_cut(snapshot, stride, round, config.shards.size()))
                        return round_ == before ? 0 : local_lsn();
                }
                const auto &header = *reinterpret_cast<const SharedData::GlobalCutHeader *>(snapshot);
                if (header.epoch != config_epoch ||
                    (header.index_offset != config.header.index[0] && header.index_offset != config.header.index[1]))
                    return round_ == before ? 0 : local_lsn();
                const auto *entries = reinterpret_cast<const SharedData::GlobalCutShard *>(snapshot + sizeof(header));
                for (std::size_t shard = 0; shard < config.shards.size(); ++shard)
                {
                    auto &history = record(shard).gsn_list_;
                    if (!history.empty() && entries[shard].value < history.back())
                        return round_ == before ? 0 : local_lsn();
                }
                for (std::size_t shard = 0; shard < config.shards.size(); ++shard)
                {
                    auto &history = record(shard).gsn_list_;
                    if (history.size() < round - 1)
                        history.resize(round - 1, 0);
                    history.push_back(entries[shard].value);
                }
                epoch_ = config_epoch;
                round_ = round;
            }
        }
        return round_ == before || record(shard_id_).gsn_list_.empty() ? 0 : record(shard_id_).gsn_list_.back();
    }
#endif

    // Return false if the LSN has no known global round.
    bool get_round_by_state_key(const std::uint32_t &lsn, const std::uint32_t &shard_id, std::uint32_t &round, std::uint32_t &state_key)
    {
        std::uint32_t start_round;
        {
            absl::MutexLock lock(&gsn_record_mutex_);
            auto &starts = round_map_[state_key];
            if (starts.size() <= shard_id)
                starts.resize(shard_id + 1, 0);
            start_round = starts[shard_id];
        }
        auto it = std::lower_bound(record(shard_id).gsn_list_.begin() + start_round, record(shard_id).gsn_list_.end(), lsn);
        if (it == record(shard_id).gsn_list_.end())
        {
            // The index can be visible before this collector knows its cut.
            // YCSB catches up before replaying to the captured index boundary.
            return false;
        }
        round = it - record(shard_id).gsn_list_.begin();
        {
            absl::MutexLock lock(&round_map_mutex_);
            round_map_[state_key][shard_id] = round;
        }
        return true;
    }

    bool get_round(const std::uint32_t &lsn, const std::uint32_t &shard_id, std::uint32_t &round)
    {
        auto it = std::lower_bound(record(shard_id).gsn_list_.begin(), record(shard_id).gsn_list_.end(), lsn);
        if (it == record(shard_id).gsn_list_.end())
        {
            // The index is published before the cut; an entry can be visible before its LSN is recorded locally.
            // That entry is beyond the current read's global order, so defer it.
            return false;
        }
        round = it - record(shard_id).gsn_list_.begin();
        return true;
    }

    // Collect newly published cuts and return the highest newly committed local LSN.
    template <bool ObserveNewConfigurations = true>
    inline std::uint32_t collect()
    {
#ifdef USE_RDMA
        void* staging = g_rdma.get_staging_buf();
        g_rdma.read(staging, round_number_offset_, sizeof(SharedData::RoundNumber));
        SharedData::RoundNumber local_rn;
        std::memcpy(&local_rn, staging, sizeof(SharedData::RoundNumber));
        std::uint64_t current_round = local_rn.round.load(std::memory_order_relaxed);
#else
        clflushopt(round_number_, sizeof(*round_number_));
        sfence();
        const auto publication = round_number_->round.load(std::memory_order_acquire);
        std::uint64_t current_round = static_cast<std::uint32_t>(publication);
        const auto published_epoch = publication >> 32;
        clflushopt(round_number_, sizeof(*round_number_));
        if (current_round >= GSN_SET_CNT)
        {
            sfence();
            CHECK(false) << "global cut producer is outside the allocated buffer";
        }
        // A new cut's pre-read fence also completes this post-read flush. If
        // there is no new cut, complete it before returning instead.
        if (current_round <= round_)
            sfence();
#endif
#ifndef USE_RDMA
        if (published_epoch != 0)
        {
            sfence();
            if constexpr (ObserveNewConfigurations)
            {
                if (!load_configurations(published_epoch))
                    return 0;
            }
            else if (published_epoch >= configurations_.size())
            {
                // An in-flight replay only needs cuts for its captured index.
                // Leave newer epochs to the worker loop: their callbacks may
                // park readers and invalidate views during maintenance.
                SharedData::ClusterConfiguration next;
                if (!SharedData::load_configuration(cluster_region_, configurations_.size(), next))
                    return 0;
                current_round = std::min(current_round, next.header.first_round - 1);
            }
            const auto previous_round = round_;
            const auto result = collect_configured(current_round);
            if (round_ != previous_round)
                consumer_->publish(epoch_, round_);
            return result;
        }
#endif
        if (current_round > round_)
        {
#ifdef USE_RDMA
            // Read all new GSN sets in one batch
            for (std::uint32_t i = round_ + 1; i <= current_round; i++)
            {
                g_rdma.read(staging, gsn_set_offset_ + i * sizeof(SharedData::GSNSet), sizeof(SharedData::GSNSet));
                SharedData::GSNSet local_gsn;
                std::memcpy(&local_gsn, staging, sizeof(SharedData::GSNSet));
                for (std::uint32_t j = 0; j < SHARD_SERVER_NUM; j++)
                {
                    // Validate: GSN values must be monotonically non-decreasing
                    std::uint64_t new_val = local_gsn.gsn[j].value;
                    if (!record(j).gsn_list_.empty())
                    {
                        std::uint64_t prev_val = record(j).gsn_list_.back();
                        CHECK(new_val >= prev_val)
                            << "[RDMA-CHECK] Collector GSN[" << j << "] decreased at round=" << i
                            << ": prev=" << prev_val << " new=" << new_val;
                    }
                    record(j).gsn_list_.push_back(new_val);
                }
            }
#else
            std::uint64_t validated_round = round_;
            std::size_t buffered = 0, cursor = 0;
            for (std::uint32_t i = round_ + 1; i <= current_round; ++i)
            {
                if (cursor == buffered)
                {
                    buffered = std::min<std::size_t>(std::size(cut_snapshots_), current_round - i + 1);
                    SharedData::read_global_cuts(gsn_set_ + i, cut_snapshots_, buffered);
                    cursor = 0;
                }
                auto &cut = cut_snapshots_[cursor++];
                if (!cut.valid(i))
                {
                    cut = SharedData::read_global_cut(backup_gsn_set_ + i);
                    if (!cut.valid(i))
                        break; // retry the torn/unavailable tail; never acknowledge it
                }
                bool matching_index = false;
                for (auto offset : index_offsets_)
                    matching_index |= offset == cut.header.index_offset;
                if (!matching_index || cut.header.epoch < epoch_)
                    break;
                bool monotonic = true;
                for (std::uint32_t j = 0; j < SHARD_SERVER_NUM; ++j)
                    if (!record(j).gsn_list_.empty() && cut.gsn[j].value < record(j).gsn_list_.back())
                        monotonic = false;
                if (!monotonic)
                    break;
                for (std::uint32_t j = 0; j < SHARD_SERVER_NUM; ++j)
                {
                    record(j).gsn_list_.push_back(cut.gsn[j].value);
                }
                epoch_ = cut.header.epoch;
                validated_round = i;
            }
            if (validated_round == round_)
                return 0;
            current_round = validated_round;
#endif
            std::uint32_t committed_lsn = record(shard_id_).gsn_list_.back();

            round_ = current_round;
#ifndef USE_RDMA
            consumer_->publish(epoch_, round_);
#endif
            return committed_lsn; // Highest newly committed local LSN.
        }
        else if (current_round < round_)
        {
            LOG(ERROR) << "current_round in cxl: " << current_round << " should not be smaller than local round: " << round_;
            exit(-1);
            return 0;
        }
        else
        {
            return 0;
        }
    }

    inline std::uint32_t get_global_cut_round()
    {
        return round_;
    }
};
