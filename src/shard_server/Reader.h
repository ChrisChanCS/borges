#pragma once

#include <cstdint>
#include <pthread.h>
#include "absl/container/btree_map.h"
#include "View.h"
#include "../common/SharedData.h"
#include <list>
#include <memory>
#include "Collector.h"

template <typename T>
class Reader
{
public:
    std::uint8_t field_id_;
    bool ready_to_read_ = false;
    std::uint32_t min_lsn_ = std::numeric_limits<std::uint32_t>::max();
    absl::Mutex mu_;
    char *value_ = nullptr;
    std::unique_ptr<char[]> recovered_value_;

    Reader(std::uint32_t min_lsn, std::uint8_t field_id)
        : field_id_(field_id), min_lsn_(min_lsn)
    {
    }
    Reader()
    {
    }
    ~Reader()
    {
    }
    void try_update_reader_by_lsn(const std::uint8_t &field_id, const std::uint32_t &lsn, const std::uint32_t &request_id, const std::uint32_t &client_id)
    {
        absl::MutexLock lock(&mu_);
        if (min_lsn_ > lsn)
        {
            field_id_ = field_id;
            min_lsn_ = lsn;
        }
    }
};

template <typename T, typename F>
class ReaderManager
{
private:
    std::uint32_t shard_id_;
    absl::Mutex mu_;

    // Reader state indexed by operation ID.
    absl::flat_hash_map<OperationId, std::shared_ptr<Reader<T>>> operation_id_to_reader_;

    // Reader_pool<T> *reader_pool_;
    View::ViewManager<T> view_manager_;

public:
    ReaderManager(std::uint32_t shard_id, WorkloadType workload_type) : shard_id_(shard_id),
                                                                        // reader_pool_(new Reader_pool<T>()),
                                                                        view_manager_(shard_id, workload_type)
    {
#ifndef USE_RDMA
        if constexpr (std::is_same_v<T, star::retwis::State>)
            if (SharedData::recovered_shard)
                restore_readers();
#endif
    }

#ifndef USE_RDMA
    // The read log already stores operation IDs and LSNs. Rebuild only their
    // earliest association; replay the requested key lazily when it is retried.
    void restore_readers()
    {
        const auto tail = SharedData::recovered_shard->read_tail;
        auto *base = SharedData::shared_pointer<char>(SharedData::shard_resources(shard_id_).read_stream[0]);
        std::map<std::uint32_t, std::uint32_t> legacy_deletions;
        const auto &configuration = SharedData::startup_configuration;
        const auto history = SharedData::configuration_history(
            static_cast<SharedData::ClusterRegion *>(cxlalloc_get_root(CLUSTER_ROOT_INDEX)),
            configuration.header.replica_mask);
        for (const auto &entry : history)
        {
            if (entry.header.epoch > configuration.header.epoch)
                break;
            for (const auto &[identity, lifecycle] : SharedData::load_stream_lifecycles(
                     entry, configuration.header.replica_mask))
                if (lifecycle.shard == shard_id_ && lifecycle.deleted)
                    legacy_deletions.emplace(lifecycle.trim_lsn, lifecycle.key);
        }
        alignas(CACHELINE_SIZE) char snapshot[CACHELINE_SIZE];
        for (std::uint32_t offset = 0; offset < tail;)
        {
            SharedData::read_record(base + offset, snapshot, sizeof(snapshot));
            std::uint64_t header;
            LogPayload payload;
            std::memcpy(&header, snapshot, sizeof(header));
            std::memcpy(&payload, snapshot + sizeof(header), sizeof(payload));
            const auto bytes = std::uint64_t(static_cast<std::uint32_t>(header)) + sizeof(header);
            CHECK_GE(bytes, CACHELINE_SIZE);
            CHECK_LE(bytes, tail - offset);
            const auto lsn = static_cast<std::uint32_t>(header >> 32);
            const auto legacy = legacy_deletions.find(lsn);
            const bool placeholder = DeletionRecord::matches(snapshot) ||
                (legacy != legacy_deletions.end() &&
                 DeletionRecord::matches_legacy(snapshot, lsn, legacy->second));
            if (!placeholder)
            {
                CHECK(payload.state_op_type_ == StateOpType::kRead);
                auto field = static_cast<std::uint8_t>(snapshot[sizeof(header) + sizeof(payload)]);
                auto read_lsn = lsn;
                std::uint32_t unused = 0;
                receive_reader(field, read_lsn, unused, unused, payload.operation_id_);
            }
            offset += bytes;
        }
    }
#endif

    void receive_reader(std::uint8_t &field_id, std::uint32_t &lsn, std::uint32_t &client_id, std::uint32_t &request_id, OperationId &operation_id)
    {
        std::shared_ptr<Reader<T>> reader;
        {
            absl::MutexLock lock(&mu_);
            auto it = operation_id_to_reader_.find(operation_id);
            if (it == operation_id_to_reader_.end())
            {
                // Create a reader if this operation has no reader yet.
                reader = std::make_shared<Reader<T>>();
                operation_id_to_reader_[operation_id] = reader;
            }
            else
            {
                reader = it->second;
            }
        }
        reader->try_update_reader_by_lsn(field_id, lsn, request_id, client_id);
    }

#ifndef USE_RDMA
    // Readers are parked at an epoch boundary. Preserve any cached post result
    // that still points into a segment about to become reusable.
    void preserve_cxl_read_results()
    {
        if constexpr (std::is_same_v<T, star::retwis::State>)
            for (auto &[operation, reader] : operation_id_to_reader_)
            {
                if (!reader->ready_to_read_ || reader->recovered_value_)
                    continue;
                const auto address = reinterpret_cast<std::uintptr_t>(reader->value_);
                for (const auto &shard : SharedData::startup_configuration.shards)
                {
                    const auto base = reinterpret_cast<std::uintptr_t>(SharedData::shared_pointer<char>(shard.write_stream[0]));
                    if (address >= base && address - base < WRITE_STREAM_SIZE)
                    {
                        reader->recovered_value_ = std::make_unique<char[]>(CONTENT_SIZE);
                        clflushopt(reader->value_, CONTENT_SIZE);
                        sfence();
                        std::memcpy(reader->recovered_value_.get(), reader->value_, CONTENT_SIZE);
                        reader->value_ = reader->recovered_value_.get();
                        break;
                    }
                }
            }
    }
    void invalidate_stream(StateKey key) { view_manager_.invalidate_stream(key); }
#endif

    std::shared_ptr<Reader<T>> get_reader(const OperationId &operation_id)
    {
        absl::MutexLock lock(&mu_);
        return operation_id_to_reader_[operation_id];
    }

    View::View<T> *get_view(const StateKey &state_key)
    {
        return view_manager_.get_view(state_key);
    }

    void create_view_for_new_state_key(StateKey &state_key, std::uint64_t &offset)
    {
        view_manager_.create_view_for_new_state_key(state_key, offset);
    }
};
