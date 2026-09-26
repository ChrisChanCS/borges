#pragma once

#include "GlobalCut.h"
#include "PersistentRecord.h"
#include "Lease.h"
#include <vector>

namespace SharedData
{
    // This is an allocator/role-ID bound, not a reservation of shard slots.
    // A sequencer uses allocator threads 0..7; its main thread uses slot 9.
    inline constexpr std::uint64_t max_cluster_shards = 8;
    inline constexpr std::uint64_t reconfiguration_pending = std::uint64_t{1} << 63;

    constexpr std::uint64_t shard_mask(std::size_t count)
    {
        return count == 64 ? ~std::uint64_t{0} : (std::uint64_t{1} << count) - 1;
    }

    constexpr std::size_t global_cut_bytes(std::size_t shards)
    {
        return cache_line_size(sizeof(GlobalCutHeader) +
            shards * sizeof(GlobalCutShard) + sizeof(std::uint64_t));
    }

    struct alignas(CACHELINE_SIZE) ShardResources
    {
        std::uint64_t write_stream[2]{};
        std::uint64_t read_stream[2]{};
        std::uint64_t commit_buffer = 0;
        std::uint64_t commit_metadata = 0;
        std::uint64_t local_cuts = 0;
        std::uint64_t reader_flags = 0;
        std::uint64_t lease = 0;
        std::uint64_t stream_catalog = 0; // Backup's immutable dynamic-stream directory.
        std::uint64_t resume_cut = 0;     // Restored LSN and delta cursor for this incarnation.
        std::uint64_t global_cut_consumers = 0; // One cache line per request worker.
        std::uint64_t reserved[4]{};
    };
    static_assert(sizeof(ShardResources) == 2 * CACHELINE_SIZE);

    struct alignas(CACHELINE_SIZE) ClusterHeader
    {
        std::uint64_t format = 4;
        std::uint64_t epoch = 0;
        std::uint64_t first_round = 1;
        std::uint64_t cut_offset = CACHELINE_SIZE;
        std::uint64_t shard_count = 0;
        std::uint64_t active_shards = 0;
        std::uint64_t index[2]{};
        std::uint64_t previous[2]{};
        std::uint64_t record_bytes = 0;
        std::uint64_t request_workers = 0;
        std::uint64_t replica_mask = 3;
        std::uint64_t lifecycle[2]{};
        std::uint64_t settings = 0;
    };
    static_assert(sizeof(ClusterHeader) == 2 * CACHELINE_SIZE);

    // Each head has one sequencer writer. A head advertises prepared metadata;
    // only the global-cut producer makes the named configuration effective.
    struct alignas(CACHELINE_SIZE) ClusterHead
    {
        std::atomic<std::uint64_t> offset{0};
        // Fills previously unused bytes in this sequencer-owned cache line.
        // Existing format-4 regions leave it zero and use the bootstrap root.
        std::atomic<std::uint64_t> cut_log{0};
    };
    struct alignas(CACHELINE_SIZE) ClusterRegion
    {
        ClusterHead heads[2];
    };
    static_assert(sizeof(ClusterHead) == CACHELINE_SIZE);

    struct ClusterConfiguration
    {
        ClusterHeader header;
        std::vector<ShardResources> shards;
        std::uint64_t offsets[2]{};

        static std::size_t bytes(std::size_t count)
        {
            return sizeof(ClusterHeader) + count * sizeof(ShardResources) + CACHELINE_SIZE;
        }

        RecordBuffer encode() const
        {
            RecordBuffer record(bytes(shards.size()));
            auto h = header;
            h.shard_count = shards.size();
            h.record_bytes = record.size();
            std::memcpy(record.data(), &h, sizeof(h));
            std::memcpy(record.data() + sizeof(h), shards.data(), shards.size() * sizeof(ShardResources));
            return record;
        }

        static bool valid_header(const ClusterHeader &h)
        {
            return h.format == 4 && h.shard_count > 0 && h.shard_count <= max_cluster_shards &&
                h.record_bytes == bytes(h.shard_count) && h.first_round > 0 &&
                h.cut_offset % CACHELINE_SIZE == 0 && h.index[0] != 0 && h.index[1] != 0 &&
                h.index[0] % CACHELINE_SIZE == 0 && h.index[1] % CACHELINE_SIZE == 0 &&
                h.request_workers > 0 && h.replica_mask > 0 && h.replica_mask <= 3 &&
                (h.active_shards & ~shard_mask(h.shard_count)) == 0;
        }
    };

    inline bool decode_configuration(const RecordBuffer &record, ClusterConfiguration &config)
    {
        if (record.size() < ClusterConfiguration::bytes(1) || record.checksum() == 0 ||
            record.checksum() != record_checksum(record.data(), record.size() - sizeof(std::uint64_t)))
            return false;
        ClusterHeader h;
        std::memcpy(&h, record.data(), sizeof(h));
        if (!ClusterConfiguration::valid_header(h) || h.record_bytes != record.size())
            return false;
        config.header = h;
        config.shards.resize(h.shard_count);
        std::memcpy(config.shards.data(), record.data() + sizeof(h), h.shard_count * sizeof(ShardResources));
        for (const auto &shard : config.shards)
        {
            const std::uint64_t fields[] = {shard.write_stream[0], shard.write_stream[1],
                shard.read_stream[0], shard.read_stream[1], shard.commit_buffer, shard.commit_metadata,
                shard.local_cuts, shard.reader_flags, shard.lease, shard.global_cut_consumers};
            for (auto offset : fields)
                if (offset == 0 || offset % CACHELINE_SIZE != 0)
                    return false;
        }
        return true;
    }

    inline bool valid_dynamic_cut(const void *data, std::size_t bytes, std::uint64_t round, std::size_t shards)
    {
        if (shards == 0 || shards > max_cluster_shards || bytes != global_cut_bytes(shards))
            return false;
        const auto *source = static_cast<const std::byte *>(data);
        const auto &header = *reinterpret_cast<const GlobalCutHeader *>(source);
        std::uint64_t checksum;
        std::memcpy(&checksum, source + bytes - sizeof(checksum), sizeof(checksum));
        if (round == 0 || header.round != round || header.index_offset == 0 ||
            checksum == 0 ||
            checksum != record_checksum(data, bytes - sizeof(std::uint64_t)))
            return false;
        const auto *lsns = reinterpret_cast<const GlobalCutShard *>(source + sizeof(header));
        for (std::size_t i = 0; i < shards; ++i)
            if (lsns[i].shard_id != i)
                return false;
        return true;
    }
}
