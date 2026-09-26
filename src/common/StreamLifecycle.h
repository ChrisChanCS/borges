#pragma once

#include "ClusterCXL.h"
#include <map>

namespace SharedData
{
    struct alignas(CACHELINE_SIZE) StreamLifecycle
    {
        std::uint32_t shard = 0;
        std::uint32_t key = 0;
        std::uint32_t head = 0;
        std::uint32_t first_capacity = 0;
        std::uint32_t trim_lsn = 0;
        std::uint32_t deleted = 0;
        std::uint32_t reserved[10]{};
    };
    static_assert(sizeof(StreamLifecycle) == CACHELINE_SIZE);
    using StreamLifecycles = std::map<std::pair<std::uint32_t, std::uint32_t>, StreamLifecycle>;

    struct alignas(CACHELINE_SIZE) LifecycleHeader
    {
        std::uint64_t format = 1;
        std::uint64_t count = 0;
        std::uint64_t bytes = 0;
    };

    inline StreamLifecycles load_stream_lifecycles(const ClusterConfiguration &config, unsigned replicas = 3)
    {
        StreamLifecycles result;
        if (config.header.lifecycle[0] == 0 && config.header.lifecycle[1] == 0)
            return result;
        for (unsigned replica = 0; replica < 2; ++replica)
        {
            if (!(replicas & (1u << replica)) || config.header.lifecycle[replica] == 0)
                continue;
            auto *base = shared_pointer<std::byte>(config.header.lifecycle[replica]);
            clflushopt(base, sizeof(LifecycleHeader));
            sfence();
            LifecycleHeader header;
            std::memcpy(&header, base, sizeof(header));
            if (header.format != 1 || header.count > COMMIT_RING_BUFFER_CAPACITY ||
                header.bytes != sizeof(header) + header.count * sizeof(StreamLifecycle) + CACHELINE_SIZE)
                continue;
            RecordBuffer record(header.bytes);
            read_record(base, record.data(), record.size());
            if (record.checksum() != record_checksum(record.data(), record.size() - sizeof(std::uint64_t)))
                continue;
            auto *entries = reinterpret_cast<const StreamLifecycle *>(record.data() + sizeof(header));
            for (std::size_t i = 0; i < header.count; ++i)
            {
                const auto &entry = entries[i];
                if (entry.shard >= config.shards.size() || entry.head >= WRITE_STREAM_SIZE ||
                    entry.head % CACHELINE_SIZE || (entry.first_capacity == 0 && entry.head != 0) ||
                    entry.first_capacity % CACHELINE_SIZE || entry.first_capacity > WRITE_STREAM_SIZE - entry.head ||
                    entry.deleted > 1)
                    throw std::runtime_error("invalid stream lifecycle entry");
                result[{entry.shard, entry.key}] = entry;
            }
            return result;
        }
        throw std::runtime_error("no valid stream lifecycle snapshot survives");
    }

    inline void persist_stream_lifecycles(ClusterConfiguration &config, const StreamLifecycles &entries)
    {
        LifecycleHeader header;
        header.count = entries.size();
        header.bytes = sizeof(header) + entries.size() * sizeof(StreamLifecycle) + CACHELINE_SIZE;
        RecordBuffer record(header.bytes);
        std::memcpy(record.data(), &header, sizeof(header));
        auto *destination = record.data() + sizeof(header);
        for (const auto &[key, entry] : entries)
        {
            std::memcpy(destination, &entry, sizeof(entry));
            destination += sizeof(entry);
        }
        std::array<void *, 2> copies;
        for (unsigned replica = 0; replica < 2; ++replica)
        {
            copies[replica] = allocate_shared(record.size());
            config.header.lifecycle[replica] = shared_offset(copies[replica]);
        }
        persist_record(record, copies);
    }

    // Read once at shard startup, never consulted on the ordinary append path.
    inline StreamLifecycles startup_lifecycles;
}
