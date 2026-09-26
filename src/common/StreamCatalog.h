#pragma once

#include "ClusterCXL.h"

namespace SharedData
{
    // A single backup replicator owns this append-only directory. Four entries
    // share a cache line; no other writer touches that line. Existing-stream
    // appends do not access the directory.
    struct StreamCatalogEntry
    {
        std::uint32_t key = 0;
        std::uint32_t head = 0;
        std::uint32_t lsn = 0;
        std::uint32_t checksum = 0;

        std::uint32_t crc() const
        {
            const std::uint64_t words[] = {(std::uint64_t(head) << 32) | key, lsn};
            const auto value = static_cast<std::uint32_t>(record_checksum(words, sizeof(words)));
            return value == 0 ? 1 : value;
        }
        bool valid() const { return lsn != 0 && head % CACHELINE_SIZE == 0 && checksum == crc(); }
    };
    static_assert(sizeof(StreamCatalogEntry) == 16);
    inline constexpr std::size_t stream_catalog_capacity = COMMIT_RING_BUFFER_CAPACITY / 2;
    inline constexpr std::size_t stream_catalog_bytes = stream_catalog_capacity * sizeof(StreamCatalogEntry);

    inline std::uint64_t allocate_stream_catalog()
    {
        auto *memory = allocate_shared(stream_catalog_bytes);
        std::memset(memory, 0, stream_catalog_bytes);
        clflushopt(memory, stream_catalog_bytes);
        sfence();
        return shared_offset(memory);
    }

    inline void append_stream_catalog(StreamCatalogEntry *catalog, std::size_t &cursor,
                                      std::uint32_t key, std::uint32_t head, std::uint32_t lsn)
    {
        if (cursor >= stream_catalog_capacity)
            throw std::runtime_error("stream catalog capacity exhausted");
        StreamCatalogEntry entry{key, head, lsn, 0};
        entry.checksum = entry.crc();
        std::memcpy(catalog + cursor, &entry, sizeof(entry));
        ++cursor;
        // The caller flushes the appended range once per batch, avoiding repeated
        // eviction of the same line for its four entries. Its existing local-cut
        // publication fence persists the directory with the new stream records.
    }

    template <typename Visitor>
    std::size_t visit_stream_catalog(std::uint64_t offset, std::uint32_t lsn, Visitor &&visitor)
    {
        auto *catalog = shared_pointer<StreamCatalogEntry>(offset);
        std::size_t cursor = 0;
        for (; cursor < stream_catalog_capacity; ++cursor)
        {
            if (cursor % (CACHELINE_SIZE / sizeof(StreamCatalogEntry)) == 0)
            {
                clflushopt(catalog + cursor, CACHELINE_SIZE);
                sfence();
            }
            const auto entry = catalog[cursor];
            if (!entry.valid() || entry.lsn > lsn)
                break;
            visitor(entry);
        }
        return cursor;
    }
}
