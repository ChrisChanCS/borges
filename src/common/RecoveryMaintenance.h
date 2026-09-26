#pragma once

#include "Recovery.h"

namespace SharedData
{
    struct RecoveryOptions
    {
        unsigned survivors = 3;
        unsigned rebuild = 0;
        bool trim = false;
        bool erase = false;
        std::uint32_t shard = 0;
        std::uint32_t key = 0;
        std::uint32_t lsn = 0;
    };

    inline void apply_lifecycle(RecoveredStream &stream, const StreamLifecycle &entry)
    {
        stream.head = entry.head;
        stream.first_capacity = entry.first_capacity;
        stream.trim_lsn = entry.trim_lsn;
        stream.initial_segments = entry.first_capacity ? 1 : 0;
    }

    // Called only after writers have stopped at a committed boundary.
    inline void append_deletion(const ClusterConfiguration &config, RecoveredShard &state,
                                 std::size_t shard, StateKey key, std::uint32_t lsn)
    {
        const auto cursor = static_cast<std::uint32_t>(state.progress);
        if (state.read_tail + CACHELINE_SIZE > READ_STREAM_SIZE)
            throw std::runtime_error("deletion control log exhausted");
        const DeletionRecord record(lsn, key);
        const auto &resources = config.shards.at(shard);
        auto *metadata = shared_pointer<CommitBufferMetadata>(resources.commit_metadata);
        const auto position = DeltaRing::restore_position(read_cxl_word(metadata->write_position), cursor);
        for (unsigned replica = 0; replica < 2; ++replica)
            if (config.header.replica_mask & (1u << replica))
            {
                auto *destination = shared_pointer<char>(resources.read_stream[replica]) + state.read_tail;
                std::memcpy(destination, &record, sizeof(record));
                clflushopt(destination, sizeof(record));
            }
        auto *delta = shared_pointer<std::uint64_t>(resources.commit_buffer) + DeltaRing::offset(position);
        *delta = std::uint64_t(key) << 32;
        clflushopt(delta, sizeof(*delta));
        sfence();
        metadata->write_position.store(position + 1, std::memory_order_relaxed);
        clwb(&metadata->write_position, sizeof(metadata->write_position));
        sfence();
        state.read_tail += CACHELINE_SIZE;
        state.progress = (std::uint64_t(lsn) << 32) | (cursor + 1);
    }

    // The backup's log and stream directory are sufficient; no primary index,
    // delta buffer, local cut, or reader announcement is read here.
    inline RecoveredShard recover_shard_from_log(const RecoveryCut &cut, std::size_t shard,
                                                std::size_t replica, const StreamLifecycles &lifecycles)
    {
        RecoveredShard result;
        result.progress = cut.progress.at(shard);
        const auto lsn = static_cast<std::uint32_t>(result.progress >> 32);
        const auto &resources = cut.configuration.shards.at(shard);
        auto *base = shared_pointer<char>(resources.write_stream[replica]);
        auto add = [&](StateKey key)
        {
            auto stream = initial_stream(key);
            if (auto entry = lifecycles.find({shard, key}); entry != lifecycles.end())
                apply_lifecycle(stream, entry->second);
            result.streams[key] = std::move(stream);
        };
        if (WORKLOAD == 0)
            add(0);
        else if (WORKLOAD >= 1 && WORKLOAD <= 3)
            for (StateKey key = 0; key < KEY_CNT_OF_SHARD; ++key)
                add(key);
        else if (WORKLOAD == 4)
            for (StateKey key = USER_CNT; key < 2 * USER_CNT; ++key)
                add(key);
        result.catalog_cursor = visit_stream_catalog(resources.stream_catalog, lsn,
            [&](const StreamCatalogEntry &entry)
            {
                auto stream = initial_stream(entry.key);
                stream.head = entry.head;
                stream.initial_segments = 1;
                // Deletion retains an empty writer segment, so appending again
                // keeps the lifecycle head/capacity even when its first retained
                // record is newer than the trim marker.
                if (auto life = lifecycles.find({shard, entry.key}); life != lifecycles.end())
                    apply_lifecycle(stream, life->second);
                result.streams[entry.key] = std::move(stream);
            });
        for (const auto &[identity, entry] : lifecycles)
            if (entry.shard == shard && !result.streams.contains(entry.key))
                add(entry.key);
        for (auto &[key, stream] : result.streams)
            recover_stream(base, stream, lsn);
        result.read_tail = recover_read_tail(shared_pointer<char>(resources.read_stream[replica]), lsn);
        return result;
    }

    inline void copy_persistent_prefix(void *destination, const void *source, std::size_t bytes)
    {
        auto *to = static_cast<std::byte *>(destination);
        auto *from = static_cast<const std::byte *>(source);
        constexpr std::size_t chunk_bytes = 1024 * 1024;
        while (bytes)
        {
            const auto count = std::min(bytes, chunk_bytes);
            clflushopt(from, count);
            sfence();
            std::memcpy(to, from, count);
            clflushopt(to, count);
            to += count;
            from += count;
            bytes -= count;
        }
        sfence();
    }

    inline void rebuild_shard_replica(ShardResources &resources, const RecoveredShard &state,
                                      unsigned source_replica, unsigned target_replica)
    {
        auto *source = shared_pointer<char>(resources.write_stream[source_replica]);
        auto *target = static_cast<char *>(allocate_shared(WRITE_STREAM_SIZE));
        for (const auto &[key, stream] : state.streams)
        {
            for (std::size_t i = 0; i < stream.segments.size(); ++i)
            {
                const auto &segment = stream.segments[i];
                const auto bytes = segment.used + (i + 1 < stream.segments.size() ? sizeof(LinkPointer) : 0);
                copy_persistent_prefix(target + segment.offset, source + segment.offset, bytes);
            }
            // Empty, preallocated segments are intentionally left zero. Their
            // addresses are restored by the writer, without copying stale tails.
        }
        resources.write_stream[target_replica] = shared_offset(target);
        auto *read = allocate_shared(READ_STREAM_SIZE);
        copy_persistent_prefix(read, shared_pointer<char>(resources.read_stream[source_replica]), state.read_tail);
        resources.read_stream[target_replica] = shared_offset(read);
        if (target_replica == 1)
        {
            resources.stream_catalog = allocate_stream_catalog();
            auto *catalog = shared_pointer<StreamCatalogEntry>(resources.stream_catalog);
            std::size_t cursor = 0;
            for (const auto &[key, stream] : state.streams)
                if (initial_stream(key).initial_segments == 0 && stream.initial_segments != 0)
                    append_stream_catalog(catalog, cursor, key, stream.head,
                        stream.first_lsn ? stream.first_lsn : stream.trim_lsn);
            if (cursor)
                clflushopt(catalog, cursor * sizeof(StreamCatalogEntry));
            sfence();
        }
    }

    inline void prepare_lifecycle_replica(const RecoveryCut &cut, ClusterConfiguration &config,
                                          const RecoveredShard &primary, std::size_t shard,
                                          StateKey key, unsigned survivors = 3)
    {
        if (!(config.header.replica_mask & survivors & 2))
            return;
        const auto backup = recover_shard_from_index(cut, shard, 1, survivors);
        const auto &left = primary.streams.at(key);
        const auto &right = backup.streams.at(key);
        const bool same_layout = left.head == right.head &&
            left.initial_segments == right.initial_segments && left.first_capacity == right.first_capacity &&
            std::equal(left.segments.begin(), left.segments.end(), right.segments.begin(), right.segments.end(),
                [](const auto &a, const auto &b)
                {
                    return a.offset == b.offset && a.used == b.used && a.capacity == b.capacity;
                });
        if (!same_layout)
        {
            // Lifecycle metadata publishes one retained head for both replicas.
            // Use a fresh backup with the primary's layout before moving that
            // head. The old backup remains intact until the new cut commits,
            // so a crash during copying cannot corrupt the previous prefix.
            rebuild_shard_replica(config.shards.at(shard), primary, 0, 1);
        }
    }

    inline void build_recovered_indexes(ClusterConfiguration &config, const std::vector<RecoveredShard> &shards)
    {
        for (unsigned index = 0; index < 2; ++index)
        {
            auto *table = new (allocate_shared(sizeof(star::CCHashTable))) star::CCHashTable(BUCKET_CNT, shards.size());
            for (std::size_t shard = 0; shard < shards.size(); ++shard)
                for (const auto &[key, stream] : shards[shard].streams)
                    table->restore(key, shard, star::ShardInfo(stream.tail, stream.head));
            clflushopt(table, sizeof(*table));
            config.header.index[index] = shared_offset(table);
        }
        sfence();
    }

    inline StreamLifecycle trim_stream(char *base, RecoveredStream &stream, std::uint32_t shard,
                                        std::uint32_t lsn, bool erase)
    {
        StreamLifecycle lifecycle;
        lifecycle.shard = shard;
        lifecycle.key = stream.key;
        lifecycle.trim_lsn = std::max(lsn, stream.trim_lsn);
        lifecycle.deleted = erase;
        if (stream.initial_segments == 0 && stream.segments.empty())
        {
            // A read-only initial key may have no writer segment at all. Do
            // not turn offset zero into an allocation owned by this key.
            stream.trim_lsn = lifecycle.trim_lsn;
            return lifecycle;
        }
        std::uint32_t removed = 0;
        std::size_t first = 0;
        std::uint32_t skip = 0;
        if (erase)
        {
            removed = stream.tail;
            // Keep one empty segment for future appends with this stream ID.
            stream.segments.resize(std::min<std::size_t>(1, stream.segments.size()));
            for (auto &segment : stream.segments)
                segment.used = 0;
        }
        else
        {
            for (; first < stream.segments.size(); ++first)
            {
                const auto &segment = stream.segments[first];
                skip = 0;
                while (skip < segment.used)
                {
                    clflushopt(base + segment.offset + skip, CACHELINE_SIZE);
                    sfence();
                    std::uint64_t header;
                    std::memcpy(&header, base + segment.offset + skip, sizeof(header));
                    if ((header >> 32) > lsn)
                        break;
                    const auto bytes = static_cast<std::uint32_t>(header) + sizeof(header);
                    if (bytes == 0 || bytes > segment.used - skip)
                        throw std::runtime_error("invalid record while trimming stream");
                    skip += bytes;
                    removed += bytes;
                }
                if (skip < segment.used || first + 1 == stream.segments.size())
                    break;
            }
            if (!stream.segments.empty())
            {
                stream.segments.erase(stream.segments.begin(), stream.segments.begin() + first);
                auto &segment = stream.segments.front();
                segment.offset += skip;
                segment.capacity -= skip;
                segment.used -= skip;
                stream.head = segment.offset;
            }
        }
        stream.tail -= removed;
        stream.initial_segments = 1;
        stream.first_capacity = stream.segments.empty() ? stream.segment_size : stream.segments.front().capacity;
        stream.trim_lsn = lifecycle.trim_lsn;
        lifecycle.head = stream.head;
        lifecycle.first_capacity = stream.first_capacity;
        return lifecycle;
    }
}
