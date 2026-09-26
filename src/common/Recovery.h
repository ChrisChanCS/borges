#pragma once

#include "SharedData.h"
#include "LogEntry.h"
#include "StreamCatalog.h"
#include "StreamLifecycle.h"
#include "../benchmark/retwis/Query.h"
#include <algorithm>
#include <limits>
#include <map>
#include <optional>
#include <unordered_set>

namespace SharedData
{
    struct RecoveryCut
    {
        ClusterConfiguration configuration;
        GlobalCutHeader header{};
        std::vector<std::uint64_t> progress; // LSN in the upper half, delta cursor in the lower half.
        std::uint64_t offset = CACHELINE_SIZE;
        std::uint64_t next_offset = CACHELINE_SIZE;
    };

    inline std::vector<ClusterConfiguration> configuration_history(ClusterRegion *region, unsigned replicas = 3)
    {
        std::uint64_t offsets[2];
        for (std::size_t replica = 0; replica < 2; ++replica)
            offsets[replica] = read_cxl_word(region->heads[replica].offset);
        std::vector<ClusterConfiguration> history;
        ClusterConfiguration config;
        while (read_configuration(offsets, config, replicas))
        {
            if (!history.empty() && config.header.epoch >= history.back().header.epoch)
                throw std::runtime_error("configuration predecessor cycle");
            history.push_back(config);
            if (config.header.epoch == 0)
                break;
            std::copy(std::begin(config.header.previous), std::end(config.header.previous), offsets);
        }
        if (history.empty() || history.back().header.epoch != 0)
            throw std::runtime_error("incomplete configuration history");
        std::reverse(history.begin(), history.end());
        return history;
    }

    // Called only after fencing the old writer, or with include_prepared=false
    // for a published snapshot. A sequencer can leave at most one unpublished cut.
    inline RecoveryCut recover_published_prefix(bool include_prepared, unsigned replicas = 3,
                                                bool scan_survivor = false)
    {
        if (replicas == 0 || replicas > 3)
            throw std::invalid_argument("invalid surviving replica mask");
        auto *region = static_cast<ClusterRegion *>(cxlalloc_get_root(CLUSTER_ROOT_INDEX));
        auto *producer = static_cast<RoundNumber *>(cxlalloc_get_root(ROUND_NUMBER_ROOT_INDEX));
        if (!region || (!producer && !scan_survivor))
            throw std::runtime_error("cluster has not been bootstrapped");
        for (;;)
        {
            const auto history = configuration_history(region, replicas);
            const auto published = scan_survivor ? 0 : read_cxl_word(producer->round);
            const auto published_round = static_cast<std::uint32_t>(published);
            auto candidate = std::uint64_t(published_round) + (include_prepared ? 1 : 0);
            const auto capacity = std::uint64_t(GSN_SET_CNT) * sizeof(GSNSet);
            if (scan_survivor)
            {
                auto *base = static_cast<std::byte *>(cxlalloc_get_root(GSN_BUFFER_ROOT_INDEX + (replicas & 1 ? 0 : 1)));
                candidate = 0;
                for (auto config = history.rbegin(); config != history.rend(); ++config)
                {
                    const auto stride = global_cut_bytes(config->shards.size());
                    std::uint64_t low = 0, high = (capacity - config->header.cut_offset) / stride;
                    while (low < high)
                    {
                        const auto middle = low + (high - low) / 2;
                        auto *address = base + config->header.cut_offset + middle * stride;
                        clflushopt(address, sizeof(GlobalCutHeader));
                        sfence();
                        GlobalCutHeader header;
                        std::memcpy(&header, address, sizeof(header));
                        if (header.round == config->header.first_round + middle && header.epoch == config->header.epoch)
                            low = middle + 1;
                        else
                            high = middle;
                    }
                    if (low)
                    {
                        candidate = config->header.first_round + low - 1;
                        break;
                    }
                }
            }
            while (candidate != 0)
            {
                auto config = std::find_if(history.rbegin(), history.rend(), [&](const auto &entry)
                    { return entry.header.first_round <= candidate; });
                if (config == history.rend())
                    break;
                const auto stride = global_cut_bytes(config->shards.size());
                const auto offset = config->header.cut_offset + (candidate - config->header.first_round) * stride;
                RecordBuffer first(stride), second(stride);
                bool valid = offset <= capacity && stride <= capacity - offset;
                const auto required = replicas & config->header.replica_mask;
                valid = valid && required != 0;
                for (unsigned replica = 0; valid && replica < 2; ++replica)
                {
                    if (!(required & (1u << replica)))
                        continue;
                    auto *base = static_cast<std::byte *>(cxlalloc_get_root(GSN_BUFFER_ROOT_INDEX + replica));
                    auto &record = replica == 0 ? first : second;
                    if (!base)
                    {
                        valid = false;
                        break;
                    }
                    read_record(base + offset, record.data(), stride);
                    const auto &h = *reinterpret_cast<const GlobalCutHeader *>(record.data());
                    valid = valid_dynamic_cut(record.data(), stride, candidate, config->shards.size()) &&
                        h.epoch == config->header.epoch &&
                        (h.index_offset == config->header.index[0] || h.index_offset == config->header.index[1]);
                }
                if (valid && required == 3)
                    valid = std::memcmp(first.data(), second.data(), stride) == 0;
                if (valid)
                {
                    const auto &record = required & 1 ? first : second;
                    RecoveryCut result;
                    result.configuration = *config;
                    result.header = *reinterpret_cast<const GlobalCutHeader *>(record.data());
                    result.offset = offset;
                    result.next_offset = offset + stride;
                    const auto *entries = reinterpret_cast<const GlobalCutShard *>(record.data() + sizeof(GlobalCutHeader));
                    bool retry = false;
                    for (std::size_t shard = 0; shard < config->shards.size(); ++shard)
                    {
                        const auto lsn = entries[shard].value;
                        if (!(replicas & 1))
                        {
                            // Primary loss also loses the delta buffer. Its replacement
                            // starts empty; rebuild the index from the surviving log.
                            result.progress.push_back(std::uint64_t(lsn) << 32);
                            continue;
                        }
                        const auto *metadata = shared_pointer<CommitBufferMetadata>(config->shards[shard].commit_metadata);
                        const auto positions = metadata->read_consumed();
                        const auto found = std::find_if(positions.begin(), positions.end(), [lsn](auto position)
                            { return position >> 32 == lsn; });
                        if (found == positions.end())
                        {
                            // A live sequencer may publish another cut while a joining
                            // shard reads this snapshot. Retry the current prefix only.
                            retry = !include_prepared && !scan_survivor && read_cxl_word(producer->round) != published;
                            if (!retry)
                                throw std::runtime_error("no consumer position matches the recovered cut");
                            break;
                        }
                        try
                        {
                            DeltaRing::restore_position(read_cxl_word(metadata->write_position),
                                                        static_cast<std::uint32_t>(*found));
                        }
                        catch (const std::runtime_error &)
                        {
                            retry = !include_prepared && !scan_survivor && read_cxl_word(producer->round) != published;
                            if (!retry)
                                throw;
                            break;
                        }
                        result.progress.push_back(*found);
                    }
                    if (retry)
                        break;
                    return result;
                }
                --candidate;
            }
            if (!include_prepared && !scan_survivor && read_cxl_word(producer->round) != published)
                continue;
            if (published_round != 0)
                throw std::runtime_error("no valid committed prefix survives");
            RecoveryCut initial;
            initial.configuration = history.front();
            initial.header.index_offset = initial.configuration.header.index[0];
            initial.progress.resize(initial.configuration.shards.size(), 0);
            return initial;
        }
    }

    struct RecoveredSegment
    {
        std::uint32_t offset = 0;
        std::uint32_t used = 0;
        std::uint32_t capacity = 0;
    };

    struct RecoveredStream
    {
        StateKey key = 0;
        std::uint32_t head = 0;
        std::uint32_t tail = 0;
        std::uint32_t segment_size = 0;
        std::uint32_t initial_segments = 0;
        std::uint32_t first_lsn = 0;
        std::uint32_t first_capacity = 0;
        std::uint32_t trim_lsn = 0;
        std::vector<RecoveredSegment> segments;
    };

    struct RecoveredShard
    {
        std::uint64_t progress = 0;
        std::uint32_t read_tail = 0;
        std::size_t catalog_cursor = 0;
        std::map<StateKey, RecoveredStream> streams;
    };

    inline std::optional<RecoveredShard> recovered_shard;
    inline std::optional<RecoveredShard> recovered_backup;

    inline std::uint32_t stream_segment_size()
    {
        return WORKLOAD == 4 ? RETWIS_SEGMENT_SIZE :
            (WORKLOAD == 1 && YCSB_OPTION == 3 ? YCSB_NEW_KEY_SEGMENT_SIZE : SEGMENT_SIZE);
    }

    inline RecoveredStream initial_stream(StateKey key)
    {
        RecoveredStream stream;
        stream.key = key;
        stream.segment_size = stream_segment_size();
        if (WORKLOAD == 0 && key == 0)
        {
            // Only the initial reservation is large. Later links point to the
            // same allocation-sized segments used by scale_segment().
            stream.first_capacity = 1024u * 1024 * (SCALE_OUT_STREAM ? 100 : 500);
            stream.initial_segments = 1;
        }
        else if (WORKLOAD == 4 && key >= USER_CNT && key < 2 * USER_CNT)
        {
            stream.head = (key - USER_CNT) * RETWIS_SEGMENT_SIZE * TIMELINE_SEG_NUM;
            stream.initial_segments = TIMELINE_SEG_NUM;
        }
        else if (WORKLOAD >= 1 && WORKLOAD <= 3 && key < KEY_CNT_OF_SHARD && !(WORKLOAD == 1 && YCSB_OPTION == 3))
        {
            if (key < YCSB_HOTTER_KEY_CNT)
            {
                stream.head = key * HOTTER_SEG_NUM * SEGMENT_SIZE;
                stream.initial_segments = HOTTER_SEG_NUM;
            }
            else if (key < YCSB_HOTTER_KEY_CNT + YCSB_HOT_KEY_CNT)
            {
                stream.head = (YCSB_HOTTER_KEY_CNT * HOTTER_SEG_NUM +
                    (key - YCSB_HOTTER_KEY_CNT) * HOT_SEG_NUM) * SEGMENT_SIZE;
                stream.initial_segments = HOT_SEG_NUM;
            }
            else
            {
                stream.head = (YCSB_HOTTER_KEY_CNT * HOTTER_SEG_NUM + YCSB_HOT_KEY_CNT * HOT_SEG_NUM +
                    (key - YCSB_HOTTER_KEY_CNT - YCSB_HOT_KEY_CNT) * COLD_SEG_NUM) * SEGMENT_SIZE;
                stream.initial_segments = COLD_SEG_NUM;
            }
        }
        return stream;
    }

    // Scan a published byte prefix. Link bytes do not contribute to index tails.
    // Recovery is deliberately separate from the normal replay/cache protocol.
    inline void recover_stream(char *base, RecoveredStream &stream, std::uint32_t committed_lsn,
                               std::optional<std::uint32_t> published_tail = std::nullopt)
    {
        if (stream.initial_segments == 0 && (!published_tail || *published_tail == 0))
            return;
        std::uint64_t position = stream.head;
        std::uint32_t previous_lsn = 0;
        std::uint32_t expected_lsn = 0;
        stream.segments.push_back({stream.head, 0, stream.first_capacity ? stream.first_capacity : stream.segment_size});
        std::unordered_set<std::uint64_t> visited{position};
        while (!published_tail || stream.tail < *published_tail)
        {
            if (position + sizeof(std::uint64_t) > WRITE_STREAM_SIZE)
                throw std::runtime_error("stream escapes its shard region");
            clflushopt(base + position, CACHELINE_SIZE);
            sfence();
            std::uint64_t header;
            std::memcpy(&header, base + position, sizeof(header));
            if (header == LINK_POINTER_MAGIC)
            {
                std::uint32_t next_lsn;
                std::memcpy(&next_lsn, base + position + 16, sizeof(next_lsn));
                if (next_lsn <= std::max(previous_lsn, stream.trim_lsn) || next_lsn > committed_lsn)
                    break;
                std::int64_t distance;
                std::memcpy(&distance, base + position + sizeof(header), sizeof(distance));
                const auto next = static_cast<std::int64_t>(position) + distance;
                if (next < 0 || std::uint64_t(next) >= WRITE_STREAM_SIZE || next % CACHELINE_SIZE != 0 ||
                    !visited.insert(next).second)
                    throw std::runtime_error("invalid stream link during recovery");
                position = next;
                expected_lsn = next_lsn;
                stream.segments.push_back({static_cast<std::uint32_t>(position), 0, stream.segment_size});
                continue;
            }
            const auto lsn = static_cast<std::uint32_t>(header >> 32);
            const auto bytes = std::uint64_t(static_cast<std::uint32_t>(header)) + sizeof(header);
            if (lsn == 0 || lsn > committed_lsn || lsn <= stream.trim_lsn)
                break;
            if (lsn <= previous_lsn || bytes % CACHELINE_SIZE != 0 ||
                (expected_lsn != 0 && lsn != expected_lsn) ||
                bytes > stream.segments.back().capacity - stream.segments.back().used ||
                bytes + position > WRITE_STREAM_SIZE ||
                (published_tail && bytes > *published_tail - stream.tail))
                throw std::runtime_error("invalid committed stream record during recovery");
            previous_lsn = lsn;
            expected_lsn = 0;
            if (stream.first_lsn == 0)
                stream.first_lsn = lsn;
            stream.segments.back().used += bytes;
            stream.tail += bytes;
            position += bytes;
        }
        if (published_tail && stream.tail != *published_tail)
            throw std::runtime_error("published stream prefix is incomplete");
    }

    inline std::uint32_t recover_read_tail(char *base, std::uint32_t committed_lsn)
    {
        std::uint32_t offset = 0, previous = 0;
        while (offset + sizeof(std::uint64_t) <= READ_STREAM_SIZE)
        {
            clflushopt(base + offset, CACHELINE_SIZE);
            sfence();
            std::uint64_t header;
            std::memcpy(&header, base + offset, sizeof(header));
            const auto lsn = static_cast<std::uint32_t>(header >> 32);
            if (lsn == 0 || lsn > committed_lsn)
                break;
            const auto bytes = std::uint64_t(static_cast<std::uint32_t>(header)) + sizeof(header);
            if (lsn <= previous || bytes % CACHELINE_SIZE || bytes > READ_STREAM_SIZE - offset)
                throw std::runtime_error("invalid read log during recovery");
            previous = lsn;
            offset += bytes;
        }
        return offset;
    }

    inline RecoveredShard recover_shard_from_index(const RecoveryCut &cut, std::size_t shard,
                                                  std::size_t replica = 0, unsigned survivors = 3)
    {
        RecoveredShard result;
        result.progress = cut.progress.at(shard);
        const auto lsn = static_cast<std::uint32_t>(result.progress >> 32);
        const auto &resources = cut.configuration.shards.at(shard);
        const auto lifecycles = load_stream_lifecycles(cut.configuration, survivors);
        auto *base = shared_pointer<char>(resources.write_stream[replica]);
        auto *index = shared_pointer<star::CCHashTable>(cut.header.index_offset);
        index->visit([&](StateKey key, const std::vector<star::ShardInfo> &values)
        {
            auto stream = initial_stream(key);
            if (stream.initial_segments == 0)
                stream.head = values.at(shard).write_stream_begin_offset_;
            if (const auto entry = lifecycles.find({shard, key}); entry != lifecycles.end())
            {
                stream.head = entry->second.head;
                stream.first_capacity = entry->second.first_capacity;
                stream.trim_lsn = entry->second.trim_lsn;
                stream.initial_segments = entry->second.first_capacity ? 1 : 0;
            }
            const auto tail = values.at(shard).cur_offset_;
            if (tail == 0 && stream.initial_segments == 0 &&
                !(WORKLOAD == 1 && YCSB_OPTION == 3 && key < KEY_CNT_OF_SHARD))
                return;
            if (tail != 0 && stream.initial_segments == 0)
                stream.initial_segments = 1;
            recover_stream(base, stream, lsn, tail);
            result.streams.emplace(key, std::move(stream));
        });
        result.read_tail = recover_read_tail(shared_pointer<char>(resources.read_stream[replica]), lsn);
        if (cut.configuration.header.replica_mask & survivors & 2)
            result.catalog_cursor = visit_stream_catalog(resources.stream_catalog, lsn,
                [](const StreamCatalogEntry &) {});
        return result;
    }

    inline void discard_uncommitted_tail(const ClusterConfiguration &config, std::size_t shard,
                                        std::size_t replica, const RecoveredShard &state)
    {
        std::vector<std::pair<std::uint64_t, std::uint64_t>> committed;
        for (const auto &[key, stream] : state.streams)
            for (std::size_t i = 0; i < stream.segments.size(); ++i)
            {
                const auto &segment = stream.segments[i];
                const auto bytes = segment.used + (i + 1 < stream.segments.size() ? sizeof(LinkPointer) : 0);
                if (bytes)
                    committed.emplace_back(segment.offset, std::uint64_t(segment.offset) + bytes);
            }
        std::sort(committed.begin(), committed.end());
        std::uint64_t end = 0;
        for (const auto &[begin, next] : committed)
        {
            if (begin < end || next > WRITE_STREAM_SIZE)
                throw std::runtime_error("committed streams overlap or escape their shard region");
            end = next;
        }
        auto *base = shared_pointer<char>(config.shards.at(shard).write_stream[replica]);
        auto clear = [](char *address, std::size_t bytes)
        {
            std::memset(address, 0, bytes);
            clflushopt(address, bytes);
        };
        // Each replica has its own physical layout. Clear only gaps outside
        // this replica's committed prefix while its writer is fenced. Doing
        // this here avoids an extra end-marker store on ordinary appends.
        std::uint64_t cursor = 0;
        for (const auto &[begin, next] : committed)
        {
            clear(base + cursor, begin - cursor);
            cursor = next;
        }
        clear(base + cursor, WRITE_STREAM_SIZE - cursor);
        clear(shared_pointer<char>(config.shards.at(shard).read_stream[replica]) + state.read_tail,
              READ_STREAM_SIZE - state.read_tail);
        if (replica == 1)
        {
            auto *catalog = shared_pointer<StreamCatalogEntry>(config.shards.at(shard).stream_catalog);
            const auto remaining = (stream_catalog_capacity - state.catalog_cursor) * sizeof(StreamCatalogEntry);
            std::memset(catalog + state.catalog_cursor, 0, remaining);
            clflushopt(catalog + state.catalog_cursor, remaining);
        }
        sfence();
    }

    inline void restore_shard_replicas(const RecoveryCut &cut, std::size_t shard)
    {
        // Validate both prefixes before clearing anything. Segment registration
        // may differ between replicas even when their logical records agree.
        auto primary = recover_shard_from_index(cut, shard);
        std::optional<RecoveredShard> backup;
        if (cut.configuration.header.replica_mask & 2)
            backup = recover_shard_from_index(cut, shard, 1);
        discard_uncommitted_tail(cut.configuration, shard, 0, primary);
        if (backup)
            discard_uncommitted_tail(cut.configuration, shard, 1, *backup);
        recovered_shard = std::move(primary);
        recovered_backup = std::move(backup);
    }
}
