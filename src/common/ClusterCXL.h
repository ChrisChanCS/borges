#pragma once

#include "Macro.h"
#include "ClusterConfig.h"

namespace SharedData
{
    inline std::uint64_t cluster_settings()
    {
        const std::uint64_t settings[] = {static_cast<std::uint64_t>(WORKLOAD),
            static_cast<std::uint64_t>(YCSB_OPTION), static_cast<std::uint64_t>(REQUEST_WORKER_NUM),
            SEGMENT_SIZE, SMALL_SEGMENT_SIZE, static_cast<std::uint64_t>(SCALE_OUT_STREAM)};
        return record_checksum(settings, sizeof(settings));
    }
    inline void *allocate_shared(std::size_t bytes)
    {
        auto *memory = cxlalloc_memalign(cache_line_size(bytes), CACHELINE_SIZE);
        if (!memory || reinterpret_cast<std::uintptr_t>(memory) % CACHELINE_SIZE != 0)
            throw std::runtime_error("cannot allocate aligned CXL configuration storage");
        return memory;
    }

    inline std::uint64_t shared_offset(const void *pointer)
    {
        std::uint64_t offset;
        if (!pointer || !cxlalloc_pointer_to_offset(pointer, &offset))
            throw std::runtime_error("invalid CXL configuration pointer");
        return offset;
    }

    template <typename T>
    inline T *shared_pointer(std::uint64_t offset)
    {
        if (offset == 0 || offset % CACHELINE_SIZE != 0)
            throw std::runtime_error("invalid CXL configuration offset");
        return static_cast<T *>(cxlalloc_offset_to_pointer(offset));
    }

    inline bool read_configuration(const std::uint64_t offsets[2], ClusterConfiguration &config, unsigned replicas = 3)
    {
        for (std::size_t replica = 0; replica < 2; ++replica)
        {
            if (!(replicas & (1u << replica)) || offsets[replica] == 0 || offsets[replica] % CACHELINE_SIZE != 0)
                continue;
            auto *source = shared_pointer<std::byte>(offsets[replica]);
            ClusterHeader header;
            clflushopt(source, sizeof(header));
            sfence();
            std::memcpy(&header, source, sizeof(header));
            clflushopt(source, sizeof(header));
            sfence();
            if (!ClusterConfiguration::valid_header(header))
                continue;
            RecordBuffer snapshot(header.record_bytes);
            read_record(source, snapshot.data(), snapshot.size());
            if (decode_configuration(snapshot, config))
            {
                std::copy(offsets, offsets + 2, config.offsets);
                return true;
            }
        }
        return false;
    }

    inline bool load_configuration(ClusterRegion *region, std::uint64_t epoch, ClusterConfiguration &config)
    {
        if (!region)
            return false;
        std::uint64_t offsets[2];
        for (std::size_t replica = 0; replica < 2; ++replica)
            offsets[replica] = read_cxl_word(region->heads[replica].offset);
        while (read_configuration(offsets, config))
        {
            if (config.header.epoch == epoch)
                return true;
            if (config.header.epoch < epoch || config.header.epoch == 0)
                return false;
            std::copy(std::begin(config.header.previous), std::end(config.header.previous), offsets);
        }
        return false;
    }

    inline void persist_configuration(ClusterRegion *region, ClusterConfiguration &config, bool advertise = true)
    {
        auto record = config.encode();
        std::array<void *, 2> copies;
        for (std::size_t replica = 0; replica < 2; ++replica)
        {
            copies[replica] = allocate_shared(record.size());
            config.offsets[replica] = shared_offset(copies[replica]);
        }
        persist_record(record, copies);
        config.header.shard_count = config.shards.size();
        config.header.record_bytes = record.size();
        if (!advertise)
            return;
        for (std::size_t replica = 0; replica < 2; ++replica)
        {
            region->heads[replica].cut_log.store(shared_offset(cxlalloc_get_root(GSN_BUFFER_ROOT_INDEX + replica)),
                                                std::memory_order_relaxed);
            region->heads[replica].offset.store(config.offsets[replica], std::memory_order_release);
            clwb(&region->heads[replica], sizeof(ClusterHead));
        }
        sfence();
    }

    // Immutable during shard service. Sequencer updates this directory only
    // while its worker pool is stopped at a configuration boundary.
    inline ClusterConfiguration startup_configuration;

    inline void load_startup_configuration(std::uint64_t epoch)
    {
        auto *region = static_cast<ClusterRegion *>(cxlalloc_get_root(CLUSTER_ROOT_INDEX));
        if (!load_configuration(region, epoch, startup_configuration))
            throw std::runtime_error("committed cluster configuration is unavailable");
        if (startup_configuration.header.request_workers != static_cast<std::uint64_t>(REQUEST_WORKER_NUM))
            throw std::runtime_error("request_worker_num differs from the cluster configuration");
    }

    inline const ShardResources &shard_resources(std::size_t shard)
    {
        return startup_configuration.shards.at(shard);
    }

    inline char *write_stream(std::size_t shard, std::size_t replica)
    {
        return shared_pointer<char>(shard_resources(shard).write_stream[replica]);
    }
}
