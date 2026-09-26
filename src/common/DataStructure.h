#pragma once

#include <cstdint>
#include "Macro.h"

namespace star
{
    struct alignas(64) ShardInfo
    {
        std::uint32_t cur_offset_ = 0;
        std::uint64_t write_stream_begin_offset_ = 0;
#ifdef USE_RDMA
        char padding[64 - sizeof(std::uint64_t) - sizeof(std::uint32_t)];
#endif

        // Default constructor
        ShardInfo() : cur_offset_(0), write_stream_begin_offset_(0) {}

        explicit ShardInfo(std::uint32_t cur_offset, std::uint64_t write_stream_begin_offset)
        {
            cur_offset_ = cur_offset;
            write_stream_begin_offset_ = write_stream_begin_offset;
        }
    };
#ifndef USE_RDMA
    // alignas supplies the trailing padding. Explicitly padding the sum of
    // member sizes missed the four internal alignment bytes and produced a
    // 128-byte entry. One cache line is sufficient for this shard's writer.
    static_assert(sizeof(ShardInfo) == 64);
    static_assert(alignof(ShardInfo) == 64);
#endif

    struct alignas(64) AllShardInfo
    {
        ShardInfo shard_infos[SHARD_SERVER_NUM];
    };

}
