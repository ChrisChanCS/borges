#pragma once

#include "Cacheline.h"
#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <type_traits>

namespace SharedData
{
    // Figure 4: a cut names a publication round, configuration epoch and index.
    // index_offset is a Cxlalloc offset, never a process-local virtual address.
    struct GlobalCutHeader
    {
        std::uint64_t round = 0;
        std::uint64_t epoch = 0;
        std::uint64_t index_offset = 0;
    };
    static_assert(sizeof(GlobalCutHeader) == 3 * sizeof(std::uint64_t));

    struct GlobalCutShard
    {
        std::uint32_t shard_id = 0;
        std::uint32_t value = 0; // fully replicated local sequence number
    };

    // Each collector owns one line, separate from the producer and other
    // collectors. This records copied cut history, not application replay.
    struct alignas(CACHELINE_SIZE) GlobalCutConsumer
    {
        std::atomic<std::uint64_t> position{0}; // (epoch << 32) | last consumed round

        void publish(std::uint64_t epoch, std::uint64_t round)
        {
            position.store((epoch << 32) | round, std::memory_order_release);
            clwb(&position, sizeof(position));
            sfence();
        }
    };
    static_assert(sizeof(GlobalCutConsumer) == CACHELINE_SIZE);
    static_assert(alignof(GlobalCutConsumer) == CACHELINE_SIZE);
    static_assert(std::atomic<std::uint64_t>::is_always_lock_free);

    template <std::size_t Shards>
    struct alignas(CACHELINE_SIZE) GlobalCut
    {
        static_assert(Shards > 0 && Shards <= 64);
        static constexpr std::size_t body_size = sizeof(GlobalCutHeader) +
            Shards * sizeof(GlobalCutShard);
        // Leave one naturally aligned 64-bit word at the end for publication.
        static constexpr std::size_t checksum_offset =
            ((body_size + sizeof(std::uint64_t) + CACHELINE_SIZE - 1) / CACHELINE_SIZE) * CACHELINE_SIZE - sizeof(std::uint64_t);
        static constexpr std::uint64_t all_shards = ~std::uint64_t{0} >> (64 - Shards);

        GlobalCutHeader header{};
        GlobalCutShard gsn[Shards]{};
        // An array of uint64_t keeps the validity word naturally aligned. With
        // two shards the complete cut (including CRC) occupies one cache line.
        std::uint64_t trailer[(checksum_offset - body_size) / sizeof(std::uint64_t) + 1]{};

        std::uint64_t &checksum() { return trailer[std::size(trailer) - 1]; }
        const std::uint64_t &checksum() const { return trailer[std::size(trailer) - 1]; }

        // CRC32C, using the same x86 ISA baseline as the project's AVX2 build.
        // The complemented second half makes zero an unambiguous invalid word,
        // even when the CRC itself is zero. All bytes preceding it are covered.
        __attribute__((target("sse4.2"))) std::uint64_t compute_checksum() const
        {
            std::uint64_t crc = 0xffffffffu;
            const auto *bytes = reinterpret_cast<const unsigned char *>(this);
            for (std::size_t i = 0; i < checksum_offset; i += sizeof(std::uint64_t))
            {
                std::uint64_t word;
                std::memcpy(&word, bytes + i, sizeof(word));
                crc = _mm_crc32_u64(crc, word);
            }
            const auto value = static_cast<std::uint32_t>(crc) ^ 0xffffffffu;
            return (std::uint64_t{value} << 32) | static_cast<std::uint32_t>(~value);
        }

        bool valid(std::uint64_t expected_round) const
        {
            if (expected_round == 0 || header.round != expected_round ||
                header.index_offset == 0 ||
                checksum() == 0 || checksum() != compute_checksum())
                return false;
            for (std::size_t i = 0; i < Shards; ++i)
                if (gsn[i].shard_id != i)
                    return false;
            return true;
        }
    };

    static_assert(sizeof(GlobalCut<2>) == CACHELINE_SIZE);
    static_assert(alignof(GlobalCut<2>) == CACHELINE_SIZE);
    static_assert(std::is_trivially_copyable_v<GlobalCut<2>>);

    // Reads must invalidate both before and after accessing non-coherent CXL.
    // Validation and replay only use the resulting private snapshot.
    template <std::size_t Shards>
    inline void read_global_cuts(const GlobalCut<Shards> *source,
                                 GlobalCut<Shards> *snapshots, std::size_t count)
    {
        clflushopt(source, sizeof(*source) * count);
        sfence();
        for (std::size_t i = 0; i < count; ++i)
        {
            std::memcpy(snapshots + i, source + i, GlobalCut<Shards>::checksum_offset);
            snapshots[i].checksum() = std::atomic_ref<const std::uint64_t>(source[i].checksum()).load(std::memory_order_acquire);
        }
        clflushopt(source, sizeof(*source) * count);
        sfence();
    }

    template <std::size_t Shards>
    inline GlobalCut<Shards> read_global_cut(const GlobalCut<Shards> *source)
    {
        GlobalCut<Shards> result;
        read_global_cuts(source, &result, 1);
        return result;
    }

    template <std::size_t Shards>
    inline void persist_global_cut(const GlobalCut<Shards> &cut,
                                   const std::array<GlobalCut<Shards> *, 2> &copies)
    {
        // Both bodies reach persistence before either validity marker. There
        // is exactly one writer; no remote read-modify-write operation is used.
        auto body = cut;
        body.checksum() = 0;
        const auto checksum = cut.compute_checksum();
        for (auto *copy : copies)
        {
            // Stream whole cache lines without a CXL read-for-ownership. NT
            // stores reach persistence at SFENCE and do not need CLWB.
            for (std::size_t offset = 0; offset < sizeof(body); offset += sizeof(__m128i))
            {
                const auto value = _mm_load_si128(reinterpret_cast<const __m128i *>(
                    reinterpret_cast<const char *>(&body) + offset));
                _mm_stream_si128(reinterpret_cast<__m128i *>(
                    reinterpret_cast<char *>(copy) + offset), value);
            }
        }
        sfence();
        for (auto *copy : copies)
        {
            // Naturally aligned 64-bit MOVNTI is the atomic validity store on
            // x86. The preceding fence orders the body; the following fence
            // completes both markers before the index/producer is published.
            _mm_stream_si64(reinterpret_cast<long long *>(&copy->checksum()),
                            static_cast<long long>(checksum));
        }
        sfence();
    }

    template <std::size_t Shards>
    inline bool read_valid_global_cut(const GlobalCut<Shards> *primary,
                                      const GlobalCut<Shards> *backup,
                                      std::uint64_t round, GlobalCut<Shards> &result)
    {
        result = read_global_cut(primary);
        if (result.valid(round))
            return true;
        result = read_global_cut(backup);
        return result.valid(round);
    }

    // Recovery must require BOTH complete copies when both devices survive.
    // A caller recovering a single surviving device passes nullptr for backup.
    // upper_bound may include the unannounced tail written before a crash.
    template <std::size_t Shards>
    inline bool recover_global_cut(const GlobalCut<Shards> *primary,
                                   const std::type_identity_t<GlobalCut<Shards>> *backup,
                                   std::uint64_t capacity, std::uint64_t upper_bound,
                                   GlobalCut<Shards> &result)
    {
        if (capacity <= 1)
            return false;
        if (upper_bound >= capacity)
            upper_bound = capacity - 1;
        for (auto round = upper_bound; round != 0; --round)
        {
            auto first = read_global_cut(primary + round);
            if (!first.valid(round))
                continue;
            if (backup)
            {
                const auto second = read_global_cut(backup + round);
                if (!second.valid(round) || std::memcmp(&first, &second, sizeof(first)) != 0)
                    continue;
            }
            result = first;
            return true;
        }
        return false;
    }
}
