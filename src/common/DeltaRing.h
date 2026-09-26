#pragma once

#include "Cacheline.h"
#include <algorithm>
#include <cstdint>
#include <cstring>
#include <stdexcept>

namespace SharedData
{
    // Logical positions never wrap at the physical end. Local cuts continue to
    // carry their low 32 bits; the producer also persists the full position.
    template<std::uint64_t Slots>
    struct DeltaRingLayout
    {
        static constexpr std::uint64_t capacity = Slots;
        static constexpr std::uint64_t entries_per_line = CACHELINE_SIZE / sizeof(std::uint64_t);
        static_assert(Slots >= 2 * entries_per_line && Slots % entries_per_line == 0);
        static_assert(Slots < (std::uint64_t{1} << 31));

        static std::uint64_t offset(std::uint64_t position) { return position % capacity; }

        static std::uint64_t reclaim_before(std::uint64_t first, std::uint64_t second,
                                            std::uint64_t recovery_floor)
        {
            // Never reuse part of a line that still contains a live delta.
            return std::min({first, second, recovery_floor}) & ~(entries_per_line - 1);
        }

        static bool fits(std::uint64_t position, std::uint64_t reclaimed, std::uint64_t count)
        {
            return position + count <= reclaimed + capacity;
        }

        static bool reached(std::uint32_t published, std::uint32_t target)
        {
            return static_cast<std::int32_t>(published - target) >= 0;
        }

        static std::uint64_t restore_position(std::uint64_t written, std::uint32_t cursor)
        {
            const auto distance = static_cast<std::uint32_t>(written) - cursor;
            if (distance > capacity || distance > written)
                throw std::runtime_error("recovered delta cursor is outside the retained ring window");
            return written - distance;
        }

        // Both spans belong to one logical batch. Callers supply the existing
        // publication/read fence after flushing all of its cache lines.
        static void flush(const std::uint64_t *base, std::uint64_t position, std::uint64_t count)
        {
            if (!count)
                return;
            const auto begin = offset(position);
            const auto first = std::min(count, capacity - begin);
            clflushopt(base + begin, first * sizeof(*base));
            if (first != count)
                clflushopt(base, (count - first) * sizeof(*base));
        }

        static void copy_out(const std::uint64_t *base, std::uint64_t position,
                             std::uint64_t count, std::uint64_t *destination)
        {
            const auto begin = offset(position);
            const auto first = std::min(count, capacity - begin);
            std::memcpy(destination, base + begin, first * sizeof(*base));
            if (first != count)
                std::memcpy(destination + first, base, (count - first) * sizeof(*base));
        }

        static void copy_in(std::uint64_t *base, std::uint64_t position,
                            std::uint64_t count, const std::uint64_t *source)
        {
            const auto begin = offset(position);
            const auto first = std::min(count, capacity - begin);
            std::memcpy(base + begin, source, first * sizeof(*base));
            if (first != count)
                std::memcpy(base, source + first, (count - first) * sizeof(*base));
        }
    };
}
