#pragma once

#include "Cacheline.h"
#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <new>
#include <stdexcept>

namespace SharedData
{
    constexpr std::size_t cache_line_size(std::size_t bytes)
    {
        return (bytes + CACHELINE_SIZE - 1) / CACHELINE_SIZE * CACHELINE_SIZE;
    }

    // Private scratch space. Shared records contain offsets, never this owner.
    class RecordBuffer
    {
        struct Delete
        {
            void operator()(std::byte *p) const
            {
                ::operator delete(p, std::align_val_t{CACHELINE_SIZE});
            }
        };
        std::unique_ptr<std::byte, Delete> data_;
        std::size_t size_ = 0;

    public:
        explicit RecordBuffer(std::size_t bytes)
            : data_(static_cast<std::byte *>(::operator new(bytes, std::align_val_t{CACHELINE_SIZE}))),
              size_(bytes)
        {
            if (bytes == 0 || bytes % CACHELINE_SIZE != 0)
                throw std::invalid_argument("persistent record size must be a positive multiple of 64");
            std::memset(data_.get(), 0, bytes);
        }
        std::byte *data() { return data_.get(); }
        const std::byte *data() const { return data_.get(); }
        std::size_t size() const { return size_; }
        std::uint64_t &checksum()
        {
            return *reinterpret_cast<std::uint64_t *>(data() + size() - sizeof(std::uint64_t));
        }
        std::uint64_t checksum() const
        {
            std::uint64_t word;
            std::memcpy(&word, data() + size() - sizeof(word), sizeof(word));
            return word;
        }
    };

    __attribute__((target("sse4.2"))) inline std::uint64_t record_checksum(const void *data, std::size_t bytes)
    {
        std::uint64_t crc = 0xffffffffu;
        const auto *source = static_cast<const unsigned char *>(data);
        for (std::size_t i = 0; i < bytes; i += sizeof(std::uint64_t))
        {
            std::uint64_t word;
            std::memcpy(&word, source + i, sizeof(word));
            crc = _mm_crc32_u64(crc, word);
        }
        const auto value = static_cast<std::uint32_t>(crc) ^ 0xffffffffu;
        return (std::uint64_t{value} << 32) | static_cast<std::uint32_t>(~value);
    }

    inline void read_record(const void *source, void *snapshot, std::size_t bytes)
    {
        clflushopt(source, bytes);
        sfence();
        std::memcpy(snapshot, source, bytes - sizeof(std::uint64_t));
        const auto *marker = reinterpret_cast<const std::uint64_t *>(
            static_cast<const std::byte *>(source) + bytes - sizeof(std::uint64_t));
        const auto checksum = std::atomic_ref<const std::uint64_t>(*marker).load(std::memory_order_acquire);
        std::memcpy(static_cast<std::byte *>(snapshot) + bytes - sizeof(checksum), &checksum, sizeof(checksum));
        clflushopt(source, bytes);
        sfence();
    }

    inline void persist_record(RecordBuffer &record, const std::array<void *, 2> &copies)
    {
        record.checksum() = 0;
        const auto checksum = record_checksum(record.data(), record.size() - sizeof(std::uint64_t));
        for (auto *copy : copies)
            for (std::size_t offset = 0; offset < record.size(); offset += sizeof(__m128i))
                _mm_stream_si128(reinterpret_cast<__m128i *>(static_cast<std::byte *>(copy) + offset),
                    _mm_load_si128(reinterpret_cast<const __m128i *>(record.data() + offset)));
        sfence();
        for (auto *copy : copies)
            _mm_stream_si64(reinterpret_cast<long long *>(static_cast<std::byte *>(copy) +
                            record.size() - sizeof(std::uint64_t)), static_cast<long long>(checksum));
        sfence();
        record.checksum() = checksum;
    }
}
