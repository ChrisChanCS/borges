#pragma once

#include <cstdint>

#define CACHELINE_SIZE 64

#ifdef USE_RDMA

// Under RDMA, cache operations are no-ops (data moves via RDMA WRITE/READ)
inline void sfence() {}
inline void clflushopt(const void *, std::uint64_t) {}
inline void clflush(const void *, std::uint64_t) {}
inline void clwb(const void *, std::uint64_t) {}
inline void prefetch(const void *, int, std::uint64_t) {}

#else

#include <immintrin.h>

inline void sfence()
{
    _mm_sfence();
}

inline void clflushopt(const void *addr, std::uint64_t len)
{
    for (std::uint64_t ptr = (std::uint64_t)addr & ~(CACHELINE_SIZE - 1);
         ptr < (std::uint64_t)addr + len;
         ptr += CACHELINE_SIZE)
    {
        _mm_clflushopt((void *)ptr);
    }
}

inline void clflush(const void *addr, std::uint64_t len)
{
    for (std::uint64_t ptr = (std::uint64_t)addr & ~(CACHELINE_SIZE - 1);
         ptr < (std::uint64_t)addr + len;
         ptr += CACHELINE_SIZE)
    {
        _mm_clflush((void *)ptr);
    }
}

inline void prefetch(const void *addr, int level, std::uint64_t len)
{
    // level: 0 (temporal, closest), 1 (mid), 2 (farther), 3 (non-temporal)
    // Default to CACHELINE_SIZE stride prefetch
    for (std::uint64_t ptr = (std::uint64_t)addr & ~(CACHELINE_SIZE - 1);
         ptr < (std::uint64_t)addr + len;
         ptr += CACHELINE_SIZE)
    {
        switch (level)
        {
        case 0:
            _mm_prefetch(reinterpret_cast<const char *>(ptr), _MM_HINT_T0);
            break;
        case 1:
            _mm_prefetch(reinterpret_cast<const char *>(ptr), _MM_HINT_T1);
            break;
        case 2:
            _mm_prefetch(reinterpret_cast<const char *>(ptr), _MM_HINT_T2);
            break;
        case 3:
            _mm_prefetch(reinterpret_cast<const char *>(ptr), _MM_HINT_NTA);
            break;
        default:
            _mm_prefetch(reinterpret_cast<const char *>(ptr), _MM_HINT_T0);
            break;
        }
    }
}

inline void clwb(const void *addr, std::uint64_t len)
{
    for (std::uint64_t ptr = (std::uint64_t)addr & ~(CACHELINE_SIZE - 1);
         ptr < (std::uint64_t)addr + len;
         ptr += CACHELINE_SIZE)
    {
        _mm_clwb((void *)ptr);
    }
}

#endif
