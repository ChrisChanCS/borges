#pragma once

#include <cstdint>
#include <limits>

 // namespace utils

namespace star
{

    class Random
    {
    public:
        Random(std::uint64_t seed = 0)
        {
            init_seed(seed);
        }

        void init_seed(std::uint64_t seed)
        {
            seed_ = (seed ^ 0x5DEECE66DULL) & ((1ULL << 48) - 1);
        }

        void set_seed(std::uint64_t seed)
        {
            seed_ = seed;
        }

        std::uint64_t next()
        {
            return ((std::uint64_t)next(32) << 32) + next(32);
        }

        std::uint64_t next(unsigned int bits)
        {
            seed_ = (seed_ * 0x5DEECE66DULL + 0xBULL) & ((1ULL << 48) - 1);
            return (seed_ >> (48 - bits));
        }

        /* [0.0, 1.0) */
        double next_double()
        {
            return (((std::uint64_t)next(26) << 27) + next(27)) / (double)(1ULL << 53);
        }

        std::uint32_t next_uint32()
        {
            return static_cast<std::uint32_t>(next(32));
        }

        std::uint64_t uniform_dist(std::uint64_t a, std::uint64_t b)
        {
            if (a == b)
                return a;
            return next() % (b - a + 1) + a;
        }

    private:

        std::uint64_t seed_;
    };
} // namespace star
