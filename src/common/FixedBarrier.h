#pragma once

#include <atomic>
#include <cstdint>

// A reusable barrier for a fixed, small group of threads in ONE process.
// It keeps blocking atomic::wait, as in std::barrier, and uses a native
// 32-bit wait word instead of a tree of arrival tickets. Never place it in CXL.
template <std::uint32_t Participants>
class FixedBarrier
{
    static_assert(Participants > 0);
    alignas(64) std::atomic<std::uint32_t> remaining_{Participants};
    alignas(64) std::atomic<std::uint32_t> generation_{0};
    std::uint32_t participants_ = Participants;

public:
    explicit FixedBarrier(std::uint32_t participants = Participants) : remaining_(participants), participants_(participants) {}
    void arrive_and_wait() noexcept
    {
        const auto generation = generation_.load(std::memory_order_relaxed);
        // The last arrival acquires every participant's preceding writes.
        if (remaining_.fetch_sub(1, std::memory_order_acq_rel) == 1)
        {
            remaining_.store(participants_, std::memory_order_relaxed);
            generation_.store(generation + 1, std::memory_order_release);
            generation_.notify_all();
        }
        else
        {
            // No following phase can finish without this participant, so the
            // generation cannot cycle back while this call is waiting.
            generation_.wait(generation, std::memory_order_acquire);
        }
    }
};
