#pragma once

#include "Cacheline.h"
#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <stdexcept>
#include <thread>
#include <mutex>
#include <vector>

namespace SharedData
{
    using LeaseClock = std::chrono::steady_clock;

    struct alignas(CACHELINE_SIZE) LeaseSlot
    {
        // Only the owner writes this monotonically increasing renewal counter.
        std::atomic<std::uint64_t> renewal{0};
        // The sequencer owns this separate line. A replacement never clears an
        // old incarnation's fence, even when its logical shard ID is reused.
        alignas(CACHELINE_SIZE) std::atomic<std::uint64_t> fenced{0};
    };
    static_assert(sizeof(LeaseSlot) == 2 * CACHELINE_SIZE);
    static_assert(alignof(LeaseSlot) == CACHELINE_SIZE);

    template <std::size_t Shards>
    struct alignas(CACHELINE_SIZE) LeaseRegion
    {
        static_assert(Shards > 0 && Shards <= 64);
        LeaseSlot nodes[Shards + 1]; // last slot belongs to the sequencer
        // The sequencer alone writes revocations; shards only read them.
        alignas(CACHELINE_SIZE) std::atomic<std::uint64_t> revoked{0};
        // Immutable after the region is published through its Cxlalloc root.
        alignas(CACHELINE_SIZE) std::uint64_t timeout_ms = 10000;
        std::uint64_t renew_interval_ms = 1000;
    };

    template <typename Word>
    inline Word read_cxl_word(const std::atomic<Word> &word)
    {
        clflushopt(&word, sizeof(word));
        sfence();
        const auto value = word.load(std::memory_order_acquire);
        clflushopt(&word, sizeof(word));
        sfence();
        return value;
    }

    // Expiry is measured by the observer's monotonic clock. Hosts do not need
    // synchronized clocks, and an expired incarnation can never renew itself.
    class LeaseObserver
    {
        std::uint64_t last_ = 0;
        LeaseClock::time_point deadline_{};
        bool expired_ = false;

    public:
        bool observe(std::uint64_t renewal, LeaseClock::time_point now,
                     std::chrono::milliseconds timeout)
        {
            if (expired_)
                return false;
            if (last_ == 0)
            {
                if (renewal == 0)
                    return true; // the role has not joined yet
            }
            else if (now >= deadline_ || renewal < last_)
            {
                expired_ = true;
                return false;
            }
            if (renewal != last_)
            {
                last_ = renewal;
                deadline_ = now + timeout;
            }
            return true;
        }

        bool joined() const { return last_ != 0; }
        bool expired() const { return expired_; }
    };

    // A process owns one lease. Renewal and peer monitoring are deliberately
    // off the append/replay path. The shared slots preserve single-writer
    // ownership even when a shard is revoked; the sequencer never clears a
    // failed reader's announcement on its behalf.
    template <std::size_t Shards>
    class LeaseService
    {
        LeaseRegion<Shards> *region_ = nullptr;
        std::thread thread_;
        std::atomic<bool> stopping_{false};
        std::chrono::milliseconds timeout_{10000};
        std::chrono::milliseconds interval_{1000};
        std::size_t owner_ = Shards;
        LeaseSlot *own_slot_ = nullptr;
        bool is_sequencer_ = true;
        struct Peer
        {
            std::size_t shard;
            LeaseSlot *slot;
            LeaseObserver observer;
        };
        std::mutex peers_mutex_;
        std::vector<Peer> additional_peers_;
        std::uint64_t replaced_initial_ = 0;
        std::uint64_t renewal_ = 0;
        LeaseClock::time_point started_{};

        [[noreturn]] static void fence_process(const char *reason)
        {
            std::fprintf(stderr, "lease expired: %s; stopping process\n", reason);
            std::fflush(stderr);
            std::_Exit(75);
        }

        void renew()
        {
            own_slot_->renewal.store(++renewal_, std::memory_order_release);
            clwb(&own_slot_->renewal, CACHELINE_SIZE);
            sfence();
        }

        void run()
        {
            std::array<LeaseObserver, Shards + 1> peers;
            auto last_renewal = started_;
            std::uint64_t failed = 0;
            while (!stopping_.load(std::memory_order_relaxed))
            {
                const auto now = LeaseClock::now();
                // Use a shorter local deadline than the observer's lease so a
                // delayed owner fails closed instead of reviving an old lease.
                if (now - last_renewal >= timeout_ / 2)
                    fence_process("local renewal deadline missed");
                if (read_cxl_word(own_slot_->fenced) != 0)
                    fence_process("old role incarnation fenced");

                if (is_sequencer_)
                {
                    std::lock_guard peer_lock(peers_mutex_);
                    failed = expired_shards.load(std::memory_order_acquire) & ~(std::uint64_t{1} << 63);
                    const auto old_failed = failed;
                    for (std::size_t shard = 0; shard < Shards; ++shard)
                    {
                        if (replaced_initial_ & (std::uint64_t{1} << shard))
                            continue;
                        if ((failed & (std::uint64_t{1} << shard)) != 0)
                            continue;
                        const auto value = read_cxl_word(region_->nodes[shard].renewal);
                        if (!peers[shard].observe(value, now, timeout_))
                        {
                            failed |= std::uint64_t{1} << shard;
                            fence_slot(&region_->nodes[shard]);
                            std::fprintf(stderr, "lease expired: shard %zu revoked\n", shard);
                        }
                    }
                    {
                        for (auto &peer : additional_peers_)
                        {
                            const auto bit = std::uint64_t{1} << peer.shard;
                            if ((failed & bit) == 0 && !peer.observer.observe(
                                    read_cxl_word(peer.slot->renewal), now, timeout_))
                            {
                                failed |= bit;
                                fence_slot(peer.slot);
                                std::fprintf(stderr, "lease expired: shard %zu revoked\n", peer.shard);
                            }
                        }
                    }
                    if (failed != old_failed)
                    {
                        region_->revoked.store(failed, std::memory_order_release);
                        clwb(&region_->revoked, sizeof(region_->revoked));
                        sfence();
                        // Preserve a concurrent, process-local reconfiguration notification.
                        expired_shards.fetch_or(failed, std::memory_order_release);
                    }
                }
                else
                {
                    const auto value = read_cxl_word(region_->nodes[Shards].renewal);
                    if (!peers[Shards].observe(value, now, timeout_) || value == 0)
                        fence_process("sequencer unavailable");
                    if ((read_cxl_word(region_->revoked) & (std::uint64_t{1} << owner_)) != 0)
                        fence_process("shard revoked by sequencer");
                }

                renew();
                last_renewal = now;
                std::this_thread::sleep_for(interval_);
            }
        }

    public:
        // The hot path reads this DRAM word. Its cache line changes only on a
        // failure, not on every heartbeat. A stuck index-swap polls it too.
        alignas(CACHELINE_SIZE) std::atomic<std::uint64_t> expired_shards{0};

        ~LeaseService()
        {
            stopping_.store(true, std::memory_order_relaxed);
            if (thread_.joinable())
                thread_.join();
        }

        static void validate_settings(std::uint64_t timeout_ms, std::uint64_t interval_ms)
        {
            if (timeout_ms < 4 || timeout_ms > 3600000 || interval_ms == 0 || interval_ms > timeout_ms / 4)
                throw std::invalid_argument("lease_renew_interval_ms must be between 1 and lease_timeout_ms / 4 (timeout: 4..3600000 ms)");
        }

        void start(LeaseRegion<Shards> *region, std::size_t owner)
        {
            if (!region || owner > Shards)
                throw std::invalid_argument("invalid lease owner");
            start_with_slot(region, &region->nodes[owner], owner, owner == Shards);
        }

        void start_shard(LeaseRegion<Shards> *region, std::size_t shard, LeaseSlot *slot)
        {
            if (shard >= 63)
                throw std::invalid_argument("invalid shard lease owner");
            start_with_slot(region, slot, shard, false);
        }

        void add_shard(std::size_t shard, LeaseSlot *slot)
        {
            if (!is_sequencer_ || shard < Shards || shard >= 63 || !slot)
                throw std::invalid_argument("invalid additional lease peer");
            std::lock_guard lock(peers_mutex_);
            for (const auto &peer : additional_peers_)
                if (peer.shard == shard)
                    throw std::invalid_argument("duplicate lease peer");
            additional_peers_.push_back({shard, slot, {}});
        }

        static void fence_slot(LeaseSlot *slot)
        {
            slot->fenced.store(1, std::memory_order_release);
            clwb(&slot->fenced, CACHELINE_SIZE);
            sfence();
        }

        void replace_shard(std::size_t shard, LeaseSlot *slot)
        {
            if (!is_sequencer_ || !slot || shard >= 63)
                throw std::invalid_argument("invalid replacement lease");
            std::lock_guard lock(peers_mutex_);
            const auto bit = std::uint64_t{1} << shard;
            if (!(expired_shards.load(std::memory_order_acquire) & bit))
                throw std::runtime_error("cannot replace a live shard lease");
            if (shard < Shards)
            {
                fence_slot(&region_->nodes[shard]);
                replaced_initial_ |= bit;
            }
            bool found = false;
            for (auto &peer : additional_peers_)
                if (peer.shard == shard)
                {
                    fence_slot(peer.slot);
                    peer = {shard, slot, {}};
                    found = true;
                    break;
                }
            if (!found)
                additional_peers_.push_back({shard, slot, {}});
            const auto failed = expired_shards.fetch_and(~bit, std::memory_order_acq_rel) &
                ~bit & ~(std::uint64_t{1} << 63);
            region_->revoked.store(failed, std::memory_order_release);
            clwb(&region_->revoked, CACHELINE_SIZE);
            sfence();
        }

    private:
        void start_with_slot(LeaseRegion<Shards> *region, LeaseSlot *slot, std::size_t owner, bool sequencer)
        {
            if (!region || !slot || thread_.joinable())
                throw std::invalid_argument("invalid lease region or duplicate lease service");
            region_ = region;
            owner_ = owner;
            own_slot_ = slot;
            is_sequencer_ = sequencer;
            clflushopt(&region_->timeout_ms, CACHELINE_SIZE);
            sfence();
            const auto timeout = region_->timeout_ms;
            const auto interval = region_->renew_interval_ms;
            clflushopt(&region_->timeout_ms, CACHELINE_SIZE);
            sfence();
            validate_settings(timeout, interval);
            timeout_ = std::chrono::milliseconds(timeout);
            interval_ = std::chrono::milliseconds(interval);
            // Reusing an incarnation without role recovery would create two
            // writers. A replacement must restore the committed role first.
            if (read_cxl_word(own_slot_->renewal) != 0)
                throw std::runtime_error("lease slot already owned; role recovery is required before reuse");
            if (read_cxl_word(own_slot_->fenced) != 0)
                throw std::runtime_error("cannot reuse a fenced lease slot");
            if (!is_sequencer_ &&
                (read_cxl_word(region_->revoked) & (std::uint64_t{1} << owner)) != 0)
                throw std::runtime_error("cannot start a revoked shard incarnation");
            started_ = LeaseClock::now();
            renew();
            thread_ = std::thread([this] { run(); });
        }
    };
}
