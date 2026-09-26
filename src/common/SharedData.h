#pragma once

#include <atomic>
#include <cstdint>
#include <thread>
#include <chrono>
#include <iostream>
#include "CCHashTable.h"
#include "Cacheline.h"
#ifndef USE_RDMA
#include "DeltaRing.h"
#include "GlobalCut.h"
#include "Lease.h"
#include "ClusterCXL.h"
#endif
#include "absl/time/time.h"
#include "absl/time/clock.h"

// all of the data are located in cxl

namespace SharedData
{
#ifndef USE_RDMA
    // A smaller compile-time window is useful for exercising wrap/full paths
    // in tests. Production retains the existing 80 MiB allocation and capacity.
#ifndef RHODES_DELTA_RING_CAPACITY
#define RHODES_DELTA_RING_CAPACITY COMMIT_RING_BUFFER_CAPACITY
#endif
    using DeltaRing = DeltaRingLayout<RHODES_DELTA_RING_CAPACITY>;
    static_assert(DeltaRing::capacity <= COMMIT_RING_BUFFER_CAPACITY);
    inline LeaseService<SHARD_SERVER_NUM> node_leases;
#endif
    // Each replicator owns a cut region; the sequencer loads these regions individually.
    struct alignas(64) CutRegion
    {
        // Upper 32 bits: LSN; lower 32 bits: number of replicated key/tail entries.
        std::atomic<std::uint64_t> value;
        char padding[64 - sizeof(std::atomic<std::uint64_t>)];
        explicit CutRegion()
        {
            value.store(0, std::memory_order_relaxed);
        }
    };

    struct alignas(64) CommitBufferMetadata
    {
        std::atomic<std::uint32_t> key_num{0}; // Producer: number of written key/tail entries.
#ifndef USE_RDMA
        // Producer-owned padding: disambiguate low-32-bit cursor rollover.
        std::atomic<std::uint64_t> write_position{0};
        // Nonzero only while space is exhausted. This complete batch boundary
        // lets the sequencer drain a prefix when replicators have run ahead.
        std::atomic<std::uint64_t> blocked_cut{0};
        // Sequencer-only cache line. Keep the preceding position while the
        // next cut is being prepared. Each atomic word is (LSN << 32) | cursor.
        alignas(CACHELINE_SIZE) std::atomic<std::uint64_t> consumed[2]{};
        std::atomic<std::uint64_t> reclaimed{0}; // Both indexes and recovery have passed this whole-line boundary.

        std::array<std::uint64_t, 2> read_consumed() const
        {
            clflushopt(consumed, sizeof(consumed));
            sfence();
            const std::array<std::uint64_t, 2> snapshot = {
                consumed[0].load(std::memory_order_acquire), consumed[1].load(std::memory_order_acquire)};
            clflushopt(consumed, sizeof(consumed));
            sfence();
            return snapshot;
        }
#endif
    };
#ifndef USE_RDMA
    static_assert(sizeof(CommitBufferMetadata) == 2 * CACHELINE_SIZE);
    static_assert(offsetof(CommitBufferMetadata, consumed) == CACHELINE_SIZE);
    static_assert(offsetof(CommitBufferMetadata, blocked_cut) < CACHELINE_SIZE);
    static_assert(offsetof(CommitBufferMetadata, reclaimed) == CACHELINE_SIZE + 16);
    static_assert(std::atomic<std::uint64_t>::is_always_lock_free);
#endif

    struct CommitRegion
    {
        // Upper 32 bits: key; lower 32 bits: write-stream tail delta.
        alignas(64) std::uint64_t *commit_buffer; // CXL buffer of per-batch write-stream tail deltas.
        alignas(64) CommitBufferMetadata *commit_buffer_metadata;
    };

    inline void writer_backoff()
    {
        for (int i = 0; i < 300; ++i)
        {
            // Use PAUSE on x86/x64.
            // Hint that this is a spin loop to reduce contention and power use.
#ifdef __x86_64__
            _mm_pause();
#elif defined(__aarch64__)
            __asm__ __volatile__("yield");
#else
            // Empty delay loop on other architectures.
            __asm__ __volatile__("");
#endif
        }
    }

    inline void reader_backoff()
    {
        for (int i = 0; i < 200; ++i)
        {
            // Use PAUSE on x86/x64.
            // Hint that this is a spin loop to reduce contention and power use.
#ifdef __x86_64__
            _mm_pause();
#elif defined(__aarch64__)
            __asm__ __volatile__("yield");
#else
            // Empty delay loop on other architectures.
            __asm__ __volatile__("");
#endif
        }
    }

    struct alignas(64) IndexRoundNumber
    {
        // Index round matching the global cut; rounds and switch versions advance independently.
        std::atomic<std::uint64_t> value;
        char padding[64 - sizeof(std::atomic<std::uint64_t>)];
        explicit IndexRoundNumber()
        {
            value.store(0, std::memory_order_relaxed);
        }
    };

    inline void wait_hash_table_update(IndexRoundNumber *index_round_number, std::uint64_t &round_number)
    {
        clflush(index_round_number, sizeof(IndexRoundNumber));
        std::uint64_t value;
        while (true)
        {
            value = index_round_number->value.load(std::memory_order_acquire);
            if (value >= round_number)
            {
                break;
            }
            else
            {
                clflush(index_round_number, sizeof(IndexRoundNumber));
                reader_backoff();
            }
        }
    }

    struct alignas(64) SwitchFlag
    {
        // Reader slots announce the complete sequencer switch value.
        // Sequencer: bit 0 selects the front index; upper 63 bits encode its version.
        // Reader: zero means idle; otherwise the value identifies the index in use.
        std::atomic<std::uint64_t> value;
        char padding[64 - sizeof(std::atomic<std::uint64_t>)];
        explicit SwitchFlag()
        {
            value.store(0, std::memory_order_relaxed);
        }
    };

    inline void write_switch_flag(SwitchFlag *flag, std::uint64_t value)
    {
        flag->value.store(value, std::memory_order_release);
        clwb(flag, sizeof(SwitchFlag));
    }

    // Return the switch value so readers can detect when their local view is stale.
    inline std::uint64_t read_switch_flag_value(SwitchFlag *sequencer_switch_flag)
    {
        clflushopt(sequencer_switch_flag, sizeof(SwitchFlag));
        sfence();
        std::uint64_t value = sequencer_switch_flag->value.load(std::memory_order_acquire);
        clflushopt(sequencer_switch_flag, sizeof(SwitchFlag));
        sfence();
        return value;
    }

    inline std::uint64_t reader_lock_switch_flag_with_version_number(SwitchFlag *sequencer_switch_flag, SwitchFlag *reader_switch_flag)
    {
        while (true)
        {
            clflushopt(sequencer_switch_flag, sizeof(SwitchFlag));
            sfence();
            std::uint64_t value = sequencer_switch_flag->value.load(std::memory_order_acquire);
            write_switch_flag(reader_switch_flag, value);
            // This invalidation/fence completes the first read and prepares
            // the confirming read, after persisting the reader announcement.
            clflushopt(sequencer_switch_flag, sizeof(SwitchFlag));
            sfence();
            std::uint64_t value_check = sequencer_switch_flag->value.load(std::memory_order_acquire);
            clflushopt(sequencer_switch_flag, sizeof(SwitchFlag));
            sfence();
            if (value_check == value)
            {
                return value;
            }
            else
            {
                write_switch_flag(reader_switch_flag, 0);
                sfence();
            }
        }
    }

    inline std::uint64_t reader_lock_switch_flag(SwitchFlag *sequencer_switch_flag, SwitchFlag *reader_switch_flag)
    {
        while (true)
        {
            clflushopt(sequencer_switch_flag, sizeof(SwitchFlag));
            sfence();
            std::uint64_t value = sequencer_switch_flag->value.load(std::memory_order_acquire);
            int front_ht_id = value & 1;
            write_switch_flag(reader_switch_flag, value);
            clflushopt(sequencer_switch_flag, sizeof(SwitchFlag));
            sfence();
            std::uint64_t value_check = sequencer_switch_flag->value.load(std::memory_order_acquire);
            clflushopt(sequencer_switch_flag, sizeof(SwitchFlag));
            sfence();
            if (value_check == value)
            {
                return front_ht_id;
            }
            else
            {
                write_switch_flag(reader_switch_flag, 0);
                sfence();
            }
        }
    }

    inline void reader_unlock_switch_flag(SwitchFlag *reader_switch_flag)
    {
        write_switch_flag(reader_switch_flag, 0);
        sfence();
    }

    inline void try_switch_flag(SwitchFlag *sequencer_switch_flag, SwitchFlag *reader_switch_flag[], const std::uint64_t &old_value, const std::uint64_t &new_value,
                                std::size_t readers = SHARD_SERVER_NUM * REQUEST_WORKER_NUM)
    {
        write_switch_flag(sequencer_switch_flag, new_value);
        for (int i = 0; i < readers; i++)
        {
            clflushopt(reader_switch_flag[i], sizeof(SwitchFlag));
        }
        sfence();
        for (int i = 0; i < readers; i++)
        {
            auto value = reader_switch_flag[i]->value.load(std::memory_order_acquire);
            clflushopt(reader_switch_flag[i], sizeof(SwitchFlag));
            // Complete post-read invalidations as a batch on the usual path.
            // A reader that is still active enters a separately fenced poll.
            if (value == old_value)
                sfence();
            while (value == old_value)
            {
#ifndef USE_RDMA
                // A failed owner cannot clear its announcement. Its expired
                // lease permits retirement without writing another host's slot.
                if (node_leases.expired_shards.load(std::memory_order_acquire) &
                    (std::uint64_t{1} << (i / REQUEST_WORKER_NUM)))
                    break;
#endif
                writer_backoff();
                clflushopt(reader_switch_flag[i], sizeof(SwitchFlag));
                sfence();
                value = reader_switch_flag[i]->value.load(std::memory_order_acquire);
                clflushopt(reader_switch_flag[i], sizeof(SwitchFlag));
                sfence();
            }
        }
        sfence();
    }

    struct alignas(64) GSN
    {
        // LSN committed by the sequencer.
        alignas(64) std::uint64_t value;
        char padding[64 - sizeof(std::uint64_t)];
        explicit GSN()
        {
            value = 0;
        }
    };

#ifndef USE_RDMA
    using GSNSet = GlobalCut<SHARD_SERVER_NUM>;
#else
    // The RDMA backend retains its existing wire representation.
    struct alignas(64) GSNSet
    {
        GSN gsn[SHARD_SERVER_NUM];
        explicit GSNSet()
        {
            for (std::uint32_t i = 0; i < SHARD_SERVER_NUM; i++)
            {
                new (&gsn[i]) GSN();
            }
        };
    };
#endif

    struct alignas(64) RoundNumber
    {
        // Current round.
        std::atomic<std::uint64_t> round;
        char padding[64 - sizeof(std::atomic<std::uint64_t>)];
        explicit RoundNumber()
        {
            round.store(0, std::memory_order_relaxed);
        }
    };

    struct alignas(64) HashTablePerShard
    {
        std::uint32_t shard_id_;
        // Per-shard index state.
        alignas(64) star::CCHashTable *hash_table[COMMIT_TAIL_HASH_TABLE_CANDIDATE_NUM];
        char padding[64 - sizeof(star::CCHashTable *) * COMMIT_TAIL_HASH_TABLE_CANDIDATE_NUM - sizeof(std::uint32_t)];
        explicit HashTablePerShard(std::uint32_t shard_id) : shard_id_(shard_id)
        {
            for (std::uint32_t i = 0; i < COMMIT_TAIL_HASH_TABLE_CANDIDATE_NUM; i++)
            {
#ifdef USE_RDMA
                hash_table[i] = reinterpret_cast<star::CCHashTable *>(g_rdma.get_root(TAIL_HASH_TABLE_ROOT_INDEX + shard_id_ * COMMIT_TAIL_HASH_TABLE_CANDIDATE_NUM + i));
#else
                hash_table[i] = reinterpret_cast<star::CCHashTable *>(cxlalloc_get_root(TAIL_HASH_TABLE_ROOT_INDEX + shard_id_ * COMMIT_TAIL_HASH_TABLE_CANDIDATE_NUM + i));
#endif
            }
        }
    };
}
