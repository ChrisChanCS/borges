#pragma once

#include "Worker.h"
#include "../common/Macro.h"
#include "../common/SharedData.h"

namespace Sequencer
{
    class CommitPtr
    {
    private:
        std::atomic<star::CCHashTable *> cxl_commit_tail_hash_table_ptr_[SHARD_SERVER_NUM];
        pthread_spinlock_t latch;

    public:
        CommitPtr()
        {
            pthread_spin_init(&latch, PTHREAD_PROCESS_SHARED);
        }
        ~CommitPtr()
        {
            pthread_spin_destroy(&latch);
        }
        void lock()
        {
            pthread_spin_lock(&latch);
        }
        void unlock()
        {
            pthread_spin_unlock(&latch);
        }
        void commit()
        {
            lock();
            for (std::uint32_t i = 0; i < SHARD_SERVER_NUM; i++)
            {
                cxl_commit_tail_hash_table_ptr_[i].store(commit_tail_hash_table_ptr_[i].ptr);
            }
            unlock();
        }
    };
    class GSNCommitter
    {
    private:
        SharedData::RoundNumber *round_number_;
        SharedData::GSNSet *gsn_set_[2];
        std::uint64_t round_;
#ifndef USE_RDMA
        std::uint64_t index_offset_[COMMIT_TAIL_HASH_TABLE_CANDIDATE_NUM]{};
        std::size_t shard_count_ = SHARD_SERVER_NUM;
        std::uint64_t next_offset_ = sizeof(SharedData::GSNSet);
        std::unique_ptr<SharedData::RecordBuffer> dynamic_cut_;
#endif
#ifdef USE_RDMA
        uint64_t round_number_offset_;
        uint64_t gsn_set_offset_[2];
#endif

    public:
        // Default constructor: no-op. Must call init() after roots are set.
        GSNCommitter() : round_(0) {}

        // init(): called after sequencer has set all RDMA/CXL roots.
        void init()
        {
#ifdef USE_RDMA
            for (std::uint64_t i = 0; i < 2; i++)
            {
                gsn_set_offset_[i] = g_rdma.get_root(GSN_BUFFER_ROOT_INDEX + i);
            }
            round_number_offset_ = g_rdma.get_root(ROUND_NUMBER_ROOT_INDEX);
#else
            for (std::uint64_t i = 0; i < 2; i++)
            {
                gsn_set_[i] = reinterpret_cast<SharedData::GSNSet *>(cxlalloc_get_root(GSN_BUFFER_ROOT_INDEX + i));
            }
            round_number_ = reinterpret_cast<SharedData::RoundNumber *>(cxlalloc_get_root(ROUND_NUMBER_ROOT_INDEX));
            for (std::size_t i = 0; i < COMMIT_TAIL_HASH_TABLE_CANDIDATE_NUM; ++i)
            {
                CHECK(cxlalloc_pointer_to_offset(cxlalloc_get_root(TAIL_HASH_TABLE_ROOT_INDEX + i), &index_offset_[i]));
            }
#endif
        }
#ifndef USE_RDMA
        void configure(const SharedData::ClusterConfiguration &configuration)
        {
            shard_count_ = configuration.shards.size();
            std::copy(std::begin(configuration.header.index), std::end(configuration.header.index), index_offset_);
            for (unsigned replica = 0; replica < 2; ++replica)
                gsn_set_[replica] = static_cast<SharedData::GSNSet *>(cxlalloc_get_root(GSN_BUFFER_ROOT_INDEX +
                    ((configuration.header.replica_mask & (1u << replica)) ? replica : (replica ^ 1))));
            if (shard_count_ != SHARD_SERVER_NUM)
                dynamic_cut_ = std::make_unique<SharedData::RecordBuffer>(SharedData::global_cut_bytes(shard_count_));
        }
        std::uint64_t next_offset() const { return next_offset_; }
        void restore(std::uint64_t round, std::uint64_t next_offset)
        {
            round_ = round;
            next_offset_ = next_offset;
        }
#endif

        void commit(std::uint64_t epoch = 0, std::uint32_t index = 0)
        {
            // Skip an unchanged cut unless the configuration changed.
            // The first committed round is 1; round 0 is the initial state.
            if (gsn_dirty.load(std::memory_order_relaxed))
            {
                ++round_;
                if (round_ >= GSN_SET_CNT)
                {
                    LOG(ERROR) << "round: " << round_ << " is too large";
                    exit(-1);
                }
#ifdef USE_RDMA
                {
                    void* staging = g_rdma.get_staging_buf();
                    std::memcpy(staging, gsn, sizeof(SharedData::GSN) * SHARD_SERVER_NUM);
                    for (std::uint64_t i = 0; i < 2; i++)
                    {
                        g_rdma.write(staging, gsn_set_offset_[i] + round_ * sizeof(SharedData::GSNSet),
                                     sizeof(SharedData::GSN) * SHARD_SERVER_NUM);
                    }
                    // Validate: read back GSN from first copy and verify
                    {
                        g_rdma.read(staging, gsn_set_offset_[0] + round_ * sizeof(SharedData::GSNSet),
                                    sizeof(SharedData::GSN) * SHARD_SERVER_NUM);
                        for (std::uint32_t s = 0; s < SHARD_SERVER_NUM; s++)
                        {
                            SharedData::GSN rb;
                            std::memcpy(&rb, reinterpret_cast<char*>(staging) + s * sizeof(SharedData::GSN),
                                        sizeof(SharedData::GSN));
                            CHECK(rb.value == gsn[s].value)
                                << "[RDMA-CHECK] GSN[" << s << "] readback mismatch at round=" << round_
                                << ": wrote=" << gsn[s].value << " read=" << rb.value;
                        }
                    }
                }
#else
                const auto bytes = SharedData::global_cut_bytes(shard_count_);
                CHECK_LE(next_offset_ + bytes, std::uint64_t(GSN_SET_CNT) * sizeof(SharedData::GSNSet));
                const std::array<void *, 2> copies = {
                    reinterpret_cast<std::byte *>(gsn_set_[0]) + next_offset_,
                    reinterpret_cast<std::byte *>(gsn_set_[1]) + next_offset_};
                if (shard_count_ == SHARD_SERVER_NUM)
                {
                    SharedData::GSNSet cut;
                    cut.header = {round_, epoch, index_offset_[index]};
                    for (std::uint32_t i = 0; i < SHARD_SERVER_NUM; ++i)
                    {
                        cut.gsn[i] = {i, static_cast<std::uint32_t>(gsn[i].value)};
                    }
                    SharedData::persist_global_cut(cut, {
                        static_cast<SharedData::GSNSet *>(copies[0]), static_cast<SharedData::GSNSet *>(copies[1])});
                }
                else
                {
                    auto *header = reinterpret_cast<SharedData::GlobalCutHeader *>(dynamic_cut_->data());
                    *header = {round_, epoch, index_offset_[index]};
                    auto *entries = reinterpret_cast<SharedData::GlobalCutShard *>(dynamic_cut_->data() + sizeof(*header));
                    for (std::uint32_t i = 0; i < shard_count_; ++i)
                    {
                        entries[i] = {i, static_cast<std::uint32_t>(gsn[i].value)};
                    }
                    SharedData::persist_record(*dynamic_cut_, copies);
                }
                next_offset_ += bytes;
#endif

                // logAppend log
                LOG_CLASS("sequencer {}", 1, "commit gsn of request id: {}", round_);
            }
        }

        void update_round_number(std::uint64_t epoch = 0)
        {
#ifdef USE_RDMA
            void* staging = g_rdma.get_staging_buf();
            std::memcpy(staging, &round_, sizeof(std::uint64_t));
            g_rdma.write(staging, round_number_offset_, sizeof(std::uint64_t));
            // Validate: read back round number
            {
                std::uint64_t rb;
                g_rdma.read(staging, round_number_offset_, sizeof(std::uint64_t));
                std::memcpy(&rb, staging, sizeof(std::uint64_t));
                CHECK(rb == round_)
                    << "[RDMA-CHECK] round_number readback mismatch: wrote=" << round_ << " read=" << rb;
            }
#else
            round_number_->round.store((epoch << 32) | round_, std::memory_order_release);
            clwb(round_number_, sizeof(SharedData::RoundNumber));
            sfence();
#endif
        }

        std::uint64_t get_round()
        {
            return round_;
        }
    };
}
