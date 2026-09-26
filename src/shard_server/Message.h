#pragma once

#include <string>
#include <vector>
#include <cstring>
#include "../common/Macro.h"
#ifndef USE_RDMA
#include <cxlalloc.h>
#else
#include "rdma/RdmaRegion.h"
#endif
#include "../common/SharedData.h"
#ifndef USE_RDMA
#include "../common/Recovery.h"
#endif
#include <glog/logging.h>

namespace Message
{
    class Report
    {
    private:
        // Each replicator writes the LSN and key count only in its own region.
        // Alignment alone does not reserve a whole cache line; size and stride also matter.
        std::uint32_t id_;
        std::uint32_t lsn_{0};
        std::uint32_t commit_buffer_key_num_{0}; // Commit-buffer key count for batches copied by this replicator.
#ifdef USE_RDMA
        uint64_t cut_region_offset_;
#else
        SharedData::CutRegion *cut_region_;
#endif

    public:
        Report(std::uint32_t shard_id, std::uint32_t replicator_id)
            : id_(shard_id * REPLICATOR_NUM + replicator_id)
        {
            // Initialized by the sequencer.
#ifdef USE_RDMA
            cut_region_offset_ = g_rdma.get_root(CUT_REGION_ROOT_INDEX + id_);
#else
            cut_region_ = SharedData::shared_pointer<SharedData::CutRegion>(SharedData::shard_resources(shard_id).local_cuts) + replicator_id;
            if (SharedData::recovered_shard)
            {
                lsn_ = SharedData::recovered_shard->progress >> 32;
                commit_buffer_key_num_ = static_cast<std::uint32_t>(SharedData::recovered_shard->progress);
            }
#endif
        }
        ~Report() {}
        void increment_commit_buffer_key_num(std::uint32_t key_num)
        {
            commit_buffer_key_num_ += key_num;
        }
        void set_lsn(std::uint64_t lsn)
        {
            lsn_ = lsn;
        }
        void send_report()
        {
#ifdef USE_RDMA
            void* staging = g_rdma.get_staging_buf();
            std::uint64_t report_value = (static_cast<std::uint64_t>(lsn_) << 32) | commit_buffer_key_num_;
            memcpy(staging, &report_value, sizeof(std::uint64_t));
            g_rdma.write(staging, cut_region_offset_, sizeof(std::uint64_t));
            // Validate: read back and verify the cut region was written correctly
            {
                std::uint64_t readback;
                g_rdma.read(staging, cut_region_offset_, sizeof(std::uint64_t));
                std::memcpy(&readback, staging, sizeof(std::uint64_t));
                CHECK(readback == report_value)
                    << "[RDMA-CHECK] cut_region readback mismatch: wrote=0x" << std::hex << report_value
                    << " read=0x" << readback << std::dec
                    << " replicator=" << id_ << " lsn=" << lsn_ << " key_num=" << commit_buffer_key_num_;
            }
#else
            std::uint64_t report_value = (static_cast<std::uint64_t>(lsn_) << 32) | commit_buffer_key_num_;
            cut_region_->value.store(report_value, std::memory_order_release);
            clwb(cut_region_, sizeof(SharedData::CutRegion));
            sfence();
#endif
        }
        std::uint32_t get_lsn()
        {
            return lsn_;
        }
    };

}
