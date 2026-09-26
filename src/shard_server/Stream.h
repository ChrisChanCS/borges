#pragma once

#include <cstdint>
#include <atomic>
#include <pthread.h>
#include <cstring>
#include <glog/logging.h>
#include <absl/synchronization/mutex.h>
#include "../common/Macro.h"
#include "../common/LogEntry.h"
#include "../common/Cacheline.h"
#ifndef USE_RDMA
#include "../common/Recovery.h"
#endif

// exclusively owned by replicator thread
class Metadata
{
private:
    std::uint32_t cxl_end_offset_;
    std::uint32_t cxl_cur_tail_offset_;
#ifdef USE_RDMA
    uint64_t cxl_begin_offset_;
#else
    char *cxl_begin_ptr_;
#endif

public:
#ifdef USE_RDMA
    Metadata(uint64_t cxl_begin_offset, std::uint32_t cxl_end_offset)
        : cxl_end_offset_(cxl_end_offset), cxl_cur_tail_offset_(0), cxl_begin_offset_(cxl_begin_offset)
    {
    }
#else
    Metadata(char *cxl_begin_ptr, std::uint32_t cxl_end_offset)
        : cxl_end_offset_(cxl_end_offset), cxl_cur_tail_offset_(0), cxl_begin_ptr_(cxl_begin_ptr)
    {
    }
#endif
    ~Metadata()
    {
    }
    void copy_to_cxl(std::uint32_t &pending_size, char *pending_buffer_begin_ptr)
    {
        if (pending_size == 0)
        {
            return;
        }

        if (cxl_cur_tail_offset_ + pending_size > cxl_end_offset_)
        {
            LOG(ERROR) << "cxl_cur_tail_offset_ + pending_size > cxl_end_offset_";
            exit(-1);
        }

#ifdef USE_RDMA
        // Copy through staging buffer in chunks
        void* staging = g_rdma.get_staging_buf();
        char* src = pending_buffer_begin_ptr;
        uint64_t dst_off = cxl_begin_offset_ + cxl_cur_tail_offset_;
        size_t remaining = pending_size;
        while (remaining > 0) {
            size_t chunk = std::min(remaining, (size_t)4096);
            memcpy(staging, src, chunk);
            g_rdma.write(staging, dst_off, chunk);
            src += chunk; dst_off += chunk; remaining -= chunk;
        }
#else
        // Copy pending_size bytes into CXL, starting at pending_begin_copy_offset.
        std::memcpy(cxl_begin_ptr_ + cxl_cur_tail_offset_, pending_buffer_begin_ptr, pending_size);
#endif

        cxl_cur_tail_offset_ += pending_size;
    }
};

struct Segment
{
    std::uint32_t cxl_end_offset_; // capacity of segment, not used
    std::uint32_t cxl_cur_tail_offset_ = 0;
#ifdef USE_RDMA
    uint64_t cxl_begin_offset_;
#else
    char *cxl_begin_ptr_;
#endif
};

// modified by worker thread and read by replicator thread
class MetadataWithScaling
{
private:
    absl::Mutex mutex_;
    std::uint32_t seg_index_ = 0; // currently used segment index
    std::vector<Segment> segments_;

public:
#ifdef USE_RDMA
    MetadataWithScaling(uint64_t cxl_begin_offset, std::uint32_t segment_num) // Use segment_num contiguous segments at cxl_begin_offset.
    {
        segments_.reserve(4);
        std::uint32_t segment_size;
        if (WORKLOAD == 4)
        {
            segment_size = RETWIS_SEGMENT_SIZE;
        }
        else
        {
            segment_size = SEGMENT_SIZE;
        }
        for (std::uint32_t i = 0; i < segment_num; i++)
        {
            segments_.push_back(Segment{segment_size, 0, cxl_begin_offset + i * segment_size});
        }
    }
#else
    MetadataWithScaling(char *cxl_begin_ptr, std::uint32_t segment_num) // Use segment_num contiguous segments at cxl_begin_ptr.
    {
        segments_.reserve(4);
        std::uint32_t segment_size;
        if (WORKLOAD == 4)
        {
            segment_size = RETWIS_SEGMENT_SIZE;
        }
        else
        {
            segment_size = SEGMENT_SIZE;
        }
        for (std::uint32_t i = 0; i < segment_num; i++)
        {
            segments_.push_back(Segment{segment_size, 0, cxl_begin_ptr + i * segment_size});
        }
    }
#endif
    ~MetadataWithScaling()
    {
    }
#ifndef USE_RDMA
    MetadataWithScaling(char *base, const SharedData::RecoveredStream &state)
    {
        for (const auto &segment : state.segments)
            segments_.push_back({segment.capacity, segment.used, base + segment.offset});
        if (segments_.empty())
            segments_.push_back({state.first_capacity ? state.first_capacity : state.segment_size, 0, base + state.head});
        seg_index_ = segments_.size() - 1;
        for (std::size_t i = segments_.size(); i < state.initial_segments; ++i)
            segments_.push_back({state.segment_size, 0, base + state.head + i * state.segment_size});
    }
#endif
    void copy_to_cxl(std::uint32_t &pending_size, char *pending_buffer_begin_ptr, std::uint32_t &remain_offset, bool is_scale)
    {
        // When copying across segments, fill the current segment, append a link, and continue in the next segment.
        if (pending_size == 0)
        {
            return;
        }
#ifdef USE_RDMA
        uint64_t cxl_offset[2];
        {
            absl::MutexLock lock(&mutex_);
            cxl_offset[0] = segments_[seg_index_].cxl_begin_offset_ + segments_[seg_index_].cxl_cur_tail_offset_;
            if (is_scale)
            {
                if (seg_index_ == segments_.size() - 1)
                {
                    LOG(ERROR) << "cxl_cur_tail_offset_ + pending_size > cxl_end_offset_";
                    exit(-1);
                }
                segments_[seg_index_].cxl_cur_tail_offset_ += remain_offset + sizeof(LinkPointer);
                seg_index_++;
                cxl_offset[1] = segments_[seg_index_].cxl_begin_offset_;
                segments_[seg_index_].cxl_cur_tail_offset_ += (pending_size - remain_offset);
            }
            else
            {
                segments_[seg_index_].cxl_cur_tail_offset_ += pending_size;
            }
        }
        void* staging = g_rdma.get_staging_buf();
        if (!is_scale)
        {
            // Copy through staging in chunks
            char* src = pending_buffer_begin_ptr;
            uint64_t dst_off = cxl_offset[0];
            size_t remaining = pending_size;
            while (remaining > 0) {
                size_t chunk = std::min(remaining, (size_t)4096);
                memcpy(staging, src, chunk);
                g_rdma.write(staging, dst_off, chunk);
                src += chunk; dst_off += chunk; remaining -= chunk;
            }
            // Validate: read back entry header from RDMA and verify lsn+payload_size
            // Entry format: [8 bytes: (lsn<<32)|payload_size] [payload_size bytes: data]
            if (pending_size >= sizeof(std::uint64_t))
            {
                std::uint64_t local_header;
                std::memcpy(&local_header, pending_buffer_begin_ptr, sizeof(std::uint64_t));
                std::uint64_t remote_header;
                g_rdma.read(staging, cxl_offset[0], sizeof(std::uint64_t));
                std::memcpy(&remote_header, staging, sizeof(std::uint64_t));
                std::uint32_t local_lsn = local_header >> 32;
                std::uint32_t local_psz = local_header & 0xFFFFFFFF;
                std::uint32_t remote_lsn = remote_header >> 32;
                std::uint32_t remote_psz = remote_header & 0xFFFFFFFF;
                CHECK(remote_header == local_header)
                    << "[RDMA-CHECK] stream entry header mismatch after write:"
                    << " local_lsn=" << local_lsn << " local_psz=" << local_psz
                    << " remote_lsn=" << remote_lsn << " remote_psz=" << remote_psz
                    << " offset=0x" << std::hex << cxl_offset[0] << std::dec;
            }
        }
        else
        {
            // Write remain_offset bytes to current segment
            if (remain_offset > 0)
            {
                char* src = pending_buffer_begin_ptr;
                uint64_t dst_off = cxl_offset[0];
                size_t remaining = remain_offset;
                while (remaining > 0) {
                    size_t chunk = std::min(remaining, (size_t)4096);
                    memcpy(staging, src, chunk);
                    g_rdma.write(staging, dst_off, chunk);
                    src += chunk; dst_off += chunk; remaining -= chunk;
                }
            }
            // Write link pointer at end of current segment
            LinkPointer link_pointer{};
            link_pointer.tag_ = LINK_POINTER_MAGIC;
            link_pointer.next_entry_offset_ =
                static_cast<std::int64_t>(cxl_offset[1]) -
                static_cast<std::int64_t>(cxl_offset[0] + remain_offset);
            memcpy(staging, &link_pointer, sizeof(LinkPointer));
            g_rdma.write(staging, cxl_offset[0] + remain_offset, sizeof(LinkPointer));
            // Write remaining data to new segment
            {
                char* src = pending_buffer_begin_ptr + remain_offset;
                uint64_t dst_off = cxl_offset[1];
                size_t remaining = pending_size - remain_offset;
                while (remaining > 0) {
                    size_t chunk = std::min(remaining, (size_t)4096);
                    memcpy(staging, src, chunk);
                    g_rdma.write(staging, dst_off, chunk);
                    src += chunk; dst_off += chunk; remaining -= chunk;
                }
            }
        }
#else
        char *cxl_begin_ptr[2];
        {
            absl::MutexLock lock(&mutex_);
            cxl_begin_ptr[0] = segments_[seg_index_].cxl_begin_ptr_ + segments_[seg_index_].cxl_cur_tail_offset_;
            if (is_scale)
            {
                if (seg_index_ == segments_.size() - 1)
                {
                    // The worker has allocated the next segment by this point.
                    LOG(ERROR) << "cxl_cur_tail_offset_ + pending_size > cxl_end_offset_";
                    exit(-1);
                }
                segments_[seg_index_].cxl_cur_tail_offset_ += remain_offset + sizeof(LinkPointer);
                seg_index_++;
                cxl_begin_ptr[1] = segments_[seg_index_].cxl_begin_ptr_;
                segments_[seg_index_].cxl_cur_tail_offset_ += (pending_size - remain_offset);
            }
            else
            {
                segments_[seg_index_].cxl_cur_tail_offset_ += pending_size;
            }
        }
        // Copy pending_size bytes into CXL, starting at pending_begin_copy_offset.
        if (!is_scale)
        {
            std::memcpy(cxl_begin_ptr[0], pending_buffer_begin_ptr, pending_size);
            std::uint32_t lsn = (*reinterpret_cast<std::uint64_t *>(pending_buffer_begin_ptr)) >> 32;
            clflushopt(cxl_begin_ptr[0], pending_size);
        }
        else
        {
            if (remain_offset > 0)
            {
                std::memcpy(cxl_begin_ptr[0], pending_buffer_begin_ptr, remain_offset);
            }
            LinkPointer link_pointer{};
            link_pointer.tag_ = LINK_POINTER_MAGIC;
            link_pointer.next_entry_offset_ =
                static_cast<std::int64_t>(
                    reinterpret_cast<std::uintptr_t>(cxl_begin_ptr[1]) -
                    reinterpret_cast<std::uintptr_t>(cxl_begin_ptr[0] + remain_offset));
            std::uint32_t lsn = (*reinterpret_cast<std::uint64_t *>(pending_buffer_begin_ptr + remain_offset)) >> 32;
            link_pointer.next_lsn_ = lsn;

            {
                // for debug
            }

            std::memcpy(cxl_begin_ptr[0] + remain_offset, &link_pointer, sizeof(LinkPointer));
            clflushopt(cxl_begin_ptr[0], remain_offset + sizeof(LinkPointer));
            std::memcpy(cxl_begin_ptr[1], pending_buffer_begin_ptr + remain_offset, pending_size - remain_offset);
            clflushopt(cxl_begin_ptr[1], pending_size - remain_offset);
        }
#endif
    }
#ifdef USE_RDMA
    template <bool IsHead = false>
    void scale_segment(uint64_t cxl_begin_offset)
    {
        absl::MutexLock lock(&mutex_);
        std::uint32_t segment_size;
        if (WORKLOAD == 4)
        {
            segment_size = RETWIS_SEGMENT_SIZE;
        }
        else
        {
            if (!IsHead && YCSB_OPTION == 3 && WORKLOAD == 1)
            {
                segment_size = YCSB_NEW_KEY_SEGMENT_SIZE;
            }
            else
            {
                segment_size = SEGMENT_SIZE;
            }
        }
        if constexpr (IsHead)
        {
            // Only the first append registers the head. Its batch cannot be
            // consumed until registration finishes, even if expansions arrive
            // first. Keep their order and prepend the authoritative head.
            CHECK_EQ(seg_index_, 0);
            CHECK_EQ(segments_.front().cxl_cur_tail_offset_, 0);
            segments_.insert(segments_.begin(), Segment{segment_size, 0, cxl_begin_offset});
        }
        else
            segments_.push_back(Segment{segment_size, 0, cxl_begin_offset});
    }
#else
    template <bool IsHead = false>
    void scale_segment(char *cxl_begin_ptr_)
    {
        absl::MutexLock lock(&mutex_);
        std::uint32_t segment_size;
        if (WORKLOAD == 4)
        {
            segment_size = RETWIS_SEGMENT_SIZE;
        }
        else
        {
            if (!IsHead && YCSB_OPTION == 3 && WORKLOAD == 1)
            {
                segment_size = YCSB_NEW_KEY_SEGMENT_SIZE;
            }
            else
            {
                segment_size = SEGMENT_SIZE;
            }
        }
        if constexpr (IsHead)
        {
            // Match the initial constructor's capacity. The head batch becomes
            // READY only after both replicas have installed this first slot.
            CHECK_EQ(seg_index_, 0);
            CHECK_EQ(segments_.front().cxl_cur_tail_offset_, 0);
            segments_.insert(segments_.begin(), Segment{segment_size, 0, cxl_begin_ptr_});
        }
        else
            segments_.push_back(Segment{segment_size, 0, cxl_begin_ptr_});
    }
#endif
};
class Stream
{
private:
    // MetadataWithScaling write_stream_metadata_;
    Metadata write_stream_metadata_;

public:
#ifdef USE_RDMA
    Stream(uint64_t cxl_write_begin_offset, std::uint64_t buffer_size)
        : write_stream_metadata_(cxl_write_begin_offset, buffer_size) {}
#else
    Stream(char *cxl_write_begin_ptr, std::uint64_t buffer_size)
        : write_stream_metadata_(cxl_write_begin_ptr, buffer_size) {}
#endif

    void copy_to_cxl(std::uint32_t &pending_size, char *pending_buffer_begin_ptr)
    {
        write_stream_metadata_.copy_to_cxl(pending_size, pending_buffer_begin_ptr);
    }
};

class StreamWithScaling
{
private:
    MetadataWithScaling write_stream_metadata_;

public:
#ifdef USE_RDMA
    StreamWithScaling(uint64_t cxl_write_begin_offset, std::uint32_t segment_num)
        : write_stream_metadata_(cxl_write_begin_offset, segment_num) {}
#else
    StreamWithScaling(char *cxl_write_begin_ptr, std::uint32_t segment_num)
        : write_stream_metadata_(cxl_write_begin_ptr, segment_num) {}
    StreamWithScaling(char *base, const SharedData::RecoveredStream &state)
        : write_stream_metadata_(base, state) {}
#endif

    void copy_to_cxl(std::uint32_t &pending_size, char *pending_buffer_begin_ptr, std::uint32_t &available_offset, bool is_scale)
    {
        write_stream_metadata_.copy_to_cxl(pending_size, pending_buffer_begin_ptr, available_offset, is_scale);
    }
#ifdef USE_RDMA
    template <bool IsHead = false>
    void scale_segment(uint64_t cxl_begin_offset)
    {
        write_stream_metadata_.scale_segment<IsHead>(cxl_begin_offset);
    }
#else
    template <bool IsHead = false>
    void scale_segment(char *cxl_begin_ptr_)
    {
        write_stream_metadata_.scale_segment<IsHead>(cxl_begin_ptr_);
    }
#endif
};
