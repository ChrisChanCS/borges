#pragma once

#include <cstdint>
#include <atomic>
#include <cstring>
#include <glog/logging.h>
#include "../common/io.h"
#include "../common/Macro.h"
#include "absl/container/inlined_vector.h"

char test_buffer[56] = {0};

class Pending_buffer
{
public:
    alignas(64) char *buffer;  // Data buffer
    std::uint64_t buffer_size; // Buffer size
    std::uint32_t tail_offset; // Current write offset in the buffer.

    Pending_buffer()
    {
        if (WORKLOAD == 4)
        {
            buffer_size = MAX_ENTRIES_PER_BATCH * RETWIS_ENTRY_MAX_SIZE;
        }
        else if (WORKLOAD == 1 && YCSB_OPTION == 3)
        {
            buffer_size = MAX_ENTRIES_PER_BATCH * ENTRY_MAX_SIZE * 11;
        }
        else
        {
            buffer_size = MAX_ENTRIES_PER_BATCH * ENTRY_MAX_SIZE;
        }
        buffer = new char[buffer_size];
        tail_offset = 0;
    }

    ~Pending_buffer()
    {
        delete[] buffer;
    }

    // Reserve write space; the caller holds the lock.
    inline void reserve_offset(const std::uint32_t &size, std::uint32_t &reserved_offset)
    {
        reserved_offset = tail_offset;
        tail_offset += size + sizeof(std::uint32_t) * 2;
        if (tail_offset > buffer_size)
        {
            LOG(ERROR) << "reserve_offset failed after reserve: " << size << ", new tail_offset is: " << tail_offset << ", larger than buffer_size: " << buffer_size;
            exit(-1);
        }
    }

    void copy_from_socket(const std::uint64_t &socket_fd, const std::uint32_t &offset, const std::uint32_t &payload_size)
    {
        bool eof = false;

        // Read the data.
        if (io_utils::RecvData(socket_fd, buffer + offset, payload_size, &eof))
        {
        }
        else if (eof)
        {
            LOG(ERROR) << "Connection closed by peer, fd=" << socket_fd;
            exit(-1);
        }
        else
        {
            LOG(ERROR) << "Failed to read from fd " << socket_fd;
            exit(-1);
        }
    }

    // Write data into the reserved space.
    void copy_to_buffer(const std::uint32_t &lsn, std::uint32_t &offset,
                        const std::uint64_t &socket_fd, const std::uint32_t &payload_size)
    {
        if (offset + sizeof(lsn) > buffer_size)
        {
            LOG(ERROR) << "buffer overflow: offset=" << offset << ", buffer_size=" << buffer_size;
            exit(-1);
        }
        // Pack the LSN in the upper 32 bits and payload size in the lower 32 bits.
        std::uint64_t combined_value = (static_cast<std::uint64_t>(lsn) << 32) | static_cast<std::uint64_t>(payload_size);

        // Write the packed value to the buffer.
        memcpy(buffer + offset, &combined_value, sizeof(combined_value));

        copy_from_socket(socket_fd, offset + sizeof(combined_value), payload_size);

    }

    void copy_to_buffer(const std::uint32_t &lsn, std::uint32_t &offset, const std::uint32_t &payload_size, char *data)
    {
        if (offset + sizeof(lsn) > buffer_size)
        {
            LOG(ERROR) << "buffer overflow: offset=" << offset << ", buffer_size=" << buffer_size;
            exit(-1);
        }
        // Pack the LSN in the upper 32 bits and payload size in the lower 32 bits.
        *reinterpret_cast<std::uint64_t *>(buffer + offset) = (static_cast<std::uint64_t>(lsn) << 32) | static_cast<std::uint64_t>(payload_size);
        memcpy(buffer + offset + sizeof(std::uint64_t), data, payload_size);
    }
    void *get_buffer_begin_ptr()
    {
        return buffer;
    }
    std::uint32_t get_current_offset()
    {
        return tail_offset;
    }
};

class Pending_area_state
{
public:
    Pending_buffer write_buffer;

    Pending_area_state()
        : write_buffer() {}
};

// TODO: Pool pending states to reduce allocation churn; writers and replicators share each state.
// Reclaim a state after all replicators have consumed its data and no producer is writing.
// non-thread safe
class Pending_area_state_pool
{
public:
    Pending_area_state_pool()
    {
        for (std::uint32_t i = 0; i < MAX_ENTRIES_PER_BATCH; i++)
        {
            available_buffers_.push_back(new Pending_area_state());
        }
    }
    ~Pending_area_state_pool() {}

    void Get(Pending_area_state **buf)
    {
        if (available_buffers_.empty())
        {
            std::unique_ptr<Pending_area_state> new_buffer(new Pending_area_state());
            available_buffers_.push_back(new_buffer.get());
            LOG(INFO) << "Pending_area_state_pool: Allocate new buffer, "
                      << "current buffer count is " << available_buffers_.size();
        }
        *buf = available_buffers_.back();
        available_buffers_.pop_back();
    }

    void Return(Pending_area_state *buf)
    {
        buf->write_buffer.tail_offset = 0;
        available_buffers_.push_back(buf);
    }

private:
    absl::Mutex mu_;
    absl::InlinedVector<Pending_area_state *, 6> available_buffers_;
};
