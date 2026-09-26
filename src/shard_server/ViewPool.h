#pragma once

#include <mutex>
#include <thread>
#include <list>
#include <unordered_map>
#include <memory>
#include <functional>
#include <atomic>
#include <condition_variable>
#include "../common/Macro.h"
#include <absl/synchronization/mutex.h>
#include <absl/container/inlined_vector.h>
#include <glog/logging.h>

class ViewBuffer
{
public:
    alignas(64) char *buffer_[SHARD_SERVER_NUM];
    ViewBuffer()
    {
        for (std::uint32_t i = 0; i < SHARD_SERVER_NUM; i++)
        {
            buffer_[i] = (char *)aligned_alloc(64, VIEW_BUFFER_SIZE);
        }
    }
    ~ViewBuffer() {}
};

// thread safe
class ViewBufferPool
{
public:
    ViewBufferPool()
    {
        for (std::uint32_t i = 0; i < VIEW_BUFFER_POOL_SIZE; i++)
        {
            available_buffers_.push_back(new ViewBuffer());
        }
    }
    ~ViewBufferPool() {}

    void Get(ViewBuffer **buf)
    {
        absl::MutexLock lock(&mu_);
        if (available_buffers_.empty())
        {
            std::unique_ptr<ViewBuffer> new_buffer(new ViewBuffer());
            available_buffers_.push_back(new_buffer.get());
            LOG(INFO) << "ViewBufferPool: Allocate new buffer, "
                      << "current buffer count is " << available_buffers_.size();
        }
        *buf = available_buffers_.back();
        available_buffers_.pop_back();
    }

    void Return(ViewBuffer *buf)
    {
        absl::MutexLock lock(&mu_);
        available_buffers_.push_back(buf);
    }

private:
    absl::Mutex mu_;
    absl::InlinedVector<ViewBuffer *, VIEW_BUFFER_POOL_SIZE> available_buffers_;
};
