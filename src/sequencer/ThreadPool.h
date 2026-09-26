#pragma once

#include <vector>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <functional>
#include <future>
#include <memory>
#include <atomic>
#include <utility>
#include "../common/CCHashTable.h"
#include "absl/synchronization/mutex.h"
#include "Worker.h"
#include <barrier>
#include "../common/FixedBarrier.h"
#include "../common/Macro.h"
#include "absl/time/time.h"

namespace Sequencer
{
    class ThreadPool
    {
    private:
        std::vector<std::unique_ptr<Worker>> workers_; // Worker-thread collection
        std::atomic<bool> task_ready_;                 // Whether the task is ready
        std::atomic<bool> pool_ready_{false};          // Whether the thread pool is ready
        std::atomic<size_t> init_count_{0};            // Count of initialized worker threads
        std::atomic<std::uint32_t> done_worker_{0};    // Count of workers that have completed work

        // Synchronization primitives
        absl::Mutex pool_mutex_;
        absl::CondVar workers_cv_; // Condition variable for worker threads waiting on tasks

        int total_workers = SHARD_SERVER_NUM;

#ifndef USE_RDMA
        FixedBarrier<SHARD_SERVER_NUM + 1> barrier_;
        FixedBarrier<SHARD_SERVER_NUM + 1> start_barrier_;
#else
        std::barrier<> barrier_{SHARD_SERVER_NUM + 1};
        std::barrier<> start_barrier_{SHARD_SERVER_NUM + 1};
#endif

        // Implement the synchronization barrier with a counter
        std::atomic<size_t> barrier_count_;    // Synchronization barrier counter
        std::atomic<bool> barrier_generation_; // Synchronization barrier generation, used to distinguish rounds

    public:
        // Constructor that creates the specified number of worker threads
        explicit ThreadPool(size_t threads, const std::vector<std::uint64_t> &sealed = {})
            : task_ready_(false), total_workers(threads), barrier_(threads + 1), start_barrier_(threads + 1),
              barrier_count_(0), barrier_generation_(false)
        {
            LOG(INFO) << "create thread pool with " << threads << " threads";
            workers_.reserve(threads);
            for (size_t i = 0; i < threads; ++i)
            {
                // Create a new Worker instance and pass the shard ID as an argument
                auto worker = std::make_unique<Worker>(i, &done_worker_, !sealed.empty(), sealed.empty() ? 0 : sealed[i]);
                // The Worker constructor initializes the shared-memory region and related data structures for this shard

                LOG(INFO) << "worker " << i << " start";

                worker->thread = std::thread([this, worker = worker.get(), id = i]
                {
                    init_cxlalloc(10, id, 16, SHARD_SERVER_NUM);
                    bool my_generation = false; // This thread's barrier generation.

                    init_count_.fetch_add(1, std::memory_order_relaxed);
                    if (init_count_.load(std::memory_order_relaxed) == total_workers)
                    {
                        pool_ready_.store(true, std::memory_order_release);
                    }

                    while (true)
                    {
                        start_barrier_.arrive_and_wait();
                        if (worker->stop.load(std::memory_order_relaxed))
                            return;
                        worker->collect_report();
                        barrier_.arrive_and_wait();
                    }
                });

                workers_.push_back(std::move(worker));
            }
        }

        // Execute a task and wake all threads to perform the same task type
        void execute_all()
        {
            start_barrier_.arrive_and_wait();
            barrier_.arrive_and_wait();
        }

        size_t size() const
        {
            return workers_.size();
        }

        bool is_pool_ready() const
        {
            return pool_ready_.load(std::memory_order_acquire);
        }

#ifndef USE_RDMA
        std::vector<std::uint64_t> sealed_cuts() const
        {
            std::vector<std::uint64_t> cuts;
            for (const auto &worker : workers_)
                cuts.push_back(worker->sealed_cut_);
            return cuts;
        }
#endif

        // Called between rounds, with every worker parked at the start barrier.
        ~ThreadPool()
        {
            {
                absl::MutexLock lock(&pool_mutex_);
                for (auto &worker : workers_)
                {
                    worker->stop = true;
                }
                workers_cv_.SignalAll();
            }

            start_barrier_.arrive_and_wait();
            for (auto &worker : workers_)
            {
                if (worker->thread.joinable())
                {
                    worker->thread.join();
                }
            }
        }
    };
}
