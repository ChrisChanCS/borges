#pragma once
#include <atomic>
#include <cstdint>
#include <vector>
#include <iostream>
#include "../common/Macro.h"
#include "Batch.h"

class SPMCBuffer
{
private:
    static constexpr size_t BUFFER_SIZE = MAX_BATCH_NUM_BUFFER; // Ring buffer capacity

    std::shared_ptr<Batch> buffer_[BUFFER_SIZE];               // Ring buffer holding batches.
    std::atomic<size_t> producer_index_;                       // Current producer position.
    std::atomic<size_t> consumer_indices_[REPLICATOR_NUM + 1]; // Current position of each consumer.

public:
    explicit SPMCBuffer()
    {
        // Initialize all batches.
        for (size_t i = 0; i < BUFFER_SIZE; ++i)
        {
            buffer_[i] = std::make_shared<Batch>();
        }

        producer_index_.store(0, std::memory_order_release);

        // Initialize consumer positions.
        for (size_t i = 0; i < REPLICATOR_NUM + 1; ++i)
        {
            consumer_indices_[i].store(0, std::memory_order_release);
        }
    }

    ~SPMCBuffer()
    {
        // Release all batches in the buffer.
        for (auto &batch : buffer_)
        {
            batch.reset();
        }
    }

    // Return the batch currently writable by the producer.
    std::shared_ptr<Batch> get_producer_batch() const
    {
        size_t current_index = producer_index_.load(std::memory_order_acquire);
        return buffer_[current_index % BUFFER_SIZE];
    }

    // Advance the producer and return the next writable batch.
    std::shared_ptr<Batch> advance_producer()
    {
        size_t old_index = producer_index_.fetch_add(1, std::memory_order_acquire);
        size_t new_index = old_index + 1;

        // Check whether the ring buffer is full.
        for (size_t i = 0; i < REPLICATOR_NUM + 1; ++i)
        {
            size_t consumer_index = consumer_indices_[i].load(std::memory_order_acquire);
            while ((new_index - consumer_index) >= BUFFER_SIZE)
            {
                // Wait for consumers to release space in the full buffer.
                LOG(WARNING) << "Ring buffer is full, waiting for consumers";
                // Back off while waiting for a consumer.
                std::this_thread::yield();
                consumer_index = consumer_indices_[i].load(std::memory_order_acquire);
            }
        }

        return buffer_[new_index % BUFFER_SIZE];
    }

    void advance_producer_index()
    {
        size_t old_index = producer_index_.fetch_add(1, std::memory_order_seq_cst);
        size_t new_index = old_index + 1;

        // Check whether the ring buffer is full.
        for (size_t i = 0; i < REPLICATOR_NUM + 1; ++i)
        {
            size_t consumer_index = consumer_indices_[i].load(std::memory_order_acquire);
            if ((new_index - consumer_index) >= BUFFER_SIZE)
            {
                // Wait for consumers to release space in the full buffer.
                LOG(ERROR) << "Ring buffer is full, waiting for consumers";
                // Back off while waiting for a consumer.
                exit(-1);
            }
        }
    }

    // Return the batch currently readable by this consumer.
    std::shared_ptr<Batch> get_consumer_batch_with_state(uint32_t consumer_id) const
    {
        if (consumer_id >= REPLICATOR_NUM + 1)
        {
            LOG(ERROR) << "Invalid consumer_id: " << consumer_id;
            exit(-1);
            return nullptr;
        }

        size_t current_index = consumer_indices_[consumer_id].load(std::memory_order_acquire);
        if (buffer_[current_index % BUFFER_SIZE] == nullptr)
        {
            LOG(ERROR) << "Buffer is empty, consumer_id: " << consumer_id;
            exit(-1);
            return nullptr;
        }
        else
        {
            if (buffer_[current_index % BUFFER_SIZE]->get_state() != BatchState::READY)
            {
                return nullptr;
            }
            else
            {
                return buffer_[current_index % BUFFER_SIZE];
            }
        }
    }

    std::shared_ptr<Batch> get_consumer_batch(uint32_t consumer_id) const
    {
        if (consumer_id >= REPLICATOR_NUM + 1)
        {
            LOG(ERROR) << "Invalid consumer_id: " << consumer_id;
            exit(-1);
        }

        size_t current_index = consumer_indices_[consumer_id].load(std::memory_order_acquire);
        return buffer_[current_index % BUFFER_SIZE];
    }

    // Advance the consumer and return its next batch.
    void advance_consumer(uint32_t consumer_id)
    {
        if (consumer_id >= REPLICATOR_NUM + 1)
        {
            LOG(ERROR) << "Invalid consumer_id: " << consumer_id;
            exit(-1);
        }

        size_t cur_index = consumer_indices_[consumer_id].load(std::memory_order_acquire);
        size_t producer_pos = producer_index_.load(std::memory_order_acquire);

        if (cur_index >= producer_pos)
        {
            LOG(ERROR) << "consumer_id: " << consumer_id << " cur_index: " << cur_index << " producer_pos: " << producer_pos;
            exit(-1);
            return;
        }
        consumer_indices_[consumer_id].store(cur_index + 1, std::memory_order_release);
        return;
    }

    // Check whether this consumer has another batch to consume.
    bool can_consume(uint32_t consumer_id) const
    {
        if (consumer_id >= REPLICATOR_NUM + 1)
        {
            LOG(ERROR) << "Invalid consumer_id: " << consumer_id;
            exit(-1);
        }

        size_t consumer_pos = consumer_indices_[consumer_id].load(std::memory_order_acquire);
        size_t producer_pos = producer_index_.load(std::memory_order_acquire);

        return consumer_pos < producer_pos;
    }

    // Return the producer index.
    size_t get_producer_index() const
    {
        return producer_index_.load(std::memory_order_acquire);
    }

    // Return this consumer's index.
    size_t get_consumer_index(uint32_t consumer_id) const
    {
        if (consumer_id >= REPLICATOR_NUM + 1)
        {
            LOG(ERROR) << "Invalid consumer_id: " << consumer_id;
            return 0;
        }
        return consumer_indices_[consumer_id].load(std::memory_order_acquire);
    }
};
