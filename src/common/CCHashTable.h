#pragma once

#include "stdint.h"
#include "CXLMemory.h"
#include "Macro.h"
#include <xxhash.h>

#include "DataStructure.h"
#include "Cacheline.h"
#include <boost/interprocess/offset_ptr.hpp>
#include <shared_mutex>
#include <vector>
#include "absl/time/clock.h"

struct alignas(64) ViewOfShard
{

        // Bytes in this shard that have not yet been read.
        std::uint32_t valid_size_ = 0;

        std::uint32_t last_replay_offset_ = 0;

        // Tail offset read from the index on the previous lookup.
        std::uint32_t last_tail_offset_ = 0;

        // Start of unread entries in the current CXL buffer.
        char *cxl_cur_ptr_ = nullptr;
#ifdef USE_RDMA
        uint64_t cxl_cur_offset_;
#endif
};

namespace star
{

        class CCHashTable
        {
        public:
                struct LookupCache
                {
                        const CCHashTable *index = nullptr;
                        ShardInfo *values = nullptr;
                        StateKey key = 0;
                };

                // bucket_cnt must be a power of two.
                CCHashTable(uint64_t bucket_cnt, std::size_t shard_count = SHARD_SERVER_NUM)
                    : bucket_cnt(bucket_cnt), shard_count_(shard_count)
                {
                        buckets = reinterpret_cast<CCBucket *>(cxl_memory.cxlalloc_malloc_wrapper(sizeof(CCBucket) * bucket_cnt,
                                                                                                  CXLMemory::INDEX_ALLOCATION));
                        for (int i = 0; i < bucket_cnt; i++)
                                new (&buckets[i]) CCBucket();
                }

                bool search(StateKey key, ViewOfShard *view_of_shard, std::size_t shards = SHARD_SERVER_NUM)
                {
                        CCBucket *cur_bkt = &buckets[hash(key)];
                        return cur_bkt->search(key, view_of_shard, shards);
                }

                bool search_cached(StateKey key, ViewOfShard *view_of_shard, std::size_t shards, LookupCache &cache)
                {
                        // Nodes are never removed or relocated within an index.
                        // A configuration/recovery clone has a different table
                        // address. Do not cache misses: a later round may insert
                        // this key into the same table. The caller holds its
                        // ordinary front-index reader announcement while reading
                        // tails; the cache retains only an immutable address.
                        if (cache.index != this || cache.key != key || !cache.values)
                                cache = {this, buckets[hash(key)].find_values(key), key};
                        return read_tails(cache.values, view_of_shard, shards);
                }

                // Dynamic streams need a head on their first replay, including
                // a stream added later on another shard. Heads share the cache
                // lines already read for tails; the caller holds the ordinary
                // front-index reader announcement throughout this lookup.
                template <typename ResolveHead>
                bool search_with_heads(StateKey key, ViewOfShard *views, std::size_t shards,
                                       ResolveHead &&resolve_head)
                {
                        auto *values = buckets[hash(key)].find_values(key);
                        if (!values)
                                return false;
                        clflushopt(values, sizeof(ShardInfo) * shards);
                        sfence();
                        for (std::size_t i = 0; i < shards; ++i)
                        {
                                views[i].last_tail_offset_ = values[i].cur_offset_;
                                if (views[i].last_replay_offset_ == 0 && values[i].cur_offset_ != 0)
                                        views[i].cxl_cur_ptr_ = resolve_head(i, values[i].write_stream_begin_offset_);
                        }
                        clflushopt(values, sizeof(ShardInfo) * shards);
                        sfence();
                        return true;
                }

                void insert(StateKey key)
                {
                        CCBucket *cur_bkt = &buckets[hash(key)];
                        cur_bkt->insert(key, shard_count_);
                }

                void insert_with_new_state_key(StateKey key, std::uint32_t shard_id, std::uint64_t write_stream_begin_offset, std::uint32_t cur_offset, std::size_t shards = SHARD_SERVER_NUM)
                {
                        CCBucket *cur_bkt = &buckets[hash(key)];
                        cur_bkt->insert_with_new_state_key(key, shard_id, write_stream_begin_offset, cur_offset, shards);
                }

                void update(StateKey key, std::uint32_t shard_id, std::uint64_t write_stream_begin_offset, std::uint32_t cur_offset, std::size_t shards = SHARD_SERVER_NUM)
                {
                        CCBucket *cur_bkt = &buckets[hash(key)];
                        cur_bkt->update(key, shard_id, write_stream_begin_offset, cur_offset, shards);
                }

                void remove(StateKey key, std::uint32_t shard_id)
                {
                        CCBucket *cur_bkt = &buckets[hash(key)];
                        cur_bkt->remove(key, shard_id);
                }

                void copy_to(CCHashTable &destination)
                {
                        CHECK_EQ(bucket_cnt, destination.bucket_cnt);
                        CHECK_LE(shard_count_, destination.shard_count_);
                        for (std::size_t i = 0; i < bucket_cnt; ++i)
                                buckets[i].copy_to(destination.buckets[i], shard_count_, destination.shard_count_);
                        sfence();
                }

                std::size_t shard_count() const { return shard_count_; }

                // Recovery and maintenance use private snapshots while writers
                // are stopped; normal lookups keep the existing layout and path.
                template <typename Visitor>
                void visit(Visitor &&visitor)
                {
                        for (std::size_t i = 0; i < bucket_cnt; ++i)
                        {
                                CHECK_EQ(reinterpret_cast<std::uintptr_t>(&buckets[i]) % CACHELINE_SIZE, 0u);
                                buckets[i].visit(shard_count_, visitor);
                        }
                }

                void restore(StateKey key, std::size_t shard, const ShardInfo &value)
                {
                        buckets[hash(key)].restore(key, shard, shard_count_, value);
                }

        private:
                static bool read_tails(ShardInfo *values, ViewOfShard *view_of_shard, std::size_t shards)
                {
                        if (!values)
                                return false;
                        clflushopt(values, sizeof(ShardInfo) * shards);
                        sfence();
                        for (std::size_t i = 0; i < shards; ++i)
                                view_of_shard[i].last_tail_offset_ = values[i].cur_offset_;
                        clflushopt(values, sizeof(ShardInfo) * shards);
                        sfence();
                        return true;
                }

                class alignas(64) CCNode
                {
                public:
                        explicit CCNode(StateKey state_key) : key(state_key), next(nullptr) {}

                        static CCNode *create(StateKey key, std::size_t shards,
                                              std::uint32_t shard_id = 0,
                                              std::uint64_t begin = 0, std::uint32_t tail = 0)
                        {
                                // The trailing array contains exactly the current shard count.
                                auto *storage = cxl_memory.cxlalloc_malloc_wrapper(
                                    sizeof(CCNode) + sizeof(ShardInfo) * shards, CXLMemory::INDEX_ALLOCATION);
                                auto *node = new (storage) CCNode(key);
                                new (node->shard_info()) ShardInfo[shards]();
                                node->shard_info()[shard_id].write_stream_begin_offset_ = begin;
                                node->shard_info()[shard_id].cur_offset_ = tail;
                                return node;
                        }

                        ShardInfo *shard_info()
                        {
                                return reinterpret_cast<ShardInfo *>(reinterpret_cast<char *>(this) + sizeof(CCNode));
                        }

                        StateKey key;
                        boost::interprocess::offset_ptr<CCNode> next;
                };

                class alignas(CACHELINE_SIZE) CCBucket
                {
                public:
                        CCBucket()
                        {
                                first_node = nullptr;
                        }

                        bool search(StateKey key, ViewOfShard *view_of_shard, std::size_t shards)
                        {
                                return read_tails(find_values(key), view_of_shard, shards);
                        }

                        ShardInfo *find_values(StateKey key)
                        {
                                auto *node = find_node(key);
                                return node ? node->shard_info() : nullptr;
                        }

                        void insert(StateKey key, std::size_t shards)
                        {
                                CCNode *node = nullptr;

                                std::unique_lock<std::shared_mutex> lock(rw_mutex);
                                node = find_node(key);
                                if (node == nullptr)
                                {
                                        node = CCNode::create(key, shards);
                                        node->next = first_node.get(); // insert node into bucket head
                                        clwb(node, sizeof(CCNode) + sizeof(ShardInfo) * shards);
                                        first_node = node;
                                        clwb(&first_node, sizeof(boost::interprocess::offset_ptr<CCNode>));
                                }
                                else
                                {
                                        // TODO. should differentiate between unique vs. nonunique indexes.
                                }
                        }

                        void insert_with_new_state_key(StateKey key, std::uint32_t shard_id, std::uint64_t write_stream_begin_offset, std::uint32_t cur_offset, std::size_t shards)
                        {
                                CCNode *node = nullptr;
                                std::unique_lock<std::shared_mutex> lock(rw_mutex);
                                node = find_node(key);
                                if (node == nullptr)
                                {
                                        node = CCNode::create(key, shards, shard_id, write_stream_begin_offset, cur_offset);
                                        node->next = first_node.get(); // insert node into bucket head
                                        clwb(node, sizeof(CCNode) + sizeof(ShardInfo) * shards);
                                        first_node = node;
                                        clwb(&first_node, sizeof(boost::interprocess::offset_ptr<CCNode>));
                                }
                                else
                                {
                                        // Another shard may already own a stream for this
                                        // key. Initialize this shard's slot independently.
                                        auto &value = node->shard_info()[shard_id];
                                        if (value.cur_offset_ == 0)
                                                value.write_stream_begin_offset_ = write_stream_begin_offset;
                                        value.cur_offset_ += cur_offset;
                                        clwb(&value, sizeof(value));
                                }
                        }

                        // Insert missing keys as part of the update.
                        void update(StateKey key, std::uint32_t shard_id, std::uint64_t write_stream_begin_offset, std::uint32_t cur_offset, std::size_t shards)
                        {
                                // Existing keys advance cur_offset.
                                // A new key also records its initial write-stream offset.
                                CCNode *node = nullptr;

                                std::shared_lock<std::shared_mutex> lock(rw_mutex);
                                node = find_node(key);
                                if (node == nullptr)
                                {
                                        node = CCNode::create(key, shards, shard_id, write_stream_begin_offset, cur_offset);
                                        {
                                                std::unique_lock<std::shared_mutex> lock(rw_mutex);
                                                node->next = first_node.get(); // insert node into bucket head
                                                clwb(node, sizeof(CCNode) + sizeof(ShardInfo) * shards);
                                                first_node = node;
                                                clwb(&first_node, sizeof(boost::interprocess::offset_ptr<CCNode>));
                                        }
                                }
                                else
                                {
                                        // TODO. should differentiate between unique vs. nonunique indexes.
                                        node->shard_info()[shard_id].cur_offset_ += cur_offset; // insert can fail if the row already exists
                                        clwb(&node->shard_info()[shard_id], sizeof(ShardInfo));
                                }
                        }

                        void remove(StateKey key, std::uint32_t shard_id)
                        {
                                CCNode *node = nullptr;
                                node = find_node(key);
                                if (node != nullptr)
                                {
                                        node->shard_info()[shard_id] = ShardInfo(0, 0); // remove row from node
                                        clwb(&node->shard_info()[shard_id], sizeof(ShardInfo));
                                }
                        }

                        void copy_to(CCBucket &destination, std::size_t old_count, std::size_t new_count)
                        {
                                clflushopt(&first_node, sizeof(first_node));
                                sfence();
                                auto *source = first_node.get();
                                clflushopt(&first_node, sizeof(first_node));
                                sfence();
                                while (source)
                                {
                                        const auto bytes = sizeof(CCNode) + old_count * sizeof(ShardInfo);
                                        clflushopt(source, bytes);
                                        sfence();
                                        const auto key = source->key;
                                        auto *next = source->next.get();
                                        std::vector<ShardInfo> snapshot(source->shard_info(), source->shard_info() + old_count);
                                        clflushopt(source, bytes);
                                        sfence();
                                        auto *copy = CCNode::create(key, new_count);
                                        std::copy(snapshot.begin(), snapshot.end(), copy->shard_info());
                                        copy->next = destination.first_node.get();
                                        destination.first_node = copy;
                                        clwb(copy, sizeof(CCNode) + new_count * sizeof(ShardInfo));
                                        source = next;
                                }
                                clwb(&destination.first_node, sizeof(destination.first_node));
                        }

                        template <typename Visitor>
                        void visit(std::size_t shards, Visitor &visitor)
                        {
                                clflushopt(&first_node, sizeof(first_node));
                                sfence();
                                auto *node = first_node.get();
                                while (node)
                                {
                                        CHECK_EQ(reinterpret_cast<std::uintptr_t>(node) % CACHELINE_SIZE, 0u);
                                        CHECK_EQ(reinterpret_cast<std::uintptr_t>(node->shard_info()) % CACHELINE_SIZE, 0u);
                                        clflushopt(node, sizeof(CCNode) + shards * sizeof(ShardInfo));
                                        sfence();
                                        std::vector<ShardInfo> values(node->shard_info(), node->shard_info() + shards);
                                        visitor(node->key, values);
                                        node = node->next.get();
                                }
                        }

                        void restore(StateKey key, std::size_t shard, std::size_t shards, const ShardInfo &value)
                        {
                                auto *node = find_node(key);
                                if (!node)
                                {
                                        node = CCNode::create(key, shards);
                                        node->next = first_node.get();
                                        first_node = node;
                                        clwb(node, sizeof(CCNode) + shards * sizeof(ShardInfo));
                                        clwb(&first_node, sizeof(first_node));
                                }
                                node->shard_info()[shard] = value;
                                clwb(node->shard_info() + shard, sizeof(ShardInfo));
                        }

                private:
                        CCNode *find_node(StateKey key)
                        {
                                CCNode *cur_node = nullptr;
                                {
                                        clflush(&first_node, sizeof(boost::interprocess::offset_ptr<CCNode>));
                                        cur_node = first_node.get();
                                }

                                while (cur_node != nullptr)
                                {
                                        if (cur_node->key == key)
                                        {
                                                return cur_node;
                                        }
                                        cur_node = cur_node->next.get();
                                }
                                return nullptr;
                        }

                        boost::interprocess::offset_ptr<CCNode> first_node;
                        std::shared_mutex rw_mutex;
                };

                static_assert(sizeof(CCNode) == CACHELINE_SIZE);
                static_assert(sizeof(CCBucket) == CACHELINE_SIZE);

                inline uint32_t hash(StateKey key)
                {
                        return XXH32(&key, sizeof(key), 0) & (bucket_cnt - 1); // bucket_cnt must be a power of two.
                }

                boost::interprocess::offset_ptr<CCBucket> buckets;
                uint64_t bucket_cnt;
                std::size_t shard_count_;
        };

} // namespace star
