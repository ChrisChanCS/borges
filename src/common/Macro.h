#pragma once

#include "Logger.h"
#include <nlohmann/json.hpp>
#include <fstream>
#ifndef USE_RDMA
#include <cxlalloc.h>
#else
#include "rdma/RdmaRegion.h"
#endif
#include <glog/logging.h>
#include <shared_mutex>

// Size used by the memory-copy probe, separate from the shared VM backing region.
#define CXL_CAPACITY size_t(1024 * 1024 * 1024) * 4 // 4 GiB

#define CLOSE_LOOP_TEST true // Closed-loop mode sends individual responses without per-client batching.

constexpr std::uint64_t LINK_POINTER_MAGIC = 0xDEADBEEFDEADBEEF;

// configure ycsb
#define KEY_CNT_OF_SHARD 102400
#define YCSB_HOTTER_KEY_CNT (SEGMENT_TEST ? 30 : 300)
#define YCSB_HOT_KEY_CNT (SEGMENT_TEST ? 70 : 700)
#define HOTTER_SEG_NUM 256
#define HOT_SEG_NUM 64 // Segments reserved per hot key.
#define COLD_SEG_NUM 1 // One segment per cold key.

// configure retwis
#define RETWIS_SEGMENT_SIZE 384 // 320+64 (entry + link pointer), 1 post write entry or 5 timeline write entry
#define RETWIS_ENTRY_MAX_SIZE 320
#define TIMELINE_SEG_NUM 10 // 10 segments per timeline, holding 50 write entries in total.
#define POST_SEG_NUM 1      // One segment per post, holding one write entry.

#define YCSB_NEW_KEY_SEGMENT_SIZE 1216 // 1152+64 (ycsb_state + link pointer)

#define REPLICATOR_NUM (uint8_t(2))
int WORKLOAD = 0;
int CLIENT_NUM = 0;
int IO_WORKER_NUM = 0;
int MAX_ENTRIES_PER_BATCH = 1;
std::shared_mutex BATCH_SIZE_MUTEX;
int YCSB_OPTION = 0;
bool LOG_ENABLE = false;
bool SCALE_OUT_STREAM = false;
bool SEGMENT_TEST = false;
double SCALE_OUT_RATE = 0;
int INIT_SEGMENT_CNT = 0;
double ZIPF_THETA = 0.99;
int REQUEST_WORKER_NUM = 0;
int BACKOFF_BASE = 200;                 // 50us
std::uint32_t SEGMENT_SIZE = 2112;      // 2KB+64 (entry + link pointer)
std::uint32_t SMALL_SEGMENT_SIZE = 320; // 320B
std::uint32_t LARGE_SEGMENT_SIZE = 2112;

// global configuration
#define SHARD_SERVER_NUM (uint8_t(2))
#define MAX_THREAD_NUM (uint8_t(64))

#define MAX_THREAD_CNT_PER_PROCESS (uint8_t(10))
#define READER_POOL_SIZE 1024

#define MAX_BATCH_NUM 8
#define MAX_BATCH_NUM_BUFFER 1000
#define ENTRY_MAX_SIZE 128 // 128B
#define WRITE_STREAM_SIZE size_t(1024) * 1024 * 512 // 512 MiB per replicator; each key has its own write stream.
#define READ_STREAM_SIZE size_t(1024) * 1024 * 64   // 64 MiB per replicator; all keys share one read stream.
#define PENDING_AREA_STATE_POOL_SIZE 128            // 128 pending states, each containing two pending buffers.
#define PENDING_BUFFER_SIZE 1024 * 1024             // 1MB
#define VIEW_BUFFER_SIZE 1024 * 1024                // 1 MiB per buffer; one buffer per shard in each view.
#define VIEW_BUFFER_POOL_SIZE 256                   // 256 views, each containing one buffer per shard.

// configure shared memory between sequencer and shard server
/*configure cxl root index*/
#define COMMIT_RING_BUFFER_CAPACITY (10 * 1024 * 1024)                         // 10 Mi entries, 80 MiB per shard.
#define COMMIT_RING_BUFFER_ROOT_INDEX 0                                        // One committer-owned ring buffer per shard, read by the sequencer.
#define COMMIT_BUFFER_METADATA_ROOT_INDEX SHARD_SERVER_NUM                     // One commit-buffer metadata record per shard.
#define CUT_REGION_ROOT_INDEX 2 * SHARD_SERVER_NUM                             // One cut region per shard/replicator pair.
#define GSN_BUFFER_ROOT_INDEX (2 + REPLICATOR_NUM) * SHARD_SERVER_NUM          // Two global-cut copies shared by all shards.
#define TAIL_HASH_TABLE_ROOT_INDEX (2 + REPLICATOR_NUM) * SHARD_SERVER_NUM + 2 // Two shared tail indexes: one for sequencer updates and one for shard reads.
#define SWITCH_FLAG_ROOT_INDEX (4 + REPLICATOR_NUM) * SHARD_SERVER_NUM + 2     // One switch flag per request worker, plus one sequencer flag.

#define INDEX_ROUND_NUMBER_ROOT_INDEX (4 + REPLICATOR_NUM + REQUEST_WORKER_NUM) * SHARD_SERVER_NUM + 3 // One round number per index, identifying its corresponding global cut.
#define ROUND_NUMBER_ROOT_INDEX (4 + REPLICATOR_NUM + REQUEST_WORKER_NUM) * SHARD_SERVER_NUM + 5       // Shared round number recording the sequencer's committed global cut.
#define READ_STREAM_ROOT_INDEX (4 + REPLICATOR_NUM + REQUEST_WORKER_NUM) * SHARD_SERVER_NUM + 6        // One read-entry stream per shard/replicator pair.
// for debug
#define STREAM_ROOT_INDEX (4 + 2 * REPLICATOR_NUM + REQUEST_WORKER_NUM) * SHARD_SERVER_NUM + 6 // One write-stream region per shard/replicator pair.
#define LEASE_ROOT_INDEX (STREAM_ROOT_INDEX + SHARD_SERVER_NUM * REPLICATOR_NUM)
#define CLUSTER_ROOT_INDEX (LEASE_ROOT_INDEX + 1)

/*configure report and commit*/
#define REPORT_RESERVED_SIZE 800       // 800 {key, offset} pairs in a batch
#define COMMIT_BUFFER_SIZE 1024 * 1024 // 1MB
#define GSN_SET_CNT 1024 * 1024 * 10   // Space for 10 Mi global-cut rounds.
#define BUCKET_CNT 1024 * 1024         // 1 Mi buckets for the commit-tail index.

// configure sequencer
#define LOCAL_CUT_BUFFER_SIZE 1024 * 1024 * 1024 // 1GB
#define ORDERING_INTERVAL_MICROSECONDS 0         // No delay between ordering rounds.
#define COMMIT_TAIL_HASH_TABLE_CANDIDATE_NUM 2   // Number of alternating commit-tail indexes.

using StateKey = std::uint32_t;
// 64 bits: upper 32 hold the stream offset within the shard region; lower 32 hold its tail.
using Offset = std::uint64_t;
using OperationId = std::uint32_t;

enum class WorkloadType
{
    ycsb,
    lock,
    counter,
};

// for logging
std::atomic<int> request_cnt = 0;

const std::string config_file_path = "config.json";

static constexpr uint64_t default_cxl_mem_size = (((1024 * 1024 * 1024) + 128 * 1024 * 1024) * (uint64_t)7);

#ifndef USE_RDMA
void init_cxlalloc(uint64_t threads_num_per_host, uint64_t thread_id, uint64_t hosts_num, uint64_t host_id)
{
    cxlalloc_init_backend("ivshmem");
    cxlalloc_init("r", default_cxl_mem_size, thread_id + threads_num_per_host * host_id, threads_num_per_host * hosts_num, host_id, hosts_num);
    LOG(INFO) << "init cxlalloc with size " << default_cxl_mem_size << " bytes, global thread_id: " << thread_id + threads_num_per_host * host_id << ", hosts_num: " << hosts_num << ", host_id: " << host_id;
}

// The initial sequencer keeps allocator host ID 2. Later shards use subsequent
// IDs, so a new shard never reuses the sequencer's allocator incarnation.
inline void init_shard_cxlalloc(uint64_t thread_id, uint64_t shard_id)
{
    const auto host = shard_id < SHARD_SERVER_NUM ? shard_id : shard_id + 1;
    init_cxlalloc(10, thread_id, 16, host);
}
#else
// Under RDMA, connection is done in main() via g_rdma.connect()
// This stub is called by threads but does nothing (connection is process-wide)
inline void init_cxlalloc(uint64_t, uint64_t, uint64_t, uint64_t) {}
inline void init_shard_cxlalloc(uint64_t, uint64_t) {}
#endif
