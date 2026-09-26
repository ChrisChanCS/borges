// Simple RDMA log-append validator.
// Connects to the RDMA memory server, reads back key metadata,
// and checks that the append-only workload completed correctly.
//
// Usage: ./validate_append <mem_node_ip> <rdma_port> <expected_requests>

#include "RdmaRegion.h"
#include <glog/logging.h>
#include <cstdint>
#include <cstring>
#include <iostream>

extern RdmaRegion g_rdma;

// Must match Macro.h compile-time constants
static constexpr int SHARD_SERVER_NUM = 2;
static constexpr int REPLICATOR_NUM = 2;

// Root index formulas (from Macro.h)
static constexpr int COMMIT_RING_BUFFER_ROOT_INDEX = 0;
static constexpr int COMMIT_BUFFER_METADATA_ROOT_INDEX = SHARD_SERVER_NUM;
static constexpr int CUT_REGION_ROOT_INDEX = 2 * SHARD_SERVER_NUM;
// GSN_BUFFER at (2 + REPLICATOR_NUM) * SHARD_SERVER_NUM = 8
static constexpr int GSN_BUFFER_ROOT_INDEX = (2 + REPLICATOR_NUM) * SHARD_SERVER_NUM;

// REQUEST_WORKER_NUM=1 from config
static constexpr int REQUEST_WORKER_NUM = 1;
// ROUND_NUMBER at (4 + REPLICATOR_NUM + REQUEST_WORKER_NUM) * SHARD_SERVER_NUM + 5
static constexpr int ROUND_NUMBER_ROOT_INDEX =
    (4 + REPLICATOR_NUM + REQUEST_WORKER_NUM) * SHARD_SERVER_NUM + 5;

int main(int argc, char* argv[]) {
    google::InitGoogleLogging(argv[0]);
    FLAGS_logtostderr = true;

    if (argc < 4) {
        std::cerr << "Usage: " << argv[0]
                  << " <mem_node_ip> <rdma_port> <expected_requests>\n";
        return 1;
    }

    const char* ip = argv[1];
    int port = atoi(argv[2]);
    int expected = atoi(argv[3]);

    g_rdma.connect(ip, port);
    LOG(INFO) << "Connected to RDMA memory server at " << ip << ":" << port;

    void* staging = g_rdma.get_staging_buf();
    int errors = 0;

    // --- 1. Check cut regions (replicator reports) ---
    std::cout << "\n=== Cut Regions (Replicator Reports) ===\n";
    for (int shard = 0; shard < SHARD_SERVER_NUM; shard++) {
        for (int rep = 0; rep < REPLICATOR_NUM; rep++) {
            int root_idx = CUT_REGION_ROOT_INDEX + shard * REPLICATOR_NUM + rep;
            uint64_t offset = g_rdma.get_root(root_idx);
            g_rdma.read(staging, offset, sizeof(uint64_t));
            uint64_t value;
            std::memcpy(&value, staging, sizeof(uint64_t));
            uint32_t lsn = value >> 32;
            uint32_t key_num = value & 0xFFFFFFFF;
            std::cout << "  shard=" << shard << " rep=" << rep
                      << " : lsn=" << lsn << ", key_num=" << key_num << "\n";

            if (shard == 0) {
                if (lsn != (uint32_t)expected) {
                    std::cerr << "  ERROR: expected lsn=" << expected
                              << " got " << lsn << "\n";
                    errors++;
                }
            }
            // shard 1 may have lsn=0 if no client sent to it
        }
    }

    // --- 2. Check commit buffer metadata ---
    std::cout << "\n=== Commit Buffer Metadata ===\n";
    for (int shard = 0; shard < SHARD_SERVER_NUM; shard++) {
        int root_idx = COMMIT_BUFFER_METADATA_ROOT_INDEX + shard;
        uint64_t offset = g_rdma.get_root(root_idx);
        g_rdma.read(staging, offset, sizeof(uint32_t));
        uint32_t key_num;
        std::memcpy(&key_num, staging, sizeof(uint32_t));
        std::cout << "  shard=" << shard << " : committed_key_num=" << key_num << "\n";

        if (shard == 0) {
            // For append-only with state_key=0, each request produces 1 key entry
            if (key_num != (uint32_t)expected) {
                std::cerr << "  ERROR: expected key_num=" << expected
                          << " got " << key_num << "\n";
                errors++;
            }
        }
    }

    // --- 3. Check commit buffer entries (sample first and last) ---
    std::cout << "\n=== Commit Buffer Entries (shard 0, sample) ===\n";
    {
        int root_idx = COMMIT_RING_BUFFER_ROOT_INDEX + 0;
        uint64_t buf_offset = g_rdma.get_root(root_idx);

        // Read first entry
        g_rdma.read(staging, buf_offset, sizeof(uint64_t));
        uint64_t first_entry;
        std::memcpy(&first_entry, staging, sizeof(uint64_t));
        uint32_t first_key = first_entry >> 32;
        uint32_t first_tail = first_entry & 0xFFFFFFFF;
        std::cout << "  entry[0]: state_key=" << first_key
                  << ", tail_offset=" << first_tail << "\n";

        // Check metadata key_num to know how many entries
        int meta_idx = COMMIT_BUFFER_METADATA_ROOT_INDEX + 0;
        uint64_t meta_offset = g_rdma.get_root(meta_idx);
        g_rdma.read(staging, meta_offset, sizeof(uint32_t));
        uint32_t total_keys;
        std::memcpy(&total_keys, staging, sizeof(uint32_t));

        if (total_keys > 0) {
            // Read last entry
            g_rdma.read(staging, buf_offset + (total_keys - 1) * sizeof(uint64_t),
                        sizeof(uint64_t));
            uint64_t last_entry;
            std::memcpy(&last_entry, staging, sizeof(uint64_t));
            uint32_t last_key = last_entry >> 32;
            uint32_t last_tail = last_entry & 0xFFFFFFFF;
            std::cout << "  entry[" << total_keys - 1 << "]: state_key=" << last_key
                      << ", tail_offset=" << last_tail << "\n";

            // All entries should have state_key=0 for append-only
            if (first_key != 0 || last_key != 0) {
                std::cerr << "  ERROR: state_key should be 0 for append-only\n";
                errors++;
            }

            // Tail offsets should be monotonically increasing
            if (last_tail <= first_tail && total_keys > 1) {
                std::cerr << "  ERROR: last tail_offset (" << last_tail
                          << ") should be > first (" << first_tail << ")\n";
                errors++;
            }

            // Verify monotonicity by sampling every 1000th entry
            std::cout << "\n  Monotonicity check (every 1000th entry):\n";
            uint32_t prev_tail = 0;
            bool monotonic = true;
            for (uint32_t i = 0; i < total_keys; i += 1000) {
                g_rdma.read(staging, buf_offset + i * sizeof(uint64_t),
                            sizeof(uint64_t));
                uint64_t entry;
                std::memcpy(&entry, staging, sizeof(uint64_t));
                uint32_t key = entry >> 32;
                uint32_t tail = entry & 0xFFFFFFFF;
                std::cout << "    entry[" << i << "]: key=" << key
                          << " tail=" << tail << "\n";
                if (tail < prev_tail) {
                    std::cerr << "    ERROR: non-monotonic at index " << i << "\n";
                    monotonic = false;
                    errors++;
                }
                prev_tail = tail;
            }
            if (monotonic) {
                std::cout << "  Monotonicity: OK\n";
            }
        }
    }

    // --- 4. Check round number ---
    std::cout << "\n=== Round Number ===\n";
    {
        uint64_t offset = g_rdma.get_root(ROUND_NUMBER_ROOT_INDEX);
        g_rdma.read(staging, offset, sizeof(uint64_t));
        uint64_t round;
        std::memcpy(&round, staging, sizeof(uint64_t));
        std::cout << "  round_number=" << round << "\n";
        if (round == 0) {
            std::cerr << "  ERROR: round_number should be > 0 after processing\n";
            errors++;
        }
    }

    // --- 5. Check GSN buffer (latest round should have shard 0 = expected) ---
    std::cout << "\n=== GSN Buffer (latest round) ===\n";
    {
        uint64_t rn_offset = g_rdma.get_root(ROUND_NUMBER_ROOT_INDEX);
        g_rdma.read(staging, rn_offset, sizeof(uint64_t));
        uint64_t round;
        std::memcpy(&round, staging, sizeof(uint64_t));

        if (round > 0) {
            uint64_t gsn_base = g_rdma.get_root(GSN_BUFFER_ROOT_INDEX);
            // GSNSet is alignas(64), contains SHARD_SERVER_NUM GSN entries each 64 bytes
            // Total size per GSNSet = SHARD_SERVER_NUM * 64
            size_t gsn_set_size = SHARD_SERVER_NUM * 64;
            uint64_t gsn_offset = gsn_base + round * gsn_set_size;
            for (int shard = 0; shard < SHARD_SERVER_NUM; shard++) {
                g_rdma.read(staging, gsn_offset + shard * 64, sizeof(uint64_t));
                uint64_t gsn_val;
                std::memcpy(&gsn_val, staging, sizeof(uint64_t));
                std::cout << "  round=" << round << " shard=" << shard
                          << " gsn=" << gsn_val << "\n";
                if (shard == 0 && gsn_val != (uint64_t)expected) {
                    std::cerr << "  ERROR: expected gsn=" << expected
                              << " got " << gsn_val << "\n";
                    errors++;
                }
            }
        }
    }

    // --- Summary ---
    std::cout << "\n=== VALIDATION RESULT ===\n";
    if (errors == 0) {
        std::cout << "PASS: All " << expected
                  << " appends verified correctly.\n";
    } else {
        std::cout << "FAIL: " << errors << " error(s) found.\n";
    }

    return errors > 0 ? 1 : 0;
}
