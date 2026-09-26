// Quick RDMA connectivity and data integrity test
#include "RdmaRegion.h"
#include <cstdio>
#include <cstring>
#include <chrono>
#include <algorithm>
#include <glog/logging.h>

int main(int argc, char* argv[]) {
    google::InitGoogleLogging(argv[0]);
    FLAGS_logtostderr = true;

    const char* ip = "10.31.1.158";
    int port = 9999;
    if (argc > 1) ip = argv[1];
    if (argc > 2) port = atoi(argv[2]);

    LOG(INFO) << "=== RDMA Test: connecting to " << ip << ":" << port << " ===";

    // 1. Connect
    g_rdma.connect(ip, port);
    LOG(INFO) << "PASS: connect";

    // 2. Alloc
    uint64_t off1 = g_rdma.alloc(4096);
    uint64_t off2 = g_rdma.alloc(4096);
    LOG(INFO) << "Allocated off1=" << off1 << " off2=" << off2;
    CHECK(off1 != off2);
    LOG(INFO) << "PASS: alloc";

    // 3. Set/Get root
    g_rdma.set_root(0, off1);
    g_rdma.set_root(1, off2);
    uint64_t r0 = g_rdma.get_root(0);
    uint64_t r1 = g_rdma.get_root(1);
    CHECK(r0 == off1) << "root 0 mismatch: " << r0 << " vs " << off1;
    CHECK(r1 == off2) << "root 1 mismatch: " << r1 << " vs " << off2;
    LOG(INFO) << "PASS: set_root / get_root";

    // 4. Write then read back
    void* staging = g_rdma.get_staging_buf();
    const char* test_str = "Hello RDMA from Rhodes!";
    size_t test_len = strlen(test_str) + 1;
    memcpy(staging, test_str, test_len);
    g_rdma.write(staging, off1, test_len);
    LOG(INFO) << "Wrote " << test_len << " bytes to offset " << off1;

    // Clear staging and read back
    memset(staging, 0, test_len);
    g_rdma.read(staging, off1, test_len);
    CHECK(memcmp(staging, test_str, test_len) == 0) << "Data mismatch after read!";
    LOG(INFO) << "PASS: write + read (string: \"" << (char*)staging << "\")";

    // 5. Larger write/read (4KB pattern)
    char* big_buf_w = (char*)aligned_alloc(4096, 4096);
    char* big_buf_r = (char*)aligned_alloc(4096, 4096);
    ibv_mr* mr_w = g_rdma.register_local_buffer(big_buf_w, 4096);
    ibv_mr* mr_r = g_rdma.register_local_buffer(big_buf_r, 4096);

    for (int i = 0; i < 4096; i++) big_buf_w[i] = (char)(i & 0xFF);
    memset(big_buf_r, 0, 4096);

    // Use raw post/poll for the registered buffers
    // We need to bypass find_lkey since these are custom MRs.
    // For now, just test with staging buffer in chunks.
    // Actually let's write in chunks via staging.
    for (int chunk = 0; chunk < 4096; chunk += RdmaRegion::STAGING_BUF_SIZE) {
        int len = std::min((int)RdmaRegion::STAGING_BUF_SIZE, 4096 - chunk);
        memcpy(staging, big_buf_w + chunk, len);
        g_rdma.write(staging, off2 + chunk, len);
    }
    for (int chunk = 0; chunk < 4096; chunk += RdmaRegion::STAGING_BUF_SIZE) {
        int len = std::min((int)RdmaRegion::STAGING_BUF_SIZE, 4096 - chunk);
        g_rdma.read(staging, off2 + chunk, len);
        memcpy(big_buf_r + chunk, staging, len);
    }
    CHECK(memcmp(big_buf_w, big_buf_r, 4096) == 0) << "4KB data mismatch!";
    LOG(INFO) << "PASS: 4KB write + read";

    g_rdma.deregister_local_buffer(mr_w);
    g_rdma.deregister_local_buffer(mr_r);
    free(big_buf_w);
    free(big_buf_r);

    // 6. Latency test (1000 8-byte writes + reads)
    auto start = std::chrono::high_resolution_clock::now();
    int iters = 1000;
    for (int i = 0; i < iters; i++) {
        uint64_t val = i;
        memcpy(staging, &val, sizeof(val));
        g_rdma.write(staging, off1, sizeof(val));
    }
    auto mid = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < iters; i++) {
        g_rdma.read(staging, off1, sizeof(uint64_t));
    }
    auto end = std::chrono::high_resolution_clock::now();
    double write_us = std::chrono::duration<double, std::micro>(mid - start).count() / iters;
    double read_us  = std::chrono::duration<double, std::micro>(end - mid).count() / iters;
    LOG(INFO) << "Latency: write=" << write_us << " us/op, read=" << read_us << " us/op";

    g_rdma.close();
    LOG(INFO) << "=== ALL TESTS PASSED ===";
    return 0;
}
