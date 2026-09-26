// Dump all root table entries without spinning
#include "RdmaRegion.h"
#include <cstdio>
#include <cstring>
#include <glog/logging.h>

int main(int argc, char* argv[]) {
    google::InitGoogleLogging(argv[0]);
    FLAGS_logtostderr = true;

    const char* ip = "10.31.2.3";
    int port = 9999;
    if (argc > 1) ip = argv[1];
    if (argc > 2) port = atoi(argv[2]);

    g_rdma.connect(ip, port);
    LOG(INFO) << "Connected. Dumping first 32 root entries:";

    void* staging = g_rdma.get_staging_buf();
    for (int i = 0; i < 32; i++) {
        uint64_t value = 0;
        g_rdma.read(staging, i * sizeof(uint64_t), sizeof(uint64_t));
        memcpy(&value, staging, sizeof(uint64_t));
        printf("root[%2d] = %lu (0x%lx) %s\n", i, value, value, value == 0 ? " <-- ZERO!" : "");
    }
    return 0;
}
