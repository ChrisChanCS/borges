// RDMA Memory Server
// Runs on the memory node (10.31.1.158).
// Allocates a large memory region, registers it as RDMA MR,
// and accepts connections from compute nodes (shard servers + sequencer).

#include "RdmaCommon.h"
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <vector>
#include <thread>
#include <signal.h>
#include <sys/mman.h>
#include <getopt.h>
#include <glog/logging.h>

struct ServerConfig {
    int port = 9999;
    size_t region_size = (((size_t)1024 * 1024 * 1024 + 128 * 1024 * 1024) * 7)
                         + 4096 * sizeof(uint64_t);  // match default_cxl_mem_size + root table
    int max_clients = 8;
};

static volatile bool running = true;

void signal_handler(int) { running = false; }

void handle_client(int client_sock, ibv_context* ctx, ibv_pd* pd,
                   ibv_mr* mr, void* region, size_t region_size, int gid_index) {
    // Create per-client CQ and QP
    ibv_cq* cq = ibv_create_cq(ctx, 512, nullptr, nullptr, 0);
    rdma::check_nonnull(cq, "server ibv_create_cq");

    ibv_qp* qp = rdma::create_rc_qp(pd, cq, cq, 512, 16);

    // Move QP to INIT
    rdma::qp_to_init(qp, 1);

    // Get local QP info
    ibv_port_attr port_attr;
    ibv_query_port(ctx, 1, &port_attr);

    rdma::QpConnInfo local_qp_info = {};
    local_qp_info.qp_num = qp->qp_num;
    local_qp_info.lid = port_attr.lid;
    union ibv_gid local_gid;
    rdma::query_gid(ctx, 1, gid_index, &local_gid);
    memcpy(local_qp_info.gid, &local_gid, 16);

    // Prepare exchange message with MR info
    rdma::ExchangeMsg my_msg = {};
    my_msg.qp_info = local_qp_info;
    my_msg.mr_info.addr = (uint64_t)region;
    my_msg.mr_info.rkey = mr->rkey;
    my_msg.mr_info.length = region_size;

    // Exchange with client
    rdma::ExchangeMsg client_msg = rdma::tcp_exchange(client_sock, my_msg);
    ::close(client_sock);

    // Activate QP
    rdma::qp_to_rtr(qp, client_msg.qp_info, 1, gid_index);
    rdma::qp_to_rts(qp);

    LOG(INFO) << "Client connected, QP num=" << client_msg.qp_info.qp_num;

    // QP stays alive. This function returns but the QP and CQ must not be destroyed
    // while the client is using them. We leak them intentionally for the server lifetime.
    // In a production system, we'd track them and clean up on disconnect.
}

int main(int argc, char* argv[]) {
    google::InitGoogleLogging(argv[0]);
    FLAGS_logtostderr = true;

    ServerConfig config;

    // Parse arguments
    static struct option long_opts[] = {
        {"port",   required_argument, 0, 'p'},
        {"size",   required_argument, 0, 's'},
        {"help",   no_argument,       0, 'h'},
        {0, 0, 0, 0}
    };

    int opt;
    while ((opt = getopt_long(argc, argv, "p:s:h", long_opts, nullptr)) != -1) {
        switch (opt) {
            case 'p': config.port = atoi(optarg); break;
            case 's': config.region_size = strtoull(optarg, nullptr, 10); break;
            case 'h':
                std::cout << "Usage: " << argv[0] << " [--port PORT] [--size BYTES]\n";
                return 0;
            default:
                return 1;
        }
    }

    LOG(INFO) << "RDMA Memory Server starting";
    LOG(INFO) << "  Port: " << config.port;
    LOG(INFO) << "  Region size: " << config.region_size << " bytes ("
              << (config.region_size / (1024.0 * 1024 * 1024)) << " GB)";

    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);

    // Allocate memory region using mmap (huge pages if available)
    void* region = mmap(nullptr, config.region_size,
                        PROT_READ | PROT_WRITE,
                        MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB,
                        -1, 0);
    if (region == MAP_FAILED) {
        LOG(WARNING) << "mmap with MAP_HUGETLB failed, falling back to regular pages";
        region = mmap(nullptr, config.region_size,
                      PROT_READ | PROT_WRITE,
                      MAP_PRIVATE | MAP_ANONYMOUS,
                      -1, 0);
    }
    CHECK(region != MAP_FAILED) << "mmap failed: " << strerror(errno);

    // Zero-initialize
    memset(region, 0, config.region_size);
    LOG(INFO) << "Memory region allocated and zeroed at " << region;

    // Open RDMA device
    ibv_context* ctx = rdma::open_default_device();
    ibv_pd* pd = ibv_alloc_pd(ctx);
    rdma::check_nonnull(pd, "ibv_alloc_pd");

    int gid_index = rdma::find_roce_gid_index(ctx, 1);
    LOG(INFO) << "Using GID index " << gid_index;

    // Register MR
    ibv_mr* mr = ibv_reg_mr(pd, region, config.region_size,
        IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE |
        IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_ATOMIC);
    rdma::check_nonnull(mr, "ibv_reg_mr");
    LOG(INFO) << "MR registered: addr=0x" << std::hex << (uint64_t)region
              << " rkey=0x" << mr->rkey << std::dec;

    // Listen for TCP connections
    int listen_sock = rdma::tcp_listen(config.port);
    LOG(INFO) << "Listening on port " << config.port << " for client connections";

    // Accept clients
    int client_count = 0;
    while (running) {
        sockaddr_in client_addr;
        socklen_t addrlen = sizeof(client_addr);
        int client_sock = accept(listen_sock, (sockaddr*)&client_addr, &addrlen);
        if (client_sock < 0) {
            if (!running) break;
            LOG(WARNING) << "accept failed: " << strerror(errno);
            continue;
        }

        char client_ip[INET_ADDRSTRLEN];
        inet_ntop(AF_INET, &client_addr.sin_addr, client_ip, sizeof(client_ip));
        LOG(INFO) << "Accepting client " << client_count << " from " << client_ip;

        handle_client(client_sock, ctx, pd, mr, region, config.region_size, gid_index);
        client_count++;
        LOG(INFO) << "Client " << client_count << " connected. Total: " << client_count;
    }

    LOG(INFO) << "Shutting down...";
    ::close(listen_sock);
    // Cleanup (QPs/CQs leaked intentionally for client lifetime)
    ibv_dereg_mr(mr);
    ibv_dealloc_pd(pd);
    ibv_close_device(ctx);
    munmap(region, config.region_size);

    return 0;
}
