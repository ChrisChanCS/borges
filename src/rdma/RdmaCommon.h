#pragma once

#include <cstdint>
#include <cstring>
#include <cstdlib>
#include <string>
#include <stdexcept>
#include <infiniband/verbs.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netdb.h>
#include <glog/logging.h>

namespace rdma {

// Information needed to connect two QPs
struct QpConnInfo {
    uint32_t qp_num;
    uint16_t lid;
    uint8_t  gid[16];  // RoCEv2 GID
};

// Information about a remote memory region
struct MrInfo {
    uint64_t addr;
    uint32_t rkey;
    uint64_t length;
};

// Wire format for exchanging connection info over TCP
struct ExchangeMsg {
    QpConnInfo qp_info;
    MrInfo     mr_info;
};

// ---- Error checking helpers ----

inline void check_nn(int ret, const char* msg) {
    if (ret < 0) {
        LOG(FATAL) << msg << ": " << strerror(errno);
    }
}

inline void check_nonnull(void* ptr, const char* msg) {
    if (!ptr) {
        LOG(FATAL) << msg << ": " << strerror(errno);
    }
}

// ---- Device and PD ----

inline ibv_context* open_default_device() {
    int num_devices = 0;
    ibv_device** dev_list = ibv_get_device_list(&num_devices);
    CHECK(dev_list && num_devices > 0) << "No RDMA devices found";
    ibv_context* ctx = ibv_open_device(dev_list[0]);
    check_nonnull(ctx, "ibv_open_device");
    ibv_free_device_list(dev_list);
    return ctx;
}

// ---- GID query (for RoCE) ----

inline void query_gid(ibv_context* ctx, int port, int gid_index, union ibv_gid* gid) {
    int ret = ibv_query_gid(ctx, port, gid_index, gid);
    check_nn(ret, "ibv_query_gid");
}

// Find the best RoCEv2 GID index. Prefer index 3 (common for RoCEv2), fallback to 0.
inline int find_roce_gid_index(ibv_context* ctx, int port) {
    // Try common RoCEv2 indices: 3, 1, 0
    for (int idx : {3, 1, 0}) {
        union ibv_gid gid;
        if (ibv_query_gid(ctx, port, idx, &gid) == 0) {
            // Check it's not all zeros
            bool all_zero = true;
            for (int i = 0; i < 16; i++) {
                if (gid.raw[i] != 0) { all_zero = false; break; }
            }
            if (!all_zero) return idx;
        }
    }
    return 0;  // fallback
}

// ---- QP Creation ----

inline ibv_qp* create_rc_qp(ibv_pd* pd, ibv_cq* send_cq, ibv_cq* recv_cq,
                              uint32_t max_send_wr = 256, uint32_t max_recv_wr = 16) {
    ibv_qp_init_attr init_attr = {};
    init_attr.qp_type = IBV_QPT_RC;
    init_attr.send_cq = send_cq;
    init_attr.recv_cq = recv_cq;
    init_attr.cap.max_send_wr = max_send_wr;
    init_attr.cap.max_recv_wr = max_recv_wr;
    init_attr.cap.max_send_sge = 1;
    init_attr.cap.max_recv_sge = 1;
    init_attr.cap.max_inline_data = 64;
    init_attr.sq_sig_all = 0;  // We'll signal selectively

    ibv_qp* qp = ibv_create_qp(pd, &init_attr);
    check_nonnull(qp, "ibv_create_qp");
    return qp;
}

// ---- QP State Transitions ----

inline void qp_to_init(ibv_qp* qp, int port = 1) {
    ibv_qp_attr attr = {};
    attr.qp_state = IBV_QPS_INIT;
    attr.pkey_index = 0;
    attr.port_num = port;
    attr.qp_access_flags = IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ |
                           IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_ATOMIC;
    int ret = ibv_modify_qp(qp, &attr,
        IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS);
    check_nn(ret, "qp_to_init");
}

inline void qp_to_rtr(ibv_qp* qp, const QpConnInfo& remote, int port = 1, int gid_index = 0) {
    ibv_qp_attr attr = {};
    attr.qp_state = IBV_QPS_RTR;
    attr.path_mtu = IBV_MTU_1024;
    attr.dest_qp_num = remote.qp_num;
    attr.rq_psn = 0;
    attr.max_dest_rd_atomic = 16;
    attr.min_rnr_timer = 12;

    attr.ah_attr.is_global = 1;
    memcpy(&attr.ah_attr.grh.dgid, remote.gid, 16);
    attr.ah_attr.grh.flow_label = 0;
    attr.ah_attr.grh.sgid_index = gid_index;
    attr.ah_attr.grh.hop_limit = 255;
    attr.ah_attr.grh.traffic_class = 0;
    attr.ah_attr.dlid = remote.lid;
    attr.ah_attr.sl = 0;
    attr.ah_attr.src_path_bits = 0;
    attr.ah_attr.port_num = port;

    int ret = ibv_modify_qp(qp, &attr,
        IBV_QP_STATE | IBV_QP_AV | IBV_QP_PATH_MTU | IBV_QP_DEST_QPN |
        IBV_QP_RQ_PSN | IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER);
    check_nn(ret, "qp_to_rtr");
}

inline void qp_to_rts(ibv_qp* qp) {
    ibv_qp_attr attr = {};
    attr.qp_state = IBV_QPS_RTS;
    attr.sq_psn = 0;
    attr.timeout = 14;
    attr.retry_cnt = 7;
    attr.rnr_retry = 7;
    attr.max_rd_atomic = 16;

    int ret = ibv_modify_qp(qp, &attr,
        IBV_QP_STATE | IBV_QP_SQ_PSN | IBV_QP_TIMEOUT |
        IBV_QP_RETRY_CNT | IBV_QP_RNR_RETRY | IBV_QP_MAX_QP_RD_ATOMIC);
    check_nn(ret, "qp_to_rts");
}

// Full QP activation: INIT -> RTR -> RTS
inline void activate_qp(ibv_qp* qp, const QpConnInfo& remote, int port = 1, int gid_index = 0) {
    qp_to_init(qp, port);
    qp_to_rtr(qp, remote, port, gid_index);
    qp_to_rts(qp);
}

// ---- TCP helpers for exchanging QP/MR info ----

inline int tcp_listen(int port, int backlog = 16) {
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    check_nn(sock, "socket");
    int opt = 1;
    setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    setsockopt(sock, SOL_SOCKET, SO_REUSEPORT, &opt, sizeof(opt));

    sockaddr_in addr = {};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    addr.sin_addr.s_addr = INADDR_ANY;
    check_nn(bind(sock, (sockaddr*)&addr, sizeof(addr)), "bind");
    check_nn(listen(sock, backlog), "listen");
    return sock;
}

inline int tcp_connect(const char* ip, int port) {
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    check_nn(sock, "socket");

    sockaddr_in addr = {};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    inet_pton(AF_INET, ip, &addr.sin_addr);

    int ret;
    for (int attempt = 0; attempt < 20; attempt++) {
        ret = connect(sock, (sockaddr*)&addr, sizeof(addr));
        if (ret == 0) break;
        usleep(500000);  // 500ms retry
    }
    check_nn(ret, "tcp_connect");
    return sock;
}

inline void tcp_send_all(int sock, const void* buf, size_t len) {
    const char* p = (const char*)buf;
    size_t sent = 0;
    while (sent < len) {
        ssize_t n = send(sock, p + sent, len - sent, 0);
        CHECK(n > 0) << "tcp_send_all failed";
        sent += n;
    }
}

inline void tcp_recv_all(int sock, void* buf, size_t len) {
    char* p = (char*)buf;
    size_t recvd = 0;
    while (recvd < len) {
        ssize_t n = recv(sock, p + recvd, len - recvd, 0);
        CHECK(n > 0) << "tcp_recv_all failed";
        recvd += n;
    }
}

// Exchange ExchangeMsg between two sides over a TCP socket.
// Each side sends its own info, then receives the other side's info.
inline ExchangeMsg tcp_exchange(int sock, const ExchangeMsg& my_info) {
    tcp_send_all(sock, &my_info, sizeof(my_info));
    ExchangeMsg remote;
    tcp_recv_all(sock, &remote, sizeof(remote));
    return remote;
}

}  // namespace rdma
