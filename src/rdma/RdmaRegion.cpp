#include "RdmaRegion.h"
#include <cstdlib>
#include <cstring>
#include <glog/logging.h>

// Global RDMA region instance
RdmaRegion g_rdma;

// Thread-local staging buffer slot index
static thread_local int tl_staging_slot = -1;

RdmaRegion::~RdmaRegion() {
    if (connected_) close();
}

void RdmaRegion::connect(const char* mem_node_ip, int mem_node_port) {
    CHECK(!connected_) << "Already connected";

    pthread_spin_init(&qp_lock_, PTHREAD_PROCESS_PRIVATE);

    // Open RDMA device
    ctx_ = rdma::open_default_device();
    pd_  = ibv_alloc_pd(ctx_);
    rdma::check_nonnull(pd_, "ibv_alloc_pd");

    // Find GID for RoCE
    gid_index_ = rdma::find_roce_gid_index(ctx_, 1);
    LOG(INFO) << "Using GID index " << gid_index_;

    // Create CQ and QP
    cq_ = ibv_create_cq(ctx_, 512, nullptr, nullptr, 0);
    rdma::check_nonnull(cq_, "ibv_create_cq");

    qp_ = rdma::create_rc_qp(pd_, cq_, cq_, 512, 16);

    // Allocate and register staging buffer pool
    staging_pool_ = aligned_alloc(4096, STAGING_POOL_SIZE);
    CHECK(staging_pool_) << "aligned_alloc staging pool";
    memset(staging_pool_, 0, STAGING_POOL_SIZE);
    staging_mr_ = ibv_reg_mr(pd_, staging_pool_, STAGING_POOL_SIZE,
        IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ);
    rdma::check_nonnull(staging_mr_, "ibv_reg_mr staging");

    // Move QP to INIT
    rdma::qp_to_init(qp_, 1);

    // Get local QP info
    ibv_port_attr port_attr;
    ibv_query_port(ctx_, 1, &port_attr);

    rdma::QpConnInfo local_qp_info = {};
    local_qp_info.qp_num = qp_->qp_num;
    local_qp_info.lid = port_attr.lid;
    union ibv_gid local_gid;
    rdma::query_gid(ctx_, 1, gid_index_, &local_gid);
    memcpy(local_qp_info.gid, &local_gid, 16);

    // TCP connect to memory server and exchange info
    LOG(INFO) << "Connecting to RDMA memory server at " << mem_node_ip << ":" << mem_node_port;
    int sock = rdma::tcp_connect(mem_node_ip, mem_node_port);

    rdma::ExchangeMsg my_msg = {};
    my_msg.qp_info = local_qp_info;
    // mr_info is empty (client doesn't expose MR to server)

    rdma::ExchangeMsg remote_msg = rdma::tcp_exchange(sock, my_msg);
    ::close(sock);

    remote_info_ = remote_msg.mr_info;
    LOG(INFO) << "Remote MR: addr=0x" << std::hex << remote_info_.addr
              << " rkey=0x" << remote_info_.rkey
              << " length=" << std::dec << remote_info_.length;

    // Activate QP: RTR -> RTS
    rdma::qp_to_rtr(qp_, remote_msg.qp_info, 1, gid_index_);
    rdma::qp_to_rts(qp_);

    connected_ = true;
    LOG(INFO) << "RDMA connection established";
}

void RdmaRegion::close() {
    if (!connected_) return;
    if (qp_) { ibv_destroy_qp(qp_); qp_ = nullptr; }
    if (cq_) { ibv_destroy_cq(cq_); cq_ = nullptr; }
    if (staging_mr_) { ibv_dereg_mr(staging_mr_); staging_mr_ = nullptr; }
    if (staging_pool_) { free(staging_pool_); staging_pool_ = nullptr; }
    if (pd_) { ibv_dealloc_pd(pd_); pd_ = nullptr; }
    if (ctx_) { ibv_close_device(ctx_); ctx_ = nullptr; }
    pthread_spin_destroy(&qp_lock_);
    connected_ = false;
}

// ── Allocation ──

uint64_t RdmaRegion::alloc(size_t size) {
    // Align to cache line
    size_t aligned = (size + ALLOC_ALIGNMENT - 1) & ~(ALLOC_ALIGNMENT - 1);
    uint64_t offset = alloc_offset_.fetch_add(aligned);
    CHECK(offset + aligned <= remote_info_.length)
        << "RDMA region OOM: tried to alloc at offset " << offset
        << " + " << aligned << " but region is " << remote_info_.length;
    return offset;
}

// ── Root Table ──

void RdmaRegion::set_root(size_t index, uint64_t offset) {
    CHECK(index * sizeof(uint64_t) < ROOT_TABLE_SIZE) << "Root index out of range: " << index;
    // Write the offset value to remote root table
    void* staging = get_staging_buf();
    memcpy(staging, &offset, sizeof(uint64_t));
    write(staging, index * sizeof(uint64_t), sizeof(uint64_t));
}

uint64_t RdmaRegion::get_root(size_t index) {
    CHECK(index * sizeof(uint64_t) < ROOT_TABLE_SIZE) << "Root index out of range: " << index;
    void* staging = get_staging_buf();
    uint64_t value = 0;
    while (value == 0) {
        read(staging, index * sizeof(uint64_t), sizeof(uint64_t));
        memcpy(&value, staging, sizeof(uint64_t));
        if (value == 0) {
            usleep(100);  // Brief sleep before retry
        }
    }
    return value;
}

// ── Core RDMA Operations ──

void RdmaRegion::post_rdma_op(ibv_wr_opcode opcode, void* local_addr, uint32_t lkey,
                               uint64_t remote_offset, size_t len, bool signaled) {
    ibv_sge sge = {};
    sge.addr   = (uint64_t)local_addr;
    sge.length = len;
    sge.lkey   = lkey;

    ibv_send_wr wr = {};
    wr.wr_id    = 0;
    wr.opcode   = opcode;
    wr.sg_list  = &sge;
    wr.num_sge  = 1;
    wr.send_flags = signaled ? IBV_SEND_SIGNALED : 0;
    if (len <= 64 && opcode == IBV_WR_RDMA_WRITE) {
        wr.send_flags |= IBV_SEND_INLINE;
    }
    wr.wr.rdma.remote_addr = remote_info_.addr + remote_offset;
    wr.wr.rdma.rkey        = remote_info_.rkey;

    ibv_send_wr* bad_wr = nullptr;
    int ret = ibv_post_send(qp_, &wr, &bad_wr);
    CHECK(ret == 0) << "ibv_post_send failed: " << strerror(ret);
}

void RdmaRegion::poll_completions(int count) {
    ibv_wc wc[16];
    int polled = 0;
    while (polled < count) {
        int n = ibv_poll_cq(cq_, std::min(16, count - polled), wc);
        CHECK(n >= 0) << "ibv_poll_cq failed";
        for (int i = 0; i < n; i++) {
            CHECK(wc[i].status == IBV_WC_SUCCESS)
                << "RDMA WC error: " << ibv_wc_status_str(wc[i].status)
                << " (opcode=" << wc[i].opcode << ")";
        }
        polled += n;
    }
}

void RdmaRegion::write(const void* local_src, uint64_t remote_offset, size_t len) {
    if (len == 0) return;
    uint32_t lkey = find_lkey(local_src);
    pthread_spin_lock(&qp_lock_);
    post_rdma_op(IBV_WR_RDMA_WRITE, const_cast<void*>(local_src), lkey,
                 remote_offset, len, true);
    poll_completions(1);
    pthread_spin_unlock(&qp_lock_);
}

void RdmaRegion::read(void* local_dst, uint64_t remote_offset, size_t len) {
    if (len == 0) return;
    uint32_t lkey = find_lkey(local_dst);
    pthread_spin_lock(&qp_lock_);
    post_rdma_op(IBV_WR_RDMA_READ, local_dst, lkey,
                 remote_offset, len, true);
    poll_completions(1);
    pthread_spin_unlock(&qp_lock_);
}

void RdmaRegion::post_write(const void* local_src, uint64_t remote_offset, size_t len, bool signaled) {
    if (len == 0) return;
    uint32_t lkey = find_lkey(local_src);
    pthread_spin_lock(&qp_lock_);
    post_rdma_op(IBV_WR_RDMA_WRITE, const_cast<void*>(local_src), lkey,
                 remote_offset, len, signaled);
    pthread_spin_unlock(&qp_lock_);
}

void RdmaRegion::post_read(void* local_dst, uint64_t remote_offset, size_t len, bool signaled) {
    if (len == 0) return;
    uint32_t lkey = find_lkey(local_dst);
    pthread_spin_lock(&qp_lock_);
    post_rdma_op(IBV_WR_RDMA_READ, local_dst, lkey,
                 remote_offset, len, signaled);
    pthread_spin_unlock(&qp_lock_);
}

// ── Local MR Management ──

ibv_mr* RdmaRegion::register_local_buffer(void* buf, size_t len) {
    ibv_mr* mr = ibv_reg_mr(pd_, buf, len,
        IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ);
    rdma::check_nonnull(mr, "register_local_buffer");
    return mr;
}

void RdmaRegion::deregister_local_buffer(ibv_mr* mr) {
    if (mr) ibv_dereg_mr(mr);
}

// ── Staging Buffer ──

void* RdmaRegion::get_staging_buf() {
    if (tl_staging_slot < 0) {
        tl_staging_slot = staging_next_slot_.fetch_add(1);
        CHECK(tl_staging_slot < (int)MAX_THREADS) << "Too many threads using staging buffers";
    }
    return (char*)staging_pool_ + tl_staging_slot * STAGING_BUF_SIZE;
}

uint32_t RdmaRegion::get_staging_lkey() {
    return staging_mr_->lkey;
}

uint32_t RdmaRegion::find_lkey(const void* addr) {
    // Check if it's in the staging pool
    uintptr_t a = (uintptr_t)addr;
    uintptr_t pool_start = (uintptr_t)staging_pool_;
    uintptr_t pool_end = pool_start + STAGING_POOL_SIZE;
    if (a >= pool_start && a < pool_end) {
        return staging_mr_->lkey;
    }
    // For other registered buffers, caller should use the buffer's own lkey.
    // As a fallback, try to use staging_mr_ (this will fail if addr is not in staging).
    // Proper solution: caller passes lkey or we maintain a lookup table.
    // For now, log a warning and return staging lkey (will work if buffer happens to be
    // within the registration range).
    LOG(WARNING) << "find_lkey: addr " << addr << " not in staging pool, "
                 << "caller should register and manage lkey explicitly";
    return staging_mr_->lkey;
}

uint32_t RdmaRegion::get_lkey_for(const void* ptr) {
    return find_lkey(ptr);
}
