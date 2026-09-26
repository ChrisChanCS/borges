#pragma once

#include <cstdint>
#include <cstddef>
#include <atomic>
#include <pthread.h>
#include <infiniband/verbs.h>
#include "RdmaCommon.h"

// Client-side RDMA region. Replaces cxlalloc for one-sided remote memory access.
// Each process creates exactly one instance (g_rdma) shared by all threads.
class RdmaRegion {
public:
    RdmaRegion() = default;
    ~RdmaRegion();

    // Connect to the RDMA memory server at (ip, port).
    // Sets up QP, registers local staging buffer, exchanges MR info.
    void connect(const char* mem_node_ip, int mem_node_port);

    // Disconnect and clean up.
    void close();

    bool is_connected() const { return connected_; }

    // ── Allocation (sequencer only, at init time) ──
    // Bump-allocate `size` bytes from the remote region.
    // Returns offset within the remote MR. Thread-safe via atomic.
    uint64_t alloc(size_t size);

    // ── Root Table ──
    // Root table is at offset 0 of the remote region.
    // set_root: RDMA WRITE 8 bytes to root[index].
    void set_root(size_t index, uint64_t offset);

    // get_root: RDMA READ 8 bytes from root[index]. Spins until non-zero.
    uint64_t get_root(size_t index);

    // ── One-Sided Data Movement ──
    // local_src/local_dst MUST be within a registered MR.
    // Use the staging buffer (get_staging_buf) for small ops,
    // or register your own buffers via register_local_buffer().

    // Synchronous write: posts RDMA WRITE, polls CQ, returns when done.
    void write(const void* local_src, uint64_t remote_offset, size_t len);

    // Synchronous read: posts RDMA READ, polls CQ, returns when done.
    void read(void* local_dst, uint64_t remote_offset, size_t len);

    // ── Batched Operations ──
    // Post without waiting. Caller must poll_completions() afterwards.
    void post_write(const void* local_src, uint64_t remote_offset, size_t len, bool signaled = true);
    void post_read(void* local_dst, uint64_t remote_offset, size_t len, bool signaled = true);

    // Wait for `count` CQ completions.
    void poll_completions(int count);

    // ── Local Memory Registration ──
    ibv_mr* register_local_buffer(void* buf, size_t len);
    void deregister_local_buffer(ibv_mr* mr);

    // Per-thread staging buffer: a small registered buffer for ad-hoc RDMA ops.
    // Each thread should call this once and reuse the returned pointer.
    // Size: STAGING_BUF_SIZE bytes per thread.
    static constexpr size_t STAGING_BUF_SIZE = 4096;
    void* get_staging_buf();
    uint32_t get_staging_lkey();

    // Get lkey for a given pointer (it must be within a registered MR).
    // For the staging buffer, returns staging_mr_->lkey.
    // For other buffers, the caller must track the lkey from register_local_buffer().
    uint32_t get_lkey_for(const void* ptr);

    // Direct access to internal info (for advanced callers)
    uint64_t remote_addr() const { return remote_info_.addr; }
    uint32_t remote_rkey() const { return remote_info_.rkey; }

    static constexpr size_t ROOT_TABLE_SIZE = 4096 * sizeof(uint64_t);  // 32 KB
    static constexpr size_t ALLOC_ALIGNMENT = 64;  // cache-line align allocations

private:
    void post_rdma_op(ibv_wr_opcode opcode, void* local_addr, uint32_t lkey,
                      uint64_t remote_offset, size_t len, bool signaled);
    uint32_t find_lkey(const void* addr);

    bool connected_ = false;

    // RDMA resources
    ibv_context*   ctx_ = nullptr;
    ibv_pd*        pd_  = nullptr;
    ibv_cq*        cq_  = nullptr;
    ibv_qp*        qp_  = nullptr;
    int            gid_index_ = 0;

    rdma::MrInfo   remote_info_ = {};

    // Per-thread staging buffers (thread_local managed externally)
    // Global staging pool: we allocate a chunk and register it.
    void*    staging_pool_ = nullptr;
    ibv_mr*  staging_mr_   = nullptr;
    static constexpr size_t MAX_THREADS = 64;
    static constexpr size_t STAGING_POOL_SIZE = STAGING_BUF_SIZE * MAX_THREADS;
    std::atomic<int> staging_next_slot_{0};

    // Bump allocator for remote region
    std::atomic<uint64_t> alloc_offset_{ROOT_TABLE_SIZE};

    // Spinlock for QP access (multiple threads share one QP)
    pthread_spinlock_t qp_lock_;

};

// Global instance, defined in RdmaRegion.cpp
extern RdmaRegion g_rdma;
