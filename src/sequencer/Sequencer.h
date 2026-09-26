#pragma once

#include "../shard_server/Message.h"
#include "../common/Macro.h"
#include "../common/CCHashTable.h"
#include <glog/logging.h>
#include <span>
#include <absl/container/flat_hash_map.h>
#include "ThreadPool.h"
#include "../common/SharedData.h"
#include "Gsn.h"
#ifndef USE_RDMA
#include "../common/ReconfigControl.h"
#include "../common/RecoveryMaintenance.h"
#include "../common/MaintenanceControl.h"
#include "ShardRecoveryAdmin.h"
#endif

// const size_t CXL_CAPACITY = size_t(1024) * 1024 * 1024 * 100; // 100GB

struct alignas(64) SwitchReady
{
    bool is_ready;
    absl::Mutex mutex;
    absl::CondVar cond_var;
    char padding[64 - sizeof(bool) - sizeof(absl::Mutex) - sizeof(absl::CondVar)];
    explicit SwitchReady()
    {
        is_ready = false;
    }
};

namespace Sequencer
{
    class Sequencer
    {
    private:
        std::unique_ptr<ThreadPool> thread_pool_;
        GSNCommitter gsn_committer_;
#ifndef USE_RDMA
        std::uint64_t configuration_epoch_ = 0;
        std::uint64_t active_shards_ = SharedData::GSNSet::all_shards;
        std::uint64_t observed_leases_ = 0;
        std::uint64_t switch_epoch_ = 0;
        SharedData::ClusterRegion *cluster_region_ = nullptr;
        SharedData::ClusterConfiguration configuration_;
        SharedData::ReconfigInbox joins_;
        std::uint64_t settings_ = 0;
        std::vector<std::string> maintenance_addresses_;
        std::uint16_t maintenance_port_ = 8091;
        ShardRecoveryAdmin recovery_admin_;
        std::uint16_t control_port_ = 8090;
        bool rebuilding_backup_ = false;
        std::uint64_t backup_epoch_ = 0;
        std::uint64_t backup_cut_offset_ = 0;
        void *backup_cut_log_ = nullptr;
        std::atomic<bool> backup_ready_{false};
        std::exception_ptr backup_error_;
        std::thread backup_thread_;
#endif
        SharedData::SwitchFlag *sequencer_switch_flag_;
        SharedData::SwitchFlag **reader_switch_flag_;
#ifdef USE_RDMA
        uint64_t sequencer_switch_flag_offset_;
        uint64_t *reader_switch_flag_offset_{nullptr}; // allocated in constructor
        uint64_t flat_table_offset_[COMMIT_TAIL_HASH_TABLE_CANDIDATE_NUM];
#endif
        SwitchReady switch_ready_[SHARD_SERVER_NUM];
        std::atomic<std::uint64_t> switch_finish_;
        std::uint64_t version_number_ = 1;
        bool running_;
        std::chrono::microseconds interval_{std::chrono::microseconds(ORDERING_INTERVAL_MICROSECONDS)};
        // CommitPtr commit_ptr_;

    public:
        Sequencer(std::uint64_t lease_timeout_ms = 10000, std::uint64_t lease_renew_interval_ms = 1000,
                  std::uint16_t reconfig_port = 8090, bool recover = false
#ifndef USE_RDMA
                  , SharedData::RecoveryOptions recovery_options = {}
#endif
                  )
        {
            LOG(INFO) << "start init sequencer";
#ifndef USE_RDMA
            control_port_ = reconfig_port;
            SharedData::LeaseService<SHARD_SERVER_NUM>::validate_settings(lease_timeout_ms, lease_renew_interval_ms);
            if (recover)
            {
                restore_sequencer(lease_timeout_ms, lease_renew_interval_ms, reconfig_port, recovery_options);
                return;
            }
            if (cxlalloc_get_root(CLUSTER_ROOT_INDEX) != nullptr)
                throw std::runtime_error("cluster already exists; use --recover instead of reinitializing it");
#endif

            // Initialize each shard/replicator region with read and write streams.
            for (std::uint64_t i = 0; i < SHARD_SERVER_NUM; i++)
            {
                for (std::uint64_t j = 0; j < REPLICATOR_NUM; j++)
                {
                    // Each region holds one replicator's read/write streams for all state keys.
#ifdef USE_RDMA
                    uint64_t ws_off = g_rdma.alloc(WRITE_STREAM_SIZE);
                    g_rdma.set_root(STREAM_ROOT_INDEX + i * REPLICATOR_NUM + j, ws_off);
#else
                    void *cxl_region_write = SharedData::allocate_shared(WRITE_STREAM_SIZE);
                    cxlalloc_set_root(STREAM_ROOT_INDEX + i * REPLICATOR_NUM + j, cxl_region_write);
#endif
                    LOG(INFO) << "allocate cxl write stream for shard " << i << " replicator " << j << " with size " << WRITE_STREAM_SIZE;

#ifdef USE_RDMA
                    uint64_t rs_off = g_rdma.alloc(READ_STREAM_SIZE);
                    g_rdma.set_root(READ_STREAM_ROOT_INDEX + i * REPLICATOR_NUM + j, rs_off);
#else
                    void *cxl_region_read = SharedData::allocate_shared(READ_STREAM_SIZE);
                    cxlalloc_set_root(READ_STREAM_ROOT_INDEX + i * REPLICATOR_NUM + j, cxl_region_read);
#endif
                    LOG(INFO) << "allocate cxl read stream for shard " << i << " replicator " << j << " with size " << READ_STREAM_SIZE;
                }
            }
            LOG(INFO) << "allocate cxl shard region";

            // Initialize the shared global-cut buffers.
            for (std::uint64_t i = 0; i < 2; i++)
            {
#ifdef USE_RDMA
                uint64_t gsn_off = g_rdma.alloc(GSN_SET_CNT * sizeof(SharedData::GSNSet));
                // GSNSet default-inits to zero, matching zero-initialized remote memory
                g_rdma.set_root(GSN_BUFFER_ROOT_INDEX + i, gsn_off);
#else
                // Reserve the complete cut log at startup. Publication never
                // allocates shared memory.
                // Align the allocation itself, not the local pointer variable.
                void *gsn_storage = cxlalloc_memalign(GSN_SET_CNT * sizeof(SharedData::GSNSet),
                                                     alignof(SharedData::GSNSet));
                CHECK(gsn_storage != nullptr);
                CHECK_EQ(reinterpret_cast<std::uintptr_t>(gsn_storage) % CACHELINE_SIZE, 0u);
                auto *gsn_buffer = static_cast<SharedData::GSNSet *>(gsn_storage);
                for (std::uint64_t j = 0; j < GSN_SET_CNT; j++)
                {
                    new (&gsn_buffer[j]) SharedData::GSNSet();
                }
                cxlalloc_set_root(GSN_BUFFER_ROOT_INDEX + i, gsn_buffer);
#endif
            }
            LOG(INFO) << "allocate cxl global cut buffer";

            // Initialize one commit buffer per shard.
            for (std::uint64_t i = 0; i < SHARD_SERVER_NUM; i++)
            {
#ifdef USE_RDMA
                uint64_t cb_off = g_rdma.alloc(COMMIT_RING_BUFFER_CAPACITY * sizeof(std::uint64_t));
                g_rdma.set_root(COMMIT_RING_BUFFER_ROOT_INDEX + i, cb_off);
#else
                void *shard_commit_buffer = SharedData::allocate_shared(COMMIT_RING_BUFFER_CAPACITY * sizeof(std::uint64_t));
                cxlalloc_set_root(COMMIT_RING_BUFFER_ROOT_INDEX + i, shard_commit_buffer);
#endif
            }
            LOG(INFO) << "allocate cxl delta region";

            // Initialize each shard's commit-buffer metadata.
            for (std::uint64_t i = 0; i < SHARD_SERVER_NUM; i++)
            {
#ifdef USE_RDMA
                uint64_t cbm_off = g_rdma.alloc(sizeof(SharedData::CommitBufferMetadata));
                // CommitBufferMetadata default-inits to zero, matching zero-initialized remote memory
                g_rdma.set_root(COMMIT_BUFFER_METADATA_ROOT_INDEX + i, cbm_off);
#else
                void *shard_commit_buffer_metadata = SharedData::allocate_shared(sizeof(SharedData::CommitBufferMetadata));
                new (shard_commit_buffer_metadata) SharedData::CommitBufferMetadata();
                clwb(shard_commit_buffer_metadata, sizeof(SharedData::CommitBufferMetadata));
                sfence();
                cxlalloc_set_root(COMMIT_BUFFER_METADATA_ROOT_INDEX + i, shard_commit_buffer_metadata);
#endif
            }
            LOG(INFO) << "allocate cxl delta metadata region";

            // Initialize each shard/replicator cut region.
            for (std::uint64_t i = 0; i < SHARD_SERVER_NUM; i++)
            {
                for (std::uint64_t j = 0; j < REPLICATOR_NUM; j++)
                {
#ifdef USE_RDMA
                    uint64_t cr_off = g_rdma.alloc(sizeof(SharedData::CutRegion));
                    // CutRegion default-inits to zero, matching zero-initialized remote memory
                    g_rdma.set_root(CUT_REGION_ROOT_INDEX + i * REPLICATOR_NUM + j, cr_off);
#else
                    void *shard_cut_region = SharedData::allocate_shared(sizeof(SharedData::CutRegion));
                    new (shard_cut_region) SharedData::CutRegion();
                    cxlalloc_set_root(CUT_REGION_ROOT_INDEX + i * REPLICATOR_NUM + j, shard_cut_region);
#endif
                }
            }
            LOG(INFO) << "allocate cxl local cut region";

            // Initialize the indexes shared by all shards.
            for (std::uint64_t j = 0; j < COMMIT_TAIL_HASH_TABLE_CANDIDATE_NUM; j++)
            {
#ifdef USE_RDMA
                // Flat array indexed by StateKey: AllShardInfo[KEY_CNT_OF_SHARD].
                // Remote memory is zeroed by mmap, so AllShardInfo default (0,0) requires no write.
                uint64_t ht_off = g_rdma.alloc(KEY_CNT_OF_SHARD * sizeof(star::AllShardInfo));
                g_rdma.set_root(TAIL_HASH_TABLE_ROOT_INDEX + j, ht_off);
                flat_table_offset_[j] = ht_off;
#else
                star::CCHashTable *commit_tail_hash_table = static_cast<star::CCHashTable *>(SharedData::allocate_shared(sizeof(star::CCHashTable)));
                new (commit_tail_hash_table) star::CCHashTable(BUCKET_CNT);
                clwb(commit_tail_hash_table, sizeof(*commit_tail_hash_table));
                sfence();
                cxlalloc_set_root(TAIL_HASH_TABLE_ROOT_INDEX + j, commit_tail_hash_table);
#endif
            }
            LOG(INFO) << "allocate cxl double hash table";

            // Initialize index-switch flags.
            {
                reader_switch_flag_ = new SharedData::SwitchFlag *[SHARD_SERVER_NUM * REQUEST_WORKER_NUM];
#ifdef USE_RDMA
                reader_switch_flag_offset_ = new uint64_t[SHARD_SERVER_NUM * REQUEST_WORKER_NUM];
                uint64_t all_rf_off = g_rdma.alloc(sizeof(SharedData::SwitchFlag) * SHARD_SERVER_NUM * REQUEST_WORKER_NUM);
                // SwitchFlag default-inits to zero, matching zero-initialized remote memory
                for (std::uint64_t i = 0; i < SHARD_SERVER_NUM * REQUEST_WORKER_NUM; i++)
                {
                    uint64_t flag_off = all_rf_off + i * sizeof(SharedData::SwitchFlag);
                    // Store offset as pointer (for RDMA, we use reader_switch_flag_offset_ for actual ops)
                    reader_switch_flag_[i] = reinterpret_cast<SharedData::SwitchFlag *>(flag_off);
                    reader_switch_flag_offset_[i] = flag_off;
                    g_rdma.set_root(SWITCH_FLAG_ROOT_INDEX + i, flag_off);
                    LOG(INFO) << "allocate cxl switch flag for reader " << i;
                }
                uint64_t seq_sf_off = g_rdma.alloc(sizeof(SharedData::SwitchFlag));
                // SwitchFlag default-inits to zero, matching zero-initialized remote memory
                sequencer_switch_flag_ = reinterpret_cast<SharedData::SwitchFlag *>(seq_sf_off);
                sequencer_switch_flag_offset_ = seq_sf_off;
                g_rdma.set_root(SWITCH_FLAG_ROOT_INDEX + SHARD_SERVER_NUM * REQUEST_WORKER_NUM, seq_sf_off);
#else
                SharedData::SwitchFlag *all_reader_flag = reinterpret_cast<SharedData::SwitchFlag *>(SharedData::allocate_shared(sizeof(SharedData::SwitchFlag) * SHARD_SERVER_NUM * REQUEST_WORKER_NUM));
                for (std::uint64_t i = 0; i < SHARD_SERVER_NUM * REQUEST_WORKER_NUM; i++)
                {
                    new (&all_reader_flag[i]) SharedData::SwitchFlag();
                    reader_switch_flag_[i] = &all_reader_flag[i];
                    cxlalloc_set_root(SWITCH_FLAG_ROOT_INDEX + i, &all_reader_flag[i]);
                    LOG(INFO) << "allocate cxl switch flag for reader " << i;
                }
                sequencer_switch_flag_ = reinterpret_cast<SharedData::SwitchFlag *>(SharedData::allocate_shared(sizeof(SharedData::SwitchFlag)));
                new (sequencer_switch_flag_) SharedData::SwitchFlag();
                cxlalloc_set_root(SWITCH_FLAG_ROOT_INDEX + SHARD_SERVER_NUM * REQUEST_WORKER_NUM, sequencer_switch_flag_);
#endif
            }
            LOG(INFO) << "allocate cxl switch flag";

            // Initialize the shared producer round.
#ifdef USE_RDMA
            {
                uint64_t rn_off = g_rdma.alloc(sizeof(SharedData::RoundNumber));
                // RoundNumber default-inits to zero, matching zero-initialized remote memory
                g_rdma.set_root(ROUND_NUMBER_ROOT_INDEX, rn_off);
            }
#else
            auto *round_number = static_cast<SharedData::RoundNumber *>(SharedData::allocate_shared(sizeof(SharedData::RoundNumber)));
            new (round_number) SharedData::RoundNumber();
            cxlalloc_set_root(ROUND_NUMBER_ROOT_INDEX, round_number);
#endif
            LOG(INFO) << "allocate cxl round number";

            // Initialize each index's round number.
            for (std::uint64_t i = 0; i < COMMIT_TAIL_HASH_TABLE_CANDIDATE_NUM; i++)
            {
#ifdef USE_RDMA
                uint64_t irn_off = g_rdma.alloc(sizeof(SharedData::IndexRoundNumber));
                // IndexRoundNumber default-inits to zero, matching zero-initialized remote memory
                g_rdma.set_root(INDEX_ROUND_NUMBER_ROOT_INDEX + i, irn_off);
#else
                auto *index_round_number = static_cast<SharedData::IndexRoundNumber *>(SharedData::allocate_shared(sizeof(SharedData::IndexRoundNumber)));
                new (index_round_number) SharedData::IndexRoundNumber();
                cxlalloc_set_root(INDEX_ROUND_NUMBER_ROOT_INDEX + i, index_round_number);
#endif
            }
            LOG(INFO) << "allocate cxl index round number";

            gsn_committer_.init();

            // Initialize the worker pool.


#ifndef USE_RDMA
            using NodeLeases = SharedData::LeaseRegion<SHARD_SERVER_NUM>;
            void *lease_storage = cxlalloc_memalign(sizeof(NodeLeases), alignof(NodeLeases));
            CHECK(lease_storage != nullptr);
            CHECK_EQ(reinterpret_cast<std::uintptr_t>(lease_storage) % CACHELINE_SIZE, 0u);
            auto *leases = static_cast<NodeLeases *>(lease_storage);
            new (leases) SharedData::LeaseRegion<SHARD_SERVER_NUM>();
            leases->timeout_ms = lease_timeout_ms;
            leases->renew_interval_ms = lease_renew_interval_ms;
            clwb(leases, sizeof(*leases));
            sfence();
            cxlalloc_set_root(LEASE_ROOT_INDEX, leases);
            cluster_region_ = new (SharedData::allocate_shared(sizeof(SharedData::ClusterRegion))) SharedData::ClusterRegion();
            clwb(cluster_region_, sizeof(*cluster_region_));
            sfence();
            cxlalloc_set_root(CLUSTER_ROOT_INDEX, cluster_region_);
            configuration_.header.active_shards = active_shards_;
            configuration_.header.request_workers = REQUEST_WORKER_NUM;
            configuration_.header.settings = SharedData::cluster_settings();
            for (std::size_t index = 0; index < 2; ++index)
                configuration_.header.index[index] = SharedData::shared_offset(cxlalloc_get_root(TAIL_HASH_TABLE_ROOT_INDEX + index));
            for (std::size_t shard = 0; shard < SHARD_SERVER_NUM; ++shard)
            {
                SharedData::ShardResources resources;
                for (std::size_t replica = 0; replica < REPLICATOR_NUM; ++replica)
                {
                    resources.write_stream[replica] = SharedData::shared_offset(cxlalloc_get_root(STREAM_ROOT_INDEX + shard * REPLICATOR_NUM + replica));
                    resources.read_stream[replica] = SharedData::shared_offset(cxlalloc_get_root(READ_STREAM_ROOT_INDEX + shard * REPLICATOR_NUM + replica));
                }
                resources.commit_buffer = SharedData::shared_offset(cxlalloc_get_root(COMMIT_RING_BUFFER_ROOT_INDEX + shard));
                resources.commit_metadata = SharedData::shared_offset(cxlalloc_get_root(COMMIT_BUFFER_METADATA_ROOT_INDEX + shard));
                resources.local_cuts = SharedData::shared_offset(cxlalloc_get_root(CUT_REGION_ROOT_INDEX + shard * REPLICATOR_NUM));
                resources.reader_flags = SharedData::shared_offset(reader_switch_flag_[shard * REQUEST_WORKER_NUM]);
                resources.lease = SharedData::shared_offset(&leases->nodes[shard]);
                resources.stream_catalog = SharedData::allocate_stream_catalog();
                resources.global_cut_consumers = SharedData::shared_offset(
                    allocate_control<SharedData::GlobalCutConsumer>(REQUEST_WORKER_NUM));
                configuration_.shards.push_back(resources);
            }
            SharedData::persist_configuration(cluster_region_, configuration_);
            SharedData::startup_configuration = configuration_;
            settings_ = SharedData::cluster_settings();
            SharedData::node_leases.start(leases, SHARD_SERVER_NUM);
            joins_.start(reconfig_port, SharedData::node_leases.expired_shards);
#endif

            thread_pool_ = std::make_unique<ThreadPool>(SHARD_SERVER_NUM);
            LOG(INFO) << "init thread pool";
            start_timer();
        }

#ifndef USE_RDMA
        void rebuild_reader_directory()
        {
            delete[] reader_switch_flag_;
            reader_switch_flag_ = new SharedData::SwitchFlag *[configuration_.shards.size() * REQUEST_WORKER_NUM];
            for (std::size_t shard = 0; shard < configuration_.shards.size(); ++shard)
                for (std::size_t worker = 0; worker < static_cast<std::size_t>(REQUEST_WORKER_NUM); ++worker)
                    reader_switch_flag_[shard * REQUEST_WORKER_NUM + worker] =
                        SharedData::shared_pointer<SharedData::SwitchFlag>(configuration_.shards[shard].reader_flags) + worker;
        }

        void clone_front_index(std::uint64_t source_offset)
        {
            auto *source = SharedData::shared_pointer<star::CCHashTable>(source_offset);
            clflushopt(source, sizeof(*source));
            sfence();
            for (std::size_t index = 0; index < 2; ++index)
            {
                auto *target = new (SharedData::allocate_shared(sizeof(star::CCHashTable)))
                    star::CCHashTable(BUCKET_CNT, configuration_.shards.size());
                source->copy_to(*target);
                clwb(target, sizeof(*target));
                configuration_.header.index[index] = SharedData::shared_offset(target);
            }
            sfence();
        }

        void reset_shard_controls(std::size_t shard, std::uint64_t progress, SharedData::LeaseSlot *slot,
                                  bool new_delta_buffer = false)
        {
            auto &resources = configuration_.shards.at(shard);
            const auto *old_metadata = SharedData::shared_pointer<SharedData::CommitBufferMetadata>(resources.commit_metadata);
            const auto position = new_delta_buffer ? static_cast<std::uint32_t>(progress) : SharedData::DeltaRing::restore_position(
                SharedData::read_cxl_word(old_metadata->write_position), static_cast<std::uint32_t>(progress));
            resources.reader_flags = SharedData::shared_offset(allocate_control<SharedData::SwitchFlag>(REQUEST_WORKER_NUM));
            resources.global_cut_consumers = SharedData::shared_offset(
                allocate_control<SharedData::GlobalCutConsumer>(REQUEST_WORKER_NUM));
            auto *cuts = allocate_control<SharedData::CutRegion>(REPLICATOR_NUM);
            for (std::size_t replica = 0; replica < REPLICATOR_NUM; ++replica)
                cuts[replica].value.store(configuration_.header.replica_mask & (1u << replica) ?
                    progress : std::numeric_limits<std::uint64_t>::max(), std::memory_order_relaxed);
            clwb(cuts, sizeof(*cuts) * REPLICATOR_NUM);
            resources.local_cuts = SharedData::shared_offset(cuts);
            auto *metadata = allocate_control<SharedData::CommitBufferMetadata>();
            metadata->key_num.store(static_cast<std::uint32_t>(progress), std::memory_order_relaxed);
            metadata->write_position.store(position, std::memory_order_relaxed);
            metadata->consumed[0].store(progress, std::memory_order_relaxed);
            metadata->reclaimed.store(SharedData::DeltaRing::reclaim_before(position, position, position),
                                      std::memory_order_relaxed);
            clwb(metadata, sizeof(*metadata));
            resources.commit_metadata = SharedData::shared_offset(metadata);
            resources.lease = SharedData::shared_offset(slot);
            resources.resume_cut = progress;
            sfence();
        }

        void publish_restored_configuration(const std::vector<std::uint64_t> &sealed)
        {
            prepare_configuration();
            SharedData::startup_configuration = configuration_;
            rebuild_reader_directory();
            gsn.resize(sealed.size());
            for (std::size_t shard = 0; shard < sealed.size(); ++shard)
            {
                gsn[shard].value = sealed[shard] >> 32;
            }
            gsn_dirty.store(true, std::memory_order_relaxed);
            gsn_committer_.commit(configuration_epoch_, 0);
            const auto old_switch = (switch_epoch_ << 32) | (version_number_ << 1) | switch_flag_value;
            const auto new_switch = (configuration_epoch_ << 32) | (++version_number_ << 1);
            SharedData::try_switch_flag(sequencer_switch_flag_, reader_switch_flag_, old_switch, new_switch,
                                        configuration_.shards.size() * REQUEST_WORKER_NUM);
            switch_flag_value = 0;
            switch_epoch_ = configuration_epoch_;
            gsn_committer_.update_round_number(configuration_epoch_);
            gsn_dirty.store(false, std::memory_order_relaxed);
            hash_table_dirty.store(false, std::memory_order_relaxed);
            thread_pool_ = std::make_unique<ThreadPool>(sealed.size(), sealed);
        }

        void restore_sequencer(std::uint64_t timeout, std::uint64_t interval, std::uint16_t port,
                               const SharedData::RecoveryOptions &options)
        {
            using Leases = SharedData::LeaseRegion<SHARD_SERVER_NUM>;
            auto *previous = static_cast<Leases *>(cxlalloc_get_root(LEASE_ROOT_INDEX));
            if (!previous)
                throw std::runtime_error("no sequencer incarnation to recover");
            const auto renewal = SharedData::read_cxl_word(previous->nodes[SHARD_SERVER_NUM].renewal);
            clflushopt(&previous->timeout_ms, CACHELINE_SIZE);
            sfence();
            const auto wait = std::chrono::milliseconds(2 * previous->timeout_ms + previous->renew_interval_ms);
            const auto deadline = SharedData::LeaseClock::now() + wait;
            while (SharedData::LeaseClock::now() < deadline)
            {
                std::this_thread::sleep_for(std::chrono::milliseconds(50));
                if (SharedData::read_cxl_word(previous->nodes[SHARD_SERVER_NUM].renewal) != renewal)
                    throw std::runtime_error("old sequencer is still renewing its lease");
            }
            SharedData::node_leases.fence_slot(&previous->nodes[SHARD_SERVER_NUM]);
            auto cut = SharedData::recover_published_prefix(true, options.survivors, options.survivors != 3);
            configuration_ = cut.configuration;
            if (configuration_.header.settings != SharedData::cluster_settings())
                throw std::runtime_error("recovery settings differ from the persisted cluster layout");
            for (const auto &resources : configuration_.shards)
                SharedData::node_leases.fence_slot(SharedData::shared_pointer<SharedData::LeaseSlot>(resources.lease));
            const auto fenced_until = SharedData::LeaseClock::now() +
                std::chrono::milliseconds(previous->timeout_ms + previous->renew_interval_ms);
            while (SharedData::LeaseClock::now() < fenced_until)
                std::this_thread::sleep_for(std::chrono::milliseconds(50));
            // Maintenance uses the same fail-stop boundary as role replacement.
            // Every old reader must be gone before any segment can be reused.
            const auto survived = options.survivors & static_cast<unsigned>(configuration_.header.replica_mask);
            if (survived == 0 || (options.rebuild & survived) ||
                ((options.trim || options.erase) && !(survived & 1)) ||
                (!(survived & 1) && !(options.rebuild & 1)))
                throw std::runtime_error("recovery requires a surviving replica and a rebuilt primary");
            const bool maintenance = options.survivors != 3 || options.rebuild || options.trim || options.erase;
            std::vector<SharedData::RecoveredShard> states;
            auto lifecycles = SharedData::load_stream_lifecycles(configuration_, survived);
            if (maintenance)
            {
                const unsigned source = survived & 1 ? 0 : 1;
                for (std::size_t shard = 0; shard < configuration_.shards.size(); ++shard)
                    states.push_back(source == 0 ? SharedData::recover_shard_from_index(cut, shard, source, survived) :
                        SharedData::recover_shard_from_log(cut, shard, source, lifecycles));
                if (options.trim || options.erase)
                {
                    const bool all_shards = options.erase && options.shard == std::numeric_limits<std::uint32_t>::max();
                    if (!all_shards && options.shard >= states.size())
                        throw std::runtime_error("invalid lifecycle shard ID");
                    bool found = false;
                    const auto begin = all_shards ? 0 : options.shard;
                    const auto end = all_shards ? states.size() : options.shard + 1;
                    for (std::size_t shard = begin; shard < end; ++shard)
                    {
                        if (!states[shard].streams.contains(options.key))
                        {
                            if (all_shards)
                                continue;
                            throw std::runtime_error("stream does not exist on the selected shard");
                        }
                        found = true;
                        if (options.lsn > (cut.progress[shard] >> 32))
                            throw std::runtime_error("trim must name an existing stream and a committed LSN");
                        auto &state = states[shard];
                        SharedData::prepare_lifecycle_replica(cut, configuration_, state, shard, options.key, survived);
                        auto &stream = state.streams.at(options.key);
                        const auto lsn = options.erase ? static_cast<std::uint32_t>(state.progress >> 32) + 1 : options.lsn;
                        lifecycles[{shard, options.key}] = SharedData::trim_stream(
                            SharedData::shared_pointer<char>(configuration_.shards[shard].write_stream[source]),
                            stream, shard, lsn, options.erase);
                        if (options.erase)
                        {
                            // Publish a placeholder and the zero-byte deletion delta
                            // through the same cut as the removed index boundary.
                            const auto cursor = static_cast<std::uint32_t>(state.progress);
                            if (state.read_tail + CACHELINE_SIZE > READ_STREAM_SIZE)
                                throw std::runtime_error("deletion control log exhausted");
                            const DeletionRecord placeholder(lsn, options.key);
                            for (unsigned replica = 0; replica < 2; ++replica)
                                if (survived & (1u << replica))
                                {
                                    auto *destination = SharedData::shared_pointer<char>(configuration_.shards[shard].read_stream[replica]) + state.read_tail;
                                    std::memcpy(destination, &placeholder, sizeof(placeholder));
                                    clflushopt(destination, sizeof(placeholder));
                                }
                            if (survived & 1)
                            {
                                auto *metadata = SharedData::shared_pointer<SharedData::CommitBufferMetadata>(
                                    configuration_.shards[shard].commit_metadata);
                                const auto position = SharedData::DeltaRing::restore_position(
                                    SharedData::read_cxl_word(metadata->write_position), cursor);
                                auto *delta = SharedData::shared_pointer<std::uint64_t>(configuration_.shards[shard].commit_buffer) +
                                    SharedData::DeltaRing::offset(position);
                                *delta = std::uint64_t(options.key) << 32;
                                clflushopt(delta, sizeof(*delta));
                                metadata->write_position.store(position + 1, std::memory_order_relaxed);
                                clwb(&metadata->write_position, sizeof(metadata->write_position));
                            }
                            sfence();
                            state.read_tail += CACHELINE_SIZE;
                            state.progress = (std::uint64_t(lsn) << 32) | (cursor + 1);
                            cut.progress[shard] = state.progress;
                        }
                    }
                    if (!found)
                        throw std::runtime_error("stream does not exist");
                }
                for (unsigned replica = 0; replica < 2; ++replica)
                    if (options.rebuild & (1u << replica))
                    {
                        for (std::size_t shard = 0; shard < states.size(); ++shard)
                            SharedData::rebuild_shard_replica(configuration_.shards[shard], states[shard], source, replica);
                        auto *log = SharedData::allocate_shared(std::uint64_t(GSN_SET_CNT) * sizeof(SharedData::GSNSet));
                        SharedData::copy_persistent_prefix(log, cxlalloc_get_root(GSN_BUFFER_ROOT_INDEX + source), cut.next_offset);
                        cxlalloc_set_root(GSN_BUFFER_ROOT_INDEX + replica, log);
                    }
                configuration_.header.replica_mask = survived | options.rebuild;
                SharedData::build_recovered_indexes(configuration_, states);
                if (!lifecycles.empty())
                    SharedData::persist_stream_lifecycles(configuration_, lifecycles);
                if (options.rebuild)
                {
                    // Copy the immutable history before publishing its new head;
                    // a crash partway through leaves the survivor's old chain intact.
                    const auto history = SharedData::configuration_history(
                        static_cast<SharedData::ClusterRegion *>(cxlalloc_get_root(CLUSTER_ROOT_INDEX)), survived);
                    std::uint64_t previous_offsets[2]{};
                    for (auto entry : history)
                    {
                        if (entry.header.epoch > cut.configuration.header.epoch)
                            break;
                        std::copy(previous_offsets, previous_offsets + 2, entry.header.previous);
                        if (entry.header.lifecycle[0] || entry.header.lifecycle[1])
                            SharedData::persist_stream_lifecycles(entry, SharedData::load_stream_lifecycles(entry, survived));
                        SharedData::persist_configuration(nullptr, entry, false);
                        std::copy(std::begin(entry.offsets), std::end(entry.offsets), previous_offsets);
                    }
                    std::copy(previous_offsets, previous_offsets + 2, configuration_.offsets);
                }
            }
            configuration_epoch_ = configuration_.header.epoch;
            switch_epoch_ = configuration_epoch_;
            cluster_region_ = static_cast<SharedData::ClusterRegion *>(cxlalloc_get_root(CLUSTER_ROOT_INDEX));
            auto *leases = allocate_control<Leases>();
            leases->timeout_ms = timeout;
            leases->renew_interval_ms = interval;
            const auto inactive = SharedData::shard_mask(configuration_.shards.size());
            leases->revoked.store(inactive, std::memory_order_relaxed);
            clwb(leases, sizeof(*leases));
            sfence();
            cxlalloc_set_root(LEASE_ROOT_INDEX, leases);
            SharedData::node_leases.expired_shards.store(inactive, std::memory_order_release);
            SharedData::node_leases.start(leases, SHARD_SERVER_NUM);
            for (std::size_t shard = 0; shard < configuration_.shards.size(); ++shard)
            {
                if (!(survived & 1))
                    configuration_.shards[shard].commit_buffer = SharedData::shared_offset(
                        SharedData::allocate_shared(COMMIT_RING_BUFFER_CAPACITY * sizeof(std::uint64_t)));
                reset_shard_controls(shard, cut.progress[shard], allocate_control<SharedData::LeaseSlot>(), !(survived & 1));
            }
            if (!maintenance)
                clone_front_index(cut.header.index_offset);
            sequencer_switch_flag_ = allocate_control<SharedData::SwitchFlag>();
            cxlalloc_set_root(SWITCH_FLAG_ROOT_INDEX + SHARD_SERVER_NUM * REQUEST_WORKER_NUM, sequencer_switch_flag_);
            reader_switch_flag_ = nullptr;
            if (!(survived & 1))
            {
                auto *producer = allocate_control<SharedData::RoundNumber>();
                producer->round.store((cut.configuration.header.epoch << 32) | cut.header.round,
                                      std::memory_order_relaxed);
                clflushopt(producer, sizeof(*producer));
                sfence();
                cxlalloc_set_root(ROUND_NUMBER_ROOT_INDEX, producer);
            }
            gsn_committer_.init();
            gsn_committer_.restore(cut.header.round, cut.next_offset);
            active_shards_ = 0;
            observed_leases_ = inactive;
            settings_ = SharedData::cluster_settings();
            publish_restored_configuration(cut.progress);
            joins_.start(port, SharedData::node_leases.expired_shards);
            start_timer();
            LOG(INFO) << "sequencer recovered: round=" << cut.header.round << " epoch=" << configuration_epoch_;
        }

        void recover_shard(const std::shared_ptr<SharedData::ReconfigInbox::Operation> &operation,
                           std::uint64_t failed)
        {
            const auto shard = operation->request.shard;
            if (shard >= configuration_.shards.size() || !(failed & (std::uint64_t{1} << shard)) ||
                operation->request.settings != settings_)
            {
                SharedData::ReconfigInbox::finish(operation, {1, configuration_epoch_});
                return;
            }
            const auto front = version_number_ == 1 ? 0 : switch_flag_value;
            gsn_committer_.commit(configuration_epoch_,
                hash_table_dirty.load(std::memory_order_acquire) ? 1 - switch_flag_value : front);
            switch_hash_table();
            auto sealed = thread_pool_->sealed_cuts();
            thread_pool_.reset();
            const auto source = configuration_.header.index[version_number_ == 1 ? 0 : switch_flag_value];
            clone_front_index(source);
            auto *slot = allocate_control<SharedData::LeaseSlot>();
            reset_shard_controls(shard, sealed[shard], slot);
            SharedData::node_leases.replace_shard(shard, slot);
            active_shards_ = SharedData::shard_mask(configuration_.shards.size()) &
                ~(failed & ~(std::uint64_t{1} << shard));
            publish_restored_configuration(sealed);
            recovery_admin_.cancel(shard);
            SharedData::ReconfigInbox::finish(operation, {0, configuration_epoch_});
            LOG(INFO) << "shard incarnation recovered: shard=" << shard << " epoch=" << configuration_epoch_;
        }

        void prepare_configuration()
        {
            std::copy(std::begin(configuration_.offsets), std::end(configuration_.offsets), configuration_.header.previous);
            configuration_.header.epoch = ++configuration_epoch_;
            configuration_.header.first_round = gsn_committer_.get_round() + 1;
            configuration_.header.cut_offset = gsn_committer_.next_offset();
            configuration_.header.active_shards = active_shards_;
            SharedData::persist_configuration(cluster_region_, configuration_);
            gsn_committer_.configure(configuration_);
        }

        template <typename T>
        static T *allocate_control(std::size_t count = 1)
        {
            auto *objects = new (SharedData::allocate_shared(sizeof(T) * count)) T[count]();
            clwb(objects, sizeof(T) * count);
            sfence();
            return objects;
        }

        void configure_maintenance(std::vector<std::string> addresses, std::uint16_t port,
                                   ShardRecoveryAdmin::Settings recovery)
        {
            maintenance_addresses_ = std::move(addresses);
            maintenance_port_ = port;
            recovery_admin_.start(std::move(recovery), maintenance_addresses_);
            recovery_admin_.schedule(SharedData::shard_mask(configuration_.shards.size()) & ~active_shards_,
                                      configuration_epoch_);
        }

        SharedData::JoinReply shard_control(std::size_t shard, std::uint32_t command,
                                            std::uint64_t value = 0,
                                            std::uint32_t key = std::numeric_limits<std::uint32_t>::max())
        {
            const auto address = shard < maintenance_addresses_.size() ? maintenance_addresses_[shard] :
                "192.168.100." + std::to_string(shard + 3);
            return SharedData::request_join(address, maintenance_port_, {command, key, value});
        }

        void finish_current_round()
        {
            const auto front = version_number_ == 1 ? 0 : switch_flag_value;
            gsn_committer_.commit(configuration_epoch_,
                hash_table_dirty.load(std::memory_order_acquire) ? 1 - switch_flag_value : front);
            switch_hash_table();
        }

        void quiesce_cluster(bool drop_backup = false)
        {
            std::vector<std::uint64_t> targets;
            for (std::size_t shard = 0; shard < configuration_.shards.size(); ++shard)
                targets.push_back(shard_control(shard, SharedData::pause_ingress).epoch);
            if (drop_backup)
            {
                for (std::size_t shard = 0; shard < targets.size(); ++shard)
                    shard_control(shard, SharedData::drop_backup);
                configuration_.header.replica_mask = 1;
                prepare_configuration();
                gsn_dirty.store(true, std::memory_order_relaxed);
                finish_current_round();
            }
            const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(100);
            while (true)
            {
                thread_pool_->execute_all();
                finish_current_round();
                const auto sealed = thread_pool_->sealed_cuts();
                bool drained = true;
                for (std::size_t shard = 0; shard < targets.size(); ++shard)
                    drained &= (sealed[shard] >> 32) >= targets[shard];
                if (drained)
                    break;
                if (std::chrono::steady_clock::now() >= deadline)
                    throw std::runtime_error("writers did not reach the maintenance cut");
                std::this_thread::yield();
            }
            for (std::size_t shard = 0; shard < targets.size(); ++shard)
                shard_control(shard, SharedData::arm_readers, configuration_epoch_ + 1);
            prepare_configuration();
            gsn_dirty.store(true, std::memory_order_relaxed);
            finish_current_round();
            for (std::size_t shard = 0; shard < targets.size(); ++shard)
                shard_control(shard, SharedData::wait_readers);
            for (std::size_t shard = 0; shard < targets.size(); ++shard)
                shard_control(shard, SharedData::stop_writers);
        }

        void resume_cluster(std::uint32_t key = std::numeric_limits<std::uint32_t>::max())
        {
            for (std::size_t shard = 0; shard < configuration_.shards.size(); ++shard)
                shard_control(shard, SharedData::reload_shard, configuration_epoch_, key);
            for (std::size_t shard = 0; shard < configuration_.shards.size(); ++shard)
                shard_control(shard, SharedData::resume_shard);
        }

        void online_lifecycle(const std::shared_ptr<SharedData::ReconfigInbox::Operation> &operation,
                              std::uint64_t failed)
        {
            const auto &request = operation->request;
            const auto key = static_cast<std::uint32_t>(request.settings >> 32);
            const auto trim_lsn = static_cast<std::uint32_t>(request.settings);
            const bool erase = request.format == SharedData::delete_request;
            const bool all = request.shard == std::numeric_limits<std::uint32_t>::max();
            if (failed || rebuilding_backup_ || (request.format != SharedData::trim_request && !erase) ||
                (!all && request.shard >= configuration_.shards.size()) || (all && !erase) ||
                (WORKLOAD == 4 && key < 2 * USER_CNT))
            {
                SharedData::ReconfigInbox::finish(operation, {1, configuration_epoch_});
                return;
            }
            finish_current_round();
            auto cut = SharedData::recover_published_prefix(false);
            // Validate before parking any service threads.
            const auto begin = all ? 0 : request.shard;
            const auto end = all ? configuration_.shards.size() : request.shard + 1;
            bool found = false;
            for (std::size_t shard = begin; shard < end; ++shard)
            {
                const auto state = SharedData::recover_shard_from_index(cut, shard);
                found |= state.streams.contains(key);
                if (!erase && (!state.streams.contains(key) || trim_lsn > (state.progress >> 32)))
                {
                    SharedData::ReconfigInbox::finish(operation, {1, configuration_epoch_});
                    return;
                }
            }
            if (!found)
            {
                SharedData::ReconfigInbox::finish(operation, {1, configuration_epoch_});
                return;
            }
            quiesce_cluster();
            // Catch the second index up before reusing a slot for a deletion.
            // Ingress and writers are stopped, so this needs no producer wait.
            thread_pool_->execute_all();
            finish_current_round();
            cut = SharedData::recover_published_prefix(false);
            auto lifecycles = SharedData::load_stream_lifecycles(configuration_);
            std::vector<SharedData::RecoveredShard> states;
            for (std::size_t shard = 0; shard < configuration_.shards.size(); ++shard)
                states.push_back(SharedData::recover_shard_from_index(cut, shard));
            for (std::size_t shard = begin; shard < end; ++shard)
            {
                auto &state = states[shard];
                if (!state.streams.contains(key))
                    continue;
                SharedData::prepare_lifecycle_replica(cut, configuration_, state, shard, key);
                const auto lsn = erase ? static_cast<std::uint32_t>(state.progress >> 32) + 1 : trim_lsn;
                lifecycles[{shard, key}] = SharedData::trim_stream(
                    SharedData::shared_pointer<char>(configuration_.shards[shard].write_stream[0]),
                    state.streams.at(key), shard, lsn, erase);
                if (erase)
                {
                    SharedData::append_deletion(configuration_, state, shard, key, lsn);
                    auto *metadata = SharedData::shared_pointer<SharedData::CommitBufferMetadata>(configuration_.shards[shard].commit_metadata);
                    metadata->key_num.store(static_cast<std::uint32_t>(state.progress), std::memory_order_release);
                    clwb(metadata, CACHELINE_SIZE);
                    auto *cuts = SharedData::shared_pointer<SharedData::CutRegion>(configuration_.shards[shard].local_cuts);
                    for (unsigned replica = 0; replica < 2; ++replica)
                        if (configuration_.header.replica_mask & (1u << replica))
                        {
                            cuts[replica].value.store(state.progress, std::memory_order_release);
                            clwb(cuts + replica, sizeof(*cuts));
                        }
                    sfence();
                }
            }
            // Zero deltas use the ordinary consumer and its publication fence.
            // The lifecycle snapshot becomes effective in the same new cut.
            thread_pool_->execute_all();
            auto sealed = thread_pool_->sealed_cuts();
            thread_pool_.reset();
            SharedData::build_recovered_indexes(configuration_, states);
            SharedData::persist_stream_lifecycles(configuration_, lifecycles);
            publish_restored_configuration(sealed);
            resume_cluster(key);
            SharedData::ReconfigInbox::finish(operation, {0, configuration_epoch_});
            LOG(INFO) << "online lifecycle committed: epoch=" << configuration_epoch_ << " key=" << key;
        }

        void online_backup(const std::shared_ptr<SharedData::ReconfigInbox::Operation> &operation,
                            std::uint64_t failed)
        {
            const auto command = operation->request.format;
            if (command == SharedData::maintenance_status)
            {
                const auto status = (std::uint64_t(rebuilding_backup_) << 63) |
                    (configuration_.header.replica_mask << 56) | configuration_epoch_;
                SharedData::ReconfigInbox::finish(operation, {0, status});
                return;
            }
            if (command == SharedData::finish_backup_request)
            {
                if (!rebuilding_backup_ || operation->request.settings != backup_epoch_ ||
                    !backup_ready_.load(std::memory_order_acquire))
                {
                    SharedData::ReconfigInbox::finish(operation, {1, configuration_epoch_});
                    return;
                }
                if (backup_error_ || failed || configuration_epoch_ != backup_epoch_)
                {
                    // The survivor remains authoritative. An interrupted copy
                    // is never admitted as a replica. A membership change also
                    // invalidates the snapshot, even if its shard has recovered.
                    for (std::size_t shard = 0; shard < configuration_.shards.size(); ++shard)
                        if (!(failed & (std::uint64_t{1} << shard)))
                        {
                            try { shard_control(shard, SharedData::cancel_backup); }
                            catch (const std::exception &error) { LOG(WARNING) << error.what(); }
                        }
                    rebuilding_backup_ = false;
                    SharedData::ReconfigInbox::finish(operation, {0, configuration_epoch_});
                    backup_thread_.join();
                    LOG(ERROR) << "online backup rebuild aborted; cluster remains degraded";
                    return;
                }
                quiesce_cluster();
                auto cut = SharedData::recover_published_prefix(false, 1);
                for (std::size_t shard = 0; shard < configuration_.shards.size(); ++shard)
                    shard_control(shard, SharedData::catchup_backup);
                SharedData::copy_persistent_prefix(static_cast<char *>(backup_cut_log_) + backup_cut_offset_,
                    static_cast<char *>(cxlalloc_get_root(GSN_BUFFER_ROOT_INDEX)) + backup_cut_offset_,
                    cut.next_offset - backup_cut_offset_);
                // Rebuild the immutable configuration chain, including lifecycle
                // snapshots whose old backup copy may have been lost.
                const auto history = SharedData::configuration_history(cluster_region_, 1);
                std::uint64_t previous[2]{};
                for (auto entry : history)
                {
                    if (entry.header.epoch > configuration_epoch_)
                        break;
                    std::copy(previous, previous + 2, entry.header.previous);
                    if (entry.header.lifecycle[0])
                        SharedData::persist_stream_lifecycles(entry, SharedData::load_stream_lifecycles(entry, 1));
                    SharedData::persist_configuration(nullptr, entry, false);
                    std::copy(std::begin(entry.offsets), std::end(entry.offsets), previous);
                    if (entry.header.epoch == configuration_epoch_)
                    {
                        std::copy(previous, previous + 2, configuration_.offsets);
                        std::copy(std::begin(entry.header.lifecycle), std::end(entry.header.lifecycle), configuration_.header.lifecycle);
                    }
                }
                cxlalloc_set_root(GSN_BUFFER_ROOT_INDEX + 1, backup_cut_log_);
                auto sealed = thread_pool_->sealed_cuts();
                thread_pool_.reset();
                clone_front_index(cut.header.index_offset);
                configuration_.header.replica_mask = 3;
                for (std::size_t shard = 0; shard < sealed.size(); ++shard)
                {
                    auto *backup = SharedData::shared_pointer<SharedData::CutRegion>(configuration_.shards[shard].local_cuts) + 1;
                    backup->value.store(sealed[shard], std::memory_order_release);
                    clwb(backup, sizeof(*backup));
                }
                sfence();
                publish_restored_configuration(sealed);
                resume_cluster();
                rebuilding_backup_ = false;
                SharedData::ReconfigInbox::finish(operation, {0, configuration_epoch_});
                backup_thread_.join();
                LOG(INFO) << "online backup rebuild committed: epoch=" << configuration_epoch_;
                return;
            }
            if (failed || rebuilding_backup_ || !(configuration_.header.replica_mask & 1) ||
                (command != SharedData::degrade_backup_request && command != SharedData::rebuild_backup_request))
            {
                SharedData::ReconfigInbox::finish(operation, {1, configuration_epoch_});
                return;
            }
            if (command == SharedData::degrade_backup_request)
            {
                if (configuration_.header.replica_mask != 1)
                {
                    quiesce_cluster(true);
                    auto cut = SharedData::recover_published_prefix(false, 1);
                    auto sealed = thread_pool_->sealed_cuts();
                    thread_pool_.reset();
                    clone_front_index(cut.header.index_offset);
                    publish_restored_configuration(sealed);
                    resume_cluster();
                }
                SharedData::ReconfigInbox::finish(operation, {0, configuration_epoch_});
                return;
            }
            if (configuration_.header.replica_mask != 1)
            {
                SharedData::ReconfigInbox::finish(operation, {1, configuration_epoch_});
                return;
            }
            auto targets = configuration_.shards;
            for (auto &resources : targets)
            {
                resources.write_stream[1] = SharedData::shared_offset(SharedData::allocate_shared(WRITE_STREAM_SIZE));
                resources.read_stream[1] = SharedData::shared_offset(SharedData::allocate_shared(READ_STREAM_SIZE));
                resources.stream_catalog = SharedData::allocate_stream_catalog();
            }
            backup_cut_log_ = SharedData::allocate_shared(std::uint64_t(GSN_SET_CNT) * sizeof(SharedData::GSNSet));
            quiesce_cluster();
            auto cut = SharedData::recover_published_prefix(false, 1);
            auto sealed = thread_pool_->sealed_cuts();
            thread_pool_.reset();
            configuration_.shards = std::move(targets);
            clone_front_index(cut.header.index_offset);
            publish_restored_configuration(sealed);
            backup_cut_offset_ = gsn_committer_.next_offset();
            backup_epoch_ = configuration_epoch_;
            rebuilding_backup_ = true;
            backup_ready_.store(false, std::memory_order_relaxed);
            backup_error_ = nullptr;
            for (std::size_t shard = 0; shard < configuration_.shards.size(); ++shard)
            {
                shard_control(shard, SharedData::reload_shard, configuration_epoch_);
                shard_control(shard, SharedData::copy_backup);
            }
            for (std::size_t shard = 0; shard < configuration_.shards.size(); ++shard)
                shard_control(shard, SharedData::resume_shard);
            const auto count = configuration_.shards.size();
            backup_thread_ = std::thread([this, count]
            {
                try
                {
                    init_cxlalloc(10, 8, 16, SHARD_SERVER_NUM);
                    SharedData::copy_persistent_prefix(backup_cut_log_, cxlalloc_get_root(GSN_BUFFER_ROOT_INDEX), backup_cut_offset_);
                    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(100);
                    while (true)
                    {
                        bool complete = true;
                        for (std::size_t shard = 0; shard < count; ++shard)
                            complete &= shard_control(shard, SharedData::backup_ready).epoch != 0;
                        if (complete)
                            break;
                        if (std::chrono::steady_clock::now() >= deadline)
                            throw std::runtime_error("backup copy timed out");
                        std::this_thread::sleep_for(std::chrono::milliseconds(50));
                    }
                }
                catch (...) { backup_error_ = std::current_exception(); }
                backup_ready_.store(true, std::memory_order_release);
                SharedData::request_join("127.0.0.1", control_port_, {SharedData::finish_backup_request, 0, backup_epoch_});
            });
            SharedData::ReconfigInbox::finish(operation, {0, configuration_epoch_});
            LOG(INFO) << "online backup copy started: epoch=" << configuration_epoch_;
        }

        void join_shard(std::uint64_t failed)
        {
            auto operation = joins_.take();
            SharedData::node_leases.expired_shards.fetch_and(~SharedData::reconfiguration_pending, std::memory_order_acq_rel);
            if (!operation)
                return;
            if (operation->request.format >= SharedData::trim_request)
            {
                if (operation->request.format <= SharedData::delete_request)
                    online_lifecycle(operation, failed);
                else
                    online_backup(operation, failed);
                return;
            }
            if (operation->request.format == 2)
            {
                recover_shard(operation, failed);
                return;
            }
            const auto shard = configuration_.shards.size();
            if (operation->request.format != 1 || operation->request.shard != shard ||
                shard >= SharedData::max_cluster_shards || operation->request.settings != settings_)
            {
                SharedData::ReconfigInbox::finish(operation, {1, configuration_epoch_});
                return;
            }

            // Finish the old round before cloning its visible prefix. The two
            // barriers have already parked the workers; appends may accumulate
            // in their existing streams while reconfiguration runs.
            const bool changed = hash_table_dirty.load(std::memory_order_acquire);
            const auto visible = version_number_ == 1 ? 0 : switch_flag_value;
            gsn_committer_.commit(configuration_epoch_, changed ? 1 - switch_flag_value : visible);
            switch_hash_table();
            auto sealed = thread_pool_->sealed_cuts();
            thread_pool_.reset();

            SharedData::ShardResources resources;
            for (std::size_t replica = 0; replica < REPLICATOR_NUM; ++replica)
            {
                resources.write_stream[replica] = SharedData::shared_offset(SharedData::allocate_shared(WRITE_STREAM_SIZE));
                resources.read_stream[replica] = SharedData::shared_offset(SharedData::allocate_shared(READ_STREAM_SIZE));
            }
            resources.commit_buffer = SharedData::shared_offset(SharedData::allocate_shared(COMMIT_RING_BUFFER_CAPACITY * sizeof(std::uint64_t)));
            resources.commit_metadata = SharedData::shared_offset(allocate_control<SharedData::CommitBufferMetadata>());
            resources.local_cuts = SharedData::shared_offset(allocate_control<SharedData::CutRegion>(REPLICATOR_NUM));
            for (unsigned replica = 0; replica < REPLICATOR_NUM; ++replica)
                if (!(configuration_.header.replica_mask & (1u << replica)))
                {
                    auto *cut = SharedData::shared_pointer<SharedData::CutRegion>(resources.local_cuts) + replica;
                    cut->value.store(std::numeric_limits<std::uint64_t>::max(), std::memory_order_relaxed);
                    clflushopt(cut, sizeof(*cut));
                }
            sfence();
            resources.reader_flags = SharedData::shared_offset(allocate_control<SharedData::SwitchFlag>(REQUEST_WORKER_NUM));
            resources.global_cut_consumers = SharedData::shared_offset(
                allocate_control<SharedData::GlobalCutConsumer>(REQUEST_WORKER_NUM));
            auto *lease = allocate_control<SharedData::LeaseSlot>();
            resources.lease = SharedData::shared_offset(lease);
            resources.stream_catalog = SharedData::allocate_stream_catalog();

            const auto front = version_number_ == 1 ? 0 : switch_flag_value;
            auto *source = SharedData::shared_pointer<star::CCHashTable>(configuration_.header.index[front]);
            clflushopt(source, sizeof(*source));
            sfence();
            for (std::size_t index = 0; index < 2; ++index)
            {
                auto *expanded = new (SharedData::allocate_shared(sizeof(star::CCHashTable))) star::CCHashTable(BUCKET_CNT, shard + 1);
                source->copy_to(*expanded);
                clwb(expanded, sizeof(*expanded));
                configuration_.header.index[index] = SharedData::shared_offset(expanded);
            }
            clflushopt(source, sizeof(*source));
            sfence();
            configuration_.shards.push_back(resources);
            active_shards_ = SharedData::shard_mask(shard + 1) & ~failed;
            prepare_configuration();
            SharedData::startup_configuration = configuration_;
            gsn.resize(shard + 1);
            sealed.push_back(0);

            auto **readers = new SharedData::SwitchFlag *[(shard + 1) * REQUEST_WORKER_NUM];
            for (std::size_t i = 0; i <= shard; ++i)
                for (std::size_t worker = 0; worker < static_cast<std::size_t>(REQUEST_WORKER_NUM); ++worker)
                    readers[i * REQUEST_WORKER_NUM + worker] = SharedData::shared_pointer<SharedData::SwitchFlag>(
                        configuration_.shards[i].reader_flags) + worker;
            delete[] reader_switch_flag_;
            reader_switch_flag_ = readers;
            SharedData::node_leases.add_shard(shard, lease);

            gsn_dirty.store(true, std::memory_order_relaxed);
            gsn_committer_.commit(configuration_epoch_, 0);
            const auto old_switch = (switch_epoch_ << 32) | (version_number_ << 1) | switch_flag_value;
            const auto new_switch = (configuration_epoch_ << 32) | (++version_number_ << 1);
            SharedData::try_switch_flag(sequencer_switch_flag_, reader_switch_flag_, old_switch, new_switch,
                                        (shard + 1) * REQUEST_WORKER_NUM);
            switch_flag_value = 0;
            switch_epoch_ = configuration_epoch_;
            gsn_committer_.update_round_number(configuration_epoch_);
            gsn_dirty.store(false, std::memory_order_relaxed);
            hash_table_dirty.store(false, std::memory_order_relaxed);
            thread_pool_ = std::make_unique<ThreadPool>(shard + 1, sealed);
            SharedData::ReconfigInbox::finish(operation, {0, configuration_epoch_});
            LOG(INFO) << "reconfiguration committed: epoch=" << configuration_epoch_
                      << " shards=" << shard + 1 << " cut_bytes=" << SharedData::global_cut_bytes(shard + 1);
        }
#endif

        void update_gsn()
        {
#ifndef USE_RDMA
            const auto notifications = SharedData::node_leases.expired_shards.load(std::memory_order_acquire);
            if (notifications != observed_leases_)
            {
                auto failed = notifications & ~SharedData::reconfiguration_pending;
                if (notifications & SharedData::reconfiguration_pending)
                {
                    join_shard(failed);
                    failed = SharedData::node_leases.expired_shards.load(std::memory_order_acquire) &
                        ~SharedData::reconfiguration_pending;
                }
                const auto active = SharedData::shard_mask(configuration_.shards.size()) & ~failed;
                if (active != active_shards_)
                {
                    const auto newly_failed = active_shards_ & ~active;
                    active_shards_ = active;
                    prepare_configuration();
                    gsn_dirty.store(true, std::memory_order_relaxed);
                    finish_current_round();
                    recovery_admin_.schedule(newly_failed, configuration_epoch_);
                }
                observed_leases_ = failed;
            }
            const bool index_changed = hash_table_dirty.load(std::memory_order_acquire);
            // The CXL switch flag starts at zero. The local flag starts at one
            // to prepare index 0 first, so before the first swap a configuration
            // cut without new deltas must still name the visible index 0.
            const auto visible_index = version_number_ == 1 ? 0 : switch_flag_value;
            const auto index = index_changed ? 1 - switch_flag_value : visible_index;
            gsn_committer_.commit(configuration_epoch_, index);
#else
            gsn_committer_.commit();
#endif
        }

        void update_round_number()
        {
            gsn_committer_.update_round_number(
#ifndef USE_RDMA
                    configuration_epoch_
#endif
                );
        }

        void switch_hash_table()
        {
            // All shards observe the same sequencer switch flag.
            bool is_hash_table_dirty = hash_table_dirty.load(std::memory_order_acquire);
            bool is_gsn_dirty = gsn_dirty.load(std::memory_order_acquire);
            if (is_hash_table_dirty && is_gsn_dirty)
            {
                std::uint64_t old_switch = (static_cast<std::uint64_t>(version_number_) << 1) | (switch_flag_value & 1);
                version_number_++;
                std::uint64_t new_switch = (static_cast<std::uint64_t>(version_number_) << 1) | (1 - (switch_flag_value & 1));
#ifndef USE_RDMA
                old_switch |= switch_epoch_ << 32;
                new_switch |= configuration_epoch_ << 32;
                switch_epoch_ = configuration_epoch_;
#endif

#ifdef USE_RDMA
                // Sequencer is sole writer to sequencer_switch_flag: simple RDMA WRITE
                void* staging = g_rdma.get_staging_buf();
                std::memcpy(staging, &new_switch, sizeof(uint64_t));
                g_rdma.write(staging, sequencer_switch_flag_offset_, sizeof(uint64_t));
                // Validate: read back switch flag
                {
                    uint64_t sf_rb;
                    g_rdma.read(staging, sequencer_switch_flag_offset_, sizeof(uint64_t));
                    std::memcpy(&sf_rb, staging, sizeof(uint64_t));
                    CHECK(sf_rb == new_switch)
                        << "[RDMA-CHECK] switch_flag readback mismatch: wrote=0x"
                        << std::hex << new_switch << " read=0x" << sf_rb << std::dec;
                }
                // Wait for all shard-server reader switch flags to stop holding old_switch.
                // Readers RDMA-WRITE their own flag when they claim a version; we RDMA-READ to poll.
                for (int i = 0; i < SHARD_SERVER_NUM * REQUEST_WORKER_NUM; i++)
                {
                    uint64_t reader_val;
                    do {
                        g_rdma.read(staging, reader_switch_flag_offset_[i], sizeof(uint64_t));
                        std::memcpy(&reader_val, staging, sizeof(uint64_t));
                    } while (reader_val == old_switch);
                }
#else
                SharedData::try_switch_flag(sequencer_switch_flag_, reader_switch_flag_, old_switch, new_switch,
                                            configuration_.shards.size() * REQUEST_WORKER_NUM);
#endif
                switch_flag_value = 1 - switch_flag_value;
                gsn_committer_.update_round_number(
#ifndef USE_RDMA
                    configuration_epoch_
#endif
                );
                LOG_CLASS("sequencer {}", 1, "switch hash table");
                gsn_dirty.store(false, std::memory_order_relaxed);
            }
            else if (is_hash_table_dirty && !is_gsn_dirty)
            {
                // hash table updated but no new GSN yet — don't switch
            }
            else if (!is_hash_table_dirty && is_gsn_dirty)
            {
                gsn_committer_.update_round_number(
#ifndef USE_RDMA
                    configuration_epoch_
#endif
                );
                gsn_dirty.store(false, std::memory_order_relaxed);
            }

            hash_table_dirty.store(false, std::memory_order_relaxed);
        }

        // Start the periodic work on the main thread.
        void start_timer()
        {
            interval_ = std::chrono::microseconds(ORDERING_INTERVAL_MICROSECONDS);
            running_ = true; // Only the main thread accesses this flag.
        }

        // Stop the periodic work.
        void stop_timer()
        {
            running_ = false; // Only the main thread accesses this flag.
        }

        // Periodic loop executed on the main thread.
        void run()
        {
            while (!thread_pool_->is_pool_ready())
            {
                std::this_thread::sleep_for(std::chrono::milliseconds(200));
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(200));

            auto next_time = std::chrono::steady_clock::now() + interval_;

            while (running_) // Main-thread-only state.
            {
                thread_pool_->execute_all();

                // Update the global cut.
                // The global cut commits appends, while index tails make their effects readable.
                // Publish them together so an immediate read after an append sees the corresponding tails.

                update_gsn();

                switch_hash_table();

                std::this_thread::sleep_for(interval_);
            }
        }
    };
};
