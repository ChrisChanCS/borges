#include "../shard_server/Server.h"

#include <glog/logging.h>
#include <absl/flags/flag.h>
#include <absl/flags/parse.h>
#include "../common/flags.h"
#include "../shard_server/Shard.h"
#include <spdlog/spdlog.h>
#include <spdlog/sinks/basic_file_sink.h>
#include <csignal>
ABSL_FLAG(bool, recover, false, "Restore a failed shard incarnation from its committed prefix");
#ifndef USE_RDMA
#include "../common/ReconfigControl.h"
#include "../common/RecoveryMaintenance.h"
#include "../common/OnlineBackup.h"
std::string reconfig_address = "192.168.100.2";
std::uint16_t reconfig_port = 8090;
std::uint16_t maintenance_port = 8091;
#endif

template <typename T, typename F>
Shard::Shard<T, F> *init_shard_server(std::uint32_t shard_id)
{
#ifndef USE_RDMA
    CHECK_LT(shard_id, SharedData::max_cluster_shards);
#endif
    init_shard_cxlalloc(MAX_THREAD_CNT_PER_PROCESS - 1, shard_id);
#ifndef USE_RDMA
    const bool recovering = absl::GetFlag(FLAGS_recover);
    if (recovering || shard_id >= SHARD_SERVER_NUM)
    {
        try
        {
            const auto reply = SharedData::request_join(reconfig_address, reconfig_port,
                {recovering ? 2u : 1u, shard_id, SharedData::cluster_settings()});
            LOG(INFO) << "join committed: shard=" << shard_id << " epoch=" << reply.epoch;
        }
        catch (const std::exception &error)
        {
            LOG(ERROR) << error.what();
            std::_Exit(64);
        }
    }
    auto *producer = static_cast<SharedData::RoundNumber *>(cxlalloc_get_root(ROUND_NUMBER_ROOT_INDEX));
    CHECK(producer != nullptr);
    SharedData::load_startup_configuration(SharedData::read_cxl_word(producer->round) >> 32);
    CHECK(shard_id < SharedData::startup_configuration.shards.size());
    SharedData::startup_lifecycles = SharedData::load_stream_lifecycles(SharedData::startup_configuration);
    CHECK(SharedData::startup_configuration.header.active_shards & (std::uint64_t{1} << shard_id));
    SharedData::node_leases.start_shard(
        static_cast<SharedData::LeaseRegion<SHARD_SERVER_NUM> *>(cxlalloc_get_root(LEASE_ROOT_INDEX)), shard_id,
        SharedData::shared_pointer<SharedData::LeaseSlot>(SharedData::shard_resources(shard_id).lease));
    if (recovering)
    {
        auto cut = SharedData::recover_published_prefix(false);
        CHECK_EQ(cut.progress.at(shard_id), SharedData::shard_resources(shard_id).resume_cut);
        CHECK_EQ(cut.configuration.shards.at(shard_id).lease, SharedData::shard_resources(shard_id).lease);
        SharedData::restore_shard_replicas(cut, shard_id);
        LOG(INFO) << "shard restored: shard=" << shard_id
                  << " lsn=" << (SharedData::recovered_shard->progress >> 32)
                  << " delta_cursor=" << static_cast<std::uint32_t>(SharedData::recovered_shard->progress);
    }
#endif
    LOG(INFO) << "Start init test workload";

    LOG(INFO) << "initialize pending area";

    // initialize batch queue and batch manager
    Responser<T, F> *responser = new Responser<T, F>(shard_id, WorkloadType::ycsb);

    LOG(INFO) << "initialize responser";

    SPMCBuffer *batch_buffer = new SPMCBuffer();
    absl::CondVar *cv = new absl::CondVar();
    absl::Mutex *cv_mu = new absl::Mutex();
    std::shared_ptr<BatchManager<T, F>> batch_manager = std::make_shared<BatchManager<T, F>>(batch_buffer, cv, cv_mu, responser);

    LOG(INFO) << "initialize batch manager";

    bool *task_ready = new bool(false);
    batch_manager->init_task_ready(task_ready, REPLICATOR_NUM);
    Committer *committer = new Committer(batch_buffer, cv, cv_mu, task_ready, shard_id);

    LOG(INFO) << "initialize committer";

    Shard::Shard<T, F> *shard = new Shard::Shard<T, F>(shard_id, batch_manager, committer);

    LOG(INFO) << "initialize shard";

    // initialize replicator array
    for (std::uint32_t i = 0; i < REPLICATOR_NUM; i++)
    {
        bool *task_ready = new bool(false);
        batch_manager->init_task_ready(task_ready, i);
        Replicator *replicator = new Replicator(shard_id, i, batch_buffer, cv, cv_mu, task_ready);
        shard->init_replicator(replicator, i);
        batch_manager->init_replicator(replicator, i);
    }

    io_utils::ServerBase<T, F> *server = new io_utils::ServerBase<T, F>(
        "0.0.0.0",
        std::stoi(absl::GetFlag(FLAGS_listen_port)),
        shard,
        responser);

#ifndef USE_RDMA
    auto *maintenance = new SharedData::MaintenanceServer();
    maintenance->start(maintenance_port, [=, backup = std::shared_ptr<SharedData::OnlineBackup>{}]
                       (SharedData::JoinRequest request) mutable -> SharedData::JoinReply {
        static thread_local bool allocator_ready = false;
        if (!allocator_ready)
        {
            init_shard_cxlalloc(MAX_THREAD_CNT_PER_PROCESS - 2, shard_id);
            allocator_ready = true;
        }
        switch (request.format)
        {
        case SharedData::pause_ingress:
            server->pause_ingress();
            return {0, shard->admitted_lsn()};
        case SharedData::arm_readers:
            responser->maintenance_readers.arm(request.settings);
            break;
        case SharedData::wait_readers:
            responser->maintenance_readers.wait(REQUEST_WORKER_NUM);
            break;
        case SharedData::stop_writers:
            shard->stop_writers();
            responser->prepare_lifecycle();
            break;
        case SharedData::drop_backup:
            shard->drop_backup();
            break;
        case SharedData::copy_backup:
            // The sequencer serializes rebuilds. A previous interrupted attempt
            // may have left a private, unpublished copy on a surviving shard.
            backup.reset();
            backup = std::make_shared<SharedData::OnlineBackup>(SharedData::shard_resources(shard_id),
                *SharedData::recovered_shard, shard_id);
            break;
        case SharedData::backup_ready:
            if (!backup)
                throw std::runtime_error("no backup copy is running");
            return {0, backup->ready() ? 1u : 0u};
        case SharedData::catchup_backup:
            if (!backup || !backup->ready())
                throw std::runtime_error("backup copy is not ready");
            backup->finish(SharedData::recover_shard_from_index(SharedData::recover_published_prefix(false, 1), shard_id));
            backup.reset();
            break;
        case SharedData::cancel_backup:
            backup.reset();
            break;
        case SharedData::reload_shard:
        {
            SharedData::load_startup_configuration(request.settings);
            SharedData::startup_lifecycles = SharedData::load_stream_lifecycles(SharedData::startup_configuration);
            auto cut = SharedData::recover_published_prefix(false);
            SharedData::restore_shard_replicas(cut, shard_id);
            if (request.shard != std::numeric_limits<std::uint32_t>::max())
                responser->invalidate_stream(request.shard);
            shard->restart_writers();
            break;
        }
        case SharedData::resume_shard:
            responser->maintenance_readers.resume();
            server->resume_ingress();
            break;
        default:
            throw std::runtime_error("unknown shard maintenance command");
        }
        return {0, request.settings};
    });
#endif

    return shard;
}

int main(int argc, char *argv[])
{
    std::signal(SIGPIPE, SIG_IGN);
    // Initialize logging.
    google::InitGoogleLogging(argv[0]);

    // Set the logging level.
    FLAGS_minloglevel = 0; // INFO level
    // Send logs to stderr.
    FLAGS_logtostderr = true;

    // Parse Abseil command-line flags (for example, --listen_port=8082).
    absl::ParseCommandLine(argc, argv);

    // Parse command-line arguments
    std::uint32_t shard_id = 0;

    if (argc > 1)
    {
        try
        {
            shard_id = static_cast<std::uint32_t>(std::stoul(argv[1]));
            LOG(INFO) << "Using shard_id: " << shard_id;
        }
        catch (const std::exception &e)
        {
            LOG(ERROR) << "Invalid shard_id argument: " << argv[1];
            exit(-1);
        }
    }
    else
    {
        LOG(ERROR) << "Missing shard_id argument";
        exit(-1);
    }

    LOG(INFO) << "Startup arguments: shard_id=" << shard_id;

#ifdef USE_RDMA
    {
        const char* mem_node_ip = "10.31.2.3";
        int rdma_port = 9999;
        if (argc > 2) mem_node_ip = argv[2];
        if (argc > 3) rdma_port = atoi(argv[3]);
        g_rdma.connect(mem_node_ip, rdma_port);
        LOG(INFO) << "RDMA connected to " << mem_node_ip << ":" << rdma_port;
    }
#endif

    auto logger = get_logger();
    LOG(INFO) << "logger initialized";

    std::ifstream config_file(config_file_path);
    nlohmann::json config;
    config_file >> config;
#ifndef USE_RDMA
    reconfig_address = config.value("sequencer_address", std::string("192.168.100.2"));
    reconfig_port = config.value("reconfig_port", std::uint16_t{8090});
    maintenance_port = config.value("maintenance_port", std::uint16_t{8091});
#endif
    CLIENT_NUM = config["client_num"].get<int>();                 // Read from config.jsonclient_num
    WORKLOAD = config["workload"].get<int>();                     // Read from config.jsonworkload
    IO_WORKER_NUM = config["io_worker_num"].get<int>();           // Read from config.jsonio_worker_num
    YCSB_OPTION = config["ycsb_option"].get<int>();               // Read from config.jsonycsb_option
    SEGMENT_TEST = config.value("segment_test", false);
    LOG_ENABLE = config["log_enable"].get<bool>();                // Read from config.jsonlog_enable
    SCALE_OUT_STREAM = config["scale_out_stream"].get<bool>();    // Read from config.jsonscale_out_stream
    SCALE_OUT_RATE = config["scale_out_rate"].get<double>();      // Read from config.jsonscale_out_rate
    REQUEST_WORKER_NUM = config["request_worker_num"].get<int>(); // Read from config.jsonrequest_worker_num
    SEGMENT_SIZE = config["segment_size"].get<int>();             // Read from config.jsonsegment_size
    LARGE_SEGMENT_SIZE = config["segment_size"].get<int>();       // Read from config.jsonlarge_segment_size
    SMALL_SEGMENT_SIZE = config["small_segment_size"].get<int>(); // Read from config.jsonsmall_segment_size
    // INIT_SEGMENT_CNT = config["init_segment_cnt"].get<int>();  // Read from config.jsoninit_segment_cnt
    // REPLICATOR_NUM = config["replicator_num"].get<int>(); // Read from config.jsonreplicator_num
    // MAX_ENTRIES_PER_BATCH = config["max_entries_per_batch"].get<int>(); // Read from config.jsonmax_entries_per_batch
    if (WORKLOAD == 0 || WORKLOAD == 1 || WORKLOAD == 3)
    {
        MAX_ENTRIES_PER_BATCH = 1;
    }
    else if (WORKLOAD == 4 || WORKLOAD == 2)
    {
        MAX_ENTRIES_PER_BATCH = 1;
    }
    else
    {
        LOG(ERROR) << "invalid workload: " << WORKLOAD;
        exit(-1);
    }

    // Create the shard instance.
    if (WORKLOAD == 0 || WORKLOAD == 1)
    {
        Shard::Shard<star::ycsb::State, char[100]> *shard = init_shard_server<star::ycsb::State, char[100]>(shard_id);
    }
    else if (WORKLOAD == 2)
    {
        Shard::Shard<star::lock::State, OperationId> *shard = init_shard_server<star::lock::State, OperationId>(shard_id);
    }
    else if (WORKLOAD == 3)
    {
        Shard::Shard<star::counter::State, std::uint32_t> *shard = init_shard_server<star::counter::State, std::uint32_t>(shard_id);
    }
    else if (WORKLOAD == 4)
    {
        Shard::Shard<star::retwis::State, char[100]> *shard = init_shard_server<star::retwis::State, char[100]>(shard_id);
    }

    LOG(INFO) << "Server initialized; accepting connections";

    // Keep the main thread alive while workers serve requests.
    while (true)
    {
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    return 0;
}
