#include "Sequencer.h"
#include "../common/Macro.h"
#include <glog/logging.h>
#include <charconv>

#ifndef USE_RDMA
namespace
{
    std::uint32_t parse_number(std::string_view text)
    {
        std::uint32_t value;
        const auto [end, error] = std::from_chars(text.data(), text.data() + text.size(), value);
        if (error != std::errc{} || end != text.data() + text.size())
            throw std::invalid_argument("invalid recovery argument");
        return value;
    }

    SharedData::RecoveryOptions parse_recovery_options(int argc, char **argv, bool &recover)
    {
        SharedData::RecoveryOptions options;
        for (int i = 1; i < argc; ++i)
        {
            const std::string_view argument(argv[i]);
            if (argument == "--recover")
                recover = true;
            else if (argument == "--survivors=primary")
                options.survivors = 1;
            else if (argument == "--survivors=backup")
                options.survivors = 2;
            else if (argument == "--rebuild=primary")
                options.rebuild |= 1;
            else if (argument == "--rebuild=backup")
                options.rebuild |= 2;
            else if (argument.starts_with("--trim=") || argument.starts_with("--delete-stream="))
            {
                if (options.trim || options.erase)
                    throw std::invalid_argument("one lifecycle operation is allowed per recovery");
                options.trim = argument.starts_with("--trim=");
                options.erase = !options.trim;
                auto fields = argument.substr(argument.find('=') + 1);
                const auto split = fields.find(':');
                if (split == std::string_view::npos && options.erase)
                {
                    options.shard = std::numeric_limits<std::uint32_t>::max();
                    options.key = parse_number(fields);
                    continue;
                }
                if (split == std::string_view::npos)
                    throw std::invalid_argument("expected shard:key[:lsn]");
                options.shard = parse_number(fields.substr(0, split));
                fields.remove_prefix(split + 1);
                const auto second = fields.find(':');
                options.key = parse_number(fields.substr(0, second));
                if (options.trim)
                {
                    if (second == std::string_view::npos)
                        throw std::invalid_argument("trim requires an LSN");
                    options.lsn = parse_number(fields.substr(second + 1));
                }
                else if (second != std::string_view::npos)
                    throw std::invalid_argument("delete-stream expects shard:key");
            }
            else
                throw std::invalid_argument("unknown sequencer argument");
        }
        if (!recover && (options.survivors != 3 || options.rebuild || options.trim || options.erase))
            throw std::invalid_argument("maintenance options require --recover");
        return options;
    }
}
#endif

int main(int argc, char *argv[])
{
    google::InitGoogleLogging(argv[0]);
    FLAGS_logtostderr = true;
    FLAGS_minloglevel = 0;
    LOG(INFO) << "start sequencer";

#ifdef USE_RDMA
    const char* mem_node_ip = "10.31.2.3";
    int rdma_port = 9999;
    if (argc > 1) mem_node_ip = argv[1];
    if (argc > 2) rdma_port = atoi(argv[2]);
    g_rdma.connect(mem_node_ip, rdma_port);
#else
    (void)argc;
    (void)argv;
    init_cxlalloc(10, 9, 16, SHARD_SERVER_NUM);
#endif

    auto logger = get_logger();
    LOG(INFO) << "logger initialized";

    std::ifstream config_file(config_file_path);
    nlohmann::json config;
    config_file >> config;
    CLIENT_NUM = config["client_num"].get<int>();   // Read from config.jsonclient_num
    WORKLOAD = config["workload"].get<int>();       // Read from config.jsonworkload
    YCSB_OPTION = config["ycsb_option"].get<int>(); // Read from config.jsonycsb_option
    SEGMENT_TEST = config.value("segment_test", false);
    // REPLICATOR_NUM = config["replicator_num"].get<int>(); // Read from config.jsonreplicator_num
    LOG_ENABLE = config["log_enable"].get<bool>();                // Read from config.jsonlog_enable
    SCALE_OUT_STREAM = config["scale_out_stream"].get<bool>();    // Read from config.jsonscale_out_stream
    SCALE_OUT_RATE = config["scale_out_rate"].get<double>();      // Read from config.jsonscale_out_rate
    REQUEST_WORKER_NUM = config["request_worker_num"].get<int>(); // Read from config.jsonrequest_worker_num
    SEGMENT_SIZE = config["segment_size"].get<int>();             // Read from config.jsonsegment_size
    SMALL_SEGMENT_SIZE = config["small_segment_size"].get<int>(); // Read from config.jsonsmall_segment_size

    bool recovering = false;
#ifndef USE_RDMA
    const auto recovery_options = parse_recovery_options(argc, argv, recovering);
#endif
    Sequencer::Sequencer sequencer(config.value("lease_timeout_ms", std::uint64_t{10000}),
                                  config.value("lease_renew_interval_ms", std::uint64_t{1000}),
                                  config.value("reconfig_port", std::uint16_t{8090}),
                                  recovering
#ifndef USE_RDMA
                                  , recovery_options
#endif
                                  );
#ifndef USE_RDMA
    Sequencer::ShardRecoveryAdmin::Settings recovery;
    recovery.enabled = config.value("auto_recover_shards", true);
    recovery.user = config.value("recovery_ssh_user", recovery.user);
    recovery.port = config.value("recovery_ssh_port", recovery.port);
    recovery.directory = config.value("recovery_working_directory", recovery.directory);
    recovery.executable = config.value("recovery_server_executable", recovery.executable);
    recovery.ssh_options = config.value("recovery_ssh_options", std::vector<std::string>{});
    sequencer.configure_maintenance(config.value("shard_addresses", std::vector<std::string>{}),
                                    config.value("maintenance_port", std::uint16_t{8091}), std::move(recovery));
#endif
    sequencer.run();
    return 0;
}
