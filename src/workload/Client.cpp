#include "../common/flags.h"
#include <absl/flags/flag.h>
#include <absl/flags/parse.h>
#include <arpa/inet.h>
#include <cstdlib>
#include <cstring>
#include <glog/logging.h>
#include <netinet/tcp.h>
#include <string>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>
#include <vector>
#include "../benchmark/ycsb/Query.h"
#include "../common/Macro.h"
#include "../common/Protocol.h"
#include <absl/time/time.h>
#include <fstream>
#include "../benchmark/counter/Query.h"
#include "../common/Time.h"
#include "../common/random.h"
#include "Lock.h"
#include "Receive.h"
#include "Retwis.h"

int req_cnt = 0;
// int backoff_base = 50;             // 50us
const int max_backoff_time = 1000; // Maximum backoff: 1 ms.

namespace
{
struct ServerEndpoint
{
  std::string addr;
  int port;
};

ServerEndpoint get_server_endpoint(std::uint32_t shard_id)
{
#ifdef USE_RDMA
  switch (shard_id)
  {
  case 0:
    return {"127.0.0.1", 8081};
  case 1:
    return {"127.0.0.1", 8082};
  case 2:
    return {"127.0.0.1", 8083};
  case 3:
    return {"127.0.0.1", 8084};
  case 4:
    return {"127.0.0.1", 8085};
  case 5:
    return {"127.0.0.1", 8086};
  }
#else
  switch (shard_id)
  {
  case 0:
    return {"192.168.100.3", 8081};
  case 1:
    return {"192.168.100.4", 8081};
  case 2:
    return {"192.168.100.5", 8081};
  case 3:
    return {"192.168.100.6", 8081};
  case 4:
    return {"192.168.100.7", 8081};
  case 5:
    return {"192.168.100.8", 8081};
  }
#endif

  LOG(ERROR) << "invalid shard_id: " << shard_id;
  exit(-1);
}
} // namespace

void send_write_request(int sock, std::uint32_t request_id,
                        OperationId operation_id, std::uint32_t client_id,
                        char *buffer)
{
  ReqHeader *header = reinterpret_cast<ReqHeader *>(buffer);
  header->is_write = true;
  header->request_id = request_id;
  header->operation_id = operation_id;
  header->client_id = client_id;
  star::Random random;
  auto random_seed = star::Time::now();
  random.set_seed(random_seed);
  if (WORKLOAD == 0)
  {
    star::ycsb::make_update_query_for_append(random, operation_id, header,
                                             buffer + sizeof(ReqHeader));
  }
  else if (WORKLOAD == 1)
  {
    star::ycsb::make_update_query(random, operation_id, header,
                                  buffer + sizeof(ReqHeader));
  }
  else
  {
    LOG(ERROR) << "invalid workload: " << WORKLOAD;
    exit(-1);
  }
  start_time.push_back(absl::Now());
  LOG_CLASS("Client {}", client_id, "send write request, request id: {}",
            request_id);
  io_utils::SendData(sock, buffer, sizeof(ReqHeader) + header->payload_size);
}

void send_read_request(int sock, std::uint32_t request_id,
                       OperationId operation_id, std::uint32_t client_id,
                       char *buffer)
{
  ReqHeader *header = reinterpret_cast<ReqHeader *>(buffer);
  header->is_write = false;
  header->request_id = request_id;
  header->operation_id = operation_id;
  header->client_id = client_id;
  star::Random random;
  auto random_seed = star::Time::now();
  random.set_seed(random_seed);
  star::ycsb::make_search_query(random, operation_id, header,
                                buffer + sizeof(ReqHeader));
  start_time.push_back(absl::Now());
  req_cnt++;
  io_utils::SendData(sock, buffer, sizeof(ReqHeader) + header->payload_size);
}

void send_insert_request(int sock, std::uint32_t request_id,
                         OperationId operation_id, std::uint32_t client_id,
                         char *buffer, int cnt)
{
  ReqHeader *header = reinterpret_cast<ReqHeader *>(buffer);
  header->is_write = true;
  header->request_id = request_id;
  header->operation_id = operation_id;
  header->client_id = client_id;
  StateKey key = (client_id + 1) << 24 | cnt;
  star::ycsb::make_insert_query(operation_id, header,
                                buffer + sizeof(ReqHeader), key);
  start_time.push_back(absl::Now());
  req_cnt++;
  io_utils::SendData(sock, buffer, sizeof(ReqHeader) + header->payload_size);
}

void ycsb_A_test(int sock, std::uint32_t request_id, OperationId operation_id,
                 std::uint32_t client_id, char *buffer)
{
  // 50% read, 50% write
  star::Random random;
  auto random_seed = star::Time::now();
  random.set_seed(random_seed);
  std::uint32_t request_type = random.next_uint32() % 100;
  if (request_type < 50)
  {
    send_read_request(sock, request_id, operation_id, client_id, buffer);
    receive_response_read_only_test(sock);
  }
  else
  {
    send_write_request(sock, request_id, operation_id, client_id, buffer);
    receive_response_append_only_test(sock);
  }
}

void ycsb_B_test(int sock, std::uint32_t request_id, OperationId operation_id,
                 std::uint32_t client_id, char *buffer)
{
  // 95% read, 5% write
  star::Random random;
  auto random_seed = star::Time::now();
  random.set_seed(random_seed);
  std::uint32_t request_type = random.next_uint32() % 100;
  if (request_type < 95)
  {
    send_read_request(sock, request_id, operation_id, client_id, buffer);
    receive_response_read_only_test(sock);
  }
  else
  {
    send_write_request(sock, request_id, operation_id, client_id, buffer);
    receive_response_append_only_test(sock);
  }
}

void ycsb_C_test(int sock, std::uint32_t request_id, OperationId operation_id,
                 std::uint32_t client_id, char *buffer)
{
  // 100% read
  send_read_request(sock, request_id, operation_id, client_id, buffer);
  receive_response_read_only_test(sock);
}

void ycsb_D_test(int sock, std::uint32_t request_id, OperationId operation_id,
                 std::uint32_t client_id, char *buffer, int cnt)
{
  // 95% read, 5% insert
  star::Random random;
  auto random_seed = star::Time::now();
  random.set_seed(random_seed);
  std::uint32_t request_type = random.next_uint32() % 100;
  if (request_type < 95)
  {
    send_read_request(sock, request_id, operation_id, client_id, buffer);
    receive_response_read_only_test(sock);
  }
  else
  {
    send_insert_request(sock, request_id, operation_id, client_id, buffer, cnt);
    receive_response_append_only_test(sock);
  }
}

void send_counter_request(int sock, std::uint32_t request_id,
                          std::uint32_t client_id, std::uint32_t delta_add,
                          char *buffer)
{
  ReqHeader *header = reinterpret_cast<ReqHeader *>(buffer);
  header->is_write = true;
  header->request_id = request_id;
  header->operation_id = delta_add;
  header->client_id = client_id;
  star::Random random;
  auto random_seed =
      star::Time::now() ^ getpid() ^ (pthread_self() & 0xFFFFFFFF);
  random.set_seed(random_seed);
  star::counter::make_add_query(random, delta_add, header,
                                buffer + sizeof(ReqHeader));
  io_utils::SendData(sock, buffer, sizeof(ReqHeader) + header->payload_size);
}

//         // send_read_request(sock, operation_id, operation_id, client_id,
//         buffer);
//         // send_write_request(sock, operation_id, operation_id, client_id,
//         buffer);

void generate_request(int sock, std::uint32_t client_id)
{
  LOG(INFO) << "client " << client_id
            << " starts to generate request of workload " << WORKLOAD;
  start_time.reserve(50000);
  end_time.reserve(50000);
  char *buffer = new char[4224];
  if (WORKLOAD == 0)
  {
    for (int j = 0; j < 30000; j++)
    {
      std::uint32_t operation_id =
          (client_id << 16) | j + 1; // Upper 16 bits: client ID; lower 16 bits: operation ID.
      send_write_request(sock, operation_id, operation_id, client_id, buffer);
      receive_response_append_only_test(sock);
      LOG_CLASS("Client {}", client_id, "receive response, request id: {}",
                operation_id);
    }
  }
  else if (WORKLOAD == 1)
  {
    for (int j = 0; j < 30000; j++)
    {
      std::uint32_t operation_id =
          (client_id << 16) | j + 1; // Upper 16 bits: client ID; lower 16 bits: operation ID.
      if (YCSB_OPTION == 0)
      {
        ycsb_A_test(sock, operation_id, operation_id, client_id, buffer);
      }
      else if (YCSB_OPTION == 1)
      {
        ycsb_B_test(sock, operation_id, operation_id, client_id, buffer);
      }
      else if (YCSB_OPTION == 2)
      {
        ycsb_C_test(sock, operation_id, operation_id, client_id, buffer);
      }
      else if (YCSB_OPTION == 3)
      {
        ycsb_D_test(sock, operation_id, operation_id, client_id, buffer, j);
      }
      else
      {
        LOG(ERROR) << "Invalid ycsb option: " << YCSB_OPTION;
        exit(-1);
      }
    }
  }
  else if (WORKLOAD == 2)
  {
    std::uint32_t lock_cnt = 0;
    std::uint32_t suc_cnt = 0;
    std::uint32_t j = 0;
    // Seed each client from its ID to generate a separate pseudorandom sequence.
    srandom(client_id);
    while (suc_cnt < 20000)
    {
      lock_cnt++;
      j++;
      std::uint32_t operation_id =
          (client_id << 16) | j + 1; // Upper 16 bits: client ID; lower 16 bits: operation ID.
      std::uint32_t owner_id = (client_id << 16) | j + 1;
      absl::Time start = absl::Now();
      start_time.push_back(start);
      ReqHeader *header = send_lock_request(sock, operation_id, owner_id, client_id, buffer);
      int try_cnt = 0;
      // const int max_backoff_time = 1000; // Maximum backoff: 1 ms.
      while (true)
      {
        bool ret = receive_response_lock_test(sock, true);
        if (ret)
        {
          break;
        }
        else
        {
          // Randomized exponential backoff.
          try_cnt = std::min(try_cnt, 10);
          int exponential_backoff = std::min(BACKOFF_BASE * (1 << try_cnt), max_backoff_time);
          int backoff_time = (random() % exponential_backoff) + 1;
          std::this_thread::sleep_for(std::chrono::microseconds(backoff_time));
          try_cnt++;
          lock_cnt++;
          io_utils::SendData(sock, buffer, sizeof(ReqHeader) + header->payload_size);
        }
      }
      send_unlock_request(sock, operation_id, owner_id, client_id, buffer,
                          header->state_key);
      bool ret = receive_response_lock_test(sock, false);
      if (!ret)
      {
        LOG(ERROR) << "unlock state key: " << header->state_key
                   << " should not be failed, owner_id: " << owner_id;
        exit(-1);
      }
      suc_cnt++;
    }
    double success_rate = static_cast<double>(suc_cnt) / lock_cnt;
    LOG(INFO) << "client " << client_id << " success rate: " << success_rate;
  }
  else if (WORKLOAD == 3)
  {
    for (int j = 0; j < 20000; j++)
    {
      std::uint32_t request_id = (client_id << 16) | j + 1;
      std::uint32_t delta_add = 1;
      start_time.push_back(absl::Now());
      send_counter_request(sock, request_id, client_id, delta_add, buffer);
      receive_response_counter_test(sock);
      end_time.push_back(absl::Now());
    }
  }
  else if (WORKLOAD == 4)
  {
    generate_retwis_request(sock, client_id, buffer);
    // Derive the post ID prefix from the client ID.
  }
  else
  {
    LOG(ERROR) << "Invalid workload type: " << WORKLOAD;
    exit(-1);
  }
  delete[] buffer; // Release the allocated buffer.
}

int connect_to_server(const std::string &server_addr, int server_port)
{
  // Create socket
  int sock = socket(AF_INET, SOCK_STREAM, 0);
  if (sock < 0)
  {
    LOG(ERROR) << "Failed to create socket: " << strerror(errno);
    return -1;
  }

  // Set the server address.
  struct sockaddr_in server_address;
  memset(&server_address, 0, sizeof(server_address));
  server_address.sin_family = AF_INET;
  server_address.sin_port = htons(server_port);

  if (inet_pton(AF_INET, server_addr.c_str(), &server_address.sin_addr) <= 0)
  {
    close(sock);
    return -1;
  }

  // Connect to the server.
  if (connect(sock, (struct sockaddr *)&server_address,
              sizeof(server_address)) < 0)
  {
    close(sock);
    return -1;
  }

  int flag = 1;
  setsockopt(sock, IPPROTO_TCP, TCP_NODELAY, (char *)&flag, sizeof(int));

  return sock;
}

// Client worker entry point.
void client_thread(int client_id, const std::string &server_addr,
                   int server_port)
{
  int sock = connect_to_server(server_addr, server_port);
  if (sock == -1)
  {
    LOG(ERROR) << "Cannot connect toServer: " << server_addr << ":" << server_port;
    exit(1);
  }
  int flag = 1;
  setsockopt(sock, IPPROTO_TCP, TCP_NODELAY, (char *)&flag, sizeof(int));

  generate_request(sock, client_id);

  std::this_thread::sleep_for(std::chrono::seconds(3));

  // Write evaluation results to a CSV file.
  if (!end_time.empty() && end_time.size() > 20 &&
      start_time.size() >= end_time.size())
  {
    // Create the CSV filename.
    std::string filename = std::to_string(client_id) + ".csv";
    std::ofstream csv_file(filename);

    // Write the CSV header.
    csv_file << "start_time,end_time,latency_us\n";

    // Write measured requests, skipping the first 20 warmup requests.
    size_t min_size = std::min(start_time.size(), end_time.size());
    for (size_t i = 20; i < min_size; i++)
    {
      double latency = absl::ToDoubleMicroseconds(end_time[i] - start_time[i]);
      csv_file << absl::ToUnixMicros(start_time[i]) << ","
               << absl::ToUnixMicros(end_time[i]) << "," << latency << "\n";
    }

    csv_file.close();
    LOG(INFO) << "Latency data written to: " << filename;
  }
  else
  {
    LOG(ERROR) << "wrong configuration when client write result to csv file, "
                  "please check start_time and end_time size";
    exit(-1);
  }

  LOG(INFO) << "client " << client_id
            << " finish request number: " << end_time.size()
            << " (csv rows: " << (std::min(start_time.size(), end_time.size()) - 20) << ", first 20 warmup skipped)";
}

int main(int argc, char *argv[])
{
  // Initialize logging.
  google::InitGoogleLogging(argv[0]);

  // Set the logging level.
  FLAGS_minloglevel = 0; // INFO level
  // Send logs to stderr.
  FLAGS_logtostderr = true;

  // Read configuration arguments.
  std::string listen_addr = absl::GetFlag(FLAGS_listen_addr);
  std::string listen_port = absl::GetFlag(FLAGS_listen_port);
  int num_io_workers = absl::GetFlag(FLAGS_num_io_workers);
  int max_fds = absl::GetFlag(FLAGS_max_fds);

  auto logger = get_logger();

  // Parse command-line arguments
  std::uint32_t client_id = 0;
  std::uint32_t shard_id = 0;

  if (argc > 2)
  {
    try
    {
      client_id = static_cast<std::uint32_t>(std::stoul(argv[1]));
      shard_id = static_cast<std::uint32_t>(std::stoul(argv[2]));
    }
    catch (const std::exception &e)
    {
      LOG(ERROR) << "Invalid client_id argument: " << argv[1];
      LOG(ERROR) << "Invalid shard_id argument: " << argv[2];
      exit(-1);
    }
  }
  else
  {
    LOG(ERROR) << "Missing client_id or shard_id argument";
    exit(-1);
  }

  std::ifstream config_file(config_file_path);
  nlohmann::json config;
  config_file >> config;
  WORKLOAD = config["workload"].get<int>(); // Read from config.jsonworkload
  YCSB_OPTION = config["ycsb_option"].get<int>();
  SEGMENT_TEST = config.value("segment_test", false);
  LOG_ENABLE =
      config["log_enable"].get<bool>(); // Read from config.jsonlog_enable
  SCALE_OUT_STREAM = config["scale_out_stream"]
                         .get<bool>(); // Read from config.jsonscale_out_stream
  SCALE_OUT_RATE = config["scale_out_rate"]
                       .get<double>(); // Read from config.jsonscale_out_rate
  ZIPF_THETA =
      config["zipf_theta"].get<double>();           // Read from config.jsonzipf_theta
  BACKOFF_BASE = config["backoff_base"].get<int>(); // Read from config.jsonbackoff_base

  ServerEndpoint endpoint = get_server_endpoint(shard_id);
  std::string server_addr = endpoint.addr;
  int server_port = endpoint.port;

  // Thread count
  const int num_threads = 1;

  // Start the client threads.
  std::vector<std::thread> threads;
  threads.emplace_back(client_thread, client_id, server_addr, server_port);

  // Wait for all threads to finish.
  for (auto &t : threads)
  {
    t.join();
  }

  LOG(INFO) << "All client threads have finished";
  return 0;
}
