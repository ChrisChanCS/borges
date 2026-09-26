#include "flags.h"

ABSL_FLAG(std::string, listen_addr, "0.0.0.0",
          "Address to listen for external TCP connections");
ABSL_FLAG(std::string, listen_port, "8081", "Port to listen for external TCP connections");
ABSL_FLAG(int, num_io_workers, 4, "Number of IO workers.");
ABSL_FLAG(int, message_conn_per_worker, 8,
          "Number of connections for message passing per IO worker.");
ABSL_FLAG(int, socket_listen_backlog, 64, "Backlog for listen");
ABSL_FLAG(int, max_fds, 10000, "Max number of open file descriptors");
