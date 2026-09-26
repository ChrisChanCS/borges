#pragma once

#include <absl/flags/flag.h>
#include <absl/flags/declare.h>

ABSL_DECLARE_FLAG(std::string, listen_addr);
ABSL_DECLARE_FLAG(std::string, listen_port);
ABSL_DECLARE_FLAG(int, num_io_workers);
ABSL_DECLARE_FLAG(int, message_conn_per_worker);
ABSL_DECLARE_FLAG(int, socket_listen_backlog);
ABSL_DECLARE_FLAG(int, max_fds);
