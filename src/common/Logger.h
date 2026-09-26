#pragma once

#include <spdlog/spdlog.h>
#include <spdlog/async.h>                 // Asynchronous logging
#include <spdlog/sinks/basic_file_sink.h> // File sink
#include <cstdlib>
#include <memory>
#include <string>
#include <thread>

// Initialize the process-wide asynchronous logger.
inline std::shared_ptr<spdlog::logger> get_logger()
{
    static auto logger = []
    {
        // Initialize the asynchronous pool with 8192 queue entries and one writer.
        spdlog::init_thread_pool(32768, 1);

        // The asynchronous logger writes to a file, as expected by test-append.sh.
        // The default is logs/async.log; CXL roles run on separate machines.
        // RDMA roles can share a machine and working directory.
        // Use RHODES_LOG_FILE to give each process a separate log file and avoid
        // overwrites (RHODES_LOG_FILE=async_server0 selects logs/async_server0.log).
        // Keep the registered name async_logger so log lines retain [async_logger]
        // for compatibility with existing latency-analysis tools.
        const char *log_name = std::getenv("RHODES_LOG_FILE");
        std::string log_path =
            log_name ? (std::string("logs/") + log_name + ".log") : "logs/async.log";
        auto async_logger =
            spdlog::basic_logger_mt<spdlog::async_factory>("async_logger", log_path);

        async_logger->set_pattern("[%Y-%m-%d %H:%M:%S.%f] [%n] %v");

        // Disable automatic flushing.
        async_logger->flush_on(spdlog::level::off);
        spdlog::flush_every(std::chrono::seconds(999999)); // Effectively disable periodic flushing.

        return async_logger;
    }();
    return logger;
}

#define LOG_CLASS(class_fmt, class_args, msg_fmt, ...)                                                          \
    do                                                                                                          \
    {                                                                                                           \
        if (LOG_ENABLE)                                                                                         \
        {                                                                                                       \
            get_logger()->info("[{}] " msg_fmt, fmt::format(class_fmt, class_args) __VA_OPT__(, ) __VA_ARGS__); \
        }                                                                                                       \
    } while (0)

#define LOG_CLASS_FLUSH(class_fmt, class_args, msg_fmt, ...)                                            \
    do                                                                                                  \
    {                                                                                                   \
        auto __logger = get_logger();                                                                   \
        __logger->info("[{}] " msg_fmt, fmt::format(class_fmt, class_args) __VA_OPT__(, ) __VA_ARGS__); \
        __logger->flush();                                                                              \
    } while (0)
