#pragma once

#include "../common/Macro.h"
#include "../common/Protocol.h"
#include "../common/io.h"
#include "../benchmark/retwis/Query.h"
#include <glog/logging.h>
#include <absl/time/time.h>
#include <cstring>
#include <cerrno>

std::vector<absl::Time> start_time;
std::vector<absl::Time> end_time;

// Thread function for receiving data
void receive_response(int sock)
{
    ResHeader header;
    std::uint32_t request_ids[10000];
    ReadContent<char[100]> read_content;
    while (true)
    {
        bool eof = false;
        bool ret = io_utils::RecvMessage(sock, &header, &eof);
        // Connection closed
        if (eof)
        {
            LOG(INFO) << "Client closed the connection";
            exit(-1);
        }

        // No more data to read or an error occurred
        if (!ret)
        {
            if (errno == EAGAIN || errno == EWOULDBLOCK)
            {
                // All data has been read; exit normally
                continue;
            }
            else
            {
                // Read error
                LOG(ERROR) << "Error reading request header: " << strerror(errno);
                exit(-1);
            }
        }

        if (header.is_write)
        {
            // Handle write response
            bool eof = false;
            io_utils::RecvData(sock, reinterpret_cast<char *>(request_ids), header.completed_req_cnt * sizeof(std::uint32_t), &eof);
            absl::Time now = absl::Now();
            for (int i = 0; i < header.completed_req_cnt; i++)
            {
                end_time.push_back(now);
            }
        }
        else
        {
            // Handle read response
            bool eof = false;
            io_utils::RecvMessage(sock, &read_content, &eof);
            if (eof)
            {
                LOG(ERROR) << "Error receiving read response data";
                break;
            }
            end_time.push_back(absl::Now());
        }
    }
}

void receive_response_close_loop(int sock)
{
    ResHeader header;
    std::uint32_t request_ids[10];
    ReadContent<char[100]> read_content;
    bool eof = false;
    bool ret = io_utils::RecvMessage(sock, &header, &eof);
    // Connection closed
    if (eof)
    {
        LOG(INFO) << "Client closed the connection";
        exit(-1);
    }

    // No more data to read or an error occurred
    if (!ret)
    {
        if (errno == EAGAIN || errno == EWOULDBLOCK)
        {
            // All data has been read; exit normally
            return;
        }
        else
        {
            // Read error
            LOG(ERROR) << "Error reading request header: " << strerror(errno);
            exit(-1);
        }
    }

    if (header.is_write)
    {
        // Handle write response
        bool eof = false;
        io_utils::RecvData(sock, reinterpret_cast<char *>(request_ids), header.completed_req_cnt * sizeof(std::uint32_t), &eof);
        absl::Time now = absl::Now();
        for (int i = 0; i < header.completed_req_cnt; i++)
        {
            end_time.push_back(now);
        }
    }
    else
    {
        // Handle read response
        bool eof = false;
        io_utils::RecvMessage(sock, &read_content, &eof);
        if (eof)
        {
            LOG(ERROR) << "Error receiving read response data";
            exit(-1);
            return;
        }
        end_time.push_back(absl::Now());
    }
}

void receive_response_append_only_test(int sock)
{
    SingleWriteResHeader response_header;
    bool eof = false;
    bool ret = io_utils::RecvMessage(sock, &response_header, &eof);
    absl::Time now = absl::Now();
    end_time.push_back(now);
    if (!ret)
    {
        if (errno == EAGAIN || errno == EWOULDBLOCK)
        {
            // All data has been read; exit normally
            return;
        }
        else
        {
            // Read error
            LOG(ERROR) << "Error reading request header: " << strerror(errno);
            exit(-1);
        }
    }
}

void receive_response_read_only_test(int sock)
{
    // ResHeader header;
    ReadContent<char[100]> read_content;

    bool eof = false;
    io_utils::RecvMessage(sock, &read_content, &eof);
    if (eof)
    {
        LOG(ERROR) << "Error receiving read response data";
        exit(-1);
        return;
    }
    end_time.push_back(absl::Now());
}

bool receive_response_lock_test(int sock, bool is_lock)
{
    SingleWriteResHeader response_header;
    bool eof = false;
    bool ret = io_utils::RecvMessage(sock, &response_header, &eof);
    absl::Time now = absl::Now();
    if (!is_lock)
    {
        // Only record the end time for unlock
        end_time.push_back(now);
    }
    if (response_header.is_write)
    {
        // is_write represents is_success
        return true;
    }
    else
    {
        return false;
    }
}

std::uint32_t receive_response_counter_test(int sock)
{
    ReadContent<std::uint32_t> read_content;
    bool eof = false;
    bool ret = io_utils::RecvMessage(sock, &read_content, &eof);
    return read_content.value;
}

void receive_response_retwis_login_test(int sock)
{
    ReadContent<std::uint32_t> read_content;
    bool eof = false;
    bool ret = io_utils::RecvMessage(sock, &read_content, &eof);
}

ReadContent<std::uint32_t[FOLLOWERS_CNT]> *receive_response_retwis_profile_test(int sock)
{
    ReadContent<std::uint32_t[FOLLOWERS_CNT]> *read_content = new ReadContent<std::uint32_t[FOLLOWERS_CNT]>();
    bool eof = false;
    bool ret = io_utils::RecvMessage(sock, read_content, &eof);
    return read_content;
}

ReadContent<std::uint64_t[MAX_RETURN_POST_CNT]> *receive_response_read_retwis_timeline_test(int sock)
{
    ReadContent<std::uint64_t[MAX_RETURN_POST_CNT]> *read_content = new ReadContent<std::uint64_t[MAX_RETURN_POST_CNT]>();
    bool eof = false;
    bool ret = io_utils::RecvMessage(sock, read_content, &eof);
    return read_content;
}

void receive_response_read_retwis_post_test(int sock)
{
    ReadContent<char[CONTENT_SIZE]> read_content;
    bool eof = false;
    bool ret = io_utils::RecvMessage(sock, &read_content, &eof);
}

void receive_response_retwis_append_only_test(int sock)
{
    SingleWriteResHeader response_header;
    bool eof = false;
    bool ret = io_utils::RecvMessage(sock, &response_header, &eof);
    // No more data to read or an error occurred
    if (!ret)
    {
        if (errno == EAGAIN || errno == EWOULDBLOCK)
        {
            // All data has been read; exit normally
            return;
        }
        else
        {
            // Read error
            LOG(ERROR) << "Error reading request header: " << strerror(errno);
            exit(-1);
        }
    }
}
