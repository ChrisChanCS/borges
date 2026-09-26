#pragma once

#include "../benchmark/retwis/Query.h"
#include "Receive.h"

std::uint32_t post_cnt = 4;

void send_retwis_login_request(
    int sock,
    const std::uint32_t &request_id,
    const OperationId &operation_id,
    const std::uint32_t &client_id,
    const std::uint32_t &username,
    char *buffer)
{
    ReqHeader *header = reinterpret_cast<ReqHeader *>(buffer);
    header->is_write = false;
    header->request_id = request_id;
    header->operation_id = operation_id;
    header->client_id = client_id;
    star::retwis::make_read_user_query(username, operation_id, header, buffer + sizeof(ReqHeader), true);
    io_utils::SendData(sock, buffer, sizeof(ReqHeader) + header->payload_size);
    receive_response_retwis_login_test(sock);
}

ReadContent<std::uint32_t[FOLLOWERS_CNT]> *send_retwis_profile_request(
    int sock,
    const std::uint32_t &request_id,
    const OperationId &operation_id,
    const std::uint32_t &client_id,
    const std::uint32_t &username,
    char *buffer)
{
    ReqHeader *header = reinterpret_cast<ReqHeader *>(buffer);
    header->is_write = false;
    header->request_id = request_id;
    header->operation_id = operation_id;
    header->client_id = client_id;
    star::retwis::make_read_user_query(username, operation_id, header, buffer + sizeof(ReqHeader), false);
    io_utils::SendData(sock, buffer, sizeof(ReqHeader) + header->payload_size);
    return receive_response_retwis_profile_test(sock);
}

ReadContent<std::uint64_t[MAX_RETURN_POST_CNT]> *send_retwis_read_timeline_request(
    int sock,
    const std::uint32_t &request_id,
    const OperationId &operation_id,
    const std::uint32_t &client_id,
    const std::uint32_t &username,
    char *buffer)
{
    ReqHeader *header = reinterpret_cast<ReqHeader *>(buffer);
    header->is_write = false;
    header->request_id = request_id;
    header->operation_id = operation_id;
    header->client_id = client_id;
    star::retwis::make_read_timeline_query(username, operation_id, header, buffer + sizeof(ReqHeader));
    io_utils::SendData(sock, buffer, sizeof(ReqHeader) + header->payload_size);
    return receive_response_read_retwis_timeline_test(sock);
}

void send_retwis_write_timeline_request(
    int sock,
    const std::uint32_t &request_id,
    const OperationId &operation_id,
    const std::uint32_t &client_id,
    const std::uint32_t &username,
    const std::uint32_t &post_id,
    const std::uint32_t &timestamp,
    char *buffer)
{
    ReqHeader *header = reinterpret_cast<ReqHeader *>(buffer);
    header->is_write = true;
    header->request_id = request_id;
    header->operation_id = operation_id;
    header->client_id = client_id;
    star::retwis::make_write_timeline_query(username, post_id, timestamp, operation_id, header, buffer + sizeof(ReqHeader));
    io_utils::SendData(sock, buffer, sizeof(ReqHeader) + header->payload_size);
    receive_response_retwis_append_only_test(sock);
}

void send_retwis_read_post_request(
    int sock,
    const std::uint32_t &request_id,
    const OperationId &operation_id,
    const std::uint32_t &client_id,
    const std::uint32_t &post_id,
    char *buffer)
{
    ReqHeader *header = reinterpret_cast<ReqHeader *>(buffer);
    header->is_write = false;
    header->request_id = request_id;
    header->operation_id = operation_id;
    header->client_id = client_id;
    star::retwis::make_read_post_query(post_id, operation_id, header, buffer + sizeof(ReqHeader));
    io_utils::SendData(sock, buffer, sizeof(ReqHeader) + header->payload_size);
    receive_response_read_retwis_post_test(sock);
}

void send_retwis_write_post_request(
    int sock,
    const std::uint32_t &request_id,
    const OperationId &operation_id,
    const std::uint32_t &client_id,
    const std::uint32_t &post_id,
    char *buffer)
{
    ReqHeader *header = reinterpret_cast<ReqHeader *>(buffer);
    header->is_write = true;
    header->request_id = request_id;
    header->operation_id = operation_id;
    header->client_id = client_id;
    star::retwis::make_write_post_query(post_id, operation_id, header, buffer + sizeof(ReqHeader));
    io_utils::SendData(sock, buffer, sizeof(ReqHeader) + header->payload_size);
    receive_response_retwis_append_only_test(sock);
}

void Login(
    int sock,
    const std::uint32_t &request_id,
    const OperationId &operation_id,
    const std::uint32_t &client_id,
    const std::uint32_t &username,
    char *buffer)
{
    send_retwis_login_request(sock, request_id, operation_id, client_id, username, buffer);
}

void Profile(
    int sock,
    const std::uint32_t &request_id,
    const OperationId &operation_id,
    const std::uint32_t &client_id,
    const std::uint32_t &username,
    char *buffer)
{
    send_retwis_profile_request(sock, request_id, operation_id, client_id, username, buffer);
}

void Timeline(
    int sock,
    std::uint32_t &request_id,
    OperationId &operation_id,
    const std::uint32_t &client_id,
    const std::uint32_t &username,
    char *buffer)
{
    ReadContent<std::uint64_t[MAX_RETURN_POST_CNT]> *read_content = send_retwis_read_timeline_request(sock, request_id, operation_id, client_id, username, buffer);
    std::uint32_t post_id;
    for (std::uint32_t i = 0; i < MAX_RETURN_POST_CNT; i++)
    {
        post_id = (read_content->value[i] >> 32); // Query construction adds 2 * USER_CNT to the post ID.
        request_id++;
        operation_id++;
        send_retwis_read_post_request(sock, request_id, operation_id, client_id, post_id, buffer);
    }
    delete read_content;
}

void Post(
    int sock,
    std::uint32_t &request_id,
    OperationId &operation_id,
    const std::uint32_t &client_id,
    const std::uint32_t &username,
    char *buffer)
{
    std::uint32_t post_id = ((client_id + 1) << 24) + post_cnt; // Query construction adds 2 * USER_CNT to the post ID.
    post_cnt++;
    std::uint32_t timestamp = star::retwis::get_timestamp_sec();
    send_retwis_write_post_request(sock, request_id, operation_id, client_id, post_id, buffer);
    request_id++;
    operation_id++;
    ReadContent<std::uint32_t[FOLLOWERS_CNT]> *read_content = send_retwis_profile_request(sock, request_id, operation_id, client_id, username, buffer);
    for (std::uint32_t i = 0; i < FOLLOWERS_CNT; i++)
    {
        request_id++;
        operation_id++;
        std::uint32_t follower = read_content->value[i];
        send_retwis_write_timeline_request(sock, request_id, operation_id, client_id, follower, post_id, timestamp, buffer);
    }
    delete read_content;
}

void generate_retwis_request(int sock, std::uint32_t client_id, char *buffer)
{
    std::uint32_t request_id = ((client_id + 1) << 24) + 1;
    std::uint32_t operation_id = request_id;
    star::Random random;
    auto random_seed = star::Time::now() ^ getpid() ^ (pthread_self() & 0xFFFFFFFF);
    random.set_seed(random_seed);
    for (std::uint32_t i = 0; i < 10000; i++)
    {
        start_time.push_back(absl::Now());
        std::uint32_t request_type = random.next_uint32() % 100;
        if (request_type < 5)
        {
            // login
            request_id++;
            operation_id++;
            std::uint32_t username = random.next_uint32() % 10000;
            Login(sock, request_id, operation_id, client_id, username, buffer);
        }
        else if (request_type < 10)
        {
            // profile
            request_id++;
            operation_id++;
            std::uint32_t username = random.next_uint32() % 10000;
            Profile(sock, request_id, operation_id, client_id, username, buffer);
        }
        else if (request_type < 90)
        {
            // timeline
            request_id++;
            operation_id++;
            std::uint32_t username = random.next_uint32() % 10000;
            Timeline(sock, request_id, operation_id, client_id, username, buffer);
        }
        else
        {
            // post
            request_id++;
            operation_id++;
            std::uint32_t username = random.next_uint32() % 10000;
            Post(sock, request_id, operation_id, client_id, username, buffer);
        }
        end_time.push_back(absl::Now());
    }
}
