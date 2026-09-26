#pragma once

#include "../common/Macro.h"
#include "../common/Protocol.h"
#include "../benchmark/lock/Query.h"
#include "../common/Time.h"
#include "../common/random.h"
#include "Receive.h"

ReqHeader *send_lock_request(int sock, std::uint32_t request_id, OperationId operation_id, std::uint32_t client_id, char *buffer)
{
    ReqHeader *header = reinterpret_cast<ReqHeader *>(buffer);
    header->is_write = true;
    header->request_id = request_id;
    header->operation_id = operation_id;
    header->client_id = client_id;
    star::Random random;
    auto random_seed = star::Time::now() ^ getpid() ^ (pthread_self() & 0xFFFFFFFF);
    random.set_seed(random_seed);
    star::lock::make_lock_query(random, operation_id, header, buffer + sizeof(ReqHeader));
    io_utils::SendData(sock, buffer, sizeof(ReqHeader) + header->payload_size);
    return header;
}

void send_unlock_request(int sock, std::uint32_t request_id, OperationId operation_id, std::uint32_t client_id, char *buffer, StateKey state_key)
{
    ReqHeader *header = reinterpret_cast<ReqHeader *>(buffer);
    header->is_write = false;
    header->request_id = request_id;
    header->operation_id = operation_id;
    header->client_id = client_id;
    star::Random random;
    auto random_seed = star::Time::now();
    random.set_seed(random_seed);
    star::lock::make_unlock_query(random, operation_id, header, buffer + sizeof(ReqHeader));
    header->state_key = state_key;
    io_utils::SendData(sock, buffer, sizeof(ReqHeader) + header->payload_size);
}
