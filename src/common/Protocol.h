#pragma once

#include <cstdint>
#include "Macro.h"
#include <vector>
#include <memory>

struct __attribute__((packed)) ReqHeader
{
    bool is_write;         // For the lock workload, true requests lock and false requests unlock.
    std::uint8_t field_id; // YCSB: 0-9; Retwis: 0-1 for user fields, 2 for a 256-byte post response.
    std::uint32_t request_id;
    StateKey state_key;
    OperationId operation_id; // Owner ID for locks; increment value for counters.
    std::uint32_t payload_size;
    std::uint32_t client_id;
    std::uint32_t lsn;
};

struct __attribute__((packed)) ResHeader
{
    bool is_write;
    std::uint32_t completed_req_cnt = 0;
};

struct WrapperHeader
{
    std::shared_ptr<ReqHeader> header;
    char *data;
};

struct __attribute__((packed)) SingleWriteResHeader // for append only and lock workload
{
    bool is_write; // For locks, true indicates success and false indicates failure.
    std::uint32_t request_id;
};

template <typename F>
struct ReadContent // for counter workload, F is std::uint32_t
{
    std::uint32_t request_id;
    F value;
};
