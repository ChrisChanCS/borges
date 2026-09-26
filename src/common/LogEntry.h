#pragma once

#include <vector>
#include <queue>
#include <cstdint>
#include <functional>
#include <memory>
#include <cstring>
#include <type_traits>
#include "../common/Macro.h"
#include "Cacheline.h"
#include <glog/logging.h>

#define MAX_FIELD_OPS_PER_LOG_ENTRY 16

enum class FieldOpType
{
    kRead,
    kSet,
    kAdd,
    kSub,
    kLock,
    kUnlock,
};

enum class StateOpType
{
    kRead,
    kUpdate,
    kInsert,
    kDelete
};

// All fields in an operation use the same value type F.
// This packed structure is part of the serialized log format.
template <typename F>
struct __attribute__((packed)) FieldOp
{
    std::uint8_t field_id_; // Retwis user field: 0 for password, 1 for followers.
    FieldOpType op_type_; // Operation applied to this field.
    F value_;             // Workload value: lock owner ID, counter delta, or YCSB byte array.
};

// template <typename F>
struct __attribute__((packed)) LogPayload
{
    StateOpType state_op_type_;
    std::uint8_t field_ops_cnt_; // Retwis object type: 0 for user, 1 for post, 2 for timeline.
    std::uint32_t operation_id_;
    // followed by FieldOp<F> field_ops_[field_ops_cnt_]
};

// Control records occupy the read log but must never create an application
// reader. The tag cannot be a serialized StateOpType.
struct alignas(CACHELINE_SIZE) DeletionRecord
{
    static constexpr std::uint64_t magic = 0x314554454c4544ffULL;
    std::uint64_t header;
    std::uint64_t tag = magic;
    std::uint32_t key;
    std::uint32_t reserved[11]{};

    DeletionRecord(std::uint32_t lsn, std::uint32_t stream_key)
        : header((std::uint64_t(lsn) << 32) | (CACHELINE_SIZE - sizeof(header))), key(stream_key) {}

    static bool matches(const void *record)
    {
        std::uint64_t tag;
        std::memcpy(&tag, static_cast<const char *>(record) + sizeof(header), sizeof(tag));
        return tag == magic;
    }

    // Earlier format-4 writers stored an untagged deletion. Only recognize it
    // using the exact committed lifecycle entry, never by zero payload bytes.
    static bool matches_legacy(const void *record, std::uint32_t lsn, std::uint32_t key)
    {
        std::uint64_t expected[CACHELINE_SIZE / sizeof(std::uint64_t)]{};
        expected[0] = (std::uint64_t(lsn) << 32) | (CACHELINE_SIZE - sizeof(std::uint64_t));
        expected[1] = std::uint64_t(key) << 32;
        return std::memcmp(record, expected, sizeof(expected)) == 0;
    }
};
static_assert(sizeof(DeletionRecord) == CACHELINE_SIZE);

// A regular entry contains LSN, payload size, and payload.
// At segment boundaries, a link pointer identifies the next entry.
struct __attribute__((packed)) LinkPointer
{
    std::uint64_t tag_; // Link-record magic number
    std::int64_t next_entry_offset_;
    std::uint32_t next_lsn_ = 0; // Recovery ignores links beyond the selected cut.
    char padding[64 - sizeof(std::uint64_t) - sizeof(std::int64_t) - sizeof(std::uint32_t)];
};
