#pragma once

#include "../../common/Macro.h"
#include "../../common/LogEntry.h"
#include "../../common/Protocol.h"
#include "../../common/Zipf.h"
#include "../../common/random.h"
#include <absl/container/flat_hash_map.h>

namespace star
{
    namespace retwis
    {

        std::uint8_t val_cnt = 0;

#define FOLLOWERS_CNT 8
#define CONTENT_SIZE 256
#define MAX_RETURN_POST_CNT 4
#define USER_CNT 10000 // equals to timeline count, so user key ranges [0, USER_CNT-1], timeline key ranges [USER_CNT, 2*USER_CNT-1], keys larger than 2*USER_CNT belong to post

        enum class DataType
        {
            kUser,
            kPost,
            kTimeline
        };

        struct Post
        {
            std::uint32_t post_id;
            char *content; // CONTENT_SIZE bytes
        };

        struct User
        {
            std::uint32_t username;
            std::uint32_t password;
            std::uint32_t followers[FOLLOWERS_CNT];
        };

        struct Timeline
        {
            // append only
            std::uint32_t username;
            std::vector<std::uint64_t> posts; // Upper 32 bits: post ID; lower 32 bits: timestamp.
        };

        struct State
        {
            Post *post;
            User *user;
            Timeline *timeline;
        };

        std::uint64_t get_timestamp_ns()
        {
            auto now = std::chrono::high_resolution_clock::now();
            auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                          now.time_since_epoch())
                          .count();
            return static_cast<std::uint64_t>(ns);
        }

        std::uint32_t get_timestamp_sec()
        {
            auto now = std::chrono::system_clock::now();
            auto duration = std::chrono::duration_cast<std::chrono::seconds>(
                now.time_since_epoch());
            return static_cast<uint32_t>(duration.count());
        }

        int cal_min_payload_size(bool is_search, DataType data_type)
        {
            int size = 0;
            if (data_type == DataType::kUser)
            {
                if (is_search)
                {
                    size = sizeof(std::uint64_t) + sizeof(LogPayload) + sizeof(FieldOp<char[1]>);
                }
                else
                {
                    LOG(ERROR) << "update user is not supported by retwis";
                    exit(-1);
                }
            }
            else if (data_type == DataType::kPost)
            {
                if (is_search)
                {
                    size = sizeof(std::uint64_t) + sizeof(LogPayload) + sizeof(FieldOp<char[1]>);
                }
                else
                {
                    size = sizeof(std::uint64_t) + sizeof(LogPayload) + sizeof(FieldOp<char[CONTENT_SIZE]>);
                }
            }
            else if (data_type == DataType::kTimeline)
            {
                if (is_search)
                {
                    size = sizeof(std::uint64_t) + sizeof(LogPayload) + sizeof(FieldOp<char[1]>);
                }
                else
                {
                    size = sizeof(std::uint64_t) + sizeof(LogPayload) + sizeof(FieldOp<std::uint64_t>);
                }
            }
            else
            {
                LOG(ERROR) << "invalid data type";
                exit(-1);
            }
            // Round size up to the next multiple of 64 bytes.
            return ((size + 63) & (~63)) - sizeof(std::uint64_t);
        }

        // read user function
        void make_read_user_entry(std::uint32_t username, OperationId operation_id, char *buffer, bool is_password)
        {
            LogPayload *payload = reinterpret_cast<LogPayload *>(buffer);
            payload->operation_id_ = operation_id;
            payload->state_op_type_ = StateOpType::kRead;
            // 0: user, 1: post, 2: timeline
            payload->field_ops_cnt_ = 0;
            FieldOp<char[1]> field_op;
            if (is_password)
            {
                // 0: password
                field_op.field_id_ = 0;
            }
            else
            {
                // 1: followers
                field_op.field_id_ = 1;
            }
            field_op.op_type_ = FieldOpType::kRead;
            memcpy(buffer + sizeof(LogPayload), &field_op, sizeof(FieldOp<char[1]>));
        }

        void make_read_user_query(std::uint32_t username, OperationId operation_id, ReqHeader *header, char *buffer, bool is_password)
        {
            header->is_write = false;
            header->state_key = username;
            header->payload_size = cal_min_payload_size(true, DataType::kUser);
            if (is_password)
            {
                header->field_id = 0;
            }
            else
            {
                header->field_id = 1;
            }
            make_read_user_entry(username, operation_id, buffer, is_password);
        }

        // write post function
        void make_write_post_entry(std::uint32_t post_id, OperationId operation_id, char *buffer)
        {
            LogPayload *payload = reinterpret_cast<LogPayload *>(buffer);
            payload->operation_id_ = operation_id;
            payload->state_op_type_ = StateOpType::kInsert;
            // 0: user, 1: post, 2: timeline
            payload->field_ops_cnt_ = 1;
            FieldOp<char[CONTENT_SIZE]> field_op;
            field_op.op_type_ = FieldOpType::kSet;
            memset(field_op.value_, 'a', CONTENT_SIZE);
            memcpy(buffer + sizeof(LogPayload), &field_op, sizeof(FieldOp<char[CONTENT_SIZE]>));
        }

        void make_write_post_query(std::uint32_t post_id, OperationId operation_id, ReqHeader *header, char *buffer)
        {
            header->is_write = true;
            header->state_key = post_id + 2 * USER_CNT;
            header->payload_size = cal_min_payload_size(false, DataType::kPost);
            make_write_post_entry(post_id, operation_id, buffer);
        }

        // read post function
        void make_read_post_entry(std::uint32_t post_id, OperationId operation_id, char *buffer)
        {
            LogPayload *payload = reinterpret_cast<LogPayload *>(buffer);
            payload->operation_id_ = operation_id;
            payload->state_op_type_ = StateOpType::kRead;
            // 0: user, 1: post, 2: timeline
            payload->field_ops_cnt_ = 1;
            FieldOp<char[1]> field_op;
            field_op.op_type_ = FieldOpType::kRead;
            memcpy(buffer + sizeof(LogPayload), &field_op, sizeof(FieldOp<char[1]>));
        }

        void make_read_post_query(std::uint32_t post_id, OperationId operation_id, ReqHeader *header, char *buffer)
        {
            header->is_write = false;
            header->state_key = post_id + 2 * USER_CNT;
            header->payload_size = cal_min_payload_size(true, DataType::kPost);
            make_read_post_entry(post_id, operation_id, buffer);
        }

        // write timeline function
        void make_write_timeline_entry(std::uint32_t username, std::uint32_t post_id, std::uint32_t timestamp, OperationId operation_id, char *buffer)
        {
            LogPayload *payload = reinterpret_cast<LogPayload *>(buffer);
            payload->operation_id_ = operation_id;
            payload->state_op_type_ = StateOpType::kUpdate;
            // 0: user, 1: post, 2: timeline
            payload->field_ops_cnt_ = 2;
            FieldOp<std::uint64_t> field_op;
            field_op.value_ = (static_cast<std::uint64_t>(post_id) << 32) | timestamp;
            field_op.op_type_ = FieldOpType::kSet;
            memcpy(buffer + sizeof(LogPayload), &field_op, sizeof(FieldOp<std::uint64_t>));
        }

        void make_write_timeline_query(std::uint32_t username, std::uint32_t post_id, std::uint32_t timestamp, OperationId operation_id, ReqHeader *header, char *buffer)
        {
            header->is_write = true;
            header->state_key = username + USER_CNT;
            header->payload_size = cal_min_payload_size(false, DataType::kTimeline);
            make_write_timeline_entry(username, post_id, timestamp, operation_id, buffer);
        }

        // read timeline function
        void make_read_timeline_entry(std::uint32_t username, OperationId operation_id, char *buffer)
        {
            LogPayload *payload = reinterpret_cast<LogPayload *>(buffer);
            payload->operation_id_ = operation_id;
            payload->state_op_type_ = StateOpType::kRead;
            // 0: user, 1: post, 2: timeline
            payload->field_ops_cnt_ = 2;
            FieldOp<char[1]> field_op;
            // Timeline reads return post IDs.
            field_op.op_type_ = FieldOpType::kRead;
            memcpy(buffer + sizeof(LogPayload), &field_op, sizeof(FieldOp<char[1]>));
        }

        void make_read_timeline_query(std::uint32_t username, OperationId operation_id, ReqHeader *header, char *buffer)
        {
            header->is_write = false;
            header->state_key = username + USER_CNT;
            header->payload_size = cal_min_payload_size(true, DataType::kTimeline);
            make_read_timeline_entry(username, operation_id, buffer);
        }

    }
}
