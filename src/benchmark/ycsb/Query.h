#pragma once

#include "../../common/Macro.h"
#include "../../common/LogEntry.h"
#include "../../common/Protocol.h"
#include "../../common/Zipf.h"
#include "../../common/random.h"

namespace star
{
    namespace ycsb
    {

        std::uint8_t val_cnt = 0;

#define FIELD_SIZE 100 // 100
#define FIELD_OPS_CNT 1

        // Initialize the Zipf distribution.
        inline void init_zipf()
        {
            static bool initialized = false;
            if (!initialized)
            {
                Zipf::globalZipf().init(KEY_CNT_OF_SHARD, ZIPF_THETA); // Use the configured key count and skew.
                initialized = true;
            }
        }

        std::int32_t get_request_type(Random &random)
        {
            init_zipf(); // Ensure the Zipf distribution is initialized.
            return Zipf::globalZipf().value(random.next_double());
        }

        struct State
        {
            bool dirty[10]; // True when this field resides in CXL.
            char *f_ptr[10];
            char f0[FIELD_SIZE];
            char f1[FIELD_SIZE];
            char f2[FIELD_SIZE];
            char f3[FIELD_SIZE];
            char f4[FIELD_SIZE];
            char f5[FIELD_SIZE];
            char f6[FIELD_SIZE];
            char f7[FIELD_SIZE];
            char f8[FIELD_SIZE];
            char f9[FIELD_SIZE];
        };

        int cal_min_payload_size(bool is_search)
        {
            if (is_search)
            {
                int size = sizeof(std::uint64_t) + sizeof(LogPayload) + sizeof(FieldOp<char[1]>);
                // Round size up to the next multiple of 64 bytes.
                return ((size + 63) & (~63)) - sizeof(std::uint64_t);
            }
            else
            {
                int size = sizeof(std::uint64_t) + sizeof(LogPayload) + sizeof(FieldOp<char[FIELD_SIZE]>);
                return ((size + 63) & (~63)) - sizeof(std::uint64_t);
            }
        }

        int cal_min_payload_size_for_insert()
        {
            int size = sizeof(std::uint64_t) + sizeof(LogPayload) + sizeof(FieldOp<State>);
            return ((size + 63) & (~63)) - sizeof(std::uint64_t);
        }

        void make_search_entry(std::uint8_t field_id, std::uint32_t &payload_size, std::uint32_t &operation_id, char *buffer)
        {
            LogPayload *payload = reinterpret_cast<LogPayload *>(buffer);
            payload->operation_id_ = operation_id;
            payload->state_op_type_ = StateOpType::kRead;
            payload->field_ops_cnt_ = 1;

            FieldOp<char[1]> field_op;
            field_op.field_id_ = field_id;
            field_op.op_type_ = FieldOpType::kRead;
            // Search entries do not store a value.
            memset(field_op.value_, '\0', 1);
            memcpy(buffer + sizeof(LogPayload), &field_op, sizeof(FieldOp<char[1]>));
        }

        void make_update_entry(std::uint8_t field_id, std::uint32_t &payload_size, std::uint32_t &operation_id, char *buffer)
        {
            LogPayload *payload = reinterpret_cast<LogPayload *>(buffer);
            payload->state_op_type_ = StateOpType::kUpdate;
            payload->field_ops_cnt_ = 1;
            payload->operation_id_ = operation_id;

            FieldOp<char[FIELD_SIZE]> field_op;
            field_op.field_id_ = field_id;
            field_op.op_type_ = FieldOpType::kSet;
            memset(field_op.value_, 'a', FIELD_SIZE);
            std::uint32_t client_id = operation_id >> 16;
            memcpy(buffer + sizeof(LogPayload), &field_op, sizeof(FieldOp<char[FIELD_SIZE]>));
        }

        void make_insert_entry(std::uint32_t &payload_size, std::uint32_t &operation_id, char *buffer, State &insert_item)
        {
            LogPayload *payload = reinterpret_cast<LogPayload *>(buffer);
            payload->state_op_type_ = StateOpType::kInsert;
            payload->field_ops_cnt_ = 1;
            payload->operation_id_ = operation_id;

            FieldOp<State> *field_op = reinterpret_cast<FieldOp<State> *>(buffer + sizeof(LogPayload));
            field_op->field_id_ = 0;
            field_op->op_type_ = FieldOpType::kSet;
            memcpy(&(field_op->value_), &insert_item, sizeof(State));
        }

        void make_search_query(Random &random, std::uint32_t &operation_id, ReqHeader *header, char *buffer)
        {
            init_zipf(); // Ensure the Zipf distribution is initialized.
            StateKey key = Zipf::globalZipf().value(random.next_double());
            std::uint8_t field_id = random.uniform_dist(0, 10 - 1);
            header->field_id = field_id;
            header->state_key = key;
            header->payload_size = 0;
            return;
        }

        void make_update_query(Random &random, std::uint32_t &operation_id, ReqHeader *header, char *buffer)
        {
            init_zipf(); // Ensure the Zipf distribution is initialized.
            StateKey key = Zipf::globalZipf().value(random.next_double());
            std::uint8_t field_id = random.uniform_dist(0, 10 - 1);
            header->field_id = field_id;
            header->state_key = key;
            header->payload_size = cal_min_payload_size(false);
            make_update_entry(field_id, header->payload_size, operation_id, buffer);
        }

        void make_update_query_for_append(Random &random, std::uint32_t &operation_id, ReqHeader *header, char *buffer)
        {
            init_zipf(); // Ensure the Zipf distribution is initialized.
            std::uint8_t field_id = 0;
            StateKey key;
            if (!SCALE_OUT_STREAM)
            {
                key = 0;
            }
            else
            {
                // Decide whether to add a new key.
                std::uint32_t if_new_key = random.uniform_dist(0, 100);
                if (if_new_key < SCALE_OUT_RATE * 100)
                {
                    // Scale out by sending a new key.
                    key = operation_id;
                }
                else
                {
                    // Send an existing key without scaling out.
                    key = 0;
                }
            }
            header->field_id = field_id;
            header->state_key = key;
            header->payload_size = cal_min_payload_size(false);
            make_update_entry(field_id, header->payload_size, operation_id, buffer);
        }

        void make_insert_query(std::uint32_t &operation_id, ReqHeader *header, char *buffer, StateKey key)
        {
            State insert_item;
            memset(insert_item.f0, '0', FIELD_SIZE);
            memset(insert_item.f1, '1', FIELD_SIZE);
            memset(insert_item.f2, '2', FIELD_SIZE);
            memset(insert_item.f3, '3', FIELD_SIZE);
            memset(insert_item.f4, '4', FIELD_SIZE);
            memset(insert_item.f5, '5', FIELD_SIZE);
            memset(insert_item.f6, '6', FIELD_SIZE);
            memset(insert_item.f7, '7', FIELD_SIZE);
            memset(insert_item.f8, '8', FIELD_SIZE);
            memset(insert_item.f9, '9', FIELD_SIZE);
            for (int i = 0; i < 10; i++)
            {
                insert_item.dirty[i] = false;
            }
            for (int i = 0; i < 10; i++)
            {
                insert_item.f_ptr[i] = insert_item.f0 + i * FIELD_SIZE;
            }
            header->field_id = 0;
            header->state_key = key;
            header->payload_size = cal_min_payload_size_for_insert();
            make_insert_entry(header->payload_size, operation_id, buffer, insert_item);
        }
    }
}
