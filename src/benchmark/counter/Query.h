#pragma once

#include "../../common/Macro.h"
#include "../../common/LogEntry.h"
#include "../../common/Protocol.h"
#include "../../common/Zipf.h"
#include "../../common/random.h"

namespace star
{
    namespace counter
    {
        struct State
        {
            std::uint32_t value = 0;
        };

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

        int cal_min_payload_size()
        {
            int size = sizeof(std::uint64_t) + sizeof(LogPayload) + sizeof(FieldOp<std::uint32_t>);
            // Round size up to the next multiple of 64 bytes.
            return ((size + 63) & (~63)) - sizeof(std::uint64_t);
        }

        void make_add_entry(std::uint32_t &add_delta, char *buffer)
        {
            LogPayload *payload = reinterpret_cast<LogPayload *>(buffer);
            payload->operation_id_ = add_delta;
            payload->state_op_type_ = StateOpType::kUpdate;
            payload->field_ops_cnt_ = 1;

            FieldOp<std::uint32_t> *field_op = reinterpret_cast<FieldOp<std::uint32_t> *>(buffer + sizeof(LogPayload));
            field_op->field_id_ = 0;
            field_op->op_type_ = FieldOpType::kAdd;
            field_op->value_ = add_delta;
        }

        void make_add_query(Random &random, std::uint32_t &add_delta, ReqHeader *header, char *buffer)
        {
            init_zipf(); // Ensure the Zipf distribution is initialized.
            StateKey key = Zipf::globalZipf().value(random.next_double());
            header->is_write = true; // means add(write)
            header->field_id = 0;
            header->state_key = key;
            header->payload_size = cal_min_payload_size();
            make_add_entry(add_delta, buffer);
        }
    }
}
