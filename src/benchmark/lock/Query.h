#pragma once

#include "../../common/Macro.h"
#include "../../common/LogEntry.h"
#include "../../common/Protocol.h"
#include "../../common/Zipf.h"
#include "../../common/random.h"

namespace star
{
    namespace lock
    {
        struct State
        {
            OperationId owner_id = 0;
            OperationId prev_owner_id = 0;
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
            int size = sizeof(std::uint64_t) + sizeof(LogPayload) + sizeof(FieldOp<OperationId>);
            // Round size up to the next multiple of 64 bytes.
            return ((size + 63) & (~63)) - sizeof(std::uint64_t);
        }

        void make_lock_entry(std::uint32_t &operation_id, char *buffer)
        {
            LogPayload *payload = reinterpret_cast<LogPayload *>(buffer);
            payload->operation_id_ = operation_id;
            payload->state_op_type_ = StateOpType::kUpdate;
            payload->field_ops_cnt_ = 1;

            FieldOp<OperationId> *field_op = reinterpret_cast<FieldOp<OperationId> *>(buffer + sizeof(LogPayload));
            field_op->field_id_ = 0;
            field_op->op_type_ = FieldOpType::kLock;
            field_op->value_ = operation_id;
        }

        void make_unlock_entry(std::uint32_t &operation_id, char *buffer)
        {
            LogPayload *payload = reinterpret_cast<LogPayload *>(buffer);
            payload->state_op_type_ = StateOpType::kUpdate;
            payload->field_ops_cnt_ = 1;
            payload->operation_id_ = operation_id;

            FieldOp<OperationId> *field_op = reinterpret_cast<FieldOp<OperationId> *>(buffer + sizeof(LogPayload));
            field_op->field_id_ = 0;
            field_op->op_type_ = FieldOpType::kUnlock;
            field_op->value_ = operation_id;
        }

        void make_lock_query(Random &random, std::uint32_t &operation_id, ReqHeader *header, char *buffer)
        {
            init_zipf(); // Ensure the Zipf distribution is initialized.
            StateKey key = Zipf::globalZipf().value(random.next_double());
            header->is_write = true; // means lock
            header->field_id = 0;
            header->state_key = key;
            header->payload_size = cal_min_payload_size();
            make_lock_entry(operation_id, buffer);
        }

        void make_unlock_query(Random &random, std::uint32_t &operation_id, ReqHeader *header, char *buffer)
        {
            init_zipf(); // Ensure the Zipf distribution is initialized.
            header->is_write = false; // means unlock
            header->field_id = 0;
            header->payload_size = cal_min_payload_size();
            make_unlock_entry(operation_id, buffer);
        }

    }
}
