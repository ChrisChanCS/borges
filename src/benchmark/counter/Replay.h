#pragma once

#include "Query.h"
#include "../../shard_server/View.h"

namespace star
{
    namespace counter
    {
        void process_entry(char *entry, View::View<State> *view)
        {
            LogPayload *payload = reinterpret_cast<LogPayload *>(entry + sizeof(std::uint64_t));
            if (payload->state_op_type_ == StateOpType::kUpdate)
            {
                FieldOp<std::uint32_t> *field_op = reinterpret_cast<FieldOp<std::uint32_t> *>(entry + sizeof(std::uint64_t) + sizeof(LogPayload));
                if (field_op->op_type_ == FieldOpType::kAdd)
                {
                    view->value_->value += field_op->value_;
                }
                else
                {
                    LOG(ERROR) << "invalid field op type for monotonic counter: " << static_cast<int>(field_op->op_type_);
                    exit(-1);
                }
            }
        }

        char *get_value(View::View<State> *view)
        {
            return reinterpret_cast<char *>(&view->value_->value);
        }
    }
}
