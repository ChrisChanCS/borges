#pragma once

#include "Query.h"
#include "../../shard_server/View.h"

namespace star
{
    namespace lock
    {
        void process_entry(char *entry, View::View<State> *view)
        {
            LogPayload *payload = reinterpret_cast<LogPayload *>(entry + sizeof(std::uint64_t));
            if (payload->state_op_type_ == StateOpType::kUpdate)
            {
                FieldOp<OperationId> *field_op = reinterpret_cast<FieldOp<OperationId> *>(entry + sizeof(std::uint64_t) + sizeof(LogPayload));
                if (field_op->op_type_ == FieldOpType::kLock)
                {
                    // Acquire an unowned lock; skip the entry if the lock is already held.
                    if (view->value_->owner_id == 0)
                    {
                        view->value_->owner_id = field_op->value_;
                    }
                }
                else if (field_op->op_type_ == FieldOpType::kUnlock)
                {
                    // Release the lock only when the entry's owner matches its current owner.
                    if (view->value_->owner_id == field_op->value_)
                    {
                        view->value_->prev_owner_id = view->value_->owner_id;
                        view->value_->owner_id = 0;
                    }
                }
                else
                {
                    LOG(ERROR) << "invalid field op type for linearizable lock: " << static_cast<int>(field_op->op_type_) << ", state_key: " << view->state_key_;
                    exit(-1);
                }
            }
        }

        bool get_result(View::View<State> *view, bool is_lock, OperationId &owner_id)
        {
            if (is_lock)
            {
                if (view->value_->owner_id == owner_id)
                {
                    return true;
                }
                else
                {
                    return false;
                }
            }
            else
            {
                // Unlock succeeds when the lock is free and the previous owner matches.
                if (view->value_->owner_id == 0 && view->value_->prev_owner_id == owner_id)
                {
                    return true;
                }
                else
                {
                    LOG(INFO) << "unlock state key: " << view->state_key_ << " failed, owner_id: " << view->value_->owner_id << ", prev_owner_id: " << view->value_->prev_owner_id << ", owner_id: " << owner_id;
                    return false;
                }
            }
        }
    }
}
