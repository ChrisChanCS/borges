#pragma once

#include "Query.h"
#include "../../shard_server/View.h"

namespace star
{
    namespace retwis
    {
        void process_entry(char *entry, View::View<State> *view)
        {
            LogPayload *payload = reinterpret_cast<LogPayload *>(entry + sizeof(std::uint64_t));
            if (WORKLOAD == 4)
            {
                if (view->is_operation_id_exist(payload->operation_id_))
                {
                    LOG(INFO) << "operation_id: " << payload->operation_id_ << " already exists, skip";
                    return;
                }
            }
            if (payload->field_ops_cnt_ == 0)
            {
                // User fields are read-only; sub_field_id_ selects password (0) or followers (1).
                // Read entries do not appear in the replay stream.
                LOG(ERROR) << "should not replay user entry";
                exit(-1);
            }
            else if (payload->field_ops_cnt_ == 1)
            {
                // A post operation creates a new post.
                FieldOp<char[CONTENT_SIZE]> *field_op = reinterpret_cast<FieldOp<char[CONTENT_SIZE]> *>(entry + sizeof(std::uint64_t) + sizeof(LogPayload));
                view->value_->post->content = field_op->value_;
                LOG(INFO) << "find a post write request, write content: " << view->value_->post->content;
            }
            else if (payload->field_ops_cnt_ == 2)
            {
                // A timeline operation appends a new post to the timeline.
                FieldOp<std::uint64_t> *field_op = reinterpret_cast<FieldOp<std::uint64_t> *>(entry + sizeof(std::uint64_t) + sizeof(LogPayload));
                // value_ packs the post ID and timestamp: Upper 32 bits: post ID; lower 32 bits: timestamp.
                view->value_->timeline->posts.emplace_back(field_op->value_);
            }
            else
            {
                LOG(ERROR) << "invalid field_ops_cnt_: " << payload->field_ops_cnt_;
                exit(-1);
            }
        }

        char *get_value(View::View<State> *view, std::uint8_t field_id)
        {
            if (view->state_key_ < USER_CNT)
            {
                // For a user object, state_key_ is the username.
                if (field_id == 0)
                {
                    return reinterpret_cast<char *>(&view->value_->user->password);
                }
                else if (field_id == 1)
                {
                    return reinterpret_cast<char *>(view->value_->user->followers);
                }
                else
                {
                    LOG(ERROR) << "invalid field_id: " << field_id << " for user";
                    exit(-1);
                }
            }
            else if (view->state_key_ < 2 * USER_CNT)
            {
                return reinterpret_cast<char *>(view->value_->timeline->posts.data());
            }
            else
            {
                return reinterpret_cast<char *>(view->value_->post->content);
            }
        }

    }
}
