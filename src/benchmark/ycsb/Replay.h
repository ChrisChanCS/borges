#pragma once

#include "Query.h"
#include "../../shard_server/View.h"

namespace star
{
    namespace ycsb
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
            if (payload->state_op_type_ == StateOpType::kUpdate)
            {
                // ycsb only manipulate 1 field per time
                FieldOp<char[FIELD_SIZE]> *field_op = reinterpret_cast<FieldOp<char[FIELD_SIZE]> *>(entry + sizeof(std::uint64_t) + sizeof(LogPayload));
                switch (field_op->field_id_)
                {
                case 0:
                    view->value_->dirty[0] = true;
                    view->value_->f_ptr[0] = field_op->value_;
                    break;
                case 1:
                    view->value_->dirty[1] = true;
                    view->value_->f_ptr[1] = field_op->value_;
                    break;
                case 2:
                    view->value_->dirty[2] = true;
                    view->value_->f_ptr[2] = field_op->value_;
                    break;
                case 3:
                    view->value_->dirty[3] = true;
                    view->value_->f_ptr[3] = field_op->value_;
                    break;
                case 4:
                    view->value_->dirty[4] = true;
                    view->value_->f_ptr[4] = field_op->value_;
                    break;
                case 5:
                    view->value_->dirty[5] = true;
                    view->value_->f_ptr[5] = field_op->value_;
                    break;
                case 6:
                    view->value_->dirty[6] = true;
                    view->value_->f_ptr[6] = field_op->value_;
                    break;
                case 7:
                    view->value_->dirty[7] = true;
                    view->value_->f_ptr[7] = field_op->value_;
                    break;
                case 8:
                    view->value_->dirty[8] = true;
                    view->value_->f_ptr[8] = field_op->value_;
                    break;
                case 9:
                    view->value_->dirty[9] = true;
                    view->value_->f_ptr[9] = field_op->value_;
                    break;
                default:
                    break;
                }
            }
            else
            {
                LOG(ERROR) << "invalid state op type for ycsb: " << static_cast<int>(payload->state_op_type_) << ", state_key: " << view->state_key_;
                exit(-1);
            }
        }

        void flush_value(View::View<State> *view)
        {
            for (int i = 0; i < 10; i++)
            {
                if (view->value_->dirty[i])
                {
                    view->value_->dirty[i] = false;
                    switch (i)
                    {
                    case 0:
                        memcpy(view->value_->f0, view->value_->f_ptr[0], FIELD_SIZE);
                        break;
                    case 1:
                        memcpy(view->value_->f1, view->value_->f_ptr[1], FIELD_SIZE);
                        break;
                    case 2:
                        memcpy(view->value_->f2, view->value_->f_ptr[2], FIELD_SIZE);
                        break;
                    case 3:
                        memcpy(view->value_->f3, view->value_->f_ptr[3], FIELD_SIZE);
                        break;
                    case 4:
                        memcpy(view->value_->f4, view->value_->f_ptr[4], FIELD_SIZE);
                        break;
                    case 5:
                        memcpy(view->value_->f5, view->value_->f_ptr[5], FIELD_SIZE);
                        break;
                    case 6:
                        memcpy(view->value_->f6, view->value_->f_ptr[6], FIELD_SIZE);
                        break;
                    case 7:
                        memcpy(view->value_->f7, view->value_->f_ptr[7], FIELD_SIZE);
                        break;
                    case 8:
                        memcpy(view->value_->f8, view->value_->f_ptr[8], FIELD_SIZE);
                        break;
                    case 9:
                        memcpy(view->value_->f9, view->value_->f_ptr[9], FIELD_SIZE);
                        break;
                    default:
                        break;
                    }
                }
            }
        }

        char *get_field(View::View<State> *view, std::uint8_t field_id)
        {
            if (view->value_->dirty[field_id])
            {
                view->value_->dirty[field_id] = false;
                switch (field_id)
                {
                case 0:
                    memcpy(view->value_->f0, view->value_->f_ptr[0], FIELD_SIZE);
                    return view->value_->f0;
                case 1:
                    memcpy(view->value_->f1, view->value_->f_ptr[1], FIELD_SIZE);
                    return view->value_->f1;
                case 2:
                    memcpy(view->value_->f2, view->value_->f_ptr[2], FIELD_SIZE);
                    return view->value_->f2;
                case 3:
                    memcpy(view->value_->f3, view->value_->f_ptr[3], FIELD_SIZE);
                    return view->value_->f3;
                case 4:
                    memcpy(view->value_->f4, view->value_->f_ptr[4], FIELD_SIZE);
                    return view->value_->f4;
                case 5:
                    memcpy(view->value_->f5, view->value_->f_ptr[5], FIELD_SIZE);
                    return view->value_->f5;
                case 6:
                    memcpy(view->value_->f6, view->value_->f_ptr[6], FIELD_SIZE);
                    return view->value_->f6;
                case 7:
                    memcpy(view->value_->f7, view->value_->f_ptr[7], FIELD_SIZE);
                    return view->value_->f7;
                case 8:
                    memcpy(view->value_->f8, view->value_->f_ptr[8], FIELD_SIZE);
                    return view->value_->f8;
                case 9:
                    memcpy(view->value_->f9, view->value_->f_ptr[9], FIELD_SIZE);
                    return view->value_->f9;
                default:
                    LOG(ERROR) << "Invalid field id: " << field_id;
                    exit(-1);
                }
            }
            else
            {
                return view->value_->f_ptr[field_id];
            }
        }
    }
}
