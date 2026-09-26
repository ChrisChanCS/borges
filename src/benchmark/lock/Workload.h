//
// Created by Yi Lu on 7/25/18.
//

#pragma once

#include "Query.h"
#include "../../common/Time.h"
#include <glog/logging.h>
#include <cstring>

namespace star
{
    namespace lock
    {
        std::shared_ptr<State> init_value()
        {
            std::shared_ptr<State> ptr = std::make_shared<State>();
            ptr->owner_id = 0;
            ptr->prev_owner_id = 0;
            return ptr;
        }

    } // namespace lock
} // namespace star
