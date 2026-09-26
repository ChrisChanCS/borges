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
    namespace counter
    {
        std::shared_ptr<State> init_value()
        {
            std::shared_ptr<State> ptr = std::make_shared<State>();
            ptr->value = 0;
            return ptr;
        }

    } // namespace counter
} // namespace star
