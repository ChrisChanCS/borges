#include "Time.h"

namespace star
{
    std::chrono::steady_clock::time_point Time::startTime = std::chrono::steady_clock::now();
} // namespace star
