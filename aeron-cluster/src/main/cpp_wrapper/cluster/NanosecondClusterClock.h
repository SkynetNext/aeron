#pragma once
#include <chrono>
#include "../service/ClusterClock.h"
#include "concurrent/HighResolutionClock.h"

namespace aeron { namespace cluster
{
using namespace aeron::concurrent;

/**
 * A ClusterClock implemented by calling HighResolutionClock::epochNanos().
 */
class NanosecondClusterClock : public service::ClusterClock
{
public:
    std::chrono::milliseconds::rep timeUnit() const override
    {
        return 1000000; // nanoseconds per millisecond
    }

    std::int64_t time() override
    {
        return HighResolutionClock::epochNanos();
    }

    std::int64_t timeMillis() const override
    {
        return std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
    }

    std::int64_t timeMicros() const override
    {
        return HighResolutionClock::epochMicros();
    }

    std::int64_t timeNanos() const override
    {
        return HighResolutionClock::epochNanos();
    }

    std::int64_t convertToNanos(std::int64_t time) const override
    {
        return time;
    }
};

}}

