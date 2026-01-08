#pragma once
#include <chrono>
#include "service/ClusterClock.h"

namespace aeron { namespace cluster
{

/**
 * A ClusterClock implemented by calling System.currentTimeMillis().
 */
class MillisecondClusterClock : public service::ClusterClock
{
public:
    std::chrono::milliseconds::rep timeUnit() const override
    {
        return 1; // milliseconds
    }

    std::int64_t time() override
    {
        return std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
    }

    std::int64_t timeMillis() const override
    {
        return std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
    }

    std::int64_t timeMicros() const override
    {
        // Java: MILLISECONDS.toMicros(System.currentTimeMillis()) = timeMillis() * 1000
        return timeMillis() * 1000;
    }

    std::int64_t timeNanos() const override
    {
        // Java: MILLISECONDS.toNanos(System.currentTimeMillis()) = timeMillis() * 1000000
        return timeMillis() * 1000000;
    }

    std::int64_t convertToNanos(std::int64_t time) const override
    {
        return std::chrono::milliseconds(time).count() * 1000000;
    }
};

}}

