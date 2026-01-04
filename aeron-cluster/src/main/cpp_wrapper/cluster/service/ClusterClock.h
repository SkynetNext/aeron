/*
 * Copyright 2014-2025 Justin Zhu.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once

#include <chrono>
#include "generated/aeron_cluster_client/ClusterTimeUnit.h"
#include "util/Exceptions.h"

namespace aeron { namespace cluster { namespace service
{

using namespace aeron::cluster::codecs;

/**
 * A clock representing a number of TimeUnits since 1 Jan 1970 UTC. Defaults to TimeUnit::MILLISECONDS.
 * 
 * This is the clock used to timestamp sequenced messages for the cluster log and timers. Implementations should
 * be efficient otherwise throughput of the cluster will be impacted due to frequency of use.
 */
class ClusterClock
{
public:
    virtual ~ClusterClock() = default;

    /**
     * The unit of time returned from the time() method.
     *
     * @return the unit of time returned from the time() method.
     */
    virtual std::chrono::milliseconds::rep timeUnit() const
    {
        return 1; // Default to milliseconds
    }

    /**
     * The count of timeUnit()s since 1 Jan 1970 UTC.
     *
     * @return the count of timeUnit()s since 1 Jan 1970 UTC.
     */
    virtual std::int64_t time() = 0;

    /**
     * Get the current time in milliseconds.
     *
     * @return the current time in milliseconds.
     */
    virtual std::int64_t timeMillis() const
    {
        // Default implementation assumes time() returns milliseconds
        return time();
    }

    /**
     * Get the current time in microseconds.
     *
     * @return the current time in microseconds.
     */
    virtual std::int64_t timeMicros() const
    {
        // Default implementation: convert from milliseconds
        return timeMillis() * 1000;
    }

    /**
     * Get the current time in nanoseconds.
     *
     * @return the current time in nanoseconds.
     */
    virtual std::int64_t timeNanos() const
    {
        // Default implementation: convert from milliseconds
        return timeMillis() * 1000000;
    }

    /**
     * Convert given Cluster time to nanoseconds.
     *
     * @param time to convert to nanoseconds.
     * @return time in nanoseconds.
     */
    virtual std::int64_t convertToNanos(std::int64_t time) const
    {
        // Default implementation: convert from milliseconds
        return time * 1000000;
    }

    /**
     * Map TimeUnit to a corresponding ClusterTimeUnit.
     *
     * @param timeUnit to map to a corresponding ClusterTimeUnit.
     * @return a corresponding ClusterTimeUnit.
     */
    static ClusterTimeUnit::Value map(std::chrono::milliseconds::rep timeUnit)
    {
        if (timeUnit == 1) // milliseconds
        {
            return ClusterTimeUnit::Value::MILLIS;
        }
        else if (timeUnit == 1000) // microseconds
        {
            return ClusterTimeUnit::Value::MICROS;
        }
        else if (timeUnit == 1000000) // nanoseconds
        {
            return ClusterTimeUnit::Value::NANOS;
        }
        else
        {
            throw IllegalArgumentException("unsupported time unit: " + std::to_string(timeUnit), SOURCEINFO);
        }
    }

    /**
     * Map ClusterTimeUnit to a corresponding TimeUnit multiplier.
     *
     * @param clusterTimeUnit to map to a corresponding TimeUnit.
     * @return a corresponding TimeUnit multiplier (nanoseconds per unit).
     */
    static std::int64_t map(ClusterTimeUnit::Value clusterTimeUnit)
    {
        switch (clusterTimeUnit)
        {
            case ClusterTimeUnit::Value::NULL_VAL:
            case ClusterTimeUnit::Value::MILLIS:
                return 1000000; // nanoseconds per millisecond

            case ClusterTimeUnit::Value::MICROS:
                return 1000; // nanoseconds per microsecond

            case ClusterTimeUnit::Value::NANOS:
                return 1; // nanoseconds per nanosecond

            default:
                throw IllegalArgumentException("unsupported time unit: " + std::to_string(static_cast<int>(clusterTimeUnit)), SOURCEINFO);
        }
    }
};

}}}

