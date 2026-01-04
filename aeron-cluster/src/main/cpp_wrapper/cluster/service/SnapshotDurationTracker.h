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

#include <cstdint>
#include <memory>
#include <limits>
#include "concurrent/AtomicCounter.h"

namespace aeron { namespace cluster { namespace service
{

using namespace aeron::concurrent;

/**
 * Snapshot duration tracker that tracks maximum snapshot duration and also keeps count of how many times a predefined
 * duration threshold is breached.
 */
class SnapshotDurationTracker
{
private:
    std::shared_ptr<AtomicCounter> m_maxSnapshotDuration;
    std::shared_ptr<AtomicCounter> m_snapshotDurationThresholdExceededCount;
    std::int64_t m_durationThresholdNs;
    std::int64_t m_snapshotStartTimeNs = std::numeric_limits<std::int64_t>::min();

public:
    /**
     * Create a tracker to track max snapshot duration and breaches of a threshold.
     *
     * @param maxSnapshotDuration                    counter for tracking.
     * @param snapshotDurationThresholdExceededCount counter for tracking.
     * @param durationThresholdNs                    to use for tracking breaches.
     */
    SnapshotDurationTracker(
        std::shared_ptr<AtomicCounter> maxSnapshotDuration,
        std::shared_ptr<AtomicCounter> snapshotDurationThresholdExceededCount,
        std::int64_t durationThresholdNs) :
        m_maxSnapshotDuration(maxSnapshotDuration),
        m_snapshotDurationThresholdExceededCount(snapshotDurationThresholdExceededCount),
        m_durationThresholdNs(durationThresholdNs)
    {
    }

    /**
     * Get max snapshot duration counter.
     *
     * @return max snapshot duration counter.
     */
    std::shared_ptr<AtomicCounter> maxSnapshotDuration() const
    {
        return m_maxSnapshotDuration;
    }

    /**
     * Get counter tracking number of times durationThresholdNs was exceeded.
     *
     * @return duration threshold exceeded counter.
     */
    std::shared_ptr<AtomicCounter> snapshotDurationThresholdExceededCount() const
    {
        return m_snapshotDurationThresholdExceededCount;
    }

    /**
     * Called when snapshotting has started.
     *
     * @param timeNanos snapshot start time in nanoseconds.
     */
    void onSnapshotBegin(std::int64_t timeNanos)
    {
        m_snapshotStartTimeNs = timeNanos;
    }

    /**
     * Called when snapshot has been taken.
     *
     * @param timeNanos snapshot end time in nanoseconds.
     */
    void onSnapshotEnd(std::int64_t timeNanos)
    {
        if (m_snapshotStartTimeNs != std::numeric_limits<std::int64_t>::min())
        {
            const std::int64_t snapshotDurationNs = timeNanos - m_snapshotStartTimeNs;

            if (snapshotDurationNs > m_durationThresholdNs)
            {
                m_snapshotDurationThresholdExceededCount->increment();
            }

            // proposeMax: atomically update to max of current and new value
            std::int64_t currentMax = m_maxSnapshotDuration->get();
            while (snapshotDurationNs > currentMax)
            {
                if (m_maxSnapshotDuration->compareAndSet(currentMax, snapshotDurationNs))
                {
                    break;
                }
                currentMax = m_maxSnapshotDuration->get();
            }
        }
    }

    std::string toString() const
    {
        return "SnapshotDurationTracker{" +
            "maxSnapshotDuration=" + std::to_string(m_maxSnapshotDuration->id()) +
            ", snapshotDurationThresholdExceededCount=" + std::to_string(m_snapshotDurationThresholdExceededCount->id()) +
            ", durationThresholdNs=" + std::to_string(m_durationThresholdNs) +
            "}";
    }
};

}}}

