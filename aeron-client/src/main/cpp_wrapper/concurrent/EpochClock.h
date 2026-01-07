/*
 * Copyright 2014-2025 Real Logic Limited.
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

#ifndef AERON_CONCURRENT_EPOCH_CLOCK_H
#define AERON_CONCURRENT_EPOCH_CLOCK_H

#include <cstdint>
#include <chrono>
#include <memory>

namespace aeron { namespace concurrent {

/**
 * Interface for getting time in milliseconds since epoch (1 Jan 1970 UTC).
 * This is a C++ wrapper equivalent of org.agrona.concurrent.EpochClock.
 */
class EpochClock
{
public:
    virtual ~EpochClock() = default;

    /**
     * Get the current time in milliseconds since epoch (1 Jan 1970 UTC).
     *
     * @return the current time in milliseconds since epoch.
     */
    virtual std::int64_t time() = 0;
};

/**
 * System implementation of EpochClock using std::chrono.
 */
class SystemEpochClock : public EpochClock
{
public:
    std::int64_t time() override
    {
        auto now = std::chrono::system_clock::now();
        auto duration = now.time_since_epoch();
        auto millis = std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
        return static_cast<std::int64_t>(millis);
    }
};

}}

#endif // AERON_CONCURRENT_EPOCH_CLOCK_H

