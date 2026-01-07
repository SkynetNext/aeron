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

#ifndef AERON_CONCURRENT_ERRORS_DISTINCT_ERROR_LOG_H
#define AERON_CONCURRENT_ERRORS_DISTINCT_ERROR_LOG_H

#include <string>
#include <memory>
#include <cstdint>
#include "../AtomicBuffer.h"
#include "../EpochClock.h"

namespace aeron { namespace concurrent { namespace errors {

/**
 * Distinct record of error observations. This is a C++ wrapper equivalent of org.agrona.concurrent.errors.DistinctErrorLog.
 * TODO: Full implementation using C API aeron_distinct_error_log_t
 */
class DistinctErrorLog
{
public:
    /**
     * Construct a DistinctErrorLog wrapping an AtomicBuffer and using an EpochClock.
     *
     * @param buffer the buffer to use for error logging.
     * @param epochClock the clock to use for timestamps.
     */
    DistinctErrorLog(AtomicBuffer& buffer, std::shared_ptr<EpochClock> epochClock) :
        m_buffer(&buffer),
        m_epochClock(epochClock)
    {
        // TODO: Initialize using C API aeron_distinct_error_log_init
    }

    ~DistinctErrorLog()
    {
        // TODO: Cleanup using C API aeron_distinct_error_log_close
    }

    /**
     * Record an error observation.
     *
     * @param errorCode the error code.
     * @param description the error description.
     */
    void record(int errorCode, const std::string& description)
    {
        // TODO: Implement using C API aeron_distinct_error_log_record
        (void)errorCode;
        (void)description;
    }

    /**
     * Record an error observation from an exception-like object.
     * This is a convenience method that extracts the message from the object.
     *
     * @param event an object with a toString() method (like ClusterEvent).
     */
    template<typename T>
    void record(const T& event)
    {
        // Extract message - assume T has a toString() or similar method
        // For now, use a simple approach
        std::string message = "Error: " + std::to_string(sizeof(T));
        record(0, message);
        (void)event;
    }

private:
    AtomicBuffer* m_buffer;
    std::shared_ptr<EpochClock> m_epochClock;
};

}}}

#endif // AERON_CONCURRENT_ERRORS_DISTINCT_ERROR_LOG_H

