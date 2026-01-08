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

#ifndef AERON_CONCURRENT_AGENT_TERMINATION_EXCEPTION_H
#define AERON_CONCURRENT_AGENT_TERMINATION_EXCEPTION_H

#include <stdexcept>
#include <string>

namespace aeron { namespace concurrent {

/**
 * Exception thrown when an agent is terminated.
 * This is a C++ wrapper equivalent of org.agrona.concurrent.AgentTerminationException.
 */
class AgentTerminationException : public std::runtime_error
{
public:
    /**
     * Construct an AgentTerminationException with a message.
     *
     * @param what the error message.
     */
    explicit AgentTerminationException(const std::string& what) : std::runtime_error(what) {}
};

}}

#endif // AERON_CONCURRENT_AGENT_TERMINATION_EXCEPTION_H

