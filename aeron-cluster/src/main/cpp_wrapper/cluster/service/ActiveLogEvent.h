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

#include <string>
#include "Cluster.h"

namespace aeron { namespace cluster { namespace service
{

/**
 * Event to signal a change of active log to follow.
 */
class ActiveLogEvent
{
public:
    const std::int64_t logPosition;
    const std::int64_t maxLogPosition;
    const std::int32_t memberId;
    const std::int32_t sessionId;
    const std::int32_t streamId;
    const bool isStartup;
    const Cluster::Role role;
    const std::string channel;

    ActiveLogEvent(
        std::int64_t logPosition,
        std::int64_t maxLogPosition,
        std::int32_t memberId,
        std::int32_t sessionId,
        std::int32_t streamId,
        bool isStartup,
        Cluster::Role role,
        const std::string& channel) :
        logPosition(logPosition),
        maxLogPosition(maxLogPosition),
        memberId(memberId),
        sessionId(sessionId),
        streamId(streamId),
        isStartup(isStartup),
        role(role),
        channel(channel)
    {
    }

    std::string toString() const
    {
        return "ActiveLogEvent{" +
            "logPosition=" + std::to_string(logPosition) +
            ", maxLogPosition=" + std::to_string(maxLogPosition) +
            ", memberId=" + std::to_string(memberId) +
            ", sessionId=" + std::to_string(sessionId) +
            ", streamId=" + std::to_string(streamId) +
            ", isStartup=" + (isStartup ? "true" : "false") +
            ", role=" + std::to_string(static_cast<int>(role)) +
            ", channel='" + channel + "'" +
            "}";
    }
};

}}}

