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

#include "concurrent/AtomicBuffer.h"
#include "ControlledFragmentHandler.h"

namespace aeron { namespace cluster { namespace client
{

using namespace aeron::concurrent;
using namespace aeron::concurrent::logbuffer;

/**
 * Interface for consuming extension messages coming from the cluster that also
 * include administrative events in a controlled fashion like ControlledFragmentHandler.
 */
class ControlledEgressListenerExtension
{
public:
    virtual ~ControlledEgressListenerExtension() = default;

    /**
     * Message of unknown schema to egress that can be handled by specific listener implementation.
     *
     * @param actingBlockLength acting block length from header
     * @param templateId        template id
     * @param schemaId          schema id
     * @param actingVersion     acting version
     * @param buffer        message buffer
     * @param offset        message offset
     * @param length        message length
     * @return action to be taken after processing the message.
     */
    virtual ControlledPollAction onExtensionMessage(
        std::int32_t actingBlockLength,
        std::int32_t templateId,
        std::int32_t schemaId,
        std::int32_t actingVersion,
        AtomicBuffer &buffer,
        std::int32_t offset,
        std::int32_t length) = 0;
};

}}}

