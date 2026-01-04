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

namespace aeron { namespace cluster { namespace service
{

/**
 * Data class for holding the properties used when interacting with a cluster for local admin control.
 */
class ClusterNodeControlProperties
{
public:
    /**
     * Member id of the cluster to which the properties belong.
     */
    const std::int32_t memberId;

    /**
     * Stream id in the control channel on which the services listen.
     */
    const std::int32_t serviceStreamId;

    /**
     * Stream id in the control channel on which the consensus module listens.
     */
    const std::int32_t consensusModuleStreamId;

    /**
     * Directory where the Aeron Media Driver is running.
     */
    const std::string aeronDirectoryName;

    /**
     * URI for the control channel.
     */
    const std::string controlChannel;

    /**
     * Construct the set of properties for interacting with a cluster.
     *
     * @param memberId                of the cluster to which the properties belong.
     * @param serviceStreamId         in the control channel on which the services listen.
     * @param consensusModuleStreamId in the control channel on which the consensus module listens.
     * @param aeronDirectoryName      where the Aeron Media Driver is running.
     * @param controlChannel          for the services and consensus module.
     */
    ClusterNodeControlProperties(
        std::int32_t memberId,
        std::int32_t serviceStreamId,
        std::int32_t consensusModuleStreamId,
        const std::string& aeronDirectoryName,
        const std::string& controlChannel) :
        memberId(memberId),
        serviceStreamId(serviceStreamId),
        consensusModuleStreamId(consensusModuleStreamId),
        aeronDirectoryName(aeronDirectoryName),
        controlChannel(controlChannel)
    {
    }
};

}}}

