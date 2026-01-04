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
#ifndef AERON_CLUSTER_CLIENT_AERON_CLUSTER_H
#define AERON_CLUSTER_CLIENT_AERON_CLUSTER_H

#include <memory>
#include <string>
#include <atomic>
#include "Aeron.h"
#include "ClusterExceptions.h"

namespace aeron { namespace cluster { namespace client
{

class EgressListener;
class ControlledEgressListener;
class EgressPoller;

/**
 * Client for interacting with an Aeron Cluster.
 * 
 * A client will attempt to open a session and then offer ingress messages which are replicated to clustered services
 * for reliability. If the clustered service responds then response messages and events are sent via the egress stream.
 * 
 * Note: Instances of this class are not threadsafe.
 * 
 * This class corresponds to io.aeron.cluster.client.AeronCluster
 */
class AeronCluster
{
public:
    /**
     * Length of a session message header for cluster ingress or egress.
     */
    static constexpr int SESSION_HEADER_LENGTH = 
        MessageHeaderEncoder::ENCODED_LENGTH + SessionMessageHeaderEncoder::BLOCK_LENGTH;

    /**
     * Context for cluster session and connection.
     * 
     * This class corresponds to io.aeron.cluster.client.AeronCluster.Context
     */
    class Context
    {
        friend class AeronCluster;

    public:
        Context();

        ~Context();

        /**
         * Conclude configuration by setting up defaults when specifics are not provided.
         */
        void conclude();

        /**
         * Has the context had the conclude() method called.
         *
         * @return true if the conclude() method has been called.
         */
        bool isConcluded() const;

        // TODO: Add all Context methods 1:1 from Java

    private:
        std::atomic<bool> m_isConcluded;
        // TODO: Add all Context members 1:1 from Java
    };

    // TODO: Add all AeronCluster methods 1:1 from Java

private:
    // TODO: Add all AeronCluster members 1:1 from Java
};

}}}

#endif // AERON_CLUSTER_CLIENT_AERON_CLUSTER_H

