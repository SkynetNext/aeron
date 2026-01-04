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

#include "Aeron.h"
#include "concurrent/logbuffer/Header.h"
#include "concurrent/AtomicBuffer.h"
#include "generated/aeron_cluster_client/EventCode.h"
#include "generated/aeron_cluster_client/AdminRequestType.h"
#include "generated/aeron_cluster_client/AdminResponseCode.h"

namespace aeron { namespace cluster { namespace client
{

/**
 * Interface for consuming messages coming from the cluster that also include administrative events in a controlled
 * fashion like ControlledFragmentHandler. Only session messages may be controlled in consumption, other are
 * consumed via Action::COMMIT.
 * 
 * This interface corresponds to io.aeron.cluster.client.ControlledEgressListener
 */
class ControlledEgressListener
{
public:
    /**
     * Action to be taken after processing a fragment.
     */
    enum class Action
    {
        CONTINUE,  ///< Continue processing more fragments
        ABORT,     ///< Abort the current polling operation
        BREAK,     ///< Break out of the current polling operation
        COMMIT     ///< Commit the current fragment
    };

    virtual ~ControlledEgressListener() = default;

    /**
     * Message event returned from the clustered service.
     *
     * @param clusterSessionId to which the message belongs.
     * @param timestamp        at which the correlated ingress was sequenced in the cluster.
     * @param buffer           containing the message.
     * @param offset           at which the message begins.
     * @param length           of the message in bytes.
     * @param header           Aeron header associated with the message fragment.
     * @return what action should be taken regarding advancement of the stream.
     */
    virtual Action onMessage(
        std::int64_t clusterSessionId,
        std::int64_t timestamp,
        concurrent::AtomicBuffer &buffer,
        util::index_t offset,
        util::index_t length,
        concurrent::logbuffer::Header &header) = 0;

    /**
     * Session event emitted from the cluster which after connect can indicate an error or session close.
     *
     * @param correlationId    associated with the cluster ingress.
     * @param clusterSessionId to which the event belongs.
     * @param leadershipTermId for identifying the active term of leadership
     * @param leaderMemberId   identity of the active leader.
     * @param code             to indicate the type of event.
     * @param detail           Textual detail to explain the event.
     */
    virtual void onSessionEvent(
        std::int64_t correlationId,
        std::int64_t clusterSessionId,
        std::int64_t leadershipTermId,
        std::int32_t leaderMemberId,
        EventCode::Value code,
        const std::string &detail)
    {
        // Default implementation does nothing
    }

    /**
     * Event indicating a new leader has been elected.
     *
     * @param clusterSessionId to which the event belongs.
     * @param leadershipTermId for identifying the active term of leadership
     * @param leaderMemberId   identity of the active leader.
     * @param ingressEndpoints for connecting to the cluster.
     */
    virtual void onNewLeader(
        std::int64_t clusterSessionId,
        std::int64_t leadershipTermId,
        std::int32_t leaderMemberId,
        const std::string &ingressEndpoints)
    {
        // Default implementation does nothing
    }

    /**
     * Message returned in response to an admin request.
     *
     * @param clusterSessionId to which the response belongs.
     * @param correlationId    of the admin request.
     * @param requestType      of the admin request.
     * @param responseCode     describing the response.
     * @param message          describing the response (e.g. error message).
     * @param payload          delivered with the response, can be empty.
     * @param payloadOffset    into the payload buffer.
     * @param payloadLength    of the payload.
     */
    virtual void onAdminResponse(
        std::int64_t clusterSessionId,
        std::int64_t correlationId,
        AdminRequestType::Value requestType,
        AdminResponseCode::Value responseCode,
        const std::string &message,
        concurrent::AtomicBuffer &payload,
        util::index_t payloadOffset,
        util::index_t payloadLength)
    {
        // Default implementation does nothing
    }
};

}}}

