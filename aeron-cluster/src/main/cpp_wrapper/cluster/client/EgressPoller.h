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
#include "ControlledFragmentAssembler.h"
#include "concurrent/logbuffer/Header.h"
#include "concurrent/AtomicBuffer.h"
#include "generated/aeron_cluster_client/MessageHeader.h"
#include "generated/aeron_cluster_client/SessionMessageHeader.h"
#include "generated/aeron_cluster_client/SessionEvent.h"
#include "generated/aeron_cluster_client/NewLeaderEvent.h"
#include "generated/aeron_cluster_client/Challenge.h"
#include "generated/aeron_cluster_client/EventCode.h"

namespace aeron { namespace cluster { namespace client
{

/**
 * Poller for the egress from a cluster to capture administration message details.
 * 
 * This class corresponds to io.aeron.cluster.client.EgressPoller
 */
class EgressPoller
{
public:
    /**
     * Construct a poller on the egress subscription.
     *
     * @param subscription  for egress from the cluster.
     * @param fragmentLimit for each poll operation.
     */
    EgressPoller(Subscription &subscription, int fragmentLimit) :
        m_subscription(subscription),
        m_fragmentLimit(fragmentLimit),
        m_fragmentAssembler([this](concurrent::AtomicBuffer &buffer, util::index_t offset, util::index_t length, concurrent::logbuffer::Header &header)
        {
            return this->onFragment(buffer, offset, length, header);
        })
    {
    }

    /**
     * Get the Subscription used for polling events.
     *
     * @return the Subscription used for polling events.
     */
    inline Subscription &subscription()
    {
        return m_subscription;
    }

    /**
     * Image for the egress response from the cluster which can be used for connection tracking.
     *
     * @return Image for the egress response from the cluster which can be used for connection tracking.
     */
    inline Image *egressImage()
    {
        return m_egressImage;
    }

    /**
     * Get the template id of the last received event.
     *
     * @return the template id of the last received event.
     */
    inline std::int32_t templateId() const
    {
        return m_templateId;
    }

    /**
     * Cluster session id of the last polled event or NULL_VALUE if poll returned nothing.
     *
     * @return cluster session id of the last polled event or NULL_VALUE if not returned.
     */
    inline std::int64_t clusterSessionId() const
    {
        return m_clusterSessionId;
    }

    /**
     * Correlation id of the last polled event or NULL_VALUE if poll returned nothing.
     *
     * @return correlation id of the last polled event or NULL_VALUE if not returned.
     */
    inline std::int64_t correlationId() const
    {
        return m_correlationId;
    }

    /**
     * Leadership term id of the last polled event or NULL_VALUE if poll returned nothing.
     *
     * @return leadership term id of the last polled event or NULL_VALUE if not returned.
     */
    inline std::int64_t leadershipTermId() const
    {
        return m_leadershipTermId;
    }

    /**
     * Leader cluster member id of the last polled event or NULL_VALUE if poll returned nothing.
     *
     * @return leader cluster member id of the last polled event or NULL_VALUE if poll returned nothing.
     */
    inline std::int32_t leaderMemberId() const
    {
        return m_leaderMemberId;
    }

    /**
     * Get the event code returned from the last session event.
     *
     * @return the event code returned from the last session event.
     */
    inline EventCode::Value eventCode() const
    {
        return m_eventCode;
    }

    /**
     * Version response from the server in semantic version form.
     *
     * @return response from the server in semantic version form.
     */
    inline std::int32_t version() const
    {
        return m_version;
    }

    /**
     * Leader heartbeat timeout of the last polled event or NULL_VALUE if not available.
     *
     * @return leader heartbeat timeout of the last polled event or NULL_VALUE if not available.
     */
    inline std::int64_t leaderHeartbeatTimeoutNs() const
    {
        return m_leaderHeartbeatTimeoutNs;
    }

    /**
     * Get the detail returned from the last session event.
     *
     * @return the detail returned from the last session event.
     */
    inline const std::string &detail() const
    {
        return m_detail;
    }

    /**
     * Get the encoded challenge in the last challenge.
     *
     * @return the encoded challenge in the last challenge or empty vector if last message was not a challenge.
     */
    inline const std::vector<std::uint8_t> &encodedChallenge() const
    {
        return m_encodedChallenge;
    }

    /**
     * Has the last polling action received a complete event?
     *
     * @return true if the last polling action received a complete event.
     */
    inline bool isPollComplete() const
    {
        return m_isPollComplete;
    }

    /**
     * Was last message a challenge or not?
     *
     * @return true if last message was a challenge or false if not.
     */
    inline bool isChallenged() const
    {
        return Challenge::TEMPLATE_ID == m_templateId;
    }

    /**
     * Reset last captured value and poll the egress subscription for output.
     *
     * @return number of fragments consumed.
     */
    int poll()
    {
        if (m_isPollComplete)
        {
            m_isPollComplete = false;
            m_clusterSessionId = Aeron::NULL_VALUE;
            m_correlationId = Aeron::NULL_VALUE;
            m_leadershipTermId = Aeron::NULL_VALUE;
            m_leaderMemberId = Aeron::NULL_VALUE;
            m_templateId = Aeron::NULL_VALUE;
            m_version = 0;
            m_leaderHeartbeatTimeoutNs = Aeron::NULL_VALUE;
            m_eventCode = EventCode::Value::NULL_VALUE;
            m_detail = "";
            m_encodedChallenge.clear();
        }

        return m_subscription.controlledPoll(m_fragmentAssembler.handler(), m_fragmentLimit);
    }

private:
    Subscription &m_subscription;
    const int m_fragmentLimit;
    ControlledFragmentAssembler m_fragmentAssembler;

    Image *m_egressImage = nullptr;
    std::int64_t m_clusterSessionId = Aeron::NULL_VALUE;
    std::int64_t m_correlationId = Aeron::NULL_VALUE;
    std::int64_t m_leadershipTermId = Aeron::NULL_VALUE;
    std::int32_t m_leaderMemberId = Aeron::NULL_VALUE;
    std::int32_t m_templateId = Aeron::NULL_VALUE;
    std::int32_t m_version = 0;
    std::int64_t m_leaderHeartbeatTimeoutNs = Aeron::NULL_VALUE;
    bool m_isPollComplete = false;
    EventCode::Value m_eventCode = EventCode::Value::NULL_VALUE;
    std::string m_detail;
    std::vector<std::uint8_t> m_encodedChallenge;

    MessageHeaderDecoder m_messageHeaderDecoder;
    SessionMessageHeaderDecoder m_sessionMessageHeaderDecoder;
    SessionEventDecoder m_sessionEventDecoder;
    NewLeaderEventDecoder m_newLeaderEventDecoder;
    ChallengeDecoder m_challengeDecoder;

    ControlledFragmentHandler::Action onFragment(
        concurrent::AtomicBuffer &buffer,
        util::index_t offset,
        util::index_t length,
        concurrent::logbuffer::Header &header)
    {
        if (m_isPollComplete)
        {
            return ControlledFragmentHandler::Action::ABORT;
        }

        m_messageHeaderDecoder.wrap(buffer, offset);

        const std::int32_t schemaId = m_messageHeaderDecoder.schemaId();
        if (schemaId != MessageHeaderDecoder::SCHEMA_ID)
        {
            return ControlledFragmentHandler::Action::CONTINUE; // skip unknown schemas
        }

        m_templateId = m_messageHeaderDecoder.templateId();
        switch (m_templateId)
        {
            case SessionMessageHeaderDecoder::TEMPLATE_ID:
            {
                m_sessionMessageHeaderDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder::ENCODED_LENGTH,
                    m_messageHeaderDecoder.blockLength(),
                    m_messageHeaderDecoder.version());

                m_leadershipTermId = m_sessionMessageHeaderDecoder.leadershipTermId();
                m_clusterSessionId = m_sessionMessageHeaderDecoder.clusterSessionId();
                m_isPollComplete = true;
                return ControlledFragmentHandler::Action::BREAK;
            }

            case SessionEventDecoder::TEMPLATE_ID:
            {
                m_sessionEventDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder::ENCODED_LENGTH,
                    m_messageHeaderDecoder.blockLength(),
                    m_messageHeaderDecoder.version());

                m_clusterSessionId = m_sessionEventDecoder.clusterSessionId();
                m_correlationId = m_sessionEventDecoder.correlationId();
                m_leadershipTermId = m_sessionEventDecoder.leadershipTermId();
                m_leaderMemberId = m_sessionEventDecoder.leaderMemberId();
                m_eventCode = m_sessionEventDecoder.code();
                m_version = m_sessionEventDecoder.version();
                m_leaderHeartbeatTimeoutNs = leaderHeartbeatTimeoutNs(m_sessionEventDecoder);
                m_detail = m_sessionEventDecoder.detail();
                m_isPollComplete = true;
                m_egressImage = header.context();
                return ControlledFragmentHandler::Action::BREAK;
            }

            case NewLeaderEventDecoder::TEMPLATE_ID:
            {
                m_newLeaderEventDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder::ENCODED_LENGTH,
                    m_messageHeaderDecoder.blockLength(),
                    m_messageHeaderDecoder.version());

                m_clusterSessionId = m_newLeaderEventDecoder.clusterSessionId();
                m_leadershipTermId = m_newLeaderEventDecoder.leadershipTermId();
                m_leaderMemberId = m_newLeaderEventDecoder.leaderMemberId();
                m_detail = m_newLeaderEventDecoder.ingressEndpoints();
                m_isPollComplete = true;
                m_egressImage = header.context();
                return ControlledFragmentHandler::Action::BREAK;
            }

            case ChallengeDecoder::TEMPLATE_ID:
            {
                m_challengeDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder::ENCODED_LENGTH,
                    m_messageHeaderDecoder.blockLength(),
                    m_messageHeaderDecoder.version());

                const std::int32_t encodedChallengeLength = m_challengeDecoder.encodedChallengeLength();
                m_encodedChallenge.resize(encodedChallengeLength == 0 ? 0 : encodedChallengeLength);
                if (encodedChallengeLength > 0)
                {
                    m_challengeDecoder.getEncodedChallenge(m_encodedChallenge.data(), 0, encodedChallengeLength);
                }

                m_clusterSessionId = m_challengeDecoder.clusterSessionId();
                m_correlationId = m_challengeDecoder.correlationId();
                m_isPollComplete = true;
                return ControlledFragmentHandler::Action::BREAK;
            }
        }

        return ControlledFragmentHandler::Action::CONTINUE;
    }

    static std::int64_t leaderHeartbeatTimeoutNs(const SessionEventDecoder &sessionEventDecoder)
    {
        const std::int64_t leaderHeartbeatTimeoutNs = sessionEventDecoder.leaderHeartbeatTimeoutNs();

        if (leaderHeartbeatTimeoutNs == SessionEventDecoder::leaderHeartbeatTimeoutNsNullValue())
        {
            return Aeron::NULL_VALUE;
        }

        return leaderHeartbeatTimeoutNs;
    }
};

}}}

