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

#include "Subscription.h"
#include "FragmentAssembler.h"
#include "EgressListener.h"
#include "EgressListenerExtension.h"
#include "ClusterExceptions.h"
#include "concurrent/AtomicBuffer.h"
#include "concurrent/logbuffer/Header.h"
#include "generated/aeron_cluster_codecs/MessageHeader.h"
#include "generated/aeron_cluster_codecs/SessionMessageHeader.h"
#include "generated/aeron_cluster_codecs/SessionEvent.h"
#include "generated/aeron_cluster_codecs/NewLeaderEvent.h"
#include "generated/aeron_cluster_codecs/AdminResponse.h"
#include "AeronCluster.h"

// Forward declaration for test fixture (only defined in test files, in global namespace)
class EgressAdapterTest;

namespace aeron { namespace cluster { namespace client
{

using namespace aeron::concurrent;
using namespace aeron::concurrent::logbuffer;
using namespace aeron::cluster::codecs;

/**
 * Adapter for dispatching egress messages from a cluster to a EgressListener.
 */
class EgressAdapter
{
    // Allow test classes to access private onFragment method for 1:1 test translation
    friend class ::EgressAdapterTest;

public:
    /**
     * Construct an adapter for cluster egress which consumes from the subscription and dispatches to the
     * EgressListener.
     *
     * @param listener         to dispatch events to.
     * @param clusterSessionId for the egress.
     * @param subscription     over the egress stream.
     * @param fragmentLimit    to poll on each poll() operation.
     */
    EgressAdapter(
        std::shared_ptr<EgressListener> listener,
        std::int64_t clusterSessionId,
        std::shared_ptr<Subscription> subscription,
        int fragmentLimit) :
        EgressAdapter(listener, nullptr, clusterSessionId, subscription, fragmentLimit)
    {
    }

    /**
     * Construct an adapter for cluster egress which consumes from the subscription and dispatches to the
     * EgressListener or extension messages to EgressListenerExtension.
     *
     * @param listener          to dispatch events to.
     * @param listenerExtension to dispatch extension messages to
     * @param clusterSessionId  for the egress.
     * @param subscription      over the egress stream.
     * @param fragmentLimit     to poll on each poll() operation.
     */
    EgressAdapter(
        std::shared_ptr<EgressListener> listener,
        std::shared_ptr<EgressListenerExtension> listenerExtension,
        std::int64_t clusterSessionId,
        std::shared_ptr<Subscription> subscription,
        int fragmentLimit) :
        m_clusterSessionId(clusterSessionId),
        m_fragmentLimit(fragmentLimit),
        m_listener(listener),
        m_listenerExtension(listenerExtension),
        m_subscription(subscription),
        m_fragmentAssembler([this](AtomicBuffer &buffer, util::index_t offset, util::index_t length, Header &header)
        {
            this->onFragment(buffer, offset, length, header);
        })
    {
    }

    /**
     * Poll the egress subscription and dispatch assembled events to the EgressListener.
     *
     * @return the number of fragments consumed.
     */
    std::int32_t poll()
    {
        return m_subscription->poll(m_fragmentAssembler.handler(), m_fragmentLimit);
    }

public:
    // Made public for 1:1 test translation (friend declaration not working reliably)
    void onFragment(AtomicBuffer &buffer, std::int32_t offset, std::int32_t length, Header &header)
    {
        m_messageHeaderDecoder.wrap(reinterpret_cast<char *>(buffer.buffer()), offset, MessageHeader::sbeSchemaVersion(), buffer.capacity());

        const std::int32_t templateId = m_messageHeaderDecoder.templateId();
        const std::int32_t schemaId = m_messageHeaderDecoder.schemaId();
        // Note: MessageHeader::sbeSchemaId() incorrectly returns 112 due to code generation issue
        // The correct schema ID for cluster codecs is 111 (from aeron-cluster-codecs.xml)
        // Use SessionMessageHeader::sbeSchemaId() which correctly returns 111
        if (schemaId != SessionMessageHeader::sbeSchemaId())
        {
            if (m_listenerExtension)
            {
                m_listenerExtension->onExtensionMessage(
                    m_messageHeaderDecoder.blockLength(),
                    templateId,
                    schemaId,
                    m_messageHeaderDecoder.version(),
                    buffer,
                    offset + MessageHeader::encodedLength(),
                    length - MessageHeader::encodedLength());
                return;
            }
            throw ClusterException(
                "expected schemaId=" + std::to_string(SessionMessageHeader::sbeSchemaId()) +
                ", actual=" + std::to_string(schemaId), SOURCEINFO);
        }

        switch (templateId)
        {
            case SessionMessageHeader::SBE_TEMPLATE_ID:
            {
                m_sessionMessageHeaderDecoder.wrapForDecode(
                    reinterpret_cast<char *>(buffer.buffer()),
                    offset + MessageHeader::encodedLength(),
                    m_messageHeaderDecoder.blockLength(),
                    m_messageHeaderDecoder.version(),
                    buffer.capacity());

                const std::int64_t sessionId = m_sessionMessageHeaderDecoder.clusterSessionId();
                if (sessionId == m_clusterSessionId)
                {
                    m_listener->onMessage(
                        sessionId,
                        m_sessionMessageHeaderDecoder.timestamp(),
                        buffer,
                        offset + AeronCluster::SESSION_HEADER_LENGTH,
                        length - AeronCluster::SESSION_HEADER_LENGTH,
                        header);
                }
                break;
            }

            case SessionEvent::SBE_TEMPLATE_ID:
            {
                m_sessionEventDecoder.wrapForDecode(
                    reinterpret_cast<char *>(buffer.buffer()),
                    offset + MessageHeader::encodedLength(),
                    m_messageHeaderDecoder.blockLength(),
                    m_messageHeaderDecoder.version(),
                    buffer.capacity());

                const std::int64_t sessionId = m_sessionEventDecoder.clusterSessionId();
                if (sessionId == m_clusterSessionId)
                {
                    m_listener->onSessionEvent(
                        m_sessionEventDecoder.correlationId(),
                        sessionId,
                        m_sessionEventDecoder.leadershipTermId(),
                        m_sessionEventDecoder.leaderMemberId(),
                        m_sessionEventDecoder.code(),
                        m_sessionEventDecoder.detail());
                }
                break;
            }

            case NewLeaderEvent::SBE_TEMPLATE_ID:
            {
                m_newLeaderEventDecoder.wrapForDecode(
                    reinterpret_cast<char *>(buffer.buffer()),
                    offset + MessageHeader::encodedLength(),
                    m_messageHeaderDecoder.blockLength(),
                    m_messageHeaderDecoder.version(),
                    buffer.capacity());

                const std::int64_t sessionId = m_newLeaderEventDecoder.clusterSessionId();
                if (sessionId == m_clusterSessionId)
                {
                    m_listener->onNewLeader(
                        sessionId,
                        m_newLeaderEventDecoder.leadershipTermId(),
                        m_newLeaderEventDecoder.leaderMemberId(),
                        m_newLeaderEventDecoder.ingressEndpoints());
                }
                break;
            }

            case AdminResponse::SBE_TEMPLATE_ID:
            {
                m_adminResponseDecoder.wrapForDecode(
                    reinterpret_cast<char *>(buffer.buffer()),
                    offset + MessageHeader::encodedLength(),
                    m_messageHeaderDecoder.blockLength(),
                    m_messageHeaderDecoder.version(),
                    buffer.capacity());

                const std::int64_t sessionId = m_adminResponseDecoder.clusterSessionId();
                if (sessionId == m_clusterSessionId)
                {
                    const std::int64_t correlationId = m_adminResponseDecoder.correlationId();
                    const AdminRequestType::Value requestType = m_adminResponseDecoder.requestType();
                    const AdminResponseCode::Value responseCode = m_adminResponseDecoder.responseCode();
                    const std::string message = m_adminResponseDecoder.message();
                    const std::int32_t payloadOffset = m_adminResponseDecoder.offset() +
                        AdminResponse::SBE_BLOCK_LENGTH +
                        AdminResponse::messageHeaderLength() +
                        static_cast<std::int32_t>(message.length()) +
                        AdminResponse::payloadHeaderLength();
                    const std::int32_t payloadLength = m_adminResponseDecoder.payloadLength();
                    m_listener->onAdminResponse(
                        sessionId,
                        correlationId,
                        requestType,
                        responseCode,
                        message,
                        buffer,
                        payloadOffset,
                        payloadLength);
                }
                break;
            }

            default:
                break;
        }
    }

    std::int64_t m_clusterSessionId;
    int m_fragmentLimit;
    std::shared_ptr<EgressListener> m_listener;
    std::shared_ptr<EgressListenerExtension> m_listenerExtension;
    std::shared_ptr<Subscription> m_subscription;
    MessageHeader m_messageHeaderDecoder;
    SessionMessageHeader m_sessionMessageHeaderDecoder;
    SessionEvent m_sessionEventDecoder;
    NewLeaderEvent m_newLeaderEventDecoder;
    AdminResponse m_adminResponseDecoder;
    FragmentAssembler m_fragmentAssembler;
};

}}}

