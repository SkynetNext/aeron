#pragma once
#include <memory>
#include <vector>
#include <string>
#include <functional>
#include "Subscription.h"
#include "../client/AeronCluster.h"
#include "../client/ClusterExceptions.h"
#include "ControlledFragmentAssembler.h"
#include "concurrent/logbuffer/ControlledFragmentHandler.h"
#include "concurrent/logbuffer/Header.h"
#include "concurrent/AtomicBuffer.h"
#include "generated/aeron_cluster_client/MessageHeader.h"
#include "generated/aeron_cluster_client/SessionConnectRequest.h"
#include "generated/aeron_cluster_client/SessionCloseRequest.h"
#include "generated/aeron_cluster_client/SessionMessageHeader.h"
#include "generated/aeron_cluster_client/SessionKeepAlive.h"
#include "generated/aeron_cluster_client/ChallengeResponse.h"
#include "generated/aeron_cluster_client/AdminRequest.h"

namespace aeron { namespace cluster
{
using namespace aeron::concurrent;
using namespace aeron::concurrent::logbuffer;
using namespace aeron::cluster::codecs;
using namespace aeron::util;

class ConsensusModuleAgent; // Forward declaration

class IngressAdapter
{
public:
    IngressAdapter(
        std::int32_t fragmentPollLimit,
        ConsensusModuleAgent& consensusModuleAgent);

    ~IngressAdapter() = default; // AutoCloseable equivalent

    void close();

    ControlledPollAction onMessage(
        AtomicBuffer& buffer,
        std::int32_t offset,
        std::int32_t length,
        Header& header);

    void connect(
        std::shared_ptr<Subscription> subscription,
        std::shared_ptr<Subscription> ipcSubscription);

    std::int32_t poll();

    void freeSessionBuffer(std::int32_t imageSessionId, bool isIpc);

private:
    std::int32_t m_fragmentPollLimit;
    MessageHeaderDecoder m_messageHeaderDecoder;
    SessionConnectRequestDecoder m_connectRequestDecoder;
    SessionCloseRequestDecoder m_closeRequestDecoder;
    SessionMessageHeaderDecoder m_sessionMessageHeaderDecoder;
    SessionKeepAliveDecoder m_sessionKeepAliveDecoder;
    ChallengeResponseDecoder m_challengeResponseDecoder;
    AdminRequestDecoder m_adminRequestDecoder;
    controlled_poll_fragment_handler_t m_onMessageHandler;
    ControlledFragmentAssembler m_udpFragmentAssembler;
    ControlledFragmentAssembler m_ipcFragmentAssembler;
    ConsensusModuleAgent& m_consensusModuleAgent;
    std::shared_ptr<Subscription> m_subscription;
    std::shared_ptr<Subscription> m_ipcSubscription;
};

// Implementation
inline IngressAdapter::IngressAdapter(
    std::int32_t fragmentPollLimit,
    ConsensusModuleAgent& consensusModuleAgent) :
    m_fragmentPollLimit(fragmentPollLimit),
    m_consensusModuleAgent(consensusModuleAgent),
    m_onMessageHandler([this](AtomicBuffer& buffer, std::int32_t offset, std::int32_t length, Header& header)
    {
        return this->onMessage(buffer, offset, length, header);
    }),
    m_udpFragmentAssembler(m_onMessageHandler),
    m_ipcFragmentAssembler(m_onMessageHandler)
{
}

inline void IngressAdapter::close()
{
    std::shared_ptr<Subscription> subscription = m_subscription;
    std::shared_ptr<Subscription> ipcSubscription = m_ipcSubscription;

    m_subscription.reset();
    m_ipcSubscription.reset();

    if (subscription)
    {
        subscription->close();
    }

    if (ipcSubscription)
    {
        ipcSubscription->close();
    }
}

inline ControlledPollAction IngressAdapter::onMessage(
    AtomicBuffer& buffer,
    std::int32_t offset,
    std::int32_t length,
    Header& header)
{
    m_messageHeaderDecoder.wrap(buffer, offset);

    const std::int32_t schemaId = m_messageHeaderDecoder.sbeSchemaId();
    const std::int32_t templateId = m_messageHeaderDecoder.sbeTemplateId();
    const std::int32_t actingVersion = m_messageHeaderDecoder.sbeVersion();
    const std::int32_t actingBlockLength = m_messageHeaderDecoder.sbeBlockLength();
    
    if (schemaId != MessageHeader::sbeSchemaId())
    {
        return m_consensusModuleAgent.onExtensionMessage(
            actingBlockLength, templateId, schemaId, actingVersion, buffer, offset, length, header);
    }

    if (templateId == SessionMessageHeader::sbeTemplateId())
    {
        m_sessionMessageHeaderDecoder.wrap(
            buffer,
            offset + MessageHeader::encodedLength(),
            m_messageHeaderDecoder.sbeBlockLength(),
            actingVersion);

        return m_consensusModuleAgent.onIngressMessage(
            m_sessionMessageHeaderDecoder.leadershipTermId(),
            m_sessionMessageHeaderDecoder.clusterSessionId(),
            buffer,
            offset + client::AeronCluster::SESSION_HEADER_LENGTH,
            length - client::AeronCluster::SESSION_HEADER_LENGTH);
    }

    switch (templateId)
    {
        case SessionConnectRequest::sbeTemplateId():
        {
            m_connectRequestDecoder.wrap(
                buffer,
                offset + MessageHeader::encodedLength(),
                m_messageHeaderDecoder.sbeBlockLength(),
                actingVersion);

            const std::string responseChannel = m_connectRequestDecoder.responseChannel();
            const std::int32_t credentialsLength = m_connectRequestDecoder.encodedCredentialsLength();
            std::vector<std::uint8_t> credentials;
            
            if (credentialsLength > 0)
            {
                credentials.resize(credentialsLength);
                m_connectRequestDecoder.getEncodedCredentials(credentials.data(), 0, credentialsLength);
            }
            else
            {
                m_connectRequestDecoder.skipEncodedCredentials();
            }
            
            const std::string clientInfo = m_connectRequestDecoder.clientInfo();

            m_consensusModuleAgent.onSessionConnect(
                m_connectRequestDecoder.correlationId(),
                m_connectRequestDecoder.responseStreamId(),
                m_connectRequestDecoder.version(),
                responseChannel,
                credentials,
                clientInfo,
                header);
            break;
        }

        case SessionCloseRequest::sbeTemplateId():
        {
            m_closeRequestDecoder.wrap(
                buffer,
                offset + MessageHeader::encodedLength(),
                m_messageHeaderDecoder.sbeBlockLength(),
                actingVersion);

            m_consensusModuleAgent.onSessionClose(
                m_closeRequestDecoder.leadershipTermId(),
                m_closeRequestDecoder.clusterSessionId());
            break;
        }

        case SessionKeepAlive::sbeTemplateId():
        {
            m_sessionKeepAliveDecoder.wrap(
                buffer,
                offset + MessageHeader::encodedLength(),
                m_messageHeaderDecoder.sbeBlockLength(),
                actingVersion);

            m_consensusModuleAgent.onSessionKeepAlive(
                m_sessionKeepAliveDecoder.leadershipTermId(),
                m_sessionKeepAliveDecoder.clusterSessionId(),
                header);
            break;
        }

        case ChallengeResponse::sbeTemplateId():
        {
            m_challengeResponseDecoder.wrap(
                buffer,
                offset + MessageHeader::encodedLength(),
                m_messageHeaderDecoder.sbeBlockLength(),
                actingVersion);

            std::vector<std::uint8_t> credentials(m_challengeResponseDecoder.encodedCredentialsLength());
            m_challengeResponseDecoder.getEncodedCredentials(credentials.data(), 0, credentials.size());

            m_consensusModuleAgent.onIngressChallengeResponse(
                m_challengeResponseDecoder.correlationId(),
                m_challengeResponseDecoder.clusterSessionId(),
                credentials);
            break;
        }

        case AdminRequest::sbeTemplateId():
        {
            m_adminRequestDecoder.wrap(
                buffer,
                offset + MessageHeader::encodedLength(),
                m_messageHeaderDecoder.sbeBlockLength(),
                actingVersion);

            const std::int32_t payloadOffset = m_adminRequestDecoder.offset() +
                m_adminRequestDecoder.encodedLength() +
                AdminRequest::payloadHeaderLength();
            
            m_consensusModuleAgent.onAdminRequest(
                m_adminRequestDecoder.leadershipTermId(),
                m_adminRequestDecoder.clusterSessionId(),
                m_adminRequestDecoder.correlationId(),
                static_cast<AdminRequestType::Value>(m_adminRequestDecoder.requestType()),
                buffer,
                payloadOffset,
                m_adminRequestDecoder.payloadLength());
            break;
        }
    }

    return ControlledPollAction::CONTINUE;
}

inline void IngressAdapter::connect(
    std::shared_ptr<Subscription> subscription,
    std::shared_ptr<Subscription> ipcSubscription)
{
    m_subscription = subscription;
    m_ipcSubscription = ipcSubscription;
}

inline std::int32_t IngressAdapter::poll()
{
    std::int32_t fragmentsRead = 0;

    if (m_subscription)
    {
        fragmentsRead += m_subscription->controlledPoll(m_udpFragmentAssembler.handler(), m_fragmentPollLimit);
    }

    if (m_ipcSubscription)
    {
        fragmentsRead += m_ipcSubscription->controlledPoll(m_ipcFragmentAssembler.handler(), m_fragmentPollLimit);
    }

    return fragmentsRead;
}

inline void IngressAdapter::freeSessionBuffer(std::int32_t imageSessionId, bool isIpc)
{
    if (isIpc)
    {
        m_ipcFragmentAssembler.deleteSessionBuffer(imageSessionId);
    }
    else
    {
        m_udpFragmentAssembler.deleteSessionBuffer(imageSessionId);
    }
}

}}
