#pragma once
#include <memory>
#include "Subscription.h"
#include "../client/ClusterExceptions.h"
#include "FragmentAssembler.h"
#include "ControlledFragmentAssembler.h"
#include "concurrent/logbuffer/ControlledFragmentHandler.h"
#include "concurrent/logbuffer/Header.h"
#include "concurrent/AtomicBuffer.h"
#include "generated/aeron_cluster_client/MessageHeader.h"
#include "generated/aeron_cluster_client/SessionMessageHeader.h"
#include "generated/aeron_cluster_client/ScheduleTimer.h"
#include "generated/aeron_cluster_client/CancelTimer.h"
#include "generated/aeron_cluster_client/ServiceAck.h"
#include "generated/aeron_cluster_client/CloseSession.h"
#include "generated/aeron_cluster_client/ClusterMembersQuery.h"
#include "generated/aeron_cluster_client/BooleanType.h"

namespace aeron { namespace cluster
{
using namespace aeron::concurrent;
using namespace aeron::concurrent::logbuffer;
using namespace aeron::cluster::codecs;

class ConsensusModuleAgent; // Forward declaration

class ConsensusModuleAdapter
{
public:
    ConsensusModuleAdapter(
        std::shared_ptr<Subscription> subscription,
        ConsensusModuleAgent& consensusModuleAgent);

    ~ConsensusModuleAdapter() = default; // AutoCloseable equivalent

    void close();

    std::int32_t poll();

private:
    static constexpr std::int32_t FRAGMENT_LIMIT = 25;

    ControlledPollAction onFragment(
        AtomicBuffer& buffer,
        std::int32_t offset,
        std::int32_t length,
        Header& header);

    std::shared_ptr<Subscription> m_subscription;
    ConsensusModuleAgent& m_consensusModuleAgent;
    MessageHeaderDecoder m_messageHeaderDecoder;
    SessionMessageHeaderDecoder m_sessionMessageHeaderDecoder;
    ScheduleTimerDecoder m_scheduleTimerDecoder;
    CancelTimerDecoder m_cancelTimerDecoder;
    ServiceAckDecoder m_serviceAckDecoder;
    CloseSessionDecoder m_closeSessionDecoder;
    ClusterMembersQueryDecoder m_clusterMembersQueryDecoder;
    ControlledFragmentAssembler m_fragmentAssembler;
};

// Implementation
inline ConsensusModuleAdapter::ConsensusModuleAdapter(
    std::shared_ptr<Subscription> subscription,
    ConsensusModuleAgent& consensusModuleAgent) :
    m_subscription(subscription),
    m_consensusModuleAgent(consensusModuleAgent),
    m_fragmentAssembler([this](AtomicBuffer& buffer, std::int32_t offset, std::int32_t length, Header& header)
    {
        return this->onFragment(buffer, offset, length, header);
    })
{
}

inline void ConsensusModuleAdapter::close()
{
    if (m_subscription)
    {
        m_subscription->close();
    }
}

inline std::int32_t ConsensusModuleAdapter::poll()
{
    if (m_subscription)
    {
        return m_subscription->controlledPoll(m_fragmentAssembler.handler(), FRAGMENT_LIMIT);
    }
    return 0;
}

inline ControlledPollAction ConsensusModuleAdapter::onFragment(
    AtomicBuffer& buffer,
    std::int32_t offset,
    std::int32_t length,
    Header& header)
{
    m_messageHeaderDecoder.wrap(buffer, offset);

    const std::int32_t schemaId = m_messageHeaderDecoder.sbeSchemaId();
    if (schemaId != MessageHeader::sbeSchemaId())
    {
        throw ClusterException(
            "expected schemaId=" + std::to_string(MessageHeader::sbeSchemaId()) + 
            ", actual=" + std::to_string(schemaId),
            SOURCEINFO);
    }

    ControlledPollAction action = ControlledPollAction::CONTINUE;

    switch (m_messageHeaderDecoder.sbeTemplateId())
    {
        case SessionMessageHeader::sbeTemplateId():
            m_sessionMessageHeaderDecoder.wrap(
                buffer,
                offset + MessageHeader::encodedLength(),
                m_messageHeaderDecoder.sbeBlockLength(),
                m_messageHeaderDecoder.sbeVersion());

            m_consensusModuleAgent.onServiceMessage(
                m_sessionMessageHeaderDecoder.clusterSessionId(),
                buffer,
                offset + client::AeronCluster::SESSION_HEADER_LENGTH,
                length - client::AeronCluster::SESSION_HEADER_LENGTH);
            break;

        case CloseSession::sbeTemplateId():
            m_closeSessionDecoder.wrap(
                buffer,
                offset + MessageHeader::encodedLength(),
                m_messageHeaderDecoder.sbeBlockLength(),
                m_messageHeaderDecoder.sbeVersion());

            m_consensusModuleAgent.onServiceCloseSession(m_closeSessionDecoder.clusterSessionId());
            break;

        case ScheduleTimer::sbeTemplateId():
            m_scheduleTimerDecoder.wrap(
                buffer,
                offset + MessageHeader::encodedLength(),
                m_messageHeaderDecoder.sbeBlockLength(),
                m_messageHeaderDecoder.sbeVersion());

            m_consensusModuleAgent.onScheduleTimer(
                m_scheduleTimerDecoder.correlationId(),
                m_scheduleTimerDecoder.deadline());
            break;

        case CancelTimer::sbeTemplateId():
            m_cancelTimerDecoder.wrap(
                buffer,
                offset + MessageHeader::encodedLength(),
                m_messageHeaderDecoder.sbeBlockLength(),
                m_messageHeaderDecoder.sbeVersion());

            m_consensusModuleAgent.onCancelTimer(m_cancelTimerDecoder.correlationId());
            break;

        case ServiceAck::sbeTemplateId():
            m_serviceAckDecoder.wrap(
                buffer,
                offset + MessageHeader::encodedLength(),
                m_messageHeaderDecoder.sbeBlockLength(),
                m_messageHeaderDecoder.sbeVersion());

            m_consensusModuleAgent.onServiceAck(
                m_serviceAckDecoder.logPosition(),
                m_serviceAckDecoder.timestamp(),
                m_serviceAckDecoder.ackId(),
                m_serviceAckDecoder.relevantId(),
                m_serviceAckDecoder.serviceId());

            action = ControlledPollAction::BREAK;
            break;

        case ClusterMembersQuery::sbeTemplateId():
            m_clusterMembersQueryDecoder.wrap(
                buffer,
                offset + MessageHeader::encodedLength(),
                m_messageHeaderDecoder.sbeBlockLength(),
                m_messageHeaderDecoder.sbeVersion());

            m_consensusModuleAgent.onClusterMembersQuery(
                m_clusterMembersQueryDecoder.correlationId(),
                m_clusterMembersQueryDecoder.extended() == BooleanType::TRUE);
            break;
    }

    return action;
}

}}

