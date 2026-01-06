#pragma once
#include <memory>
#include "Aeron.h"
#include "BufferBuilder.h"
#include "Image.h"
#include "Subscription.h"
#include "../client/ClusterExceptions.h"
#include "../service/ClusterClock.h"
#include "concurrent/logbuffer/ControlledFragmentHandler.h"
#include "concurrent/logbuffer/Header.h"
#include "concurrent/AtomicBuffer.h"
#include "util/BitUtil.h"
#include "util/CloseHelper.h"
#include "util/Exceptions.h"
#include "concurrent/logbuffer/FrameDescriptor.h"
#include "concurrent/logbuffer/DataFrameHeader.h"
#include "generated/aeron_cluster_client/MessageHeader.h"
#include "generated/aeron_cluster_client/SessionOpenEvent.h"
#include "generated/aeron_cluster_client/SessionCloseEvent.h"
#include "generated/aeron_cluster_client/SessionMessageHeader.h"
#include "generated/aeron_cluster_client/TimerEvent.h"
#include "generated/aeron_cluster_client/ClusterActionRequest.h"
#include "generated/aeron_cluster_client/NewLeadershipTermEvent.h"

namespace aeron { namespace cluster
{
using namespace aeron::concurrent;
using namespace aeron::concurrent::logbuffer;
using namespace aeron::cluster::codecs;
using namespace aeron::util;

class ConsensusModuleAgent; // Forward declaration

class LogAdapter : public ControlledFragmentHandler
{
public:
    LogAdapter(ConsensusModuleAgent& consensusModuleAgent, std::int32_t fragmentLimit);

    std::int64_t disconnect(const exception_handler_t& errorHandler);

    void disconnect(const exception_handler_t& errorHandler, std::int64_t maxLogPosition);

    std::shared_ptr<Subscription> subscription() const;

    ConsensusModuleAgent& consensusModuleAgent() const;

    std::int64_t position() const;

    std::int32_t poll(std::int64_t boundPosition);

    bool isImageClosed() const;

    bool isLogEndOfStream() const;

    bool isLogEndOfStreamAt(std::int64_t position) const;

    std::shared_ptr<Image> image() const;

    void image(std::shared_ptr<Image> image);

    void asyncRemoveDestination(const std::string& destination);

    ControlledPollAction onFragment(
        AtomicBuffer& buffer,
        std::int32_t offset,
        std::int32_t length,
        Header& header) override;

private:
    ControlledPollAction onMessage(
        AtomicBuffer& buffer,
        std::int32_t offset,
        std::int32_t length,
        Header& header);

    std::int32_t m_fragmentLimit;
    std::int64_t m_logPosition = 0;
    std::shared_ptr<Image> m_image;
    ConsensusModuleAgent& m_consensusModuleAgent;
    BufferBuilder m_builder;
    MessageHeaderDecoder m_messageHeaderDecoder;
    SessionOpenEventDecoder m_sessionOpenEventDecoder;
    SessionCloseEventDecoder m_sessionCloseEventDecoder;
    SessionMessageHeaderDecoder m_sessionHeaderDecoder;
    TimerEventDecoder m_timerEventDecoder;
    ClusterActionRequestDecoder m_clusterActionRequestDecoder;
    NewLeadershipTermEventDecoder m_newLeadershipTermEventDecoder;
};

// Implementation
inline LogAdapter::LogAdapter(ConsensusModuleAgent& consensusModuleAgent, std::int32_t fragmentLimit) :
    m_fragmentLimit(fragmentLimit),
    m_consensusModuleAgent(consensusModuleAgent)
{
}

inline std::int64_t LogAdapter::disconnect(const exception_handler_t& errorHandler)
{
    std::int64_t registrationId = Aeron::NULL_VALUE;

    if (m_image)
    {
        m_logPosition = m_image->position();
        CloseHelper::close(errorHandler, m_image->subscription());
        registrationId = m_image->subscription()->registrationId();
        m_image.reset();
    }

    return registrationId;
}

inline void LogAdapter::disconnect(const exception_handler_t& errorHandler, std::int64_t maxLogPosition)
{
    m_consensusModuleAgent.awaitLocalSocketsClosed(disconnect(errorHandler));
    m_logPosition = std::min(m_logPosition, maxLogPosition);
}

inline std::shared_ptr<Subscription> LogAdapter::subscription() const
{
    return m_image ? m_image->subscription() : nullptr;
}

inline ConsensusModuleAgent& LogAdapter::consensusModuleAgent() const
{
    return m_consensusModuleAgent;
}

inline std::int64_t LogAdapter::position() const
{
    if (!m_image)
    {
        return m_logPosition;
    }

    return m_image->position();
}

inline std::int32_t LogAdapter::poll(std::int64_t boundPosition)
{
    if (!m_image)
    {
        return 0;
    }

    return m_image->boundedControlledPoll(*this, boundPosition, m_fragmentLimit);
}

inline bool LogAdapter::isImageClosed() const
{
    return !m_image || m_image->isClosed();
}

inline bool LogAdapter::isLogEndOfStream() const
{
    return m_image && m_image->isEndOfStream();
}

inline bool LogAdapter::isLogEndOfStreamAt(std::int64_t position) const
{
    return m_image && position == m_image->endOfStreamPosition();
}

inline std::shared_ptr<Image> LogAdapter::image() const
{
    return m_image;
}

inline void LogAdapter::image(std::shared_ptr<Image> image)
{
    if (m_image)
    {
        m_logPosition = m_image->position();
    }

    m_image = image;
}

inline void LogAdapter::asyncRemoveDestination(const std::string& destination)
{
    if (m_image && !m_image->subscription()->isClosed())
    {
        m_image->subscription()->asyncRemoveDestination(destination);
    }
}

inline ControlledPollAction LogAdapter::onFragment(
    AtomicBuffer& buffer,
    std::int32_t offset,
    std::int32_t length,
    Header& header)
{
    ControlledPollAction action = ControlledPollAction::CONTINUE;
    const std::uint8_t flags = header.flags();

    if ((flags & FrameDescriptor::UNFRAGMENTED) == FrameDescriptor::UNFRAGMENTED)
    {
        action = onMessage(buffer, offset, length, header);
    }
    else if ((flags & FrameDescriptor::BEGIN_FRAG) == FrameDescriptor::BEGIN_FRAG)
    {
        m_builder.reset()
            .captureHeader(header)
            .append(buffer, offset, length);
        m_builder.nextTermOffset(BitUtil::align(offset + length + DataFrameHeader::LENGTH, FrameDescriptor::FRAME_ALIGNMENT));
    }
    else if (offset == m_builder.nextTermOffset())
    {
        const std::uint32_t limit = m_builder.limit();

        m_builder.append(buffer, offset, length);

        if ((flags & FrameDescriptor::END_FRAG) == FrameDescriptor::END_FRAG)
        {
            AtomicBuffer assembledBuffer(m_builder.buffer(), m_builder.limit());
            action = onMessage(assembledBuffer, 0, m_builder.limit(), header);

            if (ControlledPollAction::ABORT == action)
            {
                m_builder.limit(limit);
            }
            else
            {
                m_builder.reset();
            }
        }
        else
        {
            m_builder.nextTermOffset(BitUtil::align(offset + length + DataFrameHeader::LENGTH, FrameDescriptor::FRAME_ALIGNMENT));
        }
    }
    else
    {
        m_builder.reset();
    }

    return action;
}

inline ControlledPollAction LogAdapter::onMessage(
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
        return m_consensusModuleAgent.onReplayExtensionMessage(
            actingBlockLength, templateId, schemaId, actingVersion, buffer, offset, length, header);
    }

    switch (templateId)
    {
        case SessionMessageHeader::sbeTemplateId():
            m_sessionHeaderDecoder.wrap(
                buffer,
                offset + MessageHeader::encodedLength(),
                m_messageHeaderDecoder.sbeBlockLength(),
                actingVersion);

            m_consensusModuleAgent.onReplaySessionMessage(
                m_sessionHeaderDecoder.clusterSessionId(),
                m_sessionHeaderDecoder.timestamp());

            return ControlledPollAction::CONTINUE;

        case TimerEvent::sbeTemplateId():
            m_timerEventDecoder.wrap(
                buffer,
                offset + MessageHeader::encodedLength(),
                m_messageHeaderDecoder.sbeBlockLength(),
                actingVersion);

            m_consensusModuleAgent.onReplayTimerEvent(
                m_timerEventDecoder.correlationId());
            break;

        case SessionOpenEvent::sbeTemplateId():
            m_sessionOpenEventDecoder.wrap(
                buffer,
                offset + MessageHeader::encodedLength(),
                m_messageHeaderDecoder.sbeBlockLength(),
                actingVersion);

            m_consensusModuleAgent.onReplaySessionOpen(
                header.position(),
                m_sessionOpenEventDecoder.correlationId(),
                m_sessionOpenEventDecoder.clusterSessionId(),
                m_sessionOpenEventDecoder.timestamp(),
                m_sessionOpenEventDecoder.responseStreamId(),
                m_sessionOpenEventDecoder.responseChannel());
            break;

        case SessionCloseEvent::sbeTemplateId():
            m_sessionCloseEventDecoder.wrap(
                buffer,
                offset + MessageHeader::encodedLength(),
                m_messageHeaderDecoder.sbeBlockLength(),
                actingVersion);

            m_consensusModuleAgent.onReplaySessionClose(
                m_sessionCloseEventDecoder.clusterSessionId(),
                static_cast<CloseReason::Value>(m_sessionCloseEventDecoder.closeReason()));
            break;

        case ClusterActionRequest::sbeTemplateId():
            m_clusterActionRequestDecoder.wrap(
                buffer,
                offset + MessageHeader::encodedLength(),
                m_messageHeaderDecoder.sbeBlockLength(),
                actingVersion);

            {
                const std::int32_t flags = m_clusterActionRequestDecoder.flags() != ClusterActionRequest::flagsNullValue() ?
                    m_clusterActionRequestDecoder.flags() : 0; // CLUSTER_ACTION_FLAGS_DEFAULT = 0

                m_consensusModuleAgent.onReplayClusterAction(
                    m_clusterActionRequestDecoder.leadershipTermId(),
                    m_clusterActionRequestDecoder.logPosition(),
                    m_clusterActionRequestDecoder.timestamp(),
                    static_cast<ClusterAction::Value>(m_clusterActionRequestDecoder.action()),
                    flags);
            }
            return ControlledPollAction::BREAK;

        case NewLeadershipTermEvent::sbeTemplateId():
            m_newLeadershipTermEventDecoder.wrap(
                buffer,
                offset + MessageHeader::encodedLength(),
                m_messageHeaderDecoder.sbeBlockLength(),
                actingVersion);

            m_consensusModuleAgent.onReplayNewLeadershipTermEvent(
                m_newLeadershipTermEventDecoder.leadershipTermId(),
                m_newLeadershipTermEventDecoder.logPosition(),
                m_newLeadershipTermEventDecoder.timestamp(),
                m_newLeadershipTermEventDecoder.termBaseLogPosition(),
                ClusterClock::map(m_newLeadershipTermEventDecoder.timeUnit()),
                m_newLeadershipTermEventDecoder.appVersion());
            break;
    }

    return ControlledPollAction::CONTINUE;
}

}}

