#pragma once
#include <memory>
#include "Aeron.h"
#include "BufferBuilder.h"
#include "Image.h"
#include "Subscription.h"
#include "client/ClusterExceptions.h"
#include "service/ClusterClock.h"
// ControlledFragmentHandler is replaced by ControlledPollAction in C++
// #include "concurrent/logbuffer/ControlledFragmentHandler.h"
#include "concurrent/logbuffer/Header.h"
#include "concurrent/AtomicBuffer.h"
#include "util/BitUtil.h"
#include "util/CloseHelper.h"
#include "util/Exceptions.h"
#include "Context.h"
#include "concurrent/logbuffer/FrameDescriptor.h"
#include "concurrent/logbuffer/DataFrameHeader.h"
#include "generated/aeron_cluster_codecs/MessageHeader.h"
#include "generated/aeron_cluster_codecs/SessionOpenEvent.h"
#include "generated/aeron_cluster_codecs/SessionCloseEvent.h"
#include "generated/aeron_cluster_codecs/SessionMessageHeader.h"
#include "generated/aeron_cluster_codecs/TimerEvent.h"
#include "generated/aeron_cluster_codecs/ClusterActionRequest.h"
#include "generated/aeron_cluster_codecs/NewLeadershipTermEvent.h"

namespace aeron { namespace cluster
{
using namespace aeron::concurrent;
using namespace aeron::concurrent::logbuffer;
using namespace aeron::cluster::codecs;
using namespace aeron::util;

// Forward declaration - full definition needed for inline methods
class ConsensusModuleAgent;

class LogAdapter
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
        Header& header);

private:
    ControlledPollAction onMessage(
        AtomicBuffer& buffer,
        std::int32_t offset,
        std::int32_t length,
        Header& header);

    std::int32_t m_fragmentLimit;
    std::int64_t m_logPosition = 0;
    std::shared_ptr<Image> m_image;
    std::shared_ptr<Subscription> m_subscription; // Store subscription separately since Image doesn't expose it
    ConsensusModuleAgent& m_consensusModuleAgent;
    BufferBuilder m_builder;
    MessageHeader m_messageHeaderDecoder;
    SessionOpenEvent m_sessionOpenEventDecoder;
    SessionCloseEvent m_sessionCloseEventDecoder;
    SessionMessageHeader m_sessionHeaderDecoder;
    TimerEvent m_timerEventDecoder;
    ClusterActionRequest m_clusterActionRequestDecoder;
    NewLeadershipTermEvent m_newLeadershipTermEventDecoder;
};

// Implementation
inline LogAdapter::LogAdapter(ConsensusModuleAgent& consensusModuleAgent, std::int32_t fragmentLimit) :
    m_fragmentLimit(fragmentLimit),
    m_consensusModuleAgent(consensusModuleAgent)
{
}

inline std::int64_t LogAdapter::disconnect(const exception_handler_t& errorHandler)
{
    std::int64_t registrationId = NULL_VALUE;

    if (m_image)
    {
        m_logPosition = m_image->position();
        CloseHelper::close(errorHandler, m_subscription);
        registrationId = m_subscription ? m_subscription->registrationId() : NULL_VALUE;
        m_image.reset();
    }

    return registrationId;
}

// Implementation moved to ConsensusModuleAgent.h to avoid circular dependency
// inline void LogAdapter::disconnect(const exception_handler_t& errorHandler, std::int64_t maxLogPosition)

inline std::shared_ptr<Subscription> LogAdapter::subscription() const
{
    return m_subscription;
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
    // Note: In C++, Image doesn't expose subscription(), so we need to get it from elsewhere
    // This will need to be set separately or retrieved from the ConsensusModuleAgent
    // For now, we'll leave m_subscription as is and it should be set when image is set
}

inline void LogAdapter::asyncRemoveDestination(const std::string& destination)
{
    if (m_subscription && !m_subscription->isClosed())
    {
        m_subscription->removeDestination(destination);
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

// Implementation moved to ConsensusModuleAgent.h to avoid circular dependency
// inline ControlledPollAction LogAdapter::onMessage(...)

}}

