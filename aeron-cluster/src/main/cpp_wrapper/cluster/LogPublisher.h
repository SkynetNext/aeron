#pragma once
#include <memory>
#include <vector>
#include <string>
#include <chrono>
#include "ExclusivePublication.h"
#include "Publication.h"
#include "../client/ClusterExceptions.h"
#include "../service/ClusterClock.h"
#include "concurrent/logbuffer/BufferClaim.h"
#include "concurrent/AtomicBuffer.h"
#include "util/CloseHelper.h"
#include "util/BitUtil.h"
#include "protocol/DataHeaderFlyweight.h"
#include "concurrent/logbuffer/FrameDescriptor.h"
#include "generated/aeron_cluster_client/MessageHeader.h"
#include "generated/aeron_cluster_client/SessionMessageHeader.h"
#include "generated/aeron_cluster_client/SessionOpenEvent.h"
#include "generated/aeron_cluster_client/SessionCloseEvent.h"
#include "generated/aeron_cluster_client/TimerEvent.h"
#include "generated/aeron_cluster_client/ClusterActionRequest.h"
#include "generated/aeron_cluster_client/NewLeadershipTermEvent.h"
#include "generated/aeron_cluster_client/ClusterAction.h"
#include "../client/AeronCluster.h"

namespace aeron { namespace cluster
{
using namespace aeron::concurrent;
using namespace aeron::concurrent::logbuffer;
using namespace aeron::cluster::codecs;
using namespace aeron::util;

class ClusterSession; // Forward declaration

class LogPublisher
{
public:
    explicit LogPublisher(const std::string& destinationChannel);

    void publication(std::shared_ptr<ExclusivePublication> publication);

    std::shared_ptr<ExclusivePublication> publication() const;

    void disconnect(const exception_handler_t& errorHandler);

    std::int64_t position() const;

    std::int32_t sessionId() const;

    void addDestination(const std::string& followerLogEndpoint);

    std::int64_t appendMessage(
        std::int64_t leadershipTermId,
        std::int64_t clusterSessionId,
        std::int64_t timestamp,
        AtomicBuffer& buffer,
        std::int32_t offset,
        std::int32_t length);

    std::int64_t appendSessionOpen(
        ClusterSession& session,
        std::int64_t leadershipTermId,
        std::int64_t timestamp);

    bool appendSessionClose(
        std::int32_t memberId,
        ClusterSession& session,
        std::int64_t leadershipTermId,
        std::int64_t timestamp,
        std::chrono::milliseconds::rep timeUnit);

    std::int64_t appendTimer(
        std::int64_t correlationId,
        std::int64_t leadershipTermId,
        std::int64_t timestamp);

    bool appendClusterAction(
        std::int64_t leadershipTermId,
        std::int64_t timestamp,
        ClusterAction::Value action,
        std::int32_t flags);

    bool appendNewLeadershipTermEvent(
        std::int64_t leadershipTermId,
        std::int64_t timestamp,
        std::int64_t termBaseLogPosition,
        std::int32_t leaderMemberId,
        std::int32_t logSessionId,
        std::chrono::milliseconds::rep timeUnit,
        std::int32_t appVersion);

    std::string toString() const;

private:
    static constexpr std::int32_t SEND_ATTEMPTS = 3;

    static void checkResult(std::int64_t position, Publication& publication);

    MessageHeader m_messageHeader;
    SessionMessageHeader m_sessionHeader;
    SessionOpenEvent m_sessionOpenEvent;
    SessionCloseEvent m_sessionCloseEvent;
    TimerEvent m_timerEvent;
    ClusterActionRequest m_clusterActionRequest;
    NewLeadershipTermEvent m_newLeadershipTermEvent;
    AtomicBuffer m_sessionHeaderBuffer;
    std::vector<std::uint8_t> m_sessionHeaderBufferData; // Equivalent to UnsafeBuffer
    std::vector<std::uint8_t> m_expandableArrayBufferData; // Equivalent to ExpandableArrayBuffer
    AtomicBuffer m_expandableArrayBuffer;
    BufferClaim m_bufferClaim;
    std::string m_destinationChannel;
    std::shared_ptr<ExclusivePublication> m_publication;
};

// Implementation
inline LogPublisher::LogPublisher(const std::string& destinationChannel) :
    m_sessionHeaderBufferData(AeronCluster::SESSION_HEADER_LENGTH, 0),
    m_expandableArrayBufferData(1024, 0),
    m_destinationChannel(destinationChannel),
    m_publication(nullptr)
{
    m_sessionHeaderBuffer.wrap(m_sessionHeaderBufferData.data(), m_sessionHeaderBufferData.size());
    m_expandableArrayBuffer.wrap(m_expandableArrayBufferData.data(), m_expandableArrayBufferData.size());
    
    m_sessionHeader.wrapAndApplyHeader(m_sessionHeaderBuffer, 0, m_messageHeader);
}

inline void LogPublisher::publication(std::shared_ptr<ExclusivePublication> publication)
{
    if (m_publication)
    {
        m_publication->close();
    }
    m_publication = publication;
}

inline std::shared_ptr<ExclusivePublication> LogPublisher::publication() const
{
    return m_publication;
}

inline void LogPublisher::disconnect(const exception_handler_t& errorHandler)
{
    if (m_publication)
    {
        CloseHelper::close(errorHandler, m_publication);
        m_publication.reset();
    }
}

inline std::int64_t LogPublisher::position() const
{
    if (!m_publication)
    {
        return 0;
    }
    return m_publication->position();
}

inline std::int32_t LogPublisher::sessionId() const
{
    if (!m_publication)
    {
        return 0;
    }
    return m_publication->sessionId();
}

inline void LogPublisher::addDestination(const std::string& followerLogEndpoint)
{
    if (m_publication)
    {
        // Create destination URI by appending endpoint to channel
        ChannelUri channelUri = ChannelUri::parse(m_destinationChannel);
        channelUri.put("endpoint", followerLogEndpoint);
        std::string destinationUri = channelUri.toString();
        m_publication->asyncAddDestination(destinationUri);
    }
}

inline std::int64_t LogPublisher::appendMessage(
    std::int64_t leadershipTermId,
    std::int64_t clusterSessionId,
    std::int64_t timestamp,
    AtomicBuffer& buffer,
    std::int32_t offset,
    std::int32_t length)
{
    m_sessionHeader
        .leadershipTermId(leadershipTermId)
        .clusterSessionId(clusterSessionId)
        .timestamp(timestamp);

    int attempts = SEND_ATTEMPTS;
    std::int64_t position;
    do
    {
        position = m_publication->offer(
            m_sessionHeaderBuffer, 0, AeronCluster::SESSION_HEADER_LENGTH,
            buffer, offset, length, nullptr);

        if (position > 0)
        {
            break;
        }

        checkResult(position, *m_publication);
    }
    while (--attempts > 0);

    return position;
}

inline std::int64_t LogPublisher::appendSessionOpen(
    ClusterSession& session,
    std::int64_t leadershipTermId,
    std::int64_t timestamp)
{
    std::int64_t position;
    const std::vector<std::uint8_t> encodedPrincipal = session.encodedPrincipal();
    const std::string channel = session.responseChannel();

    m_sessionOpenEvent
        .wrapAndApplyHeader(m_expandableArrayBuffer, 0, m_messageHeader)
        .leadershipTermId(leadershipTermId)
        .clusterSessionId(session.id())
        .correlationId(session.correlationId())
        .timestamp(timestamp)
        .responseStreamId(session.responseStreamId())
        .responseChannel(channel)
        .putEncodedPrincipal(encodedPrincipal.data(), 0, static_cast<std::int32_t>(encodedPrincipal.size()));

    const std::int32_t length = MessageHeader::encodedLength() + m_sessionOpenEvent.encodedLength();

    int attempts = SEND_ATTEMPTS;
    do
    {
        position = m_publication->offer(m_expandableArrayBuffer, 0, length, nullptr);
        if (position > 0)
        {
            break;
        }

        checkResult(position, *m_publication);
    }
    while (--attempts > 0);

    return position;
}

inline bool LogPublisher::appendSessionClose(
    std::int32_t memberId,
    ClusterSession& session,
    std::int64_t leadershipTermId,
    std::int64_t timestamp,
    std::chrono::milliseconds::rep timeUnit)
{
    const std::int32_t length = MessageHeader::encodedLength() + SessionCloseEvent::SBE_BLOCK_LENGTH;

    int attempts = SEND_ATTEMPTS;
    do
    {
        const std::int64_t position = m_publication->tryClaim(length, m_bufferClaim);
        if (position > 0)
        {
            m_sessionCloseEvent
                .wrapAndApplyHeader(m_bufferClaim.buffer(), m_bufferClaim.offset(), m_messageHeader)
                .leadershipTermId(leadershipTermId)
                .clusterSessionId(session.id())
                .timestamp(timestamp)
                .closeReason(session.closeReason());

            m_bufferClaim.commit();
            return true;
        }

        checkResult(position, *m_publication);
    }
    while (--attempts > 0);

    return false;
}

inline std::int64_t LogPublisher::appendTimer(
    std::int64_t correlationId,
    std::int64_t leadershipTermId,
    std::int64_t timestamp)
{
    const std::int32_t length = MessageHeader::encodedLength() + TimerEvent::SBE_BLOCK_LENGTH;

    int attempts = SEND_ATTEMPTS;
    std::int64_t position;
    do
    {
        position = m_publication->tryClaim(length, m_bufferClaim);
        if (position > 0)
        {
            m_timerEvent
                .wrapAndApplyHeader(m_bufferClaim.buffer(), m_bufferClaim.offset(), m_messageHeader)
                .leadershipTermId(leadershipTermId)
                .correlationId(correlationId)
                .timestamp(timestamp);

            m_bufferClaim.commit();
            break;
        }

        checkResult(position, *m_publication);
    }
    while (--attempts > 0);

    return position;
}

inline bool LogPublisher::appendClusterAction(
    std::int64_t leadershipTermId,
    std::int64_t timestamp,
    ClusterAction::Value action,
    std::int32_t flags)
{
    const std::int32_t length = MessageHeader::encodedLength() + ClusterActionRequest::SBE_BLOCK_LENGTH;
    const std::int32_t fragmentLength = DataHeaderFlyweight::HEADER_LENGTH + length;
    const std::int32_t alignedFragmentLength = BitUtil::align(fragmentLength, FrameDescriptor::FRAME_ALIGNMENT);

    int attempts = SEND_ATTEMPTS;
    do
    {
        const std::int64_t logPosition = m_publication->position() + alignedFragmentLength;
        const std::int64_t position = m_publication->tryClaim(length, m_bufferClaim);

        if (position > 0)
        {
            m_clusterActionRequest.wrapAndApplyHeader(
                m_bufferClaim.buffer(), m_bufferClaim.offset(), m_messageHeader)
                .leadershipTermId(leadershipTermId)
                .logPosition(logPosition)
                .timestamp(timestamp)
                .action(action)
                .flags(flags);

            m_bufferClaim.commit();
            return true;
        }

        checkResult(position, *m_publication);
    }
    while (--attempts > 0);

    return false;
}

inline bool LogPublisher::appendNewLeadershipTermEvent(
    std::int64_t leadershipTermId,
    std::int64_t timestamp,
    std::int64_t termBaseLogPosition,
    std::int32_t leaderMemberId,
    std::int32_t logSessionId,
    std::chrono::milliseconds::rep timeUnit,
    std::int32_t appVersion)
{
    const std::int32_t length = MessageHeader::encodedLength() + NewLeadershipTermEvent::SBE_BLOCK_LENGTH;
    const std::int32_t fragmentLength = DataHeaderFlyweight::HEADER_LENGTH + length;
    const std::int32_t alignedFragmentLength = BitUtil::align(fragmentLength, FrameDescriptor::FRAME_ALIGNMENT);

    int attempts = SEND_ATTEMPTS;
    do
    {
        const std::int64_t logPosition = m_publication->position() + alignedFragmentLength;
        const std::int64_t position = m_publication->tryClaim(length, m_bufferClaim);

        if (position > 0)
        {
            m_newLeadershipTermEvent.wrapAndApplyHeader(
                m_bufferClaim.buffer(), m_bufferClaim.offset(), m_messageHeader)
                .leadershipTermId(leadershipTermId)
                .logPosition(logPosition)
                .timestamp(timestamp)
                .termBaseLogPosition(termBaseLogPosition)
                .leaderMemberId(leaderMemberId)
                .logSessionId(logSessionId)
                .timeUnit(ClusterClock::map(timeUnit))
                .appVersion(appVersion);

            m_bufferClaim.commit();
            return true;
        }

        checkResult(position, *m_publication);
    }
    while (--attempts > 0);

    return false;
}

inline void LogPublisher::checkResult(std::int64_t position, Publication& publication)
{
    if (position == Publication::CLOSED)
    {
        throw ClusterException("log publication is closed", SOURCEINFO);
    }

    if (position == Publication::MAX_POSITION_EXCEEDED)
    {
        throw ClusterException(
            "log publication at max position: term-length=" + std::to_string(publication.termBufferLength()), SOURCEINFO);
    }
}

inline std::string LogPublisher::toString() const
{
    return "LogPublisher{destinationChannel='" + m_destinationChannel + "'}";
}

}}

