#pragma once
#include <memory>
#include <vector>
#include <string>
#include <chrono>
#include "ExclusivePublication.h"
#include "Publication.h"
#include "client/ClusterExceptions.h"
#include "service/ClusterClock.h"
#include "concurrent/logbuffer/BufferClaim.h"
#include "concurrent/AtomicBuffer.h"
#include "util/CloseHelper.h"
#include "util/BitUtil.h"
#include "concurrent/logbuffer/DataFrameHeader.h"
#include "concurrent/logbuffer/FrameDescriptor.h"
#include "generated/aeron_cluster_codecs/MessageHeader.h"
#include "generated/aeron_cluster_codecs/SessionMessageHeader.h"
#include "generated/aeron_cluster_codecs/SessionOpenEvent.h"
#include "generated/aeron_cluster_codecs/SessionCloseEvent.h"
#include "generated/aeron_cluster_codecs/TimerEvent.h"
#include "generated/aeron_cluster_codecs/ClusterActionRequest.h"
#include "generated/aeron_cluster_codecs/NewLeadershipTermEvent.h"
#include "generated/aeron_cluster_codecs/ClusterAction.h"
#include "client/AeronCluster.h"

namespace aeron { namespace cluster
{
using namespace aeron::concurrent;
using namespace aeron::concurrent::logbuffer;
using namespace aeron::cluster::codecs;
using namespace aeron::cluster::client;
using namespace aeron::cluster::service;
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

    static void checkResult(std::int64_t position, ExclusivePublication& publication);

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
// Note: ClusterSession must be fully defined when these inline functions are instantiated.
// Since ClusterSession.h includes LogPublisher.h, ClusterSession will be fully defined
// when these functions are actually used.

inline LogPublisher::LogPublisher(const std::string& destinationChannel) :
    m_sessionHeaderBufferData(AeronCluster::SESSION_HEADER_LENGTH, 0),
    m_expandableArrayBufferData(1024, 0),
    m_destinationChannel(destinationChannel),
    m_publication(nullptr)
{
    m_sessionHeaderBuffer.wrap(m_sessionHeaderBufferData.data(), m_sessionHeaderBufferData.size());
    m_expandableArrayBuffer.wrap(m_expandableArrayBufferData.data(), m_expandableArrayBufferData.size());
    
    m_sessionHeader.wrapAndApplyHeader(reinterpret_cast<char *>(m_sessionHeaderBuffer.buffer()), 0, m_sessionHeaderBuffer.capacity());
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
        std::shared_ptr<ChannelUri> channelUri = ChannelUri::parse(m_destinationChannel);
        channelUri->put("endpoint", followerLogEndpoint);
        std::string destinationUri = channelUri->toString();
        m_publication->addDestination(destinationUri);
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

    // Combine header and message into a single buffer array for offer
    const concurrent::AtomicBuffer buffers[] = { m_sessionHeaderBuffer, buffer };
    int attempts = SEND_ATTEMPTS;
    std::int64_t position;
    do
    {
        position = m_publication->offer(buffers, 2);

        if (position > 0)
        {
            break;
        }

        checkResult(position, *m_publication);
    }
    while (--attempts > 0);

    return position;
}

// Implementation of appendSessionOpen and appendSessionClose moved to ClusterSession.h
// to resolve circular dependency - these functions need ClusterSession's full definition

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
                .wrapAndApplyHeader(reinterpret_cast<char *>(m_bufferClaim.buffer().buffer()), m_bufferClaim.offset(), m_bufferClaim.buffer().capacity())
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
    const std::int32_t fragmentLength = DataFrameHeader::LENGTH + length;
    const std::int32_t alignedFragmentLength = BitUtil::align(fragmentLength, FrameDescriptor::FRAME_ALIGNMENT);

    int attempts = SEND_ATTEMPTS;
    do
    {
        const std::int64_t logPosition = m_publication->position() + alignedFragmentLength;
        const std::int64_t position = m_publication->tryClaim(length, m_bufferClaim);

        if (position > 0)
        {
            m_clusterActionRequest.wrapAndApplyHeader(
                reinterpret_cast<char *>(m_bufferClaim.buffer().buffer()), m_bufferClaim.offset(), m_bufferClaim.buffer().capacity())
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
    const std::int32_t fragmentLength = DataFrameHeader::LENGTH + length;
    const std::int32_t alignedFragmentLength = BitUtil::align(fragmentLength, FrameDescriptor::FRAME_ALIGNMENT);

    int attempts = SEND_ATTEMPTS;
    do
    {
        const std::int64_t logPosition = m_publication->position() + alignedFragmentLength;
        const std::int64_t position = m_publication->tryClaim(length, m_bufferClaim);

        if (position > 0)
        {
            m_newLeadershipTermEvent.wrapAndApplyHeader(
                reinterpret_cast<char *>(m_bufferClaim.buffer().buffer()), m_bufferClaim.offset(), m_bufferClaim.buffer().capacity())
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

inline void LogPublisher::checkResult(std::int64_t position, ExclusivePublication& publication)
{
    if (position == PUBLICATION_CLOSED)
    {
        throw ClusterException("log publication is closed", SOURCEINFO);
    }

    if (position == MAX_POSITION_EXCEEDED)
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

