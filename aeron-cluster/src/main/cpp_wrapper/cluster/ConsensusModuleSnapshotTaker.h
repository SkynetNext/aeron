#pragma once

#include <memory>
#include <vector>
#include <cstdint>
#include "ExclusivePublication.h"
#include "concurrent/logbuffer/BufferClaim.h"
#include "concurrent/AtomicBuffer.h"
#include "concurrent/IdleStrategy.h"
#include "concurrent/AgentInvoker.h"
#include "service/SnapshotTaker.h"
#include "client/ClusterExceptions.h"
#include "util/Exceptions.h"
#include "generated/aeron_cluster_codecs/MessageHeader.h"
#include "generated/aeron_cluster_codecs/ClusterSession.h"
#include "generated/aeron_cluster_codecs/Timer.h"
#include "generated/aeron_cluster_codecs/ConsensusModule.h"
#include "generated/aeron_cluster_codecs/PendingMessageTracker.h"
#include "TimerService.h"
#include "PendingServiceMessageTracker.h"

namespace aeron { namespace cluster {

using namespace aeron::concurrent;
using namespace aeron::concurrent::logbuffer;
using namespace aeron::cluster::codecs;
using namespace aeron::util;

class ClusterSession; // Forward declaration

/**
 * Taker for consensus module snapshots.
 */
class ConsensusModuleSnapshotTaker : public service::SnapshotTaker, public TimerService::TimerSnapshotTaker
{
public:
    static constexpr std::int32_t ENCODED_TIMER_LENGTH =
        MessageHeader::encodedLength() + TimerEncoder::SBE_BLOCK_LENGTH;

    ConsensusModuleSnapshotTaker(
        std::shared_ptr<ExclusivePublication> publication,
        std::shared_ptr<IdleStrategy> idleStrategy,
        std::shared_ptr<void> aeronClientInvoker); // Generic AgentInvoker

    // MessageConsumer interface (for ExpandableRingBuffer)
    bool onMessage(AtomicBuffer& buffer, std::int32_t offset, std::int32_t length, std::int32_t headOffset);

    void snapshotConsensusModuleState(
        std::int64_t nextSessionId,
        std::int64_t nextServiceSessionId,
        std::int64_t logServiceSessionId,
        std::int32_t pendingMessageCapacity);

    void snapshotSession(ClusterSession& session);

    // TimerSnapshotTaker interface
    void snapshotTimer(std::int64_t correlationId, std::int64_t deadline) override;

    void snapshot(PendingServiceMessageTracker& tracker, const exception_handler_t& errorHandler);

private:
    void encodeSession(
        ClusterSession& session,
        const std::string& responseChannel,
        AtomicBuffer& buffer,
        std::int32_t offset);

    static std::int64_t correctNextServiceSessionId(
        PendingServiceMessageTracker& tracker,
        const exception_handler_t& errorHandler);

    std::vector<std::uint8_t> m_offerBufferData;
    AtomicBuffer m_offerBuffer;
    ClusterSessionEncoder m_clusterSessionEncoder;
    TimerEncoder m_timerEncoder;
    ConsensusModuleEncoder m_consensusModuleEncoder;
    PendingMessageTrackerEncoder m_pendingMessageTrackerEncoder;
};

// Implementation
inline ConsensusModuleSnapshotTaker::ConsensusModuleSnapshotTaker(
    std::shared_ptr<ExclusivePublication> publication,
    std::shared_ptr<IdleStrategy> idleStrategy,
    std::shared_ptr<void> aeronClientInvoker) :
    service::SnapshotTaker(publication, idleStrategy, aeronClientInvoker),
    m_offerBufferData(1024, 0)
{
    m_offerBuffer.wrap(m_offerBufferData.data(), m_offerBufferData.size());
}

inline bool ConsensusModuleSnapshotTaker::onMessage(
    AtomicBuffer& buffer,
    std::int32_t offset,
    std::int32_t length,
    std::int32_t /* headOffset */)
{
    offer(buffer, offset, length);
    return true;
}

inline void ConsensusModuleSnapshotTaker::snapshotConsensusModuleState(
    std::int64_t nextSessionId,
    std::int64_t nextServiceSessionId,
    std::int64_t logServiceSessionId,
    std::int32_t pendingMessageCapacity)
{
    const std::int32_t length = static_cast<std::int32_t>(
        MessageHeader::encodedLength() + ConsensusModuleEncoder::SBE_BLOCK_LENGTH);

    m_idleStrategy->reset();
    while (true)
    {
        const std::int64_t result = m_publication->tryClaim(length, m_bufferClaim);
        if (result > 0)
        {
            m_consensusModuleEncoder
                .wrapAndApplyHeader(m_bufferClaim.buffer(), m_bufferClaim.offset(), m_messageHeaderEncoder)
                .nextSessionId(nextSessionId)
                .nextServiceSessionId(nextServiceSessionId)
                .logServiceSessionId(logServiceSessionId)
                .pendingMessageCapacity(pendingMessageCapacity);

            m_bufferClaim.commit();
            break;
        }

        checkResultAndIdle(result);
    }
}

inline void ConsensusModuleSnapshotTaker::snapshotSession(ClusterSession& session)
{
    const std::string responseChannel = session.responseChannel();
    const std::int32_t length = static_cast<std::int32_t>(
        MessageHeader::encodedLength() + ClusterSessionEncoder::SBE_BLOCK_LENGTH +
        ClusterSessionEncoder::responseChannelHeaderLength() + responseChannel.length());

    if (length <= m_publication->maxPayloadLength())
    {
        m_idleStrategy->reset();
        while (true)
        {
            const std::int64_t result = m_publication->tryClaim(length, m_bufferClaim);
            if (result > 0)
            {
                encodeSession(session, responseChannel, m_bufferClaim.buffer(), m_bufferClaim.offset());
                m_bufferClaim.commit();
                break;
            }

            checkResultAndIdle(result);
        }
    }
    else
    {
        const std::int32_t offset = 0;
        encodeSession(session, responseChannel, m_offerBuffer, offset);
        offer(m_offerBuffer, offset, length);
    }
}

inline void ConsensusModuleSnapshotTaker::snapshotTimer(std::int64_t correlationId, std::int64_t deadline)
{
    m_idleStrategy->reset();
    while (true)
    {
        const std::int64_t result = m_publication->tryClaim(ENCODED_TIMER_LENGTH, m_bufferClaim);
        if (result > 0)
        {
            m_timerEncoder
                .wrapAndApplyHeader(m_bufferClaim.buffer(), m_bufferClaim.offset(), m_messageHeaderEncoder)
                .correlationId(correlationId)
                .deadline(deadline);
            m_bufferClaim.commit();
            break;
        }

        checkResultAndIdle(result);
    }
}

inline void ConsensusModuleSnapshotTaker::snapshot(
    PendingServiceMessageTracker& tracker,
    const exception_handler_t& errorHandler)
{
    const std::int32_t length = static_cast<std::int32_t>(
        MessageHeader::encodedLength() + PendingMessageTrackerEncoder::SBE_BLOCK_LENGTH);
    const std::int64_t nextServiceSessionId = correctNextServiceSessionId(tracker, errorHandler);

    m_idleStrategy->reset();
    while (true)
    {
        const std::int64_t result = m_publication->tryClaim(length, m_bufferClaim);
        if (result > 0)
        {
            m_pendingMessageTrackerEncoder
                .wrapAndApplyHeader(m_bufferClaim.buffer(), m_bufferClaim.offset(), m_messageHeaderEncoder)
                .nextServiceSessionId(nextServiceSessionId)
                .logServiceSessionId(tracker.logServiceSessionId())
                .pendingMessageCapacity(tracker.size())
                .serviceId(tracker.serviceId());
            m_bufferClaim.commit();
            break;
        }

        checkResultAndIdle(result);
    }

    // Use forEach to iterate through all messages
    SimpleExpandableRingBuffer::MessageConsumer consumer = [this](
        AtomicBuffer& buffer, std::int32_t offset, std::int32_t length, std::int32_t /* headOffset */) -> bool
    {
        return onMessage(buffer, offset, length, 0);
    };
    
    tracker.pendingMessages().forEach(consumer, std::numeric_limits<std::int32_t>::max());
}

inline void ConsensusModuleSnapshotTaker::encodeSession(
    ClusterSession& session,
    const std::string& responseChannel,
    AtomicBuffer& buffer,
    std::int32_t offset)
{
    m_clusterSessionEncoder
        .wrapAndApplyHeader(buffer, offset, m_messageHeaderEncoder)
        .clusterSessionId(session.id())
        .correlationId(session.correlationId())
        .openedLogPosition(session.openedLogPosition())
        .timeOfLastActivity(aeron::NULL_VALUE)
        .closeReason(session.closeReason())
        .responseStreamId(session.responseStreamId())
        .responseChannel(responseChannel);
}

inline std::int64_t ConsensusModuleSnapshotTaker::correctNextServiceSessionId(
    PendingServiceMessageTracker& tracker,
    const exception_handler_t& errorHandler)
{
    const std::int64_t nextServiceSessionId = (tracker.pendingMessages().size() == 0) ?
        tracker.logServiceSessionId() + 1 : tracker.nextServiceSessionId();
    const std::int64_t missedServiceMessageCount = nextServiceSessionId - tracker.nextServiceSessionId();

    if (0 < missedServiceMessageCount)
    {
        const std::string message = "Follower has missed " + std::to_string(missedServiceMessageCount) +
            " service message(s).  Please check service (id=" + std::to_string(tracker.serviceId()) +
            ") determinism around the use of Cluster::offer";
        errorHandler(std::make_exception_ptr(ClusterEvent(message, SOURCEINFO)));
    }

    return nextServiceSessionId;
}

}}
