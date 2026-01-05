#pragma once

#include <memory>
#include <vector>
#include <cstdint>
#include "ExclusivePublication.h"
#include "concurrent/logbuffer/BufferClaim.h"
#include "concurrent/AtomicBuffer.h"
#include "concurrent/IdleStrategy.h"
#include "concurrent/AgentInvoker.h"
#include "../service/SnapshotTaker.h"
#include "../client/ClusterExceptions.h"
#include "util/Exceptions.h"
#include "generated/aeron_cluster_client/MessageHeader.h"
#include "generated/aeron_cluster_client/ClusterSession.h"
#include "generated/aeron_cluster_client/Timer.h"
#include "generated/aeron_cluster_client/ConsensusModule.h"
#include "generated/aeron_cluster_client/PendingMessageTracker.h"
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
        MessageHeaderEncoder::encodedLength() + TimerEncoder::SBE_BLOCK_LENGTH;

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

}}

