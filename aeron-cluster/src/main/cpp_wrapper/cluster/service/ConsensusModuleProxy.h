#pragma once
#include <memory>
#include <string>
#include <vector>

#include "Publication.h"
#include "concurrent/logbuffer/BufferClaim.h"
#include "concurrent/AtomicBuffer.h"
#include "client/AeronCluster.h"
#include "client/ClusterExceptions.h"
#include "generated/aeron_cluster_codecs/MessageHeader.h"
#include "generated/aeron_cluster_codecs/ScheduleTimer.h"
#include "generated/aeron_cluster_codecs/CancelTimer.h"
#include "generated/aeron_cluster_codecs/ServiceAck.h"
#include "generated/aeron_cluster_codecs/CloseSession.h"
#include "generated/aeron_cluster_codecs/ClusterMembersQuery.h"
#include "generated/aeron_cluster_codecs/BooleanType.h"

namespace aeron { namespace cluster { namespace service
{
using namespace aeron::concurrent;
using namespace aeron::concurrent::logbuffer;
using namespace aeron::cluster::codecs;
using namespace aeron::util;

class ConsensusModuleProxy
{
public:
    explicit ConsensusModuleProxy(std::shared_ptr<Publication> publication);
    ~ConsensusModuleProxy() = default;

    void close();

    bool scheduleTimer(std::int64_t correlationId, std::int64_t deadline);
    bool cancelTimer(std::int64_t correlationId);

    std::int64_t offer(
        AtomicBuffer& headerBuffer,
        std::int32_t headerOffset,
        std::int32_t headerLength,
        AtomicBuffer& messageBuffer,
        std::int32_t messageOffset,
        std::int32_t messageLength);

    // Note: C++ does not have DirectBufferVector equivalent directly, needs adaptation or alternative.
    // For now, omitting the DirectBufferVector offer method.

    std::int64_t tryClaim(std::int32_t length, BufferClaim& bufferClaim, AtomicBuffer& sessionHeader);

    bool ack(
        std::int64_t logPosition,
        std::int64_t timestamp,
        std::int64_t ackId,
        std::int64_t relevantId,
        std::int32_t serviceId);

    bool closeSession(std::int64_t clusterSessionId);

    bool clusterMembersQuery(std::int64_t correlationId);

private:
    static void checkResult(std::int64_t position, Publication& publication);

    BufferClaim m_bufferClaim;
    MessageHeaderEncoder m_messageHeaderEncoder;
    ScheduleTimerEncoder m_scheduleTimerEncoder;
    CancelTimerEncoder m_cancelTimerEncoder;
    ServiceAckEncoder m_serviceAckEncoder;
    CloseSessionEncoder m_closeSessionEncoder;
    ClusterMembersQueryEncoder m_clusterMembersQueryEncoder;
    std::shared_ptr<Publication> m_publication;
};

}}}

