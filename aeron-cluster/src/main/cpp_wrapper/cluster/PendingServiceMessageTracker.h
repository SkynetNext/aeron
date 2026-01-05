#pragma once
#include <memory>
#include <vector>
#include "Counter.h"
#include "../client/ClusterExceptions.h"
#include "../service/ClusterClock.h"
#include "concurrent/AtomicBuffer.h"
// TODO: ExpandableRingBuffer needs to be implemented or replaced
// #include "util/ExpandableRingBuffer.h"
#include "generated/aeron_cluster_client/MessageHeader.h"
#include "generated/aeron_cluster_client/SessionMessageHeader.h"
#include "../client/AeronCluster.h"

namespace aeron { namespace cluster
{
using namespace aeron::concurrent;
using namespace aeron::cluster::codecs;
using namespace aeron::util;

class LogPublisher; // Forward declaration

class PendingServiceMessageTracker
{
public:
    static constexpr std::int32_t SERVICE_MESSAGE_LIMIT = 20;

    PendingServiceMessageTracker(
        std::int32_t serviceId,
        std::shared_ptr<Counter> commitPosition,
        LogPublisher& logPublisher,
        service::ClusterClock& clusterClock);

    void leadershipTermId(std::int64_t leadershipTermId);

    std::int32_t serviceId() const;
    std::int64_t nextServiceSessionId() const;
    std::int64_t logServiceSessionId() const;

    void enqueueMessage(AtomicBuffer& buffer, std::int32_t offset, std::int32_t length);

    void sweepFollowerMessages(std::int64_t clusterSessionId);

    void sweepLeaderMessages();

    void restoreUncommittedMessages();

    void appendMessage(AtomicBuffer& buffer, std::int32_t offset, std::int32_t length);

    void loadState(
        std::int64_t nextServiceSessionId,
        std::int64_t logServiceSessionId,
        std::int32_t pendingMessageCapacity);

    std::int32_t poll();

    std::int32_t size() const;

    void verify();

    void reset();

    // TODO: ExpandableRingBuffer needs to be implemented
    // ExpandableRingBuffer& pendingMessages();

    static std::int32_t serviceIdFromLogMessage(std::int64_t clusterSessionId);

    /**
     * Services use different approach for communicating the serviceId, this method extracts the serviceId from a
     * cluster session id sent via an inter-service message.
     *
     * @param clusterSessionId passed in on an inter-service message.
     * @return the associated serviceId.
     */
    static std::int32_t serviceIdFromServiceMessage(std::int64_t clusterSessionId);

    static std::int64_t serviceSessionId(std::int32_t serviceId, std::int64_t sessionId);

private:
    bool messageAppender(AtomicBuffer& buffer, std::int32_t offset, std::int32_t length, std::int32_t headOffset);

    static bool messageReset(AtomicBuffer& buffer, std::int32_t offset, std::int32_t length, std::int32_t headOffset);

    bool leaderMessageSweeper(AtomicBuffer& buffer, std::int32_t offset, std::int32_t length, std::int32_t headOffset);

    bool followerMessageSweeper(AtomicBuffer& buffer, std::int32_t offset, std::int32_t length, std::int32_t headOffset);

    std::int32_t m_serviceId;
    std::int32_t m_pendingMessageHeadOffset = 0;
    std::int32_t m_uncommittedMessages = 0;
    std::int64_t m_nextServiceSessionId;
    std::int64_t m_logServiceSessionId;
    std::int64_t m_leadershipTermId = aeron::NULL_VALUE;

    std::shared_ptr<Counter> m_commitPosition;
    LogPublisher& m_logPublisher;
    service::ClusterClock& m_clusterClock;
    // TODO: ExpandableRingBuffer needs to be implemented
    // ExpandableRingBuffer m_pendingMessages;
};

}}

