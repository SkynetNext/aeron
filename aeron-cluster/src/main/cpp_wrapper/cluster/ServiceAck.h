#pragma once
#include <deque>
#include <vector>
#include <string>
#include <sstream>
#include "../client/ClusterExceptions.h"

namespace aeron { namespace cluster
{

/**
 * State holder for ACKs from each of the ClusteredServices.
 */
class ServiceAck
{
public:
    static const std::vector<ServiceAck> EMPTY_SERVICE_ACKS;

    ServiceAck(std::int64_t ackId, std::int64_t logPosition, std::int64_t relevantId);

    std::int64_t ackId() const;
    std::int64_t logPosition() const;
    std::int64_t relevantId() const;

    static bool hasReached(
        std::int64_t logPosition,
        std::int64_t ackId,
        const std::vector<std::deque<ServiceAck>>& queues);

    static void removeHead(std::vector<std::deque<ServiceAck>>& queues);

    static std::vector<std::deque<ServiceAck>> newArrayOfQueues(std::int32_t serviceCount);

    std::string toString() const;

private:
    std::int64_t m_ackId;
    std::int64_t m_logPosition;
    std::int64_t m_relevantId;
};

// Implementation
const std::vector<ServiceAck> ServiceAck::EMPTY_SERVICE_ACKS;

inline ServiceAck::ServiceAck(std::int64_t ackId, std::int64_t logPosition, std::int64_t relevantId) :
    m_ackId(ackId),
    m_logPosition(logPosition),
    m_relevantId(relevantId)
{
}

inline std::int64_t ServiceAck::ackId() const
{
    return m_ackId;
}

inline std::int64_t ServiceAck::logPosition() const
{
    return m_logPosition;
}

inline std::int64_t ServiceAck::relevantId() const
{
    return m_relevantId;
}

inline bool ServiceAck::hasReached(
    std::int64_t logPosition,
    std::int64_t ackId,
    const std::vector<std::deque<ServiceAck>>& queues)
{
    for (std::size_t serviceId = 0; serviceId < queues.size(); serviceId++)
    {
        const auto& queue = queues[serviceId];
        if (queue.empty())
        {
            return false;
        }

        const ServiceAck& serviceAck = queue.front();
        if (serviceAck.m_ackId != ackId || serviceAck.m_logPosition != logPosition)
        {
            throw ClusterException(
                "ack out of sequence: expected [ackId=" + std::to_string(ackId) +
                ", logPosition=" + std::to_string(logPosition) + "] vs " +
                "received [ackId=" + std::to_string(serviceAck.m_ackId) +
                ", logPosition=" + std::to_string(serviceAck.m_logPosition) +
                ", relevantId=" + std::to_string(serviceAck.m_relevantId) +
                ", serviceId=" + std::to_string(serviceId) + "]", SOURCEINFO);
        }
    }

    return true;
}

inline void ServiceAck::removeHead(std::vector<std::deque<ServiceAck>>& queues)
{
    for (auto& queue : queues)
    {
        if (!queue.empty())
        {
            queue.pop_front();
        }
    }
}

inline std::vector<std::deque<ServiceAck>> ServiceAck::newArrayOfQueues(std::int32_t serviceCount)
{
    return std::vector<std::deque<ServiceAck>>(serviceCount);
}

inline std::string ServiceAck::toString() const
{
    std::ostringstream oss;
    oss << "ServiceAck{ackId=" << m_ackId
        << ", logPosition=" << m_logPosition
        << ", relevantId=" << m_relevantId << "}";
    return oss.str();
}

}}

