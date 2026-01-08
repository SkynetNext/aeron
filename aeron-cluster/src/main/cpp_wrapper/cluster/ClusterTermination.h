#pragma once
#include <vector>
#include "ClusterMember.h"
#include "client/ClusterEvent.h"
#include "util/Exceptions.h"

namespace aeron { namespace cluster
{
using namespace aeron::util;

class ConsensusPublisher; // Forward declaration

class ClusterTermination
{
public:
    ClusterTermination(std::int64_t deadlineNs, std::int32_t serviceCount);

    void deadlineNs(std::int64_t deadlineNs);

    bool canTerminate(const std::vector<ClusterMember>& members, std::int64_t nowNs);

    void onServicesTerminated();

    void terminationPosition(
        const exception_handler_t& errorHandler,
        ConsensusPublisher& consensusPublisher,
        const std::vector<ClusterMember>& members,
        const ClusterMember& thisMember,
        std::int64_t leadershipTermId,
        std::int64_t position);

private:
    std::int64_t m_deadlineNs;
    bool m_haveServicesTerminated;
};

// Implementation
inline ClusterTermination::ClusterTermination(std::int64_t deadlineNs, std::int32_t serviceCount) :
    m_deadlineNs(deadlineNs),
    m_haveServicesTerminated(serviceCount <= 0)
{
}

inline void ClusterTermination::deadlineNs(std::int64_t deadlineNs)
{
    m_deadlineNs = deadlineNs;
}

inline bool ClusterTermination::canTerminate(const std::vector<ClusterMember>& members, std::int64_t nowNs)
{
    if (m_haveServicesTerminated)
    {
        bool result = true;

        for (const auto& member : members)
        {
            if (!member.isLeader() && !member.hasTerminated())
            {
                result = false;
                break;
            }
        }

        return result || nowNs >= m_deadlineNs;
    }

    return false;
}

inline void ClusterTermination::onServicesTerminated()
{
    m_haveServicesTerminated = true;
}

inline void ClusterTermination::terminationPosition(
    const exception_handler_t& errorHandler,
    ConsensusPublisher& consensusPublisher,
    const std::vector<ClusterMember>& members,
    const ClusterMember& thisMember,
    std::int64_t leadershipTermId,
    std::int64_t position)
{
    for (auto& member : const_cast<std::vector<ClusterMember>&>(members))
    {
        member.hasTerminated(false);

        if (member.id() != thisMember.id())
        {
            std::shared_ptr<ExclusivePublication> publication = member.publication();
            if (publication)
            {
                if (!consensusPublisher.terminationPosition(publication, leadershipTermId, position))
                {
                    ClusterEvent event(
                        "failed to send termination position to member=" + std::to_string(member.id()),
                        ExceptionCategory::EXCEPTION_CATEGORY_WARN);
                    errorHandler(std::make_exception_ptr(event));
                }
            }
        }
    }
}

}}

