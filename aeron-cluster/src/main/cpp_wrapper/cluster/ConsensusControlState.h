#pragma once
#include <memory>
#include "ExclusivePublication.h"
#include "Subscription.h"

namespace aeron { namespace cluster
{

/**
 * The state needed to allow control of the consensus module.
 * 
 * This is a record object being passed to external entities.
 */
class ConsensusControlState
{
public:
    ConsensusControlState(
        std::shared_ptr<ExclusivePublication> logPublication,
        std::shared_ptr<Subscription> leaderLogSubscription,
        std::int64_t logRecordingId,
        std::int64_t leadershipTermId);

    /**
     * @return true iff we are the leader (and have the log publication).
     */
    bool isLeader() const;

    /**
     * @return log publication or null if follower.
     */
    std::shared_ptr<ExclusivePublication> logPublication() const;

    /**
     * @return log recording id.
     */
    std::int64_t logRecordingId() const;

    /**
     * @return leadership term id.
     */
    std::int64_t leadershipTermId() const;

    /**
     * @return a subscription to the log, joined at the log position of the election for a leader node, or null for a
     * follower.
     */
    std::shared_ptr<Subscription> leaderLogSubscription() const;

private:
    std::shared_ptr<ExclusivePublication> m_logPublication;
    std::shared_ptr<Subscription> m_leaderLogSubscription;
    std::int64_t m_logRecordingId;
    std::int64_t m_leadershipTermId;
};

}}

