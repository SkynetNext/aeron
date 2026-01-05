#pragma once
#include <string>
#include <vector>
#include "ClusterMember.h"
#include "Aeron.h"

namespace aeron { namespace cluster
{

/**
 * Detail for the cluster membership from the perspective of a given member.
 */
class ClusterMembership
{
public:
    /**
     * Member id that the query is run against.
     */
    std::int32_t memberId = aeron::NULL_VALUE;

    /**
     * Current leader id from the perspective of the member doing the query.
     */
    std::int32_t leaderMemberId = aeron::NULL_VALUE;

    /**
     * Current time in nanoseconds when the query was run.
     */
    std::int64_t currentTimeNs = aeron::NULL_VALUE;

    /**
     * List of active cluster members encoded to a String.
     */
    std::string activeMembersStr;

    /**
     * Current active members of a cluster.
     */
    std::vector<ClusterMember> activeMembers;
};

}}

