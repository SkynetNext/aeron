#pragma once

#include <memory>
#include <string>
#include <vector>
#include <unordered_map>
#include <algorithm>
#include <cstdint>
#include "Aeron.h"
#include "ExclusivePublication.h"
#include "ChannelUri.h"
#include "CommonContext.h"
#include "archive/client/AeronArchive.h"
#include "../client/ClusterExceptions.h"
#include "../client/ClusterEvent.h"
#include "util/CloseHelper.h"
#include "util/Exceptions.h"

namespace aeron { namespace cluster {

using namespace aeron::util;

/**
 * Represents a member of the cluster that participates in consensus for storing state from the perspective
 * of any single member.
 */
class ClusterMember
{
public:
    static std::vector<ClusterMember> EMPTY_MEMBERS;

    /**
     * Construct a new member of the cluster.
     */
    ClusterMember(
        std::int32_t id,
        const std::string& ingressEndpoint,
        const std::string& consensusEndpoint,
        const std::string& logEndpoint,
        const std::string& catchupEndpoint,
        const std::string& archiveEndpoint,
        const std::string& endpoints);

    /**
     * Construct a new member of the cluster with additional endpoints.
     */
    ClusterMember(
        std::int32_t id,
        const std::string& ingressEndpoint,
        const std::string& consensusEndpoint,
        const std::string& logEndpoint,
        const std::string& catchupEndpoint,
        const std::string& archiveEndpoint,
        const std::string& archiveResponseEndpoint,
        const std::string& egressResponseEndpoint,
        const std::string& endpoints);

    /**
     * Reset the state of a cluster member, so it can be canvassed and reestablished.
     */
    void reset();

    ClusterMember& isLeader(bool isLeader);
    bool isLeader() const;

    ClusterMember& isBallotSent(bool isBallotSent);
    bool isBallotSent() const;

    ClusterMember& hasTerminated(bool hasTerminated);
    bool hasTerminated() const;

    ClusterMember& id(std::int32_t id);
    std::int32_t id() const;

    ClusterMember& vote(std::shared_ptr<bool> vote);
    std::shared_ptr<bool> vote() const;

    ClusterMember& leadershipTermId(std::int64_t leadershipTermId);
    std::int64_t leadershipTermId() const;

    ClusterMember& logPosition(std::int64_t logPosition);
    std::int64_t logPosition() const;

    ClusterMember& candidateTermId(std::int64_t candidateTermId);
    std::int64_t candidateTermId() const;

    ClusterMember& catchupReplaySessionId(std::int64_t replaySessionId);
    std::int64_t catchupReplaySessionId() const;

    ClusterMember& catchupReplayCorrelationId(std::int64_t correlationId);
    std::int64_t catchupReplayCorrelationId() const;

    ClusterMember& correlationId(std::int64_t correlationId);
    std::int64_t correlationId() const;

    ClusterMember& timeOfLastAppendPositionNs(std::int64_t timeNs);
    std::int64_t timeOfLastAppendPositionNs() const;

    std::string consensusEndpoint() const;
    std::string ingressEndpoint() const;
    std::string logEndpoint() const;
    std::string catchupEndpoint() const;
    std::string archiveEndpoint() const;
    std::string archiveResponseEndpoint() const;
    std::string egressResponseEndpoint() const;
    std::string endpoints() const;

    std::shared_ptr<ExclusivePublication> publication() const;
    void publication(std::shared_ptr<ExclusivePublication> publication);

    void closePublication(const exception_handler_t& errorHandler);

    /**
     * Parse the details for cluster members from a string.
     */
    static std::vector<ClusterMember> parse(const std::string& value);

    /**
     * Parse a string containing the endpoints for a cluster node.
     */
    static ClusterMember parseEndpoints(std::int32_t id, const std::string& endpoints);

    /**
     * Encode member endpoints from a cluster members array to a String.
     */
    static std::string encodeAsString(const std::vector<ClusterMember>& clusterMembers);

    /**
     * Copy votes from one array of members to another where the ids match.
     */
    static void copyVotes(const std::vector<ClusterMember>& srcMembers, std::vector<ClusterMember>& dstMembers);

    /**
     * Add the publications for sending consensus messages to the other members of the cluster.
     */
    static void addConsensusPublications(
        std::vector<ClusterMember>& members,
        const ClusterMember& thisMember,
        const std::string& channelTemplate,
        std::int32_t streamId,
        bool bindConsensusControl,
        std::shared_ptr<Aeron> aeron,
        const exception_handler_t& errorHandler);

    /**
     * Add an exclusive Publication for communicating to a member on the consensus channel.
     */
    static void addConsensusPublication(
        const ClusterMember& thisMember,
        ClusterMember& otherMember,
        const std::string& channelTemplate,
        std::int32_t streamId,
        bool bindConsensusControl,
        std::shared_ptr<Aeron> aeron,
        const exception_handler_t& errorHandler);

    /**
     * Try and add an exclusive Publication for communicating to a member on the consensus channel.
     */
    static void tryAddPublication(
        ClusterMember& member,
        std::int32_t streamId,
        std::shared_ptr<Aeron> aeron,
        const exception_handler_t& errorHandler);

    /**
     * Close the publications associated with members of the cluster used for the consensus protocol.
     */
    static void closeConsensusPublications(
        const exception_handler_t& errorHandler,
        std::vector<ClusterMember>& clusterMembers);

    /**
     * Populate map of ClusterMembers which can be looked up by id.
     */
    static void addClusterMemberIds(
        const std::vector<ClusterMember>& clusterMembers,
        std::unordered_map<std::int32_t, ClusterMember*>& clusterMemberByIdMap);

    /**
     * Check if the cluster leader has an active quorum of cluster followers.
     */
    static bool hasActiveQuorum(
        const std::vector<ClusterMember>& clusterMembers,
        std::int64_t nowNs,
        std::int64_t timeoutNs);

    /**
     * The threshold of clusters members required to achieve quorum given a count of cluster members.
     */
    static std::int32_t quorumThreshold(std::int32_t memberCount);

    /**
     * Calculate the position reached by a quorum of cluster members.
     */
    static std::int64_t quorumPosition(const std::vector<ClusterMember>& members, std::vector<std::int64_t>& rankedPositions);

    /**
     * Reset the log position of all the members to the provided value.
     */
    static void resetLogPositions(std::vector<ClusterMember>& clusterMembers, std::int64_t logPosition);

    /**
     * Has the voting members of a cluster arrived at provided position in their log.
     */
    static bool hasVotersAtPosition(
        const std::vector<ClusterMember>& clusterMembers,
        std::int64_t position,
        std::int64_t leadershipTermId);

    /**
     * Has a quorum of members of appended a position to their local log.
     */
    static bool hasQuorumAtPosition(
        const std::vector<ClusterMember>& clusterMembers,
        std::int64_t position,
        std::int64_t leadershipTermId);

    /**
     * Reset the state of all cluster members.
     */
    static void reset(std::vector<ClusterMember>& members);

    /**
     * Become a candidate by voting for yourself and resetting the other votes.
     */
    static void becomeCandidate(
        std::vector<ClusterMember>& members,
        std::int64_t candidateTermId,
        std::int32_t candidateMemberId);

    /**
     * Is a member considered unanimously to be leader after voting.
     */
    static bool isUnanimousLeader(
        const std::vector<ClusterMember>& clusterMembers,
        std::int64_t candidateTermId,
        std::int32_t gracefulClosedLeaderId);

    /**
     * Is this member considered leader by a quorum of members.
     */
    static bool isQuorumLeader(const std::vector<ClusterMember>& clusterMembers, std::int64_t candidateTermId);

    /**
     * Determine which member of a cluster this is and check endpoints.
     */
    static ClusterMember determineMember(
        const std::vector<ClusterMember>& clusterMembers,
        std::int32_t memberId,
        const std::string& memberEndpoints);

    /**
     * Check the member with the memberEndpoints.
     */
    static void validateMemberEndpoints(const ClusterMember& member, const std::string& memberEndpoints);

    /**
     * Are two cluster members using the same endpoints?
     */
    static bool areSameEndpoints(const ClusterMember& lhs, const ClusterMember& rhs);

    /**
     * Is the member considered a candidate by a unanimous view.
     */
    static bool isUnanimousCandidate(
        const std::vector<ClusterMember>& clusterMembers,
        const ClusterMember& candidate,
        std::int32_t gracefulClosedLeaderId);

    /**
     * Has the member achieved a quorum view to be a suitable candidate.
     */
    static bool isQuorumCandidate(const std::vector<ClusterMember>& clusterMembers, const ClusterMember& candidate);

    /**
     * Compare two log positions.
     */
    static std::int32_t compareLog(
        std::int64_t lhsLogLeadershipTermId,
        std::int64_t lhsLogPosition,
        std::int64_t rhsLogLeadershipTermId,
        std::int64_t rhsLogPosition);

    /**
     * Compare two members' logs.
     */
    static std::int32_t compareLog(const ClusterMember& lhs, const ClusterMember& rhs);

    /**
     * Find a ClusterMember with a given id.
     */
    static ClusterMember* findMember(std::vector<ClusterMember>& clusterMembers, std::int32_t memberId);

    /**
     * Create a string of ingress endpoints by member id.
     */
    static std::string ingressEndpoints(const std::vector<ClusterMember>& members);

    /**
     * Run through the list of cluster members and set the isLeader field.
     */
    static void setIsLeader(std::vector<ClusterMember>& clusterMembers, std::int32_t leaderMemberId);

    std::string toString() const;

private:
    static void setControlEndpoint(ChannelUri& channelUri, bool shouldBind, const std::string& endpoint);

    bool m_isBallotSent = false;
    bool m_isLeader = false;
    bool m_hasTerminated = false;
    std::int32_t m_id;
    std::int64_t m_leadershipTermId = 0; // NULL_VALUE
    std::int64_t m_candidateTermId = 0; // NULL_VALUE
    std::int64_t m_catchupReplaySessionId = 0; // NULL_VALUE
    std::int64_t m_catchupReplayCorrelationId = 0; // NULL_VALUE
    std::int64_t m_changeCorrelationId = 0; // NULL_VALUE
    std::int64_t m_logPosition = 0; // NULL_POSITION
    std::int64_t m_timeOfLastAppendPositionNs = 0; // NULL_VALUE
    std::shared_ptr<ExclusivePublication> m_publication;
    std::string m_consensusChannel;
    std::string m_consensusEndpoint;
    std::string m_ingressEndpoint;
    std::string m_logEndpoint;
    std::string m_catchupEndpoint;
    std::string m_archiveEndpoint;
    std::string m_archiveResponseEndpoint;
    std::string m_egressResponseEndpoint;
    std::string m_endpoints;
    std::shared_ptr<bool> m_vote;
};

// Inline implementations for header-only library

inline ClusterMember::ClusterMember(
    std::int32_t id,
    const std::string& ingressEndpoint,
    const std::string& consensusEndpoint,
    const std::string& logEndpoint,
    const std::string& catchupEndpoint,
    const std::string& archiveEndpoint,
    const std::string& endpoints) :
    ClusterMember(id, ingressEndpoint, consensusEndpoint, logEndpoint, catchupEndpoint,
        archiveEndpoint, "", "", endpoints)
{
}

inline ClusterMember::ClusterMember(
    std::int32_t id,
    const std::string& ingressEndpoint,
    const std::string& consensusEndpoint,
    const std::string& logEndpoint,
    const std::string& catchupEndpoint,
    const std::string& archiveEndpoint,
    const std::string& archiveResponseEndpoint,
    const std::string& egressResponseEndpoint,
    const std::string& endpoints) :
    m_id(id),
    m_ingressEndpoint(ingressEndpoint),
    m_consensusEndpoint(consensusEndpoint),
    m_logEndpoint(logEndpoint),
    m_catchupEndpoint(catchupEndpoint),
    m_archiveEndpoint(archiveEndpoint),
    m_archiveResponseEndpoint(archiveResponseEndpoint),
    m_egressResponseEndpoint(egressResponseEndpoint),
    m_endpoints(endpoints)
{
}

inline void ClusterMember::reset()
{
    m_isBallotSent = false;
    m_isLeader = false;
    m_hasTerminated = false;
    m_vote.reset();
    m_candidateTermId = 0; // NULL_VALUE
    m_leadershipTermId = 0; // NULL_VALUE
    m_logPosition = 0; // NULL_POSITION
}

inline ClusterMember& ClusterMember::isLeader(bool isLeader)
{
    m_isLeader = isLeader;
    return *this;
}

inline bool ClusterMember::isLeader() const
{
    return m_isLeader;
}

inline ClusterMember& ClusterMember::isBallotSent(bool isBallotSent)
{
    m_isBallotSent = isBallotSent;
    return *this;
}

inline bool ClusterMember::isBallotSent() const
{
    return m_isBallotSent;
}

inline ClusterMember& ClusterMember::hasTerminated(bool hasTerminated)
{
    m_hasTerminated = hasTerminated;
    return *this;
}

inline bool ClusterMember::hasTerminated() const
{
    return m_hasTerminated;
}

inline ClusterMember& ClusterMember::id(std::int32_t id)
{
    m_id = id;
    return *this;
}

inline std::int32_t ClusterMember::id() const
{
    return m_id;
}

inline ClusterMember& ClusterMember::vote(std::shared_ptr<bool> vote)
{
    m_vote = vote;
    return *this;
}

inline std::shared_ptr<bool> ClusterMember::vote() const
{
    return m_vote;
}

inline ClusterMember& ClusterMember::leadershipTermId(std::int64_t leadershipTermId)
{
    m_leadershipTermId = leadershipTermId;
    return *this;
}

inline std::int64_t ClusterMember::leadershipTermId() const
{
    return m_leadershipTermId;
}

inline ClusterMember& ClusterMember::logPosition(std::int64_t logPosition)
{
    m_logPosition = logPosition;
    return *this;
}

inline std::int64_t ClusterMember::logPosition() const
{
    return m_logPosition;
}

inline ClusterMember& ClusterMember::candidateTermId(std::int64_t candidateTermId)
{
    m_candidateTermId = candidateTermId;
    return *this;
}

inline std::int64_t ClusterMember::candidateTermId() const
{
    return m_candidateTermId;
}

inline ClusterMember& ClusterMember::catchupReplaySessionId(std::int64_t replaySessionId)
{
    m_catchupReplaySessionId = replaySessionId;
    return *this;
}

inline std::int64_t ClusterMember::catchupReplaySessionId() const
{
    return m_catchupReplaySessionId;
}

inline ClusterMember& ClusterMember::catchupReplayCorrelationId(std::int64_t correlationId)
{
    m_catchupReplayCorrelationId = correlationId;
    return *this;
}

inline std::int64_t ClusterMember::catchupReplayCorrelationId() const
{
    return m_catchupReplayCorrelationId;
}

inline ClusterMember& ClusterMember::correlationId(std::int64_t correlationId)
{
    m_changeCorrelationId = correlationId;
    return *this;
}

inline std::int64_t ClusterMember::correlationId() const
{
    return m_changeCorrelationId;
}

inline ClusterMember& ClusterMember::timeOfLastAppendPositionNs(std::int64_t timeNs)
{
    m_timeOfLastAppendPositionNs = timeNs;
    return *this;
}

inline std::int64_t ClusterMember::timeOfLastAppendPositionNs() const
{
    return m_timeOfLastAppendPositionNs;
}

inline std::string ClusterMember::consensusEndpoint() const
{
    return m_consensusEndpoint;
}

inline std::string ClusterMember::ingressEndpoint() const
{
    return m_ingressEndpoint;
}

inline std::string ClusterMember::logEndpoint() const
{
    return m_logEndpoint;
}

inline std::string ClusterMember::catchupEndpoint() const
{
    return m_catchupEndpoint;
}

inline std::string ClusterMember::archiveEndpoint() const
{
    return m_archiveEndpoint;
}

inline std::string ClusterMember::archiveResponseEndpoint() const
{
    return m_archiveResponseEndpoint;
}

inline std::string ClusterMember::egressResponseEndpoint() const
{
    return m_egressResponseEndpoint;
}

inline std::string ClusterMember::endpoints() const
{
    return m_endpoints;
}

inline std::shared_ptr<ExclusivePublication> ClusterMember::publication() const
{
    return m_publication;
}

inline void ClusterMember::publication(std::shared_ptr<ExclusivePublication> publication)
{
    m_publication = publication;
}

inline void ClusterMember::closePublication(const exception_handler_t& errorHandler)
{
    CloseHelper::close(errorHandler, m_publication);
    m_publication.reset();
}

inline std::int32_t ClusterMember::quorumThreshold(std::int32_t memberCount)
{
    return (memberCount >> 1) + 1;
}

inline std::int32_t ClusterMember::compareLog(
    std::int64_t lhsLogLeadershipTermId,
    std::int64_t lhsLogPosition,
    std::int64_t rhsLogLeadershipTermId,
    std::int64_t rhsLogPosition)
{
    if (lhsLogLeadershipTermId > rhsLogLeadershipTermId)
    {
        return 1;
    }
    else if (lhsLogLeadershipTermId < rhsLogLeadershipTermId)
    {
        return -1;
    }
    else if (lhsLogPosition > rhsLogPosition)
    {
        return 1;
    }
    else if (lhsLogPosition < rhsLogPosition)
    {
        return -1;
    }
    return 0;
}

inline std::int32_t ClusterMember::compareLog(const ClusterMember& lhs, const ClusterMember& rhs)
{
    return compareLog(lhs.m_leadershipTermId, lhs.m_logPosition, rhs.m_leadershipTermId, rhs.m_logPosition);
}

inline ClusterMember* ClusterMember::findMember(std::vector<ClusterMember>& clusterMembers, std::int32_t memberId)
{
    for (auto& member : clusterMembers)
    {
        if (memberId == member.m_id)
        {
            return &member;
        }
    }
    return nullptr;
}

inline void ClusterMember::setIsLeader(std::vector<ClusterMember>& clusterMembers, std::int32_t leaderMemberId)
{
    for (auto& clusterMember : clusterMembers)
    {
        clusterMember.isLeader(clusterMember.id() == leaderMemberId);
    }
}

inline void ClusterMember::resetLogPositions(std::vector<ClusterMember>& clusterMembers, std::int64_t logPosition)
{
    for (auto& member : clusterMembers)
    {
        member.logPosition(logPosition);
    }
}

inline void ClusterMember::reset(std::vector<ClusterMember>& members)
{
    for (auto& member : members)
    {
        member.reset();
    }
}

inline bool ClusterMember::areSameEndpoints(const ClusterMember& lhs, const ClusterMember& rhs)
{
    return lhs.m_ingressEndpoint == rhs.m_ingressEndpoint &&
        lhs.m_consensusEndpoint == rhs.m_consensusEndpoint &&
        lhs.m_logEndpoint == rhs.m_logEndpoint &&
        lhs.m_catchupEndpoint == rhs.m_catchupEndpoint &&
        lhs.m_archiveEndpoint == rhs.m_archiveEndpoint;
}

// Note: More complex static methods like parse, encodeAsString, addConsensusPublications, etc.
// would need full implementation. These are placeholders for now.

}}


