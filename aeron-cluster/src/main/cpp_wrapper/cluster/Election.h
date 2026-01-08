#pragma once

#include <memory>
#include <vector>
#include <unordered_map>
#include <string>
#include <algorithm>
#include <cstdint>
#include "Aeron.h"
#include "ChannelUri.h"
#include "Image.h"
#include "Subscription.h"
#include "client/archive/AeronArchive.h"  // For RecordingSignal
#include "client/ClusterExceptions.h"
#include "client/ClusterEvent.h"
#include "service/Cluster.h"
#include "util/Exceptions.h"
#include "util/CloseHelper.h"
#include "concurrent/status/StatusIndicatorReader.h"  // For ChannelEndpointStatus
#include "concurrent/AgentTerminationException.h"
#include "ElectionState.h"
#include "ClusterMember.h"
#include "ConsensusPublisher.h"
// #include "ConsensusModule.h" // Not yet implemented, using forward declaration
#include "ConsensusModuleAgent.h"
#include "LogReplay.h"
#include "RecordingReplication.h"
#include "NodeStateFile.h"
#include "RecordingLog.h"
// CommonContext.h not needed - constants are in Context.h or defined inline
#include "client/archive/AeronArchive.h"

namespace aeron { namespace cluster {

using namespace aeron::archive::client;  // For RecordingSignal
using namespace aeron::concurrent::status;  // For ChannelEndpointStatus
using namespace aeron::util;

// Forward declarations
class ConsensusModule;
class ConsensusModuleAgent;
class RecordingLog;

/**
 * Election process to determine a new cluster leader and catch up followers.
 */
class Election
{
public:
    Election(
        bool isNodeStartup,
        std::int32_t gracefulClosedLeaderId,
        std::int64_t leadershipTermId,
        std::int64_t termBaseLogPosition,
        std::int64_t logPosition,
        std::int64_t appendPosition,
        std::vector<ClusterMember>& clusterMembers,
        std::unordered_map<std::int32_t, ClusterMember*>& clusterMemberByIdMap,
        ClusterMember& thisMember,
        ConsensusPublisher& consensusPublisher,
        ConsensusModuleContext& ctx,
        ConsensusModuleAgent& consensusModuleAgent);

    ClusterMember* leader();
    std::int32_t logSessionId() const;
    std::int64_t leadershipTermId() const;
    std::int64_t logPosition() const;
    bool isLeaderStartup() const;
    std::int32_t thisMemberId() const;

    std::int32_t doWork(std::int64_t nowNs);

    void handleError(std::int64_t nowNs, const std::exception& ex);

    void onRecordingSignal(
        std::int64_t correlationId,
        std::int64_t recordingId,
        std::int64_t position,
        RecordingSignal signal);

    void onCanvassPosition(
        std::int64_t logLeadershipTermId,
        std::int64_t logPosition,
        std::int64_t leadershipTermId,
        std::int32_t followerMemberId,
        std::int32_t protocolVersion);

    void onRequestVote(
        std::int64_t logLeadershipTermId,
        std::int64_t logPosition,
        std::int64_t candidateTermId,
        std::int32_t candidateId,
        std::int32_t protocolVersion);

    void onVote(
        std::int64_t candidateTermId,
        std::int64_t logLeadershipTermId,
        std::int64_t logPosition,
        std::int32_t candidateMemberId,
        std::int32_t followerMemberId,
        bool vote);

    void onNewLeadershipTerm(
        std::int64_t logLeadershipTermId,
        std::int64_t nextLeadershipTermId,
        std::int64_t nextTermBaseLogPosition,
        std::int64_t nextLogPosition,
        std::int64_t leadershipTermId,
        std::int64_t termBaseLogPosition,
        std::int64_t logPosition,
        std::int64_t commitPosition,
        std::int64_t leaderRecordingId,
        std::int64_t timestamp,
        std::int32_t leaderMemberId,
        std::int32_t logSessionId,
        bool isStartup);

    void onAppendPosition(
        std::int64_t leadershipTermId,
        std::int64_t logPosition,
        std::int32_t followerMemberId,
        std::int16_t flags);

    void onCommitPosition(
        std::int64_t leadershipTermId,
        std::int64_t logPosition,
        std::int32_t leaderMemberId);

    void onReplayNewLeadershipTermEvent(
        std::int64_t leadershipTermId,
        std::int64_t logPosition,
        std::int64_t timestamp,
        std::int64_t termBaseLogPosition);

    void onTruncateLogEntry(
        std::int32_t memberId,
        ElectionState state,
        std::int64_t logLeadershipTermId,
        std::int64_t leadershipTermId,
        std::int64_t candidateTermId,
        std::int64_t commitPosition,
        std::int64_t logPosition,
        std::int64_t appendPosition,
        std::int64_t oldPosition,
        std::int64_t newPosition);

    std::int64_t notifiedCommitPosition() const;

    static void ensureRecordingLogCoherent(
        ConsensusModuleContext& ctx,
        std::int64_t recordingId,
        std::int64_t initialLogLeadershipTermId,
        std::int64_t initialTermBaseLogPosition,
        std::int64_t leadershipTermId,
        std::int64_t termBaseLogPosition,
        std::int64_t logPosition,
        std::int64_t nowNs);

    std::string toString() const;

private:
    std::int32_t init(std::int64_t nowNs);
    std::int32_t canvass(std::int64_t nowNs);
    std::int32_t nominate(std::int64_t nowNs);
    std::int32_t candidateBallot(std::int64_t nowNs);
    std::int32_t followerBallot(std::int64_t nowNs);
    std::int32_t leaderLogReplication(std::int64_t nowNs);
    std::int32_t leaderReplay(std::int64_t nowNs);
    std::int32_t leaderInit(std::int64_t nowNs);
    std::int32_t leaderReady(std::int64_t nowNs);
    std::int32_t followerLogReplication(std::int64_t nowNs);
    std::int32_t followerReplay(std::int64_t nowNs);
    std::int32_t followerCatchupInit(std::int64_t nowNs);
    std::int32_t followerCatchupAwait(std::int64_t nowNs);
    std::int32_t followerCatchup(std::int64_t nowNs);
    std::int32_t followerLogInit(std::int64_t nowNs);
    std::int32_t followerLogAwait(std::int64_t nowNs);
    std::int32_t followerReady(std::int64_t nowNs);

    void placeVote(std::int64_t candidateTermId, std::int32_t candidateId, bool vote);
    std::int32_t publishNewLeadershipTermOnInterval(std::int64_t quorumPosition, std::int64_t nowNs);
    std::int32_t publishCommitPositionOnInterval(std::int64_t quorumPosition, std::int64_t nowNs);
    void publishCanvassPosition();
    void publishNewLeadershipTerm(std::int64_t quorumPosition, std::int64_t timestamp);
    void publishNewLeadershipTerm(
        ClusterMember& member,
        std::int64_t logLeadershipTermId,
        std::int64_t quorumPosition,
        std::int64_t timestamp);
    std::int32_t publishFollowerReplicationPosition(std::int64_t nowNs);
    std::int32_t publishFollowerAppendPosition(std::int64_t nowNs);
    bool sendCatchupPosition(const std::string& catchupEndpoint);
    void addCatchupLogDestination();
    void addLiveLogDestination();
    std::shared_ptr<Subscription> addFollowerSubscription();
    void state(ElectionState newState, std::int64_t nowNs, const std::string& reason);
    void stopCatchup();
    void resetMembers();
    void stopReplay();
    void stopLogReplication();
    void ensureRecordingLogCoherent(
        std::int64_t leadershipTermId,
        std::int64_t termBaseLogPosition,
        std::int64_t logPosition,
        std::int64_t nowNs);
    void updateRecordingLog(std::int64_t nowNs);
    void updateRecordingLogForReplication(
        std::int64_t leadershipTermId,
        std::int64_t termBaseLogPosition,
        std::int64_t logPosition,
        std::int64_t nowNs);
    void verifyLogJoinPosition(const std::string& state, std::int64_t joinPosition);
    bool hasUpdateIntervalExpired(std::int64_t nowNs, std::int64_t intervalNs);
    bool hasIntervalExpired(
        std::int64_t nowNs,
        std::int64_t previousTimestampForIntervalNs,
        std::int64_t intervalNs);
    void logStateChange(
        std::int32_t memberId,
        ElectionState oldState,
        ElectionState newState,
        std::int32_t leaderId,
        std::int64_t candidateTermId,
        std::int64_t leadershipTermId,
        std::int64_t logPosition,
        std::int64_t logLeadershipTermId,
        std::int64_t appendPosition,
        std::int64_t catchupPosition,
        const std::string& reason);
    void prepareForNewLeadership(std::int64_t nowNs);

    std::vector<ClusterMember>& m_clusterMembers;
    ClusterMember& m_thisMember;
    std::unordered_map<std::int32_t, ClusterMember*>& m_clusterMemberByIdMap;
    ConsensusPublisher& m_consensusPublisher;
    ConsensusModuleContext& m_ctx;
    ConsensusModuleAgent& m_consensusModuleAgent;
    std::int64_t m_initialLogLeadershipTermId;
    std::int64_t m_initialTimeOfLastUpdateNs;
    std::int64_t m_initialTermBaseLogPosition;
    bool m_isNodeStartup;
    std::int64_t m_timeOfLastStateChangeNs = 0;
    std::int64_t m_timeOfLastUpdateNs = 0;
    std::int64_t m_timeOfLastCommitPositionUpdateNs = 0;
    std::int64_t m_nominationDeadlineNs = 0;
    std::int64_t m_logPosition = 0;
    std::int64_t m_appendPosition = 0;
    std::int64_t m_notifiedCommitPosition = 0;
    std::int64_t m_catchupJoinPosition = 0; // NULL_POSITION
    std::int64_t m_replicationLeadershipTermId = 0; // NULL_VALUE
    std::int64_t m_replicationStopPosition = 0; // NULL_POSITION
    std::int64_t m_replicationDeadlineNs = 0;
    std::int64_t m_replicationTermBaseLogPosition = 0;
    std::int64_t m_leaderRecordingId = 0; // NULL_VALUE
    std::int64_t m_leadershipTermId = 0;
    std::int64_t m_logLeadershipTermId = 0;
    std::int64_t m_candidateTermId = 0;
    std::int64_t m_lastPublishedCommitPosition = 0;
    std::int64_t m_lastPublishedAppendPosition = 0;
    std::int32_t m_logSessionId = 0; // NULL_SESSION_ID (0 is the default)
    std::int32_t m_gracefulClosedLeaderId = 0;
    bool m_isFirstInit = true;
    bool m_isLeaderStartup = false;
    bool m_isExtendedCanvass = false;
    ClusterMember* m_leaderMember = nullptr;
    ElectionState m_state = ElectionState::INIT;
    std::shared_ptr<Subscription> m_logSubscription;
    std::unique_ptr<LogReplay> m_logReplay;
    std::unique_ptr<RecordingReplication> m_logReplication;
};

// Implementation
inline Election::Election(
    bool isNodeStartup,
    std::int32_t gracefulClosedLeaderId,
    std::int64_t leadershipTermId,
    std::int64_t termBaseLogPosition,
    std::int64_t logPosition,
    std::int64_t appendPosition,
    std::vector<ClusterMember>& clusterMembers,
    std::unordered_map<std::int32_t, ClusterMember*>& clusterMemberByIdMap,
    ClusterMember& thisMember,
    ConsensusPublisher& consensusPublisher,
    ConsensusModule::Context& ctx,
    ConsensusModuleAgent& consensusModuleAgent) :
    m_clusterMembers(clusterMembers),
    m_thisMember(thisMember),
    m_clusterMemberByIdMap(clusterMemberByIdMap),
    m_consensusPublisher(consensusPublisher),
    m_ctx(ctx),
    m_consensusModuleAgent(consensusModuleAgent),
    m_initialLogLeadershipTermId(leadershipTermId),
    m_initialTermBaseLogPosition(termBaseLogPosition),
    m_isNodeStartup(isNodeStartup),
    m_logPosition(logPosition),
    m_appendPosition(appendPosition),
    m_logLeadershipTermId(leadershipTermId),
    m_leadershipTermId(leadershipTermId),
    m_candidateTermId(leadershipTermId),
    m_gracefulClosedLeaderId(gracefulClosedLeaderId),
    m_isExtendedCanvass(isNodeStartup)
{
    const std::int64_t nowNs = ctx.clusterClock()->timeNanos();
    m_initialTimeOfLastUpdateNs = nowNs - (24LL * 60 * 60 * 1000 * 1000 * 1000); // 1 day in nanoseconds
    m_timeOfLastUpdateNs = m_initialTimeOfLastUpdateNs;
    m_timeOfLastCommitPositionUpdateNs = m_initialTimeOfLastUpdateNs;

    ctx.electionStateCounter()->setRelease(static_cast<std::int64_t>(ElectionState::INIT));
    ctx.electionCounter()->incrementRelease();

    if (clusterMembers.size() == 1 && thisMember.id() == clusterMembers[0].id())
    {
        m_candidateTermId = std::max(leadershipTermId + 1, ctx.nodeStateFile()->candidateTerm()->candidateTermId() + 1);
        m_leadershipTermId = m_candidateTermId;
        m_leaderMember = &thisMember;
        ctx.nodeStateFile()->updateCandidateTermId(m_candidateTermId, logPosition, ctx.epochClock()->time());
        state(ElectionState::LEADER_LOG_REPLICATION, nowNs, "");
    }
}

inline ClusterMember* Election::leader()
{
    return m_leaderMember;
}

inline std::int32_t Election::logSessionId() const
{
    return m_logSessionId;
}

inline std::int64_t Election::leadershipTermId() const
{
    return m_leadershipTermId;
}

inline std::int64_t Election::logPosition() const
{
    return m_logPosition;
}

inline bool Election::isLeaderStartup() const
{
    return m_isLeaderStartup;
}

inline std::int32_t Election::thisMemberId() const
{
    return m_thisMember.id();
}

inline std::int32_t Election::doWork(std::int64_t nowNs)
{
    std::int32_t workCount = 0;

    switch (m_state)
    {
        case ElectionState::INIT:
            workCount += init(nowNs);
            break;

        case ElectionState::CANVASS:
            workCount += canvass(nowNs);
            break;

        case ElectionState::NOMINATE:
            workCount += nominate(nowNs);
            break;

        case ElectionState::CANDIDATE_BALLOT:
            workCount += candidateBallot(nowNs);
            break;

        case ElectionState::FOLLOWER_BALLOT:
            workCount += followerBallot(nowNs);
            break;

        case ElectionState::LEADER_LOG_REPLICATION:
            workCount += leaderLogReplication(nowNs);
            break;

        case ElectionState::LEADER_REPLAY:
            workCount += leaderReplay(nowNs);
            break;

        case ElectionState::LEADER_INIT:
            workCount += leaderInit(nowNs);
            break;

        case ElectionState::LEADER_READY:
            workCount += leaderReady(nowNs);
            break;

        case ElectionState::FOLLOWER_LOG_REPLICATION:
            workCount += followerLogReplication(nowNs);
            break;

        case ElectionState::FOLLOWER_REPLAY:
            workCount += followerReplay(nowNs);
            break;

        case ElectionState::FOLLOWER_CATCHUP_INIT:
            workCount += followerCatchupInit(nowNs);
            break;

        case ElectionState::FOLLOWER_CATCHUP_AWAIT:
            workCount += followerCatchupAwait(nowNs);
            break;

        case ElectionState::FOLLOWER_CATCHUP:
            workCount += followerCatchup(nowNs);
            break;

        case ElectionState::FOLLOWER_LOG_INIT:
            workCount += followerLogInit(nowNs);
            break;

        case ElectionState::FOLLOWER_LOG_AWAIT:
            workCount += followerLogAwait(nowNs);
            break;

        case ElectionState::FOLLOWER_READY:
            workCount += followerReady(nowNs);
            break;

        case ElectionState::CLOSED:
            break;
    }

    return workCount;
}

inline void Election::handleError(std::int64_t nowNs, const std::exception& ex)
{
    m_ctx.countedErrorHandler()->onError(ex);
    m_logPosition = m_ctx.commitPositionCounter()->get();
    state(ElectionState::INIT, nowNs, ex.what());

    // Re-throw AgentTerminationException or InterruptedException
    // In C++, we check for specific exception types
    // TODO: Implement proper exception type checking
}

// Implementation moved to ConsensusModuleAgent.h to resolve circular dependency

inline std::int64_t Election::notifiedCommitPosition() const
{
    return m_notifiedCommitPosition;
}

// Implementation moved to ConsensusModuleAgent.h to resolve circular dependency

// Implementation moved to ConsensusModuleAgent.h to resolve circular dependency

inline void Election::resetMembers()
{
    ClusterMember::reset(m_clusterMembers);
}

inline void Election::stopReplay()
{
    if (m_logReplay)
    {
        m_logReplay->close();
        m_logReplay.reset();
    }
}

inline void Election::stopLogReplication()
{
    if (m_logReplication)
    {
        m_logReplication->close();
        m_logReplication.reset();
    }
}

inline bool Election::hasUpdateIntervalExpired(std::int64_t nowNs, std::int64_t intervalNs)
{
    return hasIntervalExpired(nowNs, m_timeOfLastUpdateNs, intervalNs);
}

inline bool Election::hasIntervalExpired(
    std::int64_t nowNs,
    std::int64_t previousTimestampForIntervalNs,
    std::int64_t intervalNs)
{
    return nowNs >= (previousTimestampForIntervalNs + intervalNs);
}

inline std::string Election::toString() const
{
    return "Election{state=" + std::to_string(static_cast<std::int32_t>(m_state)) +
        ", leadershipTermId=" + std::to_string(m_leadershipTermId) +
        ", logPosition=" + std::to_string(m_logPosition) +
        ", appendPosition=" + std::to_string(m_appendPosition) +
        "}";
}

// Helper function for compareLog
namespace {
    inline std::int32_t compareLog(
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
}

inline std::int32_t Election::init(std::int64_t nowNs)
{
    if (m_isFirstInit)
    {
        m_isFirstInit = false;
        if (!m_isNodeStartup)
        {
            prepareForNewLeadership(nowNs);
        }
    }
    else
    {
        stopLogReplication();
        stopCatchup();
        prepareForNewLeadership(nowNs);
        m_logSessionId = 0;  // NULL_SESSION_ID
        stopReplay();

        if (m_logSubscription)
        {
            m_logSubscription.reset();
            m_consensusModuleAgent.awaitLocalSocketsClosed(m_logSubscription->registrationId());
            m_logSubscription.reset();
        }
    }

    m_notifiedCommitPosition = 0;
    m_candidateTermId = std::max(
        m_ctx.nodeStateFile()->candidateTerm()->candidateTermId(),
        m_leadershipTermId);

    if (m_clusterMembers.size() == 1 && m_thisMember.id() == m_clusterMembers[0].id())
    {
        state(ElectionState::LEADER_LOG_REPLICATION, nowNs, "");
    }
    else
    {
        state(ElectionState::CANVASS, nowNs, "");
    }

    return 1;
}

inline std::int32_t Election::canvass(std::int64_t nowNs)
{
    std::int32_t workCount = 0;
    const std::int64_t deadlineNs = m_isExtendedCanvass ?
        m_timeOfLastStateChangeNs + m_ctx.startupCanvassTimeoutNs() :
        m_consensusModuleAgent.timeOfLastLeaderUpdateNs() + m_ctx.leaderHeartbeatTimeoutNs();

    if (hasUpdateIntervalExpired(nowNs, m_ctx.electionStatusIntervalNs()))
    {
        m_timeOfLastUpdateNs = nowNs;
        publishCanvassPosition();
        workCount++;
    }

    if (m_ctx.appointedLeaderId() != aeron::NULL_VALUE && m_ctx.appointedLeaderId() != m_thisMember.id())
    {
        return workCount;
    }

    // TODO: Implement ClusterMember::isUnanimousCandidate and isQuorumCandidate
    // For now, use a simplified check
    bool isUnanimous = true;
    bool isQuorum = false;
    std::int32_t possibleVotes = 0;

    for (auto& member : m_clusterMembers)
    {
        if (member.id() == m_gracefulClosedLeaderId)
        {
            continue;
        }

        if (AeronArchive::NULL_POSITION == member.logPosition() ||
            compareLog(m_thisMember.leadershipTermId(), m_appendPosition,
                      member.leadershipTermId(), member.logPosition()) < 0)
        {
            isUnanimous = false;
            continue;
        }

        possibleVotes++;
    }

    const std::int32_t quorumThreshold = (static_cast<std::int32_t>(m_clusterMembers.size()) / 2) + 1;
    isQuorum = possibleVotes >= quorumThreshold;

    if (isUnanimous || (nowNs >= deadlineNs && isQuorum))
    {
        const double delayFactor = static_cast<double>(std::rand()) / RAND_MAX;
        const std::int64_t delayNs = static_cast<std::int64_t>(delayFactor * (m_ctx.electionTimeoutNs() >> 1));
        m_nominationDeadlineNs = nowNs + delayNs;
        state(ElectionState::NOMINATE, nowNs, "");
        workCount++;
    }

    return workCount;
}

inline std::int32_t Election::nominate(std::int64_t nowNs)
{
    if (nowNs >= m_nominationDeadlineNs)
    {
        m_candidateTermId = m_ctx.nodeStateFile()->proposeMaxCandidateTermId(
            m_candidateTermId + 1, m_logPosition, m_ctx.epochClock()->time());
        
        // TODO: Implement ClusterMember::becomeCandidate
        // For now, update this member's candidate term
        m_thisMember.candidateTermId(m_candidateTermId);
        
        state(ElectionState::CANDIDATE_BALLOT, nowNs, "");
        return 1;
    }
    else if (hasUpdateIntervalExpired(nowNs, m_ctx.electionStatusIntervalNs()))
    {
        m_timeOfLastUpdateNs = nowNs;
        publishCanvassPosition();
        return 1;
    }

    return 0;
}

inline std::int32_t Election::candidateBallot(std::int64_t nowNs)
{
    std::int32_t workCount = 0;

    // TODO: Implement ClusterMember::isUnanimousLeader and isQuorumLeader
    // For now, use a simplified check
    bool isUnanimous = true;
    bool isQuorum = false;
    std::int32_t votes = 0;

    for (auto& member : m_clusterMembers)
    {
        if (member.id() == m_gracefulClosedLeaderId)
        {
            continue;
        }

        if (member.candidateTermId() != m_candidateTermId || !member.vote())
        {
            isUnanimous = false;
            continue;
        }

        votes++;
    }

    const std::int32_t quorumThreshold = (static_cast<std::int32_t>(m_clusterMembers.size()) / 2) + 1;
    isQuorum = votes >= quorumThreshold;

    if (isUnanimous)
    {
        m_leaderMember = &m_thisMember;
        m_leadershipTermId = m_candidateTermId;
        state(ElectionState::LEADER_LOG_REPLICATION, nowNs, "");
        workCount++;
    }
    else if (nowNs >= (m_timeOfLastStateChangeNs + m_ctx.electionTimeoutNs()))
    {
        if (isQuorum)
        {
            m_leaderMember = &m_thisMember;
            m_leadershipTermId = m_candidateTermId;
            state(ElectionState::LEADER_LOG_REPLICATION, nowNs, "");
        }
        else
        {
            state(ElectionState::CANVASS, nowNs, "");
        }
        workCount++;
    }
    else
    {
        for (auto& member : m_clusterMembers)
        {
            if (!member.isBallotSent())
            {
                workCount++;
                const bool sent = m_consensusPublisher.requestVote(
                    member.publication(),
                    m_logLeadershipTermId,
                    m_appendPosition,
                    m_candidateTermId,
                    m_thisMember.id());
                member.isBallotSent(sent);
            }
        }
    }

    return workCount;
}

inline std::int32_t Election::followerBallot(std::int64_t nowNs)
{
    std::int32_t workCount = 0;

    if (nowNs >= (m_timeOfLastStateChangeNs + m_ctx.electionTimeoutNs()))
    {
        state(ElectionState::CANVASS, nowNs, "");
        workCount++;
    }

    return workCount;
}

inline void Election::publishCanvassPosition()
{
    for (auto& member : m_clusterMembers)
    {
        if (member.id() != m_thisMember.id())
        {
            if (!member.publication())
            {
                // TODO: Implement ClusterMember::tryAddPublication
                // For now, skip if publication is not available
                continue;
            }

            m_consensusPublisher.canvassPosition(
                member.publication(),
                m_logLeadershipTermId,
                m_appendPosition,
                m_leadershipTermId,
                m_thisMember.id());
        }
    }
}

inline void Election::placeVote(std::int64_t candidateTermId, std::int32_t candidateId, bool vote)
{
    auto it = m_clusterMemberByIdMap.find(candidateId);
    if (it != m_clusterMemberByIdMap.end())
    {
        ClusterMember* candidate = it->second;
        m_consensusPublisher.placeVote(
            candidate->publication(),
            candidateTermId,
            m_logLeadershipTermId,
            m_appendPosition,
            candidateId,
            m_thisMember.id(),
            vote);
    }
}

inline void Election::prepareForNewLeadership(std::int64_t nowNs)
{
    const std::int64_t lastAppendPosition = m_consensusModuleAgent.prepareForNewLeadership(m_logPosition, nowNs);
    if (AeronArchive::NULL_POSITION != lastAppendPosition)
    {
        m_appendPosition = lastAppendPosition;
    }
}

inline void Election::verifyLogJoinPosition(const std::string& state, std::int64_t joinPosition)
{
    if (joinPosition != m_logPosition)
    {
        const std::string inequality = joinPosition < m_logPosition ? " less " : " greater ";
        throw ClusterEvent(
            state + " - joinPosition=" + std::to_string(joinPosition) +
            inequality + "than logPosition=" + std::to_string(m_logPosition),
            SOURCEINFO);
    }
}

inline void Election::logStateChange(
    std::int32_t memberId,
    ElectionState oldState,
    ElectionState newState,
    std::int32_t leaderId,
    std::int64_t candidateTermId,
    std::int64_t leadershipTermId,
    std::int64_t logPosition,
    std::int64_t logLeadershipTermId,
    std::int64_t appendPosition,
    std::int64_t catchupPosition,
    const std::string& reason)
{
    // Log state change - in Java this is commented out, so we'll leave it empty for now
    // Can be implemented with a logger if needed
}

inline void Election::ensureRecordingLogCoherent(
    std::int64_t leadershipTermId,
    std::int64_t termBaseLogPosition,
    std::int64_t logPosition,
    std::int64_t nowNs)
{
    ensureRecordingLogCoherent(
        m_ctx,
        m_consensusModuleAgent.logRecordingId(),
        m_initialLogLeadershipTermId,
        m_initialTermBaseLogPosition,
        leadershipTermId,
        termBaseLogPosition,
        logPosition,
        nowNs);
}

inline void Election::ensureRecordingLogCoherent(
    ConsensusModule::Context& ctx,
    std::int64_t recordingId,
    std::int64_t initialLogLeadershipTermId,
    std::int64_t initialTermBaseLogPosition,
    std::int64_t leadershipTermId,
    std::int64_t termBaseLogPosition,
    std::int64_t logPosition,
    std::int64_t nowNs)
{
    if (aeron::NULL_VALUE == recordingId)
    {
        return;
    }

    const std::int64_t timestamp = ctx.clusterClock()->convertToTimeUnit(nowNs);
    auto recordingLog = ctx.recordingLog();

    recordingLog->ensureCoherent(
        recordingId,
        initialLogLeadershipTermId,
        initialTermBaseLogPosition,
        leadershipTermId,
        (aeron::NULL_VALUE != termBaseLogPosition) ? termBaseLogPosition : initialTermBaseLogPosition,
        logPosition,
        nowNs,
        timestamp,
        ctx.fileSyncLevel());
}

inline void Election::updateRecordingLog(std::int64_t nowNs)
{
    ensureRecordingLogCoherent(m_leadershipTermId, m_logPosition, AeronArchive::NULL_POSITION, nowNs);
    m_logLeadershipTermId = m_leadershipTermId;
}

inline void Election::updateRecordingLogForReplication(
    std::int64_t leadershipTermId,
    std::int64_t termBaseLogPosition,
    std::int64_t logPosition,
    std::int64_t nowNs)
{
    ensureRecordingLogCoherent(leadershipTermId, termBaseLogPosition, logPosition, nowNs);
    m_logLeadershipTermId = leadershipTermId;
}

inline void Election::onCanvassPosition(
    std::int64_t logLeadershipTermId,
    std::int64_t logPosition,
    std::int64_t leadershipTermId,
    std::int32_t followerMemberId,
    std::int32_t protocolVersion)
{
    if (ElectionState::INIT == m_state)
    {
        return;
    }

    if (followerMemberId == m_gracefulClosedLeaderId)
    {
        m_gracefulClosedLeaderId = aeron::NULL_VALUE;
    }

    auto it = m_clusterMemberByIdMap.find(followerMemberId);
    if (it != m_clusterMemberByIdMap.end() && m_thisMember.id() != followerMemberId)
    {
        ClusterMember* follower = it->second;
        follower->leadershipTermId(logLeadershipTermId).logPosition(logPosition);

        if (logLeadershipTermId < m_leadershipTermId)
        {
            if (service::Role::LEADER == m_consensusModuleAgent.role())
            {
                publishNewLeadershipTerm(
                    *follower,
                    logLeadershipTermId,
                    m_consensusModuleAgent.quorumPosition(),
                    m_ctx.clusterClock()->time());
            }
        }
        else if (logLeadershipTermId > m_leadershipTermId)
        {
            switch (m_state)
            {
                case ElectionState::LEADER_LOG_REPLICATION:
                case ElectionState::LEADER_READY:
                    throw ClusterEvent("potential new election in progress", SOURCEINFO);

                default:
                    break;
            }
        }
    }
}

inline void Election::onRequestVote(
    std::int64_t logLeadershipTermId,
    std::int64_t logPosition,
    std::int64_t candidateTermId,
    std::int32_t candidateId,
    std::int32_t protocolVersion)
{
    if (ElectionState::INIT == m_state)
    {
        return;
    }

    if (candidateId == m_thisMember.id())
    {
        return;
    }

    if (candidateTermId <= m_candidateTermId)
    {
        placeVote(candidateTermId, candidateId, false);
    }
    else if (compareLog(m_logLeadershipTermId, m_appendPosition, logLeadershipTermId, logPosition) > 0)
    {
        m_candidateTermId = m_ctx.nodeStateFile()->proposeMaxCandidateTermId(
            candidateTermId, logPosition, m_ctx.epochClock()->time());
        placeVote(candidateTermId, candidateId, false);

        auto it = m_clusterMemberByIdMap.find(candidateId);
        if (it != m_clusterMemberByIdMap.end() &&
            service::Cluster::Role::LEADER == m_consensusModuleAgent.role())
        {
            ClusterMember* candidateMember = it->second;
            publishNewLeadershipTerm(
                *candidateMember,
                logLeadershipTermId,
                m_consensusModuleAgent.quorumPosition(),
                m_ctx.clusterClock()->time());
        }
    }
    else if (ElectionState::CANVASS == m_state ||
             ElectionState::NOMINATE == m_state ||
             ElectionState::CANDIDATE_BALLOT == m_state ||
             ElectionState::FOLLOWER_BALLOT == m_state)
    {
        m_candidateTermId = m_ctx.nodeStateFile()->proposeMaxCandidateTermId(
            candidateTermId, logPosition, m_ctx.epochClock()->time());
        placeVote(candidateTermId, candidateId, true);
        state(ElectionState::FOLLOWER_BALLOT, m_ctx.clusterClock()->timeNanos(), "");
    }
}

inline void Election::onVote(
    std::int64_t candidateTermId,
    std::int64_t logLeadershipTermId,
    std::int64_t logPosition,
    std::int32_t candidateMemberId,
    std::int32_t followerMemberId,
    bool vote)
{
    if (ElectionState::INIT == m_state)
    {
        return;
    }

    if (ElectionState::CANDIDATE_BALLOT == m_state &&
        candidateTermId == m_candidateTermId &&
        candidateMemberId == m_thisMember.id())
    {
        auto it = m_clusterMemberByIdMap.find(followerMemberId);
        if (it != m_clusterMemberByIdMap.end())
        {
            ClusterMember* follower = it->second;
            follower->candidateTermId(candidateTermId)
                .leadershipTermId(logLeadershipTermId)
                .logPosition(logPosition)
                .vote(vote);
        }
    }
}

inline void Election::onAppendPosition(
    std::int64_t leadershipTermId,
    std::int64_t logPosition,
    std::int32_t followerMemberId,
    std::int16_t flags)
{
    if (ElectionState::INIT == m_state)
    {
        return;
    }

    if (leadershipTermId <= m_leadershipTermId)
    {
        auto it = m_clusterMemberByIdMap.find(followerMemberId);
        if (it != m_clusterMemberByIdMap.end())
        {
            ClusterMember* follower = it->second;
            follower->leadershipTermId(leadershipTermId)
                .logPosition(logPosition)
                .timeOfLastAppendPositionNs(m_ctx.clusterClock()->timeNanos());

            m_consensusModuleAgent.trackCatchupCompletion(*follower, leadershipTermId, flags);
        }
    }
}

inline void Election::onCommitPosition(
    std::int64_t leadershipTermId,
    std::int64_t logPosition,
    std::int32_t leaderMemberId)
{
    const ElectionState state = m_state;
    if (ElectionState::INIT == state)
    {
        return;
    }

    if (m_leaderMember && m_leaderMember->id() == leaderMemberId)
    {
        m_notifiedCommitPosition = std::max(m_notifiedCommitPosition, logPosition);
        if (ElectionState::FOLLOWER_LOG_REPLICATION == state)
        {
            m_replicationDeadlineNs = m_ctx.clusterClock()->timeNanos() + m_ctx.leaderHeartbeatTimeoutNs();
        }
    }
    else if (leadershipTermId > m_leadershipTermId && ElectionState::LEADER_READY == state)
    {
        throw ClusterEvent(
            "new leader detected due to commit position - " +
            " memberId=" + std::to_string(m_thisMember.id()) +
            " this.leadershipTermId=" + std::to_string(m_leadershipTermId) +
            " this.leaderMemberId=" + std::to_string(m_leaderMember ? m_leaderMember->id() : aeron::NULL_VALUE) +
            " this.logPosition=" + std::to_string(m_logPosition) +
            " newLeadershipTermId=" + std::to_string(leadershipTermId) +
            " newLeaderMemberId=" + std::to_string(leaderMemberId) +
            " newCommitPosition=" + std::to_string(logPosition),
            SOURCEINFO);
    }
}

inline void Election::onReplayNewLeadershipTermEvent(
    std::int64_t leadershipTermId,
    std::int64_t logPosition,
    std::int64_t timestamp,
    std::int64_t termBaseLogPosition)
{
    if (ElectionState::INIT == m_state)
    {
        return;
    }

    if (ElectionState::FOLLOWER_CATCHUP == m_state || ElectionState::FOLLOWER_REPLAY == m_state)
    {
        const std::int64_t nowNs = m_ctx.clusterClock()->convertToNanos(timestamp);
        ensureRecordingLogCoherent(leadershipTermId, termBaseLogPosition, AeronArchive::NULL_POSITION, nowNs);
        m_logPosition = logPosition;
        m_logLeadershipTermId = leadershipTermId;
    }
}

inline void Election::onTruncateLogEntry(
    std::int32_t memberId,
    ElectionState state,
    std::int64_t logLeadershipTermId,
    std::int64_t leadershipTermId,
    std::int64_t candidateTermId,
    std::int64_t commitPosition,
    std::int64_t logPosition,
    std::int64_t appendPosition,
    std::int64_t oldPosition,
    std::int64_t newPosition)
{
    m_consensusModuleAgent.truncateLogEntry(logLeadershipTermId, newPosition);
    m_appendPosition = newPosition;
    throw ClusterEvent(
        "Truncating Cluster Log - memberId=" + std::to_string(memberId) +
        " state=" + std::to_string(static_cast<std::int32_t>(state)) +
        " this.logLeadershipTermId=" + std::to_string(logLeadershipTermId) +
        " this.leadershipTermId=" + std::to_string(leadershipTermId) +
        " this.candidateTermId=" + std::to_string(candidateTermId) +
        " this.commitPosition=" + std::to_string(commitPosition) +
        " this.logPosition=" + std::to_string(logPosition) +
        " this.appendPosition=" + std::to_string(appendPosition) +
        " oldPosition=" + std::to_string(oldPosition) +
        " newPosition=" + std::to_string(newPosition),
        SOURCEINFO);
}

// Constants for append position flags
namespace {
    constexpr std::int16_t APPEND_POSITION_FLAG_NONE = 0;
    constexpr std::int16_t APPEND_POSITION_FLAG_CATCHUP = 1;
}

inline std::int32_t Election::leaderLogReplication(std::int64_t nowNs)
{
    std::int32_t workCount = 0;

    m_thisMember.logPosition(m_appendPosition).timeOfLastAppendPositionNs(nowNs);

    const std::int64_t quorumPosition = m_consensusModuleAgent.quorumPosition();
    workCount += publishNewLeadershipTermOnInterval(quorumPosition, nowNs);
    workCount += publishCommitPositionOnInterval(quorumPosition, nowNs);

    if (quorumPosition >= m_appendPosition)
    {
        workCount++;
        state(ElectionState::LEADER_REPLAY, nowNs, "");
    }

    return workCount;
}

inline std::int32_t Election::leaderReplay(std::int64_t nowNs)
{
    std::int32_t workCount = 0;

    if (!m_logReplay)
    {
        if (m_logPosition < m_appendPosition)
        {
            m_logReplay = m_consensusModuleAgent.newLogReplay(m_logPosition, m_appendPosition);
        }
        else
        {
            state(ElectionState::LEADER_INIT, nowNs, "");
        }

        workCount++;
        m_isLeaderStartup = m_isNodeStartup;
        m_thisMember.leadershipTermId(m_leadershipTermId).logPosition(m_appendPosition);
    }
    else
    {
        workCount += m_logReplay->doWork();
        if (m_logReplay->isDone())
        {
            stopReplay();
            m_logPosition = m_appendPosition;
            state(ElectionState::LEADER_INIT, nowNs, "");
        }
    }

    const std::int64_t quorumPosition = m_consensusModuleAgent.quorumPosition();
    workCount += publishNewLeadershipTermOnInterval(quorumPosition, nowNs);
    workCount += publishCommitPositionOnInterval(quorumPosition, nowNs);

    return workCount;
}

inline std::int32_t Election::leaderInit(std::int64_t nowNs)
{
    m_consensusModuleAgent.joinLogAsLeader(m_leadershipTermId, m_logPosition, m_logSessionId, m_isLeaderStartup);
    updateRecordingLog(nowNs);
    state(ElectionState::LEADER_READY, nowNs, "");

    return 1;
}

inline std::int32_t Election::leaderReady(std::int64_t nowNs)
{
    std::int32_t workCount = m_consensusModuleAgent.updateLeaderPosition(nowNs, m_appendPosition);
    workCount += publishNewLeadershipTermOnInterval(m_consensusModuleAgent.quorumPosition(), nowNs);

    // TODO: Implement ClusterMember::hasVotersAtPosition and hasQuorumAtPosition
    // For now, use a simplified check
    bool hasVoters = false;
    bool hasQuorum = false;
    std::int32_t voters = 0;

    for (auto& member : m_clusterMembers)
    {
        if (member.leadershipTermId() == m_leadershipTermId &&
            member.logPosition() >= m_logPosition)
        {
            voters++;
            hasVoters = true;
        }
    }

    const std::int32_t quorumThreshold = (static_cast<std::int32_t>(m_clusterMembers.size()) / 2) + 1;
    hasQuorum = voters >= quorumThreshold;

    if (hasVoters ||
        (nowNs >= (m_timeOfLastStateChangeNs + m_ctx.leaderHeartbeatTimeoutNs()) && hasQuorum))
    {
        if (m_consensusModuleAgent.appendNewLeadershipTermEvent(nowNs))
        {
            m_consensusModuleAgent.electionComplete(nowNs);
            state(ElectionState::CLOSED, nowNs, "");
            workCount++;
        }
    }

    return workCount;
}

inline std::int32_t Election::followerLogReplication(std::int64_t nowNs)
{
    std::int32_t workCount = 0;

    if (!m_logReplication)
    {
        if (m_appendPosition < m_replicationStopPosition)
        {
            m_logReplication = m_consensusModuleAgent.newLogReplication(
                m_leaderMember->archiveEndpoint(),
                m_leaderMember->archiveResponseEndpoint(),
                m_leaderRecordingId,
                m_replicationStopPosition,
                nowNs);
            m_replicationDeadlineNs = nowNs + m_ctx.leaderHeartbeatTimeoutNs();
            workCount++;
        }
        else
        {
            updateRecordingLogForReplication(
                m_replicationLeadershipTermId, m_replicationTermBaseLogPosition, m_replicationStopPosition, nowNs);
            state(ElectionState::CANVASS, nowNs, "");
        }
    }
    else
    {
        workCount += m_consensusModuleAgent.pollArchiveEvents();
        workCount += m_logReplication->poll(nowNs);
        const bool replicationDone = m_logReplication->hasReplicationEnded() && m_logReplication->hasStopped();
        workCount += publishFollowerReplicationPosition(nowNs);

        if (replicationDone)
        {
            if (m_notifiedCommitPosition >= m_appendPosition)
            {
                // TODO: Implement ConsensusModuleAgent::logReplicationEnded
                m_appendPosition = m_logReplication->position();
                stopLogReplication();
                updateRecordingLogForReplication(
                    m_replicationLeadershipTermId, m_replicationTermBaseLogPosition, m_replicationStopPosition, nowNs);
                state(ElectionState::CANVASS, nowNs, "");
                workCount++;
            }
            else if (nowNs >= m_replicationDeadlineNs)
            {
                throw TimeoutException("timeout awaiting commit position", SOURCEINFO);
            }
        }
    }

    return workCount;
}

inline std::int32_t Election::followerReplay(std::int64_t nowNs)
{
    std::int32_t workCount = 0;

    if (!m_logReplay)
    {
        if (m_logPosition < m_appendPosition)
        {
            if (0 == m_notifiedCommitPosition)
            {
                return publishFollowerAppendPosition(nowNs);
            }

            m_logReplay = m_consensusModuleAgent.newLogReplay(
                m_logPosition, std::min(m_appendPosition, m_notifiedCommitPosition));
            workCount++;
        }
        else
        {
            state(
                (AeronArchive::NULL_POSITION != m_catchupJoinPosition) ?
                    ElectionState::FOLLOWER_CATCHUP_INIT : ElectionState::FOLLOWER_LOG_INIT,
                nowNs,
                "nothing to replay");
            workCount++;
        }
    }
    else
    {
        workCount += m_logReplay->doWork();
        if (m_logReplay->isDone())
        {
            m_logPosition = m_logReplay->position();
            stopReplay();

            if (m_logPosition == m_appendPosition)
            {
                state(
                    (AeronArchive::NULL_POSITION != m_catchupJoinPosition) ?
                        ElectionState::FOLLOWER_CATCHUP_INIT : ElectionState::FOLLOWER_LOG_INIT,
                    nowNs,
                    "log replay done");
            }
            else
            {
                state(ElectionState::CANVASS, nowNs,
                    "incomplete log replay: logPosition=" + std::to_string(m_logPosition) +
                    " appendPosition=" + std::to_string(m_appendPosition));
            }
        }
    }

    return workCount;
}

inline std::int32_t Election::followerCatchupInit(std::int64_t nowNs)
{
    if (!m_logSubscription)
    {
        m_logSubscription = addFollowerSubscription();
        addCatchupLogDestination();
    }

    std::string catchupEndpoint;
    const std::string endpoint = m_thisMember.catchupEndpoint();
    if (endpoint.length() >= 2 && endpoint.substr(endpoint.length() - 2) == ":0")
    {
        const std::string resolvedEndpoint = m_logSubscription->resolvedEndpoint();
        if (!resolvedEndpoint.empty())
        {
            const std::size_t i = resolvedEndpoint.find_last_of(':');
            if (i != std::string::npos)
            {
                catchupEndpoint = endpoint.substr(0, endpoint.length() - 2) + resolvedEndpoint.substr(i);
            }
        }
    }
    else
    {
        catchupEndpoint = endpoint;
    }

    if (!catchupEndpoint.empty() && sendCatchupPosition(catchupEndpoint))
    {
        m_timeOfLastUpdateNs = nowNs;
        m_consensusModuleAgent.catchupInitiated(nowNs);
        state(ElectionState::FOLLOWER_CATCHUP_AWAIT, nowNs, "");
    }
    else if (nowNs >= (m_timeOfLastStateChangeNs + m_ctx.leaderHeartbeatTimeoutNs()))
    {
        throw TimeoutException("failed to send catchup position", SOURCEINFO);
    }

    return 1;
}

inline std::int32_t Election::followerCatchupAwait(std::int64_t nowNs)
{
    std::int32_t workCount = 0;

    auto image = m_logSubscription->imageBySessionId(m_logSessionId);
    if (image)
    {
        verifyLogJoinPosition("followerCatchupAwait", image->joinPosition());
        if (m_consensusModuleAgent.tryJoinLogAsFollower(*image, m_isLeaderStartup, nowNs))
        {
            state(ElectionState::FOLLOWER_CATCHUP, nowNs, "");
            workCount++;
        }
        else if (ChannelEndpointStatus::CHANNEL_ENDPOINT_ERRORED == m_logSubscription->channelStatus())
        {
            const std::string message = "failed to add catchup log as follower - " + m_logSubscription->channel();
            throw ClusterException(message, SOURCEINFO);
        }
        else if (nowNs >= (m_timeOfLastStateChangeNs + m_ctx.leaderHeartbeatTimeoutNs()))
        {
            throw TimeoutException("failed to join catchup log as follower", SOURCEINFO);
        }
    }
    else if (nowNs >= (m_timeOfLastStateChangeNs + m_ctx.leaderHeartbeatTimeoutNs()))
    {
        throw TimeoutException("failed to join catchup log", SOURCEINFO);
    }

    return workCount;
}

inline std::int32_t Election::followerCatchup(std::int64_t nowNs)
{
    std::int32_t workCount = m_consensusModuleAgent.catchupPoll(m_notifiedCommitPosition, nowNs);

    if (!m_consensusModuleAgent.liveLogDestination() &&
        m_consensusModuleAgent.isCatchupNearLive(std::max(m_catchupJoinPosition, m_notifiedCommitPosition)))
    {
        addLiveLogDestination();
        workCount++;
    }

    const std::int64_t position = m_ctx.commitPositionCounter()->get();
    if (position >= m_catchupJoinPosition &&
        position >= m_notifiedCommitPosition &&
        !m_consensusModuleAgent.catchupLogDestination() &&
        ConsensusModule::State::SNAPSHOT != m_consensusModuleAgent.state())
    {
        m_appendPosition = position;
        m_logPosition = position;
        state(ElectionState::FOLLOWER_LOG_INIT, nowNs, "");
        workCount++;
    }

    return workCount;
}

inline std::int32_t Election::followerLogInit(std::int64_t nowNs)
{
    if (!m_logSubscription)
    {
        if (0 != m_logSessionId)  // NULL_SESSION_ID
        {
            m_logSubscription = addFollowerSubscription();
            addLiveLogDestination();
            state(ElectionState::FOLLOWER_LOG_AWAIT, nowNs, "");
        }
    }
    else
    {
        state(ElectionState::FOLLOWER_READY, nowNs, "");
    }

    return 1;
}

inline std::int32_t Election::followerLogAwait(std::int64_t nowNs)
{
    std::int32_t workCount = 0;

    auto image = m_logSubscription->imageBySessionId(m_logSessionId);
    if (image)
    {
        verifyLogJoinPosition("followerLogAwait", image->joinPosition());
        if (m_consensusModuleAgent.tryJoinLogAsFollower(*image, m_isLeaderStartup, nowNs))
        {
            updateRecordingLog(nowNs);
            state(ElectionState::FOLLOWER_READY, nowNs, "");
            workCount++;
        }
        else if (nowNs >= (m_timeOfLastStateChangeNs + m_ctx.leaderHeartbeatTimeoutNs()))
        {
            throw TimeoutException("failed to join live log as follower", SOURCEINFO);
        }
    }
    else if (ChannelEndpointStatus::CHANNEL_ENDPOINT_ERRORED == m_logSubscription->channelStatus())
    {
        const std::string message = "failed to add live log as follower - " + m_logSubscription->channel();
        throw ClusterException(message, SOURCEINFO);
    }
    else if (nowNs >= (m_timeOfLastStateChangeNs + m_ctx.leaderHeartbeatTimeoutNs()))
    {
        throw TimeoutException("failed to join live log", SOURCEINFO);
    }

    return workCount;
}

inline std::int32_t Election::followerReady(std::int64_t nowNs)
{
    if (m_consensusPublisher.appendPosition(
        m_leaderMember->publication(),
        m_leadershipTermId,
        m_logPosition,
        m_thisMember.id(),
        APPEND_POSITION_FLAG_NONE))
    {
        m_consensusModuleAgent.electionComplete(nowNs);
        state(ElectionState::CLOSED, nowNs, "");
    }
    else if (nowNs >= (m_timeOfLastStateChangeNs + m_ctx.leaderHeartbeatTimeoutNs()))
    {
        throw TimeoutException("ready follower failed to notify leader", SOURCEINFO);
    }

    return 1;
}

inline std::int32_t Election::publishNewLeadershipTermOnInterval(std::int64_t quorumPosition, std::int64_t nowNs)
{
    std::int32_t workCount = 0;

    if (hasUpdateIntervalExpired(nowNs, m_ctx.leaderHeartbeatIntervalNs()))
    {
        m_timeOfLastUpdateNs = nowNs;
        publishNewLeadershipTerm(quorumPosition, m_ctx.clusterClock()->convertToTimeUnit(nowNs));
        workCount++;
    }

    return workCount;
}

inline std::int32_t Election::publishCommitPositionOnInterval(std::int64_t quorumPosition, std::int64_t nowNs)
{
    std::int32_t workCount = 0;

    if (m_lastPublishedCommitPosition < quorumPosition ||
        (m_lastPublishedCommitPosition == quorumPosition &&
         hasIntervalExpired(nowNs, m_timeOfLastCommitPositionUpdateNs, m_ctx.leaderHeartbeatIntervalNs())))
    {
        m_timeOfLastCommitPositionUpdateNs = nowNs;
        m_lastPublishedCommitPosition = quorumPosition;
        m_consensusModuleAgent.publishCommitPosition(quorumPosition, m_leadershipTermId);
        workCount++;
    }

    return workCount;
}

inline void Election::publishNewLeadershipTerm(std::int64_t quorumPosition, std::int64_t timestamp)
{
    for (auto& member : m_clusterMembers)
    {
        publishNewLeadershipTerm(member, m_logLeadershipTermId, quorumPosition, timestamp);
    }
}

inline void Election::publishNewLeadershipTerm(
    ClusterMember& member,
    std::int64_t logLeadershipTermId,
    std::int64_t quorumPosition,
    std::int64_t timestamp)
{
    if (member.id() != m_thisMember.id() && aeron::NULL_SESSION_ID != m_logSessionId)
    {
        auto recordingLog = m_ctx.recordingLog();
        auto logNextTermEntry = recordingLog->findTermEntry(logLeadershipTermId + 1);

        const std::int64_t nextLeadershipTermId = logNextTermEntry ?
            logNextTermEntry->leadershipTermId : m_leadershipTermId;
        const std::int64_t nextTermBaseLogPosition = logNextTermEntry ?
            logNextTermEntry->termBaseLogPosition : m_appendPosition;

        const std::int64_t nextLogPosition = logNextTermEntry ?
            ((AeronArchive::NULL_POSITION != logNextTermEntry->logPosition) ?
                logNextTermEntry->logPosition : m_appendPosition) :
            AeronArchive::NULL_POSITION;

        m_consensusPublisher.newLeadershipTerm(
            member.publication(),
            logLeadershipTermId,
            nextLeadershipTermId,
            nextTermBaseLogPosition,
            nextLogPosition,
            m_leadershipTermId,
            m_appendPosition,
            m_appendPosition,
            quorumPosition,
            m_consensusModuleAgent.logRecordingId(),
            timestamp,
            m_thisMember.id(),
            m_logSessionId,
            m_ctx.appVersion(),
            m_isLeaderStartup);
    }
}

inline std::int32_t Election::publishFollowerReplicationPosition(std::int64_t nowNs)
{
    const std::int64_t position = m_logReplication->position();
    if (position > m_appendPosition ||
        (position == m_appendPosition && hasUpdateIntervalExpired(nowNs, m_ctx.leaderHeartbeatIntervalNs())))
    {
        if (m_consensusPublisher.appendPosition(
            m_leaderMember->publication(),
            m_leadershipTermId,
            position,
            m_thisMember.id(),
            APPEND_POSITION_FLAG_NONE))
        {
            m_appendPosition = position;
            m_timeOfLastUpdateNs = nowNs;
            return 1;
        }
    }
    return 0;
}

inline std::int32_t Election::publishFollowerAppendPosition(std::int64_t nowNs)
{
    if (m_lastPublishedAppendPosition != m_appendPosition ||
        hasUpdateIntervalExpired(nowNs, m_ctx.leaderHeartbeatIntervalNs()))
    {
        if (m_consensusPublisher.appendPosition(
            m_leaderMember->publication(),
            m_leadershipTermId,
            m_appendPosition,
            m_thisMember.id(),
            APPEND_POSITION_FLAG_NONE))
        {
            m_lastPublishedAppendPosition = m_appendPosition;
            m_timeOfLastUpdateNs = nowNs;
            return 1;
        }
    }
    return 0;
}

inline bool Election::sendCatchupPosition(const std::string& catchupEndpoint)
{
    return m_consensusPublisher.catchupPosition(
        m_leaderMember->publication(),
        m_leadershipTermId,
        m_logPosition,
        m_thisMember.id(),
        catchupEndpoint);
}

inline void Election::addCatchupLogDestination()
{
    const std::string destination = ChannelUri::createDestinationUri(
        m_ctx.logChannel(), m_thisMember.catchupEndpoint());
    m_logSubscription->addDestination(destination);
    m_consensusModuleAgent.catchupLogDestination(destination);
}

inline void Election::addLiveLogDestination()
{
    std::string destination;
    if (m_ctx.isLogMdc())
    {
        destination = ChannelUri::createDestinationUri(m_ctx.logChannel(), m_thisMember.logEndpoint());
    }
    else
    {
        destination = m_ctx.logChannel();
    }
    m_logSubscription->addDestination(destination);
    m_consensusModuleAgent.liveLogDestination(destination);
}

inline std::shared_ptr<Subscription> Election::addFollowerSubscription()
{
    auto aeron = m_ctx.aeron();
    ChannelUri logChannelUri = ChannelUri::parse(m_ctx.logChannel());
    
    const std::int64_t tag1 = aeron->nextCorrelationId();
    const std::int64_t tag2 = aeron->nextCorrelationId();
    
    const std::string channel = ChannelUriStringBuilder()
        .media("udp")
        .tags(std::to_string(tag1) + "," + std::to_string(tag2))
        .controlMode("manual")
        .sessionId(m_logSessionId)
        .group(true)
        .rejoin(false)
        .socketRcvbufLength(logChannelUri)
        .receiverWindowLength(logChannelUri)
        .alias("log-cm")
        .build();

    return aeron->addSubscription(channel, m_ctx.logStreamId());
}

inline void Election::onNewLeadershipTerm(
    std::int64_t logLeadershipTermId,
    std::int64_t nextLeadershipTermId,
    std::int64_t nextTermBaseLogPosition,
    std::int64_t nextLogPosition,
    std::int64_t leadershipTermId,
    std::int64_t termBaseLogPosition,
    std::int64_t logPosition,
    std::int64_t commitPosition,
    std::int64_t leaderRecordingId,
    std::int64_t timestamp,
    std::int32_t leaderMemberId,
    std::int32_t logSessionId,
    bool isStartup)
{
    if (ElectionState::INIT == m_state)
    {
        return;
    }

    auto it = m_clusterMemberByIdMap.find(leaderMemberId);
    if (it == m_clusterMemberByIdMap.end() ||
        (leaderMemberId == m_thisMember.id() && leadershipTermId == m_leadershipTermId))
    {
        return;
    }

    ClusterMember* leader = it->second;

    if (leaderMemberId == m_gracefulClosedLeaderId)
    {
        m_gracefulClosedLeaderId = aeron::NULL_VALUE;
    }

    if (((ElectionState::FOLLOWER_BALLOT == m_state || ElectionState::CANDIDATE_BALLOT == m_state) &&
         leadershipTermId == m_candidateTermId) ||
        ElectionState::CANVASS == m_state)
    {
        if (logLeadershipTermId == m_logLeadershipTermId)
        {
            if (AeronArchive::NULL_POSITION != nextTermBaseLogPosition && nextTermBaseLogPosition < m_appendPosition)
            {
                onTruncateLogEntry(
                    m_thisMember.id(),
                    m_state,
                    logLeadershipTermId,
                    m_leadershipTermId,
                    m_candidateTermId,
                    m_ctx.commitPositionCounter()->get(),
                    m_logPosition,
                    m_appendPosition,
                    m_appendPosition,
                    nextTermBaseLogPosition);
            }

            m_leaderMember = leader;
            m_isLeaderStartup = isStartup;
            m_leadershipTermId = leadershipTermId;
            m_candidateTermId = std::max(leadershipTermId, m_candidateTermId);
            m_logSessionId = logSessionId;
            m_leaderRecordingId = leaderRecordingId;
            m_catchupJoinPosition = (m_appendPosition < logPosition) ? logPosition : AeronArchive::NULL_POSITION;
            m_notifiedCommitPosition = std::max(m_notifiedCommitPosition, commitPosition);

            if (m_appendPosition < termBaseLogPosition)
            {
                if (aeron::NULL_VALUE != nextLeadershipTermId)
                {
                    if (m_appendPosition < nextTermBaseLogPosition)
                    {
                        m_replicationLeadershipTermId = logLeadershipTermId;
                        m_replicationStopPosition = nextTermBaseLogPosition;
                        m_replicationTermBaseLogPosition = aeron::NULL_VALUE;
                        state(ElectionState::FOLLOWER_LOG_REPLICATION, m_ctx.clusterClock()->timeNanos(), "");
                    }
                    else if (m_appendPosition == nextTermBaseLogPosition)
                    {
                        if (AeronArchive::NULL_POSITION != nextLogPosition)
                        {
                            m_replicationLeadershipTermId = nextLeadershipTermId;
                            m_replicationStopPosition = nextLogPosition;
                            m_replicationTermBaseLogPosition = nextTermBaseLogPosition;
                            state(ElectionState::FOLLOWER_LOG_REPLICATION, m_ctx.clusterClock()->timeNanos(), "");
                        }
                    }
                }
                else
                {
                    throw ClusterException(
                        "invalid newLeadershipTerm - this.appendPosition=" + std::to_string(m_appendPosition) +
                        " < termBaseLogPosition=" + std::to_string(termBaseLogPosition) +
                        " and nextLeadershipTermId=" + std::to_string(nextLeadershipTermId) +
                        ", logLeadershipTermId=" + std::to_string(logLeadershipTermId) +
                        ", nextTermBaseLogPosition=" + std::to_string(nextTermBaseLogPosition) +
                        ", nextLogPosition=" + std::to_string(nextLogPosition) +
                        ", leadershipTermId=" + std::to_string(leadershipTermId) +
                        ", termBaseLogPosition=" + std::to_string(termBaseLogPosition) +
                        ", logPosition=" + std::to_string(logPosition) +
                        ", commitPosition=" + std::to_string(commitPosition) +
                        ", leaderRecordingId=" + std::to_string(leaderRecordingId) +
                        ", leaderMemberId=" + std::to_string(leaderMemberId) +
                        ", logSessionId=" + std::to_string(logSessionId) +
                        ", isStartup=" + (isStartup ? "true" : "false"),
                        SOURCEINFO);
                }
            }
            else
            {
                state(ElectionState::FOLLOWER_REPLAY, m_ctx.clusterClock()->timeNanos(), "");
            }
        }
        else
        {
            state(ElectionState::CANVASS, m_ctx.clusterClock()->timeNanos(), "");
        }
    }

    if (ElectionState::FOLLOWER_LOG_REPLICATION == m_state &&
        logLeadershipTermId == m_logLeadershipTermId &&
        leader->id() == leaderMemberId)
    {
        m_replicationDeadlineNs = m_ctx.clusterClock()->timeNanos() + m_ctx.leaderHeartbeatTimeoutNs();
    }
}

}}

