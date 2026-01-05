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
#include "archive/codecs/RecordingSignal.h"
#include "../client/ClusterExceptions.h"
#include "../client/ClusterEvent.h"
#include "../service/Cluster.h"
#include "util/Exceptions.h"
#include "util/CloseHelper.h"
#include "status/ChannelEndpointStatus.h"
#include "concurrent/AgentTerminationException.h"
#include "ElectionState.h"
#include "ClusterMember.h"
#include "ConsensusPublisher.h"
#include "ConsensusModule.h"
#include "ConsensusModuleAgent.h"
#include "LogReplay.h"
#include "RecordingReplication.h"
#include "NodeStateFile.h"
#include "RecordingLog.h"
#include "CommonContext.h"
#include "archive/client/AeronArchive.h"

namespace aeron { namespace cluster {

using namespace aeron::archive::codecs;
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
        ConsensusModule::Context& ctx,
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
        ConsensusModule::Context& ctx,
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
    ConsensusModule::Context& m_ctx;
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
    std::int32_t m_logSessionId = 0; // NULL_SESSION_ID
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

}}

