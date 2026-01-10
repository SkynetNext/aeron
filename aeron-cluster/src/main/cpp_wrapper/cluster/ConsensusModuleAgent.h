#pragma once

#include "Aeron.h"
#include "Subscription.h"
#include "client/archive/AeronArchive.h"
#include <chrono>
#include <deque>
#include <functional>
#include <memory>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

// TODO: security/Authenticator.h and security/AuthorisationService.h not yet
// implemented in C++ wrapper #include "security/Authenticator.h" #include
// "security/AuthorisationService.h"
// TODO: IdleStrategy base class not yet implemented in C++ wrapper
// C++ wrapper only has concrete implementations (BackoffIdleStrategy,
// BusySpinIdleStrategy, etc.) #include "concurrent/IdleStrategy.h"

// Forward declaration for IdleStrategy (not yet implemented as base class in
// C++ wrapper)
namespace aeron {
namespace concurrent {
class IdleStrategy; // Forward declaration - C++ wrapper uses concrete types
}
} // namespace aeron
#include "concurrent/AgentTerminationException.h"
// TODO: ControlledFragmentHandler not yet implemented in C++ wrapper
// C++ wrapper uses ControlledPollAction enum and
// controlled_poll_fragment_handler_t type instead #include
// "concurrent/logbuffer/ControlledFragmentHandler.h"
#include "concurrent/AtomicBuffer.h"
#include "concurrent/AtomicCounter.h"
#include "concurrent/CountersReader.h"
#include "concurrent/logbuffer/Header.h"

// TODO: ReadableCounter not yet implemented in C++ wrapper
#include "util/Exceptions.h"
// TODO: CloseHelper not yet implemented in C++ wrapper
// Simple helper to close resources (similar to Java CloseHelper)
template <typename T> inline void closeHelper(T *ptr) {
  if (ptr) {
    delete ptr;
  }
}

template <typename T> inline void closeHelper(std::shared_ptr<T> &ptr) {
  ptr.reset();
}

template <typename T>
inline void closeHelper(std::shared_ptr<T> &ptr, void * /*errorHandler*/) {
  ptr.reset();
}
// ExpandableArrayBuffer is now in util/ExpandableArrayBuffer.h
#include "ChannelUri.h"
#include "ClusterMember.h"
#include "ClusterSession.h"
#include "ClusterSessionProxy.h"
#include "ClusterTermination.h"
#include "ConsensusAdapter.h"
#include "ConsensusModuleAdapter.h"
#include "ConsensusModuleControl.h"
#include "ConsensusModuleExtension.h"
#include "ConsensusModuleSnapshotListener.h"
#include "ConsensusPublisher.h"
#include "EgressPublisher.h"
#include "Election.h"
#include "IngressAdapter.h"
#include "LogAdapter.h"
#include "LogPublisher.h"
#include "PendingServiceMessageTracker.h"
#include "RecordingLog.h"
#include "RecordingReplication.h"
#include "ServiceAck.h"
#include "ServiceProxy.h"
#include "StandbySnapshotReplicator.h"
#include "TimerService.h"
#include "client/AeronCluster.h"
#include "client/ClusterEvent.h"
#include "client/ClusterExceptions.h"
#include "generated/aeron_cluster_codecs/AdminRequestType.h"
#include "generated/aeron_cluster_codecs/AdminResponseCode.h"
#include "generated/aeron_cluster_codecs/CloseReason.h"
#include "generated/aeron_cluster_codecs/ClusterAction.h"
#include "generated/aeron_cluster_codecs/EventCode.h"
#include "service/Cluster.h"
#include "service/ClusterClock.h"
#include "service/ClusterMarkFile.h"
#include "service/ClusterTerminationException.h"
#include "util/SemanticVersion.h"

namespace aeron {
namespace cluster {
using namespace aeron::concurrent;
using namespace aeron::concurrent::logbuffer;
using namespace aeron::concurrent::status;
using namespace aeron::archive::client;
// TODO: aeron::security namespace not yet implemented in C++ wrapper
// using namespace aeron::security;
using namespace aeron::util;

// Forward declarations for security types (not yet implemented in C++ wrapper)
namespace security {
class Authenticator;
class AuthorisationService;
} // namespace security

// Note: These using declarations must be after the namespace is fully defined
// They are moved to after the class definition to avoid namespace resolution
// issues

// Type aliases for SBE codecs
using MessageHeaderEncoder = MessageHeader;
using MessageHeaderDecoder = MessageHeader;

// Forward declarations
class ConsensusModule;
class Election;

/**
 * Agent which coordinates consensus within a cluster in concert with the
 * lifecycle of clustered services.
 *
 * This class implements:
 * - Agent interface (onStart, onClose, doWork)
 * - IdleStrategy interface (idle, idle(int), reset)
 * - TimerService::TimerHandler interface
 * - ConsensusModuleSnapshotListener interface
 * - ConsensusModuleControl interface
 */
class ConsensusModuleAgent : public ConsensusModuleControl,
                             public ConsensusModuleSnapshotListener {
public:
  static constexpr std::int64_t SLOW_TICK_INTERVAL_NS =
      10000000LL; // 10ms in nanoseconds
  static constexpr std::int16_t APPEND_POSITION_FLAG_NONE = 0;
  static constexpr std::int16_t APPEND_POSITION_FLAG_CATCHUP = 1;

  // Constants from ConsensusModule::Configuration
  static constexpr const char *SESSION_LIMIT_MSG = "concurrent session limit";
  static constexpr const char *SESSION_INVALID_VERSION_MSG =
      "invalid client version";
  static constexpr std::int32_t SERVICE_ID = ::aeron::NULL_VALUE;

  explicit ConsensusModuleAgent(ConsensusModule::Context &ctx);

  // Agent interface
  void onStart();
  void onClose();
  std::int32_t doWork();

  // IdleStrategy interface
  void idle();
  void idle(std::int32_t workCount);
  void reset();

  // ConsensusModuleControl interface
  std::int32_t memberId() override;
  std::int64_t time() override;
  std::chrono::milliseconds::rep timeUnit() override;
  std::shared_ptr<IdleStrategy> idleStrategy() override;
  ConsensusModule::Context &context() override;
  std::shared_ptr<Aeron> aeron() override;
  std::shared_ptr<AeronArchive> archive() override;
  std::shared_ptr<security::AuthorisationService>
  authorisationService() override;
  std::shared_ptr<ClusterClientSession>
  getClientSession(std::int64_t clusterSessionId) override;
  void closeClusterSession(std::int64_t clusterSessionId) override;
  std::int32_t commitPositionCounterId() override;
  std::int32_t clusterId() override;
  ClusterMember clusterMember() override;

  // ConsensusModuleSnapshotListener interface
  void onLoadBeginSnapshot(std::int32_t appVersion,
                           ClusterTimeUnit::Value timeUnit,
                           const util::DirectBuffer &buffer,
                           std::int32_t offset, std::int32_t length) override;

  void onLoadConsensusModuleState(std::int64_t nextSessionId,
                                  std::int64_t nextServiceSessionId,
                                  std::int64_t logServiceSessionId,
                                  std::int32_t pendingMessageCapacity,
                                  const util::DirectBuffer &buffer,
                                  std::int32_t offset,
                                  std::int32_t length) override;

  void onLoadPendingMessage(std::int64_t clusterSessionId,
                            const util::DirectBuffer &buffer,
                            std::int32_t offset, std::int32_t length) override;

  void onLoadClusterSession(
      std::int64_t clusterSessionId, std::int64_t correlationId,
      std::int64_t openedLogPosition, std::int64_t timeOfLastActivity,
      CloseReason::Value closeReason, std::int32_t responseStreamId,
      const std::string &responseChannel, const util::DirectBuffer &buffer,
      std::int32_t offset, std::int32_t length) override;

  void onLoadTimer(std::int64_t correlationId, std::int64_t deadline,
                   const util::DirectBuffer &buffer, std::int32_t offset,
                   std::int32_t length) override;

  void onLoadPendingMessageTracker(std::int64_t nextServiceSessionId,
                                   std::int64_t logServiceSessionId,
                                   std::int32_t pendingMessageCapacity,
                                   std::int32_t serviceId,
                                   const util::DirectBuffer &buffer,
                                   std::int32_t offset,
                                   std::int32_t length) override;

  void onLoadEndSnapshot(const util::DirectBuffer &buffer, std::int32_t offset,
                         std::int32_t length) override;

  // TimerService::TimerHandler interface
  bool onTimerEvent(std::int64_t correlationId);

  // Methods called by LogAdapter
  ControlledPollAction onReplayExtensionMessage(
      std::int32_t actingBlockLength, std::int32_t templateId,
      std::int32_t schemaId, std::int32_t actingVersion, AtomicBuffer &buffer,
      std::int32_t offset, std::int32_t length, Header &header);

  void onReplaySessionMessage(std::int64_t clusterSessionId,
                              std::int64_t timestamp);

  void onReplayTimerEvent(std::int64_t correlationId);

  void onReplaySessionOpen(std::int64_t logPosition, std::int64_t correlationId,
                           std::int64_t clusterSessionId,
                           std::int64_t timestamp,
                           std::int32_t responseStreamId,
                           const std::string &responseChannel);

  void onReplaySessionClose(std::int64_t clusterSessionId,
                            codecs::CloseReason::Value closeReason);

  void onReplayClusterAction(std::int64_t leadershipTermId,
                             std::int64_t logPosition, std::int64_t timestamp,
                             ClusterAction::Value action, std::int32_t flags);

  void onReplayNewLeadershipTermEvent(std::int64_t leadershipTermId,
                                      std::int64_t logPosition,
                                      std::int64_t timestamp,
                                      std::int64_t termBaseLogPosition,
                                      std::chrono::milliseconds::rep timeUnit,
                                      std::int32_t appVersion);

  void awaitLocalSocketsClosed(std::int64_t registrationId);

  // Methods called by IngressAdapter
  ControlledPollAction
  onExtensionMessage(std::int32_t actingBlockLength, std::int32_t templateId,
                     std::int32_t schemaId, std::int32_t actingVersion,
                     AtomicBuffer &buffer, std::int32_t offset,
                     std::int32_t length, Header &header);

  ControlledPollAction onIngressMessage(std::int64_t leadershipTermId,
                                        std::int64_t clusterSessionId,
                                        AtomicBuffer &buffer,
                                        std::int32_t offset,
                                        std::int32_t length);

  void onSessionConnect(std::int64_t correlationId,
                        std::int32_t responseStreamId, std::int32_t version,
                        const std::string &responseChannel,
                        const std::vector<std::uint8_t> &encodedCredentials,
                        const std::string &clientInfo, Header &header);

  void onSessionClose(std::int64_t leadershipTermId,
                      std::int64_t clusterSessionId);

  void onSessionKeepAlive(std::int64_t leadershipTermId,
                          std::int64_t clusterSessionId, Header &header);

  void onIngressChallengeResponse(
      std::int64_t correlationId, std::int64_t clusterSessionId,
      const std::vector<std::uint8_t> &encodedCredentials);

  void onAdminRequest(std::int64_t leadershipTermId,
                      std::int64_t clusterSessionId, std::int64_t correlationId,
                      AdminRequestType::Value requestType,
                      AtomicBuffer &payload, std::int32_t payloadOffset,
                      std::int32_t payloadLength);

  // Methods called by ConsensusAdapter
  void onCanvassPosition(std::int64_t logLeadershipTermId,
                         std::int64_t logPosition,
                         std::int64_t leadershipTermId,
                         std::int32_t followerMemberId,
                         std::int32_t protocolVersion);

  void onRequestVote(std::int64_t logLeadershipTermId, std::int64_t logPosition,
                     std::int64_t candidateTermId, std::int32_t candidateId,
                     std::int32_t protocolVersion);

  void onVote(std::int64_t candidateTermId, std::int64_t logLeadershipTermId,
              std::int64_t logPosition, std::int32_t candidateMemberId,
              std::int32_t followerMemberId, bool vote);

  void onNewLeadershipTerm(
      std::int64_t logLeadershipTermId, std::int64_t nextLeadershipTermId,
      std::int64_t nextTermBaseLogPosition, std::int64_t nextLogPosition,
      std::int64_t leadershipTermId, std::int64_t termBaseLogPosition,
      std::int64_t logPosition, std::int64_t commitPosition,
      std::int64_t leaderRecordingId, std::int64_t timestamp,
      std::int32_t leaderId, std::int32_t logSessionId, std::int32_t appVersion,
      bool isStartup);

  void onAppendPosition(std::int64_t leadershipTermId, std::int64_t logPosition,
                        std::int32_t followerMemberId, std::int16_t flags);

  void onCommitPosition(std::int64_t leadershipTermId, std::int64_t logPosition,
                        std::int32_t leaderMemberId);

  void onCatchupPosition(std::int64_t leadershipTermId,
                         std::int64_t logPosition,
                         std::int32_t followerMemberId,
                         const std::string &catchupEndpoint);

  void onStopCatchup(std::int64_t leadershipTermId,
                     std::int32_t followerMemberId);

  void onTerminationPosition(std::int64_t leadershipTermId,
                             std::int64_t logPosition);

  void onTerminationAck(std::int64_t leadershipTermId, std::int64_t logPosition,
                        std::int32_t memberId);

  void onBackupQuery(std::int64_t correlationId, std::int32_t responseStreamId,
                     std::int32_t version, const std::string &responseChannel,
                     const std::vector<std::uint8_t> &encodedCredentials,
                     Header &header);

  void onConsensusChallengeResponse(
      std::int64_t correlationId, std::int64_t clusterSessionId,
      const std::vector<std::uint8_t> &encodedCredentials);

  void onHeartbeatRequest(std::int64_t correlationId,
                          std::int32_t responseStreamId,
                          const std::string &responseChannel,
                          const std::vector<std::uint8_t> &encodedCredentials,
                          Header &header);

  void onStandbySnapshot(
      std::int64_t correlationId, std::int32_t version,
      const std::vector<StandbySnapshotEntry> &standbySnapshotEntries,
      std::int32_t responseStreamId, const std::string &responseChannel,
      const std::vector<std::uint8_t> &encodedCredentials, Header &header);

  // Utility methods
  std::string roleName() const;
  std::int64_t logRecordingId() const;
  void logRecordingId(std::int64_t recordingId);

  // Public state enum (mirroring ConsensusModule::State)
  enum class AgentState : std::int32_t {
    INIT = 0,
    ACTIVE = 1,
    SUSPENDED = 2,
    SNAPSHOT = 3,
    QUITTING = 4,
    TERMINATING = 5,
    CLOSED = 6
  };

  // Public getters for state/role (the setters are private)
  service::Role roleValue() const {
    return static_cast<service::Role>(static_cast<std::int32_t>(m_role));
  }
  AgentState agentState() const {
    return static_cast<AgentState>(static_cast<std::int32_t>(m_state));
  }

  // Time tracking
  std::int64_t timeOfLastLeaderUpdateNs() const {
    return m_timeOfLastLeaderUpdateNs;
  }

  // Election support methods
  std::int64_t prepareForNewLeadership(std::int64_t logPosition);
  std::int64_t calculateQuorumPosition();
  void trackCatchupCompletion(ClusterMember *follower,
                              std::int64_t leadershipTermId,
                              std::int16_t flags);
  std::unique_ptr<LogReplay> createLogReplay(std::int64_t startPosition,
                                             std::int64_t stopPosition);
  std::unique_ptr<RecordingReplication>
  newLogReplication(const std::string &archiveEndpoint,
                    const std::string &archiveResponseEndpoint,
                    std::int64_t replicationRecordingId,
                    std::int64_t stopPosition, std::int64_t nowNs);
  void joinLogAsLeader(std::int64_t leadershipTermId, std::int64_t logPosition,
                       std::int32_t logSessionId, bool isStartup);
  bool tryJoinLogAsFollower(std::shared_ptr<Image> image, bool isStartup, std::int64_t nowNs);
  bool appendNewLeadershipTermEvent(std::int64_t nowNs);
  void publishCommitPosition(std::int64_t position,
                             std::int64_t leadershipTermId);
  std::int32_t addLogPublication(std::int64_t position);
  bool isCatchupNearLive(std::int64_t position) const;
  std::int32_t catchupPoll(std::int64_t limitPosition, std::int64_t nowNs);
  void catchupInitiated(std::int64_t nowNs);
  void liveLogDestination(const std::string &destination);
  void catchupLogDestination(const std::string &destination);
  std::string liveLogDestination() const { return m_liveLogDestination; }
  std::string catchupLogDestination() const { return m_catchupLogDestination; }
  void stopAllCatchups();

  // Methods needed by Election (made public for access)
  std::int32_t pollArchiveEventsPublic() { return pollArchiveEvents(); }
  void electionCompletePublic(std::int64_t nowNs) { electionComplete(nowNs); }
  std::int32_t updateLeaderPositionPublic(std::int64_t nowNs,
                                          std::int64_t position) {
    return updateLeaderPosition(nowNs, position);
  }
  void setRole(service::Role newRole) {
    role(static_cast<Role>(static_cast<std::int32_t>(newRole)));
  }

private:
  ConsensusModule::Context &m_ctx;
  std::shared_ptr<Aeron> m_aeron;
  std::shared_ptr<AeronArchive> m_archive;
  std::shared_ptr<AeronArchive> m_extensionArchive;
  std::shared_ptr<ClusterClock> m_clusterClock;
  std::shared_ptr<IdleStrategy> m_idleStrategy;
  std::shared_ptr<security::Authenticator> m_authenticator;
  std::shared_ptr<security::AuthorisationService> m_authorisationService;
  std::shared_ptr<ConsensusModuleExtension> m_consensusModuleExtension;
  std::shared_ptr<TimerService> m_timerService;
  std::shared_ptr<ClusterMarkFile> m_markFile;
  std::shared_ptr<RecordingLog> m_recordingLog;
  std::shared_ptr<EgressPublisher> m_egressPublisher;
  std::shared_ptr<LogPublisher> m_logPublisher;
  std::shared_ptr<Election> m_election;
  std::shared_ptr<ClusterTermination> m_clusterTermination;
  std::shared_ptr<ClusterSessionProxy> m_sessionProxy;

  std::unique_ptr<LogAdapter> m_logAdapter;
  std::unique_ptr<IngressAdapter> m_ingressAdapter;
  std::unique_ptr<ConsensusAdapter> m_consensusAdapter;
  std::unique_ptr<ConsensusModuleAdapter> m_consensusModuleAdapter;
  std::unique_ptr<ServiceProxy> m_serviceProxy;
  std::unique_ptr<ConsensusPublisher> m_consensusPublisher;

  std::vector<ClusterMember> m_activeMembers;
  ClusterMember m_thisMember;
  ClusterMember m_leaderMember;
  std::unordered_map<std::int32_t, ClusterMember> m_clusterMemberByIdMap;
  std::unordered_map<std::int64_t, std::shared_ptr<ClusterSession>>
      m_sessionByIdMap;
  std::vector<std::shared_ptr<ClusterSession>> m_sessions;
  std::vector<std::shared_ptr<ClusterSession>> m_pendingUserSessions;
  std::vector<std::shared_ptr<ClusterSession>> m_rejectedUserSessions;
  std::vector<std::shared_ptr<ClusterSession>> m_redirectUserSessions;
  std::vector<std::shared_ptr<ClusterSession>> m_pendingBackupSessions;
  std::vector<std::shared_ptr<ClusterSession>> m_rejectedBackupSessions;
  std::deque<std::shared_ptr<ClusterSession>> m_uncommittedClosedSessions;

  std::vector<std::deque<aeron::cluster::ServiceAck>> m_serviceAckQueues;
  std::vector<std::shared_ptr<PendingServiceMessageTracker>>
      m_pendingServiceMessageTrackers;

  std::shared_ptr<AtomicCounter> m_commitPosition;
  std::shared_ptr<AtomicCounter> m_moduleState;
  std::shared_ptr<AtomicCounter> m_controlToggle;
  std::shared_ptr<AtomicCounter> m_nodeControlToggle;
  std::shared_ptr<AtomicCounter> m_clusterRoleCounter;
  // TODO: ReadableCounter not yet implemented in C++ wrapper, using
  // AtomicCounter instead
  std::shared_ptr<AtomicCounter> m_appendPosition;

  ExpandableArrayBuffer m_tempBuffer;

  std::int64_t m_sessionTimeoutNs;
  std::int64_t m_leaderHeartbeatIntervalNs;
  std::int64_t m_leaderHeartbeatTimeoutNs;
  std::int64_t m_unavailableCounterHandlerRegistrationId = aeron::NULL_VALUE;
  std::int64_t m_nextSessionId = 1;
  std::int64_t m_nextCommittedSessionId = 1;
  std::int64_t m_leadershipTermId = aeron::NULL_VALUE;
  std::int64_t m_expectedAckPosition = 0;
  std::int64_t m_serviceAckId = 0;
  std::int64_t m_terminationPosition = archive::client::NULL_POSITION;
  std::int64_t m_terminationLeadershipTermId = aeron::NULL_VALUE;
  std::int64_t m_notifiedCommitPosition = 0;
  std::int64_t m_lastAppendPosition = archive::client::NULL_POSITION;
  std::int64_t m_timeOfLastLogUpdateNs = 0;
  std::int64_t m_timeOfLastAppendPositionUpdateNs = 0;
  std::int64_t m_timeOfLastAppendPositionSendNs = 0;
  std::int64_t m_timeOfLastLeaderUpdateNs = 0;
  std::int64_t m_slowTickDeadlineNs = 0;
  std::int64_t m_markFileUpdateDeadlineNs = 0;
  std::int64_t m_logSubscriptionId = aeron::NULL_VALUE;
  std::int64_t m_logRecordingId = aeron::NULL_VALUE;
  std::int64_t m_logRecordingStopPosition = 0;
  std::int64_t m_logPublicationChannelTag = 0;

  std::int32_t m_serviceCount;
  std::int32_t m_memberId;
  std::chrono::milliseconds::rep m_clusterTimeUnit;

  std::string m_liveLogDestination;
  std::string m_catchupLogDestination;
  std::string m_ingressEndpoints;
  std::string m_localLogChannel;
  std::shared_ptr<Subscription> m_extensionLeaderSubscription;

  // Additional members from Java version
  std::shared_ptr<ChannelUri> m_responseChannelTemplate;
  std::shared_ptr<RecordingLog::RecoveryPlan> m_recoveryPlan;
  std::unique_ptr<StandbySnapshotReplicator> m_standbySnapshotReplicator;
  std::vector<std::int64_t> m_serviceClientIds;
  std::unordered_map<std::int64_t, std::int64_t>
      m_expiredTimerCountByCorrelationIdMap;

  // State
  enum class State : std::int32_t {
    INIT = 0,
    ACTIVE = 1,
    SUSPENDED = 2,
    SNAPSHOT = 3,
    QUITTING = 4,
    TERMINATING = 5,
    CLOSED = 6
  };

  enum class Role : std::int32_t { FOLLOWER = 0, CANDIDATE = 1, LEADER = 2 };

  State m_state = State::INIT;
  Role m_role = Role::FOLLOWER;

  // Helper methods - state and role getters/setters
  State state() const { return m_state; }
  void state(State newState);
  Role role() const { return m_role; }
  void role(Role newRole);
  void addSession(std::shared_ptr<ClusterSession> session);
  void closeSession(std::shared_ptr<ClusterSession> session);
  std::int32_t slowTickWork(std::int64_t nowNs);
  std::int32_t consensusWork(std::int64_t timestamp, std::int64_t nowNs);
  void connectIngress();
  void checkInterruptStatus();
  void runTerminationHook();
  void tryStopLogRecording();
  void unexpectedTermination();
  void enterElection(bool isEos, const std::string &reason);
  void electionComplete(std::int64_t nowNs);
  std::string refineResponseChannel(const std::string &responseChannel);
  std::string sessionInfo(const std::string &clientInfo, Header &header);
  void onScheduleTimer(std::int64_t correlationId, std::int64_t deadline);
  void onUnavailableCounter(CountersReader &countersReader,
                            std::int64_t registrationId,
                            std::int32_t counterId);
  void replicateStandbySnapshotsForStartup();
  std::shared_ptr<RecordingLog::RecoveryPlan> recoverFromBootstrapState();
  void onUnavailableIngressImage(Image &image);

  // Methods called by ConsensusModuleAdapter
  void onServiceMessage(std::int64_t clusterSessionId, AtomicBuffer &buffer,
                        std::int32_t offset, std::int32_t length);

  void onServiceCloseSession(std::int64_t clusterSessionId);

  void onCancelTimer(std::int64_t correlationId);

  void onServiceAck(std::int64_t logPosition, std::int64_t timestamp,
                    std::int64_t ackId, std::int64_t relevantId,
                    std::int32_t serviceId);

  void onClusterMembersQuery(std::int64_t correlationId,
                             bool isExtendedRequest);

  void onChallengeResponseForSession(
      std::vector<std::shared_ptr<ClusterSession>> &pendingSessions,
      std::int64_t correlationId, std::int64_t clusterSessionId,
      const std::vector<std::uint8_t> &encodedCredentials);

  // Additional helper methods
  void logAppendSessionClose(std::int32_t memberId, std::int64_t id,
                             CloseReason::Value closeReason,
                             std::int64_t leadershipTermId,
                             std::int64_t timestamp,
                             std::chrono::milliseconds::rep timeUnit);

  std::int32_t updateLeaderPosition(std::int64_t nowNs);
  std::int32_t updateLeaderPosition(std::int64_t nowNs, std::int64_t position);
  std::int32_t updateFollowerPosition(std::int64_t nowNs);
  std::int32_t
  updateFollowerPosition(std::shared_ptr<ExclusivePublication> publication,
                         std::int64_t nowNs, std::int64_t leadershipTermId,
                         std::int64_t appendPosition, std::int16_t flags);

  std::int32_t processPendingSessions(
      std::vector<std::shared_ptr<ClusterSession>> &pendingSessions,
      std::vector<std::shared_ptr<ClusterSession>> &rejectedSessions,
      std::int64_t nowNs);

  std::int32_t
  checkSessions(std::vector<std::shared_ptr<ClusterSession>> &sessions,
                std::int64_t nowNs);

  std::int32_t
  sendRedirects(std::vector<std::shared_ptr<ClusterSession>> &redirectSessions,
                std::int64_t nowNs);

  std::int32_t
  sendRejections(std::vector<std::shared_ptr<ClusterSession>> &rejectedSessions,
                 std::int64_t nowNs);

  void closeAndTerminate();
  void terminateOnServiceAck(std::int64_t logPosition);
  void snapshotOnServiceAck(std::int64_t logPosition, std::int64_t timestamp,
                            std::int32_t serviceId);
  void captureServiceAck(std::int64_t logPosition, std::int64_t ackId,
                         std::int64_t relevantId, std::int32_t serviceId);
  void logOnServiceAck(std::int32_t memberId, std::int64_t logPosition,
                       std::int64_t timestamp,
                       std::chrono::milliseconds::rep timeUnit,
                       std::int64_t ackId, std::int64_t relevantId,
                       std::int32_t serviceId);

  std::int32_t checkClusterControlToggle(std::int64_t nowNs);
  std::int32_t checkNodeControlToggle();
  std::int32_t pollArchiveEvents();
  std::int32_t pollStandbySnapshotReplication(std::int64_t nowNs);
  bool hasActiveQuorum();
  void sweepUncommittedEntriesTo(std::int64_t commitPosition);
  bool appendAction(ClusterAction::Value action, std::int64_t timestamp,
                    std::int32_t flags);

  // Log methods
  void logOnCanvassPosition(std::int32_t memberId,
                            std::int64_t logLeadershipTermId,
                            std::int64_t logPosition,
                            std::int64_t leadershipTermId,
                            std::int32_t followerMemberId,
                            std::int32_t protocolVersion);

  void logOnRequestVote(std::int32_t memberId, std::int64_t logLeadershipTermId,
                        std::int64_t logPosition, std::int64_t candidateTermId,
                        std::int32_t candidateId, std::int32_t protocolVersion);

  void logOnNewLeadershipTerm(std::int32_t memberId,
                              std::int64_t leadershipTermId,
                              std::int64_t logPosition, std::int64_t timestamp,
                              std::int64_t termBaseLogPosition,
                              std::chrono::milliseconds::rep timeUnit,
                              std::int32_t appVersion);

  void logOnAppendPosition(std::int32_t memberId, std::int64_t leadershipTermId,
                           std::int64_t logPosition,
                           std::int32_t followerMemberId, std::int16_t flags);

  void logOnCommitPosition(std::int32_t memberId, std::int64_t leadershipTermId,
                           std::int64_t logPosition,
                           std::int32_t followerMemberId);

  void logOnCatchupPosition(std::int32_t memberId,
                            std::int64_t leadershipTermId,
                            std::int64_t logPosition,
                            std::int32_t followerMemberId,
                            const std::string &catchupEndpoint);

  void logOnStopCatchup(std::int32_t memberId, std::int64_t leadershipTermId,
                        std::int32_t followerMemberId);

  void logOnTerminationPosition(std::int32_t memberId,
                                std::int64_t logLeadershipTermId,
                                std::int64_t logPosition);

  void logOnTerminationAck(std::int32_t memberId,
                           std::int64_t logLeadershipTermId,
                           std::int64_t logPosition,
                           std::int32_t senderMemberId);

  void prepareSessionsForNewTerm(bool isStartup);
  void updateMemberDetails(const ClusterMember &newLeader);
};

} // namespace cluster
} // namespace aeron
