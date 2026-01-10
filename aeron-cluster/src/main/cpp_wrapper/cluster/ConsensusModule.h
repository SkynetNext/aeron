#pragma once

#include "Aeron.h"
#include "AppVersionValidator.h"
#include "ClusterMember.h"
#include "ConsensusModuleStateExport.h"
#include "EgressPublisher.h"
#include "LogPublisher.h"
#include "RecordingLog.h"
#include "TimerService.h"
#include "TimerServiceSupplier.h"
#include "client/archive/AeronArchive.h"
#include "client/archive/ArchiveContext.h"
#include "concurrent/AtomicCounter.h"
#include "concurrent/CountedErrorHandler.h"
#include "concurrent/EpochClock.h"
#include "concurrent/IdleStrategy.h"
#include "concurrent/errors/DistinctErrorLog.h"
#include "service/ClusterClock.h"
#include "service/ClusterMarkFile.h"
#include <cstdint>
#include <functional>
#include <memory>
#include <string>

namespace aeron {
namespace cluster {

// Forward declarations to break circular dependency
class ConsensusModuleControl;
class ConsensusModuleExtension;
class ConsensusModuleAgent;

namespace security {
class Authenticator;
class AuthorisationService;
} // namespace security

/**
 * Component which resides on each node and is responsible for coordinating
 * consensus within a cluster in concert with the lifecycle of clustered
 * services.
 */
class ConsensusModule {
public:
  /**
   * Programmable overrides for configuring the ConsensusModule in a cluster.
   * The context will be owned by ConsensusModuleAgent after a successful launch
   * and closed via close().
   */
  class Context {
  public:
    Context();
    ~Context();

    // Copy and move semantics
    Context(const Context &) = delete;
    Context &operator=(const Context &) = delete;
    Context(Context &&) = delete;
    Context &operator=(Context &&) = delete;

    /**
     * Conclude configuration by setting up defaults when specifics are not
     * provided.
     */
    void conclude();

    /**
     * Close the context and release resources.
     */
    void close();

    // Getters for configuration values
    std::shared_ptr<Aeron> aeron() const;
    std::shared_ptr<service::ClusterClock> clusterClock() const;
    std::shared_ptr<concurrent::IdleStrategy> idleStrategy() const;
    std::shared_ptr<ConsensusModuleExtension> consensusModuleExtension() const;
    std::shared_ptr<service::ClusterMarkFile> clusterMarkFile() const;
    std::shared_ptr<RecordingLog> recordingLog() const;
    std::shared_ptr<EgressPublisher> egressPublisher() const;
    std::shared_ptr<LogPublisher> logPublisher() const;
    std::shared_ptr<concurrent::errors::DistinctErrorLog> errorLog() const;
    std::shared_ptr<concurrent::CountedErrorHandler>
    countedErrorHandler() const;
    std::shared_ptr<AppVersionValidator> appVersionValidator() const;
    std::shared_ptr<concurrent::AtomicCounter> timedOutClientCounter() const;
    std::shared_ptr<concurrent::AtomicCounter> snapshotCounter() const;
    std::shared_ptr<concurrent::AtomicCounter> electionStateCounter() const;
    std::shared_ptr<concurrent::AtomicCounter> electionCounter() const;
    std::shared_ptr<concurrent::AtomicCounter> commitPositionCounter() const;

    std::shared_ptr<class NodeStateFile> nodeStateFile() const;
    std::shared_ptr<concurrent::EpochClock> epochClock() const;

    std::int32_t serviceCount() const;
    std::int32_t clusterMemberId() const;
    std::int32_t clusterId() const;
    std::int32_t appVersion() const;
    std::int32_t appointedLeaderId() const;
    std::int32_t ingressFragmentLimit() const;
    std::int32_t logFragmentLimit() const;
    std::int32_t consensusStreamId() const;
    std::int32_t maxConcurrentSessions() const;
    std::int32_t fileSyncLevel() const;

    std::int64_t sessionTimeoutNs() const;
    std::int64_t leaderHeartbeatIntervalNs() const;
    std::int64_t leaderHeartbeatTimeoutNs() const;
    std::int64_t startupCanvassTimeoutNs() const;
    std::int64_t electionTimeoutNs() const;
    std::int64_t electionStatusIntervalNs() const;

    std::string clusterMembers() const;
    std::string memberEndpoints() const;
    std::string consensusChannel() const;
    std::string replayChannel() const;
    std::int32_t replayStreamId() const;
    std::string replicationChannel() const;
    std::string leaderArchiveControlChannel() const;
    std::string ingressChannel() const;
    std::int32_t ingressStreamId() const;
    std::string egressChannel() const;
    std::string logPublicationChannel() const;
    std::int32_t logStreamId() const;
    bool isLogMdc() const;

    bool isIpcIngressAllowed() const;
    bool ownsAeronClient() const;
    bool enableControlOnConsensusChannel() const;

    // Supplier getters
    class AuthenticatorSupplier {
    public:
      virtual ~AuthenticatorSupplier() = default;
      virtual std::shared_ptr<security::Authenticator> get() = 0;
    };

    class AuthorisationServiceSupplier {
    public:
      virtual ~AuthorisationServiceSupplier() = default;
      virtual std::shared_ptr<security::AuthorisationService> get() = 0;
    };

    std::shared_ptr<AuthenticatorSupplier> authenticatorSupplier() const;
    std::shared_ptr<AuthorisationServiceSupplier>
    authorisationServiceSupplier() const;
    std::shared_ptr<TimerServiceSupplier> timerServiceSupplier() const;

    // Archive context
    std::shared_ptr<archive::client::Context> archiveContext() const;

    // Bootstrap state
    std::shared_ptr<ConsensusModuleStateExport> bootstrapState() const;

    // Termination hook
    std::function<void()> terminationHook() const;

    // Setters (fluent API)
    Context &aeron(std::shared_ptr<Aeron> aeron);
    Context &clusterClock(std::shared_ptr<service::ClusterClock> clock);
    Context &idleStrategySupplier(
        std::function<std::shared_ptr<concurrent::IdleStrategy>()> supplier);
    Context &consensusModuleExtension(
        std::shared_ptr<ConsensusModuleExtension> extension);
    Context &
    clusterMarkFile(std::shared_ptr<service::ClusterMarkFile> markFile);
    Context &recordingLog(std::shared_ptr<RecordingLog> recordingLog);
    Context &egressPublisher(std::shared_ptr<EgressPublisher> publisher);
    Context &logPublisher(std::shared_ptr<LogPublisher> publisher);
    Context &
    errorLog(std::shared_ptr<concurrent::errors::DistinctErrorLog> errorLog);
    Context &countedErrorHandler(
        std::shared_ptr<concurrent::CountedErrorHandler> handler);
    Context &
    appVersionValidator(std::shared_ptr<AppVersionValidator> validator);
    Context &
    timedOutClientCounter(std::shared_ptr<concurrent::AtomicCounter> counter);
    Context &
    snapshotCounter(std::shared_ptr<concurrent::AtomicCounter> counter);

    Context &serviceCount(std::int32_t count);
    Context &clusterMemberId(std::int32_t id);
    Context &clusterId(std::int32_t id);
    Context &appVersion(std::int32_t version);
    Context &ingressFragmentLimit(std::int32_t limit);
    Context &logFragmentLimit(std::int32_t limit);
    Context &consensusStreamId(std::int32_t streamId);
    Context &maxConcurrentSessions(std::int32_t max);
    Context &fileSyncLevel(std::int32_t level);

    Context &sessionTimeoutNs(std::int64_t timeoutNs);
    Context &leaderHeartbeatIntervalNs(std::int64_t intervalNs);
    Context &leaderHeartbeatTimeoutNs(std::int64_t timeoutNs);

    Context &clusterMembers(const std::string &members);
    Context &memberEndpoints(const std::string &endpoints);
    Context &consensusChannel(const std::string &channel);
    Context &replayChannel(const std::string &channel);
    Context &replayStreamId(std::int32_t streamId);
    Context &replicationChannel(const std::string &channel);
    Context &leaderArchiveControlChannel(const std::string &channel);
    Context &ingressChannel(const std::string &channel);
    Context &ingressStreamId(std::int32_t streamId);
    Context &egressChannel(const std::string &channel);

    Context &isIpcIngressAllowed(bool allowed);
    Context &ownsAeronClient(bool owns);
    Context &enableControlOnConsensusChannel(bool enable);

    Context &
    authenticatorSupplier(std::shared_ptr<AuthenticatorSupplier> supplier);
    Context &authorisationServiceSupplier(
        std::shared_ptr<AuthorisationServiceSupplier> supplier);
    Context &
    timerServiceSupplier(std::shared_ptr<TimerServiceSupplier> supplier);

    Context &archiveContext(std::shared_ptr<archive::client::Context> ctx);
    Context &bootstrapState(std::shared_ptr<ConsensusModuleStateExport> state);
    Context &terminationHook(std::function<void()> hook);

    std::string agentRoleName() const;
    Context &agentRoleName(const std::string &name);

  private:
    std::atomic<bool> m_isConcluded;
    bool m_ownsAeronClient;
    std::string m_aeronDirectoryName;
    std::shared_ptr<Aeron> m_aeron;

    std::shared_ptr<service::ClusterClock> m_clusterClock;
    std::function<std::shared_ptr<concurrent::IdleStrategy>()>
        m_idleStrategySupplier;
    std::shared_ptr<ConsensusModuleExtension> m_consensusModuleExtension;
    std::shared_ptr<service::ClusterMarkFile> m_clusterMarkFile;
    std::shared_ptr<RecordingLog> m_recordingLog;
    std::shared_ptr<EgressPublisher> m_egressPublisher;
    std::shared_ptr<LogPublisher> m_logPublisher;
    std::shared_ptr<concurrent::errors::DistinctErrorLog> m_errorLog;
    std::shared_ptr<concurrent::CountedErrorHandler> m_countedErrorHandler;
    std::shared_ptr<AppVersionValidator> m_appVersionValidator;
    std::shared_ptr<concurrent::AtomicCounter> m_timedOutClientCounter;
    std::shared_ptr<concurrent::AtomicCounter> m_snapshotCounter;
    std::shared_ptr<concurrent::AtomicCounter> m_electionStateCounter;
    std::shared_ptr<concurrent::AtomicCounter> m_electionCounter;
    std::shared_ptr<concurrent::AtomicCounter> m_commitPositionCounter;
    std::shared_ptr<class NodeStateFile> m_nodeStateFile;
    std::shared_ptr<concurrent::EpochClock> m_epochClock;

    std::int32_t m_serviceCount;
    std::int32_t m_clusterMemberId;
    std::int32_t m_clusterId;
    std::int32_t m_appVersion;
    std::int32_t m_appointedLeaderId;
    std::int32_t m_ingressFragmentLimit;
    std::int32_t m_logFragmentLimit;
    std::int32_t m_consensusStreamId;
    std::int32_t m_maxConcurrentSessions;
    std::int32_t m_fileSyncLevel;

    std::int64_t m_sessionTimeoutNs;
    std::int64_t m_leaderHeartbeatIntervalNs;
    std::int64_t m_leaderHeartbeatTimeoutNs;
    std::int64_t m_startupCanvassTimeoutNs;
    std::int64_t m_electionTimeoutNs;
    std::int64_t m_electionStatusIntervalNs;

    std::string m_clusterMembers;
    std::string m_memberEndpoints;
    std::string m_consensusChannel;
    std::string m_replayChannel;
    std::int32_t m_replayStreamId;
    std::string m_replicationChannel;
    std::string m_leaderArchiveControlChannel;
    std::string m_ingressChannel;
    std::int32_t m_ingressStreamId;
    std::string m_egressChannel;
    std::string m_logPublicationChannel;
    std::int32_t m_logStreamId = 0;
    bool m_isLogMdc = false;

    bool m_isIpcIngressAllowed;
    bool m_enableControlOnConsensusChannel;

    std::shared_ptr<AuthenticatorSupplier> m_authenticatorSupplier;
    std::shared_ptr<AuthorisationServiceSupplier>
        m_authorisationServiceSupplier;
    std::shared_ptr<TimerServiceSupplier> m_timerServiceSupplier;

    std::shared_ptr<archive::client::Context> m_archiveContext;
    std::shared_ptr<ConsensusModuleStateExport> m_bootstrapState;
    std::function<void()> m_terminationHook;

    std::string m_agentRoleName;
  };

  /**
   * Launch a ConsensusModule using a default configuration.
   *
   * @return a new instance of a ConsensusModule.
   */
  static std::shared_ptr<ConsensusModule> launch();

  /**
   * Launch a ConsensusModule by providing a configuration context.
   *
   * @param ctx for the configuration parameters.
   * @return a new instance of a ConsensusModule.
   */
  static std::shared_ptr<ConsensusModule> launch(std::shared_ptr<Context> ctx);

  /**
   * Get the ConsensusModule::Context that is used by this ConsensusModule.
   *
   * @return the ConsensusModule::Context that is used by this ConsensusModule.
   */
  Context &context();

  /**
   * Close the ConsensusModule and release resources.
   */
  void close();

  // Constructor made public for launch() method
  ConsensusModule(std::shared_ptr<Context> ctx);

private:
  std::shared_ptr<Context> m_ctx;
  std::shared_ptr<ConsensusModuleAgent> m_agent;
};

} // namespace cluster
} // namespace aeron
