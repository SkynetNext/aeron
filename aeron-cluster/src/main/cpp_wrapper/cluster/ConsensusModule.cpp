/*
 * Copyright 2014-2025 Real Logic Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "ConsensusModule.h"
#include "ConsensusModuleAgent.h"
#include "ConsensusModuleExtension.h"
#include "MillisecondClusterClock.h"
#include "NodeStateFile.h"
#include "WheelTimerServiceSupplier.h"
#include "client/ClusterExceptions.h"
#include "concurrent/BusySpinIdleStrategy.h"
#include "util/CloseHelper.h"
#include "util/Exceptions.h"
#include <atomic>
#include <stdexcept>

namespace aeron {
namespace cluster {

// ============================================================================
// ConsensusModule::Context Implementation
// ============================================================================

ConsensusModule::Context::Context()
    : m_isConcluded(false), m_ownsAeronClient(false), m_serviceCount(0),
      m_clusterMemberId(0), m_clusterId(0), m_appVersion(0),
      m_appointedLeaderId(aeron::NULL_VALUE), m_ingressFragmentLimit(0),
      m_logFragmentLimit(0), m_consensusStreamId(0), m_maxConcurrentSessions(0),
      m_fileSyncLevel(0), m_sessionTimeoutNs(0), m_leaderHeartbeatIntervalNs(0),
      m_leaderHeartbeatTimeoutNs(0), m_startupCanvassTimeoutNs(0),
      m_electionTimeoutNs(0), m_electionStatusIntervalNs(0),
      m_replayStreamId(0), m_ingressStreamId(0), m_isIpcIngressAllowed(false),
      m_enableControlOnConsensusChannel(false) {}

ConsensusModule::Context::~Context() { close(); }

void ConsensusModule::Context::conclude() {
  bool expected = false;
  if (!m_isConcluded.compare_exchange_strong(expected, true)) {
    throw client::ClusterException("Context already concluded", SOURCEINFO);
  }

  // Validate cluster members
  if (m_clusterMembers.empty()) {
    throw client::ClusterException(
        "ConsensusModule.Context.clusterMembers must be set", SOURCEINFO);
  }

  // Set default cluster clock if not set
  if (!m_clusterClock) {
    m_clusterClock = std::make_shared<cluster::MillisecondClusterClock>();
  }

  // Set default app version validator if not set
  if (!m_appVersionValidator) {
    m_appVersionValidator = std::make_shared<AppVersionValidator>(
        AppVersionValidator::SEMANTIC_VERSIONING_VALIDATOR);
  }

  // Set default idle strategy supplier if not set
  if (!m_idleStrategySupplier) {
    m_idleStrategySupplier =
        []() -> std::shared_ptr<::aeron::concurrent::IdleStrategy> {
      return std::make_shared<::aeron::concurrent::BusySpinIdleStrategy>();
    };
  }

  // Set default timer service supplier if not set
  if (!m_timerServiceSupplier) {
    m_timerServiceSupplier =
        std::make_shared<cluster::WheelTimerServiceSupplier>(
            m_clusterClock->timeUnit(), 0, 8, 512);
  }
}

void ConsensusModule::Context::close() {
  if (!m_isConcluded) {
    return;
  }

  util::CloseHelper::close(m_countedErrorHandler);
  util::CloseHelper::close(m_recordingLog);
  util::CloseHelper::close(m_clusterMarkFile);

  if (m_ownsAeronClient && m_aeron) {
    util::CloseHelper::close(m_aeron);
  }
}

// Getters
std::shared_ptr<Aeron> ConsensusModule::Context::aeron() const {
  return m_aeron;
}

std::shared_ptr<::aeron::cluster::service::ClusterClock>
ConsensusModule::Context::clusterClock() const {
  return m_clusterClock;
}

std::shared_ptr<::aeron::concurrent::IdleStrategy>
ConsensusModule::Context::idleStrategy() const {
  return m_idleStrategySupplier ? m_idleStrategySupplier() : nullptr;
}

std::shared_ptr<ConsensusModuleExtension>
ConsensusModule::Context::consensusModuleExtension() const {
  return m_consensusModuleExtension;
}

std::shared_ptr<::aeron::cluster::service::ClusterMarkFile>
ConsensusModule::Context::clusterMarkFile() const {
  return m_clusterMarkFile;
}

std::shared_ptr<RecordingLog> ConsensusModule::Context::recordingLog() const {
  return m_recordingLog;
}

std::shared_ptr<EgressPublisher>
ConsensusModule::Context::egressPublisher() const {
  return m_egressPublisher;
}

std::shared_ptr<LogPublisher> ConsensusModule::Context::logPublisher() const {
  return m_logPublisher;
}

std::shared_ptr<::aeron::concurrent::errors::DistinctErrorLog>
ConsensusModule::Context::errorLog() const {
  return m_errorLog;
}

std::shared_ptr<::aeron::concurrent::CountedErrorHandler>
ConsensusModule::Context::countedErrorHandler() const {
  return m_countedErrorHandler;
}

std::shared_ptr<AppVersionValidator>
ConsensusModule::Context::appVersionValidator() const {
  return m_appVersionValidator;
}

std::shared_ptr<::aeron::concurrent::AtomicCounter>
ConsensusModule::Context::timedOutClientCounter() const {
  return m_timedOutClientCounter;
}

std::shared_ptr<::aeron::concurrent::AtomicCounter>
ConsensusModule::Context::snapshotCounter() const {
  return m_snapshotCounter;
}

std::int32_t ConsensusModule::Context::serviceCount() const {
  return m_serviceCount;
}

std::int32_t ConsensusModule::Context::clusterMemberId() const {
  return m_clusterMemberId;
}

std::int32_t ConsensusModule::Context::clusterId() const { return m_clusterId; }

std::int32_t ConsensusModule::Context::appVersion() const {
  return m_appVersion;
}

std::int32_t ConsensusModule::Context::appointedLeaderId() const {
  return m_appointedLeaderId;
}

std::int32_t ConsensusModule::Context::ingressFragmentLimit() const {
  return m_ingressFragmentLimit;
}

std::int32_t ConsensusModule::Context::logFragmentLimit() const {
  return m_logFragmentLimit;
}

std::int32_t ConsensusModule::Context::consensusStreamId() const {
  return m_consensusStreamId;
}

std::int32_t ConsensusModule::Context::maxConcurrentSessions() const {
  return m_maxConcurrentSessions;
}

std::int32_t ConsensusModule::Context::fileSyncLevel() const {
  return m_fileSyncLevel;
}

std::int64_t ConsensusModule::Context::sessionTimeoutNs() const {
  return m_sessionTimeoutNs;
}

std::int64_t ConsensusModule::Context::leaderHeartbeatIntervalNs() const {
  return m_leaderHeartbeatIntervalNs;
}

std::int64_t ConsensusModule::Context::leaderHeartbeatTimeoutNs() const {
  return m_leaderHeartbeatTimeoutNs;
}

std::int64_t ConsensusModule::Context::startupCanvassTimeoutNs() const {
  return m_startupCanvassTimeoutNs;
}

std::int64_t ConsensusModule::Context::electionTimeoutNs() const {
  return m_electionTimeoutNs;
}

std::int64_t ConsensusModule::Context::electionStatusIntervalNs() const {
  return m_electionStatusIntervalNs;
}

std::string ConsensusModule::Context::clusterMembers() const {
  return m_clusterMembers;
}

std::string ConsensusModule::Context::memberEndpoints() const {
  return m_memberEndpoints;
}

std::string ConsensusModule::Context::consensusChannel() const {
  return m_consensusChannel;
}

std::string ConsensusModule::Context::replayChannel() const {
  return m_replayChannel;
}

std::int32_t ConsensusModule::Context::replayStreamId() const {
  return m_replayStreamId;
}

std::string ConsensusModule::Context::replicationChannel() const {
  return m_replicationChannel;
}

std::string ConsensusModule::Context::leaderArchiveControlChannel() const {
  return m_leaderArchiveControlChannel;
}

std::string ConsensusModule::Context::ingressChannel() const {
  return m_ingressChannel;
}

std::int32_t ConsensusModule::Context::ingressStreamId() const {
  return m_ingressStreamId;
}

std::string ConsensusModule::Context::egressChannel() const {
  return m_egressChannel;
}

std::string ConsensusModule::Context::logPublicationChannel() const {
  return m_logPublicationChannel;
}

std::int32_t ConsensusModule::Context::logStreamId() const {
  return m_logStreamId;
}

bool ConsensusModule::Context::isLogMdc() const { return m_isLogMdc; }

bool ConsensusModule::Context::isIpcIngressAllowed() const {
  return m_isIpcIngressAllowed;
}

bool ConsensusModule::Context::ownsAeronClient() const {
  return m_ownsAeronClient;
}

bool ConsensusModule::Context::enableControlOnConsensusChannel() const {
  return m_enableControlOnConsensusChannel;
}

std::shared_ptr<ConsensusModule::Context::AuthenticatorSupplier>
ConsensusModule::Context::authenticatorSupplier() const {
  return m_authenticatorSupplier;
}

std::shared_ptr<ConsensusModule::Context::AuthorisationServiceSupplier>
ConsensusModule::Context::authorisationServiceSupplier() const {
  return m_authorisationServiceSupplier;
}

std::shared_ptr<TimerServiceSupplier>
ConsensusModule::Context::timerServiceSupplier() const {
  return m_timerServiceSupplier;
}

std::shared_ptr<archive::client::Context>
ConsensusModule::Context::archiveContext() const {
  return m_archiveContext;
}

std::shared_ptr<ConsensusModuleStateExport>
ConsensusModule::Context::bootstrapState() const {
  return m_bootstrapState;
}

std::function<void()> ConsensusModule::Context::terminationHook() const {
  return m_terminationHook;
}

std::string ConsensusModule::Context::agentRoleName() const {
  return m_agentRoleName;
}

// Setters (fluent API)
ConsensusModule::Context &
ConsensusModule::Context::aeron(std::shared_ptr<Aeron> aeron) {
  m_aeron = aeron;
  return *this;
}

ConsensusModule::Context &ConsensusModule::Context::clusterClock(
    std::shared_ptr<::aeron::cluster::service::ClusterClock> clock) {
  m_clusterClock = clock;
  return *this;
}

ConsensusModule::Context &ConsensusModule::Context::idleStrategySupplier(
    std::function<std::shared_ptr<::aeron::concurrent::IdleStrategy>()>
        supplier) {
  m_idleStrategySupplier = supplier;
  return *this;
}

ConsensusModule::Context &ConsensusModule::Context::consensusModuleExtension(
    std::shared_ptr<ConsensusModuleExtension> extension) {
  m_consensusModuleExtension = extension;
  return *this;
}

ConsensusModule::Context &ConsensusModule::Context::clusterMarkFile(
    std::shared_ptr<::aeron::cluster::service::ClusterMarkFile> markFile) {
  m_clusterMarkFile = markFile;
  return *this;
}

ConsensusModule::Context &ConsensusModule::Context::recordingLog(
    std::shared_ptr<RecordingLog> recordingLog) {
  m_recordingLog = recordingLog;
  return *this;
}

ConsensusModule::Context &ConsensusModule::Context::egressPublisher(
    std::shared_ptr<EgressPublisher> publisher) {
  m_egressPublisher = publisher;
  return *this;
}

ConsensusModule::Context &ConsensusModule::Context::logPublisher(
    std::shared_ptr<LogPublisher> publisher) {
  m_logPublisher = publisher;
  return *this;
}

ConsensusModule::Context &ConsensusModule::Context::errorLog(
    std::shared_ptr<::aeron::concurrent::errors::DistinctErrorLog> errorLog) {
  m_errorLog = errorLog;
  return *this;
}

ConsensusModule::Context &ConsensusModule::Context::countedErrorHandler(
    std::shared_ptr<::aeron::concurrent::CountedErrorHandler> handler) {
  m_countedErrorHandler = handler;
  return *this;
}

ConsensusModule::Context &ConsensusModule::Context::appVersionValidator(
    std::shared_ptr<AppVersionValidator> validator) {
  m_appVersionValidator = validator;
  return *this;
}

ConsensusModule::Context &ConsensusModule::Context::timedOutClientCounter(
    std::shared_ptr<::aeron::concurrent::AtomicCounter> counter) {
  m_timedOutClientCounter = counter;
  return *this;
}

ConsensusModule::Context &ConsensusModule::Context::snapshotCounter(
    std::shared_ptr<::aeron::concurrent::AtomicCounter> counter) {
  m_snapshotCounter = counter;
  return *this;
}

ConsensusModule::Context &
ConsensusModule::Context::serviceCount(std::int32_t count) {
  m_serviceCount = count;
  return *this;
}

ConsensusModule::Context &
ConsensusModule::Context::clusterMemberId(std::int32_t id) {
  m_clusterMemberId = id;
  return *this;
}

ConsensusModule::Context &ConsensusModule::Context::clusterId(std::int32_t id) {
  m_clusterId = id;
  return *this;
}

ConsensusModule::Context &
ConsensusModule::Context::appVersion(std::int32_t version) {
  m_appVersion = version;
  return *this;
}

ConsensusModule::Context &
ConsensusModule::Context::ingressFragmentLimit(std::int32_t limit) {
  m_ingressFragmentLimit = limit;
  return *this;
}

ConsensusModule::Context &
ConsensusModule::Context::logFragmentLimit(std::int32_t limit) {
  m_logFragmentLimit = limit;
  return *this;
}

ConsensusModule::Context &
ConsensusModule::Context::consensusStreamId(std::int32_t streamId) {
  m_consensusStreamId = streamId;
  return *this;
}

ConsensusModule::Context &
ConsensusModule::Context::maxConcurrentSessions(std::int32_t max) {
  m_maxConcurrentSessions = max;
  return *this;
}

ConsensusModule::Context &
ConsensusModule::Context::fileSyncLevel(std::int32_t level) {
  m_fileSyncLevel = level;
  return *this;
}

ConsensusModule::Context &
ConsensusModule::Context::sessionTimeoutNs(std::int64_t timeoutNs) {
  m_sessionTimeoutNs = timeoutNs;
  return *this;
}

ConsensusModule::Context &
ConsensusModule::Context::leaderHeartbeatIntervalNs(std::int64_t intervalNs) {
  m_leaderHeartbeatIntervalNs = intervalNs;
  return *this;
}

ConsensusModule::Context &
ConsensusModule::Context::leaderHeartbeatTimeoutNs(std::int64_t timeoutNs) {
  m_leaderHeartbeatTimeoutNs = timeoutNs;
  return *this;
}

ConsensusModule::Context &
ConsensusModule::Context::clusterMembers(const std::string &members) {
  m_clusterMembers = members;
  return *this;
}

ConsensusModule::Context &
ConsensusModule::Context::memberEndpoints(const std::string &endpoints) {
  m_memberEndpoints = endpoints;
  return *this;
}

ConsensusModule::Context &
ConsensusModule::Context::consensusChannel(const std::string &channel) {
  m_consensusChannel = channel;
  return *this;
}

ConsensusModule::Context &
ConsensusModule::Context::replayChannel(const std::string &channel) {
  m_replayChannel = channel;
  return *this;
}

ConsensusModule::Context &
ConsensusModule::Context::replayStreamId(std::int32_t streamId) {
  m_replayStreamId = streamId;
  return *this;
}

ConsensusModule::Context &
ConsensusModule::Context::replicationChannel(const std::string &channel) {
  m_replicationChannel = channel;
  return *this;
}

ConsensusModule::Context &ConsensusModule::Context::leaderArchiveControlChannel(
    const std::string &channel) {
  m_leaderArchiveControlChannel = channel;
  return *this;
}

ConsensusModule::Context &
ConsensusModule::Context::ingressChannel(const std::string &channel) {
  m_ingressChannel = channel;
  return *this;
}

ConsensusModule::Context &
ConsensusModule::Context::ingressStreamId(std::int32_t streamId) {
  m_ingressStreamId = streamId;
  return *this;
}

ConsensusModule::Context &
ConsensusModule::Context::egressChannel(const std::string &channel) {
  m_egressChannel = channel;
  return *this;
}

ConsensusModule::Context &
ConsensusModule::Context::isIpcIngressAllowed(bool allowed) {
  m_isIpcIngressAllowed = allowed;
  return *this;
}

ConsensusModule::Context &ConsensusModule::Context::ownsAeronClient(bool owns) {
  m_ownsAeronClient = owns;
  return *this;
}

ConsensusModule::Context &
ConsensusModule::Context::enableControlOnConsensusChannel(bool enable) {
  m_enableControlOnConsensusChannel = enable;
  return *this;
}

ConsensusModule::Context &ConsensusModule::Context::authenticatorSupplier(
    std::shared_ptr<AuthenticatorSupplier> supplier) {
  m_authenticatorSupplier = supplier;
  return *this;
}

ConsensusModule::Context &
ConsensusModule::Context::authorisationServiceSupplier(
    std::shared_ptr<AuthorisationServiceSupplier> supplier) {
  m_authorisationServiceSupplier = supplier;
  return *this;
}

ConsensusModule::Context &ConsensusModule::Context::timerServiceSupplier(
    std::shared_ptr<TimerServiceSupplier> supplier) {
  m_timerServiceSupplier = supplier;
  return *this;
}

ConsensusModule::Context &ConsensusModule::Context::archiveContext(
    std::shared_ptr<archive::client::Context> ctx) {
  m_archiveContext = ctx;
  return *this;
}

ConsensusModule::Context &ConsensusModule::Context::bootstrapState(
    std::shared_ptr<ConsensusModuleStateExport> state) {
  m_bootstrapState = state;
  return *this;
}

ConsensusModule::Context &
ConsensusModule::Context::terminationHook(std::function<void()> hook) {
  m_terminationHook = hook;
  return *this;
}

ConsensusModule::Context &
ConsensusModule::Context::agentRoleName(const std::string &name) {
  m_agentRoleName = name;
  return *this;
}

std::shared_ptr<::aeron::concurrent::AtomicCounter>
ConsensusModule::Context::electionStateCounter() const {
  return m_electionStateCounter;
}

std::shared_ptr<::aeron::concurrent::AtomicCounter>
ConsensusModule::Context::electionCounter() const {
  return m_electionCounter;
}

std::shared_ptr<::aeron::concurrent::AtomicCounter>
ConsensusModule::Context::commitPositionCounter() const {
  return m_commitPositionCounter;
}

std::shared_ptr<NodeStateFile> ConsensusModule::Context::nodeStateFile() const {
  return m_nodeStateFile;
}

std::shared_ptr<::aeron::concurrent::EpochClock>
ConsensusModule::Context::epochClock() const {
  return m_epochClock;
}

// ============================================================================
// ConsensusModule Implementation
// ============================================================================

ConsensusModule::ConsensusModule(std::shared_ptr<Context> ctx) : m_ctx(ctx) {
  try {
    ctx->conclude();
    m_agent = std::make_shared<ConsensusModuleAgent>(*ctx);
  } catch (const std::exception &ex) {
    if (ctx->clusterMarkFile()) {
      ctx->clusterMarkFile()->signalFailedStart();
    }
    ctx->close();
    throw;
  }
}

std::shared_ptr<ConsensusModule> ConsensusModule::launch() {
  return launch(std::make_shared<Context>());
}

std::shared_ptr<ConsensusModule>
ConsensusModule::launch(std::shared_ptr<Context> ctx) {
  return std::make_shared<ConsensusModule>(ctx);
}

ConsensusModule::Context &ConsensusModule::context() { return *m_ctx; }

void ConsensusModule::close() {
  if (m_agent) {
    m_agent->onClose();
    m_agent.reset();
  }
  if (m_ctx) {
    m_ctx->close();
  }
}

} // namespace cluster
} // namespace aeron
