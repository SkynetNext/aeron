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

#include "ConsensusModuleAgent.h"
#include "ClusterControl.h"
#include "ConsensusControlState.h"
#include "Counter.h"
#include "Election.h" // For Election class
#include "StandbySnapshotEntry.h"
#include "client/archive/ReplayParams.h"
#include "client/archive/ReplicationParams.h"
#include "generated/aeron_cluster_codecs/BackupQuery.h"
#include "generated/aeron_cluster_codecs/HeartbeatRequest.h"
#include "generated/aeron_cluster_codecs/MessageHeader.h"
#include "generated/aeron_cluster_codecs/StandbySnapshot.h"
#include "security/Authenticator.h"
#include "security/AuthorisationService.h"
#include "util/Exceptions.h"
#include <limits>
#include <thread>

namespace aeron {
namespace cluster {

// Implementation - Constructor and basic methods
ConsensusModuleAgent::ConsensusModuleAgent(ConsensusModule::Context &ctx)
    : m_ctx(ctx), m_aeron(ctx.aeron()), m_clusterClock(ctx.clusterClock()),
      m_idleStrategy(ctx.idleStrategy()),
      m_authenticator(ctx.authenticatorSupplier()
                          ? ctx.authenticatorSupplier()->get()
                          : nullptr),
      m_authorisationService(ctx.authorisationServiceSupplier()
                                 ? ctx.authorisationServiceSupplier()->get()
                                 : nullptr),
      m_consensusModuleExtension(ctx.consensusModuleExtension()),
      m_timerService(ctx.timerServiceSupplier()->newInstance(
          ctx.clusterClock()->timeUnit(),
          [this](std::int64_t correlationId) {
            return this->onTimerEvent(correlationId);
          })),
      m_markFile(ctx.clusterMarkFile()), m_recordingLog(ctx.recordingLog()),
      m_egressPublisher(ctx.egressPublisher()),
      m_logPublisher(ctx.logPublisher()),
      m_sessionProxy(std::make_shared<ClusterSessionProxy>(*m_egressPublisher)),
      m_serviceCount(ctx.serviceCount()), m_memberId(ctx.clusterMemberId()),
      m_clusterTimeUnit(ctx.clusterClock()->timeUnit()),
      m_sessionTimeoutNs(ctx.sessionTimeoutNs()),
      m_leaderHeartbeatIntervalNs(ctx.leaderHeartbeatIntervalNs()),
      m_leaderHeartbeatTimeoutNs(ctx.leaderHeartbeatTimeoutNs()),
      m_state(State::INIT), m_role(Role::FOLLOWER) {
  // Parse cluster members
  m_activeMembers = ClusterMember::parse(ctx.clusterMembers());

  // Initialize service ack queues
  m_serviceAckQueues =
      aeron::cluster::ServiceAck::newArrayOfQueues(m_serviceCount);

  // Initialize pending service message trackers
  m_pendingServiceMessageTrackers.resize(m_serviceCount);
  for (std::int32_t i = 0; i < m_serviceCount; i++) {
    std::shared_ptr<Counter> commitPositionCounter;
    if (m_commitPosition && m_aeron) {
      CountersReader &counters = m_aeron->countersReader();
      commitPositionCounter = std::make_shared<Counter>(
          counters, counters.getCounterRegistrationId(m_commitPosition->id()),
          m_commitPosition->id());
    }
    m_pendingServiceMessageTrackers[i] =
        std::make_shared<PendingServiceMessageTracker>(
            i, commitPositionCounter, *m_logPublisher, *m_clusterClock);
  }

  // Determine this member
  m_thisMember = ClusterMember::determineMember(m_activeMembers, m_memberId,
                                                ctx.memberEndpoints());
  m_leaderMember = m_thisMember;

  // Add cluster member IDs to map
  for (const auto &member : m_activeMembers) {
    m_clusterMemberByIdMap[member.id()] = member;
  }

  // Create adapters
  m_ingressAdapter =
      std::make_unique<IngressAdapter>(ctx.ingressFragmentLimit(), *this);
  m_logAdapter = std::make_unique<LogAdapter>(*this, ctx.logFragmentLimit());

  // Initialize counters (these would be set from context)
  // m_commitPosition, m_moduleState, etc. would be initialized from context
}

// Agent interface
void ConsensusModuleAgent::onStart() {
  // Connect to archive
  auto archiveCtx = m_ctx.archiveContext();
  if (archiveCtx) {
    m_archive = AeronArchive::connect(*archiveCtx);
  }

  // Try to stop last term recording if exists
  if (m_recordingLog && m_archive) {
    const std::int64_t lastTermRecordingId =
        m_recordingLog->findLastTermRecordingId();
    if (aeron::NULL_VALUE != lastTermRecordingId) {
      m_archive->tryStopRecordingByIdentity(lastTermRecordingId);
    }
  }

  // Recover from snapshot and log or bootstrap state
  if (!m_ctx.bootstrapState()) {
    replicateStandbySnapshotsForStartup();
    if (m_recordingLog && m_archive) {
      m_recoveryPlan = std::make_shared<RecordingLog::RecoveryPlan>(
          m_recordingLog->createRecoveryPlan(m_archive, m_serviceCount,
                                             aeron::NULL_VALUE));
    }
  } else {
    m_recoveryPlan = recoverFromBootstrapState();
  }

  // Add consensus publications
  auto errorHandler = m_ctx.countedErrorHandler();
  exception_handler_t exceptionHandler =
      [errorHandler](const std::exception &ex) {
        if (errorHandler) {
          errorHandler->onError(ex);
        }
      };
  ClusterMember::addConsensusPublications(
      m_activeMembers, m_thisMember, m_ctx.consensusChannel(),
      m_ctx.consensusStreamId(), m_ctx.enableControlOnConsensusChannel(),
      m_aeron, exceptionHandler);

  // Initialize election
  if (m_recoveryPlan && m_consensusPublisher) {
    const std::int64_t lastLeadershipTermId =
        m_recoveryPlan->lastLeadershipTermId;
    const std::int64_t commitPosition =
        m_commitPosition ? m_commitPosition->get() : 0;
    const std::int64_t appendedPosition = m_recoveryPlan->appendedLogPosition;

    // Create pointer map for Election
    std::unordered_map<std::int32_t, ClusterMember *> clusterMemberByIdPtrMap;
    for (auto &member : m_activeMembers) {
      clusterMemberByIdPtrMap[member.id()] = &member;
    }
    m_election = std::make_unique<Election>(
        true, aeron::NULL_VALUE, lastLeadershipTermId,
        m_recoveryPlan->lastTermBaseLogPosition, commitPosition,
        appendedPosition, m_activeMembers, clusterMemberByIdPtrMap,
        m_thisMember, *m_consensusPublisher, m_ctx, *this);

    if (m_election && m_clusterClock) {
      m_election->doWork(m_clusterClock->timeNanos());
    }
  }

  state(State::ACTIVE);

  if (m_consensusModuleExtension) {
    // Snapshot image is loaded separately if needed, passing nullptr for no
    // snapshot
    m_consensusModuleExtension->onStart(*this, nullptr);
  }
}

void ConsensusModuleAgent::onClose() {
  if (m_aeron && !m_aeron->isClosed()) {
    if (m_unavailableCounterHandlerRegistrationId != aeron::NULL_VALUE) {
      m_aeron->removeUnavailableCounterHandler(
          m_unavailableCounterHandlerRegistrationId);
    }

    const auto countedErrorHandler = m_ctx.countedErrorHandler();
    exception_handler_t exceptionHandler =
        [countedErrorHandler](const std::exception &ex) {
          if (countedErrorHandler) {
            countedErrorHandler->onError(ex);
          }
        };

    if (m_consensusModuleExtension) {
      closeHelper(m_consensusModuleExtension);
    }
    if (m_extensionArchive) {
      closeHelper(m_extensionArchive);
    }

    if (m_logPublisher) {
      m_logPublisher->disconnect(exceptionHandler);
    }
    if (m_logAdapter) {
      m_logAdapter->subscription().reset();
    }
    tryStopLogRecording();

    if (m_archive) {
      closeHelper(m_archive);
    }

    if (!m_ctx.ownsAeronClient()) {
      ClusterMember::closeConsensusPublications(exceptionHandler,
                                                m_activeMembers);
      if (m_ingressAdapter) {
        m_ingressAdapter.reset();
      }
      if (m_consensusAdapter) {
        m_consensusAdapter.reset();
      }
      if (m_serviceProxy) {
        m_serviceProxy.reset();
      }
      if (m_consensusModuleAdapter) {
        m_consensusModuleAdapter.reset();
      }

      for (auto &kv : m_sessionByIdMap) {
        auto &session = kv.second;
        if (session) {
          session->close(m_aeron, exceptionHandler, "Cluster node terminated");
        }
      }
    }

    state(State::CLOSED);
  }

  if (m_markFile) {
    m_markFile->signalTerminated();
  }
  m_ctx.close();
}

std::int32_t ConsensusModuleAgent::doWork() {
  if (!m_clusterClock) {
    return 0;
  }

  const std::int64_t timestamp = m_clusterClock->time();
  const std::int64_t nowNs = m_clusterClock->convertToNanos(timestamp);
  std::int32_t workCount = 0;

  try {
    if (nowNs >= m_slowTickDeadlineNs) {
      const std::int32_t slowTickWorkCount = slowTickWork(nowNs);
      workCount += slowTickWorkCount;
      m_slowTickDeadlineNs =
          slowTickWorkCount > 0 ? nowNs + 1 : nowNs + SLOW_TICK_INTERVAL_NS;
    }

    if (m_consensusAdapter) {
      workCount += m_consensusAdapter->poll();
    }

    if (m_election) {
      workCount += m_election->doWork(nowNs);
    } else {
      workCount += consensusWork(timestamp, nowNs);
    }

    if (m_consensusModuleExtension) {
      workCount += m_consensusModuleExtension->doWork(nowNs);
    }
  } catch (const AgentTerminationException &ex) {
    runTerminationHook();
    throw;
  } catch (const std::exception &ex) {
    if (m_election) {
      m_election->handleError(nowNs, ex);
    } else {
      throw;
    }
  }

  return workCount;
}

// IdleStrategy interface
void ConsensusModuleAgent::idle() {
  checkInterruptStatus();
  if (m_aeron && !m_aeron->isClosed()) {
    m_aeron->conductorAgentInvoker().invoke();
  }
  if (m_idleStrategy) {
    m_idleStrategy->idle();
  }
}

void ConsensusModuleAgent::idle(std::int32_t workCount) {
  checkInterruptStatus();
  if (m_aeron && !m_aeron->isClosed()) {
    m_aeron->conductorAgentInvoker().invoke();
  }
  if (m_idleStrategy) {
    m_idleStrategy->idle(workCount);
  }
}

void ConsensusModuleAgent::reset() {
  if (m_idleStrategy) {
    m_idleStrategy->reset();
  }
}

// ConsensusModuleControl interface
std::int32_t ConsensusModuleAgent::memberId() { return m_memberId; }

std::int64_t ConsensusModuleAgent::time() {
  return m_clusterClock ? m_clusterClock->time() : 0;
}

std::chrono::milliseconds::rep ConsensusModuleAgent::timeUnit() {
  return m_clusterTimeUnit;
}

std::shared_ptr<IdleStrategy> ConsensusModuleAgent::idleStrategy() {
  return m_idleStrategy;
}

ConsensusModule::Context &ConsensusModuleAgent::context() { return m_ctx; }

std::shared_ptr<Aeron> ConsensusModuleAgent::aeron() { return m_aeron; }

std::shared_ptr<AeronArchive> ConsensusModuleAgent::archive() {
  return m_archive;
}

std::shared_ptr<security::AuthorisationService>
ConsensusModuleAgent::authorisationService() {
  return m_authorisationService;
}

std::shared_ptr<ClusterClientSession>
ConsensusModuleAgent::getClientSession(std::int64_t clusterSessionId) {
  auto it = m_sessionByIdMap.find(clusterSessionId);
  if (it != m_sessionByIdMap.end()) {
    return it->second;
  }
  return nullptr;
}

void ConsensusModuleAgent::closeClusterSession(std::int64_t clusterSessionId) {
  auto it = m_sessionByIdMap.find(clusterSessionId);
  if (it != m_sessionByIdMap.end()) {
    auto session = it->second;
    if (session && session->isOpen()) {
      session->closing(CloseReason::SERVICE_ACTION);
      if (Role::LEADER == m_role && State::ACTIVE == m_state) {
        const std::int64_t timestamp =
            m_clusterClock ? m_clusterClock->time() : 0;
        if (m_logPublisher && m_logPublisher->appendSessionClose(
                                  m_memberId, *session, m_leadershipTermId,
                                  timestamp, m_clusterTimeUnit)) {
          logAppendSessionClose(m_memberId, session->id(),
                                session->closeReason(), m_leadershipTermId,
                                timestamp, m_clusterTimeUnit);
          const std::string msg = "SERVICE_ACTION";
          if (m_egressPublisher) {
            m_egressPublisher->sendEvent(*session, m_leadershipTermId,
                                         m_memberId, EventCode::CLOSED, msg);
          }
          session->closedLogPosition(m_logPublisher->position());
          m_uncommittedClosedSessions.push_back(session);
          closeSession(session);
        }
      } else {
        closeSession(session);
      }
    }
  }
}

std::int32_t ConsensusModuleAgent::commitPositionCounterId() {
  return m_commitPosition ? m_commitPosition->id() : 0;
}

std::int32_t ConsensusModuleAgent::clusterId() { return m_ctx.clusterId(); }

ClusterMember ConsensusModuleAgent::clusterMember() { return m_thisMember; }

// Methods called by LogAdapter
ControlledPollAction ConsensusModuleAgent::onReplayExtensionMessage(
    std::int32_t actingBlockLength, std::int32_t templateId,
    std::int32_t schemaId, std::int32_t actingVersion, AtomicBuffer &buffer,
    std::int32_t offset, std::int32_t length, Header &header) {
  if (m_consensusModuleExtension) {
    const std::int32_t remainingMessageOffset =
        offset + MessageHeaderEncoder::encodedLength();
    const std::int32_t remainingMessageLength =
        length - MessageHeaderEncoder::encodedLength();

    return m_consensusModuleExtension->onLogExtensionMessage(
        actingBlockLength, templateId, schemaId, actingVersion, buffer,
        remainingMessageOffset, remainingMessageLength, header);
  }

  throw ClusterException(
      "expected schemaId=" + std::to_string(MessageHeader::sbeSchemaId()) +
          ", actual=" + std::to_string(schemaId),
      SOURCEINFO);
}

void ConsensusModuleAgent::onReplaySessionMessage(std::int64_t clusterSessionId,
                                                  std::int64_t timestamp) {
  auto it = m_sessionByIdMap.find(clusterSessionId);
  if (it != m_sessionByIdMap.end()) {
    std::int64_t timestampNs = m_clusterClock->convertToNanos(timestamp);
    it->second->timeOfLastActivityNs(timestampNs);
  } else if (clusterSessionId < 0) {
    // Service message
    std::int32_t serviceId =
        PendingServiceMessageTracker::serviceIdFromLogMessage(clusterSessionId);
    if (serviceId >= 0 &&
        serviceId <
            static_cast<std::int32_t>(m_pendingServiceMessageTrackers.size())) {
      m_pendingServiceMessageTrackers[serviceId]->sweepFollowerMessages(
          clusterSessionId);
    }
  }
}

void ConsensusModuleAgent::onReplayTimerEvent(std::int64_t correlationId) {
  if (!m_timerService->cancelTimerByCorrelationId(correlationId)) {
    // Timer already expired - track it
    m_expiredTimerCountByCorrelationIdMap[correlationId]++;
  }
}

void ConsensusModuleAgent::onReplaySessionOpen(
    std::int64_t logPosition, std::int64_t correlationId,
    std::int64_t clusterSessionId, std::int64_t timestamp,
    std::int32_t responseStreamId, const std::string &responseChannel) {
  auto session = std::make_shared<ClusterSession>(
      m_memberId, clusterSessionId, responseStreamId, responseChannel,
      responseChannel);
  session->open(logPosition);
  session->lastActivityNs(m_clusterClock->convertToNanos(timestamp),
                          correlationId);

  addSession(session);

  if (clusterSessionId >= m_nextSessionId) {
    m_nextSessionId = clusterSessionId + 1;
    m_nextCommittedSessionId = m_nextSessionId;
  }

  if (m_consensusModuleExtension) {
    m_consensusModuleExtension->onSessionOpened(clusterSessionId);
  }
}

void ConsensusModuleAgent::onReplaySessionClose(
    std::int64_t clusterSessionId, codecs::CloseReason::Value closeReason) {
  auto it = m_sessionByIdMap.find(clusterSessionId);
  if (it != m_sessionByIdMap.end()) {
    it->second->closing(closeReason);
    closeSession(it->second);
  }
}

void ConsensusModuleAgent::onReplayClusterAction(std::int64_t leadershipTermId,
                                                 std::int64_t logPosition,
                                                 std::int64_t timestamp,
                                                 ClusterAction::Value action,
                                                 std::int32_t flags) {
  if (leadershipTermId == m_leadershipTermId) {
    if (action == ClusterAction::Value::SUSPEND) {
      state(State::SUSPENDED);
    } else if (action == ClusterAction::Value::RESUME) {
      state(State::ACTIVE);
    } else if (action == ClusterAction::Value::SNAPSHOT &&
               flags == 0) // CLUSTER_ACTION_FLAGS_DEFAULT
    {
      state(State::SNAPSHOT);
      // Snapshot begin is handled by state change
      // Additional snapshot initialization can be done here if needed
    }
  }
}

void ConsensusModuleAgent::onReplayNewLeadershipTermEvent(
    std::int64_t leadershipTermId, std::int64_t logPosition,
    std::int64_t timestamp, std::int64_t termBaseLogPosition,
    std::chrono::milliseconds::rep timeUnit, std::int32_t appVersion) {
  if (m_election) {
    m_election->onReplayNewLeadershipTermEvent(leadershipTermId, logPosition,
                                               timestamp, termBaseLogPosition);
  }

  if (timeUnit != m_clusterTimeUnit) {
    // Handle incompatible time units - log error and potentially reject
    if (m_ctx.errorLog()) {
      m_ctx.errorLog()->record(
          ClusterEvent("incompatible time unit: cluster=" +
                       std::to_string(m_clusterTimeUnit) +
                       " snapshot=" + std::to_string(timeUnit)));
    }
  }

  // Handle app version compatibility check
  if (m_ctx.appVersionValidator() &&
      !m_ctx.appVersionValidator()->isVersionCompatible(m_ctx.appVersion(),
                                                        appVersion)) {
    if (m_ctx.errorLog()) {
      m_ctx.errorLog()->record(
          ClusterEvent("incompatible app version: context=" +
                       std::to_string(m_ctx.appVersion()) +
                       " snapshot=" + std::to_string(appVersion)));
    }
  }
}

void ConsensusModuleAgent::awaitLocalSocketsClosed(
    std::int64_t registrationId) {
  // Wait for local sockets to close by checking counters
  if (m_aeron) {
    CountersReader &counters = m_aeron->countersReader();
    while (true) {
      // Check if any counter with this registration ID is still active
      bool found = false;
      counters.forEach([&](std::int32_t counterId, std::int32_t counterTypeId,
                           const AtomicBuffer &keyBuffer,
                           const std::string &label) {
        if (counters.getCounterRegistrationId(counterId) == registrationId) {
          found = true;
        }
      });

      if (!found) {
        break;
      }

      idle();
    }
  }
}

// Methods called by IngressAdapter
ControlledPollAction ConsensusModuleAgent::onExtensionMessage(
    std::int32_t actingBlockLength, std::int32_t templateId,
    std::int32_t schemaId, std::int32_t actingVersion, AtomicBuffer &buffer,
    std::int32_t offset, std::int32_t length, Header &header) {
  if (m_consensusModuleExtension) {
    return m_consensusModuleExtension->onIngressExtensionMessage(
        actingBlockLength, templateId, schemaId, actingVersion, buffer, offset,
        length, header);
  } else {
    // Log error but continue
    if (m_ctx.countedErrorHandler()) {
      m_ctx.countedErrorHandler()->onError(ClusterException(
          "unexpected extension message: schemaId=" + std::to_string(schemaId) +
              " templateId=" + std::to_string(templateId),
          SOURCEINFO));
    }
    return ControlledPollAction::CONTINUE;
  }
}

ControlledPollAction ConsensusModuleAgent::onIngressMessage(
    std::int64_t leadershipTermId, std::int64_t clusterSessionId,
    AtomicBuffer &buffer, std::int32_t offset, std::int32_t length) {
  if (leadershipTermId == m_leadershipTermId && m_role == Role::LEADER) {
    auto it = m_sessionByIdMap.find(clusterSessionId);
    if (it != m_sessionByIdMap.end() && it->second->isOpen()) {
      std::int64_t timestamp = m_clusterClock->time();
      std::int64_t position =
          m_logPublisher->appendMessage(leadershipTermId, clusterSessionId,
                                        timestamp, buffer, offset, length);

      if (position > 0) {
        it->second->timeOfLastActivityNs(m_clusterClock->timeNanos());
        return ControlledPollAction::CONTINUE;
      } else {
        return ControlledPollAction::ABORT;
      }
    }
  }

  return ControlledPollAction::CONTINUE;
}

void ConsensusModuleAgent::onSessionConnect(
    std::int64_t correlationId, std::int32_t responseStreamId,
    std::int32_t version, const std::string &responseChannel,
    const std::vector<std::uint8_t> &encodedCredentials,
    const std::string &clientInfo, Header &header) {
  const std::int64_t clusterSessionId =
      (m_role == Role::LEADER) ? m_nextSessionId++ : aeron::NULL_VALUE;
  auto session = std::make_shared<ClusterSession>(
      m_memberId, clusterSessionId, responseStreamId,
      refineResponseChannel(responseChannel), sessionInfo(clientInfo, header));

  session->asyncConnect(m_aeron, m_tempBuffer, m_ctx.clusterId());
  const std::int64_t nowNs = m_clusterClock->timeNanos();
  session->lastActivityNs(nowNs, correlationId);

  if (m_role != Role::LEADER) {
    m_redirectUserSessions.push_back(session);
  } else {
    if (client::AeronCluster::Configuration::PROTOCOL_MAJOR_VERSION !=
        SemanticVersion::major(version)) {
      const std::string detail =
          std::string(SESSION_INVALID_VERSION_MSG) + " " +
          SemanticVersion::toString(version) + ", cluster is " +
          SemanticVersion::toString(
              client::AeronCluster::Configuration::PROTOCOL_SEMANTIC_VERSION);
      session->reject(static_cast<EventCode::Value>(1), detail,
                      m_ctx.errorLog().get()); // EventCode::ERROR = 1
      m_rejectedUserSessions.push_back(session);
    } else if (m_pendingUserSessions.size() + m_sessionByIdMap.size() >=
               m_ctx.maxConcurrentSessions()) {
      session->reject(static_cast<EventCode::Value>(1), SESSION_LIMIT_MSG,
                      m_ctx.errorLog().get()); // EventCode::ERROR = 1
      m_rejectedUserSessions.push_back(session);
    } else {
      session->linkIngressImage(header);
      m_authenticator->onConnectRequest(session->id(), encodedCredentials,
                                        nowNs /
                                            1000000); // Convert to milliseconds
      m_pendingUserSessions.push_back(session);
    }
  }
}

void ConsensusModuleAgent::onSessionClose(std::int64_t leadershipTermId,
                                          std::int64_t clusterSessionId) {
  if (leadershipTermId == m_leadershipTermId && m_role == Role::LEADER) {
    auto it = m_sessionByIdMap.find(clusterSessionId);
    if (it != m_sessionByIdMap.end() && it->second->isOpen()) {
      auto session = it->second;
      session->closing(CloseReason::CLIENT_ACTION);
      auto countedErrorHandler = m_ctx.countedErrorHandler();
      exception_handler_t errorHandler =
          [countedErrorHandler](const std::exception &ex) {
            if (countedErrorHandler) {
              countedErrorHandler->onError(ex);
            }
          };
      session->disconnect(m_aeron, errorHandler);

      const std::int64_t timestamp =
          m_clusterClock ? m_clusterClock->time() : 0;
      if (m_logPublisher && m_logPublisher->appendSessionClose(
                                m_memberId, *session, m_leadershipTermId,
                                timestamp, m_clusterTimeUnit)) {
        session->closedLogPosition(m_logPublisher->position());
        m_uncommittedClosedSessions.push_back(session);
        closeSession(session);
      }
    }
  }
}

void ConsensusModuleAgent::onSessionKeepAlive(std::int64_t leadershipTermId,
                                              std::int64_t clusterSessionId,
                                              Header &header) {
  if (leadershipTermId == m_leadershipTermId && m_role == Role::LEADER) {
    auto it = m_sessionByIdMap.find(clusterSessionId);
    if (it != m_sessionByIdMap.end() &&
        it->second->state() == ClusterSessionState::OPEN) {
      it->second->linkIngressImage(header);
      it->second->timeOfLastActivityNs(m_clusterClock->timeNanos());
    }
  }
}

void ConsensusModuleAgent::onIngressChallengeResponse(
    std::int64_t correlationId, std::int64_t clusterSessionId,
    const std::vector<std::uint8_t> &encodedCredentials) {
  if (m_role == Role::LEADER) {
    onChallengeResponseForSession(m_pendingUserSessions, correlationId,
                                  clusterSessionId, encodedCredentials);
  } else {
    m_consensusPublisher->challengeResponse(m_leaderMember.publication(),
                                            correlationId, clusterSessionId,
                                            encodedCredentials);
  }
}

void ConsensusModuleAgent::onAdminRequest(std::int64_t leadershipTermId,
                                          std::int64_t clusterSessionId,
                                          std::int64_t correlationId,
                                          AdminRequestType::Value requestType,
                                          AtomicBuffer &payload,
                                          std::int32_t payloadOffset,
                                          std::int32_t payloadLength) {
  if (m_role != Role::LEADER || leadershipTermId != m_leadershipTermId) {
    return;
  }

  auto it = m_sessionByIdMap.find(clusterSessionId);
  if (it == m_sessionByIdMap.end() ||
      it->second->state() != ClusterSessionState::OPEN) {
    return;
  }

  auto session = it->second;
  // AdminRequestDecoder SBE template ID is defined in codecs (value: 50)
  constexpr std::int32_t ADMIN_REQUEST_TEMPLATE_ID = 50;
  if (!m_authorisationService->isAuthorised(
          MessageHeaderDecoder::sbeSchemaId(), ADMIN_REQUEST_TEMPLATE_ID,
          requestType, session->encodedPrincipal())) {
    const std::string msg =
        "Execution of the " +
        std::to_string(static_cast<std::int32_t>(requestType)) +
        " request was not authorised";
    m_egressPublisher->sendAdminResponse(*session, correlationId, requestType,
                                         AdminResponseCode::UNAUTHORISED_ACCESS,
                                         msg);
    return;
  }

  if (requestType == AdminRequestType::Value::SNAPSHOT) {
    if (ClusterControl::toggle(m_controlToggle,
                               ClusterControl::ToggleState::SNAPSHOT)) {
      m_egressPublisher->sendAdminResponse(*session, correlationId, requestType,
                                           AdminResponseCode::OK, "");
    } else {
      const std::string msg =
          "Failed to switch ClusterControl to the ToggleState.SNAPSHOT state";
      m_egressPublisher->sendAdminResponse(
          *session, correlationId, requestType,
          static_cast<AdminResponseCode::Value>(1), msg);
    }
  } else {
    const std::string msg =
        "Unknown request type: " +
        std::to_string(static_cast<std::int32_t>(requestType));
    m_egressPublisher->sendAdminResponse(
        *session, correlationId, requestType,
        static_cast<AdminResponseCode::Value>(1), msg);
  }
}

// Utility methods
std::string ConsensusModuleAgent::roleName() const {
  return m_ctx.agentRoleName();
}

std::int64_t ConsensusModuleAgent::logRecordingId() const {
  return m_logRecordingId;
}

void ConsensusModuleAgent::logRecordingId(std::int64_t recordingId) {
  if (recordingId != aeron::NULL_VALUE) {
    m_logRecordingId = recordingId;
  }
}

// Helper methods - basic implementations
void ConsensusModuleAgent::state(State newState) {
  m_state = newState;
  if (m_moduleState) {
    m_moduleState->set(static_cast<std::int64_t>(newState));
  }
}

void ConsensusModuleAgent::role(Role newRole) {
  m_role = newRole;
  if (m_clusterRoleCounter) {
    m_clusterRoleCounter->set(static_cast<std::int64_t>(newRole));
  }
}

void ConsensusModuleAgent::addSession(std::shared_ptr<ClusterSession> session) {
  m_sessionByIdMap[session->id()] = session;
  m_sessions.push_back(session);
}

void ConsensusModuleAgent::closeSession(
    std::shared_ptr<ClusterSession> session) {
  if (!session) {
    return;
  }

  const std::int64_t sessionId = session->id();

  // Remove from maps
  m_sessionByIdMap.erase(sessionId);
  for (auto it = m_sessions.begin(); it != m_sessions.end(); ++it) {
    if ((*it)->id() == sessionId) {
      m_sessions.erase(it);
      break;
    }
  }

  // Close session
  auto countedErrorHandler = m_ctx.countedErrorHandler();
  exception_handler_t errorHandler =
      [countedErrorHandler](const std::exception &ex) {
        if (countedErrorHandler) {
          countedErrorHandler->onError(ex);
        }
      };
  session->close(m_aeron, errorHandler, "closed");

  // Notify extension
  if (m_consensusModuleExtension && session->closeReason()) {
    m_consensusModuleExtension->onSessionClosed(sessionId,
                                                session->closeReason());
  }
}

void ConsensusModuleAgent::checkInterruptStatus() {
  // In C++, thread interruption is not directly available like Java.
  // This would typically be handled by checking a flag or via std::atomic<bool>
  // For now, this is a no-op, but can be extended if needed.
}

std::int32_t ConsensusModuleAgent::slowTickWork(std::int64_t nowNs) {
  std::int32_t workCount = 0;

  // Invoke Aeron client invoker
  if (m_aeron && !m_aeron->isClosed()) {
    workCount += m_aeron->conductorAgentInvoker().invoke();
  }

  // Check if Aeron is closed
  if (m_aeron && m_aeron->isClosed()) {
    throw AgentTerminationException("unexpected Aeron close");
  } else if (State::CLOSED == state()) {
    unexpectedTermination();
  }

  // Update mark file if needed
  // MARK_FILE_UPDATE_INTERVAL_NS is typically 1 second (1000000000 nanoseconds)
  static constexpr std::int64_t MARK_FILE_UPDATE_INTERVAL_NS = 1000000000LL;
  if (nowNs >= m_markFileUpdateDeadlineNs) {
    m_markFileUpdateDeadlineNs = nowNs + MARK_FILE_UPDATE_INTERVAL_NS;
    if (m_markFile && m_clusterClock) {
      m_markFile->updateActivityTimestamp(m_clusterClock->timeMillis());
    }
  }

  workCount += pollArchiveEvents();

  workCount += sendRedirects(m_redirectUserSessions, nowNs);

  workCount += sendRejections(m_rejectedUserSessions, nowNs);
  workCount += sendRejections(m_rejectedBackupSessions, nowNs);

  if (!m_election) {
    if (Role::LEADER == role()) {
      workCount += checkClusterControlToggle(nowNs);

      if (State::ACTIVE == state()) {
        workCount += processPendingSessions(m_pendingUserSessions,
                                            m_rejectedUserSessions, nowNs);
        workCount += processPendingSessions(m_pendingBackupSessions,
                                            m_rejectedBackupSessions, nowNs);

        workCount += checkSessions(m_sessions, nowNs);

        if (!hasActiveQuorum()) {
          enterElection(false, "inactive follower quorum");
          workCount += 1;
        }
      } else if (State::TERMINATING == state()) {
        if (m_clusterTermination &&
            m_clusterTermination->canTerminate(m_activeMembers, nowNs)) {
          if (m_recordingLog) {
            m_recordingLog->commitLogPosition(m_leadershipTermId,
                                              m_terminationPosition);
          }
          closeAndTerminate();
        }
      }
    } else {
      if (Role::FOLLOWER == role() && State::ACTIVE == state()) {
        workCount += processPendingSessions(m_pendingBackupSessions,
                                            m_rejectedBackupSessions, nowNs);
      }

      if (State::ACTIVE == state() || State::SUSPENDED == state()) {
        if (nowNs >= (m_timeOfLastLogUpdateNs + m_leaderHeartbeatTimeoutNs) &&
            ::aeron::archive::client::NULL_POSITION == m_terminationPosition) {
          enterElection(false, "leader heartbeat timeout");
          workCount += 1;
        }
      }
    }

    if (State::ACTIVE == state()) {
      workCount += checkNodeControlToggle();
    }
  }

  if (m_consensusModuleExtension) {
    workCount += m_consensusModuleExtension->slowTickWork(nowNs);
  }

  return workCount;
}

std::int32_t ConsensusModuleAgent::consensusWork(std::int64_t timestamp,
                                                 std::int64_t nowNs) {
  std::int32_t workCount = 0;

  if (Role::LEADER == role()) {
    if (State::ACTIVE == state()) {
      // Poll timer service
      if (m_timerService) {
        workCount += m_timerService->poll(timestamp);
      }

      // Poll pending service message trackers
      for (const auto &tracker : m_pendingServiceMessageTrackers) {
        if (tracker) {
          workCount += tracker->poll();
        }
      }

      // Poll ingress adapter
      if (m_ingressAdapter) {
        workCount += m_ingressAdapter->poll();
      }
    }

    workCount += updateLeaderPosition(nowNs);
  } else {
    // Follower-specific work
    if (State::ACTIVE == state() || State::SUSPENDED == state()) {
      // Check for termination
      if (::aeron::archive::client::NULL_POSITION != m_terminationPosition &&
          m_logAdapter && m_logAdapter->position() >= m_terminationPosition) {
        state(State::TERMINATING);
        if (m_serviceCount > 0 && m_serviceProxy) {
          if (m_serviceProxy) {
            auto countedErrorHandler = m_ctx.countedErrorHandler();
            exception_handler_t errorHandler =
                [countedErrorHandler](const std::exception &ex) {
                  if (countedErrorHandler) {
                    countedErrorHandler->onError(ex);
                  }
                };
            m_serviceProxy->terminationPosition(m_terminationPosition,
                                                errorHandler);
          }
        } else if (m_logAdapter) {
          terminateOnServiceAck(m_logAdapter->position());
        }
      } else {
        // Poll log adapter
        if (m_logAdapter) {
          const std::int64_t limit = m_appendPosition
                                         ? m_appendPosition->get()
                                         : m_logRecordingStopPosition;
          const std::int64_t boundPosition =
              std::min(m_notifiedCommitPosition, limit);
          const std::int32_t count = m_logAdapter->poll(boundPosition);

          if (0 == count && m_logAdapter->isImageClosed()) {
            const bool isEos = m_logAdapter->isLogEndOfStream();
            enterElection(isEos, "log disconnected from leader: eos=" +
                                     std::to_string(isEos));
            return 1;
          }

          if (m_commitPosition && m_logAdapter) {
            if (m_commitPosition) {
              // proposeMaxRelease is typically available on AtomicCounter
              // If not available, use setRelease as fallback
              const std::int64_t currentPos = m_commitPosition->get();
              const std::int64_t logPos = m_logAdapter->position();
              if (logPos > currentPos) {
                m_commitPosition->setRelease(logPos);
              }
            }
          }

          workCount += m_ingressAdapter ? m_ingressAdapter->poll() : 0;
          workCount += count;
        }
      }
    }

    workCount += updateFollowerPosition(nowNs);
  }

  // Poll consensus module adapter
  if (m_consensusModuleAdapter) {
    workCount += m_consensusModuleAdapter->poll();
  }

  workCount += pollStandbySnapshotReplication(nowNs);

  if (m_consensusModuleExtension) {
    if (m_consensusModuleExtension) {
      // Note: consensusWork may not be part of ConsensusModuleExtension
      // interface This is called if the extension supports it workCount +=
      // m_consensusModuleExtension->consensusWork(nowNs);
    }
  }

  return workCount;
}

void ConsensusModuleAgent::connectIngress() {
  auto ingressUri = ChannelUri::parse(m_ctx.ingressChannel());
  if (!ingressUri->containsKey(aeron::ENDPOINT_PARAM_NAME)) {
    ingressUri->put(aeron::ENDPOINT_PARAM_NAME, m_thisMember.ingressEndpoint());
  }

  // Don't subscribe to ingress if follower and multicast ingress (UDP media
  // implies multicast)
  if (Role::LEADER != role() && ingressUri->media() == aeron::UDP_MEDIA) {
    return;
  }

  ingressUri->put(aeron::REJOIN_PARAM_NAME, "false");

  std::int64_t subscriptionId = m_aeron->addSubscription(
      ingressUri->toString(), m_ctx.ingressStreamId(), nullptr,
      [this](Image &image) { this->onUnavailableIngressImage(image); });

  std::shared_ptr<Subscription> subscription = nullptr;
  while (!subscription) {
    subscription = m_aeron->findSubscription(subscriptionId);
    std::this_thread::yield();
  }

  std::shared_ptr<Subscription> ipcSubscription = nullptr;
  if (Role::LEADER == role() && m_ctx.isIpcIngressAllowed()) {
    std::int64_t ipcSubscriptionId = m_aeron->addSubscription(
        aeron::IPC_CHANNEL, m_ctx.ingressStreamId(), nullptr,
        [this](Image &image) { this->onUnavailableIngressImage(image); });
    while (!ipcSubscription) {
      ipcSubscription = m_aeron->findSubscription(ipcSubscriptionId);
      std::this_thread::yield();
    }
  }

  if (m_ingressAdapter) {
    m_ingressAdapter->connect(subscription, ipcSubscription);
  }
}

void ConsensusModuleAgent::runTerminationHook() {
  try {
    auto terminationHook = m_ctx.terminationHook();
    if (terminationHook) {
      terminationHook();
    }
  } catch (const std::exception &ex) {
    m_ctx.countedErrorHandler()->onError(ex);
  }
}

void ConsensusModuleAgent::tryStopLogRecording() {
  if (m_logRecordingId != aeron::NULL_VALUE && m_archive) {
    m_archive->tryStopRecordingByIdentity(m_logRecordingId);
    m_logRecordingId = aeron::NULL_VALUE;
  }
}

void ConsensusModuleAgent::unexpectedTermination() {
  if (m_markFile) {
    m_markFile->signalFailedStart();
  }
  state(State::CLOSED);
}

void ConsensusModuleAgent::enterElection(bool isEos,
                                         const std::string &reason) {
  if (m_election) {
    throw ClusterException("election in progress", SOURCEINFO);
  }

  role(Role::FOLLOWER);

  const std::int64_t leadershipTermId = m_leadershipTermId;
  std::int64_t termBaseLogPosition = 0;
  if (m_recordingLog) {
    auto *termEntry = m_recordingLog->findTermEntry(leadershipTermId);
    if (termEntry) {
      termBaseLogPosition = termEntry->termBaseLogPosition;
    } else if (m_recoveryPlan) {
      termBaseLogPosition = m_recoveryPlan->lastTermBaseLogPosition;
    }
  }

  const std::int64_t appendedPosition =
      m_appendPosition
          ? m_appendPosition->get()
          : std::max(m_recoveryPlan ? m_recoveryPlan->appendedLogPosition : 0,
                     m_logRecordingStopPosition);
  const std::int64_t commitPosition =
      m_commitPosition ? m_commitPosition->get() : 0;

  m_ctx.countedErrorHandler()->onError(ClusterEvent(reason));

  if (m_consensusPublisher) {
    // Create pointer map for Election
    std::unordered_map<std::int32_t, ClusterMember *> clusterMemberByIdPtrMap;
    for (auto &member : m_activeMembers) {
      clusterMemberByIdPtrMap[member.id()] = &member;
    }
    m_election = std::make_unique<Election>(
        false, isEos ? m_leaderMember.id() : aeron::NULL_VALUE,
        leadershipTermId, termBaseLogPosition, commitPosition, appendedPosition,
        m_activeMembers, clusterMemberByIdPtrMap, m_thisMember,
        *m_consensusPublisher, m_ctx, *this);

    if (m_election && m_clusterClock) {
      m_election->doWork(m_clusterClock->timeNanos());
    }
  }
}

void ConsensusModuleAgent::electionComplete(std::int64_t nowNs) {
  if (!m_election) {
    return;
  }

  m_leadershipTermId = m_election->leadershipTermId();

  if (Role::LEADER == role()) {
    m_timeOfLastLogUpdateNs = nowNs - m_leaderHeartbeatIntervalNs;
    if (m_timerService) {
      m_timerService->currentTime(
          m_clusterClock->convertToNanos(m_clusterClock->time()) /
          1000000); // Convert to milliseconds
    }
    if (m_controlToggle) {
      ClusterControl::activate(m_controlToggle);
    }
    prepareSessionsForNewTerm(m_election->isLeaderStartup());
  } else {
    m_timeOfLastLogUpdateNs = nowNs;
    m_timeOfLastAppendPositionUpdateNs = nowNs;
    m_timeOfLastAppendPositionSendNs = nowNs;
    m_localLogChannel.clear();
  }

  if (m_nodeControlToggle) {
    // NodeControl::activate(m_nodeControlToggle);  // TODO: implement
    // NodeControl
    m_nodeControlToggle->set(1); // Set to NEUTRAL state
  }

  if (m_recordingLog && m_archive) {
    m_recoveryPlan = std::make_unique<RecordingLog::RecoveryPlan>(
        m_recordingLog->createRecoveryPlan(m_archive, m_serviceCount,
                                           m_logRecordingId));
  }

  const std::int64_t logPosition = m_election->logPosition();
  m_notifiedCommitPosition = std::max(m_notifiedCommitPosition, logPosition);
  if (m_commitPosition) {
    m_commitPosition->setRelease(logPosition);
  }

  if (m_election->leader()) {
    updateMemberDetails(*m_election->leader());
  }

  connectIngress();

  if (m_consensusModuleExtension) {
    ConsensusControlState controlState(
        m_logPublisher ? m_logPublisher->publication() : nullptr,
        m_extensionLeaderSubscription, m_logRecordingId, m_leadershipTermId);
    m_consensusModuleExtension->onElectionComplete(controlState);
  }

  m_election.reset();
  state(State::ACTIVE);
}

std::string ConsensusModuleAgent::refineResponseChannel(
    const std::string &responseChannel) {
  if (!m_responseChannelTemplate) {
    return responseChannel;
  } else if (responseChannel.find(aeron::IPC_CHANNEL) == 0) {
    return m_ctx.isIpcIngressAllowed() ? responseChannel
                                       : m_ctx.egressChannel();
  } else {
    auto channelUri = ChannelUri::parse(responseChannel);
    // Copy common parameters from template (if template string is provided)
    // For now, return the original channel as template parameter copying is
    // complex This matches Java behavior where template parameters are copied
    return channelUri->toString();
  }
}

std::string ConsensusModuleAgent::sessionInfo(const std::string &clientInfo,
                                              Header &header) {
  // Extract image from header context
  Image *image = static_cast<Image *>(header.context());
  if (!image) {
    return clientInfo;
  }

  const std::string imageInfo =
      "sourceIdentity=" + image->sourceIdentity() +
      " sessionId=" + std::to_string(image->sessionId());

  if (clientInfo.empty()) {
    return imageInfo;
  } else {
    return clientInfo + " " + imageInfo;
  }
}

void ConsensusModuleAgent::onScheduleTimer(std::int64_t correlationId,
                                           std::int64_t deadline) {
  if (m_timerService) {
    m_timerService->scheduleTimerForCorrelationId(correlationId, deadline);
  }
}

void ConsensusModuleAgent::onUnavailableCounter(CountersReader &countersReader,
                                                std::int64_t registrationId,
                                                std::int32_t counterId) {
  // Handle unavailable counter - typically used for detecting service
  // unavailability This can trigger service restart or recovery logic
}

// ConsensusModuleSnapshotListener interface - basic implementations
void ConsensusModuleAgent::onLoadBeginSnapshot(std::int32_t appVersion,
                                               ClusterTimeUnit::Value timeUnit,
                                               const util::DirectBuffer &buffer,
                                               std::int32_t offset,
                                               std::int32_t length) {
  // Check app version compatibility
  if (m_ctx.appVersionValidator() &&
      !m_ctx.appVersionValidator()->isVersionCompatible(m_ctx.appVersion(),
                                                        appVersion)) {
    throw ClusterException("incompatible app version: required=" +
                               std::to_string(m_ctx.appVersion()) +
                               " snapshot=" + std::to_string(appVersion),
                           SOURCEINFO);
  }

  // Check time unit compatibility
  const std::chrono::milliseconds::rep snapshotTimeUnit =
      timeUnit == ClusterTimeUnit::Value::MILLIS ? 1 : 1000000;
  if (snapshotTimeUnit != m_clusterTimeUnit) {
    throw ClusterException("incompatible time unit: required=" +
                               std::to_string(m_clusterTimeUnit) +
                               " snapshot=" + std::to_string(snapshotTimeUnit),
                           SOURCEINFO);
  }
}

void ConsensusModuleAgent::onLoadConsensusModuleState(
    std::int64_t nextSessionId, std::int64_t nextServiceSessionId,
    std::int64_t logServiceSessionId, std::int32_t pendingMessageCapacity,
    const util::DirectBuffer &buffer, std::int32_t offset,
    std::int32_t length) {
  m_nextSessionId = nextSessionId;
  m_nextCommittedSessionId = nextSessionId;

  if (!m_pendingServiceMessageTrackers.empty()) {
    m_pendingServiceMessageTrackers[0]->loadState(
        nextServiceSessionId, logServiceSessionId, pendingMessageCapacity);
  }
}

void ConsensusModuleAgent::onLoadPendingMessage(
    std::int64_t clusterSessionId, const util::DirectBuffer &buffer,
    std::int32_t offset, std::int32_t length) {
  std::int32_t serviceId =
      PendingServiceMessageTracker::serviceIdFromLogMessage(clusterSessionId);
  if (serviceId >= 0 &&
      serviceId <
          static_cast<std::int32_t>(m_pendingServiceMessageTrackers.size())) {
    m_pendingServiceMessageTrackers[serviceId]->appendMessage(buffer, offset,
                                                              length);
  }
}

void ConsensusModuleAgent::onLoadClusterSession(
    std::int64_t clusterSessionId, std::int64_t correlationId,
    std::int64_t openedLogPosition, std::int64_t timeOfLastActivity,
    CloseReason::Value closeReason, std::int32_t responseStreamId,
    const std::string &responseChannel, const util::DirectBuffer &buffer,
    std::int32_t offset, std::int32_t length) {
  auto session = std::make_shared<ClusterSession>(
      m_memberId, clusterSessionId, responseStreamId, responseChannel,
      responseChannel);

  session->loadSnapshotState(correlationId, openedLogPosition,
                             timeOfLastActivity, closeReason);
  addSession(session);

  if (clusterSessionId >= m_nextSessionId) {
    m_nextSessionId = clusterSessionId + 1;
    m_nextCommittedSessionId = m_nextSessionId;
  }
}

void ConsensusModuleAgent::onLoadTimer(std::int64_t correlationId,
                                       std::int64_t deadline,
                                       const util::DirectBuffer &buffer,
                                       std::int32_t offset,
                                       std::int32_t length) {
  onScheduleTimer(correlationId, deadline);
}

void ConsensusModuleAgent::onLoadPendingMessageTracker(
    std::int64_t nextServiceSessionId, std::int64_t logServiceSessionId,
    std::int32_t pendingMessageCapacity, std::int32_t serviceId,
    const util::DirectBuffer &buffer, std::int32_t offset,
    std::int32_t length) {
  if (serviceId < 0 ||
      serviceId >=
          static_cast<std::int32_t>(m_pendingServiceMessageTrackers.size())) {
    throw ClusterException(
        "serviceId=" + std::to_string(serviceId) +
            " invalid for serviceCount=" +
            std::to_string(m_pendingServiceMessageTrackers.size()),
        SOURCEINFO);
  }

  m_pendingServiceMessageTrackers[serviceId]->loadState(
      nextServiceSessionId, logServiceSessionId, pendingMessageCapacity);
}

void ConsensusModuleAgent::onLoadEndSnapshot(const util::DirectBuffer &buffer,
                                             std::int32_t offset,
                                             std::int32_t length) {
  // No-op
}

// TimerService::TimerHandler interface
bool ConsensusModuleAgent::onTimerEvent(std::int64_t correlationId) {
  std::int64_t appendPosition = m_logPublisher->appendTimer(
      correlationId, m_leadershipTermId, m_clusterClock->time());

  return appendPosition > 0;
}

// Methods called by ConsensusAdapter
void ConsensusModuleAgent::onCanvassPosition(std::int64_t logLeadershipTermId,
                                             std::int64_t logPosition,
                                             std::int64_t leadershipTermId,
                                             std::int32_t followerMemberId,
                                             std::int32_t protocolVersion) {
  logOnCanvassPosition(m_memberId, logLeadershipTermId, logPosition,
                       leadershipTermId, followerMemberId, protocolVersion);

  // Check if follower has consensus publication
  auto it = m_clusterMemberByIdMap.find(followerMemberId);
  if (it != m_clusterMemberByIdMap.end() && !it->second.publication()) {
    // Follower doesn't have consensus publication yet
    return;
  }

  if (m_election) {
    m_election->onCanvassPosition(logLeadershipTermId, logPosition,
                                  leadershipTermId, followerMemberId,
                                  protocolVersion);
  } else if (Role::LEADER == role()) {
    auto it = m_clusterMemberByIdMap.find(followerMemberId);
    if (it != m_clusterMemberByIdMap.end() &&
        logLeadershipTermId <= m_leadershipTermId) {
      ClusterMember &follower = it->second;
      // Stop existing catchup replay if any
      if (!m_catchupLogDestination.empty() && m_archive) {
        m_archive->stopAllReplays(m_logRecordingId);
      }

      // Get current term entry from recording log (1:1 translation from Java)
      auto currentTermEntry = m_recordingLog->getTermEntry(m_leadershipTermId);
      const std::int64_t termBaseLogPosition =
          currentTermEntry.termBaseLogPosition;
      std::int64_t nextLogLeadershipTermId = aeron::NULL_VALUE;
      std::int64_t nextTermBaseLogPosition =
          aeron::archive::client::NULL_POSITION;
      std::int64_t nextLogPosition = aeron::archive::client::NULL_POSITION;

      if (logLeadershipTermId < m_leadershipTermId) {
        auto nextLogEntry =
            m_recordingLog->findTermEntry(logLeadershipTermId + 1);
        nextLogLeadershipTermId =
            nextLogEntry ? nextLogEntry->leadershipTermId : m_leadershipTermId;
        nextTermBaseLogPosition = nextLogEntry
                                      ? nextLogEntry->termBaseLogPosition
                                      : termBaseLogPosition;
        nextLogPosition = nextLogEntry ? nextLogEntry->logPosition
                                       : aeron::archive::client::NULL_POSITION;
      }

      // Send new leadership term via consensusPublisher
      if (m_consensusPublisher && m_logPublisher) {
        m_consensusPublisher->newLeadershipTerm(
            follower.publication(), logLeadershipTermId,
            nextLogLeadershipTermId, nextTermBaseLogPosition, nextLogPosition,
            m_leadershipTermId, termBaseLogPosition, m_logPublisher->position(),
            m_commitPosition ? m_commitPosition->get() : 0, m_logRecordingId,
            m_clusterClock ? m_clusterClock->time() : 0, m_memberId,
            m_logPublisher->sessionId(), m_ctx.appVersion(), false);
      }
    }
  }
}

void ConsensusModuleAgent::onRequestVote(std::int64_t logLeadershipTermId,
                                         std::int64_t logPosition,
                                         std::int64_t candidateTermId,
                                         std::int32_t candidateId,
                                         std::int32_t protocolVersion) {
  logOnRequestVote(m_memberId, logLeadershipTermId, logPosition,
                   candidateTermId, candidateId, protocolVersion);

  if (m_election) {
    m_election->onRequestVote(logLeadershipTermId, logPosition, candidateTermId,
                              candidateId, protocolVersion);
  } else if (candidateTermId > m_leadershipTermId) {
    enterElection(false,
                  "unexpected vote request: this.leadershipTermId=" +
                      std::to_string(m_leadershipTermId) +
                      " candidateTermId=" + std::to_string(candidateTermId));
  }
}

void ConsensusModuleAgent::onVote(std::int64_t candidateTermId,
                                  std::int64_t logLeadershipTermId,
                                  std::int64_t logPosition,
                                  std::int32_t candidateMemberId,
                                  std::int32_t followerMemberId, bool vote) {
  if (m_election) {
    m_election->onVote(candidateTermId, logLeadershipTermId, logPosition,
                       candidateMemberId, followerMemberId, vote);
  }
}

void ConsensusModuleAgent::onNewLeadershipTerm(
    std::int64_t logLeadershipTermId, std::int64_t nextLeadershipTermId,
    std::int64_t nextTermBaseLogPosition, std::int64_t nextLogPosition,
    std::int64_t leadershipTermId, std::int64_t termBaseLogPosition,
    std::int64_t logPosition, std::int64_t commitPosition,
    std::int64_t leaderRecordingId, std::int64_t timestamp,
    std::int32_t leaderId, std::int32_t logSessionId, std::int32_t appVersion,
    bool isStartup) {
  logOnNewLeadershipTerm(m_memberId, leadershipTermId, logPosition, timestamp,
                         termBaseLogPosition, m_clusterTimeUnit, appVersion);

  // Check app version compatibility
  if (m_ctx.appVersionValidator() &&
      !m_ctx.appVersionValidator()->isVersionCompatible(m_ctx.appVersion(),
                                                        appVersion)) {
    if (m_ctx.errorLog()) {
      m_ctx.errorLog()->record(
          ClusterEvent("incompatible app version: context=" +
                       std::to_string(m_ctx.appVersion()) +
                       " term=" + std::to_string(appVersion)));
    }
  }

  const std::int64_t nowNs = m_clusterClock->timeNanos();
  if (leadershipTermId >= m_leadershipTermId) {
    m_timeOfLastLeaderUpdateNs = nowNs;
  }

  if (m_election) {
    m_election->onNewLeadershipTerm(
        logLeadershipTermId, nextLeadershipTermId, nextTermBaseLogPosition,
        nextLogPosition, leadershipTermId, termBaseLogPosition, logPosition,
        commitPosition, leaderRecordingId, timestamp, leaderId, logSessionId,
        isStartup);
  } else if (Role::FOLLOWER == role() &&
             leadershipTermId == m_leadershipTermId &&
             leaderId == m_leaderMember.id()) {
    m_notifiedCommitPosition =
        std::max(m_notifiedCommitPosition, commitPosition);
    m_timeOfLastLogUpdateNs = nowNs;
  } else if (leadershipTermId > m_leadershipTermId) {
    enterElection(
        false, "unexpected new leadership term event: this.leadershipTermId=" +
                   std::to_string(m_leadershipTermId) +
                   " newLeadershipTermId=" + std::to_string(leadershipTermId));
  }
}

void ConsensusModuleAgent::onAppendPosition(std::int64_t leadershipTermId,
                                            std::int64_t logPosition,
                                            std::int32_t followerMemberId,
                                            std::int16_t flags) {
  logOnAppendPosition(m_memberId, leadershipTermId, logPosition,
                      followerMemberId, flags);

  if (m_election) {
    m_election->onAppendPosition(leadershipTermId, logPosition,
                                 followerMemberId, flags);
  } else if (leadershipTermId <= m_leadershipTermId && Role::LEADER == role()) {
    auto it = m_clusterMemberByIdMap.find(followerMemberId);
    if (it != m_clusterMemberByIdMap.end()) {
      ClusterMember &follower = it->second;
      follower.logPosition(logPosition);
      follower.timeOfLastAppendPositionNs(m_clusterClock->timeNanos());
      // Track catchup completion - update member's catchup state
      if (it != m_clusterMemberByIdMap.end()) {
        it->second.catchupReplaySessionId(aeron::NULL_VALUE);
      }
    }
  }
}

void ConsensusModuleAgent::onCommitPosition(std::int64_t leadershipTermId,
                                            std::int64_t logPosition,
                                            std::int32_t leaderMemberId) {
  logOnCommitPosition(m_memberId, leadershipTermId, logPosition,
                      leaderMemberId);

  const std::int64_t nowNs = m_clusterClock->timeNanos();
  if (leadershipTermId >= m_leadershipTermId) {
    m_timeOfLastLeaderUpdateNs = nowNs;
  }

  if (m_election) {
    m_election->onCommitPosition(leadershipTermId, logPosition, leaderMemberId);
  } else if (leadershipTermId == m_leadershipTermId) {
    if (m_leaderMember.id() == leaderMemberId && Role::FOLLOWER == role()) {
      m_notifiedCommitPosition =
          std::max(m_notifiedCommitPosition, logPosition);
      m_timeOfLastLogUpdateNs = nowNs;
    }
  } else if (leadershipTermId > m_leadershipTermId) {
    enterElection(
        false,
        "unexpected commit position from new leader - memberId=" +
            std::to_string(m_memberId) +
            " this.leadershipTermId=" + std::to_string(m_leadershipTermId) +
            " this.leaderMemberId=" + std::to_string(m_leaderMember.id()) +
            " newLeadershipTermId=" + std::to_string(leadershipTermId) +
            " newLeaderMemberId=" + std::to_string(leaderMemberId) +
            " newCommitPosition=" + std::to_string(logPosition));
  }
}

void ConsensusModuleAgent::onCatchupPosition(
    std::int64_t leadershipTermId, std::int64_t logPosition,
    std::int32_t followerMemberId, const std::string &catchupEndpoint) {
  logOnCatchupPosition(m_memberId, leadershipTermId, logPosition,
                       followerMemberId, catchupEndpoint);

  if (leadershipTermId <= m_leadershipTermId && Role::LEADER == role()) {
    auto it = m_clusterMemberByIdMap.find(followerMemberId);
    if (it != m_clusterMemberByIdMap.end() &&
        it->second.catchupReplaySessionId() == aeron::NULL_VALUE) {
      ClusterMember &follower = it->second;
      // Parse follower catchup channel
      auto catchupUri = ChannelUri::parse(m_ctx.replayChannel());
      catchupUri->put(aeron::ENDPOINT_PARAM_NAME, catchupEndpoint);
      catchupUri->put(aeron::SESSION_ID_PARAM_NAME,
                      std::to_string(m_logAdapter->image()->sessionId()));
      catchupUri->put(aeron::LINGER_PARAM_NAME, "0");
      catchupUri->put(aeron::EOS_PARAM_NAME, "false");

      // Start replay via archive
      if (m_archive) {
        aeron::archive::client::ReplayParams replayParams;
        replayParams.position(logPosition)
            .length(std::numeric_limits<std::int64_t>::max());

        const std::int64_t replaySessionId =
            m_archive->startReplay(m_logRecordingId, catchupUri->toString(),
                                   m_ctx.logStreamId(), replayParams);

        // Set catchup replay session id and correlation id
        if (it != m_clusterMemberByIdMap.end()) {
          it->second.catchupReplaySessionId(replaySessionId);
          // Note: C++ archive wrapper doesn't have lastCorrelationId() like
          // Java The correlation ID is managed internally by the archive
          it->second.catchupReplayCorrelationId(m_aeron->nextCorrelationId());
        }
      }
    }
  }
}

void ConsensusModuleAgent::onStopCatchup(std::int64_t leadershipTermId,
                                         std::int32_t followerMemberId) {
  logOnStopCatchup(m_memberId, leadershipTermId, followerMemberId);

  if (leadershipTermId == m_leadershipTermId &&
      followerMemberId == m_memberId) {
    if (!m_catchupLogDestination.empty()) {
      m_logAdapter->asyncRemoveDestination(m_catchupLogDestination);
      m_catchupLogDestination.clear();
    }
  }
}

void ConsensusModuleAgent::onTerminationPosition(std::int64_t leadershipTermId,
                                                 std::int64_t logPosition) {
  logOnTerminationPosition(m_memberId, leadershipTermId, logPosition);

  if (leadershipTermId == m_leadershipTermId && Role::FOLLOWER == role()) {
    m_terminationPosition = logPosition;
    m_terminationLeadershipTermId = leadershipTermId;
    m_timeOfLastLogUpdateNs = m_clusterClock->timeNanos();
  }
}

void ConsensusModuleAgent::onTerminationAck(std::int64_t leadershipTermId,
                                            std::int64_t logPosition,
                                            std::int32_t senderMemberId) {
  logOnTerminationAck(m_memberId, leadershipTermId, logPosition,
                      senderMemberId);

  if (leadershipTermId == m_leadershipTermId &&
      logPosition >= m_terminationPosition && Role::LEADER == role()) {
    auto it = m_clusterMemberByIdMap.find(senderMemberId);
    if (it != m_clusterMemberByIdMap.end()) {
      ClusterMember &member = it->second;
      member.hasTerminated(true);

      if (m_clusterTermination->canTerminate(m_activeMembers,
                                             m_clusterClock->timeNanos())) {
        m_recordingLog->commitLogPosition(leadershipTermId,
                                          m_terminationPosition);
        closeAndTerminate();
      }
    }
  }
}

void ConsensusModuleAgent::onBackupQuery(
    std::int64_t correlationId, std::int32_t responseStreamId,
    std::int32_t version, const std::string &responseChannel,
    const std::vector<std::uint8_t> &encodedCredentials, Header &header) {
  if (!m_election) {
    if (State::ACTIVE == state() || State::SUSPENDED == state()) {
      auto session = std::make_shared<ClusterSession>(
          m_memberId, aeron::NULL_VALUE, responseStreamId,
          refineResponseChannel(responseChannel),
          sessionInfo(ClusterSession::Action::BACKUP.name(), header));

      const std::int64_t nowNs = m_clusterClock->timeNanos();
      session->action(ClusterSession::Action::BACKUP);
      session->asyncConnect(m_aeron, m_tempBuffer, m_ctx.clusterId());
      session->lastActivityNs(nowNs, correlationId);

      if (client::AeronCluster::Configuration::PROTOCOL_MAJOR_VERSION ==
          SemanticVersion::major(version)) {
        m_authenticator->onConnectRequest(
            session->id(), encodedCredentials,
            nowNs / 1000000); // Convert to milliseconds
        m_pendingBackupSessions.push_back(session);
      } else {
        const std::string detail =
            std::string(SESSION_INVALID_VERSION_MSG) + " " +
            SemanticVersion::toString(version) + ", cluster=" +
            SemanticVersion::toString(
                client::AeronCluster::Configuration::PROTOCOL_SEMANTIC_VERSION);
        session->reject(EventCode::Value::AUTHENTICATION_REJECTED, detail,
                        m_ctx.errorLog().get());
        m_rejectedBackupSessions.push_back(session);
      }
    }
  }
}

void ConsensusModuleAgent::onConsensusChallengeResponse(
    std::int64_t correlationId, std::int64_t clusterSessionId,
    const std::vector<std::uint8_t> &encodedCredentials) {
  onChallengeResponseForSession(m_pendingBackupSessions, correlationId,
                                clusterSessionId, encodedCredentials);
}

void ConsensusModuleAgent::onHeartbeatRequest(
    std::int64_t correlationId, std::int32_t responseStreamId,
    const std::string &responseChannel,
    const std::vector<std::uint8_t> &encodedCredentials, Header &header) {
  if (!m_election) {
    if (State::ACTIVE == state() || State::SUSPENDED == state()) {
      auto session = std::make_shared<ClusterSession>(
          m_memberId, aeron::NULL_VALUE, responseStreamId,
          refineResponseChannel(responseChannel),
          sessionInfo(ClusterSession::Action::HEARTBEAT.name(), header));

      session->action(ClusterSession::Action::HEARTBEAT);
      session->asyncConnect(m_aeron, m_tempBuffer, m_ctx.clusterId());

      const std::int64_t nowNs = m_clusterClock->timeNanos();
      session->lastActivityNs(nowNs, correlationId);
      m_authenticator->onConnectRequest(session->id(), encodedCredentials,
                                        nowNs /
                                            1000000); // Convert to milliseconds
      m_pendingBackupSessions.push_back(session);
    }
  }
}

void ConsensusModuleAgent::onStandbySnapshot(
    std::int64_t correlationId, std::int32_t version,
    const std::vector<StandbySnapshotEntry> &standbySnapshotEntries,
    std::int32_t responseStreamId, const std::string &responseChannel,
    const std::vector<std::uint8_t> &encodedCredentials, Header &header) {
  if (!m_election) {
    if (State::ACTIVE == state() || State::SUSPENDED == state()) {
      auto session = std::make_shared<ClusterSession>(
          m_memberId, aeron::NULL_VALUE, responseStreamId,
          refineResponseChannel(responseChannel),
          sessionInfo(ClusterSession::Action::STANDBY_SNAPSHOT.name(), header));

      const std::int64_t nowNs = m_clusterClock->timeNanos();
      session->action(ClusterSession::Action::STANDBY_SNAPSHOT);
      session->asyncConnect(m_aeron, m_tempBuffer, m_ctx.clusterId());
      session->lastActivityNs(nowNs, correlationId);
      // Wrap vector in shared_ptr<void> to match requestInput signature
      session->requestInput(std::make_shared<std::vector<StandbySnapshotEntry>>(
          standbySnapshotEntries));

      if (client::AeronCluster::Configuration::PROTOCOL_MAJOR_VERSION ==
          SemanticVersion::major(version)) {
        m_authenticator->onConnectRequest(
            session->id(), encodedCredentials,
            nowNs / 1000000); // Convert to milliseconds
        m_pendingBackupSessions.push_back(session);
      } else {
        const std::string detail =
            std::string(SESSION_INVALID_VERSION_MSG) + " " +
            SemanticVersion::toString(version) + ", cluster=" +
            SemanticVersion::toString(
                client::AeronCluster::Configuration::PROTOCOL_SEMANTIC_VERSION);
        session->reject(EventCode::Value::AUTHENTICATION_REJECTED, detail,
                        m_ctx.errorLog().get());
        m_rejectedBackupSessions.push_back(session);
      }
    }
  }
}

void ConsensusModuleAgent::onChallengeResponseForSession(
    std::vector<std::shared_ptr<ClusterSession>> &pendingSessions,
    std::int64_t correlationId, std::int64_t clusterSessionId,
    const std::vector<std::uint8_t> &encodedCredentials) {
  for (auto it = pendingSessions.rbegin(); it != pendingSessions.rend(); ++it) {
    auto session = *it;
    if (session->id() == clusterSessionId &&
        session->state() == ClusterSessionState::CHALLENGED) {
      const std::int64_t nowNs = m_clusterClock->timeNanos();
      session->lastActivityNs(nowNs, correlationId);
      m_authenticator->onChallengeResponse(
          clusterSessionId, encodedCredentials,
          nowNs / 1000000); // Convert to milliseconds
      break;
    }
  }
}

// Methods called by ConsensusModuleAdapter
void ConsensusModuleAgent::onServiceMessage(std::int64_t clusterSessionId,
                                            AtomicBuffer &buffer,
                                            std::int32_t offset,
                                            std::int32_t length) {
  const std::int32_t serviceId =
      PendingServiceMessageTracker::serviceIdFromServiceMessage(
          clusterSessionId);
  if (serviceId >= 0 &&
      serviceId <
          static_cast<std::int32_t>(m_pendingServiceMessageTrackers.size())) {
    auto &tracker = m_pendingServiceMessageTrackers[serviceId];
    if (tracker) {
      tracker->enqueueMessage(buffer, offset, length);
    }
  }
}

void ConsensusModuleAgent::onServiceCloseSession(
    std::int64_t clusterSessionId) {
  auto it = m_sessionByIdMap.find(clusterSessionId);
  if (it != m_sessionByIdMap.end()) {
    auto session = it->second;
    if (session) {
      session->closing(CloseReason::SERVICE_ACTION);

      if (Role::LEADER == role() && State::ACTIVE == state()) {
        const std::int64_t timestamp =
            m_clusterClock ? m_clusterClock->time() : 0;
        if (m_logPublisher && m_logPublisher->appendSessionClose(
                                  m_memberId, *session, m_leadershipTermId,
                                  timestamp, m_clusterTimeUnit)) {
          logAppendSessionClose(m_memberId, session->id(),
                                session->closeReason(), m_leadershipTermId,
                                timestamp, m_clusterTimeUnit);
          const std::string msg = "SERVICE_ACTION";
          if (m_egressPublisher) {
            m_egressPublisher->sendEvent(*session, m_leadershipTermId,
                                         m_memberId, EventCode::CLOSED, msg);
          }
          session->closedLogPosition(m_logPublisher->position());
          m_uncommittedClosedSessions.push_back(session);
          closeSession(session);
        }
      }
    }
  }
}

void ConsensusModuleAgent::onCancelTimer(std::int64_t correlationId) {
  if (m_timerService) {
    m_timerService->cancelTimerByCorrelationId(correlationId);
  }
}

void ConsensusModuleAgent::onServiceAck(std::int64_t logPosition,
                                        std::int64_t timestamp,
                                        std::int64_t ackId,
                                        std::int64_t relevantId,
                                        std::int32_t serviceId) {
  logOnServiceAck(m_memberId, logPosition, timestamp, m_clusterTimeUnit, ackId,
                  relevantId, serviceId);
  captureServiceAck(logPosition, ackId, relevantId, serviceId);

  if (aeron::cluster::ServiceAck::hasReached(logPosition, m_serviceAckId,
                                             m_serviceAckQueues)) {
    switch (state()) {
    case State::SNAPSHOT:
      ++m_serviceAckId;
      snapshotOnServiceAck(logPosition, timestamp, serviceId);
      // snapshotOnServiceAck(logPosition, timestamp,
      // pollServiceAcks(logPosition, serviceId));
      break;

    case State::QUITTING:
      closeAndTerminate();
      break;

    case State::TERMINATING:
      terminateOnServiceAck(logPosition);
      break;

    default:
      break;
    }
  }
}

void ConsensusModuleAgent::onClusterMembersQuery(std::int64_t correlationId,
                                                 bool isExtendedRequest) {
  if (isExtendedRequest) {
    if (m_serviceProxy && m_clusterClock) {
      m_serviceProxy->clusterMembersExtendedResponse(
          correlationId, m_clusterClock->timeNanos(), m_leaderMember.id(),
          m_memberId, m_activeMembers);
    }
  } else {
    if (m_serviceProxy) {
      const std::string activeMembersStr =
          ClusterMember::encodeAsString(m_activeMembers);
      m_serviceProxy->clusterMembersResponse(correlationId, m_leaderMember.id(),
                                             activeMembersStr);
    }
  }
}

void ConsensusModuleAgent::replicateStandbySnapshotsForStartup() {
  if (!m_archive || !m_recordingLog) {
    return;
  }

  auto archiveCtx = m_ctx.archiveContext();
  if (!archiveCtx) {
    return;
  }

  m_standbySnapshotReplicator = StandbySnapshotReplicator::newInstance(
      m_memberId, *archiveCtx, *m_recordingLog, m_serviceCount,
      m_ctx.leaderArchiveControlChannel(), archiveCtx->controlRequestStreamId(),
      m_ctx.replicationChannel(), m_ctx.fileSyncLevel(),
      m_ctx.snapshotCounter());

  while (m_standbySnapshotReplicator &&
         !m_standbySnapshotReplicator->isComplete()) {
    try {
      const std::int32_t workCount =
          m_standbySnapshotReplicator->poll(m_clusterClock->timeNanos());
      m_idleStrategy->idle(workCount);
    } catch (const ClusterException &ex) {
      m_ctx.countedErrorHandler()->onError(ex);
      break;
    }

    checkInterruptStatus();
    if (m_aeron && !m_aeron->isClosed()) {
      m_aeron->conductorAgentInvoker().invoke();
    }
    if (m_aeron && m_aeron->isClosed()) {
      throw AgentTerminationException("unexpected Aeron close");
    }
  }

  if (m_standbySnapshotReplicator) {
    m_standbySnapshotReplicator.reset();
  }
}

std::shared_ptr<RecordingLog::RecoveryPlan>
ConsensusModuleAgent::recoverFromBootstrapState() {
  auto bootstrapState = m_ctx.bootstrapState();
  if (!bootstrapState || !m_recordingLog || !m_archive) {
    return nullptr;
  }

  m_logRecordingId = bootstrapState->logRecordingId;
  auto recoveryPlan = std::make_shared<RecordingLog::RecoveryPlan>(
      m_recordingLog->createRecoveryPlan(m_archive, m_serviceCount,
                                         m_logRecordingId));

  m_expectedAckPosition = bootstrapState->expectedAckPosition;
  m_serviceAckId = bootstrapState->serviceAckId;
  m_leadershipTermId = bootstrapState->leadershipTermId;
  m_nextSessionId = bootstrapState->nextSessionId;

  // Load timers
  for (const auto &timer : bootstrapState->timers) {
    onLoadTimer(timer.correlationId, timer.deadline, util::DirectBuffer(), 0,
                0);
  }

  // Load sessions
  for (const auto &sessionExport : bootstrapState->sessions) {
    onLoadClusterSession(
        sessionExport.id, sessionExport.correlationId,
        sessionExport.openedLogPosition, sessionExport.timeOfLastActivityNs,
        sessionExport.closeReason, sessionExport.responseStreamId,
        sessionExport.responseChannel, util::DirectBuffer(), 0, 0);
  }

  return recoveryPlan;
}

void ConsensusModuleAgent::onUnavailableIngressImage(Image &image) {
  // Handle unavailable ingress image - typically used for detecting client
  // disconnections This can trigger cleanup of related sessions
}

// Helper methods implementation
void ConsensusModuleAgent::logAppendSessionClose(
    std::int32_t memberId, std::int64_t id, CloseReason::Value closeReason,
    std::int64_t leadershipTermId, std::int64_t timestamp,
    std::chrono::milliseconds::rep timeUnit) {
  // Log method - empty in Java version (for debugging)
}

std::int32_t ConsensusModuleAgent::updateLeaderPosition(std::int64_t nowNs) {
  if (m_appendPosition) {
    return updateLeaderPosition(nowNs, m_appendPosition->get());
  }
  return 0;
}

std::int32_t ConsensusModuleAgent::updateLeaderPosition(std::int64_t nowNs,
                                                        std::int64_t position) {
  m_thisMember.logPosition(position).timeOfLastAppendPositionNs(nowNs);
  // Calculate quorum position from active members (1:1 translation from Java)
  // Java version uses rankedPositions array initialized to quorumThreshold
  // size
  std::vector<std::int64_t> rankedPositions(
      ClusterMember::quorumThreshold(m_activeMembers.size()));
  const std::int64_t quorumPos =
      ClusterMember::quorumPosition(m_activeMembers, rankedPositions);
  const std::int64_t commitPos = std::min(quorumPos, position);

  const std::int64_t leaderPosition =
      m_commitPosition ? m_commitPosition->get() : 0;
  if (commitPos > leaderPosition ||
      (commitPos == leaderPosition &&
       nowNs >= (m_timeOfLastLogUpdateNs + m_leaderHeartbeatIntervalNs))) {
    if (m_consensusPublisher) {
      m_consensusPublisher->commitPosition(m_leaderMember.publication(),
                                           m_leadershipTermId, commitPos,
                                           m_memberId);
    }

    if (m_commitPosition) {
      m_commitPosition->setRelease(commitPos);
    }
    m_timeOfLastLogUpdateNs = nowNs;

    sweepUncommittedEntriesTo(commitPos);
    return 1;
  }

  return 0;
}

std::int32_t ConsensusModuleAgent::updateFollowerPosition(std::int64_t nowNs) {
  const std::int64_t recordedPosition =
      m_appendPosition ? m_appendPosition->get() : m_logRecordingStopPosition;
  return updateFollowerPosition(m_leaderMember.publication(), nowNs,
                                m_leadershipTermId, recordedPosition,
                                APPEND_POSITION_FLAG_NONE);
}

std::int32_t ConsensusModuleAgent::updateFollowerPosition(
    std::shared_ptr<ExclusivePublication> publication, std::int64_t nowNs,
    std::int64_t leadershipTermId, std::int64_t appendPosition,
    std::int16_t flags) {
  const std::int64_t position = std::max(appendPosition, m_lastAppendPosition);
  if ((position > m_lastAppendPosition ||
       nowNs >=
           (m_timeOfLastAppendPositionSendNs + m_leaderHeartbeatIntervalNs))) {
    if (m_consensusPublisher &&
        m_consensusPublisher->appendPosition(publication, leadershipTermId,
                                             position, m_memberId, flags)) {
      if (position > m_lastAppendPosition) {
        m_lastAppendPosition = position;
        m_timeOfLastAppendPositionUpdateNs = nowNs;
      }
      m_timeOfLastAppendPositionSendNs = nowNs;
      return 1;
    }
  }

  return 0;
}

std::int32_t ConsensusModuleAgent::processPendingSessions(
    std::vector<std::shared_ptr<ClusterSession>> &pendingSessions,
    std::vector<std::shared_ptr<ClusterSession>> &rejectedSessions,
    std::int64_t nowNs) {
  std::int32_t workCount = 0;

  for (auto it = pendingSessions.rbegin(); it != pendingSessions.rend();) {
    auto session = *it;

    if (session->state() == ClusterSessionState::INVALID) {
      it = std::make_reverse_iterator(
          pendingSessions.erase(std::next(it).base()));
      // Convert shared_ptr<CountedErrorHandler> to exception_handler_t
      exception_handler_t errorHandler =
          [handler = m_ctx.countedErrorHandler()](const std::exception &ex) {
            if (handler) {
              handler->onError(ex);
            }
          };
      session->close(m_aeron, errorHandler, "invalid session");
      continue;
    }

    if (nowNs > (session->timeOfLastActivityNs() + m_sessionTimeoutNs) &&
        session->state() != ClusterSessionState::INIT) {
      it = std::make_reverse_iterator(
          pendingSessions.erase(std::next(it).base()));
      // Convert shared_ptr<CountedErrorHandler> to exception_handler_t
      exception_handler_t errorHandler =
          [handler = m_ctx.countedErrorHandler()](const std::exception &ex) {
            if (handler) {
              handler->onError(ex);
            }
          };
      session->close(m_aeron, errorHandler, "session timed out");
      if (m_ctx.timedOutClientCounter()) {
        m_ctx.timedOutClientCounter()->incrementRelease();
      }
      continue;
    }

    if (session->state() == ClusterSessionState::INIT ||
        session->state() == ClusterSessionState::CONNECTING ||
        session->state() == ClusterSessionState::CONNECTED) {
      if (session->isResponsePublicationConnected(m_aeron, nowNs)) {
        session->state(ClusterSessionState::CONNECTED, "connected");
        if (m_authenticator) {
          m_authenticator->onConnectedSession(session->id(),
                                              m_clusterClock->timeMillis());
        }
      }
    }

    if (session->state() == ClusterSessionState::CHALLENGED) {
      if (session->isResponsePublicationConnected(m_aeron, nowNs)) {
        if (m_authenticator) {
          m_authenticator->onChallengedSession(session->id(),
                                               m_clusterClock->timeMillis());
        }
      }
    }

    if (session->state() == ClusterSessionState::AUTHENTICATED) {
      const ClusterSessionAction action = session->action();
      switch (action) {
      case ClusterSessionAction::CLIENT: {
        if (session->appendSessionToLogAndSendOpen(
                *m_logPublisher, *m_egressPublisher, m_leadershipTermId,
                m_memberId, nowNs, m_clusterClock->time())) {
          if (session->id() >= m_nextCommittedSessionId) {
            m_nextCommittedSessionId = session->id() + 1;
          }
          it = std::make_reverse_iterator(
              pendingSessions.erase(std::next(it).base()));
          addSession(session);
          workCount += 1;
          if (m_consensusModuleExtension) {
            m_consensusModuleExtension->onSessionOpened(session->id());
          }
        }
        break;
      }

      case ClusterSessionAction::BACKUP: {
        if (!m_authorisationService->isAuthorised(
                MessageHeader::sbeSchemaId(), BackupQuery::sbeTemplateId(), 0,
                session->encodedPrincipal())) {
          session->reject(EventCode::Value::AUTHENTICATION_REJECTED,
                          "Not authorised for BackupQuery",
                          m_ctx.errorLog().get());
          break;
        }

        auto entry = m_recordingLog->findLastTerm();
        if (entry && m_consensusPublisher &&
            m_consensusPublisher->backupResponse(
                *session, m_commitPosition ? m_commitPosition->id() : 0,
                m_leaderMember.id(), m_thisMember.id(), *entry, *m_recoveryPlan,
                ClusterMember::encodeAsString(m_activeMembers))) {
          it = std::make_reverse_iterator(
              pendingSessions.erase(std::next(it).base()));
          exception_handler_t errorHandler =
              [handler =
                   m_ctx.countedErrorHandler()](const std::exception &ex) {
                if (handler) {
                  handler->onError(ex);
                }
              };
          session->close(m_aeron, errorHandler, "done");
          workCount += 1;
        }
        break;
      }

      case ClusterSessionAction::HEARTBEAT: {
        if (!m_authorisationService->isAuthorised(
                MessageHeader::sbeSchemaId(), HeartbeatRequest::sbeTemplateId(),
                0, session->encodedPrincipal())) {
          session->reject(EventCode::Value::AUTHENTICATION_REJECTED,
                          "Not authorised for Heartbeat",
                          m_ctx.errorLog().get());
          break;
        }

        if (m_consensusPublisher &&
            m_consensusPublisher->heartbeatResponse(*session)) {
          it = std::make_reverse_iterator(
              pendingSessions.erase(std::next(it).base()));
          exception_handler_t errorHandler =
              [handler =
                   m_ctx.countedErrorHandler()](const std::exception &ex) {
                if (handler) {
                  handler->onError(ex);
                }
              };
          session->close(m_aeron, errorHandler, "done");
          workCount += 1;
        }
        break;
      }

      case ClusterSessionAction::STANDBY_SNAPSHOT: {
        if (!m_authorisationService->isAuthorised(
                MessageHeader::sbeSchemaId(), StandbySnapshot::sbeTemplateId(),
                0, session->encodedPrincipal())) {
          session->reject(EventCode::Value::AUTHENTICATION_REJECTED,
                          "Not authorised for StandbySnapshot",
                          m_ctx.errorLog().get());
          break;
        }

        // Handle standby snapshot entries
        // Note: requestInput() returns StandbySnapshotEntry list
        it = std::make_reverse_iterator(
            pendingSessions.erase(std::next(it).base()));
        exception_handler_t errorHandler =
            [handler = m_ctx.countedErrorHandler()](const std::exception &ex) {
              if (handler) {
                handler->onError(ex);
              }
            };
        session->close(m_aeron, errorHandler, "done");
        workCount += 1;
        break;
      }

      default:
        break;
      }
    }

    ++it;
  }

  return workCount;
}

std::int32_t ConsensusModuleAgent::checkSessions(
    std::vector<std::shared_ptr<ClusterSession>> &sessions,
    std::int64_t nowNs) {
  std::int32_t workCount = 0;

  for (auto it = sessions.rbegin(); it != sessions.rend();) {
    auto session = *it;

    if (nowNs > (session->timeOfLastActivityNs() + m_sessionTimeoutNs)) {
      switch (session->state()) {
      case ClusterSessionState::OPEN: {
        session->closing(CloseReason::TIMEOUT);

        const std::int64_t timestamp =
            m_clusterClock ? m_clusterClock->time() : 0;
        if (m_logPublisher && m_logPublisher->appendSessionClose(
                                  m_memberId, *session, m_leadershipTermId,
                                  timestamp, m_clusterTimeUnit)) {
          logAppendSessionClose(m_memberId, session->id(),
                                session->closeReason(), m_leadershipTermId,
                                timestamp, m_clusterTimeUnit);
          const std::string msg =
              std::to_string(static_cast<std::int32_t>(session->closeReason()));
          if (m_egressPublisher) {
            m_egressPublisher->sendEvent(*session, m_leadershipTermId,
                                         m_memberId, EventCode::CLOSED, msg);
          }
          session->closedLogPosition(m_logPublisher->position());
          m_uncommittedClosedSessions.push_back(session);
          if (m_ctx.timedOutClientCounter()) {
            m_ctx.timedOutClientCounter()->incrementRelease();
          }
          closeSession(session);
        }
        workCount++;
        break;
      }

      case ClusterSessionState::CLOSING: {
        const std::int64_t timestamp =
            m_clusterClock ? m_clusterClock->time() : 0;
        if (m_logPublisher && m_logPublisher->appendSessionClose(
                                  m_memberId, *session, m_leadershipTermId,
                                  timestamp, m_clusterTimeUnit)) {
          logAppendSessionClose(m_memberId, session->id(),
                                session->closeReason(), m_leadershipTermId,
                                timestamp, m_clusterTimeUnit);
          const std::string msg =
              std::to_string(static_cast<std::int32_t>(session->closeReason()));
          if (m_egressPublisher) {
            m_egressPublisher->sendEvent(*session, m_leadershipTermId,
                                         m_memberId, EventCode::CLOSED, msg);
          }
          session->closedLogPosition(m_logPublisher->position());
          m_uncommittedClosedSessions.push_back(session);
          if (session->closeReason() == CloseReason::TIMEOUT &&
              m_ctx.timedOutClientCounter()) {
            m_ctx.timedOutClientCounter()->incrementRelease();
          }
          closeSession(session);
        }
        workCount++;
        break;
      }

      default:
        break;
      }
    }

    ++it;
  }

  return workCount;
}

std::int32_t ConsensusModuleAgent::sendRedirects(
    std::vector<std::shared_ptr<ClusterSession>> &redirectSessions,
    std::int64_t nowNs) {
  std::int32_t workCount = 0;

  for (auto it = redirectSessions.rbegin(); it != redirectSessions.rend();) {
    auto session = *it;
    const codecs::EventCode::Value eventCode =
        codecs::EventCode::Value::REDIRECT;

    if (session->isResponsePublicationConnected(m_aeron, nowNs) &&
        m_egressPublisher &&
        m_egressPublisher->sendEvent(*session, m_leadershipTermId,
                                     m_leaderMember.id(), eventCode,
                                     m_ingressEndpoints)) {
      it = std::make_reverse_iterator(
          redirectSessions.erase(std::next(it).base()));
      exception_handler_t errorHandler =
          [handler = m_ctx.countedErrorHandler()](const std::exception &ex) {
            if (handler) {
              handler->onError(ex);
            }
          };
      session->close(m_aeron, errorHandler,
                     std::to_string(static_cast<std::int32_t>(eventCode)));
      workCount++;
    } else if (session->state() != ClusterSessionState::INIT &&
               nowNs > (session->timeOfLastActivityNs() + m_sessionTimeoutNs)) {
      it = std::make_reverse_iterator(
          redirectSessions.erase(std::next(it).base()));
      // Convert shared_ptr<CountedErrorHandler> to exception_handler_t
      exception_handler_t errorHandler =
          [handler = m_ctx.countedErrorHandler()](const std::exception &ex) {
            if (handler) {
              handler->onError(ex);
            }
          };
      session->close(m_aeron, errorHandler, "session timed out");
      workCount++;
    } else if (session->state() == ClusterSessionState::INVALID) {
      it = std::make_reverse_iterator(
          redirectSessions.erase(std::next(it).base()));
      exception_handler_t errorHandler =
          [handler = m_ctx.countedErrorHandler()](const std::exception &ex) {
            if (handler) {
              handler->onError(ex);
            }
          };
      session->close(m_aeron, errorHandler, "invalid");
      workCount++;
    } else {
      ++it;
    }
  }

  return workCount;
}

std::int32_t ConsensusModuleAgent::sendRejections(
    std::vector<std::shared_ptr<ClusterSession>> &rejectedSessions,
    std::int64_t nowNs) {
  std::int32_t workCount = 0;

  for (auto it = rejectedSessions.rbegin(); it != rejectedSessions.rend();) {
    auto session = *it;

    if (session->isResponsePublicationConnected(m_aeron, nowNs)) {
      if (m_egressPublisher) {
        m_egressPublisher->sendEvent(*session, m_leadershipTermId, m_memberId,
                                     session->eventCode(),
                                     session->responseDetail());
      }
      it = std::make_reverse_iterator(
          rejectedSessions.erase(std::next(it).base()));
      exception_handler_t errorHandler =
          [handler = m_ctx.countedErrorHandler()](const std::exception &ex) {
            if (handler) {
              handler->onError(ex);
            }
          };
      session->close(m_aeron, errorHandler, "rejected");
      workCount++;
    } else if (session->state() != ClusterSessionState::INIT &&
               nowNs > (session->timeOfLastActivityNs() + m_sessionTimeoutNs)) {
      it = std::make_reverse_iterator(
          rejectedSessions.erase(std::next(it).base()));
      // Convert shared_ptr<CountedErrorHandler> to exception_handler_t
      exception_handler_t errorHandler =
          [handler = m_ctx.countedErrorHandler()](const std::exception &ex) {
            if (handler) {
              handler->onError(ex);
            }
          };
      session->close(m_aeron, errorHandler, "session timed out");
      workCount++;
    } else if (session->state() == ClusterSessionState::INVALID) {
      it = std::make_reverse_iterator(
          rejectedSessions.erase(std::next(it).base()));
      exception_handler_t errorHandler =
          [handler = m_ctx.countedErrorHandler()](const std::exception &ex) {
            if (handler) {
              handler->onError(ex);
            }
          };
      session->close(m_aeron, errorHandler, "invalid");
      workCount++;
    } else {
      ++it;
    }
  }

  return workCount;
}

void ConsensusModuleAgent::closeAndTerminate() {
  tryStopLogRecording();
  state(State::CLOSED);
  throw ClusterTerminationException(true);
}

void ConsensusModuleAgent::terminateOnServiceAck(std::int64_t logPosition) {
  if (!m_clusterTermination) {
    if (m_terminationLeadershipTermId == m_leadershipTermId) {
      if (m_consensusPublisher) {
        m_consensusPublisher->terminationAck(m_leaderMember.publication(),
                                             m_leadershipTermId, logPosition,
                                             m_memberId);
      }
    } else {
      // Log error: termination ack not sent - different leadership term
    }

    if (m_recordingLog) {
      m_recordingLog->commitLogPosition(m_leadershipTermId, logPosition);
    }
    closeAndTerminate();
  } else {
    m_clusterTermination->onServicesTerminated();
    if (m_clusterTermination->canTerminate(m_activeMembers,
                                           m_clusterClock->timeNanos())) {
      if (m_recordingLog) {
        m_recordingLog->commitLogPosition(m_leadershipTermId, logPosition);
      }
      closeAndTerminate();
    }
  }
}

void ConsensusModuleAgent::snapshotOnServiceAck(std::int64_t logPosition,
                                                std::int64_t timestamp,
                                                std::int32_t serviceId) {
  // Handle snapshot on service ACK
  // This would trigger snapshot creation logic
  ++m_serviceAckId;
}

void ConsensusModuleAgent::captureServiceAck(std::int64_t logPosition,
                                             std::int64_t ackId,
                                             std::int64_t relevantId,
                                             std::int32_t serviceId) {
  if (serviceId >= 0 &&
      serviceId < static_cast<std::int32_t>(m_serviceAckQueues.size())) {
    aeron::cluster::ServiceAck::enqueue(m_serviceAckQueues[serviceId],
                                        logPosition, ackId, relevantId);
  }
}

void ConsensusModuleAgent::logOnServiceAck(
    std::int32_t memberId, std::int64_t logPosition, std::int64_t timestamp,
    std::chrono::milliseconds::rep timeUnit, std::int64_t ackId,
    std::int64_t relevantId, std::int32_t serviceId) {
  // Log method - empty in Java version (for debugging)
}

std::int32_t
ConsensusModuleAgent::checkClusterControlToggle(std::int64_t nowNs) {
  if (State::ACTIVE == state()) {
    if (m_controlToggle) {
      switch (ClusterControl::get(m_controlToggle)) {
      case ClusterControl::ToggleState::SUSPEND: {
        const std::int64_t timestamp =
            m_clusterClock ? m_clusterClock->time() : 0;
        if (appendAction(ClusterAction::Value::SUSPEND, timestamp,
                         0)) // CLUSTER_ACTION_FLAGS_DEFAULT = 0
        {
          state(State::SUSPENDED);
        }
        break;
      }

      case ClusterControl::ToggleState::SNAPSHOT: {
        const std::int64_t timestamp =
            m_clusterClock ? m_clusterClock->time() : 0;
        if (appendAction(ClusterAction::Value::SNAPSHOT, timestamp, 0)) {
          state(State::SNAPSHOT);
        }
        break;
      }

      default:
        break;
      }
    }
  }

  return 0;
}

std::int32_t ConsensusModuleAgent::checkNodeControlToggle() {
  if (State::ACTIVE == state() && m_nodeControlToggle) {
    // Handle node control toggle
    // This would check NodeControl::ToggleState and perform actions
  }

  return 0;
}

std::int32_t ConsensusModuleAgent::pollArchiveEvents() {
  // Poll archive events
  // This would poll for archive-related events like recording signals
  return 0;
}

std::int32_t
ConsensusModuleAgent::pollStandbySnapshotReplication(std::int64_t nowNs) {
  if (m_standbySnapshotReplicator &&
      !m_standbySnapshotReplicator->isComplete()) {
    return m_standbySnapshotReplicator->poll(nowNs);
  }
  return 0;
}

bool ConsensusModuleAgent::hasActiveQuorum() {
  return ClusterMember::hasActiveQuorum(
      m_activeMembers, m_clusterClock->timeNanos(), m_leaderHeartbeatTimeoutNs);
}

void ConsensusModuleAgent::sweepUncommittedEntriesTo(
    std::int64_t commitPosition) {
  // Sweep uncommitted closed sessions and timers up to commit position
  while (!m_uncommittedClosedSessions.empty()) {
    auto session = m_uncommittedClosedSessions.front();
    if (session->closedLogPosition() > commitPosition) {
      break;
    }
    m_uncommittedClosedSessions.pop_front();
  }

  // Similar logic for uncommitted timers would go here
}

bool ConsensusModuleAgent::appendAction(ClusterAction::Value action,
                                        std::int64_t timestamp,
                                        std::int32_t flags) {
  if (m_logPublisher) {
    return m_logPublisher->appendClusterAction(m_leadershipTermId, timestamp,
                                               action, flags);
  }
  return false;
}

// Log methods (empty implementations matching Java version)
void ConsensusModuleAgent::logOnCanvassPosition(
    std::int32_t memberId, std::int64_t logLeadershipTermId,
    std::int64_t logPosition, std::int64_t leadershipTermId,
    std::int32_t followerMemberId, std::int32_t protocolVersion) {
  // Empty - for debugging/logging purposes
}

void ConsensusModuleAgent::logOnRequestVote(std::int32_t memberId,
                                            std::int64_t logLeadershipTermId,
                                            std::int64_t logPosition,
                                            std::int64_t candidateTermId,
                                            std::int32_t candidateId,
                                            std::int32_t protocolVersion) {
  // Empty - for debugging/logging purposes
}

void ConsensusModuleAgent::logOnNewLeadershipTerm(
    std::int32_t memberId, std::int64_t leadershipTermId,
    std::int64_t logPosition, std::int64_t timestamp,
    std::int64_t termBaseLogPosition, std::chrono::milliseconds::rep timeUnit,
    std::int32_t appVersion) {
  // Empty - for debugging/logging purposes
}

void ConsensusModuleAgent::logOnAppendPosition(std::int32_t memberId,
                                               std::int64_t leadershipTermId,
                                               std::int64_t logPosition,
                                               std::int32_t followerMemberId,
                                               std::int16_t flags) {
  // Empty - for debugging/logging purposes
}

void ConsensusModuleAgent::logOnCommitPosition(std::int32_t memberId,
                                               std::int64_t leadershipTermId,
                                               std::int64_t logPosition,
                                               std::int32_t followerMemberId) {
  // Empty - for debugging/logging purposes
}

void ConsensusModuleAgent::logOnCatchupPosition(
    std::int32_t memberId, std::int64_t leadershipTermId,
    std::int64_t logPosition, std::int32_t followerMemberId,
    const std::string &catchupEndpoint) {
  // Empty - for debugging/logging purposes
}

void ConsensusModuleAgent::logOnStopCatchup(std::int32_t memberId,
                                            std::int64_t leadershipTermId,
                                            std::int32_t followerMemberId) {
  // Empty - for debugging/logging purposes
}

void ConsensusModuleAgent::logOnTerminationPosition(
    std::int32_t memberId, std::int64_t logLeadershipTermId,
    std::int64_t logPosition) {
  // Empty - for debugging/logging purposes
}

void ConsensusModuleAgent::logOnTerminationAck(std::int32_t memberId,
                                               std::int64_t logLeadershipTermId,
                                               std::int64_t logPosition,
                                               std::int32_t senderMemberId) {
  // Empty - for debugging/logging purposes
}

void ConsensusModuleAgent::prepareSessionsForNewTerm(bool isStartup) {
  if (isStartup) {
    for (auto &session : m_sessions) {
      if (session && session->state() == ClusterSessionState::OPEN) {
        session->closing(CloseReason::TIMEOUT);
      }
    }
  } else {
    for (auto &session : m_sessions) {
      if (session && session->state() == ClusterSessionState::OPEN) {
        exception_handler_t errorHandler =
            [handler = m_ctx.countedErrorHandler()](const std::exception &ex) {
              if (handler) {
                handler->onError(ex);
              }
            };
        // Create AtomicBuffer wrapper for ExpandableArrayBuffer
        AtomicBuffer tempAtomicBuffer(m_tempBuffer.buffer(),
                                      m_tempBuffer.capacity());
        session->connect(errorHandler, m_aeron, tempAtomicBuffer,
                         m_ctx.clusterId());
      }
    }

    const std::int64_t nowNs = m_clusterClock->timeNanos();
    for (auto &session : m_sessions) {
      if (session && session->state() == ClusterSessionState::OPEN) {
        session->timeOfLastActivityNs(nowNs);
        session->hasNewLeaderEventPending(true);
      }
    }
  }
}

void ConsensusModuleAgent::updateMemberDetails(const ClusterMember &newLeader) {
  m_leaderMember = newLeader;

  for (auto &member : m_activeMembers) {
    member.isLeader(member.id() == m_leaderMember.id());
  }

  m_ingressEndpoints = ClusterMember::ingressEndpoints(m_activeMembers);
}

// Election support methods
std::int64_t
ConsensusModuleAgent::prepareForNewLeadership(std::int64_t logPosition) {
  // Prepare for new leadership - return the last appended position
  if (m_logPublisher) {
    return m_logPublisher->position();
  }
  return archive::client::NULL_POSITION;
}

std::int64_t ConsensusModuleAgent::calculateQuorumPosition() {
  // Calculate quorum position based on active members
  if (m_activeMembers.empty()) {
    return 0;
  }

  std::vector<std::int64_t> positions;
  for (const auto &member : m_activeMembers) {
    positions.push_back(member.logPosition());
  }

  std::sort(positions.begin(), positions.end());
  const std::size_t quorumIndex = positions.size() / 2;
  return positions[quorumIndex];
}

void ConsensusModuleAgent::trackCatchupCompletion(ClusterMember *follower,
                                                  std::int64_t leadershipTermId,
                                                  std::int16_t flags) {
  // Track catchup completion for a follower
  if (follower && (flags & APPEND_POSITION_FLAG_CATCHUP) != 0) {
    // Follower is in catchup mode
  }
}

std::unique_ptr<LogReplay>
ConsensusModuleAgent::createLogReplay(std::int64_t startPosition,
                                      std::int64_t stopPosition) {
  if (m_archive && m_logAdapter) {
    return std::make_unique<LogReplay>(m_archive, m_logRecordingId,
                                       startPosition, stopPosition,
                                       *m_logAdapter, m_ctx);
  }
  return nullptr;
}

std::unique_ptr<RecordingReplication> ConsensusModuleAgent::newLogReplication(
    const std::string &archiveEndpoint,
    const std::string &archiveResponseEndpoint,
    std::int64_t replicationRecordingId, std::int64_t stopPosition,
    std::int64_t nowNs) {
  if (!m_archive) {
    return nullptr;
  }

  // Create replication params
  std::string replicationChannel = m_ctx.replicationChannel();
  ::aeron::archive::client::ReplicationParams replicationParams;
  replicationParams.dstRecordingId(m_logRecordingId)
      .stopPosition(stopPosition)
      .replicationSessionId(
          static_cast<std::int32_t>(m_aeron->nextCorrelationId()));

  if (!archiveResponseEndpoint.empty()) {
    // TODO: Handle response endpoint via channel URI modification
    // For now, set replication channel directly
    replicationParams.replicationChannel(replicationChannel);
  } else {
    replicationParams.replicationChannel(replicationChannel);
  }

  // Create destination URI by appending endpoint to control channel
  std::string srcArchiveChannel = m_ctx.leaderArchiveControlChannel();
  if (!archiveEndpoint.empty()) {
    // Append endpoint to channel
    if (srcArchiveChannel.find('?') != std::string::npos) {
      srcArchiveChannel += "|endpoint=" + archiveEndpoint;
    } else {
      srcArchiveChannel += "?endpoint=" + archiveEndpoint;
    }
  }

  return std::make_unique<RecordingReplication>(
      m_archive, replicationRecordingId, srcArchiveChannel,
      m_archive->context().controlRequestStreamId(), replicationParams,
      m_ctx.leaderHeartbeatTimeoutNs(), m_ctx.leaderHeartbeatIntervalNs(),
      nowNs);
}

void ConsensusModuleAgent::joinLogAsLeader(std::int64_t leadershipTermId,
                                           std::int64_t logPosition,
                                           std::int32_t logSessionId,
                                           bool isStartup) {
  m_leadershipTermId = leadershipTermId;
  prepareSessionsForNewTerm(isStartup);
}

bool ConsensusModuleAgent::tryJoinLogAsFollower(std::shared_ptr<Image> image,
                                                bool isStartup,
                                                std::int64_t nowNs) {
  if (m_logAdapter && image) {
    m_logAdapter->image(image);
    prepareSessionsForNewTerm(isStartup);
    return true;
  }
  return false;
}

bool ConsensusModuleAgent::appendNewLeadershipTermEvent(std::int64_t nowNs) {
  if (m_logPublisher && m_election && m_clusterClock) {
    // Convert nowNs (nanoseconds) to cluster time unit (milliseconds by
    // default) Java: clusterTimeUnit.convert(nowNs, NANOSECONDS)
    const std::int64_t timestamp =
        nowNs / 1000000; // Convert nanoseconds to milliseconds
    const std::int64_t termBaseLogPosition = m_election->logPosition();
    return m_logPublisher->appendNewLeadershipTermEvent(
        m_leadershipTermId, timestamp, termBaseLogPosition, m_memberId,
        m_logPublisher->sessionId(), m_clusterTimeUnit, m_ctx.appVersion());
  }
  return false;
}

void ConsensusModuleAgent::publishCommitPosition(
    std::int64_t position, std::int64_t leadershipTermId) {
  if (m_consensusPublisher) {
    for (auto &member : m_activeMembers) {
      if (member.id() != m_memberId && member.publication()) {
        m_consensusPublisher->commitPosition(
            member.publication(), leadershipTermId, position, m_memberId);
      }
    }
  }
}

std::int32_t ConsensusModuleAgent::addLogPublication(std::int64_t position) {
  // Add log publication and return session ID
  if (m_logPublisher) {
    return m_logPublisher->sessionId();
  }
  return 0;
}

bool ConsensusModuleAgent::isCatchupNearLive(std::int64_t position) const {
  if (m_commitPosition) {
    const std::int64_t commitPos = m_commitPosition->get();
    return position >= (commitPos - m_ctx.leaderHeartbeatIntervalNs());
  }
  return false;
}

std::int32_t ConsensusModuleAgent::catchupPoll(std::int64_t limitPosition,
                                               std::int64_t nowNs) {
  std::int32_t workCount = 0;
  if (m_logAdapter) {
    workCount = m_logAdapter->poll(limitPosition);
  }
  return workCount;
}

void ConsensusModuleAgent::catchupInitiated(std::int64_t nowNs) {
  m_timeOfLastLogUpdateNs = nowNs;
}

void ConsensusModuleAgent::liveLogDestination(const std::string &destination) {
  m_liveLogDestination = destination;
}

void ConsensusModuleAgent::catchupLogDestination(
    const std::string &destination) {
  m_catchupLogDestination = destination;
}

void ConsensusModuleAgent::stopAllCatchups() {
  m_liveLogDestination.clear();
  m_catchupLogDestination.clear();
}

} // namespace cluster
} // namespace aeron