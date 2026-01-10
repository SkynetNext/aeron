#pragma once

#ifndef NOMINMAX
#define NOMINMAX
#endif

#include "ExclusivePublication.h"
#include "Publication.h"
#include "client/ClusterExceptions.h"
#include <algorithm>
#include <memory>
#include <string>
#include <thread>
#include <vector>

// CommonContext constants are available through ChannelUri.h
#include "RecordingLog.h"
#include "ClusterSession.h"
#include "concurrent/AtomicBuffer.h"
#include "concurrent/logbuffer/BufferClaim.h"
#include "generated/aeron_cluster_codecs/AppendPosition.h"
#include "generated/aeron_cluster_codecs/BackupQuery.h"
#include "generated/aeron_cluster_codecs/BackupResponse.h"
// Undefine Windows macros that conflict with BooleanType
#ifdef TRUE
#undef TRUE
#endif
#ifdef FALSE
#undef FALSE
#endif
#include "generated/aeron_cluster_codecs/BooleanType.h"
#include "generated/aeron_cluster_codecs/CanvassPosition.h"
#include "generated/aeron_cluster_codecs/CatchupPosition.h"
#include "generated/aeron_cluster_codecs/ChallengeResponse.h"
#include "generated/aeron_cluster_codecs/CommitPosition.h"
#include "generated/aeron_cluster_codecs/HeartbeatRequest.h"
#include "generated/aeron_cluster_codecs/HeartbeatResponse.h"
#include "generated/aeron_cluster_codecs/MessageHeader.h"
#include "generated/aeron_cluster_codecs/NewLeadershipTerm.h"
#include "generated/aeron_cluster_codecs/RequestVote.h"
#include "generated/aeron_cluster_codecs/StandbySnapshot.h"
#include "generated/aeron_cluster_codecs/StopCatchup.h"
#include "generated/aeron_cluster_codecs/TerminationAck.h"
#include "generated/aeron_cluster_codecs/TerminationPosition.h"
#include "generated/aeron_cluster_codecs/Vote.h"
#include "util/DirectBuffer.h"

namespace aeron {
namespace cluster {
using namespace aeron::concurrent;
using namespace aeron::concurrent::logbuffer;
using namespace aeron::cluster::codecs;

class ClusterSession; // Forward declaration

class ConsensusPublisher {
public:
  ConsensusPublisher();

  void canvassPosition(std::shared_ptr<ExclusivePublication> publication,
                       std::int64_t logLeadershipTermId,
                       std::int64_t logPosition, std::int64_t leadershipTermId,
                       std::int32_t followerMemberId);

  bool requestVote(std::shared_ptr<ExclusivePublication> publication,
                   std::int64_t logLeadershipTermId, std::int64_t logPosition,
                   std::int64_t candidateTermId,
                   std::int32_t candidateMemberId);

  void placeVote(std::shared_ptr<ExclusivePublication> publication,
                 std::int64_t candidateTermId, std::int64_t logLeadershipTermId,
                 std::int64_t logPosition, std::int32_t candidateMemberId,
                 std::int32_t followerMemberId, bool vote);

  void newLeadershipTerm(std::shared_ptr<ExclusivePublication> publication,
                         std::int64_t logLeadershipTermId,
                         std::int64_t nextLeadershipTermId,
                         std::int64_t nextTermBaseLogPosition,
                         std::int64_t nextLogPosition,
                         std::int64_t leadershipTermId,
                         std::int64_t termBaseLogPosition,
                         std::int64_t logPosition, std::int64_t commitPosition,
                         std::int64_t leaderRecordingId, std::int64_t timestamp,
                         std::int32_t leaderMemberId, std::int32_t logSessionId,
                         std::int32_t appVersion, bool isStartup);

  bool appendPosition(std::shared_ptr<ExclusivePublication> publication,
                      std::int64_t leadershipTermId, std::int64_t logPosition,
                      std::int32_t followerMemberId, std::int16_t flags);

  void commitPosition(std::shared_ptr<ExclusivePublication> publication,
                      std::int64_t leadershipTermId, std::int64_t logPosition,
                      std::int32_t leaderMemberId);

  bool catchupPosition(std::shared_ptr<ExclusivePublication> publication,
                       std::int64_t leadershipTermId, std::int64_t logPosition,
                       std::int32_t followerMemberId,
                       const std::string &catchupEndpoint);

  bool stopCatchup(std::shared_ptr<ExclusivePublication> publication,
                   std::int64_t leadershipTermId,
                   std::int32_t followerMemberId);

  bool terminationPosition(std::shared_ptr<ExclusivePublication> publication,
                           std::int64_t leadershipTermId,
                           std::int64_t logPosition);

  bool terminationAck(std::shared_ptr<ExclusivePublication> publication,
                      std::int64_t leadershipTermId, std::int64_t logPosition,
                      std::int32_t memberId);

  bool backupQuery(std::shared_ptr<ExclusivePublication> publication,
                   std::int64_t correlationId, std::int32_t responseStreamId,
                   std::int32_t version, const std::string &responseChannel,
                   const std::vector<std::uint8_t> &encodedCredentials);

  bool backupResponse(ClusterSession &session,
                      std::int32_t commitPositionCounterId,
                      std::int32_t leaderMemberId, std::int32_t memberId,
                      const RecordingLog::Entry &lastEntry,
                      const RecordingLog::RecoveryPlan &recoveryPlan,
                      const std::string &clusterMembers);

  bool heartbeatRequest(std::shared_ptr<ExclusivePublication> publication,
                        std::int64_t correlationId,
                        std::int32_t responseStreamId,
                        const std::string &responseChannel,
                        const std::vector<std::uint8_t> &encodedCredentials);

  bool heartbeatResponse(ClusterSession &session);

  bool
  challengeResponse(std::shared_ptr<ExclusivePublication> publication,
                    std::int64_t nextCorrelationId,
                    std::int64_t clusterSessionId,
                    const std::vector<std::uint8_t> &encodedChallengeResponse);

  bool standbySnapshotTaken(std::shared_ptr<ExclusivePublication> publication,
                            std::int64_t correlationId, std::int32_t version,
                            std::int32_t responseStreamId,
                            const std::string &responseChannel,
                            const std::vector<std::uint8_t> &encodedCredentials,
                            const std::vector<RecordingLog::Entry> &snapshots,
                            const std::string &archiveEndpoint);

private:
  static constexpr std::int32_t SEND_ATTEMPTS = 3;

  static void checkResult(std::int64_t position,
                          ExclusivePublication &publication);
  static void checkResult(std::int64_t position, Publication &publication);

  static bool sendPublication(std::shared_ptr<ExclusivePublication> publication,
                              AtomicBuffer &buffer, std::int32_t length);

  static bool sendSession(ClusterSession &session, AtomicBuffer &buffer,
                          std::int32_t length);

  std::vector<std::uint8_t> m_bufferData; // Equivalent to ExpandableArrayBuffer
  AtomicBuffer m_buffer;
  BufferClaim m_bufferClaim;
  MessageHeader m_messageHeaderEncoder;
  CanvassPosition m_canvassPositionEncoder;
  RequestVote m_requestVoteEncoder;
  Vote m_voteEncoder;
  NewLeadershipTerm m_newLeadershipTermEncoder;
  AppendPosition m_appendPositionEncoder;
  CommitPosition m_commitPositionEncoder;
  CatchupPosition m_catchupPositionEncoder;
  StopCatchup m_stopCatchupEncoder;
  TerminationPosition m_terminationPositionEncoder;
  TerminationAck m_terminationAckEncoder;
  BackupQuery m_backupQueryEncoder;
  BackupResponse m_backupResponseEncoder;
  HeartbeatRequest m_heartbeatRequestEncoder;
  HeartbeatResponse m_heartbeatResponseEncoder;
  ChallengeResponse m_challengeResponseEncoder;
  StandbySnapshot m_standbySnapshotEncoder;

  // Protocol version for consensus module to consensus module communication
  // Major=1, Minor=0, Patch=0 -> (1 << 16) | (0 << 8) | 0 = 0x10000
  static constexpr std::int32_t PROTOCOL_SEMANTIC_VERSION =
      (1 << 16) | (0 << 8) | 0;
};

// Implementation
inline ConsensusPublisher::ConsensusPublisher()
    : m_bufferData(4096), m_buffer(m_bufferData.data(), m_bufferData.size()) {}

inline void ConsensusPublisher::checkResult(std::int64_t position,
                                            ExclusivePublication &publication) {
  if (aeron::PUBLICATION_CLOSED == position) {
    throw ClusterException("publication is closed", SOURCEINFO);
  }
}

inline void ConsensusPublisher::checkResult(std::int64_t position,
                                            Publication &publication) {
  if (aeron::PUBLICATION_CLOSED == position) {
    throw ClusterException("publication is closed", SOURCEINFO);
  }

  if (aeron::MAX_POSITION_EXCEEDED == position) {
    throw ClusterException("publication at max position: term-length=" +
                               std::to_string(publication.termBufferLength()),
                           SOURCEINFO);
  }
}

inline bool ConsensusPublisher::sendPublication(
    std::shared_ptr<ExclusivePublication> publication, AtomicBuffer &buffer,
    std::int32_t length) {
  int attempts = SEND_ATTEMPTS;
  do {
    const std::int64_t position = publication->offer(buffer, 0, length);
    if (position > 0) {
      return true;
    }

    checkResult(position, *publication);
  } while (--attempts > 0);

  return false;
}

inline bool ConsensusPublisher::sendSession(ClusterSession &session,
                                            AtomicBuffer &buffer,
                                            std::int32_t length) {
  int attempts = SEND_ATTEMPTS;
  std::shared_ptr<Publication> publication = session.responsePublication();
  do {
    util::DirectBuffer directBuffer(buffer.buffer(), buffer.capacity());
    const std::int64_t position = publication->offer(directBuffer, 0, length);
    if (position > 0) {
      return true;
    }

    checkResult(position, *publication);
  } while (--attempts > 0);

  return false;
}

inline void ConsensusPublisher::canvassPosition(
    std::shared_ptr<ExclusivePublication> publication,
    std::int64_t logLeadershipTermId, std::int64_t logPosition,
    std::int64_t leadershipTermId, std::int32_t followerMemberId) {
  if (!publication) {
    return;
  }

  const std::int32_t length = static_cast<std::int32_t>(
      MessageHeader::encodedLength() + CanvassPosition::SBE_BLOCK_LENGTH);

  int attempts = SEND_ATTEMPTS;
  do {
    const std::int64_t position = publication->tryClaim(length, m_bufferClaim);
    if (position > 0) {
      m_canvassPositionEncoder
          .wrapAndApplyHeader(
              reinterpret_cast<char *>(m_bufferClaim.buffer().buffer()),
              m_bufferClaim.offset(), m_bufferClaim.buffer().capacity())
          .logLeadershipTermId(logLeadershipTermId)
          .logPosition(logPosition)
          .leadershipTermId(leadershipTermId)
          .followerMemberId(followerMemberId)
          .protocolVersion(PROTOCOL_SEMANTIC_VERSION);

      m_bufferClaim.commit();
      return;
    }

    checkResult(position, *publication);
  } while (--attempts > 0);
}

inline bool ConsensusPublisher::requestVote(
    std::shared_ptr<ExclusivePublication> publication,
    std::int64_t logLeadershipTermId, std::int64_t logPosition,
    std::int64_t candidateTermId, std::int32_t candidateMemberId) {
  if (!publication) {
    return false;
  }

  const std::int32_t length = static_cast<std::int32_t>(
      MessageHeader::encodedLength() + RequestVote::SBE_BLOCK_LENGTH);

  int attempts = SEND_ATTEMPTS;
  do {
    const std::int64_t position = publication->tryClaim(length, m_bufferClaim);
    if (position > 0) {
      m_requestVoteEncoder
          .wrapAndApplyHeader(
              reinterpret_cast<char *>(m_bufferClaim.buffer().buffer()),
              m_bufferClaim.offset(), m_bufferClaim.buffer().capacity())
          .logLeadershipTermId(logLeadershipTermId)
          .logPosition(logPosition)
          .candidateTermId(candidateTermId)
          .candidateMemberId(candidateMemberId)
          .protocolVersion(PROTOCOL_SEMANTIC_VERSION);

      m_bufferClaim.commit();
      return true;
    }

    checkResult(position, *publication);
  } while (--attempts > 0);

  return false;
}

inline void ConsensusPublisher::placeVote(
    std::shared_ptr<ExclusivePublication> publication,
    std::int64_t candidateTermId, std::int64_t logLeadershipTermId,
    std::int64_t logPosition, std::int32_t candidateMemberId,
    std::int32_t followerMemberId, bool vote) {
  if (!publication) {
    return;
  }

  const std::int32_t length = static_cast<std::int32_t>(
      MessageHeader::encodedLength() + Vote::SBE_BLOCK_LENGTH);

  int attempts = SEND_ATTEMPTS;
  do {
    const std::int64_t position = publication->tryClaim(length, m_bufferClaim);
    if (position > 0) {
      m_voteEncoder
          .wrapAndApplyHeader(
              reinterpret_cast<char *>(m_bufferClaim.buffer().buffer()),
              m_bufferClaim.offset(), m_bufferClaim.buffer().capacity())
          .candidateTermId(candidateTermId)
          .logLeadershipTermId(logLeadershipTermId)
          .logPosition(logPosition)
          .candidateMemberId(candidateMemberId)
          .followerMemberId(followerMemberId)
          .vote(vote ? (BooleanType::TRUE) : (BooleanType::FALSE));

      m_bufferClaim.commit();
      return;
    }

    checkResult(position, *publication);
  } while (--attempts > 0);
}

inline void ConsensusPublisher::commitPosition(
    std::shared_ptr<ExclusivePublication> publication,
    std::int64_t leadershipTermId, std::int64_t logPosition,
    std::int32_t leaderMemberId) {
  if (!publication) {
    return;
  }

  const std::int32_t length = static_cast<std::int32_t>(
      MessageHeader::encodedLength() + CommitPosition::SBE_BLOCK_LENGTH);

  int attempts = SEND_ATTEMPTS;
  do {
    const std::int64_t position = publication->tryClaim(length, m_bufferClaim);
    if (position > 0) {
      m_commitPositionEncoder
          .wrapAndApplyHeader(
              reinterpret_cast<char *>(m_bufferClaim.buffer().buffer()),
              m_bufferClaim.offset(), m_bufferClaim.buffer().capacity())
          .leadershipTermId(leadershipTermId)
          .logPosition(logPosition)
          .leaderMemberId(leaderMemberId);

      m_bufferClaim.commit();
      return;
    }

    checkResult(position, *publication);
  } while (--attempts > 0);
}

inline void ConsensusPublisher::newLeadershipTerm(
    std::shared_ptr<ExclusivePublication> publication,
    std::int64_t logLeadershipTermId, std::int64_t nextLeadershipTermId,
    std::int64_t nextTermBaseLogPosition, std::int64_t nextLogPosition,
    std::int64_t leadershipTermId, std::int64_t termBaseLogPosition,
    std::int64_t logPosition, std::int64_t commitPosition,
    std::int64_t leaderRecordingId, std::int64_t timestamp,
    std::int32_t leaderMemberId, std::int32_t logSessionId,
    std::int32_t appVersion, bool isStartup) {
  if (!publication) {
    return;
  }

  const std::int32_t length = static_cast<std::int32_t>(
      MessageHeader::encodedLength() + NewLeadershipTerm::SBE_BLOCK_LENGTH);

  int attempts = SEND_ATTEMPTS;
  do {
    const std::int64_t position = publication->tryClaim(length, m_bufferClaim);
    if (position > 0) {
      m_newLeadershipTermEncoder
          .wrapAndApplyHeader(
              reinterpret_cast<char *>(m_bufferClaim.buffer().buffer()),
              m_bufferClaim.offset(), m_bufferClaim.buffer().capacity())
          .logLeadershipTermId(logLeadershipTermId)
          .nextLeadershipTermId(nextLeadershipTermId)
          .nextTermBaseLogPosition(nextTermBaseLogPosition)
          .nextLogPosition(nextLogPosition)
          .leadershipTermId(leadershipTermId)
          .termBaseLogPosition(termBaseLogPosition)
          .logPosition(logPosition)
          .leaderRecordingId(leaderRecordingId)
          .timestamp(timestamp)
          .leaderMemberId(leaderMemberId)
          .logSessionId(logSessionId)
          .appVersion(appVersion)
          .isStartup(isStartup ? (BooleanType::TRUE) : (BooleanType::FALSE))
          .commitPosition(commitPosition);

      m_bufferClaim.commit();
      return;
    }

    checkResult(position, *publication);
  } while (--attempts > 0);
}

inline bool ConsensusPublisher::appendPosition(
    std::shared_ptr<ExclusivePublication> publication,
    std::int64_t leadershipTermId, std::int64_t logPosition,
    std::int32_t followerMemberId, std::int16_t flags) {
  if (!publication) {
    return false;
  }

  const std::int32_t length = static_cast<std::int32_t>(
      MessageHeader::encodedLength() + AppendPosition::SBE_BLOCK_LENGTH);

  int attempts = SEND_ATTEMPTS;
  do {
    const std::int64_t position = publication->tryClaim(length, m_bufferClaim);
    if (position > 0) {
      m_appendPositionEncoder
          .wrapAndApplyHeader(
              reinterpret_cast<char *>(m_bufferClaim.buffer().buffer()),
              m_bufferClaim.offset(), m_bufferClaim.buffer().capacity())
          .leadershipTermId(leadershipTermId)
          .logPosition(logPosition)
          .followerMemberId(followerMemberId)
          .flags(flags);

      m_bufferClaim.commit();
      return true;
    }

    checkResult(position, *publication);
  } while (--attempts > 0);

  return false;
}

inline bool ConsensusPublisher::catchupPosition(
    std::shared_ptr<ExclusivePublication> publication,
    std::int64_t leadershipTermId, std::int64_t logPosition,
    std::int32_t followerMemberId, const std::string &catchupEndpoint) {
  if (!publication) {
    return false;
  }

  const std::int32_t length = static_cast<std::int32_t>(
      MessageHeader::encodedLength() + CatchupPosition::SBE_BLOCK_LENGTH +
      CatchupPosition::catchupEndpointHeaderLength() +
      catchupEndpoint.length());

  int attempts = SEND_ATTEMPTS;
  do {
    const std::int64_t position = publication->tryClaim(length, m_bufferClaim);
    if (position > 0) {
      m_catchupPositionEncoder
          .wrapAndApplyHeader(
              reinterpret_cast<char *>(m_bufferClaim.buffer().buffer()),
              m_bufferClaim.offset(), m_bufferClaim.buffer().capacity())
          .leadershipTermId(leadershipTermId)
          .logPosition(logPosition)
          .followerMemberId(followerMemberId)
          .putCatchupEndpoint(
              catchupEndpoint.data(),
              static_cast<std::uint32_t>(catchupEndpoint.length()));

      m_bufferClaim.commit();
      return true;
    }

    checkResult(position, *publication);
  } while (--attempts > 0);

  return false;
}

inline bool ConsensusPublisher::stopCatchup(
    std::shared_ptr<ExclusivePublication> publication,
    std::int64_t leadershipTermId, std::int32_t followerMemberId) {
  if (!publication) {
    return false;
  }

  const std::int32_t length = static_cast<std::int32_t>(
      MessageHeader::encodedLength() + StopCatchup::SBE_BLOCK_LENGTH);

  int attempts = SEND_ATTEMPTS;
  do {
    const std::int64_t position = publication->tryClaim(length, m_bufferClaim);
    if (position > 0) {
      m_stopCatchupEncoder
          .wrapAndApplyHeader(
              reinterpret_cast<char *>(m_bufferClaim.buffer().buffer()),
              m_bufferClaim.offset(), m_bufferClaim.buffer().capacity())
          .leadershipTermId(leadershipTermId)
          .followerMemberId(followerMemberId);

      m_bufferClaim.commit();
      return true;
    }

    checkResult(position, *publication);
  } while (--attempts > 0);

  return false;
}

inline bool ConsensusPublisher::terminationPosition(
    std::shared_ptr<ExclusivePublication> publication,
    std::int64_t leadershipTermId, std::int64_t logPosition) {
  if (!publication) {
    return false;
  }

  const std::int32_t length = static_cast<std::int32_t>(
      MessageHeader::encodedLength() + TerminationPosition::SBE_BLOCK_LENGTH);

  int attempts = SEND_ATTEMPTS;
  do {
    const std::int64_t position = publication->tryClaim(length, m_bufferClaim);
    if (position > 0) {
      m_terminationPositionEncoder
          .wrapAndApplyHeader(
              reinterpret_cast<char *>(m_bufferClaim.buffer().buffer()),
              m_bufferClaim.offset(), m_bufferClaim.buffer().capacity())
          .leadershipTermId(leadershipTermId)
          .logPosition(logPosition);

      m_bufferClaim.commit();
      return true;
    }

    checkResult(position, *publication);
  } while (--attempts > 0);

  return false;
}

inline bool ConsensusPublisher::terminationAck(
    std::shared_ptr<ExclusivePublication> publication,
    std::int64_t leadershipTermId, std::int64_t logPosition,
    std::int32_t memberId) {
  if (!publication) {
    return false;
  }

  const std::int32_t length = static_cast<std::int32_t>(
      MessageHeader::encodedLength() + TerminationAck::SBE_BLOCK_LENGTH);

  int attempts = SEND_ATTEMPTS;
  do {
    const std::int64_t position = publication->tryClaim(length, m_bufferClaim);
    if (position > 0) {
      m_terminationAckEncoder
          .wrapAndApplyHeader(
              reinterpret_cast<char *>(m_bufferClaim.buffer().buffer()),
              m_bufferClaim.offset(), m_bufferClaim.buffer().capacity())
          .leadershipTermId(leadershipTermId)
          .logPosition(logPosition)
          .memberId(memberId);

      m_bufferClaim.commit();
      return true;
    }

    checkResult(position, *publication);
  } while (--attempts > 0);

  return false;
}

inline bool ConsensusPublisher::backupQuery(
    std::shared_ptr<ExclusivePublication> publication,
    std::int64_t correlationId, std::int32_t responseStreamId,
    std::int32_t version, const std::string &responseChannel,
    const std::vector<std::uint8_t> &encodedCredentials) {
  if (!publication) {
    return false;
  }

  m_backupQueryEncoder
      .wrapAndApplyHeader(reinterpret_cast<char *>(m_buffer.buffer()), 0,
                          m_buffer.capacity())
      .correlationId(correlationId)
      .responseStreamId(responseStreamId)
      .version(version)
      .putResponseChannel(responseChannel.data(),
                          static_cast<std::uint32_t>(responseChannel.length()))
      .putEncodedCredentials(
          reinterpret_cast<const char *>(encodedCredentials.data()),
          static_cast<std::uint32_t>(encodedCredentials.size()));

  const std::int32_t length = static_cast<std::int32_t>(
      MessageHeader::encodedLength() + m_backupQueryEncoder.encodedLength());
  return sendPublication(publication, m_buffer, length);
}

inline bool ConsensusPublisher::backupResponse(
    ClusterSession &session, std::int32_t commitPositionCounterId,
    std::int32_t leaderMemberId, std::int32_t memberId,
    const RecordingLog::Entry &lastEntry,
    const RecordingLog::RecoveryPlan &recoveryPlan,
    const std::string &clusterMembers) {
  m_backupResponseEncoder
      .wrapAndApplyHeader(reinterpret_cast<char *>(m_buffer.buffer()), 0,
                          m_buffer.capacity())
      .correlationId(session.correlationId())
      .logRecordingId(recoveryPlan.log->recordingId)
      .logLeadershipTermId(recoveryPlan.log->leadershipTermId)
      .logTermBaseLogPosition(recoveryPlan.log->termBaseLogPosition)
      .lastLeadershipTermId(lastEntry.leadershipTermId)
      .lastTermBaseLogPosition(lastEntry.termBaseLogPosition)
      .commitPositionCounterId(commitPositionCounterId)
      .leaderMemberId(leaderMemberId)
      .memberId(memberId);

  auto snapshotsEncoder = m_backupResponseEncoder.snapshotsCount(
      static_cast<std::uint32_t>(recoveryPlan.snapshots.size()));
  for (const auto &snapshot : recoveryPlan.snapshots) {
    snapshotsEncoder.next()
        .recordingId(snapshot.recordingId)
        .leadershipTermId(snapshot.leadershipTermId)
        .termBaseLogPosition(snapshot.termBaseLogPosition)
        .logPosition(snapshot.logPosition)
        .timestamp(snapshot.timestamp)
        .serviceId(snapshot.serviceId);
  }

  m_backupResponseEncoder.putClusterMembers(
      clusterMembers.data(),
      static_cast<std::uint32_t>(clusterMembers.length()));

  const std::int32_t length = static_cast<std::int32_t>(
      MessageHeader::encodedLength() + m_backupResponseEncoder.encodedLength());
  return sendSession(session, m_buffer, length);
}

inline bool ConsensusPublisher::heartbeatRequest(
    std::shared_ptr<ExclusivePublication> publication,
    std::int64_t correlationId, std::int32_t responseStreamId,
    const std::string &responseChannel,
    const std::vector<std::uint8_t> &encodedCredentials) {
  if (!publication) {
    return false;
  }

  m_heartbeatRequestEncoder
      .wrapAndApplyHeader(reinterpret_cast<char *>(m_buffer.buffer()), 0,
                          m_buffer.capacity())
      .correlationId(correlationId)
      .responseStreamId(responseStreamId)
      .putResponseChannel(responseChannel.data(),
                          static_cast<std::uint32_t>(responseChannel.length()))
      .putEncodedCredentials(
          reinterpret_cast<const char *>(encodedCredentials.data()),
          static_cast<std::uint32_t>(encodedCredentials.size()));

  const std::int32_t length =
      static_cast<std::int32_t>(MessageHeader::encodedLength() +
                                m_heartbeatRequestEncoder.encodedLength());
  return sendPublication(publication, m_buffer, length);
}

inline bool ConsensusPublisher::heartbeatResponse(ClusterSession &session) {
  m_heartbeatResponseEncoder
      .wrapAndApplyHeader(reinterpret_cast<char *>(m_buffer.buffer()), 0,
                          m_buffer.capacity())
      .correlationId(session.correlationId());

  const std::int32_t length =
      static_cast<std::int32_t>(MessageHeader::encodedLength() +
                                m_heartbeatResponseEncoder.encodedLength());
  return sendSession(session, m_buffer, length);
}

inline bool ConsensusPublisher::challengeResponse(
    std::shared_ptr<ExclusivePublication> publication,
    std::int64_t nextCorrelationId, std::int64_t clusterSessionId,
    const std::vector<std::uint8_t> &encodedChallengeResponse) {
  m_challengeResponseEncoder
      .wrapAndApplyHeader(reinterpret_cast<char *>(m_buffer.buffer()), 0,
                          m_buffer.capacity())
      .correlationId(nextCorrelationId)
      .clusterSessionId(clusterSessionId)
      .putEncodedCredentials(
          reinterpret_cast<const char *>(encodedChallengeResponse.data()),
          static_cast<std::uint32_t>(encodedChallengeResponse.size()));

  const std::int32_t length =
      static_cast<std::int32_t>(MessageHeader::encodedLength() +
                                m_challengeResponseEncoder.encodedLength());

  return sendPublication(publication, m_buffer, length);
}

inline bool ConsensusPublisher::standbySnapshotTaken(
    std::shared_ptr<ExclusivePublication> publication,
    std::int64_t correlationId, std::int32_t version,
    std::int32_t responseStreamId, const std::string &responseChannel,
    const std::vector<std::uint8_t> &encodedCredentials,
    const std::vector<RecordingLog::Entry> &snapshots,
    const std::string &archiveEndpoint) {
  const std::int32_t snapshotsLength =
      static_cast<std::int32_t>(snapshots.size());
  m_standbySnapshotEncoder.wrapAndApplyHeader(
      reinterpret_cast<char *>(m_buffer.buffer()), 0, m_buffer.capacity());

  m_standbySnapshotEncoder.correlationId(correlationId)
      .version(version)
      .responseStreamId(responseStreamId);

  auto snapshotsEncoder = m_standbySnapshotEncoder.snapshotsCount(
      static_cast<std::uint32_t>(snapshotsLength));

  for (const auto &entry : snapshots) {
    snapshotsEncoder.next()
        .recordingId(entry.recordingId)
        .leadershipTermId(entry.leadershipTermId)
        .termBaseLogPosition(entry.termBaseLogPosition)
        .logPosition(entry.logPosition)
        .timestamp(entry.timestamp)
        .serviceId(entry.serviceId)
        .putArchiveEndpoint(
            archiveEndpoint.data(),
            static_cast<std::uint32_t>(archiveEndpoint.length()));
  }

  m_standbySnapshotEncoder
      .putResponseChannel(responseChannel.data(),
                          static_cast<std::uint32_t>(responseChannel.length()))
      .putEncodedCredentials(
          reinterpret_cast<const char *>(encodedCredentials.data()),
          static_cast<std::uint32_t>(encodedCredentials.size()));

  const std::int32_t encodedLength =
      static_cast<std::int32_t>(MessageHeader::encodedLength() +
                                m_standbySnapshotEncoder.encodedLength());

  return sendPublication(publication, m_buffer, encodedLength);
}

} // namespace cluster
} // namespace aeron
