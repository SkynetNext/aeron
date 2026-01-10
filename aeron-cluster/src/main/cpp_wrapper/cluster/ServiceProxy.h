#pragma once

#ifndef NOMINMAX
#define NOMINMAX
#endif

#include "ClusterMember.h"
#include "Publication.h"
#include "client/ClusterEvent.h"
#include "client/ClusterExceptions.h"
#include "concurrent/AtomicBuffer.h"
#include "concurrent/logbuffer/BufferClaim.h"
// Undefine Windows macros that conflict with BooleanType
#ifdef TRUE
#undef TRUE
#endif
#ifdef FALSE
#undef FALSE
#endif
#include "generated/aeron_cluster_codecs/BooleanType.h"
#include "generated/aeron_cluster_codecs/ClusterMembersExtendedResponse.h"
#include "generated/aeron_cluster_codecs/ClusterMembersResponse.h"
#include "generated/aeron_cluster_codecs/JoinLog.h"
#include "generated/aeron_cluster_codecs/MessageHeader.h"
#include "generated/aeron_cluster_codecs/RequestServiceAck.h"
#include "generated/aeron_cluster_codecs/ServiceTerminationPosition.h"
#include "service/Cluster.h"
#include "util/CloseHelper.h"
#include "util/Exceptions.h"
#include <memory>
#include <thread>
#include <vector>


namespace aeron {
namespace cluster {
using namespace aeron::concurrent;
using namespace aeron::concurrent::logbuffer;
using namespace aeron::cluster::codecs;
using namespace aeron::util;

class ServiceProxy {
public:
  explicit ServiceProxy(std::shared_ptr<Publication> publication);
  ~ServiceProxy() = default;

  void close();

  void joinLog(std::int64_t logPosition, std::int64_t maxLogPosition,
               std::int32_t memberId, std::int32_t logSessionId,
               std::int32_t logStreamId, bool isStartup, service::Role role,
               const std::string &channel);

  void clusterMembersResponse(std::int64_t correlationId,
                              std::int32_t leaderMemberId,
                              const std::string &activeMembers);

  void clusterMembersExtendedResponse(
      std::int64_t correlationId, std::int64_t currentTimeNs,
      std::int32_t leaderMemberId, std::int32_t memberId,
      const std::vector<ClusterMember> &activeMembers);

  void terminationPosition(std::int64_t logPosition,
                           const exception_handler_t &errorHandler);

  void requestServiceAck(std::int64_t logPosition);

private:
  static constexpr std::int32_t SEND_ATTEMPTS = 5;

  static void checkResult(std::int64_t position, Publication &publication);

  BufferClaim m_bufferClaim;
  MessageHeader m_messageHeaderEncoder;
  JoinLog m_joinLogEncoder;
  ClusterMembersResponse m_clusterMembersResponseEncoder;
  ServiceTerminationPosition m_serviceTerminationPositionEncoder;
  ClusterMembersExtendedResponse m_clusterMembersExtendedResponseEncoder;
  RequestServiceAck m_requestServiceAckEncoder;
  std::vector<std::uint8_t> m_expandableArrayBufferData;
  AtomicBuffer m_expandableArrayBuffer;
  std::shared_ptr<Publication> m_publication;
};

// Implementation
inline ServiceProxy::ServiceProxy(std::shared_ptr<Publication> publication)
    : m_expandableArrayBufferData(4096, 0), m_publication(publication) {
  m_expandableArrayBuffer.wrap(m_expandableArrayBufferData.data(),
                               m_expandableArrayBufferData.size());
}

inline void ServiceProxy::close() {
  if (m_publication) {
    CloseHelper::close(m_publication);
  }
}

inline void
ServiceProxy::joinLog(std::int64_t logPosition, std::int64_t maxLogPosition,
                      std::int32_t memberId, std::int32_t logSessionId,
                      std::int32_t logStreamId, bool isStartup,
                      service::Role role, const std::string &channel) {
  const std::int32_t length = MessageHeader::encodedLength() +
                              JoinLog::SBE_BLOCK_LENGTH +
                              JoinLog::logChannelHeaderLength() +
                              static_cast<std::int32_t>(channel.length());

  std::int64_t position;
  std::int32_t attempts = SEND_ATTEMPTS;

  do {
    position = m_publication->tryClaim(length, m_bufferClaim);
    if (position > 0) {
      m_joinLogEncoder
          .wrapAndApplyHeader(
              reinterpret_cast<char *>(m_bufferClaim.buffer().buffer()),
              m_bufferClaim.offset(), m_bufferClaim.buffer().capacity())
          .logPosition(logPosition)
          .maxLogPosition(maxLogPosition)
          .memberId(memberId)
          .logSessionId(logSessionId)
          .logStreamId(logStreamId)
          .isStartup(isStartup ? BooleanType::TRUE : BooleanType::FALSE)
          .role(static_cast<std::int32_t>(role))
          .putLogChannel(channel.data(),
                         static_cast<std::uint32_t>(channel.length()));

      m_bufferClaim.commit();
      return;
    }

    checkResult(position, *m_publication);
    if (BACK_PRESSURED == position) {
      std::this_thread::yield();
    }
  } while (--attempts > 0);

  throw ClusterException("failed to send join log request: " +
                             std::to_string(position),
                         SOURCEINFO);
}

inline void
ServiceProxy::clusterMembersResponse(std::int64_t correlationId,
                                     std::int32_t leaderMemberId,
                                     const std::string &activeMembers) {
  const std::string passiveFollowers = "";
  const std::int32_t length =
      MessageHeader::encodedLength() +
      ClusterMembersResponse::sbeBlockLength() +
      ClusterMembersResponse::activeMembersHeaderLength() +
      static_cast<std::int32_t>(activeMembers.length()) +
      ClusterMembersResponse::passiveFollowersHeaderLength() +
      static_cast<std::int32_t>(passiveFollowers.length());

  std::int64_t result;
  std::int32_t attempts = SEND_ATTEMPTS;

  do {
    result = m_publication->tryClaim(length, m_bufferClaim);
    if (result > 0) {
      m_clusterMembersResponseEncoder
          .wrapAndApplyHeader(
              reinterpret_cast<char *>(m_bufferClaim.buffer().buffer()),
              m_bufferClaim.offset(), m_bufferClaim.buffer().capacity())
          .correlationId(correlationId)
          .leaderMemberId(leaderMemberId)
          .putActiveMembers(activeMembers.data(),
                            static_cast<std::uint32_t>(activeMembers.length()))
          .putPassiveFollowers(
              passiveFollowers.data(),
              static_cast<std::uint32_t>(passiveFollowers.length()));

      m_bufferClaim.commit();
      return;
    }

    if (BACK_PRESSURED == result) {
      std::this_thread::yield();
    }
  } while (--attempts > 0);

  throw ClusterException("failed to send cluster members response: result=" +
                             std::to_string(result),
                         SOURCEINFO);
}

inline void ServiceProxy::clusterMembersExtendedResponse(
    std::int64_t correlationId, std::int64_t currentTimeNs,
    std::int32_t leaderMemberId, std::int32_t memberId,
    const std::vector<ClusterMember> &activeMembers) {
  // Ensure buffer is large enough - expand if needed
  std::int32_t estimatedLength =
      MessageHeader::encodedLength() +
      ClusterMembersExtendedResponse::sbeBlockLength() +
      (activeMembers.size() * 200); // Rough estimate per member

  if (static_cast<std::int32_t>(m_expandableArrayBufferData.size()) <
      estimatedLength) {
    m_expandableArrayBufferData.resize(estimatedLength * 2);
    m_expandableArrayBuffer.wrap(m_expandableArrayBufferData.data(),
                                 m_expandableArrayBufferData.size());
  }

  m_clusterMembersExtendedResponseEncoder
      .wrapAndApplyHeader(
          reinterpret_cast<char *>(m_expandableArrayBuffer.buffer()), 0,
          m_expandableArrayBuffer.capacity())
      .correlationId(correlationId)
      .currentTimeNs(currentTimeNs)
      .leaderMemberId(leaderMemberId)
      .memberId(memberId);

  auto activeMembersEncoder =
      m_clusterMembersExtendedResponseEncoder.activeMembersCount(
          static_cast<std::uint32_t>(activeMembers.size()));

  for (const auto &member : activeMembers) {
    activeMembersEncoder.next()
        .leadershipTermId(member.leadershipTermId())
        .logPosition(member.logPosition())
        .timeOfLastAppendNs(member.timeOfLastAppendPositionNs())
        .memberId(member.id())
        .putIngressEndpoint(
            member.ingressEndpoint().data(),
            static_cast<std::uint32_t>(member.ingressEndpoint().length()))
        .putConsensusEndpoint(
            member.consensusEndpoint().data(),
            static_cast<std::uint32_t>(member.consensusEndpoint().length()))
        .putLogEndpoint(
            member.logEndpoint().data(),
            static_cast<std::uint32_t>(member.logEndpoint().length()))
        .putCatchupEndpoint(
            member.catchupEndpoint().data(),
            static_cast<std::uint32_t>(member.catchupEndpoint().length()))
        .putArchiveEndpoint(
            member.archiveEndpoint().data(),
            static_cast<std::uint32_t>(member.archiveEndpoint().length()));
  }

  m_clusterMembersExtendedResponseEncoder.passiveMembersCount(0);

  const std::int32_t length =
      MessageHeader::encodedLength() +
      m_clusterMembersExtendedResponseEncoder.encodedLength();

  std::int64_t result;
  std::int32_t attempts = SEND_ATTEMPTS;

  do {
    result = m_publication->offer(m_expandableArrayBuffer, 0, length, nullptr);
    if (result > 0) {
      return;
    }

    if (BACK_PRESSURED == result) {
      std::this_thread::yield();
    }
  } while (--attempts > 0);

  throw ClusterException(
      "failed to send cluster members extended response: result=" +
          std::to_string(result),
      SOURCEINFO);
}

inline void
ServiceProxy::terminationPosition(std::int64_t logPosition,
                                  const exception_handler_t &errorHandler) {
  if (m_publication && !m_publication->isClosed()) {
    const std::int32_t length = MessageHeader::encodedLength() +
                                ServiceTerminationPosition::sbeBlockLength();

    std::int64_t result;
    std::int32_t attempts = SEND_ATTEMPTS;

    do {
      result = m_publication->tryClaim(length, m_bufferClaim);
      if (result > 0) {
        m_serviceTerminationPositionEncoder
            .wrapAndApplyHeader(
                reinterpret_cast<char *>(m_bufferClaim.buffer().buffer()),
                m_bufferClaim.offset(), m_bufferClaim.buffer().capacity())
            .logPosition(logPosition);

        m_bufferClaim.commit();
        return;
      }

      if (BACK_PRESSURED == result) {
        std::this_thread::yield();
      }
    } while (--attempts > 0);

    if (errorHandler) {
      errorHandler(
          ClusterEvent("failed to send service termination position: result=" +
                       std::to_string(result)));
    }
  }
}

inline void ServiceProxy::requestServiceAck(std::int64_t logPosition) {
  const std::int32_t length =
      MessageHeader::encodedLength() + RequestServiceAck::sbeBlockLength();

  std::int64_t result;
  std::int32_t attempts = SEND_ATTEMPTS;

  do {
    result = m_publication->tryClaim(length, m_bufferClaim);
    if (result > 0) {
      m_requestServiceAckEncoder
          .wrapAndApplyHeader(
              reinterpret_cast<char *>(m_bufferClaim.buffer().buffer()),
              m_bufferClaim.offset(), m_bufferClaim.buffer().capacity())
          .logPosition(logPosition);

      m_bufferClaim.commit();
      return;
    }

    if (BACK_PRESSURED == result) {
      std::this_thread::yield();
    }
  } while (--attempts > 0);

  throw ClusterException("failed to send request for service ack: result=" +
                             std::to_string(result),
                         SOURCEINFO);
}

inline void ServiceProxy::checkResult(std::int64_t position,
                                      Publication &publication) {
  if (NOT_CONNECTED == position) {
    throw ClusterException("publication is not connected", SOURCEINFO);
  }

  if (PUBLICATION_CLOSED == position) {
    throw ClusterException("publication is closed", SOURCEINFO);
  }

  if (MAX_POSITION_EXCEEDED == position) {
    throw ClusterException("publication at max position: term-length=" +
                               std::to_string(publication.termBufferLength()),
                           SOURCEINFO);
  }
}

} // namespace cluster
} // namespace aeron
