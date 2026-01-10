#include "PendingServiceMessageTracker.h"
#include "LogPublisher.h"
#include "client/AeronCluster.h"
#include "client/ClusterExceptions.h"
#include "generated/aeron_cluster_codecs/MessageHeader.h"
#include "generated/aeron_cluster_codecs/SessionMessageHeader.h"
#include <limits>

namespace aeron {
namespace cluster {
using namespace aeron::concurrent;
using namespace aeron::cluster::codecs;
using namespace aeron::cluster::client;
using namespace aeron::util;

// Implementation
PendingServiceMessageTracker::PendingServiceMessageTracker(
    std::int32_t serviceId, std::shared_ptr<Counter> commitPosition,
    LogPublisher &logPublisher, service::ClusterClock &clusterClock)
    : m_serviceId(serviceId), m_commitPosition(commitPosition),
      m_logPublisher(logPublisher), m_clusterClock(clusterClock),
      m_messageAppender([this](AtomicBuffer &buffer, std::int32_t offset,
                               std::int32_t length, std::int32_t headOffset) {
        return this->messageAppender(buffer, offset, length, headOffset);
      }),
      m_leaderMessageSweeper([this](AtomicBuffer &buffer, std::int32_t offset,
                                    std::int32_t length,
                                    std::int32_t headOffset) {
        return this->leaderMessageSweeper(buffer, offset, length, headOffset);
      }),
      m_followerMessageSweeper([this](AtomicBuffer &buffer, std::int32_t offset,
                                      std::int32_t length,
                                      std::int32_t headOffset) {
        return this->followerMessageSweeper(buffer, offset, length, headOffset);
      }) {
  m_logServiceSessionId =
      serviceSessionId(serviceId, std::numeric_limits<std::int64_t>::min());
  m_nextServiceSessionId = m_logServiceSessionId + 1;
}

void PendingServiceMessageTracker::leadershipTermId(
    std::int64_t leadershipTermId) {
  m_leadershipTermId = leadershipTermId;
}

std::int32_t PendingServiceMessageTracker::serviceId() const {
  return m_serviceId;
}

std::int64_t PendingServiceMessageTracker::nextServiceSessionId() const {
  return m_nextServiceSessionId;
}

std::int64_t PendingServiceMessageTracker::logServiceSessionId() const {
  return m_logServiceSessionId;
}

void PendingServiceMessageTracker::enqueueMessage(AtomicBuffer &buffer,
                                                  std::int32_t offset,
                                                  std::int32_t length) {
  const std::int64_t clusterSessionId = m_nextServiceSessionId++;
  if (clusterSessionId > m_logServiceSessionId) {
    const std::int32_t headerOffset =
        offset - SessionMessageHeader::sbeBlockLength();
    const std::int32_t clusterSessionIdOffset =
        headerOffset + SessionMessageHeader::clusterSessionIdEncodingOffset();
    const std::int32_t timestampOffset =
        headerOffset + SessionMessageHeader::timestampEncodingOffset();

    buffer.putInt64(clusterSessionIdOffset, clusterSessionId);
    buffer.putInt64(timestampOffset, std::numeric_limits<std::int64_t>::max());

    if (!m_pendingMessages.append(
            buffer, offset - client::AeronCluster::SESSION_HEADER_LENGTH,
            length + client::AeronCluster::SESSION_HEADER_LENGTH)) {
      throw client::ClusterException(
          "pending service message buffer at capacity=" +
              std::to_string(m_pendingMessages.size()) +
              " for serviceId=" + std::to_string(m_serviceId),
          SOURCEINFO);
    }
  }
}

void PendingServiceMessageTracker::sweepFollowerMessages(
    std::int64_t clusterSessionId) {
  m_logServiceSessionId = clusterSessionId;
  m_pendingMessages.consume(m_followerMessageSweeper,
                            std::numeric_limits<std::int32_t>::max());
}

void PendingServiceMessageTracker::sweepLeaderMessages() {
  if (m_uncommittedMessages > 0) {
    m_pendingMessageHeadOffset -= m_pendingMessages.consume(
        m_leaderMessageSweeper, std::numeric_limits<std::int32_t>::max());
    m_pendingMessageHeadOffset = std::max(m_pendingMessageHeadOffset, 0);
  }
}

void PendingServiceMessageTracker::restoreUncommittedMessages() {
  if (m_uncommittedMessages > 0) {
    m_pendingMessages.consume(m_leaderMessageSweeper,
                              std::numeric_limits<std::int32_t>::max());
    SimpleExpandableRingBuffer::MessageConsumer resetConsumer =
        [](AtomicBuffer &buffer, std::int32_t offset, std::int32_t length,
           std::int32_t headOffset) {
          return messageReset(buffer, offset, length, headOffset);
        };
    m_pendingMessages.forEach(resetConsumer,
                              std::numeric_limits<std::int32_t>::max());
    m_uncommittedMessages = 0;
    m_pendingMessageHeadOffset = 0;
  }
}

void PendingServiceMessageTracker::appendMessage(const AtomicBuffer &buffer,
                                                 std::int32_t offset,
                                                 std::int32_t length) {
  m_pendingMessages.append(buffer, offset, length);
}

void PendingServiceMessageTracker::loadState(
    std::int64_t nextServiceSessionId, std::int64_t logServiceSessionId,
    std::int32_t pendingMessageCapacity) {
  m_nextServiceSessionId = nextServiceSessionId;
  m_logServiceSessionId = logServiceSessionId;
  m_pendingMessages.reset(pendingMessageCapacity);
}

std::int32_t PendingServiceMessageTracker::poll() {
  return m_pendingMessages.forEach(m_pendingMessageHeadOffset,
                                   m_messageAppender, SERVICE_MESSAGE_LIMIT);
}

std::int32_t PendingServiceMessageTracker::size() const {
  return m_pendingMessages.size();
}

void PendingServiceMessageTracker::verify() {
  std::int32_t messageCount = 0;
  SimpleExpandableRingBuffer::MessageConsumer messageConsumer =
      [this, &messageCount](AtomicBuffer &buffer, std::int32_t offset,
                            std::int32_t length, std::int32_t headOffset) {
        messageCount++;

        const std::int32_t headerOffset =
            offset + MessageHeader::encodedLength();
        const std::int32_t clusterSessionIdOffset =
            headerOffset +
            SessionMessageHeader::clusterSessionIdEncodingOffset();

        const std::int64_t clusterSessionId =
            buffer.getInt64(clusterSessionIdOffset);

        if (clusterSessionId != (m_logServiceSessionId + messageCount)) {
          throw client::ClusterException(
              std::string("snapshot has incorrect pending message:") +
                  " serviceId=" + std::to_string(m_serviceId) +
                  " nextServiceSessionId=" +
                  std::to_string(m_nextServiceSessionId) +
                  " logServiceSessionId=" +
                  std::to_string(m_logServiceSessionId) +
                  " clusterSessionId=" + std::to_string(clusterSessionId) +
                  " pendingMessageIndex=" + std::to_string(messageCount),
              SOURCEINFO);
        }

        return true;
      };

  m_pendingMessages.forEach(messageConsumer,
                            std::numeric_limits<std::int32_t>::max());

  if (m_nextServiceSessionId != (m_logServiceSessionId + messageCount + 1)) {
    throw client::ClusterException(
        std::string("snapshot has incorrect pending message state:") +
            " serviceId=" + std::to_string(m_serviceId) +
            " nextServiceSessionId=" + std::to_string(m_nextServiceSessionId) +
            " logServiceSessionId=" + std::to_string(m_logServiceSessionId) +
            " pendingMessageCount=" + std::to_string(messageCount),
        SOURCEINFO);
  }
}

void PendingServiceMessageTracker::reset() {
  SimpleExpandableRingBuffer::MessageConsumer resetConsumer =
      [](AtomicBuffer &buffer, std::int32_t offset, std::int32_t length,
         std::int32_t headOffset) {
        return messageReset(buffer, offset, length, headOffset);
      };
  m_pendingMessages.forEach(resetConsumer,
                            std::numeric_limits<std::int32_t>::max());
}

std::int32_t PendingServiceMessageTracker::serviceIdFromLogMessage(
    std::int64_t clusterSessionId) {
  return static_cast<std::int32_t>((clusterSessionId >> 56) & 0x7F);
}

std::int32_t PendingServiceMessageTracker::serviceIdFromServiceMessage(
    std::int64_t clusterSessionId) {
  return static_cast<std::int32_t>(clusterSessionId);
}

std::int64_t
PendingServiceMessageTracker::serviceSessionId(std::int32_t serviceId,
                                               std::int64_t sessionId) {
  return (static_cast<std::int64_t>(serviceId) << 56) | sessionId;
}

bool PendingServiceMessageTracker::messageAppender(AtomicBuffer &buffer,
                                                   std::int32_t offset,
                                                   std::int32_t length,
                                                   std::int32_t headOffset) {
  const std::int32_t headerOffset = offset + MessageHeader::encodedLength();
  const std::int32_t clusterSessionIdOffset =
      headerOffset + SessionMessageHeader::clusterSessionIdEncodingOffset();
  const std::int32_t timestampOffset =
      headerOffset + SessionMessageHeader::timestampEncodingOffset();
  const std::int64_t clusterSessionId = buffer.getInt64(clusterSessionIdOffset);

  const std::int64_t appendPosition = m_logPublisher.appendMessage(
      m_leadershipTermId, clusterSessionId, m_clusterClock.time(), buffer,
      offset + client::AeronCluster::SESSION_HEADER_LENGTH,
      length - client::AeronCluster::SESSION_HEADER_LENGTH);

  if (appendPosition > 0) {
    ++m_uncommittedMessages;
    m_pendingMessageHeadOffset = headOffset;
    buffer.putInt64(timestampOffset, appendPosition);
    return true;
  }

  return false;
}

bool PendingServiceMessageTracker::messageReset(AtomicBuffer &buffer,
                                                std::int32_t offset,
                                                std::int32_t length,
                                                std::int32_t headOffset) {
  const std::int32_t timestampOffset =
      offset + MessageHeader::encodedLength() +
      SessionMessageHeader::timestampEncodingOffset();
  const std::int64_t appendPosition = buffer.getInt64(timestampOffset);

  if (appendPosition < std::numeric_limits<std::int64_t>::max()) {
    buffer.putInt64(timestampOffset, std::numeric_limits<std::int64_t>::max());
    return true;
  }

  return false;
}

bool PendingServiceMessageTracker::leaderMessageSweeper(
    AtomicBuffer &buffer, std::int32_t offset, std::int32_t length,
    std::int32_t headOffset) {
  const std::int32_t headerOffset = offset + MessageHeader::encodedLength();
  const std::int32_t clusterSessionIdOffset =
      headerOffset + SessionMessageHeader::clusterSessionIdEncodingOffset();
  const std::int32_t timestampOffset =
      headerOffset + SessionMessageHeader::timestampEncodingOffset();
  const std::int64_t appendPosition = buffer.getInt64(timestampOffset);

  if (m_commitPosition && m_commitPosition->get() >= appendPosition) {
    m_logServiceSessionId = buffer.getInt64(clusterSessionIdOffset);
    --m_uncommittedMessages;
    return true;
  }

  return false;
}

bool PendingServiceMessageTracker::followerMessageSweeper(
    AtomicBuffer &buffer, std::int32_t offset, std::int32_t length,
    std::int32_t headOffset) {
  const std::int32_t clusterSessionIdOffset =
      offset + MessageHeader::encodedLength() +
      SessionMessageHeader::clusterSessionIdEncodingOffset();

  return buffer.getInt64(clusterSessionIdOffset) <= m_logServiceSessionId;
}

} // namespace cluster
} // namespace aeron