#pragma once
#include "Context.h"
#include "Counter.h"
#include "concurrent/AtomicBuffer.h"
#include "service/ClusterClock.h"
#include <cstring>
#include <functional>
#include <memory>
#include <vector>

namespace aeron {
namespace cluster {
using namespace aeron::concurrent;

class LogPublisher; // Forward declaration

/**
 * Simple ExpandableRingBuffer replacement using std::vector.
 * Stores variable-length messages in a continuous buffer with offset tracking.
 */
class SimpleExpandableRingBuffer {
public:
  using MessageConsumer = std::function<bool(AtomicBuffer &, std::int32_t,
                                             std::int32_t, std::int32_t)>;

  SimpleExpandableRingBuffer() : m_capacity(4096) {
    m_buffer.resize(m_capacity);
    m_wrapBuffer.wrap(m_buffer.data(), m_buffer.size());
  }

  void reset(std::int32_t capacity) {
    m_capacity = capacity;
    m_buffer.clear();
    m_buffer.resize(m_capacity);
    m_wrapBuffer.wrap(m_buffer.data(), m_buffer.size());
    m_writeOffset = 0;
    m_readOffset = 0;
    m_size = 0;
  }

  bool append(const AtomicBuffer &buffer, std::int32_t offset,
              std::int32_t length) {
    const std::int32_t totalLength =
        length + sizeof(std::int32_t); // length prefix
    if (m_writeOffset + totalLength >
        static_cast<std::int32_t>(m_buffer.size())) {
      // Expand buffer if needed
      std::int32_t newSize = m_buffer.size() * 2;
      while (newSize < m_writeOffset + totalLength) {
        newSize *= 2;
      }
      m_buffer.resize(newSize);
      m_wrapBuffer.wrap(m_buffer.data(), m_buffer.size());
    }

    // Write length prefix
    m_wrapBuffer.putInt32(m_writeOffset, length);
    // Write message data
    std::memcpy(m_buffer.data() + m_writeOffset + sizeof(std::int32_t),
                buffer.buffer() + offset, length);

    m_writeOffset += totalLength;
    m_size++;
    return true;
  }

  std::int32_t consume(MessageConsumer &consumer, std::int32_t limit) {
    std::int32_t consumed = 0;
    std::int32_t offset = m_readOffset;
    std::int32_t count = 0;

    while (offset < m_writeOffset && count < limit && count < m_size) {
      std::int32_t length = m_wrapBuffer.getInt32(offset);
      offset += sizeof(std::int32_t);

      AtomicBuffer msgBuffer;
      msgBuffer.wrap(m_buffer.data() + offset, length);

      if (consumer(msgBuffer, 0, length, offset - sizeof(std::int32_t))) {
        offset += length;
        m_readOffset = offset;
        consumed += sizeof(std::int32_t) + length;
        count++;
        m_size--;
      } else {
        break;
      }
    }

    return consumed;
  }

  std::int32_t forEach(MessageConsumer &consumer, std::int32_t limit) {
    std::int32_t offset = m_readOffset;
    std::int32_t count = 0;

    while (offset < m_writeOffset && count < limit && count < m_size) {
      std::int32_t length = m_wrapBuffer.getInt32(offset);
      offset += sizeof(std::int32_t);

      AtomicBuffer msgBuffer;
      msgBuffer.wrap(m_buffer.data() + offset, length);

      if (!consumer(msgBuffer, 0, length, offset - sizeof(std::int32_t))) {
        break;
      }

      offset += length;
      count++;
    }

    return count;
  }

  std::int32_t forEach(std::int32_t headOffset, MessageConsumer &consumer,
                       std::int32_t limit) {
    std::int32_t offset = headOffset;
    std::int32_t count = 0;

    while (offset < m_writeOffset && count < limit) {
      if (offset >= static_cast<std::int32_t>(m_buffer.size())) {
        break;
      }

      std::int32_t length = m_wrapBuffer.getInt32(offset);
      offset += sizeof(std::int32_t);

      AtomicBuffer msgBuffer;
      msgBuffer.wrap(m_buffer.data() + offset, length);

      if (!consumer(msgBuffer, 0, length, offset - sizeof(std::int32_t))) {
        break;
      }

      offset += length;
      count++;
    }

    return count;
  }

  std::int32_t size() const { return m_size; }

  bool isEmpty() const { return m_size == 0; }

private:
  std::vector<std::uint8_t> m_buffer;
  AtomicBuffer m_wrapBuffer;
  std::int32_t m_capacity;
  std::int32_t m_writeOffset = 0;
  std::int32_t m_readOffset = 0;
  std::int32_t m_size = 0;
};

class PendingServiceMessageTracker {
public:
  static constexpr std::int32_t SERVICE_MESSAGE_LIMIT = 20;

  PendingServiceMessageTracker(std::int32_t serviceId,
                               std::shared_ptr<Counter> commitPosition,
                               LogPublisher &logPublisher,
                               service::ClusterClock &clusterClock);

  void leadershipTermId(std::int64_t leadershipTermId);

  std::int32_t serviceId() const;
  std::int64_t nextServiceSessionId() const;
  std::int64_t logServiceSessionId() const;

  void enqueueMessage(AtomicBuffer &buffer, std::int32_t offset,
                      std::int32_t length);

  void sweepFollowerMessages(std::int64_t clusterSessionId);

  void sweepLeaderMessages();

  void restoreUncommittedMessages();

  void appendMessage(const AtomicBuffer &buffer, std::int32_t offset,
                     std::int32_t length);

  void loadState(std::int64_t nextServiceSessionId,
                 std::int64_t logServiceSessionId,
                 std::int32_t pendingMessageCapacity);

  std::int32_t poll();

  std::int32_t size() const;

  void verify();

  void reset();

  SimpleExpandableRingBuffer &pendingMessages() { return m_pendingMessages; }

  static std::int32_t serviceIdFromLogMessage(std::int64_t clusterSessionId);

  /**
   * Services use different approach for communicating the serviceId, this
   * method extracts the serviceId from a cluster session id sent via an
   * inter-service message.
   *
   * @param clusterSessionId passed in on an inter-service message.
   * @return the associated serviceId.
   */
  static std::int32_t
  serviceIdFromServiceMessage(std::int64_t clusterSessionId);

  static std::int64_t serviceSessionId(std::int32_t serviceId,
                                       std::int64_t sessionId);

private:
  bool messageAppender(AtomicBuffer &buffer, std::int32_t offset,
                       std::int32_t length, std::int32_t headOffset);

  static bool messageReset(AtomicBuffer &buffer, std::int32_t offset,
                           std::int32_t length, std::int32_t headOffset);

  bool leaderMessageSweeper(AtomicBuffer &buffer, std::int32_t offset,
                            std::int32_t length, std::int32_t headOffset);

  bool followerMessageSweeper(AtomicBuffer &buffer, std::int32_t offset,
                              std::int32_t length, std::int32_t headOffset);

  std::int32_t m_serviceId;
  std::int32_t m_pendingMessageHeadOffset = 0;
  std::int32_t m_uncommittedMessages = 0;
  std::int64_t m_nextServiceSessionId;
  std::int64_t m_logServiceSessionId;
  std::int64_t m_leadershipTermId =
      static_cast<std::int64_t>(aeron::NULL_VALUE);

  std::shared_ptr<Counter> m_commitPosition;
  LogPublisher &m_logPublisher;
  service::ClusterClock &m_clusterClock;
  SimpleExpandableRingBuffer m_pendingMessages;
  SimpleExpandableRingBuffer::MessageConsumer m_messageAppender;
  SimpleExpandableRingBuffer::MessageConsumer m_leaderMessageSweeper;
  SimpleExpandableRingBuffer::MessageConsumer m_followerMessageSweeper;
};

} // namespace cluster
} // namespace aeron
