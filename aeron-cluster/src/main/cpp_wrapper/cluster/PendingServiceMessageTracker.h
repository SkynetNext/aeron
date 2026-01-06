#pragma once
#include <memory>
#include <vector>
#include <functional>
#include <cstring>
#include "Counter.h"
#include "../client/ClusterExceptions.h"
#include "../service/ClusterClock.h"
#include "concurrent/AtomicBuffer.h"
#include "generated/aeron_cluster_client/MessageHeader.h"
#include "generated/aeron_cluster_client/SessionMessageHeader.h"
#include "../client/AeronCluster.h"

namespace aeron { namespace cluster
{
using namespace aeron::concurrent;
using namespace aeron::cluster::codecs;
using namespace aeron::util;

class LogPublisher; // Forward declaration

/**
 * Simple ExpandableRingBuffer replacement using std::vector.
 * Stores variable-length messages in a continuous buffer with offset tracking.
 */
class SimpleExpandableRingBuffer
{
public:
    using MessageConsumer = std::function<bool(AtomicBuffer&, std::int32_t, std::int32_t, std::int32_t)>;

    SimpleExpandableRingBuffer() : m_capacity(4096)
    {
        m_buffer.resize(m_capacity);
        m_wrapBuffer.wrap(m_buffer.data(), m_buffer.size());
    }

    void reset(std::int32_t capacity)
    {
        m_capacity = capacity;
        m_buffer.clear();
        m_buffer.resize(m_capacity);
        m_wrapBuffer.wrap(m_buffer.data(), m_buffer.size());
        m_writeOffset = 0;
        m_readOffset = 0;
        m_size = 0;
    }

    bool append(AtomicBuffer& buffer, std::int32_t offset, std::int32_t length)
    {
        const std::int32_t totalLength = length + sizeof(std::int32_t); // length prefix
        if (m_writeOffset + totalLength > static_cast<std::int32_t>(m_buffer.size()))
        {
            // Expand buffer if needed
            std::int32_t newSize = m_buffer.size() * 2;
            while (newSize < m_writeOffset + totalLength)
            {
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

    std::int32_t consume(MessageConsumer& consumer, std::int32_t limit)
    {
        std::int32_t consumed = 0;
        std::int32_t offset = m_readOffset;
        std::int32_t count = 0;

        while (offset < m_writeOffset && count < limit && count < m_size)
        {
            std::int32_t length = m_wrapBuffer.getInt32(offset);
            offset += sizeof(std::int32_t);

            AtomicBuffer msgBuffer;
            msgBuffer.wrap(m_buffer.data() + offset, length);

            if (consumer(msgBuffer, 0, length, offset - sizeof(std::int32_t)))
            {
                offset += length;
                m_readOffset = offset;
                consumed += sizeof(std::int32_t) + length;
                count++;
                m_size--;
            }
            else
            {
                break;
            }
        }

        return consumed;
    }

    std::int32_t forEach(MessageConsumer& consumer, std::int32_t limit)
    {
        std::int32_t offset = m_readOffset;
        std::int32_t count = 0;

        while (offset < m_writeOffset && count < limit && count < m_size)
        {
            std::int32_t length = m_wrapBuffer.getInt32(offset);
            offset += sizeof(std::int32_t);

            AtomicBuffer msgBuffer;
            msgBuffer.wrap(m_buffer.data() + offset, length);

            if (!consumer(msgBuffer, 0, length, offset - sizeof(std::int32_t)))
            {
                break;
            }

            offset += length;
            count++;
        }

        return count;
    }

    std::int32_t forEach(std::int32_t headOffset, MessageConsumer& consumer, std::int32_t limit)
    {
        std::int32_t offset = headOffset;
        std::int32_t count = 0;

        while (offset < m_writeOffset && count < limit)
        {
            if (offset >= static_cast<std::int32_t>(m_buffer.size()))
            {
                break;
            }

            std::int32_t length = m_wrapBuffer.getInt32(offset);
            offset += sizeof(std::int32_t);

            AtomicBuffer msgBuffer;
            msgBuffer.wrap(m_buffer.data() + offset, length);

            if (!consumer(msgBuffer, 0, length, offset - sizeof(std::int32_t)))
            {
                break;
            }

            offset += length;
            count++;
        }

        return count;
    }

    std::int32_t size() const
    {
        return m_size;
    }

    bool isEmpty() const
    {
        return m_size == 0;
    }

private:
    std::vector<std::uint8_t> m_buffer;
    AtomicBuffer m_wrapBuffer;
    std::int32_t m_capacity;
    std::int32_t m_writeOffset = 0;
    std::int32_t m_readOffset = 0;
    std::int32_t m_size = 0;
};

class PendingServiceMessageTracker
{
public:
    static constexpr std::int32_t SERVICE_MESSAGE_LIMIT = 20;

    PendingServiceMessageTracker(
        std::int32_t serviceId,
        std::shared_ptr<Counter> commitPosition,
        LogPublisher& logPublisher,
        service::ClusterClock& clusterClock);

    void leadershipTermId(std::int64_t leadershipTermId);

    std::int32_t serviceId() const;
    std::int64_t nextServiceSessionId() const;
    std::int64_t logServiceSessionId() const;

    void enqueueMessage(AtomicBuffer& buffer, std::int32_t offset, std::int32_t length);

    void sweepFollowerMessages(std::int64_t clusterSessionId);

    void sweepLeaderMessages();

    void restoreUncommittedMessages();

    void appendMessage(AtomicBuffer& buffer, std::int32_t offset, std::int32_t length);

    void loadState(
        std::int64_t nextServiceSessionId,
        std::int64_t logServiceSessionId,
        std::int32_t pendingMessageCapacity);

    std::int32_t poll();

    std::int32_t size() const;

    void verify();

    void reset();

    SimpleExpandableRingBuffer& pendingMessages()
    {
        return m_pendingMessages;
    }

    static std::int32_t serviceIdFromLogMessage(std::int64_t clusterSessionId);

    /**
     * Services use different approach for communicating the serviceId, this method extracts the serviceId from a
     * cluster session id sent via an inter-service message.
     *
     * @param clusterSessionId passed in on an inter-service message.
     * @return the associated serviceId.
     */
    static std::int32_t serviceIdFromServiceMessage(std::int64_t clusterSessionId);

    static std::int64_t serviceSessionId(std::int32_t serviceId, std::int64_t sessionId);

private:
    bool messageAppender(AtomicBuffer& buffer, std::int32_t offset, std::int32_t length, std::int32_t headOffset);

    static bool messageReset(AtomicBuffer& buffer, std::int32_t offset, std::int32_t length, std::int32_t headOffset);

    bool leaderMessageSweeper(AtomicBuffer& buffer, std::int32_t offset, std::int32_t length, std::int32_t headOffset);

    bool followerMessageSweeper(AtomicBuffer& buffer, std::int32_t offset, std::int32_t length, std::int32_t headOffset);

    std::int32_t m_serviceId;
    std::int32_t m_pendingMessageHeadOffset = 0;
    std::int32_t m_uncommittedMessages = 0;
    std::int64_t m_nextServiceSessionId;
    std::int64_t m_logServiceSessionId;
    std::int64_t m_leadershipTermId = aeron::NULL_VALUE;

    std::shared_ptr<Counter> m_commitPosition;
    LogPublisher& m_logPublisher;
    service::ClusterClock& m_clusterClock;
    SimpleExpandableRingBuffer m_pendingMessages;
    SimpleExpandableRingBuffer::MessageConsumer m_messageAppender;
    SimpleExpandableRingBuffer::MessageConsumer m_leaderMessageSweeper;
    SimpleExpandableRingBuffer::MessageConsumer m_followerMessageSweeper;
};

// Implementation
inline PendingServiceMessageTracker::PendingServiceMessageTracker(
    std::int32_t serviceId,
    std::shared_ptr<Counter> commitPosition,
    LogPublisher& logPublisher,
    service::ClusterClock& clusterClock) :
    m_serviceId(serviceId),
    m_commitPosition(commitPosition),
    m_logPublisher(logPublisher),
    m_clusterClock(clusterClock),
    m_messageAppender([this](AtomicBuffer& buffer, std::int32_t offset, std::int32_t length, std::int32_t headOffset)
    {
        return this->messageAppender(buffer, offset, length, headOffset);
    }),
    m_leaderMessageSweeper([this](AtomicBuffer& buffer, std::int32_t offset, std::int32_t length, std::int32_t headOffset)
    {
        return this->leaderMessageSweeper(buffer, offset, length, headOffset);
    }),
    m_followerMessageSweeper([this](AtomicBuffer& buffer, std::int32_t offset, std::int32_t length, std::int32_t headOffset)
    {
        return this->followerMessageSweeper(buffer, offset, length, headOffset);
    })
{
    m_logServiceSessionId = serviceSessionId(serviceId, std::numeric_limits<std::int64_t>::min());
    m_nextServiceSessionId = m_logServiceSessionId + 1;
}

inline void PendingServiceMessageTracker::leadershipTermId(std::int64_t leadershipTermId)
{
    m_leadershipTermId = leadershipTermId;
}

inline std::int32_t PendingServiceMessageTracker::serviceId() const
{
    return m_serviceId;
}

inline std::int64_t PendingServiceMessageTracker::nextServiceSessionId() const
{
    return m_nextServiceSessionId;
}

inline std::int64_t PendingServiceMessageTracker::logServiceSessionId() const
{
    return m_logServiceSessionId;
}

inline void PendingServiceMessageTracker::enqueueMessage(AtomicBuffer& buffer, std::int32_t offset, std::int32_t length)
{
    const std::int64_t clusterSessionId = m_nextServiceSessionId++;
    if (clusterSessionId > m_logServiceSessionId)
    {
        const std::int32_t headerOffset = offset - SessionMessageHeader::sbeBlockLength();
        const std::int32_t clusterSessionIdOffset = 
            headerOffset + SessionMessageHeader::clusterSessionIdEncodingOffset();
        const std::int32_t timestampOffset = headerOffset + SessionMessageHeader::timestampEncodingOffset();

        buffer.putInt64(clusterSessionIdOffset, clusterSessionId, SessionMessageHeader::sbeByteOrder());
        buffer.putInt64(timestampOffset, std::numeric_limits<std::int64_t>::max(), SessionMessageHeader::sbeByteOrder());
        
        if (!m_pendingMessages.append(buffer, offset - client::AeronCluster::SESSION_HEADER_LENGTH, 
                                      length + client::AeronCluster::SESSION_HEADER_LENGTH))
        {
            throw ClusterException(
                "pending service message buffer at capacity=" + std::to_string(m_pendingMessages.size()) +
                " for serviceId=" + std::to_string(m_serviceId),
                SOURCEINFO);
        }
    }
}

inline void PendingServiceMessageTracker::sweepFollowerMessages(std::int64_t clusterSessionId)
{
    m_logServiceSessionId = clusterSessionId;
    m_pendingMessages.consume(m_followerMessageSweeper, std::numeric_limits<std::int32_t>::max());
}

inline void PendingServiceMessageTracker::sweepLeaderMessages()
{
    if (m_uncommittedMessages > 0)
    {
        m_pendingMessageHeadOffset -= m_pendingMessages.consume(m_leaderMessageSweeper, std::numeric_limits<std::int32_t>::max());
        m_pendingMessageHeadOffset = std::max(m_pendingMessageHeadOffset, 0);
    }
}

inline void PendingServiceMessageTracker::restoreUncommittedMessages()
{
    if (m_uncommittedMessages > 0)
    {
        m_pendingMessages.consume(m_leaderMessageSweeper, std::numeric_limits<std::int32_t>::max());
        m_pendingMessages.forEach(
            [](AtomicBuffer& buffer, std::int32_t offset, std::int32_t length, std::int32_t headOffset)
            {
                return messageReset(buffer, offset, length, headOffset);
            },
            std::numeric_limits<std::int32_t>::max());
        m_uncommittedMessages = 0;
        m_pendingMessageHeadOffset = 0;
    }
}

inline void PendingServiceMessageTracker::appendMessage(AtomicBuffer& buffer, std::int32_t offset, std::int32_t length)
{
    m_pendingMessages.append(buffer, offset, length);
}

inline void PendingServiceMessageTracker::loadState(
    std::int64_t nextServiceSessionId,
    std::int64_t logServiceSessionId,
    std::int32_t pendingMessageCapacity)
{
    m_nextServiceSessionId = nextServiceSessionId;
    m_logServiceSessionId = logServiceSessionId;
    m_pendingMessages.reset(pendingMessageCapacity);
}

inline std::int32_t PendingServiceMessageTracker::poll()
{
    return m_pendingMessages.forEach(m_pendingMessageHeadOffset, m_messageAppender, SERVICE_MESSAGE_LIMIT);
}

inline std::int32_t PendingServiceMessageTracker::size() const
{
    return m_pendingMessages.size();
}

inline void PendingServiceMessageTracker::verify()
{
    std::int32_t messageCount = 0;
    SimpleExpandableRingBuffer::MessageConsumer messageConsumer = 
        [this, &messageCount](AtomicBuffer& buffer, std::int32_t offset, std::int32_t length, std::int32_t headOffset)
        {
            messageCount++;

            const std::int32_t headerOffset = offset + MessageHeader::encodedLength();
            const std::int32_t clusterSessionIdOffset = 
                headerOffset + SessionMessageHeader::clusterSessionIdEncodingOffset();

            const std::int64_t clusterSessionId = buffer.getInt64(
                clusterSessionIdOffset, SessionMessageHeader::sbeByteOrder());

            if (clusterSessionId != (m_logServiceSessionId + messageCount))
            {
                throw ClusterException(
                    "snapshot has incorrect pending message:" +
                    " serviceId=" + std::to_string(m_serviceId) +
                    " nextServiceSessionId=" + std::to_string(m_nextServiceSessionId) +
                    " logServiceSessionId=" + std::to_string(m_logServiceSessionId) +
                    " clusterSessionId=" + std::to_string(clusterSessionId) +
                    " pendingMessageIndex=" + std::to_string(messageCount),
                    SOURCEINFO);
            }

            return true;
        };

    m_pendingMessages.forEach(messageConsumer, std::numeric_limits<std::int32_t>::max());

    if (m_nextServiceSessionId != (m_logServiceSessionId + messageCount + 1))
    {
        throw ClusterException(
            "snapshot has incorrect pending message state:" +
            " serviceId=" + std::to_string(m_serviceId) +
            " nextServiceSessionId=" + std::to_string(m_nextServiceSessionId) +
            " logServiceSessionId=" + std::to_string(m_logServiceSessionId) +
            " pendingMessageCount=" + std::to_string(messageCount),
            SOURCEINFO);
    }
}

inline void PendingServiceMessageTracker::reset()
{
    m_pendingMessages.forEach(
        [](AtomicBuffer& buffer, std::int32_t offset, std::int32_t length, std::int32_t headOffset)
        {
            return messageReset(buffer, offset, length, headOffset);
        },
        std::numeric_limits<std::int32_t>::max());
}

inline std::int32_t PendingServiceMessageTracker::serviceIdFromLogMessage(std::int64_t clusterSessionId)
{
    return static_cast<std::int32_t>((clusterSessionId >> 56) & 0x7F);
}

inline std::int32_t PendingServiceMessageTracker::serviceIdFromServiceMessage(std::int64_t clusterSessionId)
{
    return static_cast<std::int32_t>(clusterSessionId);
}

inline std::int64_t PendingServiceMessageTracker::serviceSessionId(std::int32_t serviceId, std::int64_t sessionId)
{
    return (static_cast<std::int64_t>(serviceId) << 56) | sessionId;
}

inline bool PendingServiceMessageTracker::messageAppender(
    AtomicBuffer& buffer, std::int32_t offset, std::int32_t length, std::int32_t headOffset)
{
    const std::int32_t headerOffset = offset + MessageHeader::encodedLength();
    const std::int32_t clusterSessionIdOffset = headerOffset + SessionMessageHeader::clusterSessionIdEncodingOffset();
    const std::int32_t timestampOffset = headerOffset + SessionMessageHeader::timestampEncodingOffset();
    const std::int64_t clusterSessionId = buffer.getInt64(clusterSessionIdOffset, SessionMessageHeader::sbeByteOrder());

    const std::int64_t appendPosition = m_logPublisher.appendMessage(
        m_leadershipTermId,
        clusterSessionId,
        m_clusterClock.time(),
        buffer,
        offset + client::AeronCluster::SESSION_HEADER_LENGTH,
        length - client::AeronCluster::SESSION_HEADER_LENGTH);

    if (appendPosition > 0)
    {
        ++m_uncommittedMessages;
        m_pendingMessageHeadOffset = headOffset;
        buffer.putInt64(timestampOffset, appendPosition, SessionMessageHeader::sbeByteOrder());
        return true;
    }

    return false;
}

inline bool PendingServiceMessageTracker::messageReset(
    AtomicBuffer& buffer, std::int32_t offset, std::int32_t length, std::int32_t headOffset)
{
    const std::int32_t timestampOffset = offset +
        MessageHeader::encodedLength() + SessionMessageHeader::timestampEncodingOffset();
    const std::int64_t appendPosition = buffer.getInt64(timestampOffset, SessionMessageHeader::sbeByteOrder());

    if (appendPosition < std::numeric_limits<std::int64_t>::max())
    {
        buffer.putInt64(timestampOffset, std::numeric_limits<std::int64_t>::max(), SessionMessageHeader::sbeByteOrder());
        return true;
    }

    return false;
}

inline bool PendingServiceMessageTracker::leaderMessageSweeper(
    AtomicBuffer& buffer, std::int32_t offset, std::int32_t length, std::int32_t headOffset)
{
    const std::int32_t headerOffset = offset + MessageHeader::encodedLength();
    const std::int32_t clusterSessionIdOffset = headerOffset + SessionMessageHeader::clusterSessionIdEncodingOffset();
    const std::int32_t timestampOffset = headerOffset + SessionMessageHeader::timestampEncodingOffset();
    const std::int64_t appendPosition = buffer.getInt64(timestampOffset, SessionMessageHeader::sbeByteOrder());

    if (m_commitPosition && m_commitPosition->get() >= appendPosition)
    {
        m_logServiceSessionId = buffer.getInt64(clusterSessionIdOffset, SessionMessageHeader::sbeByteOrder());
        --m_uncommittedMessages;
        return true;
    }

    return false;
}

inline bool PendingServiceMessageTracker::followerMessageSweeper(
    AtomicBuffer& buffer, std::int32_t offset, std::int32_t length, std::int32_t headOffset)
{
    const std::int32_t clusterSessionIdOffset = offset +
        MessageHeader::encodedLength() + SessionMessageHeader::clusterSessionIdEncodingOffset();

    return buffer.getInt64(clusterSessionIdOffset, SessionMessageHeader::sbeByteOrder()) <= m_logServiceSessionId;
}

}}

