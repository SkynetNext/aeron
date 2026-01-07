#pragma once
#include <memory>
#include <vector>
#include <string>
#include "client/AeronCluster.h"
#include "concurrent/logbuffer/BufferClaim.h"
#include "concurrent/AtomicBuffer.h"
#include "util/ExpandableArrayBuffer.h"
// ArrayUtil not needed in C++ wrapper
#include "util/DirectBuffer.h"
#include "generated/aeron_cluster_codecs/MessageHeader.h"
#include "generated/aeron_cluster_codecs/SessionEvent.h"
#include "generated/aeron_cluster_codecs/Challenge.h"
#include "generated/aeron_cluster_codecs/NewLeaderEvent.h"
#include "generated/aeron_cluster_codecs/AdminResponse.h"
#include "generated/aeron_cluster_codecs/EventCode.h"
#include "generated/aeron_cluster_codecs/AdminRequestType.h"
#include "generated/aeron_cluster_codecs/AdminResponseCode.h"

namespace aeron { namespace cluster
{
using namespace aeron::concurrent;
using namespace aeron::concurrent::logbuffer;
using namespace aeron::cluster::codecs;
using namespace aeron::util;
using namespace aeron::cluster::client;

// Forward declaration - full definition needed in implementation
class ClusterSession;

class EgressPublisher
{
public:
    explicit EgressPublisher(std::int64_t leaderHeartbeatTimeoutNs);

    bool sendEvent(
        ClusterSession& session,
        std::int64_t leadershipTermId,
        std::int32_t leaderMemberId,
        EventCode::Value code,
        const std::string& detail);

    bool sendChallenge(ClusterSession& session, const std::vector<std::uint8_t>& encodedChallenge);

    bool newLeader(
        ClusterSession& session,
        std::int64_t leadershipTermId,
        std::int32_t leaderMemberId,
        const std::string& ingressEndpoints);

    bool sendAdminResponse(
        ClusterSession& session,
        std::int64_t correlationId,
        AdminRequestType::Value adminRequestType,
        AdminResponseCode::Value responseCode,
        const std::string& message);

    std::string toString() const;

private:
    static constexpr std::int32_t SEND_ATTEMPTS = 3;
    // MAX_ENCODED_MEMBERSHIP_QUERY_LENGTH is defined in ClusterSession.h
    // Using a local constant to avoid dependency on incomplete type
    static constexpr std::int32_t MAX_ENCODED_MEMBERSHIP_QUERY_LENGTH = 4 * 1024; // Same as ClusterSession::MAX_ENCODED_MEMBERSHIP_QUERY_LENGTH

    BufferClaim m_bufferClaim;
    ExpandableArrayBuffer m_buffer;
    MessageHeader m_messageHeaderEncoder;
    SessionEvent m_sessionEventEncoder;
    Challenge m_challengeEncoder;
    NewLeaderEvent m_newLeaderEventEncoder;
    AdminResponse m_adminResponseEncoder;
    std::int64_t m_leaderHeartbeatTimeoutNs;
};

// Implementation
// Note: ClusterSession must be fully defined when these inline functions are instantiated.
// Since ClusterSession.h includes EgressPublisher.h, ClusterSession will be fully defined
// when these functions are actually used.

inline EgressPublisher::EgressPublisher(std::int64_t leaderHeartbeatTimeoutNs) :
    m_buffer(MAX_ENCODED_MEMBERSHIP_QUERY_LENGTH),
    m_leaderHeartbeatTimeoutNs(leaderHeartbeatTimeoutNs)
{
}

inline bool EgressPublisher::sendEvent(
    ClusterSession& session,
    std::int64_t leadershipTermId,
    std::int32_t leaderMemberId,
    EventCode::Value code,
    const std::string& detail)
{
    const std::int32_t length = static_cast<std::int32_t>(
        MessageHeader::encodedLength() +
        SessionEvent::SBE_BLOCK_LENGTH +
        SessionEvent::detailHeaderLength() +
        detail.length());

    int attempts = SEND_ATTEMPTS;
    do
    {
        const std::int64_t position = session.tryClaim(length, m_bufferClaim);
        if (position > 0)
        {
            m_sessionEventEncoder
                .wrapAndApplyHeader(reinterpret_cast<char *>(m_bufferClaim.buffer().buffer()), m_bufferClaim.offset(), m_bufferClaim.buffer().capacity())
                .clusterSessionId(session.id())
                .correlationId(session.correlationId())
                .leadershipTermId(leadershipTermId)
                .leaderMemberId(leaderMemberId)
                .code(code)
                .version(client::AeronCluster::Configuration::PROTOCOL_SEMANTIC_VERSION)
                .leaderHeartbeatTimeoutNs(m_leaderHeartbeatTimeoutNs)
                .detail(detail);

            m_bufferClaim.commit();
            return true;
        }
    }
    while (--attempts > 0);

    return false;
}

inline bool EgressPublisher::sendChallenge(ClusterSession& session, const std::vector<std::uint8_t>& encodedChallenge)
{
    AtomicBuffer challengeBuffer;
    challengeBuffer.wrap(m_buffer.buffer(), m_buffer.capacity());
    m_challengeEncoder
        .wrapAndApplyHeader(reinterpret_cast<char *>(challengeBuffer.buffer()), 0, challengeBuffer.capacity())
        .clusterSessionId(session.id())
        .correlationId(session.correlationId());
    
    if (!encodedChallenge.empty())
    {
        m_challengeEncoder.putEncodedChallenge(reinterpret_cast<const char *>(encodedChallenge.data()), static_cast<std::uint32_t>(encodedChallenge.size()));
    }

    const std::int32_t length = static_cast<std::int32_t>(
        MessageHeader::encodedLength() + m_challengeEncoder.encodedLength());

    int attempts = SEND_ATTEMPTS;
    do
    {
        util::DirectBuffer directBuffer(m_buffer.buffer(), m_buffer.capacity());
        const std::int64_t position = session.offer(directBuffer, 0, length);
        if (position > 0)
        {
            return true;
        }
    }
    while (--attempts > 0);

    return false;
}

inline bool EgressPublisher::newLeader(
    ClusterSession& session,
    std::int64_t leadershipTermId,
    std::int32_t leaderMemberId,
    const std::string& ingressEndpoints)
{
    const std::int32_t length = static_cast<std::int32_t>(
        MessageHeader::encodedLength() +
        NewLeaderEvent::SBE_BLOCK_LENGTH +
        NewLeaderEvent::ingressEndpointsHeaderLength() +
        ingressEndpoints.length());

    int attempts = SEND_ATTEMPTS;
    do
    {
        const std::int64_t position = session.tryClaim(length, m_bufferClaim);
        if (position > 0)
        {
            m_newLeaderEventEncoder
                .wrapAndApplyHeader(reinterpret_cast<char *>(m_bufferClaim.buffer().buffer()), m_bufferClaim.offset(), m_bufferClaim.buffer().capacity())
                .clusterSessionId(session.id())
                .leadershipTermId(leadershipTermId)
                .leaderMemberId(leaderMemberId)
                .putIngressEndpoints(ingressEndpoints.data(), static_cast<std::uint32_t>(ingressEndpoints.length()));

            m_bufferClaim.commit();
            return true;
        }
    }
    while (--attempts > 0);

    return false;
}

inline bool EgressPublisher::sendAdminResponse(
    ClusterSession& session,
    std::int64_t correlationId,
    AdminRequestType::Value adminRequestType,
    AdminResponseCode::Value responseCode,
    const std::string& message)
{
    AtomicBuffer adminBuffer;
    adminBuffer.wrap(m_buffer.buffer(), m_buffer.capacity());
    m_adminResponseEncoder
        .wrapAndApplyHeader(reinterpret_cast<char *>(adminBuffer.buffer()), 0, adminBuffer.capacity())
        .clusterSessionId(session.id())
        .correlationId(correlationId)
        .requestType(adminRequestType)
        .responseCode(responseCode)
        .putMessage(message.data(), static_cast<std::uint32_t>(message.length()))
        .putPayload(nullptr, 0);

    const std::int32_t length = static_cast<std::int32_t>(
        MessageHeader::encodedLength() + m_adminResponseEncoder.encodedLength());

    int attempts = SEND_ATTEMPTS;
    do
    {
        util::DirectBuffer directBuffer(m_buffer.buffer(), m_buffer.capacity());
        const std::int64_t position = session.offer(directBuffer, 0, length);
        if (position > 0)
        {
            return true;
        }
    }
    while (--attempts > 0);

    return false;
}

inline std::string EgressPublisher::toString() const
{
    return "EgressPublisher{}";
}

}}

