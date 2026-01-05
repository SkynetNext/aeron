#pragma once
#include <memory>
#include <vector>
#include <string>
#include "../client/AeronCluster.h"
#include "concurrent/logbuffer/BufferClaim.h"
#include "concurrent/AtomicBuffer.h"
#include "util/ExpandableArrayBuffer.h"
#include "util/ArrayUtil.h"
#include "util/DirectBuffer.h"
#include "generated/aeron_cluster_client/MessageHeader.h"
#include "generated/aeron_cluster_client/SessionEvent.h"
#include "generated/aeron_cluster_client/Challenge.h"
#include "generated/aeron_cluster_client/NewLeaderEvent.h"
#include "generated/aeron_cluster_client/AdminResponse.h"
#include "generated/aeron_cluster_client/EventCode.h"
#include "generated/aeron_cluster_client/AdminRequestType.h"
#include "generated/aeron_cluster_client/AdminResponseCode.h"

namespace aeron { namespace cluster
{
using namespace aeron::concurrent;
using namespace aeron::concurrent::logbuffer;
using namespace aeron::cluster::codecs;
using namespace aeron::util;
using namespace aeron::cluster::client;

class ClusterSession; // Forward declaration

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
    static constexpr std::int32_t MAX_ENCODED_MEMBERSHIP_QUERY_LENGTH = ClusterSession::MAX_ENCODED_MEMBERSHIP_QUERY_LENGTH;

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
                .wrapAndApplyHeader(m_bufferClaim.buffer(), m_bufferClaim.offset(), m_messageHeaderEncoder)
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
    m_challengeEncoder
        .wrapAndApplyHeader(m_buffer, 0, m_messageHeaderEncoder)
        .clusterSessionId(session.id())
        .correlationId(session.correlationId());
    
    if (!encodedChallenge.empty())
    {
        m_challengeEncoder.putEncodedCredentials(encodedChallenge.data(), encodedChallenge.size());
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
                .wrapAndApplyHeader(m_bufferClaim.buffer(), m_bufferClaim.offset(), m_messageHeaderEncoder)
                .clusterSessionId(session.id())
                .leadershipTermId(leadershipTermId)
                .leaderMemberId(leaderMemberId)
                .ingressEndpoints(ingressEndpoints);

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
    m_adminResponseEncoder
        .wrapAndApplyHeader(m_buffer, 0, m_messageHeaderEncoder)
        .clusterSessionId(session.id())
        .correlationId(correlationId)
        .requestType(adminRequestType)
        .responseCode(responseCode)
        .message(message)
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

