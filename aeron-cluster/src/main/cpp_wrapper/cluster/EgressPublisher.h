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

// Implementation of sendEvent, sendChallenge, newLeader, and sendAdminResponse moved to ClusterSession.h
// to resolve circular dependency - these functions need ClusterSession's full definition

inline std::string EgressPublisher::toString() const
{
    return "EgressPublisher{}";
}

}}

