#pragma once
#include <memory>
#include <vector>
#include "Publication.h"
#include "../client/ClusterExceptions.h"
#include "../client/ClusterEvent.h"
#include "../service/Cluster.h"
#include "ClusterMember.h"
#include "concurrent/logbuffer/BufferClaim.h"
#include "concurrent/AtomicBuffer.h"
#include "util/Exceptions.h"
#include "generated/aeron_cluster_client/MessageHeader.h"
#include "generated/aeron_cluster_client/JoinLog.h"
#include "generated/aeron_cluster_client/ClusterMembersResponse.h"
#include "generated/aeron_cluster_client/ClusterMembersExtendedResponse.h"
#include "generated/aeron_cluster_client/ServiceTerminationPosition.h"
#include "generated/aeron_cluster_client/RequestServiceAck.h"
#include "generated/aeron_cluster_client/BooleanType.h"

namespace aeron { namespace cluster
{
using namespace aeron::concurrent;
using namespace aeron::concurrent::logbuffer;
using namespace aeron::cluster::codecs;
using namespace aeron::util;

class ServiceProxy
{
public:
    explicit ServiceProxy(std::shared_ptr<Publication> publication);
    ~ServiceProxy() = default;

    void close();

    void joinLog(
        std::int64_t logPosition,
        std::int64_t maxLogPosition,
        std::int32_t memberId,
        std::int32_t logSessionId,
        std::int32_t logStreamId,
        bool isStartup,
        service::Cluster::Role::Value role,
        const std::string& channel);

    void clusterMembersResponse(
        std::int64_t correlationId,
        std::int32_t leaderMemberId,
        const std::string& activeMembers);

    void clusterMembersExtendedResponse(
        std::int64_t correlationId,
        std::int64_t currentTimeNs,
        std::int32_t leaderMemberId,
        std::int32_t memberId,
        const std::vector<ClusterMember>& activeMembers);

    void terminationPosition(std::int64_t logPosition, const exception_handler_t& errorHandler);

    void requestServiceAck(std::int64_t logPosition);

private:
    static constexpr std::int32_t SEND_ATTEMPTS = 5;

    static void checkResult(std::int64_t position, Publication& publication);

    BufferClaim m_bufferClaim;
    MessageHeaderEncoder m_messageHeaderEncoder;
    JoinLogEncoder m_joinLogEncoder;
    ClusterMembersResponseEncoder m_clusterMembersResponseEncoder;
    ServiceTerminationPositionEncoder m_serviceTerminationPositionEncoder;
    ClusterMembersExtendedResponseEncoder m_clusterMembersExtendedResponseEncoder;
    RequestServiceAckEncoder m_requestServiceAckEncoder;
    std::vector<std::uint8_t> m_expandableArrayBufferData;
    AtomicBuffer m_expandableArrayBuffer;
    std::shared_ptr<Publication> m_publication;
};

}}

