#pragma once
#include <memory>
#include <vector>
#include <string>
#include "Subscription.h"
#include "../client/ClusterExceptions.h"
#include "ClusterMember.h"
#include "FragmentAssembler.h"
#include "concurrent/logbuffer/Header.h"
#include "concurrent/AtomicBuffer.h"
#include "util/CloseHelper.h"
#include "generated/aeron_cluster_client/MessageHeader.h"
#include "generated/aeron_cluster_client/ClusterMembersResponse.h"
#include "generated/aeron_cluster_client/ClusterMembersExtendedResponse.h"

namespace aeron { namespace cluster
{
using namespace aeron::concurrent;
using namespace aeron::concurrent::logbuffer;
using namespace aeron::cluster::codecs;
using namespace aeron::util;

class ClusterControlAdapter
{
public:
    class Listener
    {
    public:
        virtual ~Listener() = default;

        virtual void onClusterMembersResponse(
            std::int64_t correlationId,
            std::int32_t leaderMemberId,
            const std::string& activeMembers) = 0;

        virtual void onClusterMembersExtendedResponse(
            std::int64_t correlationId,
            std::int64_t currentTimeNs,
            std::int32_t leaderMemberId,
            std::int32_t memberId,
            const std::vector<ClusterMember>& activeMembers) = 0;
    };

    ClusterControlAdapter(
        std::shared_ptr<Subscription> subscription,
        Listener& listener);

    ~ClusterControlAdapter() = default; // AutoCloseable equivalent

    void close();

    std::int32_t poll();

    bool isBound() const;

private:
    void onFragment(
        AtomicBuffer& buffer,
        std::int32_t offset,
        std::int32_t length,
        Header& header);

    std::shared_ptr<Subscription> m_subscription;
    Listener& m_listener;
    FragmentAssembler m_fragmentAssembler;
    MessageHeader m_messageHeaderDecoder;
    ClusterMembersResponse m_clusterMembersResponseDecoder;
    ClusterMembersExtendedResponse m_clusterMembersExtendedResponseDecoder;
};

// Implementation
inline ClusterControlAdapter::ClusterControlAdapter(
    std::shared_ptr<Subscription> subscription,
    Listener& listener) :
    m_subscription(subscription),
    m_listener(listener),
    m_fragmentAssembler([this](AtomicBuffer& buffer, util::index_t offset, util::index_t length, Header& header)
    {
        this->onFragment(buffer, offset, length, header);
    })
{
}

inline void ClusterControlAdapter::close()
{
    if (m_subscription)
    {
        m_subscription->close();
    }
}

inline std::int32_t ClusterControlAdapter::poll()
{
    return m_subscription->poll(m_fragmentAssembler.handler(), 1);
}

inline bool ClusterControlAdapter::isBound() const
{
    return m_subscription->isConnected();
}

inline void ClusterControlAdapter::onFragment(
    AtomicBuffer& buffer,
    std::int32_t offset,
    std::int32_t length,
    Header& header)
{
    m_messageHeaderDecoder.wrap(buffer, offset);

    const std::int32_t schemaId = m_messageHeaderDecoder.schemaId();
    if (schemaId != MessageHeader::sbeSchemaId())
    {
        throw ClusterException(
            "expected schemaId=" + std::to_string(MessageHeader::sbeSchemaId()) +
            ", actual=" + std::to_string(schemaId), SOURCEINFO);
    }

    const std::int32_t templateId = m_messageHeaderDecoder.templateId();
    if (templateId == ClusterMembersResponse::SBE_TEMPLATE_ID)
    {
        m_clusterMembersResponseDecoder.wrap(
            buffer,
            offset + MessageHeader::encodedLength(),
            m_messageHeaderDecoder.blockLength(),
            m_messageHeaderDecoder.version());

        const std::int64_t correlationId = m_clusterMembersResponseDecoder.correlationId();
        const std::int32_t leaderMemberId = m_clusterMembersResponseDecoder.leaderMemberId();
        const std::string activeMembers = m_clusterMembersResponseDecoder.activeMembers();
        // Note: skipPassiveFollowers() is not available in C++ codec, skip manually if needed

        m_listener.onClusterMembersResponse(correlationId, leaderMemberId, activeMembers);
    }
    else if (templateId == ClusterMembersExtendedResponse::SBE_TEMPLATE_ID)
    {
        m_clusterMembersExtendedResponseDecoder.wrap(
            buffer,
            offset + MessageHeader::encodedLength(),
            m_messageHeaderDecoder.blockLength(),
            m_messageHeaderDecoder.version());

        const std::int64_t correlationId = m_clusterMembersExtendedResponseDecoder.correlationId();
        const std::int64_t currentTimeNs = m_clusterMembersExtendedResponseDecoder.currentTimeNs();
        const std::int32_t leaderMemberId = m_clusterMembersExtendedResponseDecoder.leaderMemberId();
        const std::int32_t memberId = m_clusterMembersExtendedResponseDecoder.memberId();

        std::vector<ClusterMember> activeMembers;
        auto activeMembersDecoder = m_clusterMembersExtendedResponseDecoder.activeMembers();
        for (auto activeMemberDecoder : activeMembersDecoder)
        {
            const std::int32_t id = activeMemberDecoder.memberId();
            const std::string ingressEndpoint = activeMemberDecoder.ingressEndpoint();
            const std::string consensusEndpoint = activeMemberDecoder.consensusEndpoint();
            const std::string logEndpoint = activeMemberDecoder.logEndpoint();
            const std::string catchupEndpoint = activeMemberDecoder.catchupEndpoint();
            const std::string archiveEndpoint = activeMemberDecoder.archiveEndpoint();
            
            // Build endpoints string
            std::string endpoints = ingressEndpoint + "," + consensusEndpoint + "," +
                                   logEndpoint + "," + catchupEndpoint + "," + archiveEndpoint;

            ClusterMember member(
                id,
                ingressEndpoint,
                consensusEndpoint,
                logEndpoint,
                catchupEndpoint,
                archiveEndpoint,
                endpoints);
            member.isLeader(id == leaderMemberId)
                  .leadershipTermId(activeMemberDecoder.leadershipTermId())
                  .logPosition(activeMemberDecoder.logPosition())
                  .timeOfLastAppendPositionNs(activeMemberDecoder.timeOfLastAppendNs());

            activeMembers.push_back(member);
        }

        // Note: passiveMembers iteration would be similar but we skip them as in Java version
        auto passiveMembersDecoder = m_clusterMembersExtendedResponseDecoder.passiveMembers();
        for (auto passiveMemberDecoder : passiveMembersDecoder)
        {
            // Skip passive members as in Java version
            (void)passiveMemberDecoder; // Suppress unused variable warning
        }

        m_listener.onClusterMembersExtendedResponse(
            correlationId, currentTimeNs, leaderMemberId, memberId, activeMembers);
    }
}

}}

