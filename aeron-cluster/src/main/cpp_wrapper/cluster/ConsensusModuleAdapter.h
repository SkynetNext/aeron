#pragma once
#include <memory>
#include "Subscription.h"
#include "Image.h"
#include "client/ClusterExceptions.h"
#include "FragmentAssembler.h"
#include "ControlledFragmentAssembler.h"
#include "concurrent/logbuffer/Header.h"
#include "concurrent/AtomicBuffer.h"
#include "generated/aeron_cluster_codecs/MessageHeader.h"
#include "generated/aeron_cluster_codecs/SessionMessageHeader.h"
#include "generated/aeron_cluster_codecs/ScheduleTimer.h"
#include "generated/aeron_cluster_codecs/CancelTimer.h"
#include "generated/aeron_cluster_codecs/ServiceAck.h"
#include "generated/aeron_cluster_codecs/CloseSession.h"
#include "generated/aeron_cluster_codecs/ClusterMembersQuery.h"
#include "generated/aeron_cluster_codecs/BooleanType.h"

namespace aeron { namespace cluster
{
using namespace aeron::concurrent;
using namespace aeron::concurrent::logbuffer;
using namespace aeron::cluster::codecs;

class ConsensusModuleAgent; // Forward declaration

class ConsensusModuleAdapter
{
public:
    ConsensusModuleAdapter(
        std::shared_ptr<Subscription> subscription,
        ConsensusModuleAgent& consensusModuleAgent);

    ~ConsensusModuleAdapter() = default; // AutoCloseable equivalent

    void close();

    std::int32_t poll();

private:
    static constexpr std::int32_t FRAGMENT_LIMIT = 25;

    ControlledPollAction onFragment(
        AtomicBuffer& buffer,
        std::int32_t offset,
        std::int32_t length,
        Header& header);

    std::shared_ptr<Subscription> m_subscription;
    ConsensusModuleAgent& m_consensusModuleAgent;
    MessageHeader m_messageHeader;
    SessionMessageHeader m_sessionMessageHeader;
    ScheduleTimer m_scheduleTimer;
    CancelTimer m_cancelTimer;
    ServiceAck m_serviceAck;
    CloseSession m_closeSession;
    ClusterMembersQuery m_clusterMembersQuery;
    ControlledFragmentAssembler m_fragmentAssembler;
};

// Implementation
inline ConsensusModuleAdapter::ConsensusModuleAdapter(
    std::shared_ptr<Subscription> subscription,
    ConsensusModuleAgent& consensusModuleAgent) :
    m_subscription(subscription),
    m_consensusModuleAgent(consensusModuleAgent),
    m_fragmentAssembler([this](AtomicBuffer& buffer, std::int32_t offset, std::int32_t length, Header& header)
    {
        return this->onFragment(buffer, offset, length, header);
    })
{
}

inline void ConsensusModuleAdapter::close()
{
    if (m_subscription)
    {
        m_subscription.reset();
    }
}

inline std::int32_t ConsensusModuleAdapter::poll()
{
    if (m_subscription)
    {
        return m_subscription->controlledPoll(m_fragmentAssembler.handler(), FRAGMENT_LIMIT);
    }
    return 0;
}

// Implementation moved to ConsensusModuleAgent.h to avoid circular dependency
// inline ControlledPollAction ConsensusModuleAdapter::onFragment(...)

}}

