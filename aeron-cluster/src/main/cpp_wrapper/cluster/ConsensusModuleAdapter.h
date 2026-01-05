#pragma once
#include <memory>
#include "Subscription.h"
#include "../client/ClusterExceptions.h"
#include "FragmentAssembler.h"
#include "ControlledFragmentAssembler.h"
#include "concurrent/logbuffer/ControlledFragmentHandler.h"
#include "concurrent/logbuffer/Header.h"
#include "concurrent/AtomicBuffer.h"
#include "generated/aeron_cluster_client/MessageHeader.h"
#include "generated/aeron_cluster_client/SessionMessageHeader.h"
#include "generated/aeron_cluster_client/ScheduleTimer.h"
#include "generated/aeron_cluster_client/CancelTimer.h"
#include "generated/aeron_cluster_client/ServiceAck.h"
#include "generated/aeron_cluster_client/CloseSession.h"
#include "generated/aeron_cluster_client/ClusterMembersQuery.h"
#include "generated/aeron_cluster_client/BooleanType.h"

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
    MessageHeaderDecoder m_messageHeaderDecoder;
    SessionMessageHeaderDecoder m_sessionMessageHeaderDecoder;
    ScheduleTimerDecoder m_scheduleTimerDecoder;
    CancelTimerDecoder m_cancelTimerDecoder;
    ServiceAckDecoder m_serviceAckDecoder;
    CloseSessionDecoder m_closeSessionDecoder;
    ClusterMembersQueryDecoder m_clusterMembersQueryDecoder;
    ControlledFragmentAssembler m_fragmentAssembler;
};

}}

