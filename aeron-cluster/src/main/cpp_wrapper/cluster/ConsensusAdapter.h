#pragma once
#include <memory>
#include <vector>
#include <string>
#include "Subscription.h"
#include "client/archive/AeronArchive.h"
#include "client/ClusterExceptions.h"
#include "FragmentAssembler.h"
#include "concurrent/logbuffer/Header.h"
#include "concurrent/AtomicBuffer.h"
#include "util/CloseHelper.h"
#include "generated/aeron_cluster_codecs/MessageHeader.h"
#include "generated/aeron_cluster_codecs/CanvassPosition.h"
#include "generated/aeron_cluster_codecs/RequestVote.h"
#include "generated/aeron_cluster_codecs/Vote.h"
#include "generated/aeron_cluster_codecs/NewLeadershipTerm.h"
#include "generated/aeron_cluster_codecs/AppendPosition.h"
#include "generated/aeron_cluster_codecs/CommitPosition.h"
#include "generated/aeron_cluster_codecs/CatchupPosition.h"
#include "generated/aeron_cluster_codecs/StopCatchup.h"
#include "generated/aeron_cluster_codecs/TerminationPosition.h"
#include "generated/aeron_cluster_codecs/TerminationAck.h"
#include "generated/aeron_cluster_codecs/BackupQuery.h"
#include "generated/aeron_cluster_codecs/ChallengeResponse.h"
#include "generated/aeron_cluster_codecs/HeartbeatRequest.h"
#include "generated/aeron_cluster_codecs/StandbySnapshot.h"
#include "generated/aeron_cluster_codecs/BooleanType.h"
#include "StandbySnapshotEntry.h"

namespace aeron { namespace cluster
{
using namespace aeron::archive::client;
using namespace aeron::concurrent;
using namespace aeron::concurrent::logbuffer;
using namespace aeron::cluster::codecs;
using namespace aeron::util;

class ConsensusModuleAgent; // Forward declaration

class ConsensusAdapter
{
public:
    static constexpr std::int32_t FRAGMENT_LIMIT = 10;

    ConsensusAdapter(
        std::shared_ptr<Subscription> subscription,
        ConsensusModuleAgent& consensusModuleAgent);

    ~ConsensusAdapter() = default; // AutoCloseable equivalent

    void close();

    std::int32_t poll();

    std::int32_t poll(std::int32_t limit);

    void onFragment(
        AtomicBuffer& buffer,
        std::int32_t offset,
        std::int32_t length,
        Header& header);

private:
    MessageHeader m_messageHeader;
    CanvassPosition m_canvassPosition;
    RequestVote m_requestVote;
    Vote m_vote;
    NewLeadershipTerm m_newLeadershipTerm;
    AppendPosition m_appendPosition;
    CommitPosition m_commitPosition;
    CatchupPosition m_catchupPosition;
    StopCatchup m_stopCatchup;
    TerminationPosition m_terminationPosition;
    TerminationAck m_terminationAck;
    BackupQuery m_backupQuery;
    ChallengeResponse m_challengeResponse;
    HeartbeatRequest m_heartbeatRequest;
    StandbySnapshot m_standbySnapshot;
    FragmentAssembler m_fragmentAssembler;
    std::shared_ptr<Subscription> m_subscription;
    ConsensusModuleAgent& m_consensusModuleAgent;
};

// Implementation
inline ConsensusAdapter::ConsensusAdapter(
    std::shared_ptr<Subscription> subscription,
    ConsensusModuleAgent& consensusModuleAgent) :
    m_fragmentAssembler([this](AtomicBuffer& buffer, std::int32_t offset, std::int32_t length, Header& header)
    {
        this->onFragment(buffer, offset, length, header);
    }),
    m_subscription(subscription),
    m_consensusModuleAgent(consensusModuleAgent)
{
}

inline void ConsensusAdapter::close()
{
    CloseHelper::close(m_subscription);
}

inline std::int32_t ConsensusAdapter::poll()
{
    return m_subscription->poll(m_fragmentAssembler.handler(), FRAGMENT_LIMIT);
}

inline std::int32_t ConsensusAdapter::poll(std::int32_t limit)
{
    return m_subscription->poll(m_fragmentAssembler.handler(), limit);
}

// Implementation moved to ConsensusModuleAgent.h to avoid circular dependency
// inline void ConsensusAdapter::onFragment(...)

}}

