#pragma once
#include <memory>
#include <vector>
#include <string>
#include "Subscription.h"
#include "archive/client/AeronArchive.h"
#include "../client/ClusterExceptions.h"
#include "FragmentAssembler.h"
#include "concurrent/logbuffer/Header.h"
#include "concurrent/AtomicBuffer.h"
#include "util/CloseHelper.h"
#include "util/ArrayUtil.h"
#include "generated/aeron_cluster_client/MessageHeader.h"
#include "generated/aeron_cluster_client/CanvassPosition.h"
#include "generated/aeron_cluster_client/RequestVote.h"
#include "generated/aeron_cluster_client/Vote.h"
#include "generated/aeron_cluster_client/NewLeadershipTerm.h"
#include "generated/aeron_cluster_client/AppendPosition.h"
#include "generated/aeron_cluster_client/CommitPosition.h"
#include "generated/aeron_cluster_client/CatchupPosition.h"
#include "generated/aeron_cluster_client/StopCatchup.h"
#include "generated/aeron_cluster_client/TerminationPosition.h"
#include "generated/aeron_cluster_client/TerminationAck.h"
#include "generated/aeron_cluster_client/BackupQuery.h"
#include "generated/aeron_cluster_client/ChallengeResponse.h"
#include "generated/aeron_cluster_client/HeartbeatRequest.h"
#include "generated/aeron_cluster_client/StandbySnapshot.h"
#include "generated/aeron_cluster_client/BooleanType.h"
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
    MessageHeaderDecoder m_messageHeaderDecoder;
    CanvassPositionDecoder m_canvassPositionDecoder;
    RequestVoteDecoder m_requestVoteDecoder;
    VoteDecoder m_voteDecoder;
    NewLeadershipTermDecoder m_newLeadershipTermDecoder;
    AppendPositionDecoder m_appendPositionDecoder;
    CommitPositionDecoder m_commitPositionDecoder;
    CatchupPositionDecoder m_catchupPositionDecoder;
    StopCatchupDecoder m_stopCatchupDecoder;
    TerminationPositionDecoder m_terminationPositionDecoder;
    TerminationAckDecoder m_terminationAckDecoder;
    BackupQueryDecoder m_backupQueryDecoder;
    ChallengeResponseDecoder m_challengeResponseDecoder;
    HeartbeatRequestDecoder m_heartbeatRequestDecoder;
    StandbySnapshotDecoder m_standbySnapshotDecoder;
    FragmentAssembler m_fragmentAssembler;
    std::shared_ptr<Subscription> m_subscription;
    ConsensusModuleAgent& m_consensusModuleAgent;
};

}}

