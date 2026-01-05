#pragma once
#include <memory>
#include <vector>
#include <string>
#include <functional>
#include "Subscription.h"
#include "../client/AeronCluster.h"
#include "../client/ClusterExceptions.h"
#include "ControlledFragmentAssembler.h"
#include "concurrent/logbuffer/ControlledFragmentHandler.h"
#include "concurrent/logbuffer/Header.h"
#include "concurrent/AtomicBuffer.h"
#include "util/ArrayUtil.h"
#include "generated/aeron_cluster_client/MessageHeader.h"
#include "generated/aeron_cluster_client/SessionConnectRequest.h"
#include "generated/aeron_cluster_client/SessionCloseRequest.h"
#include "generated/aeron_cluster_client/SessionMessageHeader.h"
#include "generated/aeron_cluster_client/SessionKeepAlive.h"
#include "generated/aeron_cluster_client/ChallengeResponse.h"
#include "generated/aeron_cluster_client/AdminRequest.h"

namespace aeron { namespace cluster
{
using namespace aeron::concurrent;
using namespace aeron::concurrent::logbuffer;
using namespace aeron::cluster::codecs;
using namespace aeron::util;

class ConsensusModuleAgent; // Forward declaration

class IngressAdapter
{
public:
    IngressAdapter(
        std::int32_t fragmentPollLimit,
        ConsensusModuleAgent& consensusModuleAgent);

    ~IngressAdapter() = default; // AutoCloseable equivalent

    void close();

    ControlledPollAction onMessage(
        AtomicBuffer& buffer,
        std::int32_t offset,
        std::int32_t length,
        Header& header);

    void connect(
        std::shared_ptr<Subscription> subscription,
        std::shared_ptr<Subscription> ipcSubscription);

    std::int32_t poll();

    void freeSessionBuffer(std::int32_t imageSessionId, bool isIpc);

private:
    std::int32_t m_fragmentPollLimit;
    MessageHeaderDecoder m_messageHeaderDecoder;
    SessionConnectRequestDecoder m_connectRequestDecoder;
    SessionCloseRequestDecoder m_closeRequestDecoder;
    SessionMessageHeaderDecoder m_sessionMessageHeaderDecoder;
    SessionKeepAliveDecoder m_sessionKeepAliveDecoder;
    ChallengeResponseDecoder m_challengeResponseDecoder;
    AdminRequestDecoder m_adminRequestDecoder;
    controlled_poll_fragment_handler_t m_onMessageHandler;
    ControlledFragmentAssembler m_udpFragmentAssembler;
    ControlledFragmentAssembler m_ipcFragmentAssembler;
    ConsensusModuleAgent& m_consensusModuleAgent;
    std::shared_ptr<Subscription> m_subscription;
    std::shared_ptr<Subscription> m_ipcSubscription;
};

}}
