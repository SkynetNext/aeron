#pragma once
#include <memory>
#include <vector>
#include <string>
#include <functional>
#include "Subscription.h"
#include "client/AeronCluster.h"
#include "client/ClusterExceptions.h"
#include "ControlledFragmentAssembler.h"
// ControlledFragmentHandler is replaced by ControlledPollAction in C++
// #include "concurrent/logbuffer/ControlledFragmentHandler.h"
#include "concurrent/logbuffer/Header.h"
#include "concurrent/AtomicBuffer.h"
#include "generated/aeron_cluster_codecs/MessageHeader.h"
#include "generated/aeron_cluster_codecs/SessionConnectRequest.h"
#include "generated/aeron_cluster_codecs/SessionCloseRequest.h"
#include "generated/aeron_cluster_codecs/SessionMessageHeader.h"
#include "generated/aeron_cluster_codecs/SessionKeepAlive.h"
#include "generated/aeron_cluster_codecs/ChallengeResponse.h"
#include "generated/aeron_cluster_codecs/AdminRequest.h"

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
    MessageHeader m_messageHeader;
    SessionConnectRequest m_connectRequest;
    SessionCloseRequest m_closeRequest;
    SessionMessageHeader m_sessionMessageHeader;
    SessionKeepAlive m_sessionKeepAlive;
    ChallengeResponse m_challengeResponse;
    AdminRequest m_adminRequest;
    controlled_poll_fragment_handler_t m_onMessageHandler;
    ControlledFragmentAssembler m_udpFragmentAssembler;
    ControlledFragmentAssembler m_ipcFragmentAssembler;
    ConsensusModuleAgent& m_consensusModuleAgent;
    std::shared_ptr<Subscription> m_subscription;
    std::shared_ptr<Subscription> m_ipcSubscription;
};

// Implementation
inline IngressAdapter::IngressAdapter(
    std::int32_t fragmentPollLimit,
    ConsensusModuleAgent& consensusModuleAgent) :
    m_fragmentPollLimit(fragmentPollLimit),
    m_consensusModuleAgent(consensusModuleAgent),
    m_onMessageHandler([this](AtomicBuffer& buffer, std::int32_t offset, std::int32_t length, Header& header)
    {
        return this->onMessage(buffer, offset, length, header);
    }),
    m_udpFragmentAssembler(m_onMessageHandler),
    m_ipcFragmentAssembler(m_onMessageHandler)
{
}

inline void IngressAdapter::close()
{
    std::shared_ptr<Subscription> subscription = m_subscription;
    std::shared_ptr<Subscription> ipcSubscription = m_ipcSubscription;

    m_subscription.reset();
    m_ipcSubscription.reset();

    if (subscription)
    {
        subscription.reset();
    }

    if (ipcSubscription)
    {
        ipcSubscription.reset();
    }
}

// Implementation moved to ConsensusModuleAgent.h to avoid circular dependency
// inline ControlledPollAction IngressAdapter::onMessage(...)

inline void IngressAdapter::connect(
    std::shared_ptr<Subscription> subscription,
    std::shared_ptr<Subscription> ipcSubscription)
{
    m_subscription = subscription;
    m_ipcSubscription = ipcSubscription;
}

inline std::int32_t IngressAdapter::poll()
{
    std::int32_t fragmentsRead = 0;

    if (m_subscription)
    {
        fragmentsRead += m_subscription->controlledPoll(m_udpFragmentAssembler.handler(), m_fragmentPollLimit);
    }

    if (m_ipcSubscription)
    {
        fragmentsRead += m_ipcSubscription->controlledPoll(m_ipcFragmentAssembler.handler(), m_fragmentPollLimit);
    }

    return fragmentsRead;
}

inline void IngressAdapter::freeSessionBuffer(std::int32_t imageSessionId, bool isIpc)
{
    if (isIpc)
    {
        m_ipcFragmentAssembler.deleteSessionBuffer(imageSessionId);
    }
    else
    {
        m_udpFragmentAssembler.deleteSessionBuffer(imageSessionId);
    }
}

}}
