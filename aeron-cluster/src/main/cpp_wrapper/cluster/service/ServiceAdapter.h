#pragma once
#include <memory>
#include <string>

#include "Subscription.h"
#include "FragmentAssembler.h"
#include "concurrent/AtomicBuffer.h"
#include "concurrent/logbuffer/Header.h"
#include "cluster/client/ClusterExceptions.h"
#include "Cluster.h"
#include "generated/aeron_cluster_codecs/MessageHeader.h"
#include "generated/aeron_cluster_codecs/JoinLog.h"
#include "generated/aeron_cluster_codecs/RequestServiceAck.h"
#include "generated/aeron_cluster_codecs/ServiceTerminationPosition.h"
#include "generated/aeron_cluster_codecs/BooleanType.h"

namespace aeron { namespace cluster { namespace service
{
using namespace aeron::concurrent;
using namespace aeron::concurrent::logbuffer;
using namespace aeron::cluster::codecs;

class ClusteredServiceAgent; // Forward declaration

class ServiceAdapter
{
public:
    ServiceAdapter(std::shared_ptr<Subscription> subscription, ClusteredServiceAgent& clusteredServiceAgent);
    ~ServiceAdapter() = default;

    void close();
    int poll();

private:
    void onFragment(AtomicBuffer& buffer, std::int32_t offset, std::int32_t length, Header& header);

    static constexpr int FRAGMENT_LIMIT = 1;

    std::shared_ptr<Subscription> m_subscription;
    ClusteredServiceAgent& m_clusteredServiceAgent;
    FragmentAssembler m_fragmentAssembler;

    MessageHeaderDecoder m_messageHeaderDecoder;
    JoinLogDecoder m_joinLogDecoder;
    RequestServiceAckDecoder m_requestServiceAckDecoder;
    ServiceTerminationPositionDecoder m_serviceTerminationPositionDecoder;
};

}}}

