#pragma once
#include <memory>
#include <string>

#include "Image.h"
#include "BufferBuilder.h"
#include "concurrent/AtomicBuffer.h"
#include "concurrent/logbuffer/ControlledFragmentHandler.h"
#include "concurrent/logbuffer/Header.h"
#include "concurrent/logbuffer/FrameDescriptor.h"
#include "protocol/DataHeaderFlyweight.h"
#include "util/BitUtil.h"
#include "client/AeronCluster.h"
#include "client/ClusterExceptions.h"
#include "ClusterClock.h"
#include "generated/aeron_cluster_codecs/MessageHeader.h"
#include "generated/aeron_cluster_codecs/SessionMessageHeader.h"
#include "generated/aeron_cluster_codecs/TimerEvent.h"
#include "generated/aeron_cluster_codecs/SessionOpenEvent.h"
#include "generated/aeron_cluster_codecs/SessionCloseEvent.h"
#include "generated/aeron_cluster_codecs/ClusterActionRequest.h"
#include "generated/aeron_cluster_codecs/NewLeadershipTermEvent.h"

namespace aeron { namespace cluster { namespace service
{
using namespace aeron::concurrent;
using namespace aeron::concurrent::logbuffer;
using namespace aeron::cluster::codecs;
using namespace aeron::util;

class ClusteredServiceAgent; // Forward declaration

class BoundedLogAdapter : public ControlledFragmentHandler
{
public:
    BoundedLogAdapter(ClusteredServiceAgent& agent, std::int32_t fragmentLimit);
    ~BoundedLogAdapter() = default;

    void close();
    void maxLogPosition(std::int64_t position);
    bool isDone();
    void image(std::shared_ptr<Image> image);
    std::shared_ptr<Image> image();
    int poll(std::int64_t limit);

    ControlledPollAction onFragment(AtomicBuffer& buffer, std::int32_t offset, std::int32_t length, Header& header) override;

private:
    ControlledPollAction onMessage(AtomicBuffer& buffer, std::int32_t offset, std::int32_t length, Header& header);

    std::int32_t m_fragmentLimit;
    std::int64_t m_maxLogPosition = 0;
    std::shared_ptr<Image> m_image;
    ClusteredServiceAgent& m_agent;
    BufferBuilder m_builder;
    MessageHeaderDecoder m_messageHeaderDecoder;
    SessionMessageHeaderDecoder m_sessionHeaderDecoder;
    TimerEventDecoder m_timerEventDecoder;
    SessionOpenEventDecoder m_openEventDecoder;
    SessionCloseEventDecoder m_closeEventDecoder;
    ClusterActionRequestDecoder m_actionRequestDecoder;
    NewLeadershipTermEventDecoder m_newLeadershipTermEventDecoder;
    
    // CLUSTER_ACTION_FLAGS_DEFAULT from ConsensusModule
    static constexpr std::int32_t CLUSTER_ACTION_FLAGS_DEFAULT = 0;
};

}}}

