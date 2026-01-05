#pragma once
#include <memory>
#include "Aeron.h"
#include "BufferBuilder.h"
#include "Image.h"
#include "Subscription.h"
#include "../client/ClusterExceptions.h"
#include "../service/ClusterClock.h"
#include "concurrent/logbuffer/ControlledFragmentHandler.h"
#include "concurrent/logbuffer/Header.h"
#include "concurrent/AtomicBuffer.h"
#include "util/BitUtil.h"
#include "util/CloseHelper.h"
#include "util/Exceptions.h"
#include "concurrent/logbuffer/FrameDescriptor.h"
#include "protocol/DataHeaderFlyweight.h"
#include "generated/aeron_cluster_client/MessageHeader.h"
#include "generated/aeron_cluster_client/SessionOpenEvent.h"
#include "generated/aeron_cluster_client/SessionCloseEvent.h"
#include "generated/aeron_cluster_client/SessionMessageHeader.h"
#include "generated/aeron_cluster_client/TimerEvent.h"
#include "generated/aeron_cluster_client/ClusterActionRequest.h"
#include "generated/aeron_cluster_client/NewLeadershipTermEvent.h"

namespace aeron { namespace cluster
{
using namespace aeron::concurrent;
using namespace aeron::concurrent::logbuffer;
using namespace aeron::cluster::codecs;
using namespace aeron::util;

class ConsensusModuleAgent; // Forward declaration

class LogAdapter : public ControlledFragmentHandler
{
public:
    LogAdapter(ConsensusModuleAgent& consensusModuleAgent, std::int32_t fragmentLimit);

    std::int64_t disconnect(const exception_handler_t& errorHandler);

    void disconnect(const exception_handler_t& errorHandler, std::int64_t maxLogPosition);

    std::shared_ptr<Subscription> subscription() const;

    ConsensusModuleAgent& consensusModuleAgent() const;

    std::int64_t position() const;

    std::int32_t poll(std::int64_t boundPosition);

    bool isImageClosed() const;

    bool isLogEndOfStream() const;

    bool isLogEndOfStreamAt(std::int64_t position) const;

    std::shared_ptr<Image> image() const;

    void image(std::shared_ptr<Image> image);

    void asyncRemoveDestination(const std::string& destination);

    ControlledPollAction onFragment(
        AtomicBuffer& buffer,
        std::int32_t offset,
        std::int32_t length,
        Header& header) override;

private:
    ControlledPollAction onMessage(
        AtomicBuffer& buffer,
        std::int32_t offset,
        std::int32_t length,
        Header& header);

    std::int32_t m_fragmentLimit;
    std::int64_t m_logPosition = 0;
    std::shared_ptr<Image> m_image;
    ConsensusModuleAgent& m_consensusModuleAgent;
    BufferBuilder m_builder;
    MessageHeaderDecoder m_messageHeaderDecoder;
    SessionOpenEventDecoder m_sessionOpenEventDecoder;
    SessionCloseEventDecoder m_sessionCloseEventDecoder;
    SessionMessageHeaderDecoder m_sessionHeaderDecoder;
    TimerEventDecoder m_timerEventDecoder;
    ClusterActionRequestDecoder m_clusterActionRequestDecoder;
    NewLeadershipTermEventDecoder m_newLeadershipTermEventDecoder;
};

}}

