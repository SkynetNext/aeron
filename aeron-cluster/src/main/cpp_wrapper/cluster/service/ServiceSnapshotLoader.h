#pragma once
#include <memory>
#include <string>
#include <chrono>

#include "Image.h"
#include "ImageControlledFragmentAssembler.h"
#include "concurrent/AtomicBuffer.h"
#include "concurrent/logbuffer/ControlledFragmentHandler.h"
#include "concurrent/logbuffer/Header.h"
#include "client/ClusterExceptions.h"
#include "ClusterClock.h"
#include "generated/aeron_cluster_codecs/MessageHeader.h"
#include "generated/aeron_cluster_codecs/SnapshotMarker.h"
#include "generated/aeron_cluster_codecs/ClientSession.h"

namespace aeron { namespace cluster { namespace service
{
using namespace aeron::concurrent;
using namespace aeron::concurrent::logbuffer;
using namespace aeron::cluster::codecs;

class ClusteredServiceAgent; // Forward declaration

class ServiceSnapshotLoader : public ControlledFragmentHandler
{
public:
    ServiceSnapshotLoader(std::shared_ptr<Image> image, ClusteredServiceAgent& agent);

    bool isDone();
    std::int32_t appVersion();
    std::chrono::milliseconds::rep timeUnit();

    int poll();

    ControlledPollAction onFragment(AtomicBuffer& buffer, std::int32_t offset, std::int32_t length, Header& header) override;

private:
    static constexpr int FRAGMENT_LIMIT = 10;
    // SNAPSHOT_TYPE_ID from ClusteredServiceContainer::Configuration
    static constexpr std::int64_t SNAPSHOT_TYPE_ID = 2;

    bool m_inSnapshot = false;
    bool m_isDone = false;
    std::int32_t m_appVersion = 0;
    std::chrono::milliseconds::rep m_timeUnit = 0;

    MessageHeaderDecoder m_messageHeaderDecoder;
    SnapshotMarkerDecoder m_snapshotMarkerDecoder;
    ClientSessionDecoder m_clientSessionDecoder;
    ImageControlledFragmentAssembler m_fragmentAssembler;
    std::shared_ptr<Image> m_image;
    ClusteredServiceAgent& m_agent;
};

}}}

