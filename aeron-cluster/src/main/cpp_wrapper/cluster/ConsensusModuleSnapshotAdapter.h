#pragma once

#include <memory>
#include <cstdint>
#include "Image.h"
#include "ImageControlledFragmentAssembler.h"
#include "../client/ClusterExceptions.h"
#include "generated/aeron_cluster_client/MessageHeader.h"
#include "generated/aeron_cluster_client/SnapshotMarker.h"
#include "generated/aeron_cluster_client/ClusterSession.h"
#include "generated/aeron_cluster_client/SessionMessageHeader.h"
#include "generated/aeron_cluster_client/Timer.h"
#include "generated/aeron_cluster_client/ConsensusModule.h"
#include "generated/aeron_cluster_client/ClusterMembers.h"
#include "generated/aeron_cluster_client/PendingMessageTracker.h"
#include "concurrent/logbuffer/ControlledFragmentHandler.h"
#include "concurrent/logbuffer/Header.h"
#include "util/DirectBuffer.h"
#include "../service/ClusterClock.h"
#include "ConsensusModuleSnapshotListener.h"

namespace aeron { namespace cluster {

using namespace aeron::concurrent::logbuffer;

/**
 * Adapter for loading consensus module snapshots.
 */
class ConsensusModuleSnapshotAdapter : public ControlledFragmentHandler
{
public:
    static constexpr std::int32_t FRAGMENT_LIMIT = 10;
    // SNAPSHOT_TYPE_ID from ConsensusModule::Configuration
    static constexpr std::int64_t SNAPSHOT_TYPE_ID = 1;

    ConsensusModuleSnapshotAdapter(
        std::shared_ptr<Image> image,
        ConsensusModuleSnapshotListener& listener);

    bool isDone() const;

    std::int32_t poll();

    ControlledPollAction onFragment(
        AtomicBuffer& buffer,
        std::int32_t offset,
        std::int32_t length,
        Header& header) override;

private:
    bool m_inSnapshot = false;
    bool m_isDone = false;

    MessageHeaderDecoder m_messageHeaderDecoder;
    SnapshotMarkerDecoder m_snapshotMarkerDecoder;
    ClusterSessionDecoder m_clusterSessionDecoder;
    SessionMessageHeaderDecoder m_sessionMessageHeaderDecoder;
    TimerDecoder m_timerDecoder;
    ConsensusModuleDecoder m_consensusModuleDecoder;
    ClusterMembersDecoder m_clusterMembersDecoder;
    PendingMessageTrackerDecoder m_pendingMessageTrackerDecoder;
    ImageControlledFragmentAssembler m_fragmentAssembler;
    std::shared_ptr<Image> m_image;
    ConsensusModuleSnapshotListener& m_listener;
};

}}

