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

// Implementation
inline ConsensusModuleSnapshotAdapter::ConsensusModuleSnapshotAdapter(
    std::shared_ptr<Image> image,
    ConsensusModuleSnapshotListener& listener) :
    m_image(image),
    m_listener(listener),
    m_fragmentAssembler(*this)
{
}

inline bool ConsensusModuleSnapshotAdapter::isDone() const
{
    return m_isDone;
}

inline std::int32_t ConsensusModuleSnapshotAdapter::poll()
{
    return m_image->controlledPoll(m_fragmentAssembler, FRAGMENT_LIMIT);
}

inline ControlledPollAction ConsensusModuleSnapshotAdapter::onFragment(
    AtomicBuffer& buffer,
    std::int32_t offset,
    std::int32_t length,
    Header& header)
{
    m_messageHeaderDecoder.wrap(buffer, offset);

    const std::int32_t schemaId = m_messageHeaderDecoder.sbeSchemaId();
    if (MessageHeader::sbeSchemaId() != schemaId)
    {
        throw ClusterException(
            "expected schemaId=" + std::to_string(MessageHeader::sbeSchemaId()) +
            ", actual=" + std::to_string(schemaId),
            SOURCEINFO);
    }

    switch (m_messageHeaderDecoder.sbeTemplateId())
    {
        case SessionMessageHeaderDecoder::sbeTemplateId():
        {
            m_sessionMessageHeaderDecoder.wrap(
                buffer,
                offset + MessageHeader::encodedLength(),
                m_messageHeaderDecoder.sbeBlockLength(),
                m_messageHeaderDecoder.sbeVersion());

            m_listener.onLoadPendingMessage(
                m_sessionMessageHeaderDecoder.clusterSessionId(),
                buffer,
                offset,
                length);
            break;
        }

        case SnapshotMarkerDecoder::sbeTemplateId():
        {
            m_snapshotMarkerDecoder.wrap(
                buffer,
                offset + MessageHeader::encodedLength(),
                m_messageHeaderDecoder.sbeBlockLength(),
                m_messageHeaderDecoder.sbeVersion());

            const std::int64_t typeId = m_snapshotMarkerDecoder.typeId();
            if (SNAPSHOT_TYPE_ID != typeId)
            {
                throw ClusterException("unexpected snapshot type: " + std::to_string(typeId), SOURCEINFO);
            }

            switch (m_snapshotMarkerDecoder.mark())
            {
                case SnapshotMarker::Mark::BEGIN:
                {
                    if (m_inSnapshot)
                    {
                        throw ClusterException("already in snapshot", SOURCEINFO);
                    }
                    m_inSnapshot = true;

                    m_listener.onLoadBeginSnapshot(
                        m_snapshotMarkerDecoder.appVersion(),
                        ClusterClock::map(m_snapshotMarkerDecoder.timeUnit()),
                        buffer,
                        offset,
                        length);
                    return ControlledPollAction::CONTINUE;
                }

                case SnapshotMarker::Mark::END:
                {
                    if (!m_inSnapshot)
                    {
                        throw ClusterException("missing begin snapshot", SOURCEINFO);
                    }
                    m_listener.onLoadEndSnapshot(buffer, offset, length);
                    m_isDone = true;
                    return ControlledPollAction::BREAK;
                }

                case SnapshotMarker::Mark::SECTION:
                case SnapshotMarker::Mark::NULL_VAL:
                    break;
            }
            break;
        }

        case ClusterSessionDecoder::sbeTemplateId():
        {
            m_clusterSessionDecoder.wrap(
                buffer,
                offset + MessageHeader::encodedLength(),
                m_messageHeaderDecoder.sbeBlockLength(),
                m_messageHeaderDecoder.sbeVersion());

            m_listener.onLoadClusterSession(
                m_clusterSessionDecoder.clusterSessionId(),
                m_clusterSessionDecoder.correlationId(),
                m_clusterSessionDecoder.openedLogPosition(),
                m_clusterSessionDecoder.timeOfLastActivity(),
                m_clusterSessionDecoder.closeReason(),
                m_clusterSessionDecoder.responseStreamId(),
                m_clusterSessionDecoder.responseChannel(),
                buffer,
                offset,
                length);
            break;
        }

        case TimerDecoder::sbeTemplateId():
        {
            m_timerDecoder.wrap(
                buffer,
                offset + MessageHeader::encodedLength(),
                m_messageHeaderDecoder.sbeBlockLength(),
                m_messageHeaderDecoder.sbeVersion());

            m_listener.onLoadTimer(
                m_timerDecoder.correlationId(),
                m_timerDecoder.deadline(),
                buffer,
                offset,
                length);
            break;
        }

        case ConsensusModuleDecoder::sbeTemplateId():
        {
            m_consensusModuleDecoder.wrap(
                buffer,
                offset + MessageHeader::encodedLength(),
                m_messageHeaderDecoder.sbeBlockLength(),
                m_messageHeaderDecoder.sbeVersion());

            m_listener.onLoadConsensusModuleState(
                m_consensusModuleDecoder.nextSessionId(),
                m_consensusModuleDecoder.nextServiceSessionId(),
                m_consensusModuleDecoder.logServiceSessionId(),
                m_consensusModuleDecoder.pendingMessageCapacity(),
                buffer,
                offset,
                length);
            break;
        }

        case ClusterMembersDecoder::sbeTemplateId():
            // Ignored
            break;

        case PendingMessageTrackerDecoder::sbeTemplateId():
        {
            m_pendingMessageTrackerDecoder.wrap(
                buffer,
                offset + MessageHeader::encodedLength(),
                m_messageHeaderDecoder.sbeBlockLength(),
                m_messageHeaderDecoder.sbeVersion());

            m_listener.onLoadPendingMessageTracker(
                m_pendingMessageTrackerDecoder.nextServiceSessionId(),
                m_pendingMessageTrackerDecoder.logServiceSessionId(),
                m_pendingMessageTrackerDecoder.pendingMessageCapacity(),
                m_pendingMessageTrackerDecoder.serviceId(),
                buffer,
                offset,
                length);
            break;
        }
    }

    return ControlledPollAction::CONTINUE;
}

}}
