#pragma once

#include <cstdint>
#include <string>
#include <vector>
#include <memory>
#include "aeron_cluster/CloseReason.h"

namespace aeron { namespace cluster {

// Forward declarations
class ExpandableRingBuffer;

/**
 * Export of consensus module state.
 */
class ConsensusModuleStateExport
{
public:
    /**
     * Timer state export.
     */
    struct TimerStateExport
    {
        std::int64_t correlationId;
        std::int64_t deadline;

        TimerStateExport(std::int64_t correlationId, std::int64_t deadline)
            : correlationId(correlationId), deadline(deadline)
        {
        }
    };

    /**
     * Cluster session state export.
     */
    struct ClusterSessionStateExport
    {
        std::int64_t id;
        std::int64_t correlationId;
        std::int64_t openedLogPosition;
        std::int64_t timeOfLastActivityNs;
        std::int32_t responseStreamId;
        std::string responseChannel;
        CloseReason closeReason;

        ClusterSessionStateExport(
            std::int64_t id,
            std::int64_t correlationId,
            std::int64_t openedLogPosition,
            std::int64_t timeOfLastActivityNs,
            std::int32_t responseStreamId,
            const std::string& responseChannel,
            CloseReason closeReason)
            : id(id),
              correlationId(correlationId),
              openedLogPosition(openedLogPosition),
              timeOfLastActivityNs(timeOfLastActivityNs),
              responseStreamId(responseStreamId),
              responseChannel(responseChannel),
              closeReason(closeReason)
        {
        }
    };

    /**
     * Pending service message tracker state export.
     */
    struct PendingServiceMessageTrackerStateExport
    {
        std::int64_t nextServiceSessionId;
        std::int64_t logServiceSessionId;
        std::int32_t capacity;
        std::int32_t serviceId;
        // TODO: Replace with C++ equivalent of ExpandableRingBuffer
        std::shared_ptr<void> pendingMessages;

        PendingServiceMessageTrackerStateExport(
            std::int64_t nextServiceSessionId,
            std::int64_t logServiceSessionId,
            std::int32_t capacity,
            std::int32_t serviceId,
            std::shared_ptr<void> pendingMessages)
            : nextServiceSessionId(nextServiceSessionId),
              logServiceSessionId(logServiceSessionId),
              capacity(capacity),
              serviceId(serviceId),
              pendingMessages(pendingMessages)
        {
        }
    };

    std::int64_t logRecordingId;
    std::int64_t leadershipTermId;
    std::int64_t expectedAckPosition;
    std::int64_t nextSessionId;
    std::int64_t serviceAckId;
    std::vector<TimerStateExport> timers;
    std::vector<ClusterSessionStateExport> sessions;
    std::vector<PendingServiceMessageTrackerStateExport> pendingMessageTrackers;

    ConsensusModuleStateExport(
        std::int64_t logRecordingId,
        std::int64_t leadershipTermId,
        std::int64_t expectedAckPosition,
        std::int64_t nextSessionId,
        std::int64_t serviceAckId,
        const std::vector<TimerStateExport>& timers,
        const std::vector<ClusterSessionStateExport>& sessions,
        const std::vector<PendingServiceMessageTrackerStateExport>& pendingMessageTrackers)
        : logRecordingId(logRecordingId),
          leadershipTermId(leadershipTermId),
          expectedAckPosition(expectedAckPosition),
          nextSessionId(nextSessionId),
          serviceAckId(serviceAckId),
          timers(timers),
          sessions(sessions),
          pendingMessageTrackers(pendingMessageTrackers)
    {
    }
};

}}

