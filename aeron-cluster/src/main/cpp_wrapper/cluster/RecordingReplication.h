#pragma once

#include <memory>
#include <cstdint>
#include <string>
#include "archive/client/AeronArchive.h"
#include "archive/codecs/RecordingSignal.h"
#include "../client/ClusterExceptions.h"
#include "util/Exceptions.h"
#include "concurrent/status/CountersReader.h"

namespace aeron { namespace cluster {

using namespace aeron::archive::client;
using namespace aeron::archive::codecs;

/**
 * Replication of a single recording.
 */
class RecordingReplication
{
public:
    RecordingReplication(
        std::shared_ptr<AeronArchive> archive,
        std::int64_t srcRecordingId,
        const std::string& srcArchiveChannel,
        std::int32_t srcControlStreamId,
        const ReplicationParams& replicationParams,
        std::int64_t progressCheckTimeoutNs,
        std::int64_t progressCheckIntervalNs,
        std::int64_t nowNs);

    ~RecordingReplication();

    void close();

    std::int32_t poll(std::int64_t nowNs);

    std::int64_t position() const;

    std::int64_t recordingId() const;

    bool hasReplicationEnded() const;

    bool hasSynced() const;

    bool hasStopped() const;

    void onSignal(
        std::int64_t correlationId,
        std::int64_t recordingId,
        std::int64_t position,
        RecordingSignal signal);

    std::string srcArchiveChannel() const;

    std::string toString() const;

private:
    bool pollDstRecordingPosition();

    std::int64_t m_replicationId;
    std::int64_t m_stopPosition;
    std::int64_t m_progressCheckTimeoutNs;
    std::int64_t m_progressCheckIntervalNs;
    std::string m_srcArchiveChannel;

    std::int32_t m_recordingPositionCounterId = 0; // NULL_COUNTER_ID
    std::int64_t m_recordingId = 0; // NULL_VALUE
    std::int64_t m_position = 0; // NULL_POSITION

    std::int64_t m_progressDeadlineNs;
    std::int64_t m_progressCheckDeadlineNs;
    std::shared_ptr<AeronArchive> m_archive;
    RecordingSignal m_lastRecordingSignal = RecordingSignal::NULL_VAL;

    bool m_hasReplicationEnded = false;
    bool m_hasSynced = false;
    bool m_hasStopped = false;
};

}}

