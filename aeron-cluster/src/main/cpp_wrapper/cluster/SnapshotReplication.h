#pragma once

#include <memory>
#include <vector>
#include <cstdint>
#include <string>
#include "archive/client/AeronArchive.h"
#include "archive/codecs/RecordingSignal.h"

namespace aeron { namespace cluster {

// Forward declaration - RecordingLog::Snapshot will be defined in RecordingLog.h
namespace RecordingLog {
    struct Snapshot;
}

class MultipleRecordingReplication;

/**
 * Replication of snapshots.
 */
class SnapshotReplication
{
public:
    SnapshotReplication(
        std::shared_ptr<archive::client::AeronArchive> archive,
        std::int32_t srcControlStreamId,
        const std::string& srcControlChannel,
        const std::string& replicationChannel);

    SnapshotReplication(
        std::shared_ptr<archive::client::AeronArchive> archive,
        std::int32_t srcControlStreamId,
        const std::string& srcControlChannel,
        const std::string& replicationChannel,
        std::int64_t replicationProgressTimeoutNs,
        std::int64_t replicationProgressIntervalNs);

    ~SnapshotReplication();

    void addSnapshot(const RecordingLog::Snapshot& snapshot);

    std::int32_t poll(std::int64_t nowNs);

    void onSignal(
        std::int64_t correlationId,
        std::int64_t recordingId,
        std::int64_t position,
        archive::codecs::RecordingSignal signal);

    bool isComplete() const;

    std::vector<RecordingLog::Snapshot> snapshotsRetrieved() const;

    static RecordingLog::Snapshot retrievedSnapshot(
        const RecordingLog::Snapshot& pending,
        std::int64_t recordingId);

private:
    std::vector<RecordingLog::Snapshot> m_snapshotsPending;
    std::unique_ptr<MultipleRecordingReplication> m_multipleRecordingReplication;
};

}}

