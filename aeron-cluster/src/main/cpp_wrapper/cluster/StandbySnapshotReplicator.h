#pragma once

#include <memory>
#include <vector>
#include <unordered_map>
#include <string>
#include <algorithm>
#include <cstdint>
#include "archive/client/AeronArchive.h"
#include "archive/codecs/RecordingSignal.h"
#include "Counter.h"
#include "../client/ClusterExceptions.h"
#include "util/Exceptions.h"
#include "util/CloseHelper.h"
#include "ChannelUri.h"
#include "MultipleRecordingReplication.h"

namespace aeron { namespace cluster {

using namespace aeron::archive::client;
using namespace aeron::archive::codecs;

// Forward declarations
class RecordingLog;
class ConsensusModuleAgent;

/**
 * Replicator for standby snapshots.
 */
class StandbySnapshotReplicator
{
public:
    /**
     * Snapshot replication entry.
     */
    struct SnapshotReplicationEntry
    {
        std::string endpoint;
        std::int64_t logPosition;
        std::vector<RecordingLog::Entry> recordingLogEntries;

        SnapshotReplicationEntry(
            const std::string& endpoint,
            std::int64_t logPosition,
            const std::vector<RecordingLog::Entry>& entries)
            : endpoint(endpoint), logPosition(logPosition), recordingLogEntries(entries)
        {
        }
    };

    static std::unique_ptr<StandbySnapshotReplicator> newInstance(
        std::int32_t memberId,
        const archive::client::AeronArchive::Context& archiveCtx,
        RecordingLog& recordingLog,
        std::int32_t serviceCount,
        const std::string& archiveControlChannel,
        std::int32_t archiveControlStreamId,
        const std::string& replicationChannel,
        std::int32_t fileSyncLevel,
        std::shared_ptr<Counter> snapshotCounter);

    ~StandbySnapshotReplicator();

    std::int32_t poll(std::int64_t nowNs);

    bool isComplete() const;

    void onSignal(
        std::int64_t controlSessionId,
        std::int64_t correlationId,
        std::int64_t recordingId,
        std::int64_t subscriptionId,
        std::int64_t position,
        RecordingSignal signal);

    void close();

private:
    StandbySnapshotReplicator(
        std::int32_t memberId,
        std::shared_ptr<AeronArchive> archive,
        RecordingLog& recordingLog,
        std::int32_t serviceCount,
        const std::string& archiveControlChannel,
        std::int32_t archiveControlStreamId,
        const std::string& replicationChannel,
        std::int32_t fileSyncLevel,
        std::shared_ptr<Counter> snapshotCounter);

    std::vector<SnapshotReplicationEntry> computeSnapshotsToReplicate();

    static int compareTo(const SnapshotReplicationEntry& a, const SnapshotReplicationEntry& b);

    void logReplicationEnded(
        const std::string& controlUri,
        std::int64_t srcRecordingId,
        std::int64_t dstRecordingId,
        std::int64_t position,
        bool hasSynced);

    std::unordered_map<std::string, std::vector<RecordingLog::Entry>> filterByExistingRecordingLogEntries(
        const std::unordered_map<std::string, std::vector<RecordingLog::Entry>>& standbySnapshotsByEndpoint);

    std::int32_t m_memberId;
    std::shared_ptr<AeronArchive> m_archive;
    RecordingLog& m_recordingLog;
    std::int32_t m_serviceCount;
    std::string m_archiveControlChannel;
    std::int32_t m_archiveControlStreamId;
    std::string m_replicationChannel;
    std::int32_t m_fileSyncLevel;
    std::shared_ptr<Counter> m_snapshotCounter;
    std::unordered_map<std::string, std::string> m_errorsByEndpoint;
    std::unique_ptr<MultipleRecordingReplication> m_recordingReplication;
    std::vector<SnapshotReplicationEntry> m_snapshotsToReplicate;
    SnapshotReplicationEntry* m_currentSnapshotToReplicate = nullptr;
    bool m_isComplete = false;
};

}}

