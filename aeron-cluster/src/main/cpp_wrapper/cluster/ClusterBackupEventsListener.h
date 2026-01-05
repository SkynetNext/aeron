#pragma once
#include <vector>
#include <memory>
#include "ClusterMember.h"
#include "RecordingLog.h"

namespace aeron { namespace cluster
{

/**
 * Listener which can be registered via ClusterBackup.Context for tracking backup progress.
 */
class ClusterBackupEventsListener
{
public:
    virtual ~ClusterBackupEventsListener() = default;

    /**
     * Backup has moved into backup query state. Backup process has been started.
     */
    virtual void onBackupQuery() = 0;

    /**
     * Possible failure of cluster leader detected.
     *
     * @param ex the underlying exception.
     */
    virtual void onPossibleFailure(const std::exception& ex) = 0;

    /**
     * Backup response was received for a backup query.
     *
     * @param clusterMembers      in the backup response.
     * @param logSourceMember     to be used to replicate data from.
     * @param snapshotsToRetrieve snapshots to be retrieved.
     */
    virtual void onBackupResponse(
        const std::vector<ClusterMember>& clusterMembers,
        const ClusterMember& logSourceMember,
        const std::vector<RecordingLog::Snapshot>& snapshotsToRetrieve) = 0;

    /**
     * Updated recording log.
     *
     * @param recordingLog       that was updated.
     * @param snapshotsRetrieved the snapshots that were retrieved.
     */
    virtual void onUpdatedRecordingLog(
        const RecordingLog& recordingLog,
        const std::vector<RecordingLog::Snapshot>& snapshotsRetrieved) = 0;

    /**
     * Update to the live log position as recorded to the local archive.
     *
     * @param recordingId           of the live log.
     * @param recordingPosCounterId RecordingPos counter id for the live log.
     * @param logPosition           of the live log.
     */
    virtual void onLiveLogProgress(
        std::int64_t recordingId,
        std::int64_t recordingPosCounterId,
        std::int64_t logPosition) = 0;
};

}}

