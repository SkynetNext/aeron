#pragma once

#include <memory>
#include <vector>
#include <cstdint>
#include <string>
#include "client/archive/AeronArchive.h"
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

// Implementation
inline SnapshotReplication::SnapshotReplication(
    std::shared_ptr<archive::client::AeronArchive> archive,
    std::int32_t srcControlStreamId,
    const std::string& srcControlChannel,
    const std::string& replicationChannel) :
    SnapshotReplication(
        archive,
        srcControlStreamId,
        srcControlChannel,
        replicationChannel,
        std::chrono::seconds(10).count() * 1000000000LL, // 10 seconds in nanoseconds
        std::chrono::seconds(1).count() * 1000000000LL) // 1 second in nanoseconds
{
}

inline SnapshotReplication::SnapshotReplication(
    std::shared_ptr<archive::client::AeronArchive> archive,
    std::int32_t srcControlStreamId,
    const std::string& srcControlChannel,
    const std::string& replicationChannel,
    std::int64_t replicationProgressTimeoutNs,
    std::int64_t replicationProgressIntervalNs) :
    m_multipleRecordingReplication(
        MultipleRecordingReplication::newInstance(
            archive,
            srcControlStreamId,
            srcControlChannel,
            replicationChannel,
            replicationProgressTimeoutNs,
            replicationProgressIntervalNs))
{
}

inline SnapshotReplication::~SnapshotReplication()
{
    if (m_multipleRecordingReplication)
    {
        m_multipleRecordingReplication->close();
    }
}

inline void SnapshotReplication::addSnapshot(const RecordingLog::Snapshot& snapshot)
{
    m_snapshotsPending.push_back(snapshot);
    m_multipleRecordingReplication->addRecording(
        snapshot.recordingId,
        aeron::NULL_VALUE, // dstRecordingId
        aeron::NULL_VALUE); // stopPosition
}

inline std::int32_t SnapshotReplication::poll(std::int64_t nowNs)
{
    return m_multipleRecordingReplication->poll(nowNs);
}

inline void SnapshotReplication::onSignal(
    std::int64_t correlationId,
    std::int64_t recordingId,
    std::int64_t position,
    archive::codecs::RecordingSignal signal)
{
    m_multipleRecordingReplication->onSignal(correlationId, recordingId, position, signal);
}

inline bool SnapshotReplication::isComplete() const
{
    return m_multipleRecordingReplication->isComplete();
}

inline std::vector<RecordingLog::Snapshot> SnapshotReplication::snapshotsRetrieved() const
{
    std::vector<RecordingLog::Snapshot> result;
    result.reserve(m_snapshotsPending.size());
    
    for (const auto& pending : m_snapshotsPending)
    {
        const std::int64_t dstRecordingId = m_multipleRecordingReplication->completedDstRecordingId(pending.recordingId);
        if (aeron::NULL_VALUE != dstRecordingId)
        {
            result.push_back(retrievedSnapshot(pending, dstRecordingId));
        }
    }
    
    return result;
}

inline RecordingLog::Snapshot SnapshotReplication::retrievedSnapshot(
    const RecordingLog::Snapshot& pending,
    std::int64_t recordingId)
{
    return RecordingLog::Snapshot(
        recordingId,
        pending.leadershipTermId,
        pending.termBaseLogPosition,
        pending.logPosition,
        pending.timestamp,
        pending.serviceId);
}

}}
