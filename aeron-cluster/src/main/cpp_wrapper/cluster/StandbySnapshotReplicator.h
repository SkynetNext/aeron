#pragma once

#include <memory>
#include <vector>
#include <unordered_map>
#include <string>
#include <algorithm>
#include <cstdint>
#include "archive/client/AeronArchive.h"
#include "archive/codecs/RecordingSignal.h"
#include "archive/client/ArchiveExceptions.h"
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
    
    // Event listener wrapper for MultipleRecordingReplication
    struct EventListenerWrapper : public MultipleRecordingReplication::EventListener
    {
        StandbySnapshotReplicator* m_replicator;
        
        explicit EventListenerWrapper(StandbySnapshotReplicator* r) : m_replicator(r) {}
        
        void onReplicationEnded(
            const std::string& controlUri,
            std::int64_t srcRecordingId,
            std::int64_t dstRecordingId,
            std::int64_t position,
            bool hasSynced) override
        {
            m_replicator->logReplicationEnded(controlUri, srcRecordingId, dstRecordingId, position, hasSynced);
        }
    };
    
    EventListenerWrapper m_eventListenerWrapper;
};

// Implementation
inline std::unique_ptr<StandbySnapshotReplicator> StandbySnapshotReplicator::newInstance(
    std::int32_t memberId,
    const archive::client::AeronArchive::Context& archiveCtx,
    RecordingLog& recordingLog,
    std::int32_t serviceCount,
    const std::string& archiveControlChannel,
    std::int32_t archiveControlStreamId,
    const std::string& replicationChannel,
    std::int32_t fileSyncLevel,
    std::shared_ptr<Counter> snapshotCounter)
{
    auto archiveCtxClone = archiveCtx;
    archiveCtxClone.errorHandler(nullptr);
    
    // Create replicator first to capture it in lambda
    std::shared_ptr<StandbySnapshotReplicator> replicatorShared;
    
    // Set up recording signal consumer before connecting
    archiveCtxClone.recordingSignalConsumer([&replicatorShared](
        std::int64_t controlSessionId,
        std::int64_t correlationId,
        std::int64_t recordingId,
        std::int64_t subscriptionId,
        std::int64_t position,
        RecordingSignal signal)
    {
        if (replicatorShared)
        {
            replicatorShared->onSignal(controlSessionId, correlationId, recordingId, subscriptionId, position, signal);
        }
    });
    
    auto archive = AeronArchive::connect(archiveCtxClone);
    
    auto replicator = std::unique_ptr<StandbySnapshotReplicator>(
        new StandbySnapshotReplicator(
            memberId,
            archive,
            recordingLog,
            serviceCount,
            archiveControlChannel,
            archiveControlStreamId,
            replicationChannel,
            fileSyncLevel,
            snapshotCounter));
    
    // Store shared pointer for lambda capture
    replicatorShared = std::shared_ptr<StandbySnapshotReplicator>(
        replicator.get(), 
        [](StandbySnapshotReplicator*){ /* no-op deleter, owned by unique_ptr */ });
    
    return replicator;
}

inline StandbySnapshotReplicator::StandbySnapshotReplicator(
    std::int32_t memberId,
    std::shared_ptr<AeronArchive> archive,
    RecordingLog& recordingLog,
    std::int32_t serviceCount,
    const std::string& archiveControlChannel,
    std::int32_t archiveControlStreamId,
    const std::string& replicationChannel,
    std::int32_t fileSyncLevel,
    std::shared_ptr<Counter> snapshotCounter) :
    m_memberId(memberId),
    m_archive(archive),
    m_recordingLog(recordingLog),
    m_serviceCount(serviceCount),
    m_archiveControlChannel(archiveControlChannel),
    m_archiveControlStreamId(archiveControlStreamId),
    m_replicationChannel(replicationChannel),
    m_fileSyncLevel(fileSyncLevel),
    m_snapshotCounter(snapshotCounter),
    m_eventListenerWrapper(this)
{
}

inline StandbySnapshotReplicator::~StandbySnapshotReplicator()
{
    close();
}

inline std::int32_t StandbySnapshotReplicator::poll(std::int64_t nowNs)
{
    std::int32_t workCount = 0;

    if (!m_recordingReplication)
    {
        workCount++;

        if (m_snapshotsToReplicate.empty())
        {
            m_snapshotsToReplicate = computeSnapshotsToReplicate();

            if (m_snapshotsToReplicate.empty())
            {
                m_isComplete = true;
                return workCount;
            }
        }

        if (m_snapshotsToReplicate.empty())
        {
            std::string errorMsg = "failed to replicate any standby snapshots, errors: ";
            for (const auto& pair : m_errorsByEndpoint)
            {
                errorMsg += pair.first + "=" + pair.second + ", ";
            }
            throw ClusterException(errorMsg, SOURCEINFO);
        }

        m_currentSnapshotToReplicate = &m_snapshotsToReplicate[0];
        m_snapshotsToReplicate.erase(m_snapshotsToReplicate.begin());
        
        // Create destination URI by appending endpoint to control channel
        std::string srcChannel = m_archiveControlChannel;
        if (!srcChannel.empty() && srcChannel.back() != '?')
        {
            srcChannel += "|endpoint=" + m_currentSnapshotToReplicate->endpoint;
        }
        else
        {
            srcChannel += "endpoint=" + m_currentSnapshotToReplicate->endpoint;
        }

        const std::int64_t progressTimeoutNs = m_archive->context().messageTimeoutNs() * 2;
        const std::int64_t progressIntervalNs = progressTimeoutNs / 10;

        m_recordingReplication = MultipleRecordingReplication::newInstance(
            m_archive,
            m_archiveControlStreamId,
            srcChannel,
            m_replicationChannel,
            progressTimeoutNs,
            progressIntervalNs);
        
        m_recordingReplication->setEventListener(&m_eventListenerWrapper);

        for (const auto& entry : m_currentSnapshotToReplicate->recordingLogEntries)
        {
            m_recordingReplication->addRecording(
                entry.recordingId,
                aeron::NULL_VALUE, // NULL_RECORDING_ID
                aeron::NULL_POSITION); // NULL_POSITION
        }

        workCount++;
    }

    try
    {
        workCount += m_recordingReplication->poll(nowNs);
        m_archive->pollForRecordingSignals();
    }
    catch (const ArchiveException& ex)
    {
        m_errorsByEndpoint[m_currentSnapshotToReplicate->endpoint] = ex.what();
        if (m_recordingReplication)
        {
            m_recordingReplication->close();
            m_recordingReplication.reset();
        }
    }
    catch (const ClusterException& ex)
    {
        m_errorsByEndpoint[m_currentSnapshotToReplicate->endpoint] = ex.what();
        if (m_recordingReplication)
        {
            m_recordingReplication->close();
            m_recordingReplication.reset();
        }
    }

    if (m_recordingReplication && m_recordingReplication->isComplete())
    {
        for (const auto& entry : m_currentSnapshotToReplicate->recordingLogEntries)
        {
            const std::int64_t dstRecordingId = m_recordingReplication->completedDstRecordingId(entry.recordingId);
            m_recordingLog.appendSnapshot(
                dstRecordingId,
                entry.leadershipTermId,
                entry.termBaseLogPosition,
                entry.logPosition,
                entry.timestamp,
                entry.serviceId);
        }
        m_recordingLog.force(m_fileSyncLevel);

        m_snapshotCounter->incrementRelease();

        m_recordingReplication->close();
        m_recordingReplication.reset();
        m_isComplete = true;
    }

    return workCount;
}

inline bool StandbySnapshotReplicator::isComplete() const
{
    return m_isComplete;
}

inline void StandbySnapshotReplicator::onSignal(
    std::int64_t /* controlSessionId */,
    std::int64_t correlationId,
    std::int64_t recordingId,
    std::int64_t /* subscriptionId */,
    std::int64_t position,
    RecordingSignal signal)
{
    if (m_recordingReplication)
    {
        m_recordingReplication->onSignal(correlationId, recordingId, position, signal);
    }
}

inline void StandbySnapshotReplicator::close()
{
    if (m_recordingReplication)
    {
        m_recordingReplication->close();
        m_recordingReplication.reset();
    }
    if (m_archive)
    {
        m_archive->close();
        m_archive.reset();
    }
}

inline std::vector<StandbySnapshotReplicator::SnapshotReplicationEntry> StandbySnapshotReplicator::computeSnapshotsToReplicate()
{
    const auto snapshotsByEndpoint = filterByExistingRecordingLogEntries(
        m_recordingLog.latestStandbySnapshots(m_serviceCount));

    std::vector<SnapshotReplicationEntry> orderedSnapshotToReplicate;
    if (snapshotsByEndpoint.empty())
    {
        return orderedSnapshotToReplicate;
    }

    for (const auto& pair : snapshotsByEndpoint)
    {
        const std::int64_t logPosition = pair.second[0].logPosition;
        orderedSnapshotToReplicate.emplace_back(pair.first, logPosition, pair.second);
    }

    std::sort(orderedSnapshotToReplicate.begin(), orderedSnapshotToReplicate.end(), compareTo);

    return orderedSnapshotToReplicate;
}

inline int StandbySnapshotReplicator::compareTo(const SnapshotReplicationEntry& a, const SnapshotReplicationEntry& b)
{
    const int descendingOrderCompare = -static_cast<int>(a.logPosition - b.logPosition);
    if (0 != descendingOrderCompare)
    {
        return descendingOrderCompare;
    }

    return a.endpoint.compare(b.endpoint);
}

inline void StandbySnapshotReplicator::logReplicationEnded(
    const std::string& controlUri,
    std::int64_t srcRecordingId,
    std::int64_t dstRecordingId,
    std::int64_t position,
    bool hasSynced)
{
    // TODO: Implement ConsensusModuleAgent::logReplicationEnded
    // ConsensusModuleAgent::logReplicationEnded(
    //     m_memberId, "STANDBY_SNAPSHOT", controlUri, srcRecordingId, dstRecordingId, position, hasSynced);
}

inline std::unordered_map<std::string, std::vector<RecordingLog::Entry>> StandbySnapshotReplicator::filterByExistingRecordingLogEntries(
    const std::unordered_map<std::string, std::vector<RecordingLog::Entry>>& standbySnapshotsByEndpoint)
{
    std::unordered_map<std::string, std::vector<RecordingLog::Entry>> filteredSnapshotsByEndpoint;

    for (const auto& pair : standbySnapshotsByEndpoint)
    {
        std::vector<RecordingLog::Entry> filteredEntries;
        for (auto it = pair.second.rbegin(); it != pair.second.rend(); ++it)
        {
            const auto& standbySnapshotEntry = *it;
            auto* snapshotEntry = m_recordingLog.getLatestSnapshot(standbySnapshotEntry.serviceId);
            if (snapshotEntry && standbySnapshotEntry.logPosition <= snapshotEntry->logPosition)
            {
                // Skip this entry
            }
            else
            {
                filteredEntries.push_back(standbySnapshotEntry);
            }
        }
        std::reverse(filteredEntries.begin(), filteredEntries.end());

        if (!filteredEntries.empty())
        {
            filteredSnapshotsByEndpoint[pair.first] = filteredEntries;
        }
    }

    return filteredSnapshotsByEndpoint;
}

}}
