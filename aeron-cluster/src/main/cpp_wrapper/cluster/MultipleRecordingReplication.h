#pragma once

#include <memory>
#include <vector>
#include <unordered_map>
#include <cstdint>
#include <string>
#include "archive/client/AeronArchive.h"
#include "archive/codecs/RecordingSignal.h"
#include "util/CloseHelper.h"
#include "RecordingReplication.h"

namespace aeron { namespace cluster {

using namespace aeron::archive::client;
using namespace aeron::archive::codecs;

/**
 * Replication of multiple recordings.
 */
class MultipleRecordingReplication
{
public:
    /**
     * Event listener for replication events.
     */
    class EventListener
    {
    public:
        virtual ~EventListener() = default;
        virtual void onReplicationEnded(
            const std::string& controlUri,
            std::int64_t srcRecordingId,
            std::int64_t dstRecordingId,
            std::int64_t position,
            bool hasSynced) = 0;
    };

    /**
     * Recording information.
     */
    struct RecordingInfo
    {
        std::int64_t srcRecordingId;
        std::int64_t dstRecordingId;
        std::int64_t stopPosition;

        RecordingInfo(std::int64_t srcRecordingId, std::int64_t dstRecordingId, std::int64_t stopPosition)
            : srcRecordingId(srcRecordingId), dstRecordingId(dstRecordingId), stopPosition(stopPosition)
        {
        }
    };

    static std::unique_ptr<MultipleRecordingReplication> newInstance(
        std::shared_ptr<AeronArchive> archive,
        std::int32_t srcControlStreamId,
        const std::string& srcControlChannel,
        const std::string& replicationChannel,
        std::int64_t replicationProgressTimeoutNs,
        std::int64_t replicationProgressIntervalNs);

    static std::unique_ptr<MultipleRecordingReplication> newInstance(
        std::shared_ptr<AeronArchive> archive,
        std::int32_t srcControlStreamId,
        const std::string& srcControlChannel,
        const std::string& replicationChannel,
        const std::string& srcResponseChannel,
        std::int64_t replicationProgressTimeoutNs,
        std::int64_t replicationProgressIntervalNs);

    ~MultipleRecordingReplication();

    void addRecording(std::int64_t srcRecordingId, std::int64_t dstRecordingId, std::int64_t stopPosition);

    std::int32_t poll(std::int64_t nowNs);

    std::int64_t completedDstRecordingId(std::int64_t srcRecordingId) const;

    void onSignal(
        std::int64_t correlationId,
        std::int64_t recordingId,
        std::int64_t position,
        RecordingSignal signal);

    bool isComplete() const;

    void close();

    void setEventListener(EventListener* eventListener);

private:
    MultipleRecordingReplication(
        std::shared_ptr<AeronArchive> archive,
        std::int32_t srcControlStreamId,
        const std::string& srcControlChannel,
        const std::string& replicationChannel,
        const std::string& srcResponseChannel,
        std::int64_t replicationProgressTimeoutNs,
        std::int64_t replicationProgressIntervalNs);

    void replicateCurrentSnapshot(std::int64_t nowNs);

    void onReplicationEnded(
        const std::string& srcArchiveControlChannel,
        std::int64_t srcRecordingId,
        std::int64_t dstRecordingId,
        std::int64_t position,
        bool hasSynced);

    std::shared_ptr<AeronArchive> m_archive;
    std::int32_t m_srcControlStreamId;
    std::string m_srcControlChannel;
    std::string m_replicationChannel;
    std::string m_srcResponseChannel;
    std::vector<RecordingInfo> m_recordingsPending;
    std::unordered_map<std::int64_t, std::int64_t> m_recordingsCompleted;
    std::int64_t m_progressTimeoutNs;
    std::int64_t m_progressIntervalNs;
    std::int32_t m_recordingCursor = 0;
    std::unique_ptr<RecordingReplication> m_recordingReplication;
    EventListener* m_eventListener = nullptr;
};

}}

