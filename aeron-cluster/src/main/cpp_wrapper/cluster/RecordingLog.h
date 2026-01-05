#pragma once

#include <memory>
#include <vector>
#include <string>
#include <unordered_map>
#include <map>
#include <algorithm>
#include <fstream>
#include <filesystem>
#include <cstdint>
#include "archive/client/AeronArchive.h"
#include "../client/ClusterExceptions.h"
#include "util/Exceptions.h"
#include "util/BitUtil.h"
#include "util/StringUtil.h"
#include "concurrent/AtomicBuffer.h"
#include "RecordingExtent.h"
#include "ConsensusModule.h"

namespace aeron { namespace cluster {

using namespace aeron::concurrent;
using namespace aeron::util;
using namespace aeron::archive::client;

/**
 * A log of recordings which make up the history of a Raft log across leadership terms.
 */
class RecordingLog
{
public:
    /**
     * Representation of the entry in the RecordingLog.
     */
    class Entry
    {
    public:
        std::int64_t recordingId;
        std::int64_t leadershipTermId;
        std::int64_t termBaseLogPosition;
        std::int64_t logPosition;
        std::int64_t timestamp;
        std::int32_t serviceId;
        std::int32_t type;
        std::int32_t entryIndex;
        bool isValid;
        std::string archiveEndpoint;
        std::int64_t position;

        Entry(
            std::int64_t recordingId,
            std::int64_t leadershipTermId,
            std::int64_t termBaseLogPosition,
            std::int64_t logPosition,
            std::int64_t timestamp,
            std::int32_t serviceId,
            std::int32_t type,
            const std::string& archiveEndpoint,
            bool isValid,
            std::int64_t position,
            std::int32_t entryIndex);

        Entry(
            std::int64_t recordingId,
            std::int64_t leadershipTermId,
            std::int64_t termBaseLogPosition,
            std::int64_t logPosition,
            std::int64_t timestamp,
            std::int32_t serviceId,
            std::int32_t type,
            const std::string& archiveEndpoint,
            bool isValid,
            std::int32_t entryIndex);

        std::int32_t length() const;
        Entry invalidate() const;
        Entry logPosition(std::int64_t logPosition) const;
        std::int64_t serviceId() const;

        bool operator==(const Entry& other) const;
        std::string toString() const;
    };

    /**
     * Representation of a snapshot entry in the RecordingLog.
     */
    class Snapshot
    {
    public:
        std::int64_t recordingId;
        std::int64_t leadershipTermId;
        std::int64_t termBaseLogPosition;
        std::int64_t logPosition;
        std::int64_t timestamp;
        std::int32_t serviceId;

        Snapshot(
            std::int64_t recordingId,
            std::int64_t leadershipTermId,
            std::int64_t termBaseLogPosition,
            std::int64_t logPosition,
            std::int64_t timestamp,
            std::int32_t serviceId);

        std::string toString() const;
    };

    /**
     * Representation of a log entry in the RecordingLog.
     */
    class Log
    {
    public:
        std::int64_t recordingId;
        std::int64_t leadershipTermId;
        std::int64_t termBaseLogPosition;
        std::int64_t logPosition;
        std::int64_t startPosition;
        std::int64_t stopPosition;
        std::int32_t initialTermId;
        std::int32_t termBufferLength;
        std::int32_t mtuLength;
        std::int32_t sessionId;

        Log(
            std::int64_t recordingId,
            std::int64_t leadershipTermId,
            std::int64_t termBaseLogPosition,
            std::int64_t logPosition,
            std::int64_t startPosition,
            std::int64_t stopPosition,
            std::int32_t initialTermId,
            std::int32_t termBufferLength,
            std::int32_t mtuLength,
            std::int32_t sessionId);

        std::string toString() const;
    };

    /**
     * The snapshots and steps to recover the state of a cluster.
     */
    class RecoveryPlan
    {
    public:
        std::int64_t lastLeadershipTermId;
        std::int64_t lastTermBaseLogPosition;
        std::int64_t appendedLogPosition;
        std::int64_t committedLogPosition;
        std::vector<Snapshot> snapshots;
        std::shared_ptr<Log> log;

        RecoveryPlan(
            std::int64_t lastLeadershipTermId,
            std::int64_t lastTermBaseLogPosition,
            std::int64_t appendedLogPosition,
            std::int64_t committedLogPosition,
            const std::vector<Snapshot>& snapshots,
            std::shared_ptr<Log> log);

        std::string toString() const;
    };

    static constexpr const char* RECORDING_LOG_FILE_NAME = "recording.log";
    static constexpr std::int32_t ENTRY_TYPE_TERM = 0;
    static constexpr std::int32_t ENTRY_TYPE_SNAPSHOT = 1;
    static constexpr std::int32_t ENTRY_TYPE_STANDBY_SNAPSHOT = 2;
    static constexpr std::int32_t ENTRY_TYPE_INVALID_FLAG = 1 << 31;
    static constexpr std::int32_t RECORDING_ID_OFFSET = 0;
    static constexpr std::int32_t LEADERSHIP_TERM_ID_OFFSET = RECORDING_ID_OFFSET + BitUtil::SIZE_OF_LONG;
    static constexpr std::int32_t TERM_BASE_LOG_POSITION_OFFSET = LEADERSHIP_TERM_ID_OFFSET + BitUtil::SIZE_OF_LONG;
    static constexpr std::int32_t LOG_POSITION_OFFSET = TERM_BASE_LOG_POSITION_OFFSET + BitUtil::SIZE_OF_LONG;
    static constexpr std::int32_t TIMESTAMP_OFFSET = LOG_POSITION_OFFSET + BitUtil::SIZE_OF_LONG;
    static constexpr std::int32_t SERVICE_ID_OFFSET = TIMESTAMP_OFFSET + BitUtil::SIZE_OF_LONG;
    static constexpr std::int32_t ENTRY_TYPE_OFFSET = SERVICE_ID_OFFSET + BitUtil::SIZE_OF_INT;
    static constexpr std::int32_t ENDPOINT_OFFSET = ENTRY_TYPE_OFFSET + BitUtil::SIZE_OF_INT;
    static constexpr std::int32_t MAX_ENTRY_LENGTH = 4096;
    static constexpr std::int32_t MAX_ENDPOINT_LENGTH = MAX_ENTRY_LENGTH - ENDPOINT_OFFSET - BitUtil::SIZE_OF_INT;
    static constexpr std::int32_t RECORD_ALIGNMENT = 64;

    RecordingLog(const std::filesystem::path& parentDir, bool createNew);
    ~RecordingLog();

    void close();
    void force(std::int32_t fileSyncLevel);
    std::vector<Entry> entries() const;
    std::int32_t nextEntryIndex() const;
    void reload();
    std::int64_t findLastTermRecordingId() const;
    Entry* findLastTerm();
    Entry getTermEntry(std::int64_t leadershipTermId) const;
    Entry* findTermEntry(std::int64_t leadershipTermId);
    Entry* getLatestSnapshot(std::int32_t serviceId);
    bool invalidateLatestSnapshot();
    std::int64_t getTermTimestamp(std::int64_t leadershipTermId) const;
    RecoveryPlan createRecoveryPlan(
        std::shared_ptr<AeronArchive> archive,
        std::int32_t serviceCount,
        std::int64_t replicatedRecordingId);
    bool isUnknown(std::int64_t leadershipTermId) const;
    void appendTerm(
        std::int64_t recordingId,
        std::int64_t leadershipTermId,
        std::int64_t termBaseLogPosition,
        std::int64_t timestamp);
    void appendSnapshot(
        std::int64_t recordingId,
        std::int64_t leadershipTermId,
        std::int64_t termBaseLogPosition,
        std::int64_t logPosition,
        std::int64_t timestamp,
        std::int32_t serviceId);
    void appendStandbySnapshot(
        std::int64_t recordingId,
        std::int64_t leadershipTermId,
        std::int64_t termBaseLogPosition,
        std::int64_t logPosition,
        std::int64_t timestamp,
        std::int32_t serviceId,
        const std::string& archiveEndpoint);
    void commitLogPosition(std::int64_t leadershipTermId, std::int64_t logPosition);
    void invalidateEntry(std::int32_t index);
    void removeEntry(std::int64_t leadershipTermId, std::int32_t entryIndex);
    std::map<std::string, std::vector<Entry>> latestStandbySnapshots(std::int32_t serviceCount);
    void ensureCoherent(
        std::int64_t recordingId,
        std::int64_t initialLogLeadershipTermId,
        std::int64_t initialTermBaseLogPosition,
        std::int64_t leadershipTermId,
        std::int64_t termBaseLogPosition,
        std::int64_t logPosition,
        std::int64_t nowNs,
        std::int64_t timestamp,
        std::int32_t fileSyncLevel);

    static std::string typeAsString(std::int32_t type);
    static void addSnapshots(
        std::vector<Snapshot>& snapshots,
        const std::vector<Entry>& entries,
        std::int32_t serviceCount,
        std::int32_t snapshotIndex);
    static void writeEntryToBuffer(const Entry& entry, AtomicBuffer& buffer);
    static bool isValidSnapshot(const Entry& entry);
    static bool isInvalidSnapshot(const Entry& entry);
    static bool isValidAnySnapshot(const Entry& entry);
    static bool isValidTerm(const Entry& entry);
    static bool matchesEntry(
        const Entry& entry,
        std::int64_t leadershipTermId,
        std::int64_t termBaseLogPosition,
        std::int64_t logPosition,
        std::int32_t serviceId);

    std::string toString() const;

private:
    static void validateRecordingId(std::int64_t recordingId);
    void validateTermRecordingId(std::int64_t recordingId);
    bool restoreInvalidSnapshot(
        std::int32_t snapshotEntryType,
        std::int64_t recordingId,
        std::int64_t leadershipTermId,
        std::int64_t termBaseLogPosition,
        std::int64_t logPosition,
        std::int64_t timestamp,
        std::int32_t serviceId);
    std::int32_t append(
        std::int32_t entryType,
        std::int64_t recordingId,
        std::int64_t leadershipTermId,
        std::int64_t termBaseLogPosition,
        std::int64_t logPosition,
        std::int64_t timestamp,
        std::int32_t serviceId,
        const std::string& endpoint);
    void persistToStorage(std::int64_t entryPosition, std::int32_t offset, std::int32_t length);
    void persistToStorage(const Entry& entry, AtomicBuffer& directBuffer);
    std::int32_t captureEntriesFromBuffer(
        std::int64_t filePosition,
        std::vector<std::uint8_t>& byteBuffer,
        AtomicBuffer& buffer,
        std::vector<Entry>& entries);
    static void syncDirectory(const std::filesystem::path& dir);
    static void planRecovery(
        std::vector<Snapshot>& snapshots,
        std::shared_ptr<Log>& logRef,
        const std::vector<Entry>& entries,
        std::shared_ptr<AeronArchive> archive,
        std::int32_t serviceCount,
        std::int64_t replicatedRecordingId);

    static std::int32_t compareEntries(const Entry& e1, const Entry& e2);

    std::int64_t m_termRecordingId = 0; // NULL_VALUE
    std::int32_t m_nextEntryIndex = 0;
    std::fstream m_fileStream;
    std::vector<std::uint8_t> m_byteBuffer;
    AtomicBuffer m_buffer;
    std::vector<Entry> m_entriesCache;
    std::unordered_map<std::int64_t, std::int32_t> m_cacheIndexByLeadershipTermIdMap;
    std::vector<std::int32_t> m_invalidSnapshots;
    std::filesystem::path m_logFilePath;
};

}}

