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
#include "ConsensusModuleAgent.h"

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

// Implementation - Entry class
inline RecordingLog::Entry::Entry(
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
    std::int32_t entryIndex) :
    recordingId(recordingId),
    leadershipTermId(leadershipTermId),
    termBaseLogPosition(termBaseLogPosition),
    logPosition(logPosition),
    timestamp(timestamp),
    serviceId(serviceId),
    type(type),
    entryIndex(entryIndex),
    isValid(isValid),
    archiveEndpoint(archiveEndpoint),
    position(position)
{
    if (ENTRY_TYPE_STANDBY_SNAPSHOT == type && archiveEndpoint.empty())
    {
        throw ClusterException("Remote snapshots must has a valid endpoint", SOURCEINFO);
    }
}

inline RecordingLog::Entry::Entry(
    std::int64_t recordingId,
    std::int64_t leadershipTermId,
    std::int64_t termBaseLogPosition,
    std::int64_t logPosition,
    std::int64_t timestamp,
    std::int32_t serviceId,
    std::int32_t type,
    const std::string& archiveEndpoint,
    bool isValid,
    std::int32_t entryIndex) :
    Entry(recordingId, leadershipTermId, termBaseLogPosition, logPosition, timestamp,
          serviceId, type, archiveEndpoint, isValid, aeron::NULL_VALUE, entryIndex)
{
}

inline std::int32_t RecordingLog::Entry::length() const
{
    const std::int32_t unalignedLength = (ENTRY_TYPE_STANDBY_SNAPSHOT == type) ?
        (ENDPOINT_OFFSET + BitUtil::SIZE_OF_INT + static_cast<std::int32_t>(archiveEndpoint.length())) : ENDPOINT_OFFSET;
    return static_cast<std::int32_t>(BitUtil::align(static_cast<std::int64_t>(unalignedLength), RECORD_ALIGNMENT));
}

inline RecordingLog::Entry RecordingLog::Entry::invalidate() const
{
    return Entry(
        recordingId, leadershipTermId, termBaseLogPosition, logPosition, timestamp,
        serviceId, type, archiveEndpoint, false, position, entryIndex);
}

inline RecordingLog::Entry RecordingLog::Entry::logPosition(std::int64_t newLogPosition) const
{
    return Entry(
        recordingId, leadershipTermId, termBaseLogPosition, newLogPosition, timestamp,
        serviceId, type, archiveEndpoint, isValid, position, entryIndex);
}

inline std::int64_t RecordingLog::Entry::serviceId() const
{
    return serviceId;
}

inline bool RecordingLog::Entry::operator==(const Entry& other) const
{
    return recordingId == other.recordingId &&
           leadershipTermId == other.leadershipTermId &&
           termBaseLogPosition == other.termBaseLogPosition &&
           logPosition == other.logPosition &&
           timestamp == other.timestamp &&
           serviceId == other.serviceId &&
           type == other.type &&
           entryIndex == other.entryIndex &&
           isValid == other.isValid;
}

inline std::string RecordingLog::Entry::toString() const
{
    return "Entry{" +
        "recordingId=" + std::to_string(recordingId) +
        ", leadershipTermId=" + std::to_string(leadershipTermId) +
        ", termBaseLogPosition=" + std::to_string(termBaseLogPosition) +
        ", logPosition=" + std::to_string(logPosition) +
        ", timestamp=" + std::to_string(timestamp) +
        ", serviceId=" + std::to_string(serviceId) +
        ", type=" + RecordingLog::typeAsString(type) +
        ", entryIndex=" + std::to_string(entryIndex) +
        ", isValid=" + (isValid ? "true" : "false") +
        ", archiveEndpoint='" + archiveEndpoint + "'" +
        "}";
}

// Implementation - Snapshot class
inline RecordingLog::Snapshot::Snapshot(
    std::int64_t recordingId,
    std::int64_t leadershipTermId,
    std::int64_t termBaseLogPosition,
    std::int64_t logPosition,
    std::int64_t timestamp,
    std::int32_t serviceId) :
    recordingId(recordingId),
    leadershipTermId(leadershipTermId),
    termBaseLogPosition(termBaseLogPosition),
    logPosition(logPosition),
    timestamp(timestamp),
    serviceId(serviceId)
{
}

inline std::string RecordingLog::Snapshot::toString() const
{
    return "Snapshot{" +
        "recordingId=" + std::to_string(recordingId) +
        ", leadershipTermId=" + std::to_string(leadershipTermId) +
        ", termBaseLogPosition=" + std::to_string(termBaseLogPosition) +
        ", logPosition=" + std::to_string(logPosition) +
        ", timestamp=" + std::to_string(timestamp) +
        ", serviceId=" + std::to_string(serviceId) +
        "}";
}

// Implementation - Log class
inline RecordingLog::Log::Log(
    std::int64_t recordingId,
    std::int64_t leadershipTermId,
    std::int64_t termBaseLogPosition,
    std::int64_t logPosition,
    std::int64_t startPosition,
    std::int64_t stopPosition,
    std::int32_t initialTermId,
    std::int32_t termBufferLength,
    std::int32_t mtuLength,
    std::int32_t sessionId) :
    recordingId(recordingId),
    leadershipTermId(leadershipTermId),
    termBaseLogPosition(termBaseLogPosition),
    logPosition(logPosition),
    startPosition(startPosition),
    stopPosition(stopPosition),
    initialTermId(initialTermId),
    termBufferLength(termBufferLength),
    mtuLength(mtuLength),
    sessionId(sessionId)
{
}

inline std::string RecordingLog::Log::toString() const
{
    return "Log{" +
        "recordingId=" + std::to_string(recordingId) +
        ", leadershipTermId=" + std::to_string(leadershipTermId) +
        ", termBaseLogPosition=" + std::to_string(termBaseLogPosition) +
        ", logPosition=" + std::to_string(logPosition) +
        ", startPosition=" + std::to_string(startPosition) +
        ", stopPosition=" + std::to_string(stopPosition) +
        ", initialTermId=" + std::to_string(initialTermId) +
        ", termBufferLength=" + std::to_string(termBufferLength) +
        ", mtuLength=" + std::to_string(mtuLength) +
        ", sessionId=" + std::to_string(sessionId) +
        "}";
}

// Implementation - RecoveryPlan class
inline RecordingLog::RecoveryPlan::RecoveryPlan(
    std::int64_t lastLeadershipTermId,
    std::int64_t lastTermBaseLogPosition,
    std::int64_t appendedLogPosition,
    std::int64_t committedLogPosition,
    const std::vector<Snapshot>& snapshots,
    std::shared_ptr<Log> log) :
    lastLeadershipTermId(lastLeadershipTermId),
    lastTermBaseLogPosition(lastTermBaseLogPosition),
    appendedLogPosition(appendedLogPosition),
    committedLogPosition(committedLogPosition),
    snapshots(snapshots),
    log(log)
{
}

inline std::string RecordingLog::RecoveryPlan::toString() const
{
    std::string result = "RecoveryPlan{" +
        "lastLeadershipTermId=" + std::to_string(lastLeadershipTermId) +
        ", lastTermBaseLogPosition=" + std::to_string(lastTermBaseLogPosition) +
        ", appendedLogPosition=" + std::to_string(appendedLogPosition) +
        ", committedLogPosition=" + std::to_string(committedLogPosition) +
        ", snapshots=[";
    
    for (size_t i = 0; i < snapshots.size(); i++)
    {
        if (i > 0) result += ", ";
        result += snapshots[i].toString();
    }
    
    result += "], log=" + (log ? log->toString() : "null") + "}";
    return result;
}

// Implementation - RecordingLog class
inline RecordingLog::RecordingLog(const std::filesystem::path& parentDir, bool createNew) :
    m_byteBuffer(MAX_ENTRY_LENGTH, 0)
{
    m_logFilePath = parentDir / RECORDING_LOG_FILE_NAME;
    bool isNewFile = !std::filesystem::exists(m_logFilePath);
    
    std::ios_base::openmode mode = std::ios::in | std::ios::out | std::ios::binary;
    if (createNew || isNewFile)
    {
        mode |= std::ios::trunc;
    }
    
    m_fileStream.open(m_logFilePath, mode);
    if (!m_fileStream.is_open())
    {
        throw ClusterException("Failed to open recording log file: " + m_logFilePath.string(), SOURCEINFO);
    }
    
    m_buffer.wrap(m_byteBuffer.data(), m_byteBuffer.size());
    
    if (isNewFile)
    {
        syncDirectory(parentDir);
    }
    else
    {
        reload();
    }
}

inline RecordingLog::~RecordingLog()
{
    close();
}

inline void RecordingLog::close()
{
    if (m_fileStream.is_open())
    {
        m_fileStream.close();
    }
}

inline void RecordingLog::force(std::int32_t fileSyncLevel)
{
    if (fileSyncLevel > 0 && m_fileStream.is_open())
    {
        m_fileStream.flush();
        if (fileSyncLevel > 1)
        {
            // On Windows, we can't easily sync directory, but flush should be sufficient
            // On Unix, we would call fsync
        }
    }
}

inline std::vector<RecordingLog::Entry> RecordingLog::entries() const
{
    return m_entriesCache;
}

inline std::int32_t RecordingLog::nextEntryIndex() const
{
    return m_nextEntryIndex;
}

// Static constants for size
namespace {
    constexpr std::int32_t SIZE_OF_LONG = sizeof(std::int64_t);
    constexpr std::int32_t SIZE_OF_INT = sizeof(std::int32_t);
}

// Static helper methods
inline std::string RecordingLog::typeAsString(std::int32_t type)
{
    const std::int32_t cleanType = type & ~ENTRY_TYPE_INVALID_FLAG;
    switch (cleanType)
    {
        case ENTRY_TYPE_TERM:
            return "TERM";
        case ENTRY_TYPE_SNAPSHOT:
            return "SNAPSHOT";
        case ENTRY_TYPE_STANDBY_SNAPSHOT:
            return "STANDBY_SNAPSHOT";
        default:
            return "UNKNOWN(" + std::to_string(cleanType) + ")";
    }
}

inline bool RecordingLog::isValidSnapshot(const Entry& entry)
{
    return (ENTRY_TYPE_SNAPSHOT == entry.type || ENTRY_TYPE_STANDBY_SNAPSHOT == entry.type) && entry.isValid;
}

inline bool RecordingLog::isInvalidSnapshot(const Entry& entry)
{
    return (ENTRY_TYPE_SNAPSHOT == entry.type || ENTRY_TYPE_STANDBY_SNAPSHOT == entry.type) && !entry.isValid;
}

inline bool RecordingLog::isValidAnySnapshot(const Entry& entry)
{
    return ENTRY_TYPE_SNAPSHOT == entry.type || ENTRY_TYPE_STANDBY_SNAPSHOT == entry.type;
}

inline bool RecordingLog::isValidTerm(const Entry& entry)
{
    return ENTRY_TYPE_TERM == entry.type && entry.isValid;
}

inline bool RecordingLog::matchesEntry(
    const Entry& entry,
    std::int64_t leadershipTermId,
    std::int64_t termBaseLogPosition,
    std::int64_t logPosition,
    std::int32_t serviceId)
{
    return entry.leadershipTermId == leadershipTermId &&
           entry.termBaseLogPosition == termBaseLogPosition &&
           entry.logPosition == logPosition &&
           entry.serviceId == serviceId;
}

inline void RecordingLog::validateRecordingId(std::int64_t recordingId)
{
    if (recordingId < 0)
    {
        throw ClusterException("recordingId must be >= 0: " + std::to_string(recordingId), SOURCEINFO);
    }
}

inline void RecordingLog::validateTermRecordingId(std::int64_t recordingId)
{
    validateRecordingId(recordingId);
    if (m_termRecordingId != 0 && recordingId != m_termRecordingId)
    {
        throw ClusterException(
            "recordingId=" + std::to_string(recordingId) + 
            " does not match termRecordingId=" + std::to_string(m_termRecordingId),
            SOURCEINFO);
    }
    m_termRecordingId = recordingId;
}

inline void RecordingLog::syncDirectory(const std::filesystem::path& dir)
{
    // On Windows, directory sync is not easily available
    // On Unix, we would call fsync on the directory file descriptor
    // For now, we'll just ensure the directory exists
    if (!std::filesystem::exists(dir))
    {
        std::filesystem::create_directories(dir);
    }
}

inline void RecordingLog::reload()
{
    m_entriesCache.clear();
    m_cacheIndexByLeadershipTermIdMap.clear();
    m_invalidSnapshots.clear();
    m_nextEntryIndex = 0;
    
    if (!m_fileStream.is_open())
    {
        return;
    }
    
    m_fileStream.seekg(0, std::ios::beg);
    std::int64_t filePosition = 0;
    std::int64_t consumePosition = 0;
    
    while (true)
    {
        m_fileStream.read(reinterpret_cast<char*>(m_byteBuffer.data()), MAX_ENTRY_LENGTH);
        const std::streamsize bytesRead = m_fileStream.gcount();
        
        if (bytesRead > 0)
        {
            m_buffer.wrap(m_byteBuffer.data(), static_cast<std::size_t>(bytesRead));
            consumePosition += captureEntriesFromBuffer(
                consumePosition, m_byteBuffer, m_buffer, m_entriesCache);
        }
        else
        {
            break;
        }
    }
    
    // Sort entries
    std::sort(m_entriesCache.begin(), m_entriesCache.end(),
        [](const Entry& a, const Entry& b)
        {
            if (a.leadershipTermId != b.leadershipTermId)
            {
                return a.leadershipTermId < b.leadershipTermId;
            }
            if (a.termBaseLogPosition != b.termBaseLogPosition)
            {
                return a.termBaseLogPosition < b.termBaseLogPosition;
            }
            if (a.logPosition != b.logPosition)
            {
                return a.logPosition < b.logPosition;
            }
            return a.serviceId < b.serviceId;
        });
    
    // Build cache index map
    for (std::size_t i = 0; i < m_entriesCache.size(); i++)
    {
        const Entry& entry = m_entriesCache[i];
        if (isValidTerm(entry))
        {
            m_cacheIndexByLeadershipTermIdMap[entry.leadershipTermId] = static_cast<std::int32_t>(i);
        }
        
        if (!entry.isValid && isValidAnySnapshot(entry))
        {
            m_invalidSnapshots.push_back(static_cast<std::int32_t>(i));
        }
    }
}

inline std::int64_t RecordingLog::findLastTermRecordingId() const
{
    const Entry* lastTerm = findLastTerm();
    return lastTerm ? lastTerm->recordingId : AeronArchive::NULL_RECORDING_ID;
}

inline RecordingLog::Entry* RecordingLog::findLastTerm()
{
    for (int i = static_cast<int>(m_entriesCache.size()) - 1; i >= 0; i--)
    {
        Entry& entry = m_entriesCache[i];
        if (isValidTerm(entry))
        {
            return &entry;
        }
    }
    return nullptr;
}

inline RecordingLog::Entry RecordingLog::getTermEntry(std::int64_t leadershipTermId) const
{
    auto it = m_cacheIndexByLeadershipTermIdMap.find(leadershipTermId);
    if (it != m_cacheIndexByLeadershipTermIdMap.end())
    {
        return m_entriesCache[it->second];
    }
    throw ClusterException("unknown leadershipTermId=" + std::to_string(leadershipTermId), SOURCEINFO);
}

inline RecordingLog::Entry* RecordingLog::findTermEntry(std::int64_t leadershipTermId)
{
    auto it = m_cacheIndexByLeadershipTermIdMap.find(leadershipTermId);
    if (it != m_cacheIndexByLeadershipTermIdMap.end())
    {
        return &m_entriesCache[it->second];
    }
    return nullptr;
}

inline bool RecordingLog::isUnknown(std::int64_t leadershipTermId) const
{
    return m_cacheIndexByLeadershipTermIdMap.find(leadershipTermId) == m_cacheIndexByLeadershipTermIdMap.end();
}

inline void RecordingLog::appendTerm(
    std::int64_t recordingId,
    std::int64_t leadershipTermId,
    std::int64_t termBaseLogPosition,
    std::int64_t timestamp)
{
    validateTermRecordingId(recordingId);
    
    std::int64_t logPosition = AeronArchive::NULL_POSITION;
    
    if (!m_entriesCache.empty())
    {
        if (m_cacheIndexByLeadershipTermIdMap.find(leadershipTermId) != m_cacheIndexByLeadershipTermIdMap.end())
        {
            throw ClusterException("duplicate TERM entry for leadershipTermId=" + std::to_string(leadershipTermId), SOURCEINFO);
        }
        
        const std::int64_t previousLeadershipTermId = leadershipTermId - 1;
        if (m_cacheIndexByLeadershipTermIdMap.find(previousLeadershipTermId) != m_cacheIndexByLeadershipTermIdMap.end())
        {
            commitLogPosition(previousLeadershipTermId, termBaseLogPosition);
        }
        
        Entry* nextTermEntry = findTermEntry(leadershipTermId + 1);
        if (nextTermEntry)
        {
            logPosition = nextTermEntry->termBaseLogPosition;
        }
    }
    
    const std::int32_t index = append(
        ENTRY_TYPE_TERM,
        recordingId,
        leadershipTermId,
        termBaseLogPosition,
        logPosition,
        timestamp,
        aeron::NULL_VALUE,
        "");
    
    m_cacheIndexByLeadershipTermIdMap[leadershipTermId] = index;
}

inline void RecordingLog::appendSnapshot(
    std::int64_t recordingId,
    std::int64_t leadershipTermId,
    std::int64_t termBaseLogPosition,
    std::int64_t logPosition,
    std::int64_t timestamp,
    std::int32_t serviceId)
{
    validateRecordingId(recordingId);
    
    if (!restoreInvalidSnapshot(
        ENTRY_TYPE_SNAPSHOT, recordingId, leadershipTermId, termBaseLogPosition, logPosition, timestamp, serviceId))
    {
        append(
            ENTRY_TYPE_SNAPSHOT,
            recordingId,
            leadershipTermId,
            termBaseLogPosition,
            logPosition,
            timestamp,
            serviceId,
            "");
    }
}

inline void RecordingLog::appendStandbySnapshot(
    std::int64_t recordingId,
    std::int64_t leadershipTermId,
    std::int64_t termBaseLogPosition,
    std::int64_t logPosition,
    std::int64_t timestamp,
    std::int32_t serviceId,
    const std::string& archiveEndpoint)
{
    validateRecordingId(recordingId);
    
    if (archiveEndpoint.empty())
    {
        throw ClusterException("Remote snapshots must have a valid endpoint", SOURCEINFO);
    }
    
    if (static_cast<std::int32_t>(archiveEndpoint.length()) > MAX_ENDPOINT_LENGTH)
    {
        throw ClusterException(
            "Endpoint is too long: " + std::to_string(archiveEndpoint.length()) + 
            " vs " + std::to_string(MAX_ENDPOINT_LENGTH),
            SOURCEINFO);
    }
    
    if (!restoreInvalidSnapshot(
        ENTRY_TYPE_STANDBY_SNAPSHOT, recordingId, leadershipTermId, termBaseLogPosition, logPosition, timestamp, serviceId))
    {
        append(
            ENTRY_TYPE_STANDBY_SNAPSHOT,
            recordingId,
            leadershipTermId,
            termBaseLogPosition,
            logPosition,
            timestamp,
            serviceId,
            archiveEndpoint);
    }
}

inline void RecordingLog::commitLogPosition(std::int64_t leadershipTermId, std::int64_t logPosition)
{
    auto it = m_cacheIndexByLeadershipTermIdMap.find(leadershipTermId);
    if (it == m_cacheIndexByLeadershipTermIdMap.end())
    {
        throw ClusterException("unknown leadershipTermId=" + std::to_string(leadershipTermId), SOURCEINFO);
    }
    
    Entry& entry = m_entriesCache[it->second];
    if (entry.logPosition != logPosition)
    {
        m_buffer.putInt64(0, logPosition);
        persistToStorage(entry.position, LOG_POSITION_OFFSET, SIZE_OF_LONG);
        entry = entry.logPosition(logPosition);
    }
}

inline std::int32_t RecordingLog::captureEntriesFromBuffer(
    std::int64_t filePosition,
    std::vector<std::uint8_t>& byteBuffer,
    AtomicBuffer& buffer,
    std::vector<Entry>& entries)
{
    std::int32_t consumed = 0;
    const std::int32_t length = static_cast<std::int32_t>(byteBuffer.size());
    
    while (consumed + ENDPOINT_OFFSET < length)
    {
        const std::int64_t position = filePosition + consumed;
        const std::int32_t entryType = buffer.getInt32(consumed + ENTRY_TYPE_OFFSET);
        const std::int32_t type = entryType & ~ENTRY_TYPE_INVALID_FLAG;
        const bool isValid = (entryType & ENTRY_TYPE_INVALID_FLAG) == 0;
        const std::int32_t endPointOffset = consumed + ENDPOINT_OFFSET;
        
        if (ENTRY_TYPE_STANDBY_SNAPSHOT == type &&
            (endPointOffset + SIZE_OF_INT > length ||
             endPointOffset + SIZE_OF_INT + buffer.getInt32(endPointOffset) > length))
        {
            break;
        }
        
        std::string archiveEndpoint;
        if (ENTRY_TYPE_STANDBY_SNAPSHOT == type)
        {
            const std::int32_t endpointLength = buffer.getInt32(endPointOffset);
            if (endpointLength > 0 && endPointOffset + SIZE_OF_INT + endpointLength <= length)
            {
                archiveEndpoint = buffer.getStringWithoutLength(endPointOffset + SIZE_OF_INT, endpointLength);
            }
        }
        
        Entry entry(
            buffer.getInt64(consumed + RECORDING_ID_OFFSET),
            buffer.getInt64(consumed + LEADERSHIP_TERM_ID_OFFSET),
            buffer.getInt64(consumed + TERM_BASE_LOG_POSITION_OFFSET),
            buffer.getInt64(consumed + LOG_POSITION_OFFSET),
            buffer.getInt64(consumed + TIMESTAMP_OFFSET),
            buffer.getInt32(consumed + SERVICE_ID_OFFSET),
            type,
            archiveEndpoint,
            isValid,
            position,
            m_nextEntryIndex);
        
        if (aeron::NULL_VALUE != entryType)
        {
            if (ENTRY_TYPE_TERM == type)
            {
                validateTermRecordingId(entry.recordingId);
            }
            
            entries.push_back(entry);
        }
        
        consumed += entry.length();
        ++m_nextEntryIndex;
    }
    
    return consumed;
}

inline void RecordingLog::writeEntryToBuffer(const Entry& entry, AtomicBuffer& buffer)
{
    buffer.putInt64(RECORDING_ID_OFFSET, entry.recordingId);
    buffer.putInt64(LEADERSHIP_TERM_ID_OFFSET, entry.leadershipTermId);
    buffer.putInt64(TERM_BASE_LOG_POSITION_OFFSET, entry.termBaseLogPosition);
    buffer.putInt64(LOG_POSITION_OFFSET, entry.logPosition);
    buffer.putInt64(TIMESTAMP_OFFSET, entry.timestamp);
    buffer.putInt32(SERVICE_ID_OFFSET, entry.serviceId);
    
    std::int32_t entryType = entry.type;
    if (!entry.isValid)
    {
        entryType |= ENTRY_TYPE_INVALID_FLAG;
    }
    buffer.putInt32(ENTRY_TYPE_OFFSET, entryType);
    
    if (ENTRY_TYPE_STANDBY_SNAPSHOT == entry.type)
    {
        const std::int32_t endpointLength = static_cast<std::int32_t>(entry.archiveEndpoint.length());
        buffer.putInt32(ENDPOINT_OFFSET, endpointLength);
        if (endpointLength > 0)
        {
            buffer.putStringWithoutLength(ENDPOINT_OFFSET + SIZE_OF_INT, entry.archiveEndpoint);
        }
    }
}

inline void RecordingLog::persistToStorage(std::int64_t entryPosition, std::int32_t offset, std::int32_t length)
{
    if (!m_fileStream.is_open())
    {
        throw ClusterException("File stream is not open", SOURCEINFO);
    }
    
    const std::int64_t position = entryPosition + offset;
    m_fileStream.seekp(position);
    m_fileStream.write(reinterpret_cast<const char*>(m_buffer.buffer() + offset), length);
    
    if (!m_fileStream.good())
    {
        throw ClusterException("failed to write field atomically", SOURCEINFO);
    }
}

inline void RecordingLog::persistToStorage(const Entry& entry, AtomicBuffer& directBuffer)
{
    if (!m_fileStream.is_open())
    {
        throw ClusterException("File stream is not open", SOURCEINFO);
    }
    
    std::int64_t entryPosition = entry.position;
    if (aeron::NULL_VALUE == entryPosition)
    {
        m_fileStream.seekp(0, std::ios::end);
        const std::int64_t currentSize = m_fileStream.tellp();
        entryPosition = BitUtil::align(currentSize, RECORD_ALIGNMENT);
    }
    
    writeEntryToBuffer(entry, directBuffer);
    
    m_fileStream.seekp(entryPosition);
    const std::int32_t entryLength = entry.length();
    m_fileStream.write(reinterpret_cast<const char*>(directBuffer.buffer()), entryLength);
    
    if (!m_fileStream.good())
    {
        throw ClusterException("failed to write entry atomically", SOURCEINFO);
    }
}

inline std::int32_t RecordingLog::append(
    std::int32_t entryType,
    std::int64_t recordingId,
    std::int64_t leadershipTermId,
    std::int64_t termBaseLogPosition,
    std::int64_t logPosition,
    std::int64_t timestamp,
    std::int32_t serviceId,
    const std::string& endpoint)
{
    Entry entry(
        recordingId,
        leadershipTermId,
        termBaseLogPosition,
        logPosition,
        timestamp,
        serviceId,
        entryType,
        endpoint,
        true,
        aeron::NULL_VALUE,
        m_nextEntryIndex);
    
    writeEntryToBuffer(entry, m_buffer);
    persistToStorage(entry, m_buffer);
    
    m_nextEntryIndex++;
    
    // Insert entry in sorted order
    std::size_t index = m_entriesCache.size();
    for (int i = static_cast<int>(m_entriesCache.size()) - 1; i >= 0; i--)
    {
        const Entry& e = m_entriesCache[i];
        if (entry.leadershipTermId < e.leadershipTermId ||
            (entry.leadershipTermId == e.leadershipTermId && entry.termBaseLogPosition < e.termBaseLogPosition) ||
            (entry.leadershipTermId == e.leadershipTermId && entry.termBaseLogPosition == e.termBaseLogPosition &&
             entry.logPosition < e.logPosition) ||
            (entry.leadershipTermId == e.leadershipTermId && entry.termBaseLogPosition == e.termBaseLogPosition &&
             entry.logPosition == e.logPosition && entry.serviceId < e.serviceId))
        {
            index--;
        }
        else
        {
            break;
        }
    }
    
    if (index < m_entriesCache.size())
    {
        m_entriesCache.insert(m_entriesCache.begin() + index, entry);
        
        // Update cache indices for entries after insertion point
        for (auto& pair : m_cacheIndexByLeadershipTermIdMap)
        {
            if (pair.first > leadershipTermId)
            {
                pair.second++;
            }
        }
        
        for (auto& snapshotIndex : m_invalidSnapshots)
        {
            if (snapshotIndex >= static_cast<std::int32_t>(index))
            {
                snapshotIndex++;
            }
        }
    }
    else
    {
        m_entriesCache.push_back(entry);
    }
    
    return static_cast<std::int32_t>(index);
}

inline bool RecordingLog::restoreInvalidSnapshot(
    std::int32_t snapshotEntryType,
    std::int64_t recordingId,
    std::int64_t leadershipTermId,
    std::int64_t termBaseLogPosition,
    std::int64_t logPosition,
    std::int64_t timestamp,
    std::int32_t serviceId)
{
    for (int i = static_cast<int>(m_invalidSnapshots.size()) - 1; i >= 0; i--)
    {
        const std::int32_t entryCacheIndex = m_invalidSnapshots[i];
        Entry& entry = m_entriesCache[entryCacheIndex];
        
        if (matchesEntry(entry, leadershipTermId, termBaseLogPosition, logPosition, serviceId) &&
            snapshotEntryType == entry.type && !entry.isValid)
        {
            Entry validatedEntry = entry;
            validatedEntry.recordingId = recordingId;
            validatedEntry.timestamp = timestamp;
            validatedEntry.isValid = true;
            
            writeEntryToBuffer(validatedEntry, m_buffer);
            persistToStorage(validatedEntry, m_buffer);
            
            entry = validatedEntry;
            
            // Remove from invalid snapshots
            m_invalidSnapshots.erase(m_invalidSnapshots.begin() + i);
            
            return true;
        }
    }
    
    return false;
}

inline RecordingLog::Entry* RecordingLog::getLatestSnapshot(std::int32_t serviceId)
{
    const std::int32_t CONSENSUS_MODULE_SERVICE_ID = ConsensusModuleAgent::SERVICE_ID;
    
    for (int i = static_cast<int>(m_entriesCache.size()) - 1; i >= 0; i--)
    {
        Entry& entry = m_entriesCache[i];
        if (isValidSnapshot(entry) && CONSENSUS_MODULE_SERVICE_ID == entry.serviceId)
        {
            if (CONSENSUS_MODULE_SERVICE_ID == serviceId)
            {
                return &entry;
            }
            
            const int serviceSnapshotIndex = i - (serviceId + 1);
            if (serviceSnapshotIndex >= 0)
            {
                Entry& snapshot = m_entriesCache[serviceSnapshotIndex];
                if (isValidSnapshot(snapshot) && serviceId == snapshot.serviceId)
                {
                    return &snapshot;
                }
            }
        }
    }
    
    return nullptr;
}

inline bool RecordingLog::invalidateLatestSnapshot()
{
    const std::int32_t CONSENSUS_MODULE_SERVICE_ID = ConsensusModuleAgent::SERVICE_ID;
    
    std::vector<std::int32_t> indices;
    std::int64_t highLogPosition = AeronArchive::NULL_POSITION;
    
    for (int idx = static_cast<int>(m_entriesCache.size()) - 1; idx >= 0; idx--)
    {
        Entry& entry = m_entriesCache[idx];
        if (isValidAnySnapshot(entry) && CONSENSUS_MODULE_SERVICE_ID == entry.serviceId)
        {
            if (entry.logPosition >= highLogPosition)
            {
                if (m_cacheIndexByLeadershipTermIdMap.find(entry.leadershipTermId) == m_cacheIndexByLeadershipTermIdMap.end())
                {
                    throw ClusterException(
                        "no matching term for snapshot: leadershipTermId=" + std::to_string(entry.leadershipTermId),
                        SOURCEINFO);
                }
                
                if (entry.logPosition > highLogPosition)
                {
                    indices.clear();
                    highLogPosition = entry.logPosition;
                }
                
                indices.push_back(idx);
            }
            else
            {
                break;
            }
        }
    }
    
    const bool found = !indices.empty();
    
    while (!indices.empty())
    {
        const int startingIndex = indices.back();
        indices.pop_back();
        
        std::int32_t serviceId = CONSENSUS_MODULE_SERVICE_ID;
        
        for (int idx = startingIndex; idx >= 0; idx--)
        {
            Entry& entry = m_entriesCache[idx];
            if (isValidAnySnapshot(entry) && entry.serviceId == serviceId)
            {
                invalidateEntry(idx);
                serviceId++;
            }
            else
            {
                break;
            }
        }
    }
    
    return found;
}

inline std::int64_t RecordingLog::getTermTimestamp(std::int64_t leadershipTermId) const
{
    auto it = m_cacheIndexByLeadershipTermIdMap.find(leadershipTermId);
    if (it != m_cacheIndexByLeadershipTermIdMap.end())
    {
        return m_entriesCache[it->second].timestamp;
    }
    return aeron::NULL_VALUE;
}

inline void RecordingLog::invalidateEntry(std::int32_t index)
{
    if (index < 0 || index >= static_cast<std::int32_t>(m_entriesCache.size()))
    {
        throw ClusterException("invalid entry index: " + std::to_string(index), SOURCEINFO);
    }
    
    Entry invalidEntry = m_entriesCache[index].invalidate();
    m_entriesCache[index] = invalidEntry;
    
    if (ENTRY_TYPE_TERM == invalidEntry.type)
    {
        m_cacheIndexByLeadershipTermIdMap.erase(invalidEntry.leadershipTermId);
    }
    else if (ENTRY_TYPE_SNAPSHOT == invalidEntry.type || ENTRY_TYPE_STANDBY_SNAPSHOT == invalidEntry.type)
    {
        m_invalidSnapshots.push_back(index);
    }
    
    const std::int32_t invalidEntryType = ENTRY_TYPE_INVALID_FLAG | invalidEntry.type;
    m_buffer.putInt32(0, invalidEntryType);
    persistToStorage(invalidEntry.position, ENTRY_TYPE_OFFSET, SIZE_OF_INT);
}

inline void RecordingLog::removeEntry(std::int64_t leadershipTermId, std::int32_t entryIndex)
{
    Entry* entryToRemove = nullptr;
    std::size_t removeIndex = 0;
    
    for (int i = static_cast<int>(m_entriesCache.size()) - 1; i >= 0; i--)
    {
        Entry& entry = m_entriesCache[i];
        if (entry.leadershipTermId == leadershipTermId && entry.entryIndex == entryIndex)
        {
            entryToRemove = &entry;
            removeIndex = i;
            break;
        }
    }
    
    if (!entryToRemove)
    {
        throw ClusterException("unknown entry index: " + std::to_string(entryIndex), SOURCEINFO);
    }
    
    m_buffer.putInt32(0, aeron::NULL_VALUE);
    persistToStorage(entryToRemove->position, ENTRY_TYPE_OFFSET, SIZE_OF_INT);
    
    reload();
}

inline std::map<std::string, std::vector<RecordingLog::Entry>> RecordingLog::latestStandbySnapshots(std::int32_t serviceCount)
{
    std::map<std::string, std::map<std::int64_t, std::vector<Entry>>> standbySnapshots;
    
    for (int i = static_cast<int>(m_entriesCache.size()) - 1; i >= 0; i--)
    {
        Entry& entry = m_entriesCache[i];
        if (ENTRY_TYPE_STANDBY_SNAPSHOT == entry.type && entry.isValid)
        {
            standbySnapshots[entry.archiveEndpoint][entry.logPosition].push_back(entry);
        }
    }
    
    std::map<std::string, std::vector<Entry>> latestStandbySnapshots;
    
    for (auto& endpointPair : standbySnapshots)
    {
        auto& logPositionMap = endpointPair.second;
        while (!logPositionMap.empty())
        {
            auto lastEntry = logPositionMap.rbegin();
            const int snapshotCount = serviceCount + 1;
            if (static_cast<int>(lastEntry->second.size()) == snapshotCount)
            {
                latestStandbySnapshots[endpointPair.first] = lastEntry->second;
                break;
            }
            else
            {
                logPositionMap.erase(lastEntry->first);
            }
        }
    }
    
    return latestStandbySnapshots;
}

inline void RecordingLog::addSnapshots(
    std::vector<Snapshot>& snapshots,
    const std::vector<Entry>& entries,
    std::int32_t serviceCount,
    std::int32_t snapshotIndex)
{
    const Entry& snapshot = entries[snapshotIndex];
    snapshots.push_back(Snapshot(
        snapshot.recordingId,
        snapshot.leadershipTermId,
        snapshot.termBaseLogPosition,
        snapshot.logPosition,
        snapshot.timestamp,
        snapshot.serviceId));
    
    for (int i = 1; i <= serviceCount; i++)
    {
        if ((snapshotIndex - i) < 0)
        {
            throw ClusterException("snapshot missing for service at index " + std::to_string(i), SOURCEINFO);
        }
        
        const Entry& entry = entries[snapshotIndex - i];
        if (!isValidSnapshot(entry) || entry.serviceId != i)
        {
            throw ClusterException("snapshot missing for service at index " + std::to_string(i), SOURCEINFO);
        }
        
        snapshots.push_back(Snapshot(
            entry.recordingId,
            entry.leadershipTermId,
            entry.termBaseLogPosition,
            entry.logPosition,
            entry.timestamp,
            entry.serviceId));
    }
}

inline void RecordingLog::planRecovery(
    std::vector<Snapshot>& snapshots,
    std::shared_ptr<Log>& logRef,
    const std::vector<Entry>& entries,
    std::shared_ptr<AeronArchive> archive,
    std::int32_t serviceCount,
    std::int64_t replicatedRecordingId)
{
    if (entries.empty())
    {
        if (aeron::NULL_VALUE != replicatedRecordingId)
        {
            RecordingExtent recordingExtent;
            if (archive->listRecording(replicatedRecordingId, recordingExtent) == 0)
            {
                throw ClusterException("unknown recording id: " + std::to_string(replicatedRecordingId), SOURCEINFO);
            }
            
            logRef = std::make_shared<Log>(
                replicatedRecordingId,
                aeron::NULL_VALUE,
                0,
                0,
                recordingExtent.startPosition,
                recordingExtent.stopPosition,
                recordingExtent.initialTermId,
                recordingExtent.termBufferLength,
                recordingExtent.mtuLength,
                recordingExtent.sessionId);
        }
        
        return;
    }
    
    int logIndex = -1;
    int snapshotIndex = -1;
    
    const std::int32_t CONSENSUS_MODULE_SERVICE_ID = ConsensusModuleAgent::SERVICE_ID;
    
    for (int i = static_cast<int>(entries.size()) - 1; i >= 0; i--)
    {
        const Entry& entry = entries[i];
        if (-1 == snapshotIndex && isValidSnapshot(entry) &&
            entry.serviceId == CONSENSUS_MODULE_SERVICE_ID)
        {
            snapshotIndex = i;
        }
        else if (-1 == logIndex && isValidTerm(entry) && aeron::NULL_VALUE != entry.recordingId)
        {
            logIndex = i;
        }
        else if (-1 != snapshotIndex && -1 != logIndex)
        {
            break;
        }
    }
    
    if (-1 != snapshotIndex)
    {
        addSnapshots(snapshots, entries, serviceCount, snapshotIndex);
    }
    
    if (-1 != logIndex)
    {
        const Entry& entry = entries[logIndex];
        RecordingExtent recordingExtent;
        if (archive->listRecording(entry.recordingId, recordingExtent) == 0)
        {
            throw ClusterException("unknown recording id: " + std::to_string(entry.recordingId), SOURCEINFO);
        }
        
        const std::int64_t startPosition = (-1 == snapshotIndex) ?
            recordingExtent.startPosition : snapshots[0].logPosition;
        
        logRef = std::make_shared<Log>(
            entry.recordingId,
            entry.leadershipTermId,
            entry.termBaseLogPosition,
            entry.logPosition,
            startPosition,
            recordingExtent.stopPosition,
            recordingExtent.initialTermId,
            recordingExtent.termBufferLength,
            recordingExtent.mtuLength,
            recordingExtent.sessionId);
    }
}

inline RecordingLog::RecoveryPlan RecordingLog::createRecoveryPlan(
    std::shared_ptr<AeronArchive> archive,
    std::int32_t serviceCount,
    std::int64_t replicatedRecordingId)
{
    std::vector<Snapshot> snapshots;
    std::shared_ptr<Log> logRef;
    planRecovery(snapshots, logRef, m_entriesCache, archive, serviceCount, replicatedRecordingId);
    
    std::int64_t lastLeadershipTermId = aeron::NULL_VALUE;
    std::int64_t lastTermBaseLogPosition = 0;
    std::int64_t committedLogPosition = 0;
    std::int64_t appendedLogPosition = 0;
    
    const std::size_t snapshotStepsSize = snapshots.size();
    if (snapshotStepsSize > 0)
    {
        const Snapshot& snapshot = snapshots[0];
        
        lastLeadershipTermId = snapshot.leadershipTermId;
        lastTermBaseLogPosition = snapshot.termBaseLogPosition;
        appendedLogPosition = snapshot.logPosition;
        committedLogPosition = snapshot.logPosition;
    }
    
    if (logRef)
    {
        lastLeadershipTermId = logRef->leadershipTermId;
        lastTermBaseLogPosition = logRef->termBaseLogPosition;
        appendedLogPosition = logRef->stopPosition;
        committedLogPosition = (AeronArchive::NULL_POSITION != logRef->logPosition) ? 
            logRef->logPosition : committedLogPosition;
    }
    
    return RecoveryPlan(
        lastLeadershipTermId,
        lastTermBaseLogPosition,
        appendedLogPosition,
        committedLogPosition,
        snapshots,
        logRef);
}

inline void RecordingLog::ensureCoherent(
    std::int64_t recordingId,
    std::int64_t initialLogLeadershipTermId,
    std::int64_t initialTermBaseLogPosition,
    std::int64_t leadershipTermId,
    std::int64_t termBaseLogPosition,
    std::int64_t logPosition,
    std::int64_t nowNs,
    std::int64_t timestamp,
    std::int32_t fileSyncLevel)
{
    validateTermRecordingId(recordingId);
    
    Entry* entry = findTermEntry(leadershipTermId);
    if (!entry)
    {
        throw ClusterException("unknown leadershipTermId=" + std::to_string(leadershipTermId), SOURCEINFO);
    }
    
    if (entry->recordingId != recordingId ||
        entry->termBaseLogPosition != termBaseLogPosition ||
        entry->logPosition != logPosition)
    {
        throw ClusterException(
            "recording log entry mismatch: expected recordingId=" + std::to_string(recordingId) +
            " termBaseLogPosition=" + std::to_string(termBaseLogPosition) +
            " logPosition=" + std::to_string(logPosition) +
            " actual recordingId=" + std::to_string(entry->recordingId) +
            " termBaseLogPosition=" + std::to_string(entry->termBaseLogPosition) +
            " logPosition=" + std::to_string(entry->logPosition),
            SOURCEINFO);
    }
    
    if (entry->timestamp != timestamp)
    {
        m_buffer.putInt64(0, timestamp);
        persistToStorage(entry->position, TIMESTAMP_OFFSET, SIZE_OF_LONG);
        // Update entry in cache
        Entry updatedEntry = *entry;
        updatedEntry.timestamp = timestamp;
        m_entriesCache[m_cacheIndexByLeadershipTermIdMap[leadershipTermId]] = updatedEntry;
    }
    
    force(fileSyncLevel);
}

inline std::string RecordingLog::toString() const
{
    std::string result = "RecordingLog{entries=[";
    for (size_t i = 0; i < m_entriesCache.size(); i++)
    {
        if (i > 0) result += ", ";
        result += m_entriesCache[i].toString();
    }
    result += "]}";
    return result;
}

}}



