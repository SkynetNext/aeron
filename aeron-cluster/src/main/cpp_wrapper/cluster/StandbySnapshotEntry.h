#pragma once
#include <string>
#include <cstdint>

namespace aeron { namespace cluster
{

class StandbySnapshotEntry
{
public:
    StandbySnapshotEntry(
        std::int64_t recordingId,
        std::int64_t leadershipTermId,
        std::int64_t termBaseLogPosition,
        std::int64_t logPosition,
        std::int64_t timestamp,
        std::int32_t serviceId,
        const std::string& archiveEndpoint);

    std::int64_t recordingId() const;
    std::int64_t leadershipTermId() const;
    std::int64_t termBaseLogPosition() const;
    std::int64_t logPosition() const;
    std::int64_t timestamp() const;
    std::int32_t serviceId() const;
    std::string archiveEndpoint() const;

private:
    std::int64_t m_recordingId;
    std::int64_t m_leadershipTermId;
    std::int64_t m_termBaseLogPosition;
    std::int64_t m_logPosition;
    std::int64_t m_timestamp;
    std::int32_t m_serviceId;
    std::string m_archiveEndpoint;
};

// Implementation
inline StandbySnapshotEntry::StandbySnapshotEntry(
    std::int64_t recordingId,
    std::int64_t leadershipTermId,
    std::int64_t termBaseLogPosition,
    std::int64_t logPosition,
    std::int64_t timestamp,
    std::int32_t serviceId,
    const std::string& archiveEndpoint) :
    m_recordingId(recordingId),
    m_leadershipTermId(leadershipTermId),
    m_termBaseLogPosition(termBaseLogPosition),
    m_logPosition(logPosition),
    m_timestamp(timestamp),
    m_serviceId(serviceId),
    m_archiveEndpoint(archiveEndpoint)
{
}

inline std::int64_t StandbySnapshotEntry::recordingId() const
{
    return m_recordingId;
}

inline std::int64_t StandbySnapshotEntry::leadershipTermId() const
{
    return m_leadershipTermId;
}

inline std::int64_t StandbySnapshotEntry::termBaseLogPosition() const
{
    return m_termBaseLogPosition;
}

inline std::int64_t StandbySnapshotEntry::logPosition() const
{
    return m_logPosition;
}

inline std::int64_t StandbySnapshotEntry::timestamp() const
{
    return m_timestamp;
}

inline std::int32_t StandbySnapshotEntry::serviceId() const
{
    return m_serviceId;
}

inline std::string StandbySnapshotEntry::archiveEndpoint() const
{
    return m_archiveEndpoint;
}

}}
