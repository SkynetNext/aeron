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

}}

