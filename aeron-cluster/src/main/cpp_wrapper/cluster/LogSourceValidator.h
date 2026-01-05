#pragma once
#include "ClusterBackup.h"
#include "Aeron.h"
#include "../client/ClusterExceptions.h"
#include "util/Exceptions.h"

namespace aeron { namespace cluster
{

class LogSourceValidator
{
public:
    explicit LogSourceValidator(ClusterBackup::SourceType sourceType);

    bool isAcceptable(std::int64_t leaderMemberId, std::int64_t memberId);

private:
    ClusterBackup::SourceType m_sourceType;
};

// Implementation
inline LogSourceValidator::LogSourceValidator(ClusterBackup::SourceType sourceType) :
    m_sourceType(sourceType)
{
}

inline bool LogSourceValidator::isAcceptable(std::int64_t leaderMemberId, std::int64_t memberId)
{
    switch (m_sourceType)
    {
        case ClusterBackup::SourceType::LEADER:
            return aeron::NULL_VALUE != leaderMemberId && leaderMemberId == memberId;
        case ClusterBackup::SourceType::FOLLOWER:
            return aeron::NULL_VALUE == leaderMemberId || leaderMemberId != memberId;
        case ClusterBackup::SourceType::ANY:
            return true;
        default:
            throw IllegalStateException("Unknown sourceType=" + std::to_string(static_cast<int>(m_sourceType)), SOURCEINFO);
    }
}

}}

