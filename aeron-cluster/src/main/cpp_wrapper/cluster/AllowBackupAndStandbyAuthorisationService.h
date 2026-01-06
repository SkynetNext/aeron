#pragma once
#include <vector>
#include "security/AuthorisationService.h"
#include "generated/aeron_cluster_client/MessageHeader.h"
#include "generated/aeron_cluster_client/BackupQuery.h"
#include "generated/aeron_cluster_client/HeartbeatRequest.h"
#include "generated/aeron_cluster_client/StandbySnapshot.h"

namespace aeron { namespace cluster
{
using namespace aeron::security;
using namespace aeron::cluster::codecs;

/**
 * An AuthorisationService that allows the actions required by Cluster Backup
 * and Aeron Cluster Standby.
 */
class AllowBackupAndStandbyAuthorisationService : public AuthorisationService
{
public:
    /**
     * As there is no instance state then this object can be used to save on allocation.
     */
    static const AllowBackupAndStandbyAuthorisationService INSTANCE;

    bool isAuthorised(
        std::int32_t protocolId,
        std::int32_t actionId,
        const void* type,
        const std::vector<std::uint8_t>& encodedPrincipal) override;
};

// Implementation
inline const AllowBackupAndStandbyAuthorisationService AllowBackupAndStandbyAuthorisationService::INSTANCE{};

inline bool AllowBackupAndStandbyAuthorisationService::isAuthorised(
    std::int32_t protocolId,
    std::int32_t actionId,
    const void* type,
    const std::vector<std::uint8_t>& encodedPrincipal)
{
    // Java: MessageHeaderDecoder.SCHEMA_ID == protocolId
    // C++: MessageHeader::sbeSchemaId()
    return MessageHeader::sbeSchemaId() == protocolId &&
        (BackupQuery::sbeTemplateId() == actionId ||
         HeartbeatRequest::sbeTemplateId() == actionId ||
         StandbySnapshot::sbeTemplateId() == actionId);
}

}}
