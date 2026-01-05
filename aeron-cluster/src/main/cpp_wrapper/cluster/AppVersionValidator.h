#pragma once
#include "util/SemanticVersion.h"

namespace aeron { namespace cluster
{
using namespace aeron::util;

/**
 * Class to be used for determining AppVersion compatibility.
 * 
 * Default is to use SemanticVersion major version for checking compatibility.
 */
class AppVersionValidator
{
public:
    /**
     * Singleton instance of AppVersionValidator version which can be used to avoid allocation.
     */
    static const AppVersionValidator SEMANTIC_VERSIONING_VALIDATOR;

    /**
     * Check version compatibility between configured context appVersion and appVersion in
     * new leadership term or snapshot.
     *
     * @param contextAppVersion   configured appVersion value from context.
     * @param appVersionUnderTest to check against configured appVersion.
     * @return true for compatible or false for not compatible.
     */
    bool isVersionCompatible(std::int32_t contextAppVersion, std::int32_t appVersionUnderTest) const
    {
        return SemanticVersion::major(contextAppVersion) == SemanticVersion::major(appVersionUnderTest);
    }
};

}}

