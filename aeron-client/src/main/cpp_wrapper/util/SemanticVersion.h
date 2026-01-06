#pragma once

#include <cstdint>
#include <string>
#include "MacroUtil.h"

namespace aeron { namespace util {

/**
 * Utility class for semantic versioning.
 */
class SemanticVersion
{
public:
    /**
     * Compose a semantic version from major, minor, and patch components.
     */
    static constexpr std::int32_t compose(
        std::uint8_t major, std::uint8_t minor, std::uint8_t patch) noexcept
    {
        return semanticVersionCompose(major, minor, patch);
    }

    /**
     * Extract the major version from a semantic version.
     */
    static constexpr std::uint8_t major(std::int32_t version) noexcept
    {
        return semanticVersionMajor(version);
    }

    /**
     * Extract the minor version from a semantic version.
     */
    static constexpr std::uint8_t minor(std::int32_t version) noexcept
    {
        return semanticVersionMinor(version);
    }

    /**
     * Extract the patch version from a semantic version.
     */
    static constexpr std::uint8_t patch(std::int32_t version) noexcept
    {
        return semanticVersionPatch(version);
    }

    /**
     * Convert a semantic version to a string representation.
     */
    static std::string toString(std::int32_t version) noexcept
    {
        return semanticVersionToString(version);
    }
};

}}

