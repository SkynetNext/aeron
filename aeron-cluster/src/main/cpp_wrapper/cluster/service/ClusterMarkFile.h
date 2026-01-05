#pragma once

#include <memory>
#include <string>
#include <filesystem>
#include <functional>
#include <cstdint>
#include "../client/ClusterExceptions.h"
#include "util/Exceptions.h"
#include "util/BitUtil.h"
#include "util/IoUtil.h"
#include "util/SemanticVersion.h"
#include "util/SystemUtil.h"
#include "concurrent/AtomicBuffer.h"
#include "concurrent/EpochClock.h"
#include "concurrent/UnsafeBuffer.h"
#include "logbuffer/LogBufferDescriptor.h"
#include "generated/aeron_cluster_mark/MessageHeader.h"
#include "generated/aeron_cluster_mark/MarkFileHeader.h"
#include "generated/aeron_cluster_mark/ClusterComponentType.h"
#include "ClusterNodeControlProperties.h"

namespace aeron { namespace cluster { namespace service {

using namespace aeron::concurrent;
using namespace aeron::util;
using namespace aeron::cluster::codecs::mark;

// Forward declaration - MarkFile needs to be implemented or available
class MarkFile;

/**
 * Used to indicate if a cluster component is running and what configuration it is using.
 */
class ClusterMarkFile
{
public:
    static constexpr std::int32_t MAJOR_VERSION = 0;
    static constexpr std::int32_t MINOR_VERSION = 3;
    static constexpr std::int32_t PATCH_VERSION = 0;
    static constexpr std::int32_t SEMANTIC_VERSION = SemanticVersion::compose(MAJOR_VERSION, MINOR_VERSION, PATCH_VERSION);
    static constexpr std::int32_t HEADER_LENGTH = 8 * 1024;
    static constexpr std::int32_t VERSION_FAILED = -1;
    static constexpr std::int32_t ERROR_BUFFER_MIN_LENGTH = 1024 * 1024;
    static constexpr std::int32_t ERROR_BUFFER_MAX_LENGTH = std::numeric_limits<std::int32_t>::max() - HEADER_LENGTH;
    static constexpr const char* FILE_EXTENSION = ".dat";
    static constexpr const char* LINK_FILE_EXTENSION = ".lnk";
    static constexpr const char* FILENAME = "cluster-mark.dat";
    static constexpr const char* LINK_FILENAME = "cluster-mark.lnk";
    static constexpr const char* SERVICE_FILENAME_PREFIX = "cluster-mark-service-";

    /**
     * Create new MarkFile for a cluster component but check if an existing component is active.
     */
    ClusterMarkFile(
        const std::filesystem::path& file,
        ClusterComponentType type,
        std::int32_t errorBufferLength,
        std::shared_ptr<EpochClock> epochClock,
        std::int64_t timeoutMs,
        std::int32_t filePageSize);

    /**
     * Construct to read the status of an existing MarkFile for a cluster component.
     */
    ClusterMarkFile(
        const std::filesystem::path& directory,
        const std::string& filename,
        std::shared_ptr<EpochClock> epochClock,
        std::int64_t timeoutMs,
        std::function<void(const std::string&)> logger);

    /**
     * Construct from existing MarkFile.
     */
    explicit ClusterMarkFile(std::shared_ptr<MarkFile> markFile);

    ~ClusterMarkFile();

    void close();

    std::filesystem::path parentDirectory() const;

    static bool isServiceMarkFile(const std::filesystem::path& path);
    static bool isConsensusModuleMarkFile(const std::filesystem::path& path);

    bool isClosed() const;

    std::int64_t candidateTermId() const;

    std::int32_t memberId() const;
    void memberId(std::int32_t memberId);

    std::int32_t clusterId() const;
    void clusterId(std::int32_t clusterId);

    void signalReady();
    void signalReady(std::int64_t activityTimestamp);
    void signalFailedStart();
    void signalTerminated();
    void updateActivityTimestamp(std::int64_t nowMs);
    std::int64_t activityTimestampVolatile() const;

    MarkFileHeaderEncoder& encoder();
    MarkFileHeaderDecoder& decoder();

    AtomicBuffer errorBuffer() const;

    static void saveExistingErrors(
        const std::filesystem::path& markFile,
        AtomicBuffer& errorBuffer,
        ClusterComponentType type,
        std::ostream& logger);

    static void checkHeaderLength(
        const std::string& aeronDirectory,
        const std::string& controlChannel,
        const std::string& ingressChannel,
        const std::string& serviceName,
        const std::string& authenticator);

    static std::string markFilenameForService(std::int32_t serviceId);
    static std::string linkFilenameForService(std::int32_t serviceId);

    std::shared_ptr<ClusterNodeControlProperties> loadControlProperties();

    void force();

    std::string toString() const;

private:
    void signalReady(std::int32_t version, std::int64_t activityTimestamp);
    static std::int32_t headerOffset(const std::filesystem::path& file);
    static std::int32_t headerOffset(UnsafeBuffer& headerBuffer);
    static std::shared_ptr<MarkFile> openExistingMarkFile(
        const std::filesystem::path& directory,
        const std::string& filename,
        std::shared_ptr<EpochClock> epochClock,
        std::int64_t timeoutMs,
        std::function<void(const std::string&)> logger);

    static constexpr std::int32_t HEADER_OFFSET = MessageHeaderDecoder::ENCODED_LENGTH;

    MarkFileHeaderDecoder m_headerDecoder;
    MarkFileHeaderEncoder m_headerEncoder;
    std::shared_ptr<MarkFile> m_markFile;
    UnsafeBuffer m_buffer;
    UnsafeBuffer m_errorBuffer;
    std::int32_t m_headerOffset;
};

}}}

