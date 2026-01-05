#pragma once
#include <string>
#include <sstream>
#include "archive/client/AeronArchive.h"

namespace aeron { namespace cluster
{
using namespace aeron::archive::client;

/**
 * The extent covered by a recording in the archive in terms of position and time.
 */
class RecordingExtent : public RecordingDescriptorConsumer
{
public:
    /**
     * Id of the recording.
     */
    std::int64_t recordingId = 0;

    /**
     * Start position.
     */
    std::int64_t startPosition = 0;

    /**
     * Stop position.
     */
    std::int64_t stopPosition = 0;

    /**
     * Initial term id.
     */
    std::int32_t initialTermId = 0;

    /**
     * Term buffer length.
     */
    std::int32_t termBufferLength = 0;

    /**
     * MTU length.
     */
    std::int32_t mtuLength = 0;

    /**
     * Session id.
     */
    std::int32_t sessionId = 0;

    void onRecordingDescriptor(
        std::int64_t controlSessionId,
        std::int64_t correlationId,
        std::int64_t recordingId,
        std::int64_t startTimestamp,
        std::int64_t stopTimestamp,
        std::int64_t startPosition,
        std::int64_t stopPosition,
        std::int32_t initialTermId,
        std::int32_t segmentFileLength,
        std::int32_t termBufferLength,
        std::int32_t mtuLength,
        std::int32_t sessionId,
        std::int32_t streamId,
        const std::string& strippedChannel,
        const std::string& originalChannel,
        const std::string& sourceIdentity) override;

    std::string toString() const;
};

// Implementation
inline void RecordingExtent::onRecordingDescriptor(
    std::int64_t /* controlSessionId */,
    std::int64_t /* correlationId */,
    std::int64_t recordingId,
    std::int64_t /* startTimestamp */,
    std::int64_t /* stopTimestamp */,
    std::int64_t startPosition,
    std::int64_t stopPosition,
    std::int32_t initialTermId,
    std::int32_t /* segmentFileLength */,
    std::int32_t termBufferLength,
    std::int32_t mtuLength,
    std::int32_t sessionId,
    std::int32_t /* streamId */,
    const std::string& /* strippedChannel */,
    const std::string& /* originalChannel */,
    const std::string& /* sourceIdentity */)
{
    this->recordingId = recordingId;
    this->startPosition = startPosition;
    this->stopPosition = stopPosition;
    this->initialTermId = initialTermId;
    this->termBufferLength = termBufferLength;
    this->mtuLength = mtuLength;
    this->sessionId = sessionId;
}

inline std::string RecordingExtent::toString() const
{
    std::ostringstream oss;
    oss << "RecordingExtent{recordingId=" << recordingId
        << ", startPosition=" << startPosition
        << ", stopPosition=" << stopPosition
        << ", initialTermId=" << initialTermId
        << ", termBufferLength=" << termBufferLength
        << ", mtuLength=" << mtuLength
        << ", sessionId=" << sessionId << "}";
    return oss.str();
}

}}

