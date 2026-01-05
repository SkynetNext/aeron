#pragma once

#include <iostream>
#include <iomanip>
#include <sstream>
#include <cstdint>
#include <string>
#include <vector>
#include "ConsensusModuleSnapshotListener.h"
#include "aeron_cluster/ClusterTimeUnit.h"
#include "aeron_cluster/CloseReason.h"
#include "util/DirectBuffer.h"

namespace aeron { namespace cluster {

/**
 * Printer for consensus module snapshots.
 */
class ConsensusModuleSnapshotPrinter : public ConsensusModuleSnapshotListener
{
public:
    explicit ConsensusModuleSnapshotPrinter(std::ostream& out);

    void onLoadBeginSnapshot(
        std::int32_t appVersion,
        ClusterTimeUnit timeUnit,
        const util::DirectBuffer& buffer,
        std::int32_t offset,
        std::int32_t length) override;

    void onLoadEndSnapshot(
        const util::DirectBuffer& buffer,
        std::int32_t offset,
        std::int32_t length) override;

    void onLoadConsensusModuleState(
        std::int64_t nextSessionId,
        std::int64_t nextServiceSessionId,
        std::int64_t logServiceSessionId,
        std::int32_t pendingMessageCapacity,
        const util::DirectBuffer& buffer,
        std::int32_t offset,
        std::int32_t length) override;

    void onLoadPendingMessage(
        std::int64_t clusterSessionId,
        const util::DirectBuffer& buffer,
        std::int32_t offset,
        std::int32_t length) override;

    void onLoadClusterSession(
        std::int64_t clusterSessionId,
        std::int64_t correlationId,
        std::int64_t openedLogPosition,
        std::int64_t timeOfLastActivity,
        CloseReason closeReason,
        std::int32_t responseStreamId,
        const std::string& responseChannel,
        const util::DirectBuffer& buffer,
        std::int32_t offset,
        std::int32_t length) override;

    void onLoadTimer(
        std::int64_t correlationId,
        std::int64_t deadline,
        const util::DirectBuffer& buffer,
        std::int32_t offset,
        std::int32_t length) override;

    void onLoadPendingMessageTracker(
        std::int64_t nextServiceSessionId,
        std::int64_t logServiceSessionId,
        std::int32_t pendingMessageCapacity,
        std::int32_t serviceId,
        const util::DirectBuffer& buffer,
        std::int32_t offset,
        std::int32_t length) override;

private:
    static std::string formatHexDump(const std::vector<std::uint8_t>& array, std::int32_t offset, std::int32_t length);

    std::ostream& m_out;
};

// Implementation
inline ConsensusModuleSnapshotPrinter::ConsensusModuleSnapshotPrinter(std::ostream& out) :
    m_out(out)
{
}

inline void ConsensusModuleSnapshotPrinter::onLoadBeginSnapshot(
    std::int32_t appVersion,
    ClusterTimeUnit timeUnit,
    const util::DirectBuffer& buffer,
    std::int32_t offset,
    std::int32_t length)
{
    m_out << "Snapshot:"
          << " appVersion=" << appVersion
          << " timeUnit=" << static_cast<int>(timeUnit) << std::endl;
}

inline void ConsensusModuleSnapshotPrinter::onLoadEndSnapshot(
    const util::DirectBuffer& buffer,
    std::int32_t offset,
    std::int32_t length)
{
    m_out << "End Snapshot, offset=" << offset << ", length=" << length << std::endl;
    std::vector<std::uint8_t> b(length);
    buffer.getBytes(offset, b.data(), 0, length);
    m_out << formatHexDump(b, 0, length);
}

inline void ConsensusModuleSnapshotPrinter::onLoadConsensusModuleState(
    std::int64_t nextSessionId,
    std::int64_t nextServiceSessionId,
    std::int64_t logServiceSessionId,
    std::int32_t pendingMessageCapacity,
    const util::DirectBuffer& buffer,
    std::int32_t offset,
    std::int32_t length)
{
    m_out << "Consensus Module State:"
          << " nextSessionId=" << nextSessionId
          << " nextServiceSessionId=" << nextServiceSessionId
          << " logServiceSessionId=" << logServiceSessionId
          << " pendingMessageCapacity=" << pendingMessageCapacity << std::endl;
    std::vector<std::uint8_t> b(length);
    buffer.getBytes(offset, b.data(), 0, length);
    m_out << formatHexDump(b, 0, length);
}

inline void ConsensusModuleSnapshotPrinter::onLoadPendingMessage(
    std::int64_t clusterSessionId,
    const util::DirectBuffer& buffer,
    std::int32_t offset,
    std::int32_t length)
{
    m_out << "Pending Message:"
          << " length=" << length
          << " clusterSessionId=" << clusterSessionId << std::endl;
}

inline void ConsensusModuleSnapshotPrinter::onLoadClusterSession(
    std::int64_t clusterSessionId,
    std::int64_t correlationId,
    std::int64_t openedLogPosition,
    std::int64_t timeOfLastActivity,
    CloseReason closeReason,
    std::int32_t responseStreamId,
    const std::string& responseChannel,
    const util::DirectBuffer& buffer,
    std::int32_t offset,
    std::int32_t length)
{
    m_out << "Cluster Session:"
          << " clusterSessionId=" << clusterSessionId
          << " correlationId=" << correlationId
          << " openedLogPosition=" << openedLogPosition
          << " timeOfLastActivity=" << timeOfLastActivity
          << " closeReason=" << static_cast<int>(closeReason)
          << " responseStreamId=" << responseStreamId
          << " responseChannel=" << responseChannel << std::endl;
}

inline void ConsensusModuleSnapshotPrinter::onLoadTimer(
    std::int64_t correlationId,
    std::int64_t deadline,
    const util::DirectBuffer& buffer,
    std::int32_t offset,
    std::int32_t length)
{
    m_out << "Timer:"
          << " correlationId=" << correlationId
          << " deadline=" << deadline << std::endl;
}

inline void ConsensusModuleSnapshotPrinter::onLoadPendingMessageTracker(
    std::int64_t nextServiceSessionId,
    std::int64_t logServiceSessionId,
    std::int32_t pendingMessageCapacity,
    std::int32_t serviceId,
    const util::DirectBuffer& buffer,
    std::int32_t offset,
    std::int32_t length)
{
    m_out << "Pending Message Tracker:"
          << " nextServiceSessionId=" << nextServiceSessionId
          << " logServiceSessionId=" << logServiceSessionId
          << " pendingMessageCapacity=" << pendingMessageCapacity
          << " serviceId=" << serviceId << std::endl;
}

inline std::string ConsensusModuleSnapshotPrinter::formatHexDump(
    const std::vector<std::uint8_t>& array,
    std::int32_t offset,
    std::int32_t length)
{
    constexpr int width = 16;
    std::ostringstream builder;

    for (int rowOffset = offset; rowOffset < offset + length; rowOffset += width)
    {
        builder << std::setfill('0') << std::setw(6) << std::hex << rowOffset << ":  ";

        for (int index = 0; index < width; index++)
        {
            if (rowOffset + index < static_cast<int>(array.size()))
            {
                builder << std::setfill('0') << std::setw(2) << std::hex
                        << static_cast<int>(array[rowOffset + index]) << " ";
            }
            else
            {
                builder << "   ";
            }
        }

        if (rowOffset < static_cast<int>(array.size()))
        {
            const int asciiWidth = std::min(width, static_cast<int>(array.size()) - rowOffset);
            builder << "  |  ";
            for (int i = 0; i < asciiWidth; i++)
            {
                char c = static_cast<char>(array[rowOffset + i]);
                if (c == '\r' || c == '\n')
                {
                    builder << ' ';
                }
                else if (c >= 32 && c < 127)
                {
                    builder << c;
                }
                else
                {
                    builder << '.';
                }
            }
        }

        builder << std::endl;
    }

    return builder.str();
}

}}

