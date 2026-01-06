#pragma once

#include <string>
#include <memory>
#include <cstdint>
#include <filesystem>
#include "Aeron.h"
#include "../client/ClusterExceptions.h"
#include "../service/ClusterMarkFile.h"
#include "concurrent/AtomicBuffer.h"
#include "util/MemoryMappedFile.h"
#include "util/BitUtil.h"
#include "util/SemanticVersion.h"
#include "generated/aeron_cluster_node/MessageHeader.h"
#include "generated/aeron_cluster_node/NodeStateHeader.h"
#include "generated/aeron_cluster_node/CandidateTerm.h"
#include "generated/aeron_cluster_node/NodeStateFooter.h"

namespace aeron { namespace cluster {

using namespace aeron::concurrent;
using namespace aeron::cluster::codecs::node;
using namespace aeron::util;

/**
 * An extensible list of information relating to a specific cluster node. Used to track persistent state that is node
 * specific and shouldn't be present in the snapshot. E.g. candidateTermId.
 */
class NodeStateFile
{
public:
    /**
     * File name.
     */
    static constexpr const char* FILENAME = "node-state.dat";
    static constexpr std::int32_t MINIMUM_FILE_LENGTH = 1 << 20;

    /**
     * Wrapper class for the candidate term.
     */
    class CandidateTerm
    {
    public:
        CandidateTerm(NodeStateFile& nodeStateFile, CandidateTermDecoder& decoder, std::int32_t candidateTermIdOffset);

        /**
         * Gets the current candidateTermId.
         *
         * @return the candidateTermId.
         */
        std::int64_t candidateTermId() const;

        /**
         * Get the timestamp of the latest candidateTermId update.
         *
         * @return epoch timestamp in ms.
         */
        std::int64_t timestamp() const;

        /**
         * Get the log position of the latest candidateTermId update.
         *
         * @return log position.
         */
        std::int64_t logPosition() const;

    private:
        NodeStateFile& m_nodeStateFile;
        CandidateTermDecoder& m_decoder;
        std::int32_t m_candidateTermIdOffset;
    };

    /**
     * Construct the NodeStateFile.
     *
     * @param clusterDir    directory containing the NodeStateFile.
     * @param createNew     whether a new file should be created if one does not already exist.
     * @param fileSyncLevel whether the mapped byte buffer should be synchronised with the underlying filesystem on
     *                      change.
     * @throws std::runtime_error if there is an error creating the file or createNew == false and the file does
     * not already exist.
     */
    NodeStateFile(
        const std::filesystem::path& clusterDir,
        bool createNew,
        std::int32_t fileSyncLevel);

    ~NodeStateFile();

    /**
     * Close and unmap the file.
     */
    void close();

    /**
     * Set the current candidate term id with associated information.
     *
     * @param candidateTermId current candidate term id.
     * @param logPosition    log position where the term id change occurred.
     * @param timestampMs    timestamp of the candidate term id change.
     */
    void updateCandidateTermId(std::int64_t candidateTermId, std::int64_t logPosition, std::int64_t timestampMs);

    /**
     * Set the current candidate term id with associated information.
     *
     * @param candidateTermId current candidate term id.
     * @param logPosition     log position where the term id change occurred.
     * @param timestampMs     timestamp of the candidate term id change.
     * @return the new candidate term id.
     */
    std::int64_t proposeMaxCandidateTermId(
        std::int64_t candidateTermId,
        std::int64_t logPosition,
        std::int64_t timestampMs);

    /**
     * Get the reference to CandidateTerm wrapper that can be used to fetch the values associated with the current
     * candidate term.
     *
     * @return the CandidateTerm wrapper.
     */
    CandidateTerm& candidateTerm();

private:
    static void verifyAlignment(std::int32_t offset);

    static void loadInitialState(
        AtomicBuffer& buffer,
        NodeStateHeaderDecoder& nodeStateHeaderDecoder,
        CandidateTermDecoder& candidateTermDecoder,
        MessageHeaderDecoder& messageHeaderDecoder);

    static void initialiseDecodersOnCreation(
        AtomicBuffer& buffer,
        NodeStateHeaderDecoder& nodeStateHeaderDecoder,
        MessageHeaderDecoder& messageHeaderDecoder,
        CandidateTermDecoder& candidateTermDecoder);

    std::int32_t calculateAndVerifyCandidateTermIdOffset();

    void loadDecodersAndOffsets(AtomicBuffer& buffer);

    static std::int64_t scanForMessageTypeOffset(
        std::int32_t startPosition,
        std::int32_t templateId,
        AtomicBuffer& buffer,
        MessageHeaderDecoder& messageHeaderDecoder);

    void syncFile();

    std::int32_t m_fileSyncLevel;
    std::shared_ptr<MemoryMappedFile> m_mappedFile;
    AtomicBuffer m_buffer;
    NodeStateHeaderDecoder m_nodeStateHeaderDecoder;
    MessageHeaderDecoder m_messageHeaderDecoder;
    CandidateTermDecoder m_candidateTermDecoder;
    std::int32_t m_candidateTermIdOffset;
    std::unique_ptr<CandidateTerm> m_candidateTerm;
};

// Implementation
namespace {
    static constexpr std::int32_t ALIGNMENT = sizeof(std::int64_t); // 8 bytes, same as Java UnsafeBuffer.ALIGNMENT
}

inline void NodeStateFile::verifyAlignment(std::int32_t offset)
{
    if (0 != (offset & (ALIGNMENT - 1)))
    {
        throw IllegalStateException(
            "offset=" + std::to_string(offset) + " is not correctly aligned, it is not divisible by " + std::to_string(ALIGNMENT),
            SOURCEINFO);
    }
}

inline NodeStateFile::CandidateTerm::CandidateTerm(
    NodeStateFile& nodeStateFile,
    CandidateTermDecoder& decoder,
    std::int32_t candidateTermIdOffset) :
    m_nodeStateFile(nodeStateFile),
    m_decoder(decoder),
    m_candidateTermIdOffset(candidateTermIdOffset)
{
}

inline std::int64_t NodeStateFile::CandidateTerm::candidateTermId() const
{
    return m_nodeStateFile.m_buffer.getInt64Volatile(m_candidateTermIdOffset);
}

inline std::int64_t NodeStateFile::CandidateTerm::timestamp() const
{
    return m_decoder.timestamp();
}

inline std::int64_t NodeStateFile::CandidateTerm::logPosition() const
{
    return m_decoder.logPosition();
}

inline NodeStateFile::NodeStateFile(
    const std::filesystem::path& clusterDir,
    bool createNew,
    std::int32_t fileSyncLevel) :
    m_fileSyncLevel(fileSyncLevel)
{
    std::filesystem::path nodeStateFilePath = clusterDir / FILENAME;
    std::shared_ptr<MemoryMappedFile> mappedFile = nullptr;
    AtomicBuffer buffer(nullptr, 0);

    try
    {
        if (!std::filesystem::exists(nodeStateFilePath))
        {
            if (!createNew)
            {
                throw IOException("NodeStateFile does not exist and createNew=false", SOURCEINFO);
            }

            mappedFile = MemoryMappedFile::createNew(
                nodeStateFilePath.string().c_str(), 0, MINIMUM_FILE_LENGTH, false);
            buffer = AtomicBuffer(mappedFile->getMemoryPtr(), mappedFile->getMemorySize());

            // Verify alignment (Java: buffer.verifyAlignment())
            if (reinterpret_cast<std::uintptr_t>(mappedFile->getMemoryPtr()) % ALIGNMENT != 0)
            {
                throw IllegalStateException("buffer is not correctly aligned", SOURCEINFO);
            }

            initialiseDecodersOnCreation(
                buffer,
                m_nodeStateHeaderDecoder,
                m_messageHeaderDecoder,
                m_candidateTermDecoder);

            m_candidateTermIdOffset = calculateAndVerifyCandidateTermIdOffset();
            buffer.putInt64Ordered(m_candidateTermIdOffset, Aeron::NULL_VALUE);
        }
        else
        {
            mappedFile = MemoryMappedFile::mapExisting(nodeStateFilePath.string().c_str(), 0, 0, false, false);
            buffer = AtomicBuffer(mappedFile->getMemoryPtr(), mappedFile->getMemorySize());

            loadDecodersAndOffsets(buffer);
        }

        syncFile();
    }
    catch (const std::exception& ex)
    {
        // MemoryMappedFile destructor will handle cleanup
        throw;
    }

    m_mappedFile = mappedFile;
    m_buffer = buffer;
    m_candidateTerm = std::make_unique<CandidateTerm>(*this, m_candidateTermDecoder, m_candidateTermIdOffset);
}

inline NodeStateFile::~NodeStateFile()
{
    close();
}

inline void NodeStateFile::close()
{
    m_mappedFile.reset();
}

inline std::int32_t NodeStateFile::calculateAndVerifyCandidateTermIdOffset()
{
    const std::int32_t candidateTermIdOffset =
        m_candidateTermDecoder.offset() + CandidateTermDecoder::candidateTermIdEncodingOffset();
    verifyAlignment(candidateTermIdOffset);
    return candidateTermIdOffset;
}

inline void NodeStateFile::loadInitialState(
    AtomicBuffer& buffer,
    NodeStateHeaderDecoder& nodeStateHeaderDecoder,
    CandidateTermDecoder& candidateTermDecoder,
    MessageHeaderDecoder& messageHeaderDecoder)
{
    nodeStateHeaderDecoder.wrap(
        buffer.buffer(), 0, NodeStateHeaderDecoder::blockLength(), NodeStateHeaderDecoder::schemaVersion());

    const std::int32_t version = nodeStateHeaderDecoder.version();
    if (ClusterMarkFile::MAJOR_VERSION != SemanticVersion::major(version))
    {
        throw ClusterException(
            "mark file major version " + std::to_string(SemanticVersion::major(version)) +
            " does not match software: " + std::to_string(ClusterMarkFile::MAJOR_VERSION),
            SOURCEINFO);
    }

    const std::int64_t footerOffset = scanForMessageTypeOffset(
        nodeStateHeaderDecoder.sbeBlockLength(),
        NodeStateFooterDecoder::sbeTemplateId(),
        buffer,
        messageHeaderDecoder);

    if (Aeron::NULL_VALUE == footerOffset)
    {
        throw IllegalStateException("failed to find NodeStateFooter entry, file corrupt?", SOURCEINFO);
    }

    const std::int64_t candidateTermOffset = scanForMessageTypeOffset(
        nodeStateHeaderDecoder.sbeBlockLength(),
        CandidateTermDecoder::sbeTemplateId(),
        buffer,
        messageHeaderDecoder);

    if (Aeron::NULL_VALUE == candidateTermOffset)
    {
        throw IllegalStateException("failed to find CandidateTerm entry", SOURCEINFO);
    }

    candidateTermDecoder.wrapAndApplyHeader(
        buffer.buffer(), static_cast<std::int32_t>(candidateTermOffset), messageHeaderDecoder);
}

inline void NodeStateFile::initialiseDecodersOnCreation(
    AtomicBuffer& buffer,
    NodeStateHeaderDecoder& nodeStateHeaderDecoder,
    MessageHeaderDecoder& messageHeaderDecoder,
    CandidateTermDecoder& candidateTermDecoder)
{
    MessageHeaderEncoder messageHeaderEncoder;

    nodeStateHeaderDecoder.wrap(
        buffer.buffer(), 0, NodeStateHeaderDecoder::blockLength(), NodeStateHeaderDecoder::schemaVersion());
    NodeStateHeaderEncoder nodeStateHeaderEncoder;
    nodeStateHeaderEncoder.wrap(buffer.buffer(), 0);
    nodeStateHeaderEncoder.version(ClusterMarkFile::SEMANTIC_VERSION);

    const std::int32_t candidateTermFrameOffset = NodeStateHeaderDecoder::blockLength();
    verifyAlignment(candidateTermFrameOffset);

    // Set candidateTermId
    CandidateTermEncoder candidateTermEncoder;
    candidateTermEncoder.wrapAndApplyHeader(
        buffer.buffer(), candidateTermFrameOffset, messageHeaderEncoder);
    candidateTermDecoder.wrapAndApplyHeader(
        buffer.buffer(), candidateTermFrameOffset, messageHeaderDecoder);
    candidateTermEncoder
        .logPosition(Aeron::NULL_VALUE)
        .timestamp(Aeron::NULL_VALUE)
        .candidateTermId(Aeron::NULL_VALUE);
    messageHeaderEncoder.frameLength(MessageHeader::encodedLength() + candidateTermEncoder.encodedLength());

    const std::int32_t footerOffset = candidateTermFrameOffset +
        static_cast<std::int32_t>(BitUtil::align(messageHeaderDecoder.frameLength(), ALIGNMENT));
    NodeStateFooterEncoder nodeStateFooterEncoder;
    nodeStateFooterEncoder.wrapAndApplyHeader(buffer.buffer(), footerOffset, messageHeaderEncoder);
    messageHeaderEncoder.frameLength(MessageHeader::encodedLength() + nodeStateFooterEncoder.encodedLength());
}

inline void NodeStateFile::loadDecodersAndOffsets(AtomicBuffer& buffer)
{
    loadInitialState(
        buffer,
        m_nodeStateHeaderDecoder,
        m_candidateTermDecoder,
        m_messageHeaderDecoder);

    m_candidateTermIdOffset = calculateAndVerifyCandidateTermIdOffset();
}

inline std::int64_t NodeStateFile::scanForMessageTypeOffset(
    std::int32_t startPosition,
    std::int32_t templateId,
    AtomicBuffer& buffer,
    MessageHeaderDecoder& messageHeaderDecoder)
{
    verifyAlignment(startPosition);
    std::int32_t position = startPosition;

    while (position < static_cast<std::int32_t>(buffer.capacity()))
    {
        messageHeaderDecoder.wrap(buffer.buffer(), position);

        const std::int32_t messageLength = messageHeaderDecoder.frameLength();

        if (templateId == messageHeaderDecoder.templateId())
        {
            return position;
        }
        else if (NodeStateFooterEncoder::sbeTemplateId() == messageHeaderDecoder.templateId())
        {
            return Aeron::NULL_VALUE;
        }

        if (messageLength < 0)
        {
            throw IllegalStateException("Message length < 0, file corrupt?", SOURCEINFO);
        }
        else if (0 == messageLength)
        {
            return Aeron::NULL_VALUE;
        }

        position += static_cast<std::int32_t>(BitUtil::align(messageLength, ALIGNMENT));
    }

    return Aeron::NULL_VALUE;
}

inline void NodeStateFile::syncFile()
{
    if (0 < m_fileSyncLevel)
    {
        // MemoryMappedFile doesn't have a force() method, but the destructor will sync
        // For now, we rely on the OS to sync the file
        // TODO: Add force() method to MemoryMappedFile if needed
    }
}

inline void NodeStateFile::updateCandidateTermId(
    std::int64_t candidateTermId,
    std::int64_t logPosition,
    std::int64_t timestampMs)
{
    m_buffer.putInt64(
        m_candidateTermDecoder.offset() + CandidateTermDecoder::logPositionEncodingOffset(),
        logPosition);
    m_buffer.putInt64(
        m_candidateTermDecoder.offset() + CandidateTermDecoder::timestampEncodingOffset(),
        timestampMs);
    m_buffer.putInt64Ordered(m_candidateTermIdOffset, candidateTermId);
    syncFile();
}

inline std::int64_t NodeStateFile::proposeMaxCandidateTermId(
    std::int64_t candidateTermId,
    std::int64_t logPosition,
    std::int64_t timestampMs)
{
    const std::int64_t existingCandidateTermId = m_candidateTerm->candidateTermId();

    if (candidateTermId > existingCandidateTermId)
    {
        updateCandidateTermId(candidateTermId, logPosition, timestampMs);
        return candidateTermId;
    }

    return existingCandidateTermId;
}

inline NodeStateFile::CandidateTerm& NodeStateFile::candidateTerm()
{
    return *m_candidateTerm;
}

}}
