#pragma once

#include <string>
#include <memory>
#include <cstdint>
#include <filesystem>
#include "Aeron.h"
#include "../client/ClusterExceptions.h"
#include "../service/ClusterMarkFile.h"
#include "concurrent/AtomicBuffer.h"
#include "util/IoUtil.h"
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
    std::unique_ptr<IoUtil::MappedFile> m_mappedFile;
    AtomicBuffer m_buffer;
    NodeStateHeaderDecoder m_nodeStateHeaderDecoder;
    MessageHeaderDecoder m_messageHeaderDecoder;
    CandidateTermDecoder m_candidateTermDecoder;
    std::int32_t m_candidateTermIdOffset;
    std::unique_ptr<CandidateTerm> m_candidateTerm;
};

}}

