#pragma once

#include <cstdint>
#include <memory>
#include <string>

// #include <filesystem>  // Not available in C++14, using std::string instead
#include "Aeron.h"
#include "Context.h" // For NULL_VALUE
#include "client/ClusterExceptions.h"
#include "concurrent/AtomicBuffer.h"
#include "generated/aeron_cluster_codecs/CandidateTerm.h"
#include "generated/aeron_cluster_codecs/MessageHeader.h"
#include "generated/aeron_cluster_codecs/NodeStateFooter.h"
#include "generated/aeron_cluster_codecs/NodeStateHeader.h"
#include "service/ClusterMarkFile.h"
#include "util/BitUtil.h"
#include "util/MemoryMappedFile.h"
#include "util/SemanticVersion.h"


namespace aeron {
namespace cluster {

using namespace aeron::concurrent;
using namespace aeron::cluster::codecs;
using namespace aeron::util;

// Type aliases for decoder types (SBE generates without Decoder suffix)
using NodeStateHeaderDecoder = codecs::NodeStateHeader;
using CandidateTermDecoder = codecs::CandidateTerm;
using NodeStateFooterDecoder = codecs::NodeStateFooter;
using MessageHeaderDecoder = codecs::MessageHeader;
using MessageHeader = codecs::MessageHeader;

/**
 * An extensible list of information relating to a specific cluster node. Used
 * to track persistent state that is node specific and shouldn't be present in
 * the snapshot. E.g. candidateTermId.
 */
class NodeStateFile {
public:
  /**
   * File name.
   */
  static constexpr const char *FILENAME = "node-state.dat";
  static constexpr std::int32_t MINIMUM_FILE_LENGTH = 1 << 20;

  /**
   * Wrapper class for the candidate term.
   */
  class CandidateTerm {
  public:
    CandidateTerm(NodeStateFile &nodeStateFile, CandidateTermDecoder &decoder,
                  std::int32_t candidateTermIdOffset);

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
    NodeStateFile &m_nodeStateFile;
    CandidateTermDecoder &m_decoder;
    std::int32_t m_candidateTermIdOffset;
  };

  /**
   * Construct the NodeStateFile.
   *
   * @param clusterDir    directory containing the NodeStateFile.
   * @param createNew     whether a new file should be created if one does not
   * already exist.
   * @param fileSyncLevel whether the mapped byte buffer should be synchronised
   * with the underlying filesystem on change.
   * @throws std::runtime_error if there is an error creating the file or
   * createNew == false and the file does not already exist.
   */
  NodeStateFile(const std::string &clusterDir, bool createNew,
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
  void updateCandidateTermId(std::int64_t candidateTermId,
                             std::int64_t logPosition,
                             std::int64_t timestampMs);

  /**
   * Set the current candidate term id with associated information.
   *
   * @param candidateTermId current candidate term id.
   * @param logPosition     log position where the term id change occurred.
   * @param timestampMs     timestamp of the candidate term id change.
   * @return the new candidate term id.
   */
  std::int64_t proposeMaxCandidateTermId(std::int64_t candidateTermId,
                                         std::int64_t logPosition,
                                         std::int64_t timestampMs);

  /**
   * Get the reference to CandidateTerm wrapper that can be used to fetch the
   * values associated with the current candidate term.
   *
   * @return the CandidateTerm wrapper.
   */
  CandidateTerm &candidateTerm();

private:
  static void verifyAlignment(std::int32_t offset);

  static void loadInitialState(AtomicBuffer &buffer,
                               NodeStateHeaderDecoder &nodeStateHeaderDecoder,
                               CandidateTermDecoder &candidateTermDecoder,
                               MessageHeaderDecoder &messageHeaderDecoder);

  static void
  initialiseDecodersOnCreation(AtomicBuffer &buffer,
                               NodeStateHeaderDecoder &nodeStateHeaderDecoder,
                               MessageHeaderDecoder &messageHeaderDecoder,
                               CandidateTermDecoder &candidateTermDecoder);

  std::int32_t calculateAndVerifyCandidateTermIdOffset();

  void loadDecodersAndOffsets(AtomicBuffer &buffer);

  static std::int64_t
  scanForMessageTypeOffset(std::int32_t startPosition, std::int32_t templateId,
                           AtomicBuffer &buffer,
                           MessageHeaderDecoder &messageHeaderDecoder);

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
static constexpr std::int32_t ALIGNMENT =
    sizeof(std::int64_t); // 8 bytes, same as Java UnsafeBuffer.ALIGNMENT
}

inline void NodeStateFile::verifyAlignment(std::int32_t offset) {
  if (0 != (offset & (ALIGNMENT - 1))) {
    throw IllegalStateException(
        "offset=" + std::to_string(offset) +
            " is not correctly aligned, it is not divisible by " +
            std::to_string(ALIGNMENT),
        SOURCEINFO);
  }
}

inline NodeStateFile::CandidateTerm::CandidateTerm(
    NodeStateFile &nodeStateFile, CandidateTermDecoder &decoder,
    std::int32_t candidateTermIdOffset)
    : m_nodeStateFile(nodeStateFile), m_decoder(decoder),
      m_candidateTermIdOffset(candidateTermIdOffset) {}

inline std::int64_t NodeStateFile::CandidateTerm::candidateTermId() const {
  return m_nodeStateFile.m_buffer.getInt64Volatile(m_candidateTermIdOffset);
}

inline std::int64_t NodeStateFile::CandidateTerm::timestamp() const {
  return m_decoder.timestamp();
}

inline std::int64_t NodeStateFile::CandidateTerm::logPosition() const {
  return m_decoder.logPosition();
}

inline NodeStateFile::NodeStateFile(const std::string &clusterDir,
                                    bool createNew, std::int32_t fileSyncLevel)
    : m_fileSyncLevel(fileSyncLevel) {
  std::string nodeStateFilePath = clusterDir + "/" + FILENAME;
  std::shared_ptr<MemoryMappedFile> mappedFile = nullptr;
  AtomicBuffer buffer(nullptr, 0);

  try {
    // Check if file exists using C++14 compatible method
    std::ifstream testFile(nodeStateFilePath);
    bool fileExists = testFile.good();
    testFile.close();
    if (!fileExists) {
      if (!createNew) {
        throw IOException("NodeStateFile does not exist and createNew=false",
                          SOURCEINFO);
      }

      mappedFile = MemoryMappedFile::createNew(nodeStateFilePath.c_str(), 0,
                                               MINIMUM_FILE_LENGTH, false);
      buffer =
          AtomicBuffer(mappedFile->getMemoryPtr(), mappedFile->getMemorySize());

      // Verify alignment (Java: buffer.verifyAlignment())
      if (reinterpret_cast<std::uintptr_t>(mappedFile->getMemoryPtr()) %
              ALIGNMENT !=
          0) {
        throw IllegalStateException("buffer is not correctly aligned",
                                    SOURCEINFO);
      }

      initialiseDecodersOnCreation(buffer, m_nodeStateHeaderDecoder,
                                   m_messageHeaderDecoder,
                                   m_candidateTermDecoder);

      m_candidateTermIdOffset = calculateAndVerifyCandidateTermIdOffset();
      buffer.putInt64Ordered(m_candidateTermIdOffset, NULL_VALUE);
    } else {
      mappedFile = MemoryMappedFile::mapExisting(nodeStateFilePath.c_str(), 0,
                                                 0, false, false);
      buffer =
          AtomicBuffer(mappedFile->getMemoryPtr(), mappedFile->getMemorySize());

      loadDecodersAndOffsets(buffer);
    }

    syncFile();
  } catch (const std::exception &ex) {
    // MemoryMappedFile destructor will handle cleanup
    throw;
  }

  m_mappedFile = mappedFile;
  m_buffer = buffer;
  m_candidateTerm = std::make_unique<CandidateTerm>(
      *this, m_candidateTermDecoder, m_candidateTermIdOffset);
}

inline NodeStateFile::~NodeStateFile() { close(); }

inline void NodeStateFile::close() { m_mappedFile.reset(); }

inline std::int32_t NodeStateFile::calculateAndVerifyCandidateTermIdOffset() {
  const std::int32_t candidateTermIdOffset =
      m_candidateTermDecoder.offset() +
      CandidateTermDecoder::candidateTermIdEncodingOffset();
  verifyAlignment(candidateTermIdOffset);
  return candidateTermIdOffset;
}

inline void
NodeStateFile::loadInitialState(AtomicBuffer &buffer,
                                NodeStateHeaderDecoder &nodeStateHeaderDecoder,
                                CandidateTermDecoder &candidateTermDecoder,
                                MessageHeaderDecoder &messageHeaderDecoder) {
  nodeStateHeaderDecoder.wrapForDecode(
      reinterpret_cast<char *>(buffer.buffer()), 0,
      static_cast<std::uint64_t>(buffer.capacity()),
      NodeStateHeaderDecoder::sbeBlockLength(),
      NodeStateHeaderDecoder::sbeSchemaVersion());

  const std::int32_t version = nodeStateHeaderDecoder.version();
  if (service::ClusterMarkFile::MAJOR_VERSION !=
      SemanticVersion::major(version)) {
    throw client::ClusterException(
        "mark file major version " +
            std::to_string(SemanticVersion::major(version)) +
            " does not match software: " +
            std::to_string(service::ClusterMarkFile::MAJOR_VERSION),
        SOURCEINFO);
  }

  const std::int64_t footerOffset = scanForMessageTypeOffset(
      static_cast<std::int32_t>(nodeStateHeaderDecoder.sbeBlockLength()),
      static_cast<std::int32_t>(NodeStateFooterDecoder::SBE_TEMPLATE_ID),
      buffer, messageHeaderDecoder);

  if (NULL_VALUE == footerOffset) {
    throw IllegalStateException(
        "failed to find NodeStateFooter entry, file corrupt?", SOURCEINFO);
  }

  const std::int64_t candidateTermOffset = scanForMessageTypeOffset(
      static_cast<std::int32_t>(nodeStateHeaderDecoder.sbeBlockLength()),
      static_cast<std::int32_t>(CandidateTermDecoder::SBE_TEMPLATE_ID), buffer,
      messageHeaderDecoder);

  if (NULL_VALUE == candidateTermOffset) {
    throw IllegalStateException("failed to find CandidateTerm entry",
                                SOURCEINFO);
  }

  candidateTermDecoder.wrapForDecode(
      reinterpret_cast<char *>(buffer.buffer()),
      static_cast<std::uint64_t>(candidateTermOffset),
      static_cast<std::uint64_t>(buffer.capacity()),
      CandidateTermDecoder::sbeBlockLength(),
      CandidateTermDecoder::sbeSchemaVersion());
}

inline void NodeStateFile::initialiseDecodersOnCreation(
    AtomicBuffer &buffer, NodeStateHeaderDecoder &nodeStateHeaderDecoder,
    MessageHeaderDecoder &messageHeaderDecoder,
    CandidateTermDecoder &candidateTermDecoder) {
  char *buf = reinterpret_cast<char *>(buffer.buffer());
  std::uint64_t bufLen = static_cast<std::uint64_t>(buffer.capacity());

  // Initialize NodeStateHeader using wrapAndApplyHeader for encoding
  nodeStateHeaderDecoder.wrapAndApplyHeader(buf, 0, bufLen);
  nodeStateHeaderDecoder.version(service::ClusterMarkFile::SEMANTIC_VERSION);

  const std::int32_t candidateTermFrameOffset =
      static_cast<std::int32_t>(NodeStateHeaderDecoder::SBE_BLOCK_LENGTH);
  verifyAlignment(candidateTermFrameOffset);

  // Set candidateTermId using wrapAndApplyHeader for encoding
  CandidateTermDecoder candidateTermEncoder;
  candidateTermEncoder.wrapAndApplyHeader(
      buf, static_cast<std::uint64_t>(candidateTermFrameOffset), bufLen);
  candidateTermEncoder.logPosition(NULL_VALUE)
      .timestamp(NULL_VALUE)
      .candidateTermId(NULL_VALUE);

  // Update message header frame length for CandidateTerm
  MessageHeader msgHdr(buf,
                       static_cast<std::uint64_t>(candidateTermFrameOffset),
                       bufLen, MessageHeader::sbeSchemaVersion());
  msgHdr.frameLength(static_cast<std::int32_t>(
      MessageHeader::encodedLength() + candidateTermEncoder.encodedLength()));

  // Also wrap for decode to read back
  candidateTermDecoder.wrapForDecode(
      buf, static_cast<std::uint64_t>(candidateTermFrameOffset), bufLen,
      CandidateTermDecoder::sbeBlockLength(),
      CandidateTermDecoder::sbeSchemaVersion());
  messageHeaderDecoder.wrap(
      buf, static_cast<std::uint64_t>(candidateTermFrameOffset),
      MessageHeader::sbeSchemaVersion(), bufLen);

  // Add NodeStateFooter
  const std::int32_t footerOffset =
      candidateTermFrameOffset +
      static_cast<std::int32_t>(
          BitUtil::align(static_cast<std::int64_t>(msgHdr.frameLength()),
                         static_cast<std::int64_t>(8)));
  NodeStateFooterDecoder nodeStateFooterEncoder;
  nodeStateFooterEncoder.wrapAndApplyHeader(
      buf, static_cast<std::uint64_t>(footerOffset), bufLen);

  // Update message header frame length for NodeStateFooter
  MessageHeader footerMsgHdr(buf, static_cast<std::uint64_t>(footerOffset),
                             bufLen, MessageHeader::sbeSchemaVersion());
  footerMsgHdr.frameLength(static_cast<std::int32_t>(
      MessageHeader::encodedLength() + nodeStateFooterEncoder.encodedLength()));
}

inline void NodeStateFile::loadDecodersAndOffsets(AtomicBuffer &buffer) {
  loadInitialState(buffer, m_nodeStateHeaderDecoder, m_candidateTermDecoder,
                   m_messageHeaderDecoder);

  m_candidateTermIdOffset = calculateAndVerifyCandidateTermIdOffset();
}

inline std::int64_t NodeStateFile::scanForMessageTypeOffset(
    std::int32_t startPosition, std::int32_t templateId, AtomicBuffer &buffer,
    MessageHeaderDecoder &messageHeaderDecoder) {
  verifyAlignment(startPosition);
  std::int32_t position = startPosition;

  while (position < static_cast<std::int32_t>(buffer.capacity())) {
    messageHeaderDecoder.wrap(reinterpret_cast<char *>(buffer.buffer()),
                              static_cast<std::uint64_t>(position),
                              MessageHeader::sbeSchemaVersion(),
                              static_cast<std::uint64_t>(buffer.capacity()));

    const std::int32_t messageLength = messageHeaderDecoder.frameLength();

    if (templateId == messageHeaderDecoder.templateId()) {
      return position;
    } else if (NodeStateFooterDecoder::SBE_TEMPLATE_ID ==
               messageHeaderDecoder.templateId()) {
      return NULL_VALUE;
    }

    if (messageLength < 0) {
      throw IllegalStateException("Message length < 0, file corrupt?",
                                  SOURCEINFO);
    } else if (0 == messageLength) {
      return NULL_VALUE;
    }

    position += static_cast<std::int32_t>(
        BitUtil::align(static_cast<std::int64_t>(messageLength),
                       static_cast<std::int64_t>(8)));
  }

  return NULL_VALUE;
}

inline void NodeStateFile::syncFile() {
  if (0 < m_fileSyncLevel) {
    // MemoryMappedFile doesn't have a force() method, but the destructor will
    // sync For now, we rely on the OS to sync the file
    // TODO: Add force() method to MemoryMappedFile if needed
  }
}

inline void NodeStateFile::updateCandidateTermId(std::int64_t candidateTermId,
                                                 std::int64_t logPosition,
                                                 std::int64_t timestampMs) {
  m_buffer.putInt64(m_candidateTermDecoder.offset() +
                        CandidateTermDecoder::logPositionEncodingOffset(),
                    logPosition);
  m_buffer.putInt64(m_candidateTermDecoder.offset() +
                        CandidateTermDecoder::timestampEncodingOffset(),
                    timestampMs);
  m_buffer.putInt64Ordered(m_candidateTermIdOffset, candidateTermId);
  syncFile();
}

inline std::int64_t
NodeStateFile::proposeMaxCandidateTermId(std::int64_t candidateTermId,
                                         std::int64_t logPosition,
                                         std::int64_t timestampMs) {
  const std::int64_t existingCandidateTermId =
      m_candidateTerm->candidateTermId();

  if (candidateTermId > existingCandidateTermId) {
    updateCandidateTermId(candidateTermId, logPosition, timestampMs);
    return candidateTermId;
  }

  return existingCandidateTermId;
}

inline NodeStateFile::CandidateTerm &NodeStateFile::candidateTerm() {
  return *m_candidateTerm;
}

} // namespace cluster
} // namespace aeron
