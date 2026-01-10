#pragma once

#include "Aeron.h"
#include "client/ClusterExceptions.h"
#include "client/archive/AeronArchive.h"
#include "client/archive/AeronArchive.h" // For RecordingSignal
#include "client/archive/RecordingPos.h"
#include "concurrent/CountersReader.h"
#include "util/Exceptions.h"
#include <cstdint>
#include <memory>
#include <string>

namespace aeron {
namespace cluster {

// Avoid using namespace directives in headers to prevent namespace pollution

/**
 * Replication of a single recording.
 */
class RecordingReplication {
public:
  RecordingReplication(
      std::shared_ptr<::aeron::archive::client::AeronArchive> archive,
      std::int64_t srcRecordingId, const std::string &srcArchiveChannel,
      std::int32_t srcControlStreamId,
      ::aeron::archive::client::ReplicationParams &replicationParams,
      std::int64_t progressCheckTimeoutNs, std::int64_t progressCheckIntervalNs,
      std::int64_t nowNs);

  ~RecordingReplication();

  void close();

  std::int32_t poll(std::int64_t nowNs);

  std::int64_t position() const;

  std::int64_t recordingId() const;

  bool hasReplicationEnded() const;

  bool hasSynced() const;

  bool hasStopped() const;

  void onSignal(std::int64_t correlationId, std::int64_t recordingId,
                std::int64_t position,
                ::aeron::archive::client::RecordingSignal signal);

  std::string srcArchiveChannel() const;

  std::string toString() const;

private:
  bool pollDstRecordingPosition();

  std::int64_t m_replicationId;
  std::int64_t m_stopPosition;
  std::int64_t m_progressCheckTimeoutNs;
  std::int64_t m_progressCheckIntervalNs;
  std::string m_srcArchiveChannel;

  std::int32_t m_recordingPositionCounterId = 0; // NULL_COUNTER_ID
  std::int64_t m_recordingId = 0;                // NULL_VALUE
  std::int64_t m_position = 0;                   // NULL_POSITION

  std::int64_t m_progressDeadlineNs;
  std::int64_t m_progressCheckDeadlineNs;
  std::shared_ptr<::aeron::archive::client::AeronArchive> m_archive;
  ::aeron::archive::client::RecordingSignal
      m_lastRecordingSignal; // Will be initialized with NULL_VALUE

  bool m_hasReplicationEnded = false;
  bool m_hasSynced = false;
  bool m_hasStopped = false;
};

// Implementation
inline RecordingReplication::RecordingReplication(
    std::shared_ptr<::aeron::archive::client::AeronArchive> archive,
    std::int64_t srcRecordingId, const std::string &srcArchiveChannel,
    std::int32_t srcControlStreamId,
    ::aeron::archive::client::ReplicationParams &replicationParams,
    std::int64_t progressCheckTimeoutNs, std::int64_t progressCheckIntervalNs,
    std::int64_t nowNs)
    : m_archive(archive), m_stopPosition(replicationParams.stopPosition()),
      m_progressCheckTimeoutNs(progressCheckTimeoutNs),
      m_progressCheckIntervalNs(progressCheckIntervalNs),
      m_srcArchiveChannel(srcArchiveChannel),
      m_progressDeadlineNs(nowNs + progressCheckTimeoutNs),
      m_progressCheckDeadlineNs(nowNs + progressCheckIntervalNs),
      m_lastRecordingSignal(
          0, 0, 0, 0,
          static_cast<std::int32_t>(
              ::aeron::archive::client::RecordingSignal::Value::NULL_VALUE)) {
  m_replicationId = archive->replicate(srcRecordingId, srcControlStreamId,
                                       srcArchiveChannel, replicationParams);
}

inline RecordingReplication::~RecordingReplication() { close(); }

inline void RecordingReplication::close() {
  if (!m_hasReplicationEnded) {
    try {
      m_hasReplicationEnded = true;
      m_archive->tryStopReplication(m_replicationId);
    } catch (const std::exception &ex) {
      throw client::ClusterException("failed to stop log replication",
                                     SOURCEINFO);
    }
  }
}

inline std::int32_t RecordingReplication::poll(std::int64_t nowNs) {
  std::int32_t workCount = 0;

  if (m_hasReplicationEnded) {
    return workCount;
  }

  try {
    if (nowNs >= m_progressCheckDeadlineNs) {
      m_progressCheckDeadlineNs = nowNs + m_progressCheckIntervalNs;
      if (pollDstRecordingPosition()) {
        m_progressDeadlineNs = nowNs + m_progressCheckTimeoutNs;
      }
      workCount++;
    }

    if (nowNs >= m_progressDeadlineNs) {
      if (archive::client::NULL_POSITION == m_stopPosition ||
          m_position < m_stopPosition) {
        throw client::ClusterException("log replication has not progressed",
                                       SOURCEINFO);
      } else {
        throw client::ClusterException("log replication failed to stop",
                                       SOURCEINFO);
      }
    }

    return workCount;
  } catch (const client::ClusterException &ex) {
    try {
      close();
    } catch (const client::ClusterException &ex1) {
      // Note: C++ doesn't have addSuppressed, so we just close
    }

    throw;
  }
}

inline std::int64_t RecordingReplication::position() const {
  return m_position;
}

inline std::int64_t RecordingReplication::recordingId() const {
  return m_recordingId;
}

inline bool RecordingReplication::hasReplicationEnded() const {
  return m_hasReplicationEnded;
}

inline bool RecordingReplication::hasSynced() const { return m_hasSynced; }

inline bool RecordingReplication::hasStopped() const { return m_hasStopped; }

inline void RecordingReplication::onSignal(
    std::int64_t correlationId, std::int64_t recordingId, std::int64_t position,
    ::aeron::archive::client::RecordingSignal signal) {
  if (correlationId == m_replicationId) {
    if (::aeron::archive::client::RecordingSignal::Value::EXTEND ==
        static_cast<::aeron::archive::client::RecordingSignal::Value>(
            signal.m_recordingSignalCode)) {
      auto &counters = m_archive->context().aeron()->countersReader();
      m_recordingPositionCounterId =
          ::aeron::archive::client::RecordingPos::findCounterIdByRecordingId(
              counters, recordingId);
    } else if (::aeron::archive::client::RecordingSignal::Value::SYNC ==
               static_cast<::aeron::archive::client::RecordingSignal::Value>(
                   signal.m_recordingSignalCode)) {
      m_hasSynced = true;
    } else if (::aeron::archive::client::RecordingSignal::Value::
                   REPLICATE_END ==
               static_cast<::aeron::archive::client::RecordingSignal::Value>(
                   signal.m_recordingSignalCode)) {
      m_hasReplicationEnded = true;
    } else if (::aeron::archive::client::RecordingSignal::Value::STOP ==
               static_cast<::aeron::archive::client::RecordingSignal::Value>(
                   signal.m_recordingSignalCode)) {
      if (::aeron::archive::client::NULL_POSITION != position) {
        m_position = position;
      }
      m_hasStopped = true;
    } else if (::aeron::archive::client::RecordingSignal::Value::ABC_DELETE ==
               static_cast<::aeron::archive::client::RecordingSignal::Value>(
                   signal.m_recordingSignalCode)) {
      throw client::ClusterException(
          "recording was deleted during replication: " + toString(),
          SOURCEINFO);
    }

    m_lastRecordingSignal = signal; // Store the signal object

    if (::aeron::NULL_VALUE != recordingId) {
      m_recordingId = recordingId;
    }

    if (::aeron::archive::client::NULL_POSITION != position) {
      m_position = position;
    }
  }
}

inline bool RecordingReplication::pollDstRecordingPosition() {
  if (CountersReader::NULL_COUNTER_ID != m_recordingPositionCounterId) {
    auto &counters = m_archive->context().aeron()->countersReader();
    const std::int64_t recordingPosition =
        counters.getCounterValue(m_recordingPositionCounterId);

    if (::aeron::archive::client::RecordingPos::isActive(
            counters, m_recordingPositionCounterId, m_recordingId) &&
        recordingPosition > m_position) {
      m_position = recordingPosition;
      return true;
    }
  }

  return false;
}

inline std::string RecordingReplication::srcArchiveChannel() const {
  return m_srcArchiveChannel;
}

inline std::string RecordingReplication::toString() const {
  return std::string("RecordingReplication{") +
         "replicationId=" + std::to_string(m_replicationId) +
         ", stopPosition=" + std::to_string(m_stopPosition) +
         ", progressCheckTimeoutNs=" +
         std::to_string(m_progressCheckTimeoutNs) +
         ", progressCheckIntervalNs=" +
         std::to_string(m_progressCheckIntervalNs) +
         ", recordingPositionCounterId=" +
         std::to_string(m_recordingPositionCounterId) +
         ", recordingId=" + std::to_string(m_recordingId) +
         ", position=" + std::to_string(m_position) +
         ", progressDeadlineNs=" + std::to_string(m_progressDeadlineNs) +
         ", progressCheckDeadlineNs=" +
         std::to_string(m_progressCheckDeadlineNs) + ", lastRecordingSignal=" +
         std::to_string(m_lastRecordingSignal.m_recordingSignalCode) +
         ", hasReplicationEnded=" + (m_hasReplicationEnded ? "true" : "false") +
         ", hasSynced=" + (m_hasSynced ? "true" : "false") +
         ", hasStopped=" + (m_hasStopped ? "true" : "false") + "}";
}

} // namespace cluster
} // namespace aeron
