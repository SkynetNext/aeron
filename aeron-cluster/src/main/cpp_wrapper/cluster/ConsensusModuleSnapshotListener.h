#pragma once

#include "generated/aeron_cluster_codecs/CloseReason.h"
#include "generated/aeron_cluster_codecs/ClusterTimeUnit.h"
#include "util/DirectBuffer.h"
#include <cstdint>
#include <string>


namespace aeron {
namespace cluster {

/**
 * Listener for loading consensus module snapshots.
 */
class ConsensusModuleSnapshotListener {
public:
  virtual ~ConsensusModuleSnapshotListener() = default;

  virtual void onLoadBeginSnapshot(std::int32_t appVersion,
                                   ClusterTimeUnit::Value timeUnit,
                                   const util::DirectBuffer &buffer,
                                   std::int32_t offset,
                                   std::int32_t length) = 0;

  virtual void onLoadConsensusModuleState(std::int64_t nextSessionId,
                                          std::int64_t nextServiceSessionId,
                                          std::int64_t logServiceSessionId,
                                          std::int32_t pendingMessageCapacity,
                                          const util::DirectBuffer &buffer,
                                          std::int32_t offset,
                                          std::int32_t length) = 0;

  virtual void onLoadPendingMessage(std::int64_t clusterSessionId,
                                    const util::DirectBuffer &buffer,
                                    std::int32_t offset,
                                    std::int32_t length) = 0;

  virtual void onLoadClusterSession(
      std::int64_t clusterSessionId, std::int64_t correlationId,
      std::int64_t openedLogPosition, std::int64_t timeOfLastActivity,
      CloseReason::Value closeReason, std::int32_t responseStreamId,
      const std::string &responseChannel, const util::DirectBuffer &buffer,
      std::int32_t offset, std::int32_t length) = 0;

  virtual void onLoadTimer(std::int64_t correlationId, std::int64_t deadline,
                           const util::DirectBuffer &buffer,
                           std::int32_t offset, std::int32_t length) = 0;

  virtual void onLoadPendingMessageTracker(std::int64_t nextServiceSessionId,
                                           std::int64_t logServiceSessionId,
                                           std::int32_t pendingMessageCapacity,
                                           std::int32_t serviceId,
                                           const util::DirectBuffer &buffer,
                                           std::int32_t offset,
                                           std::int32_t length) = 0;

  virtual void onLoadEndSnapshot(const util::DirectBuffer &buffer,
                                 std::int32_t offset, std::int32_t length) = 0;
};

} // namespace cluster
} // namespace aeron
