#pragma once
#include <chrono>
#include <memory>
#include <string>

#include "ClusterClock.h"
#include "ExclusivePublication.h"
#include "Publication.h"
#include "cluster/client/ClusterExceptions.h"
#include "concurrent/AgentInvoker.h"
#include "concurrent/AtomicBuffer.h"
#include "concurrent/IdleStrategy.h"
#include "concurrent/logbuffer/BufferClaim.h"
#include "generated/aeron_cluster_codecs/MessageHeader.h"
#include "generated/aeron_cluster_codecs/SnapshotMarker.h"

namespace aeron {
namespace cluster {
namespace service {
using namespace aeron::concurrent;
using namespace aeron::concurrent::logbuffer;
using namespace aeron::cluster::codecs;
using namespace aeron::util;

class SnapshotTaker {
public:
  SnapshotTaker(std::shared_ptr<ExclusivePublication> publication,
                std::shared_ptr<IdleStrategy> idleStrategy,
                std::shared_ptr<void>
                    aeronAgentInvoker); // Use void for generic AgentInvoker

  void markBegin(std::int64_t snapshotTypeId, std::int64_t logPosition,
                 std::int64_t leadershipTermId, std::int32_t snapshotIndex,
                 std::chrono::milliseconds::rep timeUnit,
                 std::int32_t appVersion);

  void markEnd(std::int64_t snapshotTypeId, std::int64_t logPosition,
               std::int64_t leadershipTermId, std::int32_t snapshotIndex,
               std::chrono::milliseconds::rep timeUnit,
               std::int32_t appVersion);

  void markSnapshot(std::int64_t snapshotTypeId, std::int64_t logPosition,
                    std::int64_t leadershipTermId, std::int32_t snapshotIndex,
                    SnapshotMark::Value snapshotMark,
                    std::chrono::milliseconds::rep timeUnit,
                    std::int32_t appVersion);

protected:
  static void checkInterruptStatus();
  static void checkResult(std::int64_t position, Publication &publication);
  void checkResultAndIdle(std::int64_t position);
  void invokeAgentClient();
  void offer(AtomicBuffer &buffer, std::int32_t offset, std::int32_t length);

  BufferClaim m_bufferClaim;
  MessageHeader m_messageHeaderEncoder;
  std::shared_ptr<ExclusivePublication> m_publication;
  std::shared_ptr<IdleStrategy> m_idleStrategy;

private:
  static constexpr std::int32_t ENCODED_MARKER_LENGTH =
      MessageHeader::encodedLength() + SnapshotMarker::SBE_BLOCK_LENGTH;

  std::shared_ptr<void> m_aeronAgentInvoker; // Generic AgentInvoker
  SnapshotMarker m_snapshotMarkerEncoder;
};

} // namespace service
} // namespace cluster
} // namespace aeron
