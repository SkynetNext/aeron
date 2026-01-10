#pragma once

#include "ChannelUri.h"
#include "ConsensusModule.h" // Must include for ConsensusModule::Context
#include "Image.h"
#include "LogAdapter.h"
#include "Subscription.h"
#include "client/ClusterExceptions.h"
#include "client/archive/AeronArchive.h"
#include "concurrent/CountedErrorHandler.h"
#include "util/CloseHelper.h"
#include <cstdint>
#include <memory>
#include <string>

namespace aeron {
namespace cluster {

// Forward declaration
class ConsensusModuleAgent;

/**
 * Replay of log from archive.
 */
class LogReplay {
public:
  LogReplay(std::shared_ptr<archive::client::AeronArchive> archive,
            std::int64_t recordingId, std::int64_t startPosition,
            std::int64_t stopPosition, LogAdapter &logAdapter,
            ConsensusModule::Context &ctx);

  ~LogReplay();

  void close();

  std::int32_t doWork();

  bool isDone() const;

  std::int64_t position() const;

  std::string toString() const;

private:
  std::shared_ptr<archive::client::AeronArchive> m_archive;
  std::int64_t m_startPosition;
  std::int64_t m_stopPosition;
  std::int64_t m_replaySessionId;
  std::int32_t m_logSessionId;
  ConsensusModuleAgent *m_consensusModuleAgent;
  ConsensusModule::Context *m_ctx;
  LogAdapter &m_logAdapter;
  std::shared_ptr<Subscription> m_logSubscription;
};

// Implementation moved to ConsensusModuleAgent.h to resolve circular
// dependencies

} // namespace cluster
} // namespace aeron
