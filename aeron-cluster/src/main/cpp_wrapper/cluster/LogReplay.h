#pragma once

#include <memory>
#include <cstdint>
#include <string>
#include "client/archive/AeronArchive.h"
#include "ChannelUri.h"
#include "Image.h"
#include "Subscription.h"
#include "client/ClusterExceptions.h"
#include "util/CloseHelper.h"
#include "concurrent/CountedErrorHandler.h"
#include "LogAdapter.h"
#include "ConsensusModuleAgent.h"  // For ConsensusModuleAgent::State

namespace aeron { namespace cluster {

using namespace aeron::concurrent;

class ConsensusModuleAgent; // Forward declaration
class ConsensusModule; // Forward declaration

/**
 * Replay of log from archive.
 */
class LogReplay
{
public:
    LogReplay(
        std::shared_ptr<archive::client::AeronArchive> archive,
        std::int64_t recordingId,
        std::int64_t startPosition,
        std::int64_t stopPosition,
        LogAdapter& logAdapter,
        ConsensusModuleContext& ctx);

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
    ConsensusModuleAgent* m_consensusModuleAgent;
    ConsensusModuleContext* m_ctx;
    LogAdapter& m_logAdapter;
    std::shared_ptr<Subscription> m_logSubscription;
};

// Implementation moved to ConsensusModuleAgent.h to resolve circular dependencies

}}

