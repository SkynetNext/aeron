#pragma once

#include <memory>
#include <cstdint>
#include <string>
#include "archive/client/AeronArchive.h"
#include "ChannelUri.h"
#include "Image.h"
#include "Subscription.h"
#include "../client/ClusterExceptions.h"
#include "util/CloseHelper.h"
#include "concurrent/status/CountedErrorHandler.h"
#include "LogAdapter.h"

namespace aeron { namespace cluster {

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
        ConsensusModule::Context& ctx);

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
    ConsensusModule::Context* m_ctx;
    LogAdapter& m_logAdapter;
    std::shared_ptr<Subscription> m_logSubscription;
};

// Implementation
inline LogReplay::LogReplay(
    std::shared_ptr<archive::client::AeronArchive> archive,
    std::int64_t recordingId,
    std::int64_t startPosition,
    std::int64_t stopPosition,
    LogAdapter& logAdapter,
    ConsensusModule::Context& ctx) :
    m_archive(archive),
    m_startPosition(startPosition),
    m_stopPosition(stopPosition),
    m_logAdapter(logAdapter),
    m_consensusModuleAgent(logAdapter.consensusModuleAgent()),
    m_ctx(&ctx)
{
    const std::string channel = ctx.replayChannel();
    const std::int32_t streamId = ctx.replayStreamId();
    const std::int64_t length = stopPosition - startPosition;
    
    m_replaySessionId = archive->startReplay(recordingId, startPosition, length, channel, streamId);
    m_logSessionId = static_cast<std::int32_t>(m_replaySessionId);
    
    std::string channelWithSession = ChannelUri::addSessionId(channel, m_logSessionId);
    m_logSubscription = ctx.aeron()->addSubscription(channelWithSession, streamId);
}

inline LogReplay::~LogReplay()
{
    close();
}

inline void LogReplay::close()
{
    const auto errorHandler = m_ctx->countedErrorHandler();
    try
    {
        if (m_archive && m_replaySessionId != 0)
        {
            m_archive->stopReplay(m_replaySessionId);
        }
    }
    catch (const std::exception& ex)
    {
        if (errorHandler)
        {
            errorHandler(ex);
        }
    }
    
    m_logAdapter.disconnect(errorHandler);
    if (m_logSubscription)
    {
        CloseHelper::close(errorHandler, m_logSubscription);
    }
}

inline std::int32_t LogReplay::doWork()
{
    std::int32_t workCount = 0;

    if (!m_logAdapter.image())
    {
        auto image = m_logSubscription->imageBySessionId(m_logSessionId);
        if (image)
        {
            if (image->joinPosition() != m_startPosition)
            {
                throw ClusterException(
                    "joinPosition=" + std::to_string(image->joinPosition()) + 
                    " expected startPosition=" + std::to_string(m_startPosition),
                    SOURCEINFO);
            }

            m_logAdapter.image(image);
            if (m_consensusModuleAgent)
            {
                m_consensusModuleAgent->awaitServicesReady(
                    m_logSubscription->channel(),
                    m_logSubscription->streamId(),
                    m_logSessionId,
                    m_startPosition,
                    m_stopPosition,
                    true,
                    service::Cluster::Role::FOLLOWER);
            }

            workCount += 1;
        }
    }
    else
    {
        if (m_consensusModuleAgent)
        {
            workCount += m_consensusModuleAgent->replayLogPoll(m_logAdapter, m_stopPosition);
        }
    }

    return workCount;
}

inline bool LogReplay::isDone() const
{
    return m_logAdapter.position() >= m_stopPosition &&
        m_consensusModuleAgent &&
        m_consensusModuleAgent->state() != ConsensusModule::State::SNAPSHOT;
}

inline std::int64_t LogReplay::position() const
{
    return m_logAdapter.position();
}

inline std::string LogReplay::toString() const
{
    return "LogReplay{" +
        "startPosition=" + std::to_string(m_startPosition) +
        ", stopPosition=" + std::to_string(m_stopPosition) +
        ", replaySessionId=" + std::to_string(m_replaySessionId) +
        ", logSessionId=" + std::to_string(m_logSessionId) +
        ", logSubscription=" + (m_logSubscription ? m_logSubscription->toString() : "null") +
        ", position=" + std::to_string(position()) +
        "}";
}

}}

