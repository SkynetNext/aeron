#pragma once

#include <memory>
#include <string>
#include <vector>
#include <cstdint>
#include <thread>
#include <chrono>
#include "Aeron.h"
#include "Counter.h"
#include "Publication.h"
#include "Image.h"
#include "concurrent/logbuffer/BufferClaim.h"
#include "concurrent/logbuffer/Header.h"
#include "util/DirectBuffer.h"
#include "util/Exceptions.h"
#include "client/ClusterExceptions.h"
#include "client/ClusterEvent.h"
#include "generated/aeron_cluster_codecs/CloseReason.h"
#include "generated/aeron_cluster_codecs/EventCode.h"
#include "client/archive/AeronArchive.h"
#include "service/ClusterCounters.h"
#include "concurrent/errors/DistinctErrorLog.h"
#include "ClusterClientSession.h"
#include "LogPublisher.h"
#include "EgressPublisher.h"
#include "AeronCounters.h"
#include "util/CloseHelper.h"
#include "util/BitUtil.h"
#include "util/StringUtil.h"

namespace aeron { namespace cluster {

using namespace aeron::concurrent;
using namespace aeron::concurrent::logbuffer;
using namespace aeron::util;

class LogPublisher; // Forward declaration
class EgressPublisher; // Forward declaration

/**
 * State of a cluster session.
 */
enum class ClusterSessionState : std::int32_t
{
    INIT = 0,
    CONNECTING = 1,
    CONNECTED = 2,
    CHALLENGED = 3,
    AUTHENTICATED = 4,
    REJECTED = 5,
    OPEN = 6,
    CLOSING = 7,
    INVALID = 8,
    CLOSED = 9
};

/**
 * Action type for a cluster session.
 */
enum class ClusterSessionAction : std::int32_t
{
    CLIENT = 0,
    BACKUP = 1,
    HEARTBEAT = 2,
    STANDBY_SNAPSHOT = 3
};

/**
 * Cluster session implementation.
 */
class ClusterSession : public ClusterClientSession
{
public:
    static constexpr std::int32_t MAX_ENCODED_PRINCIPAL_LENGTH = 4 * 1024;
    static constexpr std::int32_t MAX_ENCODED_MEMBERSHIP_QUERY_LENGTH = 4 * 1024;

    ClusterSession(
        std::int32_t clusterMemberId,
        std::int64_t sessionId,
        std::int32_t responseStreamId,
        const std::string& responseChannel,
        const std::string& sessionInfo);

    void close(std::shared_ptr<Aeron> aeron, const exception_handler_t& errorHandler, const std::string& reason);

    std::int64_t id() override;

    std::vector<std::uint8_t> encodedPrincipal() override;

    bool isOpen() override;

    std::shared_ptr<Publication> responsePublication() override;

    std::int64_t timeOfLastActivityNs() override;

    void timeOfLastActivityNs(std::int64_t timeNs) override;

    void loadSnapshotState(
        std::int64_t correlationId,
        std::int64_t openedLogPosition,
        std::int64_t timeOfLastActivityNs,
        CloseReason::Value closeReason);

    std::int32_t responseStreamId() const;

    std::string responseChannel() const;

    void closing(CloseReason::Value closeReason);

    CloseReason::Value closeReason() const;

    void resetCloseReason();

    void asyncConnect(std::shared_ptr<Aeron> aeron, AtomicBuffer& tempBuffer, std::int32_t clusterId);

    void connect(
        const exception_handler_t& errorHandler,
        std::shared_ptr<Aeron> aeron,
        AtomicBuffer& tempBuffer,
        std::int32_t clusterId);

    void disconnect(std::shared_ptr<Aeron> aeron, const exception_handler_t& errorHandler);

    bool isResponsePublicationConnected(std::shared_ptr<Aeron> aeron, std::int64_t nowNs);

    std::int64_t tryClaim(std::int32_t length, BufferClaim& bufferClaim);

    std::int64_t offer(const util::DirectBuffer& buffer, std::int32_t offset, std::int32_t length);

    ClusterSessionState state() const;

    void state(ClusterSessionState newState, const std::string& reason);

    void authenticate(const std::vector<std::uint8_t>& encodedPrincipal);

    void open(std::int64_t openedLogPosition);

    bool appendSessionToLogAndSendOpen(
        LogPublisher& logPublisher,
        EgressPublisher& egressPublisher,
        std::int64_t leadershipTermId,
        std::int32_t memberId,
        std::int64_t nowNs,
        std::int64_t clusterTimestamp);

    std::int32_t sendSessionOpenEvent(
        EgressPublisher& egressPublisher,
        std::int64_t leadershipTermId,
        std::int32_t memberId);

    void lastActivityNs(std::int64_t timeNs, std::int64_t correlationId);

    void reject(EventCode::Value code, const std::string& responseDetail, concurrent::errors::DistinctErrorLog* errorLog);

    EventCode::Value eventCode() const;

    std::string responseDetail() const;

    std::int64_t correlationId() const;

    std::int64_t openedLogPosition() const;

    void closedLogPosition(std::int64_t closedLogPosition);

    std::int64_t closedLogPosition() const;

    void hasNewLeaderEventPending(bool flag);

    bool hasNewLeaderEventPending() const;

    bool hasOpenEventPending() const;

    void clearOpenEventPending();

    ClusterSessionAction action() const;

    void action(ClusterSessionAction action);

    void requestInput(std::shared_ptr<void> requestInput);

    std::shared_ptr<void> requestInput() const;

    void linkIngressImage(Header& header);

    void unlinkIngressImage();

    std::int64_t ingressImageCorrelationId() const;

    static void checkEncodedPrincipalLength(const std::vector<std::uint8_t>& encodedPrincipal);

    std::string toString() const;

private:
    std::int64_t addSessionCounter(std::shared_ptr<Aeron> aeron, AtomicBuffer& tempBuffer, std::int32_t clusterId);

    static void logStateChange(
        std::int32_t memberId,
        std::int64_t sessionId,
        ClusterSessionAction action,
        ClusterSessionState oldState,
        ClusterSessionState newState,
        const std::string& reason);

    std::int64_t m_id;
    std::int32_t m_clusterMemberId;
    std::int32_t m_responseStreamId;
    std::string m_responseChannel;
    std::string m_sessionInfo;
    bool m_hasNewLeaderEventPending = false;
    bool m_hasOpenEventPending = true;
    std::int64_t m_correlationId = 0;
    std::int64_t m_openedLogPosition = 0; // NULL_POSITION
    std::int64_t m_closedLogPosition = 0; // NULL_POSITION
    std::int64_t m_timeOfLastActivityNs = 0;
    std::int64_t m_ingressImageCorrelationId = 0; // NULL_VALUE
    std::int64_t m_responsePublicationId = 0; // NULL_VALUE
    std::int64_t m_counterRegistrationId = 0; // NULL_VALUE
    std::shared_ptr<Publication> m_responsePublication;
    std::shared_ptr<Counter> m_counter;
    ClusterSessionState m_state = ClusterSessionState::INIT;
    std::string m_responseDetail;
    EventCode::Value m_eventCode = EventCode::Value::NULL_VALUE;
    CloseReason::Value m_closeReason = CloseReason::Value::NULL_VALUE;
    std::vector<std::uint8_t> m_encodedPrincipal;
    ClusterSessionAction m_action = ClusterSessionAction::CLIENT;
    std::shared_ptr<void> m_requestInput;
};

// Implementation

inline ClusterSession::ClusterSession(
    std::int32_t clusterMemberId,
    std::int64_t sessionId,
    std::int32_t responseStreamId,
    const std::string& responseChannel,
    const std::string& sessionInfo) :
    m_id(sessionId),
    m_clusterMemberId(clusterMemberId),
    m_responseStreamId(responseStreamId),
    m_responseChannel(responseChannel),
    m_sessionInfo(sessionInfo)
{
    state(ClusterSessionState::INIT, "");
}

inline void ClusterSession::close(
    std::shared_ptr<Aeron> aeron,
    const exception_handler_t& errorHandler,
    const std::string& reason)
{
    disconnect(aeron, errorHandler);
    state(ClusterSessionState::CLOSED, reason);
}

inline std::int64_t ClusterSession::id()
{
    return m_id;
}

inline std::vector<std::uint8_t> ClusterSession::encodedPrincipal()
{
    return m_encodedPrincipal;
}

inline bool ClusterSession::isOpen()
{
    return ClusterSessionState::OPEN == m_state;
}

inline std::shared_ptr<Publication> ClusterSession::responsePublication()
{
    return m_responsePublication;
}

inline std::int64_t ClusterSession::timeOfLastActivityNs()
{
    return m_timeOfLastActivityNs;
}

inline void ClusterSession::timeOfLastActivityNs(std::int64_t timeNs)
{
    m_timeOfLastActivityNs = timeNs;
}

inline void ClusterSession::loadSnapshotState(
    std::int64_t correlationId,
    std::int64_t openedLogPosition,
    std::int64_t timeOfLastActivityNs,
    CloseReason::Value closeReason)
{
    m_openedLogPosition = openedLogPosition;
    m_timeOfLastActivityNs = timeOfLastActivityNs;
    m_correlationId = correlationId;
    m_closeReason = closeReason;

    if (CloseReason::Value::NULL_VALUE != closeReason)
    {
        state(ClusterSessionState::CLOSING, "closeReason=" + std::to_string(static_cast<std::int32_t>(closeReason)));
    }
    else
    {
        state(ClusterSessionState::OPEN, "openedLogPosition=" + std::to_string(openedLogPosition));
    }
}

inline std::int32_t ClusterSession::responseStreamId() const
{
    return m_responseStreamId;
}

inline std::string ClusterSession::responseChannel() const
{
    return m_responseChannel;
}

inline void ClusterSession::closing(CloseReason::Value closeReason)
{
    m_closeReason = closeReason;
    m_hasOpenEventPending = false;
    m_hasNewLeaderEventPending = false;
    m_timeOfLastActivityNs = 0;
    state(ClusterSessionState::CLOSING, "closeReason=" + std::to_string(static_cast<std::int32_t>(closeReason)));
}

inline CloseReason::Value ClusterSession::closeReason() const
{
    return m_closeReason;
}

inline void ClusterSession::resetCloseReason()
{
    m_closedLogPosition = 0; // NULL_POSITION
    m_closeReason = CloseReason::Value::NULL_VALUE;
}

inline void ClusterSession::asyncConnect(
    std::shared_ptr<Aeron> aeron,
    AtomicBuffer& tempBuffer,
    std::int32_t clusterId)
{
    m_counterRegistrationId = addSessionCounter(aeron, tempBuffer, clusterId);
    m_responsePublicationId = aeron->addPublication(m_responseChannel, m_responseStreamId);
}

inline void ClusterSession::connect(
    const exception_handler_t& errorHandler,
    std::shared_ptr<Aeron> aeron,
    AtomicBuffer& tempBuffer,
    std::int32_t clusterId)
{
    if (m_responsePublication)
    {
        throw ClusterException("response publication already added", SOURCEINFO);
    }

    m_counterRegistrationId = addSessionCounter(aeron, tempBuffer, clusterId);

    try
    {
        std::int64_t registrationId = aeron->addPublication(m_responseChannel, m_responseStreamId);
        // Wait for publication to become available
        std::shared_ptr<Publication> publication;
        while (!publication)
        {
            publication = aeron->findPublication(registrationId);
            if (!publication)
            {
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }
        }
        m_responsePublication = publication;
    }
    catch (const std::exception& ex)
    {
        errorHandler(ClusterException(
            "failed to connect session response publication: " + std::string(ex.what()), SOURCEINFO));
    }
}

inline void ClusterSession::disconnect(
    std::shared_ptr<Aeron> aeron,
    const exception_handler_t& errorHandler)
{
    if (0 != m_responsePublicationId) // NULL_VALUE
    {
        // Publications are automatically removed when closed/reset
        m_responsePublicationId = 0; // NULL_VALUE
    }
    else
    {
        CloseHelper::close(errorHandler, m_responsePublication);
        m_responsePublication.reset();
    }
    
    if (0 != m_counterRegistrationId) // NULL_VALUE
    {
        // Counters are automatically removed when closed/reset
        m_counter.reset();
        m_counterRegistrationId = 0; // NULL_VALUE
    }
    else
    {
        CloseHelper::close(errorHandler, m_counter);
        m_counter.reset();
    }
}

inline bool ClusterSession::isResponsePublicationConnected(
    std::shared_ptr<Aeron> aeron,
    std::int64_t nowNs)
{
    if (!m_responsePublication)
    {
        // Check if publication is available
        if (m_responsePublicationId != 0)
        {
            m_responsePublication = aeron->findPublication(m_responsePublicationId);
            if (m_responsePublication)
            {
                m_responsePublicationId = 0; // NULL_VALUE
            }

            if (m_counterRegistrationId != 0)
            {
                m_counter = aeron->findCounter(m_counterRegistrationId);
                if (m_counter)
                {
                    m_counterRegistrationId = 0; // NULL_VALUE
                }
            }

            if (m_responsePublication)
            {
                if (m_counter)
                {
                    // TODO: Implement AeronCounters::setReferenceId equivalent
                    // For now, we skip this as it requires access to counters metadata/values buffers
                    // AeronCounters::setReferenceId(
                    //     aeron->context().countersMetaDataBuffer(),
                    //     aeron->context().countersValuesBuffer(),
                    //     m_counter->id(),
                    //     m_responsePublication->registrationId());
                    // m_counter->setRelease(m_id);
                }

                m_timeOfLastActivityNs = nowNs;
                state(ClusterSessionState::CONNECTING, "connecting");
            }
            else
            {
                state(ClusterSessionState::INVALID, "responsePublication is null");
            }
        }
    }

    return m_responsePublication && m_responsePublication->isConnected();
}

inline std::int64_t ClusterSession::tryClaim(std::int32_t length, BufferClaim& bufferClaim)
{
    if (!m_responsePublication)
    {
        return NOT_CONNECTED;
    }
    else
    {
        return m_responsePublication->tryClaim(length, bufferClaim);
    }
}

inline std::int64_t ClusterSession::offer(
    const util::DirectBuffer& buffer,
    std::int32_t offset,
    std::int32_t length)
{
    if (!m_responsePublication)
    {
        return NOT_CONNECTED;
    }
    else
    {
        return m_responsePublication->offer(buffer, offset, length);
    }
}

inline ClusterSessionState ClusterSession::state() const
{
    return m_state;
}

inline void ClusterSession::state(ClusterSessionState newState, const std::string& reason)
{
    logStateChange(m_clusterMemberId, m_id, m_action, m_state, newState, reason);
    m_state = newState;
}

inline void ClusterSession::authenticate(const std::vector<std::uint8_t>& encodedPrincipal)
{
    if (!encodedPrincipal.empty())
    {
        m_encodedPrincipal = encodedPrincipal;
    }

    state(ClusterSessionState::AUTHENTICATED, "authenticated");
}

inline void ClusterSession::open(std::int64_t openedLogPosition)
{
    m_openedLogPosition = openedLogPosition;
    state(ClusterSessionState::OPEN, "openedLogPosition=" + std::to_string(openedLogPosition));
}

inline bool ClusterSession::appendSessionToLogAndSendOpen(
    LogPublisher& logPublisher,
    EgressPublisher& egressPublisher,
    std::int64_t leadershipTermId,
    std::int32_t memberId,
    std::int64_t nowNs,
    std::int64_t clusterTimestamp)
{
    if (m_responsePublication && m_responsePublication->availableWindow() > 0)
    {
        const std::int64_t resultingPosition = logPublisher.appendSessionOpen(
            *this, leadershipTermId, clusterTimestamp);
        if (resultingPosition > 0)
        {
            open(resultingPosition);
            timeOfLastActivityNs(nowNs);
            sendSessionOpenEvent(egressPublisher, leadershipTermId, memberId);
            return true;
        }
    }

    return false;
}

inline std::int32_t ClusterSession::sendSessionOpenEvent(
    EgressPublisher& egressPublisher,
    std::int64_t leadershipTermId,
    std::int32_t memberId)
{
    if (egressPublisher.sendEvent(*this, leadershipTermId, memberId, EventCode::OK, ""))
    {
        clearOpenEventPending();
        return 1;
    }

    return 0;
}

inline void ClusterSession::lastActivityNs(std::int64_t timeNs, std::int64_t correlationId)
{
    m_timeOfLastActivityNs = timeNs;
    m_correlationId = correlationId;
}

inline void ClusterSession::reject(
    EventCode::Value code,
    const std::string& responseDetail,
    concurrent::errors::DistinctErrorLog* errorLog)
{
    m_eventCode = code;
    m_responseDetail = responseDetail;
    const std::string codeStr = "EventCode=" + std::to_string(static_cast<std::int32_t>(code));
    const std::string reason = responseDetail.empty() ? codeStr : codeStr + ": " + responseDetail;
    state(ClusterSessionState::REJECTED, reason);
    if (errorLog)
    {
        errorLog->record(ClusterEvent(
            codeStr + " " + responseDetail + ", clusterMemberId=" + std::to_string(m_clusterMemberId) +
            ", id=" + std::to_string(m_id)));
    }
}

inline EventCode::Value ClusterSession::eventCode() const
{
    return m_eventCode;
}

inline std::string ClusterSession::responseDetail() const
{
    return m_responseDetail;
}

inline std::int64_t ClusterSession::correlationId() const
{
    return m_correlationId;
}

inline std::int64_t ClusterSession::openedLogPosition() const
{
    return m_openedLogPosition;
}

inline void ClusterSession::closedLogPosition(std::int64_t closedLogPosition)
{
    m_closedLogPosition = closedLogPosition;
}

inline std::int64_t ClusterSession::closedLogPosition() const
{
    return m_closedLogPosition;
}

inline void ClusterSession::hasNewLeaderEventPending(bool flag)
{
    m_hasNewLeaderEventPending = flag;
}

inline bool ClusterSession::hasNewLeaderEventPending() const
{
    return m_hasNewLeaderEventPending;
}

inline bool ClusterSession::hasOpenEventPending() const
{
    return m_hasOpenEventPending;
}

inline void ClusterSession::clearOpenEventPending()
{
    m_hasOpenEventPending = false;
}

inline ClusterSessionAction ClusterSession::action() const
{
    return m_action;
}

inline void ClusterSession::action(ClusterSessionAction action)
{
    m_action = action;
}

inline void ClusterSession::requestInput(std::shared_ptr<void> requestInput)
{
    m_requestInput = requestInput;
}

inline std::shared_ptr<void> ClusterSession::requestInput() const
{
    return m_requestInput;
}

inline void ClusterSession::linkIngressImage(Header& header)
{
    if (0 == m_ingressImageCorrelationId) // NULL_VALUE
    {
        // TODO: Get correlationId from header context (Image)
        // m_ingressImageCorrelationId = static_cast<Image*>(header.context())->correlationId();
    }
}

inline void ClusterSession::unlinkIngressImage()
{
    m_ingressImageCorrelationId = 0; // NULL_VALUE
}

inline std::int64_t ClusterSession::ingressImageCorrelationId() const
{
    return m_ingressImageCorrelationId;
}

inline void ClusterSession::checkEncodedPrincipalLength(const std::vector<std::uint8_t>& encodedPrincipal)
{
    if (!encodedPrincipal.empty() && encodedPrincipal.size() > MAX_ENCODED_PRINCIPAL_LENGTH)
    {
        throw ClusterException(
            "encoded principal max length " + std::to_string(MAX_ENCODED_PRINCIPAL_LENGTH) +
            " exceeded: length=" + std::to_string(encodedPrincipal.size()), SOURCEINFO);
    }
}

inline std::string ClusterSession::toString() const
{
    return std::string("ClusterSession{") +
        "id=" + std::to_string(m_id) +
        ", clusterMemberId=" + std::to_string(m_clusterMemberId) +
        ", responseStreamId=" + std::to_string(m_responseStreamId) +
        ", responseChannel='" + m_responseChannel + '\'' +
        ", sessionInfo='" + m_sessionInfo + '\'' +
        ", hasNewLeaderEventPending=" + (m_hasNewLeaderEventPending ? "true" : "false") +
        ", hasOpenEventPending=" + (m_hasOpenEventPending ? "true" : "false") +
        ", correlationId=" + std::to_string(m_correlationId) +
        ", openedLogPosition=" + std::to_string(m_openedLogPosition) +
        ", closedLogPosition=" + std::to_string(m_closedLogPosition) +
        ", timeOfLastActivityNs=" + std::to_string(m_timeOfLastActivityNs) +
        ", ingressImageCorrelationId=" + std::to_string(m_ingressImageCorrelationId) +
        ", responsePublicationId=" + std::to_string(m_responsePublicationId) +
        ", counterRegistrationId=" + std::to_string(m_counterRegistrationId) +
        ", state=" + std::to_string(static_cast<std::int32_t>(m_state)) +
        ", responseDetail='" + m_responseDetail + '\'' +
        ", eventCode=" + std::to_string(static_cast<std::int32_t>(m_eventCode)) +
        ", closeReason=" + std::to_string(static_cast<std::int32_t>(m_closeReason)) +
        ", action=" + std::to_string(static_cast<std::int32_t>(m_action)) +
        "}";
}

inline std::int64_t ClusterSession::addSessionCounter(
    std::shared_ptr<Aeron> aeron,
    AtomicBuffer& tempBuffer,
    std::int32_t clusterId)
{
    tempBuffer.putInt32(0, clusterId);
    tempBuffer.putInt64(sizeof(std::int32_t), m_id);

    const std::int32_t keyLength = sizeof(std::int32_t) + sizeof(std::int64_t);

    std::string label = "cluster-session: " + m_sessionInfo + ClusterCounters::CLUSTER_ID_LABEL_SUFFIX + std::to_string(clusterId);
    return aeron->addCounter(
        AeronCounters::CLUSTER_SESSION_TYPE_ID,
        tempBuffer.buffer(),
        keyLength,
        label);
}

inline void ClusterSession::logStateChange(
    std::int32_t memberId,
    std::int64_t sessionId,
    ClusterSessionAction action,
    ClusterSessionState oldState,
    ClusterSessionState newState,
    const std::string& reason)
{
    // Java version has this commented out, so we do nothing
    // System.out.println("ClusterSession: memberId=" + memberId + " id=" + sessionId + " action=" + action + " " +
    //     oldState + " -> " + newState + " " + reason);
}

}

// Implementation of LogPublisher and EgressPublisher functions that need ClusterSession's full definition
// These are placed here to resolve circular dependency

// LogPublisher::appendSessionOpen
inline std::int64_t cluster::LogPublisher::appendSessionOpen(
    cluster::ClusterSession& session,
    std::int64_t leadershipTermId,
    std::int64_t timestamp)
{
    std::int64_t position;
    const std::vector<std::uint8_t> encodedPrincipal = session.encodedPrincipal();
    const std::string channel = session.responseChannel();

    m_sessionOpenEvent
        .wrapAndApplyHeader(reinterpret_cast<char *>(m_expandableArrayBuffer.buffer()), 0, m_expandableArrayBuffer.capacity())
        .leadershipTermId(leadershipTermId)
        .clusterSessionId(session.id())
        .correlationId(session.correlationId())
        .timestamp(timestamp)
        .responseStreamId(session.responseStreamId())
        .putResponseChannel(channel.data(), static_cast<std::uint32_t>(channel.length()))
        .putEncodedPrincipal(reinterpret_cast<const char *>(encodedPrincipal.data()), static_cast<std::uint32_t>(encodedPrincipal.size()));

    const std::int32_t length = codecs::MessageHeader::encodedLength() + m_sessionOpenEvent.encodedLength();

    int attempts = cluster::LogPublisher::SEND_ATTEMPTS;
    do
    {
        AtomicBuffer expandableBuffer;
        expandableBuffer.wrap(m_expandableArrayBuffer.buffer(), m_expandableArrayBuffer.capacity());
        position = m_publication->offer(expandableBuffer, 0, length);
        if (position > 0)
        {
            break;
        }

        cluster::LogPublisher::checkResult(position, *m_publication);
    }
    while (--attempts > 0);

    return position;
}

// LogPublisher::appendSessionClose
inline bool cluster::LogPublisher::appendSessionClose(
    std::int32_t memberId,
    cluster::ClusterSession& session,
    std::int64_t leadershipTermId,
    std::int64_t timestamp,
    std::chrono::milliseconds::rep timeUnit)
{
    const std::int32_t length = codecs::MessageHeader::encodedLength() + codecs::SessionCloseEvent::SBE_BLOCK_LENGTH;

    int attempts = cluster::LogPublisher::SEND_ATTEMPTS;
    do
    {
        const std::int64_t position = m_publication->tryClaim(length, m_bufferClaim);
        if (position > 0)
        {
            m_sessionCloseEvent
                .wrapAndApplyHeader(reinterpret_cast<char *>(m_bufferClaim.buffer().buffer()), m_bufferClaim.offset(), m_bufferClaim.buffer().capacity())
                .leadershipTermId(leadershipTermId)
                .clusterSessionId(session.id())
                .timestamp(timestamp)
                .closeReason(session.closeReason());

            m_bufferClaim.commit();
            return true;
        }

        cluster::LogPublisher::checkResult(position, *m_publication);
    }
    while (--attempts > 0);

    return false;
}

// EgressPublisher::sendEvent
inline bool cluster::EgressPublisher::sendEvent(
    cluster::ClusterSession& session,
    std::int64_t leadershipTermId,
    std::int32_t leaderMemberId,
    codecs::EventCode::Value code,
    const std::string& detail)
{
    const std::int32_t length = static_cast<std::int32_t>(
        codecs::MessageHeader::encodedLength() +
        codecs::SessionEvent::SBE_BLOCK_LENGTH +
        codecs::SessionEvent::detailHeaderLength() +
        detail.length());

    int attempts = cluster::EgressPublisher::SEND_ATTEMPTS;
    do
    {
        const std::int64_t position = session.tryClaim(length, m_bufferClaim);
        if (position > 0)
        {
            m_sessionEventEncoder
                .wrapAndApplyHeader(reinterpret_cast<char *>(m_bufferClaim.buffer().buffer()), m_bufferClaim.offset(), m_bufferClaim.buffer().capacity())
                .clusterSessionId(session.id())
                .correlationId(session.correlationId())
                .leadershipTermId(leadershipTermId)
                .leaderMemberId(leaderMemberId)
                .code(code)
                .version(client::AeronCluster::Configuration::PROTOCOL_SEMANTIC_VERSION)
                .leaderHeartbeatTimeoutNs(m_leaderHeartbeatTimeoutNs)
                .putDetail(detail.data(), static_cast<std::uint32_t>(detail.length()));

            m_bufferClaim.commit();
            return true;
        }
    }
    while (--attempts > 0);

    return false;
}

// EgressPublisher::sendChallenge
inline bool cluster::EgressPublisher::sendChallenge(cluster::ClusterSession& session, const std::vector<std::uint8_t>& encodedChallenge)
{
    AtomicBuffer challengeBuffer;
    challengeBuffer.wrap(m_buffer.buffer(), m_buffer.capacity());
    m_challengeEncoder
        .wrapAndApplyHeader(reinterpret_cast<char *>(challengeBuffer.buffer()), 0, challengeBuffer.capacity())
        .clusterSessionId(session.id())
        .correlationId(session.correlationId());
    
    if (!encodedChallenge.empty())
    {
        m_challengeEncoder.putEncodedChallenge(reinterpret_cast<const char *>(encodedChallenge.data()), static_cast<std::uint32_t>(encodedChallenge.size()));
    }

    const std::int32_t length = static_cast<std::int32_t>(
        codecs::MessageHeader::encodedLength() + m_challengeEncoder.encodedLength());

    int attempts = cluster::EgressPublisher::SEND_ATTEMPTS;
    do
    {
        util::DirectBuffer directBuffer(m_buffer.buffer(), m_buffer.capacity());
        const std::int64_t position = session.offer(directBuffer, 0, length);
        if (position > 0)
        {
            return true;
        }
    }
    while (--attempts > 0);

    return false;
}

// EgressPublisher::newLeader
inline bool cluster::EgressPublisher::newLeader(
    cluster::ClusterSession& session,
    std::int64_t leadershipTermId,
    std::int32_t leaderMemberId,
    const std::string& ingressEndpoints)
{
    const std::int32_t length = static_cast<std::int32_t>(
        codecs::MessageHeader::encodedLength() +
        codecs::NewLeaderEvent::SBE_BLOCK_LENGTH +
        codecs::NewLeaderEvent::ingressEndpointsHeaderLength() +
        ingressEndpoints.length());

    int attempts = cluster::EgressPublisher::SEND_ATTEMPTS;
    do
    {
        const std::int64_t position = session.tryClaim(length, m_bufferClaim);
        if (position > 0)
        {
            m_newLeaderEventEncoder
                .wrapAndApplyHeader(reinterpret_cast<char *>(m_bufferClaim.buffer().buffer()), m_bufferClaim.offset(), m_bufferClaim.buffer().capacity())
                .clusterSessionId(session.id())
                .leadershipTermId(leadershipTermId)
                .leaderMemberId(leaderMemberId)
                .putIngressEndpoints(ingressEndpoints.data(), static_cast<std::uint32_t>(ingressEndpoints.length()));

            m_bufferClaim.commit();
            return true;
        }
    }
    while (--attempts > 0);

    return false;
}

// EgressPublisher::sendAdminResponse
inline bool cluster::EgressPublisher::sendAdminResponse(
    cluster::ClusterSession& session,
    std::int64_t correlationId,
    codecs::AdminRequestType::Value adminRequestType,
    codecs::AdminResponseCode::Value responseCode,
    const std::string& message)
{
    AtomicBuffer adminBuffer;
    adminBuffer.wrap(m_buffer.buffer(), m_buffer.capacity());
    m_adminResponseEncoder
        .wrapAndApplyHeader(reinterpret_cast<char *>(adminBuffer.buffer()), 0, adminBuffer.capacity())
        .clusterSessionId(session.id())
        .correlationId(correlationId)
        .requestType(adminRequestType)
        .responseCode(responseCode)
        .putMessage(message.data(), static_cast<std::uint32_t>(message.length()))
        .putPayload(nullptr, 0);

    const std::int32_t length = static_cast<std::int32_t>(
        codecs::MessageHeader::encodedLength() + m_adminResponseEncoder.encodedLength());

    int attempts = cluster::EgressPublisher::SEND_ATTEMPTS;
    do
    {
        util::DirectBuffer directBuffer(m_buffer.buffer(), m_buffer.capacity());
        const std::int64_t position = session.offer(directBuffer, 0, length);
        if (position > 0)
        {
            return true;
        }
    }
    while (--attempts > 0);

    return false;
}

}

