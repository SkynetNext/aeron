#pragma once

#include <memory>
#include <string>
#include <vector>
#include <cstdint>
#include "Aeron.h"
#include "Counter.h"
#include "Publication.h"
#include "Image.h"
#include "concurrent/logbuffer/BufferClaim.h"
#include "concurrent/logbuffer/Header.h"
#include "util/DirectBuffer.h"
#include "util/Exceptions.h"
#include "../client/ClusterExceptions.h"
#include "../client/ClusterEvent.h"
#include "aeron_cluster/CloseReason.h"
#include "aeron_cluster/EventCode.h"
#include "archive/client/AeronArchive.h"
#include "../service/ClusterCounters.h"
#include "concurrent/errors/DistinctErrorLog.h"
#include "ClusterClientSession.h"
#include "LogPublisher.h"
#include "EgressPublisher.h"

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

    std::int64_t id() const override;

    std::vector<std::uint8_t> encodedPrincipal() const override;

    bool isOpen() const override;

    std::shared_ptr<Publication> responsePublication() const override;

    std::int64_t timeOfLastActivityNs() const override;

    void timeOfLastActivityNs(std::int64_t timeNs);

    void loadSnapshotState(
        std::int64_t correlationId,
        std::int64_t openedLogPosition,
        std::int64_t timeOfLastActivityNs,
        CloseReason closeReason);

    std::int32_t responseStreamId() const;

    std::string responseChannel() const;

    void closing(CloseReason closeReason);

    CloseReason closeReason() const override;

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

    void reject(EventCode code, const std::string& responseDetail, DistinctErrorLog* errorLog);

    EventCode eventCode() const;

    std::string responseDetail() const;

    std::int64_t correlationId() const override;

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
    EventCode m_eventCode = EventCode::NULL_VAL;
    CloseReason m_closeReason = CloseReason::NULL_VAL;
    std::vector<std::uint8_t> m_encodedPrincipal;
    ClusterSessionAction m_action = ClusterSessionAction::CLIENT;
    std::shared_ptr<void> m_requestInput;
};

}}

