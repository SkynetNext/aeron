#pragma once

#include <memory>
#include <vector>
#include <functional>
#include <cstdint>
#include <string>
#include "ExclusivePublication.h"
#include "Image.h"
#include "client/AeronCluster.h"
#include "client/ClusterExceptions.h"
#include "generated/aeron_cluster_codecs/CloseReason.h"
#include "concurrent/logbuffer/BufferClaim.h"
#include "concurrent/logbuffer/Header.h"
#include "util/DirectBuffer.h"
#include "concurrent/IdleStrategy.h"
#include "concurrent/AtomicCounter.h"
#include "ClientSession.h"

namespace aeron { namespace cluster { namespace service {

using namespace aeron::concurrent;
using namespace aeron::concurrent::logbuffer;

/**
 * Role of the node in the cluster.
 */
enum class Role : std::int32_t
{
    /**
     * The cluster node is a follower in the current leadership term.
     */
    FOLLOWER = 0,

    /**
     * The cluster node is a candidate to become a leader in an election.
     */
    CANDIDATE = 1,

    /**
     * The cluster node is the leader for the current leadership term.
     */
    LEADER = 2
};

/**
 * Get the role from a code read from a counter.
 *
 * @param code for the Role.
 * @return the Role of the cluster node.
 */
inline Role getRole(std::int64_t code)
{
    if (code < 0 || code > 2)
    {
        throw IllegalStateException("Invalid role counter code: " + std::to_string(code), SOURCEINFO);
    }
    return static_cast<Role>(code);
}

/**
 * Get the role by reading the code from a counter.
 *
 * @param counter containing the value of the role.
 * @return the role for the cluster member.
 */
inline Role getRole(std::shared_ptr<AtomicCounter> counter)
{
    if (!counter)
    {
        return Role::FOLLOWER;
    }
    return getRole(counter->get());
}

/**
 * Interface for a ClusteredService to interact with cluster hosting it.
 */
class Cluster
{
public:
    virtual ~Cluster() = default;

    /**
     * The unique id for the hosting member of the cluster. Useful only for debugging purposes.
     *
     * @return unique id for the hosting member of the cluster.
     */
    virtual std::int32_t memberId() = 0;

    /**
     * The role the cluster node is playing.
     *
     * @return the role the cluster node is playing.
     */
    virtual Role role() = 0;

    /**
     * Position the log has reached in bytes as of the current message.
     *
     * @return position the log has reached in bytes as of the current message.
     */
    virtual std::int64_t logPosition() = 0;

    /**
     * Get the Aeron client used by the cluster.
     *
     * @return the Aeron client used by the cluster.
     */
    virtual std::shared_ptr<Aeron> aeron() = 0;

    /**
     * Get the ClusteredServiceContainer::Context under which the container is running.
     *
     * @return the ClusteredServiceContainer::Context under which the container is running.
     */
    // TODO: Implement ClusteredServiceContainer::Context
    // virtual ClusteredServiceContainer::Context& context() = 0;

    /**
     * Get the ClientSession for a given cluster session id.
     *
     * @param clusterSessionId to be looked up.
     * @return the ClientSession that matches the clusterSessionId.
     */
    virtual std::shared_ptr<ClientSession> getClientSession(std::int64_t clusterSessionId) = 0;

    /**
     * Get the current collection of cluster client sessions.
     *
     * @return the current collection of cluster client sessions.
     */
    virtual std::vector<std::shared_ptr<ClientSession>> clientSessions() = 0;

    /**
     * For each iterator over ClientSessions using the most efficient method possible.
     *
     * @param action to be taken for each ClientSession in turn.
     */
    virtual void forEachClientSession(std::function<void(std::shared_ptr<ClientSession>)> action) = 0;

    /**
     * Request the close of a ClientSession by sending the request to the consensus module.
     *
     * @param clusterSessionId to be closed.
     * @return true if the event to close a session was sent or false if back pressure was applied.
     * @throws ClusterException if the clusterSessionId is not recognised.
     */
    virtual bool closeClientSession(std::int64_t clusterSessionId) = 0;

    /**
     * Cluster time as timeUnit()s since 1 Jan 1970 UTC.
     *
     * @return time as timeUnit()s since 1 Jan 1970 UTC.
     */
    virtual std::int64_t time() = 0;

    /**
     * The unit of time applied when timestamping and time() operations.
     *
     * @return the unit of time applied when timestamping and time() operations.
     */
    virtual std::chrono::milliseconds::rep timeUnit() = 0;

    /**
     * Schedule a timer for a given deadline and provide a correlation id to identify the timer when it expires or
     * for cancellation.
     *
     * @param correlationId to identify the timer when it expires. Long::MAX_VALUE not supported.
     * @param deadline      time after which the timer will fire. Long::MAX_VALUE not supported.
     * @return true if the request to schedule a timer has been sent or false in case of ADMIN_ACTION or BACK_PRESSURED.
     * @throws ClusterException if request fails with an error.
     */
    virtual bool scheduleTimer(std::int64_t correlationId, std::int64_t deadline) = 0;

    /**
     * Cancel a previously scheduled timer.
     *
     * @param correlationId for the timer provided when it was scheduled. Long::MAX_VALUE not supported.
     * @return true if the request to cancel a timer has been sent or false in case of ADMIN_ACTION or BACK_PRESSURED.
     * @throws ClusterException if request fails with an error.
     */
    virtual bool cancelTimer(std::int64_t correlationId) = 0;

    /**
     * Offer a message as ingress to the cluster for sequencing.
     *
     * @param buffer containing the message to be offered.
     * @param offset in the buffer at which the encoded message begins.
     * @param length in the buffer of the encoded message.
     * @return positive value if successful.
     */
    virtual std::int64_t offer(const util::DirectBuffer& buffer, std::int32_t offset, std::int32_t length) = 0;

    /**
     * Try to claim a range in the publication log into which a message can be written with zero copy semantics.
     *
     * @param length      of the range to claim, in bytes.
     * @param bufferClaim to be populated if the claim succeeds.
     * @return positive value if successful.
     * @throws IllegalArgumentException if the length is greater than Publication::maxPayloadLength().
     */
    virtual std::int64_t tryClaim(std::int32_t length, BufferClaim& bufferClaim) = 0;

    /**
     * IdleStrategy which should be used by the service when it experiences back-pressure on egress,
     * closing sessions, making timer requests, or any long-running actions.
     *
     * @return the IdleStrategy which should be used by the service when it experiences back-pressure.
     */
    virtual std::shared_ptr<IdleStrategy> idleStrategy() = 0;
};

}}}

