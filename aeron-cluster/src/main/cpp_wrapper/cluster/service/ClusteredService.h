/*
 * Copyright 2014-2025 Justin Zhu.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once

#include <chrono>
#include "ExclusivePublication.h"
#include "Image.h"
#include "concurrent/logbuffer/Header.h"
#include "concurrent/AtomicBuffer.h"
#include "generated/aeron_cluster_codecs/CloseReason.h"
#include "ClientSession.h"

// Forward declaration
namespace aeron { namespace cluster { namespace service
{
    class Cluster;
}}}

namespace aeron { namespace cluster { namespace service
{

using namespace aeron::concurrent;
using namespace aeron::concurrent::logbuffer;
using namespace aeron::cluster::codecs;

/**
 * Interface which a service must implement to be contained in the cluster.
 * 
 * The cluster object should only be used to send messages to the cluster or schedule timers in
 * response to other messages and timers. Sending messages and timers should not happen from cluster lifecycle
 * methods like onStart(Cluster, Image), onRoleChange(Cluster::Role) or
 * onTakeSnapshot(ExclusivePublication), or onTerminate(Cluster), except the session lifecycle
 * methods.
 */
class ClusteredService
{
public:
    virtual ~ClusteredService() = default;

    /**
     * Start event where the service can perform any initialisation required and load snapshot state.
     * The snapshot image can be null if no previous snapshot exists.
     * 
     * Note: As this is a potentially long-running operation the implementation should use
     * Cluster::idleStrategy() and then occasionally call IdleStrategy::idle() or
     * IdleStrategy::idle(int), especially when polling the Image returns 0.
     *
     * @param cluster       with which the service can interact.
     * @param snapshotImage from which the service can load its archived state which can be null when no snapshot.
     */
    virtual void onStart(Cluster& cluster, std::shared_ptr<Image> snapshotImage) = 0;

    /**
     * A session has been opened for a client to the cluster.
     *
     * @param session   for the client which have been opened.
     * @param timestamp at which the session was opened.
     */
    virtual void onSessionOpen(ClientSession& session, std::int64_t timestamp) = 0;

    /**
     * A session has been closed for a client to the cluster.
     *
     * @param session     that has been closed.
     * @param timestamp   at which the session was closed.
     * @param closeReason the session was closed.
     */
    virtual void onSessionClose(ClientSession& session, std::int64_t timestamp, CloseReason::Value closeReason) = 0;

    /**
     * A message has been received to be processed by a clustered service.
     *
     * @param session   for the client which sent the message. This can be null if the client was a service.
     * @param timestamp for when the message was received.
     * @param buffer    containing the message.
     * @param offset    in the buffer at which the message is encoded.
     * @param length    of the encoded message.
     * @param header    aeron header for the incoming message.
     */
    virtual void onSessionMessage(
        ClientSession* session,
        std::int64_t timestamp,
        AtomicBuffer& buffer,
        std::int32_t offset,
        std::int32_t length,
        Header& header) = 0;

    /**
     * A scheduled timer has expired.
     *
     * @param correlationId for the expired timer.
     * @param timestamp     at which the timer expired.
     */
    virtual void onTimerEvent(std::int64_t correlationId, std::int64_t timestamp) = 0;

    /**
     * The service should take a snapshot and store its state to the provided archive ExclusivePublication.
     * 
     * Note: As this is a potentially long-running operation the implementation should use
     * Cluster::idleStrategy() and then occasionally call IdleStrategy::idle() or
     * IdleStrategy::idle(int),
     * especially when the snapshot ExclusivePublication returns Publication::BACK_PRESSURED.
     *
     * @param snapshotPublication to which the state should be recorded.
     */
    virtual void onTakeSnapshot(std::shared_ptr<ExclusivePublication> snapshotPublication) = 0;

    /**
     * Notify that the cluster node has changed role.
     *
     * @param newRole that the node has assumed.
     */
    virtual void onRoleChange(/* Cluster::Role newRole */) = 0; // TODO: Forward declare Cluster::Role

    /**
     * Called when the container is going to terminate but only after a successful start.
     *
     * @param cluster with which the service can interact.
     */
    virtual void onTerminate(Cluster& cluster) = 0;

    /**
     * An election has been successful and a leader has entered a new term.
     *
     * @param leadershipTermId    identity for the new leadership term.
     * @param logPosition         position the log has reached as the result of this message.
     * @param timestamp           for the new leadership term.
     * @param termBaseLogPosition position at the beginning of the leadership term.
     * @param leaderMemberId      who won the election.
     * @param logSessionId        session id for the publication of the log.
     * @param timeUnit            for the timestamps in the coming leadership term.
     * @param appVersion          for the application configured in the consensus module.
     */
    virtual void onNewLeadershipTermEvent(
        std::int64_t leadershipTermId,
        std::int64_t logPosition,
        std::int64_t timestamp,
        std::int64_t termBaseLogPosition,
        std::int32_t leaderMemberId,
        std::int32_t logSessionId,
        std::chrono::milliseconds::rep timeUnit,
        std::int32_t appVersion)
    {
        // Default implementation does nothing
    }

    /**
     * Implement this method to perform background tasks that are not related to the deterministic state machine
     * model, such as keeping external connections alive to the cluster. This method must not be used to
     * directly, or indirectly, update the service state. This method cannot be used for making calls on
     * Cluster which could update the log such as Cluster::scheduleTimer(long, long) or
     * Cluster::offer(AtomicBuffer, int, int).
     * 
     * This method is not for long-running operations. Time taken can impact latency and should only be used for
     * short constant time operations.
     *
     * @param nowNs which can be used for measuring elapsed time and be used in the same way as
     *              System::nanoTime(). This is not Cluster::time().
     * @return 0 if no work is done otherwise a positive number.
     */
    virtual int doBackgroundWork(std::int64_t nowNs)
    {
        return 0;
    }
};

}}}

