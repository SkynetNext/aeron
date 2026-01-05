#pragma once
#include <memory>
#include "ExclusivePublication.h"
#include "Image.h"
#include "Publication.h"
#include "../client/ClusterExceptions.h"
#include "../service/Cluster.h"
#include "concurrent/logbuffer/ControlledFragmentHandler.h"
#include "concurrent/logbuffer/Header.h"
#include "concurrent/AtomicBuffer.h"
#include "generated/aeron_cluster_client/CloseReason.h"
#include "ConsensusModuleControl.h"
#include "ConsensusControlState.h"

namespace aeron { namespace cluster
{
using namespace aeron::concurrent;
using namespace aeron::concurrent::logbuffer;
using namespace aeron::cluster::codecs;

/**
 * Extension for handling messages from external schemas unknown to core Aeron Cluster code
 * thus providing an extension to the core ingress consensus module behaviour.
 */
class ConsensusModuleExtension
{
public:
    virtual ~ConsensusModuleExtension() = default;

    /**
     * Schema supported by this extension.
     *
     * @return schema id supported.
     */
    virtual std::int32_t supportedSchemaId() = 0;

    /**
     * Start event where the extension can perform any initialisation required and load snapshot state.
     * The snapshot image can be null if no previous snapshot exists.
     * <p>
     * <b>Note:</b> As this is a potentially long-running operation the implementation should use
     * Cluster::idleStrategy() and then occasionally call IdleStrategy::idle() or
     * IdleStrategy::idle(int), especially when polling the Image returns 0.
     *
     * @param consensusModuleControl with which the extension can interact.
     * @param snapshotImage          from which the extension can load its state which can be null when no snapshot.
     */
    virtual void onStart(
        ConsensusModuleControl& consensusModuleControl,
        std::shared_ptr<Image> snapshotImage) = 0;

    /**
     * An extension should implement this method to do its work. Long-running operations should be decomposed.
     * <p>
     * The return value is used for implementing an idle strategy that can be employed when no work is
     * currently available for the extension to process.
     * <p>
     * If the extension wished to terminate and close then an AgentTerminationException can be thrown.
     *
     * @param nowNs is cluster time in nanoseconds.
     * @return 0 to indicate no work was currently available, a positive value otherwise.
     */
    virtual std::int32_t doWork(std::int64_t nowNs) = 0;

    /**
     * Similar to doWork(long), but executed less frequently.
     *
     * @param nowNs is cluster time in nanoseconds.
     * @return 0 to indicate no work was currently available, a positive value otherwise.
     */
    virtual std::int32_t slowTickWork(std::int64_t nowNs) = 0;

    /**
     * Similar to doWork(long), but executed only when there's no election in progress.
     *
     * @param nowNs is cluster time in nanoseconds.
     * @return 0 to indicate no work was currently available, a positive value otherwise.
     */
    virtual std::int32_t consensusWork(std::int64_t nowNs) = 0;

    /**
     * Cluster election is complete and new publication is added for the leadership term. If the node is a follower
     * then the publication will be null.
     *
     * @param consensusControlState state to allow extension to control the consensus module.
     */
    virtual void onElectionComplete(ConsensusControlState& consensusControlState) = 0;

    /**
     * New Leadership term and the consensus control state has changed.
     *
     * @param consensusControlState state to allow extension to control the consensus module.
     */
    virtual void onNewLeadershipTerm(ConsensusControlState& consensusControlState) = 0;

    /**
     * Callback for handling messages received as ingress to a cluster.
     * <p>
     * Within this callback reentrant calls to the Aeron client are not permitted and
     * will result in undefined behaviour.
     *
     * @param actingBlockLength acting block length.
     * @param templateId        the message template id (already parsed from header).
     * @param schemaId          the schema id.
     * @param actingVersion     acting version from header
     * @param buffer            containing the data.
     * @param offset            at which the data begins.
     * @param length            of the data in bytes.
     * @param header            representing the metadata for the data.
     * @return The action to be taken with regard to the stream position after the callback.
     */
    virtual ControlledPollAction onIngressExtensionMessage(
        std::int32_t actingBlockLength,
        std::int32_t templateId,
        std::int32_t schemaId,
        std::int32_t actingVersion,
        AtomicBuffer& buffer,
        std::int32_t offset,
        std::int32_t length,
        Header& header) = 0;

    /**
     * Callback for handling committed log messages (for follower or recovery).
     * <p>
     * Within this callback reentrant calls to the Aeron client are not permitted and
     * will result in undefined behaviour.
     *
     * @param actingBlockLength acting block length.
     * @param templateId        the message template id (already parsed from header).
     * @param schemaId          the schema id.
     * @param actingVersion     acting version from header
     * @param buffer            containing the data.
     * @param offset            at which the data begins.
     * @param length            of the data in bytes.
     * @param header            representing the metadata for the data.
     * @return The action to be taken with regard to the stream position after the callback.
     */
    virtual ControlledPollAction onLogExtensionMessage(
        std::int32_t actingBlockLength,
        std::int32_t templateId,
        std::int32_t schemaId,
        std::int32_t actingVersion,
        AtomicBuffer& buffer,
        std::int32_t offset,
        std::int32_t length,
        Header& header) = 0;

    /**
     * Close the extension.
     */
    virtual void close() = 0;

    /**
     * Callback indicating a cluster session has opened.
     *
     * @param clusterSessionId of the opened session which is unique and not reused.
     */
    virtual void onSessionOpened(std::int64_t clusterSessionId) = 0;

    /**
     * Callback indicating a cluster session has closed.
     *
     * @param clusterSessionId  of the opened session which is unique and not reused.
     * @param closeReason       reason to closing session
     */
    virtual void onSessionClosed(std::int64_t clusterSessionId, CloseReason::Value closeReason) = 0;

    /**
     * Callback when preparing for a new Raft leadership term - before election.
     */
    virtual void onPrepareForNewLeadership() = 0;

    /**
     * The extension should take a snapshot and store its state to the provided archive ExclusivePublication.
     * <p>
     * <b>Note:</b> As this is a potentially long-running operation the implementation should use
     * Cluster::idleStrategy() and then occasionally call IdleStrategy::idle() or
     * IdleStrategy::idle(int),
     * especially when the snapshot ExclusivePublication returns Publication::BACK_PRESSURED.
     *
     * @param snapshotPublication to which the state should be recorded.
     */
    virtual void onTakeSnapshot(std::shared_ptr<ExclusivePublication> snapshotPublication) = 0;
};

}}

