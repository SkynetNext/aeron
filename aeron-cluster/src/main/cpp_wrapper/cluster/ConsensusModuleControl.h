#pragma once
#include <memory>
#include <chrono>
#include "Aeron.h"
#include "archive/client/AeronArchive.h"
#include "security/AuthorisationService.h"
#include "concurrent/IdleStrategy.h"
#include "ClusterClientSession.h"
#include "ClusterMember.h"

namespace aeron { namespace cluster
{
using namespace aeron::archive::client;
using namespace aeron::security;
using namespace aeron::concurrent;

class ConsensusModule; // Forward declaration

/**
 * Control interface for performing operations on the consensus module from a ConsensusModuleExtension.
 */
class ConsensusModuleControl
{
public:
    virtual ~ConsensusModuleControl() = default;

    /**
     * The unique id for the hosting member of the cluster.
     *
     * @return unique id for the hosting member of the cluster.
     */
    virtual std::int32_t memberId() = 0;

    /**
     * Cluster time as timeUnit()s since 1 Jan 1970 UTC.
     *
     * @return time as timeUnit()s since 1 Jan 1970 UTC.
     * @see timeUnit()
     */
    virtual std::int64_t time() = 0;

    /**
     * The unit of time applied when timestamping and invoking time() operations.
     *
     * @return the unit of time applied when timestamping and invoking time() operations.
     * @see time()
     */
    virtual std::chrono::milliseconds::rep timeUnit() = 0;

    /**
     * IdleStrategy which should be used by the extension when it experiences back-pressure or is undertaking
     * any long-running actions.
     *
     * @return the IdleStrategy which should be used by the extension when it experiences back-pressure or is
     * undertaking any long-running actions.
     */
    virtual std::shared_ptr<IdleStrategy> idleStrategy() = 0;

    /**
     * The ConsensusModule::Context under which the extension is running.
     *
     * @return the ConsensusModule::Context under which the extension is running.
     */
    virtual ConsensusModule::Context& context() = 0;

    /**
     * The Aeron client to be used by the extension.
     *
     * @return the Aeron client to be used by the extension.
     */
    virtual std::shared_ptr<Aeron> aeron() = 0;

    /**
     * The AeronArchive client to be used by the extension.
     *
     * @return the AeronArchive client to be used by the extension.
     */
    virtual std::shared_ptr<AeronArchive> archive() = 0;

    /**
     * The AuthorisationService used by the consensus module.
     *
     * @return the AuthorisationService used by the consensus module.
     */
    virtual std::shared_ptr<AuthorisationService> authorisationService() = 0;

    /**
     * Lookup a ClusterClientSession for a given id.
     *
     * @param clusterSessionId for the session to lookup.
     * @return a ClusterClientSession for a given id, otherwise nullptr if not found.
     */
    virtual std::shared_ptr<ClusterClientSession> getClientSession(std::int64_t clusterSessionId) = 0;

    /**
     * Close a cluster session as an administrative function.
     *
     * @param clusterSessionId to be closed.
     */
    virtual void closeClusterSession(std::int64_t clusterSessionId) = 0;

    /**
     * Numeric id for the commit position counter.
     *
     * @return commit position counter id.
     */
    virtual std::int32_t commitPositionCounterId() = 0;

    /**
     * Numeric id for the cluster (used when running multiple clusters on the same media driver).
     *
     * @return numeric id for the cluster.
     * @see ConsensusModule::Context::clusterId(int)
     */
    virtual std::int32_t clusterId() = 0;

    /**
     * The current cluster member for this node.
     *
     * @return cluster member for this node.
     */
    virtual ClusterMember clusterMember() = 0;
};

}}

