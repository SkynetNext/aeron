#pragma once
#include "Publication.h"

namespace aeron { namespace cluster
{

/**
 * Representation of a client session to an Aeron Cluster for use in an ConsensusModuleExtension.
 */
class ClusterClientSession
{
public:
    virtual ~ClusterClientSession() = default;

    /**
     * Cluster session identifier.
     *
     * @return cluster session identifier.
     */
    virtual std::int64_t id() = 0;

    /**
     * Determine if a cluster client session is open, so it is active for operations.
     *
     * @return true of the session is open otherwise false.
     */
    virtual bool isOpen() = 0;

    /**
     * Authenticated principal for a session encoded in byte form.
     *
     * @return authenticated principal for a session encoded in byte form.
     */
    virtual std::vector<std::uint8_t> encodedPrincipal() = 0;

    /**
     * Response Publication to be used for sending responses privately to a client.
     *
     * @return response Publication to be used for sending responses privately to a client.
     */
    virtual std::shared_ptr<Publication> responsePublication() = 0;

    /**
     * The time last activity has been recorded for a session to determine if it is active.
     *
     * @return time last activity has been recorded for a session to determine if it is active.
     */
    virtual std::int64_t timeOfLastActivityNs() = 0;

    /**
     * The time last activity recorded for a session to determine if it is active. This should be updated on valid
     * ingress.
     *
     * @param timeNs of last activity recorded for a session to determine if it is active.
     */
    virtual void timeOfLastActivityNs(std::int64_t timeNs) = 0;
};

}}

