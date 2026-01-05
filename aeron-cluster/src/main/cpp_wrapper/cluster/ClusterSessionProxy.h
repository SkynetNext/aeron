#pragma once
#include <vector>
#include "generated/aeron_cluster_client/EventCode.h"

namespace aeron { namespace cluster
{
using namespace aeron::cluster::codecs;

class ClusterSession; // Forward declaration
class EgressPublisher; // Forward declaration

/**
 * Proxy for a session being authenticated by an Authenticator.
 * 
 * Note: SessionProxy interface is not yet translated to C++, so this class
 * implements the SessionProxy interface methods directly.
 */
class ClusterSessionProxy
{
public:
    explicit ClusterSessionProxy(EgressPublisher& egressPublisher);

    ClusterSessionProxy& session(ClusterSession& clusterSession);

    /**
     * The identity of the potential session assigned by the system.
     *
     * @return identity for the potential session.
     */
    std::int64_t sessionId();

    /**
     * Inform the system that the session requires a challenge by sending the provided encoded challenge.
     *
     * @param encodedChallenge to be sent to the client.
     * @return true if challenge was accepted to be sent at present time or false if it will be retried later.
     */
    bool challenge(const std::vector<std::uint8_t>& encodedChallenge);

    /**
     * Inform the system that the session has met authentication requirements.
     *
     * @param encodedPrincipal that has passed authentication.
     * @return true if authentication was accepted at present time or false if it will be retried later.
     */
    bool authenticate(const std::vector<std::uint8_t>& encodedPrincipal);

    /**
     * Inform the system that the session should be rejected.
     */
    void reject();

private:
    EgressPublisher& m_egressPublisher;
    ClusterSession* m_clusterSession;
};

// Implementation
inline ClusterSessionProxy::ClusterSessionProxy(EgressPublisher& egressPublisher) :
    m_egressPublisher(egressPublisher),
    m_clusterSession(nullptr)
{
}

inline ClusterSessionProxy& ClusterSessionProxy::session(ClusterSession& clusterSession)
{
    m_clusterSession = &clusterSession;
    return *this;
}

inline std::int64_t ClusterSessionProxy::sessionId()
{
    return m_clusterSession->id();
}

inline bool ClusterSessionProxy::challenge(const std::vector<std::uint8_t>& encodedChallenge)
{
    if (m_egressPublisher.sendChallenge(*m_clusterSession, encodedChallenge))
    {
        m_clusterSession->state(ClusterSessionState::CHALLENGED, "challenged");
        return true;
    }

    return false;
}

inline bool ClusterSessionProxy::authenticate(const std::vector<std::uint8_t>& encodedPrincipal)
{
    ClusterSession::checkEncodedPrincipalLength(encodedPrincipal);
    m_clusterSession->authenticate(encodedPrincipal);
    return true;
}

inline void ClusterSessionProxy::reject()
{
    // SESSION_REJECTED_MSG constant would be defined in ConsensusModule::Configuration
    // For now, using a default message
    static constexpr const char* SESSION_REJECTED_MSG = "session rejected";
    m_clusterSession->reject(EventCode::AUTHENTICATION_REJECTED, SESSION_REJECTED_MSG, nullptr);
}

}}

