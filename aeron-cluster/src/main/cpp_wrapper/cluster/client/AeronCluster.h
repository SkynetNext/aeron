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

#include <memory>
#include <string>
#include <atomic>
#include <functional>
#include <unordered_map>
#include <vector>
#include <chrono>
#include <typeindex>
#include <stdexcept>
#include <sstream>
#include <algorithm>
#include <cstdlib>
#include <thread>

#include "Aeron.h"
#include "Publication.h"
#include "ExclusivePublication.h"
#include "Subscription.h"
#include "Image.h"
#include "FragmentAssembler.h"
#include "ControlledFragmentAssembler.h"
#include "ChannelUri.h"
#include "Context.h"
#include "ClientConductor.h"
#include "concurrent/logbuffer/BufferClaim.h"
#include "concurrent/AtomicBuffer.h"
#include "concurrent/logbuffer/Header.h"
#include "concurrent/BackOffIdleStrategy.h"
#include "util/StringUtil.h"
#include "ClusterExceptions.h"
#include "EgressListener.h"
#include "ControlledEgressListener.h"
#include "EgressPoller.h"
#include "CredentialsSupplier.h"

#include "generated/aeron_cluster_codecs/MessageHeader.h"
#include "generated/aeron_cluster_codecs/SessionMessageHeader.h"
#include "generated/aeron_cluster_codecs/SessionEvent.h"
#include "generated/aeron_cluster_codecs/NewLeaderEvent.h"
#include "generated/aeron_cluster_codecs/SessionConnectRequest.h"
#include "generated/aeron_cluster_codecs/SessionCloseRequest.h"
#include "generated/aeron_cluster_codecs/SessionKeepAlive.h"
#include "generated/aeron_cluster_codecs/ChallengeResponse.h"
#include "generated/aeron_cluster_codecs/AdminRequest.h"
#include "generated/aeron_cluster_codecs/AdminResponse.h"
#include "generated/aeron_cluster_codecs/EventCode.h"
#include "generated/aeron_cluster_codecs/AdminRequestType.h"
#include "generated/aeron_cluster_codecs/AdminResponseCode.h"

using namespace aeron::concurrent;
using namespace aeron::concurrent::logbuffer;

// Forward declaration for test fixture (only defined in test files, in global namespace)
class AeronClusterTestFixture;

namespace aeron { namespace cluster { namespace client
{

// Type aliases for SBE codecs (Java uses Encoder/Decoder, C++ uses single class)
using MessageHeaderEncoder = MessageHeader;
using MessageHeaderDecoder = MessageHeader;
using SessionMessageHeaderEncoder = SessionMessageHeader;
using SessionMessageHeaderDecoder = SessionMessageHeader;

/**
 * Client for interacting with an Aeron Cluster.
 * 
 * A client will attempt to open a session and then offer ingress messages which are replicated to clustered services
 * for reliability. If the clustered service responds then response messages and events are sent via the egress stream.
 * 
 * Note: Instances of this class are not threadsafe.
 * 
 * This class corresponds to io.aeron.cluster.client.AeronCluster
 */
class AeronCluster
{
    // Allow test classes to access private methods for 1:1 test translation
    friend class ::AeronClusterTestFixture;

public:
    // Forward declaration
    class MemberIngress;

public:
    /**
     * Length of a session message header for cluster ingress or egress.
     */
    static constexpr int SESSION_HEADER_LENGTH = 
        static_cast<int>(MessageHeaderEncoder::encodedLength() + SessionMessageHeaderEncoder::SBE_BLOCK_LENGTH);

    /**
     * Configuration options for cluster client.
     */
    class Configuration
    {
    public:
        /**
         * Major version of the network protocol from client to consensus module.
         */
        static constexpr int PROTOCOL_MAJOR_VERSION = 0;

        /**
         * Minor version of the network protocol from client to consensus module.
         */
        static constexpr int PROTOCOL_MINOR_VERSION = 3;

        /**
         * Patch version of the network protocol from client to consensus module.
         */
        static constexpr int PROTOCOL_PATCH_VERSION = 0;

        /**
         * Combined semantic version for the client to consensus module protocol.
         * Semantic version is composed as: (major << 16) | (minor << 8) | patch
         */
        static constexpr int PROTOCOL_SEMANTIC_VERSION = 
            (PROTOCOL_MAJOR_VERSION << 16) | (PROTOCOL_MINOR_VERSION << 8) | PROTOCOL_PATCH_VERSION;

        /**
         * Property name for message timeout.
         */
        static constexpr const char* MESSAGE_TIMEOUT_PROP_NAME = "aeron.cluster.message.timeout";

        /**
         * Default timeout when waiting on a message to be sent or received (5 seconds in nanoseconds).
         */
        static constexpr std::int64_t MESSAGE_TIMEOUT_DEFAULT_NS = 5000000000LL;

        /**
         * Property name for ingress endpoints.
         */
        static constexpr const char* INGRESS_ENDPOINTS_PROP_NAME = "aeron.cluster.ingress.endpoints";

        /**
         * Property name for ingress channel.
         */
        static constexpr const char* INGRESS_CHANNEL_PROP_NAME = "aeron.cluster.ingress.channel";

        /**
         * Property name for ingress stream id.
         */
        static constexpr const char* INGRESS_STREAM_ID_PROP_NAME = "aeron.cluster.ingress.stream.id";

        /**
         * Default stream id for ingress channel.
         */
        static constexpr int INGRESS_STREAM_ID_DEFAULT = 101;

        /**
         * Property name for egress channel.
         */
        static constexpr const char* EGRESS_CHANNEL_PROP_NAME = "aeron.cluster.egress.channel";

        /**
         * Property name for egress stream id.
         */
        static constexpr const char* EGRESS_STREAM_ID_PROP_NAME = "aeron.cluster.egress.stream.id";

        /**
         * Default stream id for egress channel.
         */
        static constexpr int EGRESS_STREAM_ID_DEFAULT = 102;

        /**
         * Property name for client name.
         */
        static constexpr const char* CLIENT_NAME_PROP_NAME = "aeron.cluster.client.name";

        /**
         * Get message timeout in nanoseconds from environment or default.
         */
        static std::int64_t messageTimeoutNs();

        /**
         * Get ingress endpoints from environment or empty string.
         */
        static std::string ingressEndpoints();

        /**
         * Get ingress channel from environment or empty string.
         */
        static std::string ingressChannel();

        /**
         * Get ingress stream id from environment or default.
         */
        static int ingressStreamId();

        /**
         * Get egress channel from environment or empty string.
         */
        static std::string egressChannel();

        /**
         * Get egress stream id from environment or default.
         */
        static int egressStreamId();

        /**
         * Get client name from environment or empty string.
         */
        static std::string clientName();
    };

    /**
     * Context for cluster session and connection.
     * 
     * This class corresponds to io.aeron.cluster.client.AeronCluster.Context
     */
    class Context
    {
        friend class AeronCluster;

    public:
        using this_t = Context;

        Context();
        
        Context(const Context&) = delete;
        Context& operator=(const Context&) = delete;
        Context(Context&&) = delete;
        Context& operator=(Context&&) = delete;
        
        ~Context();

        /**
         * Conclude configuration by setting up defaults when specifics are not provided.
         */
        void conclude();

        /**
         * Has the context had the conclude() method called.
         *
         * @return true if the conclude() method has been called.
         */
        bool isConcluded() const;

        /**
         * Set the message timeout in nanoseconds.
         */
        this_t& messageTimeoutNs(std::int64_t messageTimeoutNs);

        /**
         * Get the message timeout in nanoseconds.
         */
        std::int64_t messageTimeoutNs() const;

        /**
         * Set the new leader timeout in nanoseconds.
         */
        this_t& newLeaderTimeoutNs(std::int64_t newLeaderTimeoutNs);

        /**
         * Get the new leader timeout in nanoseconds.
         */
        std::int64_t newLeaderTimeoutNs() const;

        /**
         * Set ingress endpoints.
         */
        this_t& ingressEndpoints(const std::string& ingressEndpoints);

        /**
         * Get ingress endpoints.
         */
        const std::string& ingressEndpoints() const;

        /**
         * Set ingress channel.
         */
        this_t& ingressChannel(const std::string& ingressChannel);

        /**
         * Get ingress channel.
         */
        const std::string& ingressChannel() const;

        /**
         * Set ingress stream id.
         */
        this_t& ingressStreamId(std::int32_t ingressStreamId);

        /**
         * Get ingress stream id.
         */
        std::int32_t ingressStreamId() const;

        /**
         * Set egress channel.
         */
        this_t& egressChannel(const std::string& egressChannel);

        /**
         * Get egress channel.
         */
        const std::string& egressChannel() const;

        /**
         * Set egress stream id.
         */
        this_t& egressStreamId(std::int32_t egressStreamId);

        /**
         * Get egress stream id.
         */
        std::int32_t egressStreamId() const;

        /**
         * Set idle strategy.
         */
        template<typename IdleStrategyType>
        this_t& idleStrategy(std::shared_ptr<IdleStrategyType> idleStrategy)
        {
            m_idleStrategy = std::static_pointer_cast<void>(idleStrategy);
            m_idleStrategyType = std::type_index(typeid(IdleStrategyType));
            return *this;
        }

        /**
         * Get idle strategy.
         */
        template<typename IdleStrategyType>
        std::shared_ptr<IdleStrategyType> idleStrategy() const
        {
            if (m_idleStrategyType == std::type_index(typeid(IdleStrategyType)))
            {
                return std::static_pointer_cast<IdleStrategyType>(m_idleStrategy);
            }
            return nullptr;
        }

        /**
         * Set client name.
         */
        this_t& clientName(const std::string& clientName);

        /**
         * Get client name.
         */
        const std::string& clientName() const;

        /**
         * Set aeron directory name.
         */
        this_t& aeronDirectoryName(const std::string& aeronDirectoryName);

        /**
         * Get aeron directory name.
         */
        const std::string& aeronDirectoryName() const;

        /**
         * Set Aeron client.
         */
        this_t& aeron(std::shared_ptr<Aeron> aeron);

        /**
         * Get Aeron client.
         */
        std::shared_ptr<Aeron> aeron() const;

        /**
         * Set whether this context owns the Aeron client.
         */
        this_t& ownsAeronClient(bool ownsAeronClient);

        /**
         * Get whether this context owns the Aeron client.
         */
        bool ownsAeronClient() const;

        /**
         * Set whether ingress is exclusive.
         */
        this_t& isIngressExclusive(bool isIngressExclusive);

        /**
         * Get whether ingress is exclusive.
         */
        bool isIngressExclusive() const;

        /**
         * Set credentials supplier.
         */
        this_t& credentialsSupplier(std::shared_ptr<CredentialsSupplier> credentialsSupplier);

        /**
         * Get credentials supplier.
         */
        std::shared_ptr<CredentialsSupplier> credentialsSupplier() const;

        /**
         * Set error handler.
         */
        this_t& errorHandler(const exception_handler_t& errorHandler);

        /**
         * Get error handler.
         */
        const exception_handler_t& errorHandler() const;

        /**
         * Set whether to use direct assemblers.
         */
        this_t& isDirectAssemblers(bool isDirectAssemblers);

        /**
         * Get whether to use direct assemblers.
         */
        bool isDirectAssemblers() const;

        /**
         * Set egress listener.
         */
        this_t& egressListener(std::shared_ptr<EgressListener> egressListener);

        /**
         * Get egress listener.
         */
        std::shared_ptr<EgressListener> egressListener() const;

        /**
         * Set controlled egress listener.
         */
        this_t& controlledEgressListener(std::shared_ptr<ControlledEgressListener> controlledEgressListener);

        /**
         * Get controlled egress listener.
         */
        std::shared_ptr<ControlledEgressListener> controlledEgressListener() const;

        /**
         * Set agent invoker.
         */
        template<typename AgentInvokerType>
        this_t& agentInvoker(std::shared_ptr<AgentInvokerType> agentInvoker)
        {
            m_agentInvoker = std::static_pointer_cast<void>(agentInvoker);
            return *this;
        }

        /**
         * Get agent invoker.
         */
        template<typename AgentInvokerType>
        std::shared_ptr<AgentInvokerType> agentInvoker() const
        {
            return std::static_pointer_cast<AgentInvokerType>(m_agentInvoker);
        }

        /**
         * Close the context and free applicable resources.
         */
        void close();

    private:
        std::atomic<bool> m_isConcluded{false};
        std::int64_t m_messageTimeoutNs = Configuration::messageTimeoutNs();
        std::int64_t m_newLeaderTimeoutNs = aeron::NULL_VALUE;
        std::string m_ingressEndpoints = Configuration::ingressEndpoints();
        std::string m_ingressChannel = Configuration::ingressChannel();
        std::int32_t m_ingressStreamId = Configuration::ingressStreamId();
        std::string m_egressChannel = Configuration::egressChannel();
        std::int32_t m_egressStreamId = Configuration::egressStreamId();
        std::shared_ptr<void> m_idleStrategy;
        std::type_index m_idleStrategyType = std::type_index(typeid(void));
        std::string m_aeronDirectoryName = aeron::Context::defaultAeronPath();
        std::shared_ptr<Aeron> m_aeron;
        std::shared_ptr<CredentialsSupplier> m_credentialsSupplier;
        bool m_ownsAeronClient = false;
        bool m_isIngressExclusive = true;
        exception_handler_t m_errorHandler = aeron::defaultErrorHandler;
        bool m_isDirectAssemblers = false;
        std::shared_ptr<EgressListener> m_egressListener;
        std::shared_ptr<ControlledEgressListener> m_controlledEgressListener;
        std::shared_ptr<void> m_agentInvoker;
        std::string m_clientName = Configuration::clientName();
    };

    /**
     * Allows for the async establishment of a cluster session.
     */
    class AsyncConnect
    {
    public:
        /**
         * Represents connection state.
         */
        enum class State
        {
            CREATE_EGRESS_SUBSCRIPTION = -1,
            CREATE_INGRESS_PUBLICATIONS = 0,
            AWAIT_PUBLICATION_CONNECTED = 1,
            SEND_MESSAGE = 2,
            POLL_RESPONSE = 3,
            CONCLUDE_CONNECT = 4,
            DONE = 5
        };

        /**
         * Construct async connect with context and deadline.
         */
        AsyncConnect(std::shared_ptr<Context> ctx, std::int64_t deadlineNs);

        /**
         * Close allocated resources.
         */
        void close();

        /**
         * Get current connection state.
         */
        State state() const;

        /**
         * Get step number from state.
         */
        int step() const;

        /**
         * Poll to advance steps in the connection until complete or error.
         * 
         * @return AeronCluster instance when complete, nullptr if not yet complete.
         */
        std::shared_ptr<AeronCluster> poll();

    private:
        void checkDeadline();
        void createEgressSubscription();
        void createIngressPublications();
        void awaitPublicationConnected();
        void sendMessage();
        void pollResponse();
        std::shared_ptr<AeronCluster> concludeConnect();
        void prepareConnectRequest(const std::string& responseChannel);
        void prepareChallengeResponse(const std::vector<std::uint8_t>& encodedCredentials);
        void updateMembers();

        std::shared_ptr<Context> m_ctx;
        std::int64_t m_deadlineNs;
        std::int64_t m_leaderHeartbeatTimeoutNs = aeron::NULL_VALUE;
        std::int64_t m_correlationId = aeron::NULL_VALUE;
        std::int64_t m_clusterSessionId = aeron::NULL_VALUE;
        std::int64_t m_leadershipTermId = aeron::NULL_VALUE;
        std::int32_t m_leaderMemberId = 0;
        State m_state = State::CREATE_EGRESS_SUBSCRIPTION;
        int m_messageLength = 0;

        std::shared_ptr<Image> m_egressImage;
        std::shared_ptr<Subscription> m_egressSubscription;
        std::unique_ptr<EgressPoller> m_egressPoller;
        std::int64_t m_egressRegistrationId = aeron::NULL_VALUE;
        std::unordered_map<int, std::unique_ptr<MemberIngress>> m_memberByIdMap;
        std::int64_t m_ingressRegistrationId = aeron::NULL_VALUE;
        std::shared_ptr<Publication> m_ingressPublication;

        std::vector<std::uint8_t> m_bufferData;
        AtomicBuffer m_buffer;
        MessageHeaderEncoder m_messageHeaderEncoder;
        std::function<std::int64_t()> m_nanoClock;
    };

    /**
     * Helper class for managing ingress publications for cluster members.
     */
    class MemberIngress
    {
    public:
        MemberIngress(std::shared_ptr<Context> ctx, int memberId, const std::string& endpoint);

        void asyncAddPublication();
        void asyncGetPublication();
        void close();

        int m_memberId;
        std::string m_endpoint;
        std::int64_t m_registrationId = aeron::NULL_VALUE;
        std::shared_ptr<Publication> m_publication;
        std::exception_ptr m_publicationException;

    private:
        std::shared_ptr<Context> m_ctx;
    };

    /**
     * Connect to the cluster using default configuration.
     */
    static std::shared_ptr<AeronCluster> connect();

    /**
     * Connect to the cluster providing Context for configuration.
     */
    static std::shared_ptr<AeronCluster> connect(std::shared_ptr<Context> ctx);

    /**
     * Begin an async connection attempt.
     */
    static std::unique_ptr<AsyncConnect> asyncConnect();

    /**
     * Begin an async connection attempt with context.
     */
    static std::unique_ptr<AsyncConnect> asyncConnect(std::shared_ptr<Context> ctx);

    /**
     * Get the context used to launch this cluster client.
     */
    std::shared_ptr<Context> context() const;

    /**
     * Cluster session id for the session that was opened.
     */
    std::int64_t clusterSessionId() const;

    /**
     * Leadership term identity for the cluster.
     */
    std::int64_t leadershipTermId() const;

    /**
     * Get the current leader member id for the cluster.
     */
    std::int32_t leaderMemberId() const;

    /**
     * Get the raw Publication for sending to the cluster.
     */
    std::shared_ptr<Publication> ingressPublication() const;

    /**
     * Get the raw Subscription for receiving from the cluster.
     */
    std::shared_ptr<Subscription> egressSubscription() const;

    /**
     * Try to claim a range in the publication log.
     */
    std::int64_t tryClaim(std::int32_t length, BufferClaim& bufferClaim);

    /**
     * Non-blocking publish of a buffer containing a message.
     */
    std::int64_t offer(AtomicBuffer& buffer, std::int32_t offset, std::int32_t length);

    /**
     * Send a keep alive message to the cluster.
     */
    bool sendKeepAlive();

    /**
     * Send an admin request to initiate a snapshot action.
     */
    bool sendAdminRequestToTakeASnapshot(std::int64_t correlationId);

    /**
     * Poll the egress subscription for session messages.
     */
    std::int32_t pollEgress();

    /**
     * Controlled poll the egress subscription for session messages.
     */
    std::int32_t controlledPollEgress();

    /**
     * Poll for client state changes.
     */
    std::int32_t pollStateChanges();

    /**
     * Called when a new leader event is delivered.
     */
    void onNewLeader(
        std::int64_t clusterSessionId,
        std::int64_t leadershipTermId,
        std::int32_t leaderMemberId,
        const std::string& ingressEndpoints);

    /**
     * Track ingress publication result.
     */
    void trackIngressPublicationResult(std::int64_t result);

    /**
     * Close session and release associated resources.
     */
    void close();

    /**
     * Is the client closed?
     */
    bool isClosed() const;

    /**
     * Parse ingress endpoints string into a map.
     */
    static std::unordered_map<int, std::unique_ptr<MemberIngress>> parseIngressEndpoints(
        std::shared_ptr<Context> ctx, const std::string& endpoints);

private:
    enum class State
    {
        CONNECTED,
        AWAIT_NEW_LEADER,
        AWAIT_NEW_LEADER_CONNECTION,
        PENDING_CLOSE,
        CLOSED
    };

    AeronCluster(
        std::shared_ptr<Context> ctx,
        MessageHeaderEncoder& messageHeaderEncoder,
        std::shared_ptr<Publication> publication,
        std::shared_ptr<Subscription> subscription,
        std::shared_ptr<Image> egressImage,
        std::unordered_map<int, std::unique_ptr<MemberIngress>> endpointByIdMap,
        std::int64_t clusterSessionId,
        std::int64_t leadershipTermId,
        std::int32_t leaderMemberId);

    void state(State newState, std::int64_t newStateDeadline);
    void onDisconnected();
    void closeSession();
    void invokeInvokers();
    void onFragment(AtomicBuffer& buffer, std::int32_t offset, std::int32_t length, Header& header);
    ControlledPollAction onControlledFragment(
        AtomicBuffer& buffer, std::int32_t offset, std::int32_t length, Header& header);
    void updateMemberEndpoints(const std::string& ingressEndpoints, int leaderMemberId);

    static std::shared_ptr<Publication> addIngressPublication(
        std::shared_ptr<Context> ctx, const std::string& channel, std::int32_t streamId);
    static std::int64_t asyncAddIngressPublication(
        std::shared_ptr<Context> ctx, const std::string& channel, std::int32_t streamId);
    static std::shared_ptr<Publication> getIngressPublication(
        std::shared_ptr<Context> ctx, std::int64_t registrationId);

    static constexpr int SEND_ATTEMPTS = 3;
    static constexpr int FRAGMENT_LIMIT = 10;

    std::shared_ptr<Context> m_ctx;
    std::int64_t m_clusterSessionId;
    std::int64_t m_leadershipTermId;
    std::int32_t m_leaderMemberId;
    std::shared_ptr<Subscription> m_subscription;
    State m_state;
    std::int64_t m_stateDeadline;
    std::shared_ptr<Image> m_egressImage;
    std::shared_ptr<Publication> m_publication;
    std::function<std::int64_t()> m_nanoClock;
    std::shared_ptr<void> m_idleStrategy;
    BufferClaim m_bufferClaim;
    std::vector<std::uint8_t> m_headerBufferData;
    AtomicBuffer m_headerBuffer;
    MessageHeaderEncoder m_messageHeaderEncoder;
    SessionMessageHeaderEncoder m_sessionMessageHeaderEncoder;
    MessageHeaderDecoder m_messageHeaderDecoder;
    SessionMessageHeaderDecoder m_sessionMessageHeaderDecoder;
    NewLeaderEvent m_newLeaderEventDecoder;
    SessionEvent m_sessionEventDecoder;
    AdminRequest m_adminRequestEncoder;
    AdminResponse m_adminResponseDecoder;
    FragmentAssembler m_fragmentAssembler;
    std::shared_ptr<EgressListener> m_egressListener;
    ControlledFragmentAssembler m_controlledFragmentAssembler;
    std::shared_ptr<ControlledEgressListener> m_controlledEgressListener;
    std::unordered_map<int, std::unique_ptr<MemberIngress>> m_endpointByIdMap;
};

}}}

// Implementation details
namespace aeron { namespace cluster { namespace client
{

inline std::int64_t AeronCluster::Configuration::messageTimeoutNs()
{
    const char* envValue = std::getenv(MESSAGE_TIMEOUT_PROP_NAME);
    if (envValue)
    {
        try
        {
            std::int64_t value = std::stoll(envValue);
            // Convert from milliseconds to nanoseconds if value is small (< 1000000)
            if (value < 1000000)
            {
                value *= 1000000; // Assume milliseconds
            }
            return value;
        }
        catch (...)
        {
            // Fall through to default
        }
    }
    return MESSAGE_TIMEOUT_DEFAULT_NS;
}

inline std::string AeronCluster::Configuration::ingressEndpoints()
{
    const char* envValue = std::getenv(INGRESS_ENDPOINTS_PROP_NAME);
    return envValue ? std::string(envValue) : "";
}

inline std::string AeronCluster::Configuration::ingressChannel()
{
    const char* envValue = std::getenv(INGRESS_CHANNEL_PROP_NAME);
    return envValue ? std::string(envValue) : "";
}

inline int AeronCluster::Configuration::ingressStreamId()
{
    const char* envValue = std::getenv(INGRESS_STREAM_ID_PROP_NAME);
    if (envValue)
    {
        try
        {
            return std::stoi(envValue);
        }
        catch (...)
        {
            // Fall through to default
        }
    }
    return INGRESS_STREAM_ID_DEFAULT;
}

inline std::string AeronCluster::Configuration::egressChannel()
{
    const char* envValue = std::getenv(EGRESS_CHANNEL_PROP_NAME);
    return envValue ? std::string(envValue) : "";
}

inline int AeronCluster::Configuration::egressStreamId()
{
    const char* envValue = std::getenv(EGRESS_STREAM_ID_PROP_NAME);
    if (envValue)
    {
        try
        {
            return std::stoi(envValue);
        }
        catch (...)
        {
            // Fall through to default
        }
    }
    return EGRESS_STREAM_ID_DEFAULT;
}

inline std::string AeronCluster::Configuration::clientName()
{
    const char* envValue = std::getenv(CLIENT_NAME_PROP_NAME);
    return envValue ? std::string(envValue) : "";
}

// Context implementation
inline AeronCluster::Context::Context()
{
}

inline AeronCluster::Context::~Context()
{
}

inline bool AeronCluster::Context::isConcluded() const
{
    return m_isConcluded.load();
}

inline AeronCluster::Context::this_t& AeronCluster::Context::messageTimeoutNs(std::int64_t messageTimeoutNs)
{
    m_messageTimeoutNs = messageTimeoutNs;
    return *this;
}

inline std::int64_t AeronCluster::Context::messageTimeoutNs() const
{
    return m_messageTimeoutNs;
}

inline AeronCluster::Context::this_t& AeronCluster::Context::newLeaderTimeoutNs(std::int64_t newLeaderTimeoutNs)
{
    m_newLeaderTimeoutNs = newLeaderTimeoutNs;
    return *this;
}

inline std::int64_t AeronCluster::Context::newLeaderTimeoutNs() const
{
    return m_newLeaderTimeoutNs;
}

inline AeronCluster::Context::this_t& AeronCluster::Context::ingressEndpoints(const std::string& ingressEndpoints)
{
    m_ingressEndpoints = ingressEndpoints;
    return *this;
}

inline const std::string& AeronCluster::Context::ingressEndpoints() const
{
    return m_ingressEndpoints;
}

inline AeronCluster::Context::this_t& AeronCluster::Context::ingressChannel(const std::string& ingressChannel)
{
    m_ingressChannel = ingressChannel;
    return *this;
}

inline const std::string& AeronCluster::Context::ingressChannel() const
{
    return m_ingressChannel;
}

inline AeronCluster::Context::this_t& AeronCluster::Context::ingressStreamId(std::int32_t ingressStreamId)
{
    m_ingressStreamId = ingressStreamId;
    return *this;
}

inline std::int32_t AeronCluster::Context::ingressStreamId() const
{
    return m_ingressStreamId;
}

inline AeronCluster::Context::this_t& AeronCluster::Context::egressChannel(const std::string& egressChannel)
{
    m_egressChannel = egressChannel;
    return *this;
}

inline const std::string& AeronCluster::Context::egressChannel() const
{
    return m_egressChannel;
}

inline AeronCluster::Context::this_t& AeronCluster::Context::egressStreamId(std::int32_t egressStreamId)
{
    m_egressStreamId = egressStreamId;
    return *this;
}

inline std::int32_t AeronCluster::Context::egressStreamId() const
{
    return m_egressStreamId;
}

inline AeronCluster::Context::this_t& AeronCluster::Context::clientName(const std::string& clientName)
{
    m_clientName = clientName.empty() ? "" : clientName;
    return *this;
}

inline const std::string& AeronCluster::Context::clientName() const
{
    return m_clientName;
}

inline AeronCluster::Context::this_t& AeronCluster::Context::aeronDirectoryName(const std::string& aeronDirectoryName)
{
    m_aeronDirectoryName = aeronDirectoryName;
    return *this;
}

inline const std::string& AeronCluster::Context::aeronDirectoryName() const
{
    return m_aeronDirectoryName;
}

inline AeronCluster::Context::this_t& AeronCluster::Context::aeron(std::shared_ptr<Aeron> aeron)
{
    m_aeron = aeron;
    return *this;
}

inline std::shared_ptr<Aeron> AeronCluster::Context::aeron() const
{
    return m_aeron;
}

inline AeronCluster::Context::this_t& AeronCluster::Context::ownsAeronClient(bool ownsAeronClient)
{
    m_ownsAeronClient = ownsAeronClient;
    return *this;
}

inline bool AeronCluster::Context::ownsAeronClient() const
{
    return m_ownsAeronClient;
}

inline AeronCluster::Context::this_t& AeronCluster::Context::isIngressExclusive(bool isIngressExclusive)
{
    m_isIngressExclusive = isIngressExclusive;
    return *this;
}

inline bool AeronCluster::Context::isIngressExclusive() const
{
    return m_isIngressExclusive;
}

inline AeronCluster::Context::this_t& AeronCluster::Context::credentialsSupplier(
    std::shared_ptr<CredentialsSupplier> credentialsSupplier)
{
    m_credentialsSupplier = credentialsSupplier;
    return *this;
}

inline std::shared_ptr<CredentialsSupplier> AeronCluster::Context::credentialsSupplier() const
{
    return m_credentialsSupplier;
}

inline AeronCluster::Context::this_t& AeronCluster::Context::errorHandler(const exception_handler_t& errorHandler)
{
    m_errorHandler = errorHandler;
    return *this;
}

inline const exception_handler_t& AeronCluster::Context::errorHandler() const
{
    return m_errorHandler;
}

inline AeronCluster::Context::this_t& AeronCluster::Context::isDirectAssemblers(bool isDirectAssemblers)
{
    m_isDirectAssemblers = isDirectAssemblers;
    return *this;
}

inline bool AeronCluster::Context::isDirectAssemblers() const
{
    return m_isDirectAssemblers;
}

inline AeronCluster::Context::this_t& AeronCluster::Context::egressListener(
    std::shared_ptr<EgressListener> egressListener)
{
    m_egressListener = egressListener;
    return *this;
}

inline std::shared_ptr<EgressListener> AeronCluster::Context::egressListener() const
{
    return m_egressListener;
}

inline AeronCluster::Context::this_t& AeronCluster::Context::controlledEgressListener(
    std::shared_ptr<ControlledEgressListener> controlledEgressListener)
{
    m_controlledEgressListener = controlledEgressListener;
    return *this;
}

inline std::shared_ptr<ControlledEgressListener> AeronCluster::Context::controlledEgressListener() const
{
    return m_controlledEgressListener;
}

void AeronCluster::Context::conclude()
{
    bool expected = false;
    if (!m_isConcluded.compare_exchange_strong(expected, true))
    {
        throw ConcurrentConcludeException("Context already concluded", SOURCEINFO);
    }

    if (m_ingressChannel.empty())
    {
        throw ConfigurationException("ingressChannel must be specified", SOURCEINFO);
    }

    if (aeron::util::startsWith(m_ingressChannel, 0, aeron::IPC_CHANNEL))
    {
        if (!m_ingressEndpoints.empty())
        {
            throw ConfigurationException(
                "AeronCluster.Context ingressEndpoints must be empty when using IPC ingress", SOURCEINFO);
        }
    }

    if (m_egressChannel.empty())
    {
        throw ConfigurationException("egressChannel must be specified", SOURCEINFO);
    }

    std::shared_ptr<ChannelUri> egressChannelUri = ChannelUri::parse(m_egressChannel);
    if (egressChannelUri->media() == aeron::UDP_MEDIA)
    {
        egressChannelUri->put(aeron::REJOIN_PARAM_NAME, "false");
        m_egressChannel = egressChannelUri->toString();
    }

    static constexpr int MAX_CLIENT_NAME_LENGTH = 100;
    if (m_clientName.length() > MAX_CLIENT_NAME_LENGTH)
    {
        throw ConfigurationException(
            "AeronCluster.Context.clientName length must be <= " + std::to_string(MAX_CLIENT_NAME_LENGTH), SOURCEINFO);
    }

    if (!m_aeron)
    {
        aeron::Context aeronContext;
        aeronContext.aeronDir(m_aeronDirectoryName);
        aeronContext.errorHandler(m_errorHandler);
        aeronContext.clientName(m_clientName.empty() ? "cluster-client" : m_clientName);
        m_aeron = Aeron::connect(aeronContext);
        m_ownsAeronClient = true;
    }

    if (!m_idleStrategy)
    {
        m_idleStrategy = std::make_shared<concurrent::BackoffIdleStrategy>(
            1, 10,
            std::chrono::duration<long, std::nano>(1000),
            std::chrono::duration<long, std::milli>(1));
        m_idleStrategyType = std::type_index(typeid(concurrent::BackoffIdleStrategy));
    }

    if (!m_credentialsSupplier)
    {
        // Create a default null credentials supplier
        m_credentialsSupplier = std::make_shared<CredentialsSupplier>();
    }

    if (!m_egressListener)
    {
        throw ConfigurationException(
            "egressListener must be specified on AeronCluster.Context", SOURCEINFO);
    }

    if (!m_controlledEgressListener)
    {
        throw ConfigurationException(
            "controlledEgressListener must be specified on AeronCluster.Context", SOURCEINFO);
    }
}

inline void AeronCluster::Context::close()
{
    if (m_ownsAeronClient && m_aeron)
    {
        m_aeron.reset();
    }
}

// MemberIngress implementation
inline AeronCluster::MemberIngress::MemberIngress(
    std::shared_ptr<Context> ctx, int memberId, const std::string& endpoint) :
    m_memberId(memberId),
    m_endpoint(endpoint),
    m_ctx(ctx)
{
}

inline void AeronCluster::MemberIngress::asyncAddPublication()
{
    std::shared_ptr<ChannelUri> channelUri = ChannelUri::parse(m_ctx->ingressChannel());
    if (channelUri->media() == aeron::UDP_MEDIA)
    {
        channelUri->put(aeron::ENDPOINT_PARAM_NAME, m_endpoint);
    }

    m_registrationId = AeronCluster::asyncAddIngressPublication(
        m_ctx, channelUri->toString(), m_ctx->ingressStreamId());
    m_publication = nullptr;
}

inline void AeronCluster::MemberIngress::asyncGetPublication()
{
    try
    {
        m_publication = AeronCluster::getIngressPublication(m_ctx, m_registrationId);
        if (m_publication)
        {
            m_registrationId = aeron::NULL_VALUE;
        }
    }
    catch (...)
    {
        m_publicationException = std::current_exception();
        m_registrationId = aeron::NULL_VALUE;
    }
}

inline void AeronCluster::MemberIngress::close()
{
    if (m_publication)
    {
        m_publication.reset();
    }
    // Note: asyncRemovePublication not available in C++ API
    // The resources will be cleaned up when Aeron client is closed
    m_registrationId = aeron::NULL_VALUE;
    m_publication = nullptr;
}

// AsyncConnect implementation
inline AeronCluster::AsyncConnect::AsyncConnect(std::shared_ptr<Context> ctx, std::int64_t deadlineNs) :
    m_ctx(ctx),
    m_deadlineNs(deadlineNs),
    m_bufferData(4096),
    m_buffer(m_bufferData.data(), m_bufferData.size())
{
    m_memberByIdMap = AeronCluster::parseIngressEndpoints(ctx, ctx->ingressEndpoints());
    m_nanoClock = []() -> std::int64_t {
        return aeron::systemNanoClock();
    };
}

inline void AeronCluster::AsyncConnect::close()
{
    if (m_state != State::DONE)
    {
        if (m_ingressPublication)
        {
            m_ingressPublication->close();
        }
        // Note: asyncRemovePublication/asyncRemoveSubscription not available in C++ API
        // The resources will be cleaned up when Aeron client is closed

        if (m_egressSubscription)
        {
            m_egressSubscription.reset();
        }

        for (auto& pair : m_memberByIdMap)
        {
            if (pair.second)
            {
                pair.second->close();
            }
        }
        m_ctx->close();
    }
}

inline AeronCluster::AsyncConnect::State AeronCluster::AsyncConnect::state() const
{
    return m_state;
}

inline int AeronCluster::AsyncConnect::step() const
{
    return static_cast<int>(m_state);
}

inline std::shared_ptr<AeronCluster> AeronCluster::AsyncConnect::poll()
{
    checkDeadline();

    switch (m_state)
    {
        case State::CREATE_EGRESS_SUBSCRIPTION:
            createEgressSubscription();
            break;

        case State::CREATE_INGRESS_PUBLICATIONS:
            createIngressPublications();
            break;

        case State::AWAIT_PUBLICATION_CONNECTED:
            awaitPublicationConnected();
            break;

        case State::SEND_MESSAGE:
            sendMessage();
            break;

        case State::POLL_RESPONSE:
            pollResponse();
            break;

        case State::CONCLUDE_CONNECT:
            return concludeConnect();

        default:
            break;
    }

    return nullptr;
}

inline void AeronCluster::AsyncConnect::checkDeadline()
{
    if (m_deadlineNs - m_nanoClock() < 0)
    {
        const bool isConnected = m_egressSubscription && m_egressSubscription->isConnected();
        const std::string egressChannel = m_egressSubscription ?
            m_egressSubscription->tryResolveChannelEndpointPort() : "<unknown>";
        
        std::string errorMessage = "cluster connect timeout: state=" + std::to_string(static_cast<int>(m_state)) +
                                   " messageTimeout=" + std::to_string(m_ctx->messageTimeoutNs()) + "ns" +
                                   " ingressChannel=" + m_ctx->ingressChannel() +
                                   " ingressEndpoints=" + m_ctx->ingressEndpoints() +
                                   " ingressPublication=" + (m_ingressPublication ? m_ingressPublication->channel() : "null") +
                                   " egress.isConnected=" + (isConnected ? "true" : "false") +
                                   " responseChannel=" + egressChannel;

        TimeoutException ex(errorMessage, SOURCEINFO);

        for (auto const& pair : m_memberByIdMap)
        {
            if (pair.second->m_publicationException)
            {
                // Note: C++ doesn't have addSuppressed like Java, but we've captured the exception
            }
        }

        throw ex;
    }
}

inline void AeronCluster::AsyncConnect::createEgressSubscription()
{
    if (aeron::NULL_VALUE == m_egressRegistrationId)
    {
        m_egressRegistrationId = m_ctx->aeron()->addSubscription(m_ctx->egressChannel(), m_ctx->egressStreamId());
    }

    m_egressSubscription = m_ctx->aeron()->findSubscription(m_egressRegistrationId);
    if (m_egressSubscription)
    {
        m_egressPoller = std::make_unique<EgressPoller>(*m_egressSubscription, FRAGMENT_LIMIT);
        m_egressRegistrationId = aeron::NULL_VALUE;
        m_state = State::CREATE_INGRESS_PUBLICATIONS;
    }
}

inline void AeronCluster::AsyncConnect::createIngressPublications()
{
    if (m_ctx->ingressEndpoints().empty())
    {
        if (!m_ingressPublication)
        {
            if (aeron::NULL_VALUE == m_ingressRegistrationId)
            {
                m_ingressRegistrationId =
                    AeronCluster::asyncAddIngressPublication(m_ctx, m_ctx->ingressChannel(), m_ctx->ingressStreamId());
            }

            try
            {
                m_ingressPublication = AeronCluster::getIngressPublication(m_ctx, m_ingressRegistrationId);
            }
            catch (...)
            {
                m_ingressRegistrationId = aeron::NULL_VALUE;
                throw;
            }
        }
        else
        {
            m_ingressRegistrationId = aeron::NULL_VALUE;
            m_state = State::AWAIT_PUBLICATION_CONNECTED;
        }
    }
    else
    {
        int count = 0;
        for (auto const& pair : m_memberByIdMap)
        {
            if (pair.second->m_publication || pair.second->m_publicationException)
            {
                count++;
            }
            else
            {
                if (aeron::NULL_VALUE == pair.second->m_registrationId)
                {
                    pair.second->asyncAddPublication();
                }
                pair.second->asyncGetPublication();
            }
        }

        if (m_memberByIdMap.size() == static_cast<size_t>(count))
        {
            m_state = State::AWAIT_PUBLICATION_CONNECTED;
        }
    }
}

inline void AeronCluster::AsyncConnect::awaitPublicationConnected()
{
    const std::string responseChannel = m_egressSubscription->tryResolveChannelEndpointPort();
    if (!responseChannel.empty())
    {
        if (!m_ingressPublication)
        {
            for (auto const& pair : m_memberByIdMap)
            {
                if (!pair.second->m_publication && aeron::NULL_VALUE != pair.second->m_registrationId)
                {
                    pair.second->asyncGetPublication();
                }

                if (pair.second->m_publication && pair.second->m_publication->isConnected())
                {
                    m_ingressPublication = pair.second->m_publication;
                    prepareConnectRequest(responseChannel);
                    break;
                }
            }
        }
        else if (m_ingressPublication->isConnected())
        {
            prepareConnectRequest(responseChannel);
        }
    }
}

inline void AeronCluster::AsyncConnect::prepareConnectRequest(const std::string& responseChannel)
{
    m_correlationId = m_ctx->aeron()->nextCorrelationId();
    auto credentialsPair = m_ctx->credentialsSupplier()->encodedCredentials();
    std::vector<std::uint8_t> encodedCredentials;
    if (credentialsPair.first && credentialsPair.second > 0)
    {
        encodedCredentials.assign(credentialsPair.first, credentialsPair.first + credentialsPair.second);
    }

    // Format version info: "version=<version> commit=<commit>"
    // For C++ version, we use a placeholder since version info is typically generated at build time
    std::string clientInfo = "name=" + m_ctx->clientName() + " version=1.50.0 commit=unknown";

    SessionConnectRequest sessionConnectRequestEncoder;
    sessionConnectRequestEncoder
        .wrapAndApplyHeader(reinterpret_cast<char *>(m_buffer.buffer()), 0, m_buffer.capacity())
        .correlationId(m_correlationId)
        .responseStreamId(m_ctx->egressStreamId())
        .version(Configuration::PROTOCOL_SEMANTIC_VERSION)
        .putResponseChannel(responseChannel);
    
    if (!encodedCredentials.empty())
    {
        sessionConnectRequestEncoder.putEncodedCredentials(reinterpret_cast<const char *>(encodedCredentials.data()), static_cast<std::uint32_t>(encodedCredentials.size()));
    }
    if (!clientInfo.empty())
    {
        sessionConnectRequestEncoder.putClientInfo(clientInfo);
    }

    m_messageLength = MessageHeaderEncoder::encodedLength() + sessionConnectRequestEncoder.encodedLength();
    m_state = State::SEND_MESSAGE;
}

inline void AeronCluster::AsyncConnect::sendMessage()
{
    const std::int64_t position = m_ingressPublication->offer(m_buffer, 0, m_messageLength);
    if (position > 0)
    {
        m_state = State::POLL_RESPONSE;
    }
    else if (aeron::PUBLICATION_CLOSED == position || aeron::NOT_CONNECTED == position)
    {
        throw ClusterException("unexpected loss of connection to cluster", SOURCEINFO);
    }
}

inline void AeronCluster::AsyncConnect::pollResponse()
{
    if (m_egressPoller->poll() > 0 &&
        m_egressPoller->isPollComplete() &&
        m_egressPoller->correlationId() == m_correlationId)
    {
        if (m_egressPoller->isChallenged())
        {
            m_correlationId = aeron::NULL_VALUE;
            m_clusterSessionId = m_egressPoller->clusterSessionId();
            const auto& challenge = m_egressPoller->encodedChallenge();
            auto responsePair = m_ctx->credentialsSupplier()->onChallenge(
                challenge.empty() ? nullptr : challenge.data(), 
                static_cast<std::uint32_t>(challenge.size()));
            std::vector<std::uint8_t> responseData;
            if (responsePair.first && responsePair.second > 0)
            {
                responseData.assign(responsePair.first, responsePair.first + responsePair.second);
            }
            prepareChallengeResponse(responseData);
            return;
        }

        switch (m_egressPoller->eventCode())
        {
            case codecs::EventCode::OK:
            {
                m_leadershipTermId = m_egressPoller->leadershipTermId();
                m_leaderMemberId = m_egressPoller->leaderMemberId();
                m_clusterSessionId = m_egressPoller->clusterSessionId();
                m_leaderHeartbeatTimeoutNs = m_egressPoller->leaderHeartbeatTimeoutNs();
                // Note: egressImage() returns Image*, but Image is managed by Subscription
                // Get the Image from the subscription using the sessionId to get proper shared_ptr ownership
                Image* rawImage = m_egressPoller->egressImage();
                if (rawImage != nullptr && m_egressSubscription != nullptr)
                {
                    m_egressImage = m_egressSubscription->imageBySessionId(rawImage->sessionId());
                }
                else
                {
                    m_egressImage = nullptr;
                }
                m_state = State::CONCLUDE_CONNECT;
                break;
            }

            case codecs::EventCode::ERROR:
            {
                throw ClusterException(m_egressPoller->detail(), SOURCEINFO);
            }

            case codecs::EventCode::REDIRECT:
            {
                updateMembers();
                break;
            }

            case codecs::EventCode::AUTHENTICATION_REJECTED:
            {
                throw AuthenticationException(m_egressPoller->detail(), SOURCEINFO);
            }

            case codecs::EventCode::CLOSED:
            case codecs::EventCode::NULL_VALUE:
            {
                break;
            }
        }
    }
}

inline void AeronCluster::AsyncConnect::prepareChallengeResponse(const std::vector<std::uint8_t>& encodedCredentials)
{
    m_correlationId = m_ctx->aeron()->nextCorrelationId();

    ChallengeResponse challengeResponseEncoder;
    challengeResponseEncoder
        .wrapAndApplyHeader(reinterpret_cast<char *>(m_buffer.buffer()), 0, m_buffer.capacity())
        .correlationId(m_correlationId)
        .clusterSessionId(m_clusterSessionId);
    
    if (!encodedCredentials.empty())
    {
        challengeResponseEncoder.putEncodedCredentials(reinterpret_cast<const char *>(encodedCredentials.data()), static_cast<std::uint32_t>(encodedCredentials.size()));
    }

    m_messageLength = MessageHeaderEncoder::encodedLength() + challengeResponseEncoder.encodedLength();

    m_state = State::SEND_MESSAGE;
}

inline void AeronCluster::AsyncConnect::updateMembers()
{
    m_leaderMemberId = m_egressPoller->leaderMemberId();
    auto oldLeaderIt = m_memberByIdMap.find(m_leaderMemberId);
    std::unique_ptr<MemberIngress> oldLeader = nullptr;
    if (oldLeaderIt != m_memberByIdMap.end())
    {
        oldLeader = std::move(oldLeaderIt->second);
        m_memberByIdMap.erase(oldLeaderIt);
    }

    if (m_ingressPublication)
    {
        m_ingressPublication->close();
        m_ingressPublication = nullptr;
    }
    
    for (auto const& pair : m_memberByIdMap)
    {
        pair.second->close();
    }
    m_memberByIdMap.clear();

    m_memberByIdMap = AeronCluster::parseIngressEndpoints(m_ctx, m_egressPoller->detail());

    auto newLeaderIt = m_memberByIdMap.find(m_leaderMemberId);
    MemberIngress* newLeader = (newLeaderIt != m_memberByIdMap.end()) ? newLeaderIt->second.get() : nullptr;

    if (oldLeader &&
        !oldLeader->m_publicationException &&
        newLeader &&
        newLeader->m_endpoint == oldLeader->m_endpoint)
    {
        m_ingressPublication = newLeader->m_publication = oldLeader->m_publication;
        newLeader->m_registrationId = oldLeader->m_registrationId;
    }
    else if (newLeader)
    {
        if (oldLeader)
        {
            oldLeader->close();
        }
        newLeader->asyncAddPublication();
    }

    m_state = State::AWAIT_PUBLICATION_CONNECTED;
}

inline std::shared_ptr<AeronCluster> AeronCluster::AsyncConnect::concludeConnect()
{
    if (m_ctx->newLeaderTimeoutNs() == aeron::NULL_VALUE)
    {
        // Default leader heartbeat timeout is 10 seconds (10 * 1000 * 1000 * 1000 nanoseconds)
        static constexpr std::int64_t LEADER_HEARTBEAT_TIMEOUT_DEFAULT_NS = 10LL * 1000 * 1000 * 1000;
        m_ctx->newLeaderTimeoutNs(2 * (m_leaderHeartbeatTimeoutNs != aeron::NULL_VALUE ?
            m_leaderHeartbeatTimeoutNs : LEADER_HEARTBEAT_TIMEOUT_DEFAULT_NS));
    }

    std::shared_ptr<AeronCluster> aeronCluster = std::shared_ptr<AeronCluster>(new AeronCluster(
        m_ctx,
        m_messageHeaderEncoder,
        m_ingressPublication,
        m_egressSubscription,
        m_egressImage,
        std::move(m_memberByIdMap),
        m_clusterSessionId,
        m_leadershipTermId,
        m_leaderMemberId));

    m_ingressPublication = nullptr;
    m_memberByIdMap.clear();

    m_state = State::DONE;

    return aeronCluster;
}

// Static helper methods for AeronCluster
inline std::unordered_map<int, std::unique_ptr<AeronCluster::MemberIngress>>
AeronCluster::parseIngressEndpoints(std::shared_ptr<Context> ctx, const std::string& endpoints)
{
    std::unordered_map<int, std::unique_ptr<MemberIngress>> endpointByIdMap;

    if (!endpoints.empty())
    {
        std::stringstream ss(endpoints);
        std::string endpoint;
        while (std::getline(ss, endpoint, ','))
        {
            std::size_t pos = endpoint.find('=');
            if (pos == std::string::npos)
            {
                throw ConfigurationException(
                    "endpoint missing '=' separator: " + endpoints, SOURCEINFO);
            }

            int memberId = aeron::util::parse<int>(endpoint.substr(0, pos));
            std::string endpointValue = endpoint.substr(pos + 1);
            endpointByIdMap[memberId] = std::make_unique<MemberIngress>(ctx, memberId, endpointValue);
        }
    }

    return endpointByIdMap;
}

inline std::shared_ptr<Publication> AeronCluster::addIngressPublication(
    std::shared_ptr<Context> ctx, const std::string& channel, std::int32_t streamId)
{
    if (ctx->isIngressExclusive())
    {
        // Note: ExclusivePublication doesn't inherit from Publication in C++
        // For now, we'll return nullptr and handle it differently
        // TODO: Use variant or change storage approach
        return nullptr;  // Will be stored separately
    }
    else
    {
        // Java version is synchronous, so we need to wait for the publication
        std::int64_t registrationId = ctx->aeron()->addPublication(channel, streamId);
        std::shared_ptr<Publication> publication;
        while (!publication)
        {
            try
            {
                publication = ctx->aeron()->findPublication(registrationId);
            }
            catch (const IllegalArgumentException&)
            {
                // Registration ID not found yet, continue waiting
            }
            if (!publication)
            {
                std::this_thread::yield();  // Yield to allow other threads to run
            }
        }
        return publication;
    }
}

inline std::int64_t AeronCluster::asyncAddIngressPublication(
    std::shared_ptr<Context> ctx, const std::string& channel, std::int32_t streamId)
{
    if (ctx->isIngressExclusive())
    {
        AsyncAddExclusivePublication* async = ctx->aeron()->addExclusivePublicationAsync(channel, streamId);
        return ctx->aeron()->addExclusivePublicationAsyncGetRegistrationId(async);
    }
    else
    {
        AsyncAddPublication* async = ctx->aeron()->addPublicationAsync(channel, streamId);
        return ctx->aeron()->addPublicationAsyncGetRegistrationId(async);
    }
}

inline std::shared_ptr<Publication> AeronCluster::getIngressPublication(
    std::shared_ptr<Context> ctx, std::int64_t registrationId)
{
    if (ctx->isIngressExclusive())
    {
        // Note: ExclusivePublication doesn't inherit from Publication in C++
        // For now, we'll return nullptr and handle it differently
        // TODO: Use variant or change storage approach
        return nullptr;  // Will be stored separately
    }
    else
    {
        return ctx->aeron()->findPublication(registrationId);
    }
}

// AeronCluster constructor
inline AeronCluster::AeronCluster(
    std::shared_ptr<Context> ctx,
    MessageHeaderEncoder& messageHeaderEncoder,
    std::shared_ptr<Publication> publication,
    std::shared_ptr<Subscription> subscription,
    std::shared_ptr<Image> egressImage,
    std::unordered_map<int, std::unique_ptr<MemberIngress>> endpointByIdMap,
    std::int64_t clusterSessionId,
    std::int64_t leadershipTermId,
    std::int32_t leaderMemberId) :
    m_ctx(ctx),
    m_clusterSessionId(clusterSessionId),
    m_leadershipTermId(leadershipTermId),
    m_leaderMemberId(leaderMemberId),
    m_subscription(subscription),
    m_state(State::CONNECTED),
    m_stateDeadline(0),
    m_egressImage(egressImage),
    m_publication(publication),
    m_nanoClock([]() -> std::int64_t { 
        // Use system nano clock
        return aeron::systemNanoClock(); 
    }),
    m_idleStrategy(ctx->m_idleStrategy),
    m_headerBufferData(SESSION_HEADER_LENGTH),
    m_headerBuffer(m_headerBufferData.data(), SESSION_HEADER_LENGTH),
    m_messageHeaderEncoder(messageHeaderEncoder),
    m_fragmentAssembler([this](AtomicBuffer &buffer, util::index_t offset, util::index_t length, Header &header) {
        this->onFragment(buffer, offset, length, header);
    }),
    m_egressListener(ctx->egressListener()),
    m_controlledFragmentAssembler([this](AtomicBuffer &buffer, util::index_t offset, util::index_t length, Header &header) {
        return this->onControlledFragment(buffer, offset, length, header);
    }),
    m_controlledEgressListener(ctx->controlledEgressListener()),
    m_endpointByIdMap(std::move(endpointByIdMap))
{
    m_sessionMessageHeaderEncoder
        .wrapAndApplyHeader(reinterpret_cast<char *>(m_headerBuffer.buffer()), 0, m_headerBuffer.capacity())
        .clusterSessionId(clusterSessionId)
        .leadershipTermId(leadershipTermId);
}

// AeronCluster public methods
inline std::shared_ptr<AeronCluster::Context> AeronCluster::context() const
{
    return m_ctx;
}

inline std::int64_t AeronCluster::clusterSessionId() const
{
    return m_clusterSessionId;
}

inline std::int64_t AeronCluster::leadershipTermId() const
{
    return m_leadershipTermId;
}

inline std::int32_t AeronCluster::leaderMemberId() const
{
    return m_leaderMemberId;
}

inline std::shared_ptr<Publication> AeronCluster::ingressPublication() const
{
    return m_publication;
}

inline std::shared_ptr<Subscription> AeronCluster::egressSubscription() const
{
    return m_subscription;
}

inline std::int64_t AeronCluster::tryClaim(std::int32_t length, BufferClaim& bufferClaim)
{
    const std::int64_t position = m_publication->tryClaim(length + SESSION_HEADER_LENGTH, bufferClaim);
    if (position > 0)
    {
        bufferClaim.buffer().putBytes(bufferClaim.offset(), m_headerBuffer, 0, SESSION_HEADER_LENGTH);
    }

    trackIngressPublicationResult(position);

    return position;
}

inline std::int64_t AeronCluster::offer(AtomicBuffer& buffer, std::int32_t offset, std::int32_t length)
{
    // Use tryClaim to write header + message
    BufferClaim headerClaim;
    const std::int64_t position = m_publication->tryClaim(SESSION_HEADER_LENGTH + length, headerClaim);
    if (position > 0)
    {
        // Write header
        headerClaim.buffer().putBytes(headerClaim.offset(), m_headerBuffer, 0, SESSION_HEADER_LENGTH);
        // Write message
        headerClaim.buffer().putBytes(headerClaim.offset() + SESSION_HEADER_LENGTH, buffer, offset, length);
        headerClaim.commit();
    }
    
    trackIngressPublicationResult(position);

    return position;
}

inline bool AeronCluster::sendKeepAlive()
{
    const std::int32_t length = MessageHeaderEncoder::encodedLength() + SessionKeepAlive::SBE_BLOCK_LENGTH;

    std::int64_t result = m_publication->tryClaim(length, m_bufferClaim);
    if (result > 0)
    {
        SessionKeepAlive sessionKeepAliveEncoder;
        sessionKeepAliveEncoder
            .wrapAndApplyHeader(reinterpret_cast<char *>(m_bufferClaim.buffer().buffer()), m_bufferClaim.offset(), m_bufferClaim.buffer().capacity())
            .leadershipTermId(m_leadershipTermId)
            .clusterSessionId(m_clusterSessionId);

        m_bufferClaim.commit();
        return true;
    }

    trackIngressPublicationResult(result);

    return false;
}

inline bool AeronCluster::sendAdminRequestToTakeASnapshot(std::int64_t correlationId)
{
    const std::int32_t length = MessageHeaderEncoder::encodedLength() +
        AdminRequest::SBE_BLOCK_LENGTH +
        AdminRequest::payloadHeaderLength();

    std::int64_t result = m_publication->tryClaim(length, m_bufferClaim);
    if (result > 0)
    {
        m_adminRequestEncoder
            .wrapAndApplyHeader(reinterpret_cast<char *>(m_bufferClaim.buffer().buffer()), m_bufferClaim.offset(), m_bufferClaim.buffer().capacity())
            .leadershipTermId(m_leadershipTermId)
            .clusterSessionId(m_clusterSessionId)
            .correlationId(correlationId)
            .requestType(codecs::AdminRequestType::SNAPSHOT);

        m_adminRequestEncoder.putPayload(nullptr, 0);

        m_bufferClaim.commit();
        return true;
    }

    trackIngressPublicationResult(result);

    return false;
}

inline std::int32_t AeronCluster::pollEgress()
{
    int workCount = m_subscription->poll(m_fragmentAssembler.handler(), FRAGMENT_LIMIT);

    if (m_egressImage && m_egressImage->isClosed() && 
        (State::CONNECTED == m_state || State::AWAIT_NEW_LEADER_CONNECTION == m_state))
    {
        onDisconnected();
        workCount++;
    }

    workCount += pollStateChanges();

    return workCount;
}

inline std::int32_t AeronCluster::controlledPollEgress()
{
    int workCount = m_subscription->controlledPoll(m_controlledFragmentAssembler.handler(), FRAGMENT_LIMIT);

    if (m_egressImage && m_egressImage->isClosed() &&
        (State::CONNECTED == m_state || State::AWAIT_NEW_LEADER_CONNECTION == m_state))
    {
        onDisconnected();
        workCount++;
    }

    workCount += pollStateChanges();

    return workCount;
}

inline std::int32_t AeronCluster::pollStateChanges()
{
    if (State::PENDING_CLOSE == m_state ||
        ((State::AWAIT_NEW_LEADER == m_state || State::AWAIT_NEW_LEADER_CONNECTION == m_state) &&
        0 <= m_nanoClock() - m_stateDeadline))
    {
        close();
        return 1;
    }

    return 0;
}

inline void AeronCluster::trackIngressPublicationResult(std::int64_t result)
{
    if (State::CONNECTED == m_state)
    {
        if (aeron::NOT_CONNECTED == result || aeron::PUBLICATION_CLOSED == result)
        {
            onDisconnected();
        }
        else if (aeron::MAX_POSITION_EXCEEDED == result)
        {
            m_publication.reset();
            state(State::PENDING_CLOSE, 0);
        }
    }
    else if (State::AWAIT_NEW_LEADER_CONNECTION == m_state)
    {
        if (result > 0)
        {
            state(State::CONNECTED, 0);
        }
    }
}

inline void AeronCluster::close()
{
    if (State::CLOSED == m_state)
    {
        return;
    }

    if (m_publication && m_publication->isConnected() && State::CONNECTED == m_state)
    {
        closeSession();
    }

    if (!m_ctx->ownsAeronClient())
    {
        const exception_handler_t& errorHandler = m_ctx->errorHandler();
        if (m_subscription)
        {
            try { m_subscription.reset(); } catch (const std::exception& ex) { errorHandler(ex); } catch (...) { }
        }
        if (m_publication)
        {
            try { m_publication.reset(); } catch (const std::exception& ex) { errorHandler(ex); } catch (...) { }
        }
    }

    state(State::CLOSED, 0);
    m_ctx->close();
}

inline bool AeronCluster::isClosed() const
{
    return State::CLOSED == m_state;
}

inline void AeronCluster::onNewLeader(
    std::int64_t clusterSessionId,
    std::int64_t leadershipTermId,
    std::int32_t leaderMemberId,
    const std::string& ingressEndpoints)
{
    if (clusterSessionId != m_clusterSessionId)
    {
        throw ClusterException(
            "invalid clusterSessionId=" + std::to_string(clusterSessionId) + 
            " expected=" + std::to_string(m_clusterSessionId), SOURCEINFO);
    }

    state(State::AWAIT_NEW_LEADER_CONNECTION, m_nanoClock() + m_ctx->messageTimeoutNs());

    m_leadershipTermId = leadershipTermId;
    m_leaderMemberId = leaderMemberId;
    m_sessionMessageHeaderEncoder.leadershipTermId(leadershipTermId);

    if (m_publication)
    {
        m_publication.reset();
    }

    if (m_ctx->ingressEndpoints().empty())
    {
        m_publication = addIngressPublication(m_ctx, m_ctx->ingressChannel(), m_ctx->ingressStreamId());
    }
    else
    {
        m_ctx->ingressEndpoints(ingressEndpoints);
        updateMemberEndpoints(ingressEndpoints, leaderMemberId);
    }

    // Clear fragment assemblers
    // Note: FragmentAssembler/ControlledFragmentAssembler don't have clear() in C++ version
    // They automatically reset on new fragments

    m_egressListener->onNewLeader(clusterSessionId, leadershipTermId, leaderMemberId, ingressEndpoints);
    m_controlledEgressListener->onNewLeader(clusterSessionId, leadershipTermId, leaderMemberId, ingressEndpoints);
}

// Private helper methods
inline void AeronCluster::state(State newState, std::int64_t newStateDeadline)
{
    m_state = newState;
    m_stateDeadline = newStateDeadline;
}

inline void AeronCluster::onDisconnected()
{
    if (m_publication)
    {
        m_publication.reset();
    }
    state(State::AWAIT_NEW_LEADER, m_nanoClock() + m_ctx->newLeaderTimeoutNs());
}

inline void AeronCluster::closeSession()
{
    const std::int32_t length = MessageHeaderEncoder::encodedLength() + SessionCloseRequest::SBE_BLOCK_LENGTH;
    
    for (int i = 0; i < SEND_ATTEMPTS; i++)
    {
        std::int64_t result = m_publication->tryClaim(length, m_bufferClaim);
        if (result > 0)
        {
            SessionCloseRequest sessionCloseRequestEncoder;
            sessionCloseRequestEncoder
                .wrapAndApplyHeader(reinterpret_cast<char *>(m_bufferClaim.buffer().buffer()), m_bufferClaim.offset(), m_bufferClaim.buffer().capacity())
                .leadershipTermId(m_leadershipTermId)
                .clusterSessionId(m_clusterSessionId);

            m_bufferClaim.commit();
            break;
        }
    }
}

inline void AeronCluster::updateMemberEndpoints(const std::string& ingressEndpoints, int leaderMemberId)
{
    for (auto& pair : m_endpointByIdMap)
    {
        pair.second->close();
    }

    auto map = parseIngressEndpoints(m_ctx, ingressEndpoints);
    auto newLeaderIt = map.find(leaderMemberId);
    if (newLeaderIt != map.end())
    {
        MemberIngress* newLeader = newLeaderIt->second.get();
        std::shared_ptr<ChannelUri> channelUri = ChannelUri::parse(m_ctx->ingressChannel());
        if (channelUri->media() == aeron::UDP_MEDIA)
        {
            channelUri->put(aeron::ENDPOINT_PARAM_NAME, newLeader->m_endpoint);
        }
        m_publication = newLeader->m_publication = addIngressPublication(m_ctx, channelUri->toString(), m_ctx->ingressStreamId());
    }
    m_endpointByIdMap = std::move(map);
}

inline void AeronCluster::onFragment(AtomicBuffer& buffer, std::int32_t offset, std::int32_t length, Header& header)
{
    m_messageHeaderDecoder.wrap(
        reinterpret_cast<char *>(buffer.buffer()),
        offset,
        MessageHeader::sbeSchemaVersion(),
        buffer.capacity());

    const std::int32_t schemaId = m_messageHeaderDecoder.schemaId();
    const std::int32_t templateId = m_messageHeaderDecoder.templateId();

    if (schemaId != MessageHeader::sbeSchemaId())
    {
        throw ClusterException(
            "expected cluster egress schemaId=" + std::to_string(MessageHeader::sbeSchemaId()) + 
            " actual=" + std::to_string(schemaId), SOURCEINFO);
    }

    switch (templateId)
    {
        case SessionMessageHeader::SBE_TEMPLATE_ID:
        {
            m_sessionMessageHeaderDecoder.wrapForDecode(
                reinterpret_cast<char *>(buffer.buffer()),
                offset + MessageHeader::encodedLength(),
                m_messageHeaderDecoder.blockLength(),
                m_messageHeaderDecoder.version(),
                buffer.capacity());

            const std::int64_t sessionId = m_sessionMessageHeaderDecoder.clusterSessionId();
            if (sessionId == m_clusterSessionId)
            {
                m_egressListener->onMessage(
                    sessionId,
                    m_sessionMessageHeaderDecoder.timestamp(),
                    buffer,
                    offset + SESSION_HEADER_LENGTH,
                    length - SESSION_HEADER_LENGTH,
                    header);
            }
            break;
        }

        case SessionEvent::SBE_TEMPLATE_ID:
        {
            m_sessionEventDecoder.wrapForDecode(
                reinterpret_cast<char *>(buffer.buffer()),
                offset + MessageHeader::encodedLength(),
                m_messageHeaderDecoder.blockLength(),
                m_messageHeaderDecoder.version(),
                buffer.capacity());

            const std::int64_t sessionId = m_sessionEventDecoder.clusterSessionId();
            if (sessionId == m_clusterSessionId)
            {
                const codecs::EventCode::Value code = m_sessionEventDecoder.code();
                if (codecs::EventCode::CLOSED == code)
                {
                    state(State::PENDING_CLOSE, 0);
                }

                m_egressListener->onSessionEvent(
                    m_sessionEventDecoder.correlationId(),
                    sessionId,
                    m_sessionEventDecoder.leadershipTermId(),
                    m_sessionEventDecoder.leaderMemberId(),
                    code,
                    m_sessionEventDecoder.detail());
            }
            break;
        }

        case NewLeaderEvent::SBE_TEMPLATE_ID:
        {
            m_newLeaderEventDecoder.wrapForDecode(
                reinterpret_cast<char *>(buffer.buffer()),
                offset + MessageHeader::encodedLength(),
                m_messageHeaderDecoder.blockLength(),
                m_messageHeaderDecoder.version(),
                buffer.capacity());

            const std::int64_t sessionId = m_newLeaderEventDecoder.clusterSessionId();
            if (sessionId == m_clusterSessionId)
            {
                Image* image = static_cast<Image*>(header.context());
                if (image != nullptr && m_subscription != nullptr)
                {
                    m_egressImage = m_subscription->imageBySessionId(image->sessionId());
                }
                else
                {
                    m_egressImage = nullptr;
                }
                onNewLeader(
                    sessionId,
                    m_newLeaderEventDecoder.leadershipTermId(),
                    m_newLeaderEventDecoder.leaderMemberId(),
                    m_newLeaderEventDecoder.ingressEndpoints());
            }
            break;
        }

        case AdminResponse::SBE_TEMPLATE_ID:
        {
            m_adminResponseDecoder.wrapForDecode(
                reinterpret_cast<char *>(buffer.buffer()),
                offset + MessageHeader::encodedLength(),
                m_messageHeaderDecoder.blockLength(),
                m_messageHeaderDecoder.version(),
                buffer.capacity());

            const std::int64_t sessionId = m_adminResponseDecoder.clusterSessionId();
            if (sessionId == m_clusterSessionId)
            {
                const std::int64_t correlationId = m_adminResponseDecoder.correlationId();
                const codecs::AdminRequestType::Value requestType = m_adminResponseDecoder.requestType();
                const codecs::AdminResponseCode::Value responseCode = m_adminResponseDecoder.responseCode();
                const std::string message = m_adminResponseDecoder.message();
                const std::int32_t payloadOffset = m_adminResponseDecoder.offset() +
                    AdminResponse::SBE_BLOCK_LENGTH +
                    AdminResponse::messageHeaderLength() +
                    static_cast<std::int32_t>(message.length()) +
                    AdminResponse::payloadHeaderLength();
                const std::int32_t payloadLength = m_adminResponseDecoder.payloadLength();

                m_egressListener->onAdminResponse(
                    sessionId,
                    correlationId,
                    requestType,
                    responseCode,
                    message,
                    buffer,
                    payloadOffset,
                    payloadLength);
            }
            break;
        }

        default:
            break;
    }
}

inline ControlledPollAction AeronCluster::onControlledFragment(
    AtomicBuffer& buffer, std::int32_t offset, std::int32_t length, Header& header)
{
    m_messageHeaderDecoder.wrap(reinterpret_cast<char *>(buffer.buffer()), offset, MessageHeader::sbeSchemaVersion(), buffer.capacity());

    const std::int32_t schemaId = m_messageHeaderDecoder.schemaId();
    const std::int32_t templateId = m_messageHeaderDecoder.templateId();

    if (schemaId != MessageHeader::sbeSchemaId())
    {
        throw ClusterException(
            "expected cluster egress schemaId=" + std::to_string(MessageHeader::sbeSchemaId()) + 
            " actual=" + std::to_string(schemaId), SOURCEINFO);
    }

    switch (templateId)
    {
        case SessionMessageHeader::SBE_TEMPLATE_ID:
        {
            m_sessionMessageHeaderDecoder.wrapForDecode(
                reinterpret_cast<char *>(buffer.buffer()),
                offset + MessageHeader::encodedLength(),
                m_messageHeaderDecoder.blockLength(),
                m_messageHeaderDecoder.version(),
                buffer.capacity());

            const std::int64_t sessionId = m_sessionMessageHeaderDecoder.clusterSessionId();
            if (sessionId == m_clusterSessionId)
            {
                return static_cast<ControlledPollAction>(m_controlledEgressListener->onMessage(
                    sessionId,
                    m_sessionMessageHeaderDecoder.timestamp(),
                    buffer,
                    offset + SESSION_HEADER_LENGTH,
                    length - SESSION_HEADER_LENGTH,
                    header));
            }
            break;
        }

        case SessionEvent::SBE_TEMPLATE_ID:
        {
            m_sessionEventDecoder.wrapForDecode(
                reinterpret_cast<char *>(buffer.buffer()),
                offset + MessageHeader::encodedLength(),
                m_messageHeaderDecoder.blockLength(),
                m_messageHeaderDecoder.version(),
                buffer.capacity());

            const std::int64_t sessionId = m_sessionEventDecoder.clusterSessionId();
            if (sessionId == m_clusterSessionId)
            {
                const codecs::EventCode::Value code = m_sessionEventDecoder.code();
                if (codecs::EventCode::CLOSED == code)
                {
                    state(State::PENDING_CLOSE, 0);
                }

                m_controlledEgressListener->onSessionEvent(
                    m_sessionEventDecoder.correlationId(),
                    sessionId,
                    m_sessionEventDecoder.leadershipTermId(),
                    m_sessionEventDecoder.leaderMemberId(),
                    code,
                    m_sessionEventDecoder.detail());
            }
            break;
        }

        case NewLeaderEvent::SBE_TEMPLATE_ID:
        {
            m_newLeaderEventDecoder.wrapForDecode(
                reinterpret_cast<char *>(buffer.buffer()),
                offset + MessageHeader::encodedLength(),
                m_messageHeaderDecoder.blockLength(),
                m_messageHeaderDecoder.version(),
                buffer.capacity());

            const std::int64_t sessionId = m_newLeaderEventDecoder.clusterSessionId();
            if (sessionId == m_clusterSessionId)
            {
                Image* image = static_cast<Image*>(header.context());
                if (image != nullptr && m_subscription != nullptr)
                {
                    m_egressImage = m_subscription->imageBySessionId(image->sessionId());
                }
                else
                {
                    m_egressImage = nullptr;
                }
                onNewLeader(
                    sessionId,
                    m_newLeaderEventDecoder.leadershipTermId(),
                    m_newLeaderEventDecoder.leaderMemberId(),
                    m_newLeaderEventDecoder.ingressEndpoints());
            }
            break;
        }

        case AdminResponse::SBE_TEMPLATE_ID:
        {
            m_adminResponseDecoder.wrapForDecode(
                reinterpret_cast<char *>(buffer.buffer()),
                offset + MessageHeader::encodedLength(),
                m_messageHeaderDecoder.blockLength(),
                m_messageHeaderDecoder.version(),
                buffer.capacity());

            const std::int64_t sessionId = m_adminResponseDecoder.clusterSessionId();
            if (sessionId == m_clusterSessionId)
            {
                const std::int64_t correlationId = m_adminResponseDecoder.correlationId();
                const codecs::AdminRequestType::Value requestType = m_adminResponseDecoder.requestType();
                const codecs::AdminResponseCode::Value responseCode = m_adminResponseDecoder.responseCode();
                const std::string message = m_adminResponseDecoder.message();
                const std::int32_t payloadOffset = m_adminResponseDecoder.offset() +
                    AdminResponse::SBE_BLOCK_LENGTH +
                    AdminResponse::messageHeaderLength() +
                    static_cast<std::int32_t>(message.length()) +
                    AdminResponse::payloadHeaderLength();
                const std::int32_t payloadLength = m_adminResponseDecoder.payloadLength();

                m_controlledEgressListener->onAdminResponse(
                    sessionId,
                    correlationId,
                    requestType,
                    responseCode,
                    message,
                    buffer,
                    payloadOffset,
                    payloadLength);
            }
            break;
        }

        default:
            break;
    }
    return ControlledPollAction::CONTINUE;
}

// Static connect methods
inline std::shared_ptr<AeronCluster> AeronCluster::connect()
{
    return connect(std::make_shared<Context>());
}

inline std::shared_ptr<AeronCluster> AeronCluster::connect(std::shared_ptr<Context> ctx)
{
    std::unique_ptr<AsyncConnect> asyncConnect;
    try
    {
        ctx->conclude();

        auto aeron = ctx->aeron();
        const std::int64_t deadlineNs = aeron::systemNanoClock() + ctx->messageTimeoutNs();
        asyncConnect = std::make_unique<AsyncConnect>(ctx, deadlineNs);
        auto idleStrategy = ctx->idleStrategy<concurrent::BackoffIdleStrategy>();

        std::shared_ptr<AeronCluster> aeronCluster;
        AsyncConnect::State state = asyncConnect->state();
        while (!(aeronCluster = asyncConnect->poll()))
        {
            if (aeron->usesAgentInvoker())
            {
                aeron->conductorAgentInvoker().invoke();
            }

            if (state != asyncConnect->state())
            {
                state = asyncConnect->state();
                if (idleStrategy)
                {
                    idleStrategy->reset();
                }
            }
            else
            {
                if (idleStrategy)
                {
                    idleStrategy->idle();
                }
            }
        }

        return aeronCluster;
    }
    catch (const ConcurrentConcludeException&)
    {
        throw;
    }
    catch (...)
    {
        if (!ctx->ownsAeronClient())
        {
            if (asyncConnect)
            {
                asyncConnect->close();
            }
        }

        ctx->close();

        throw;
    }
}

inline std::unique_ptr<AeronCluster::AsyncConnect> AeronCluster::asyncConnect()
{
    return asyncConnect(std::make_shared<Context>());
}

inline std::unique_ptr<AeronCluster::AsyncConnect> AeronCluster::asyncConnect(std::shared_ptr<Context> ctx)
{
    try
    {
        ctx->conclude();

        const std::int64_t deadlineNs = aeron::systemNanoClock() + ctx->messageTimeoutNs();

        return std::make_unique<AsyncConnect>(ctx, deadlineNs);
    }
    catch (...)
    {
        ctx->close();
        throw;
    }
}

// Definition of static constexpr members (required for ODR-use in some contexts)
constexpr int AeronCluster::FRAGMENT_LIMIT;
constexpr int AeronCluster::SEND_ATTEMPTS;

}}}
