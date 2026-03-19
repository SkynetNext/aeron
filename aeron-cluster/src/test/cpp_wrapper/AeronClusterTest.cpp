/*
 * Copyright 2014-2025 Real Logic Limited.
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

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "cluster/client/AeronCluster.h"
#include "cluster/client/EgressListener.h"
#include "concurrent/AtomicBuffer.h"
#include "concurrent/NoOpIdleStrategy.h"
#include "concurrent/logbuffer/DataFrameHeader.h"
#include "concurrent/logbuffer/Header.h"
#include "generated/aeron_cluster_codecs/MessageHeader.h"
#include "generated/aeron_cluster_codecs/NewLeaderEvent.h"
#include "concurrent/logbuffer/FrameDescriptor.h"

extern "C" {
#include "aeron_image.h"
}

using namespace aeron;
using namespace aeron::cluster::client;
using namespace aeron::cluster::codecs;
using namespace aeron::concurrent;
using namespace aeron::concurrent::logbuffer;
using namespace testing;

static const std::string INGRESS_ENDPOINTS = "0=foo:1000,1=bar:1000,2=baz:1000";
static const std::int64_t CLUSTER_SESSION_ID = 123;

class MockEgressListener : public EgressListener
{
public:
    MOCK_METHOD(void, onMessage,
        (std::int64_t clusterSessionId, std::int64_t timestamp,
         AtomicBuffer &buffer, util::index_t offset, util::index_t length,
         Header &header),
        (override));

    MOCK_METHOD(void, onNewLeader,
        (std::int64_t clusterSessionId, std::int64_t leadershipTermId,
         std::int32_t leaderMemberId, const std::string &ingressEndpoints),
        (override));
};

// ============================================================================
// Test fixture — friend of AeronCluster, so it can call private constructor
// and access private injectable function members.
// ============================================================================
class AeronClusterTestFixture : public testing::TestWithParam<std::tuple<bool, bool>>
{
public:
    AeronClusterTestFixture()
        : m_nanoTime(0)
        , m_leadershipTermId(2)
        , m_leaderMemberId(1)
        , m_newLeaderEventPending(false)
        , m_egressImageClosed(false)
        , m_ingressPublicationResult(NOT_CONNECTED)
        , m_bufferData(1024, 0)
        , m_appMessageData(8, 0)
        , m_buffer(m_bufferData.data(), static_cast<util::index_t>(m_bufferData.size()))
        , m_appMessage(m_appMessageData.data(), static_cast<util::index_t>(m_appMessageData.size()))
        , m_header(createTestHeader())
        , m_egressListener(std::make_shared<MockEgressListener>())
    {
    }

    ~AeronClusterTestFixture() override
    {
        if (m_aeronCluster)
        {
            m_aeronCluster->close();
            m_aeronCluster.reset();
        }
        if (m_header.hdr() != nullptr)
        {
            delete m_header.hdr();
        }
    }

    void SetUp() override
    {
        // Build context without calling conclude() — we bypass it since there is
        // no real Aeron driver.  conclude() would create an Aeron client if none
        // is present; we skip that path entirely.
        auto ctx = std::make_shared<AeronCluster::Context>();
        ctx->aeron(nullptr)
            .ownsAeronClient(false)
            .egressChannel("aeron:udp?endpoint=localhost:0")
            .ingressChannel("aeron:udp")
            .idleStrategy(std::make_shared<NoOpIdleStrategy>())
            .egressListener(m_egressListener)
            .newLeaderTimeoutNs(std::chrono::seconds(1).count());

        // Construct AeronCluster directly via friend access (private constructor).
        MessageHeader msgHdr;
        m_aeronCluster = std::shared_ptr<AeronCluster>(new AeronCluster(
            ctx,
            msgHdr,
            /*publication=*/nullptr,
            /*subscription=*/nullptr,
            /*egressImage=*/nullptr,
            /*endpointByIdMap=*/std::unordered_map<int, std::unique_ptr<AeronCluster::MemberIngress>>{},
            CLUSTER_SESSION_ID,
            m_leadershipTermId,
            m_leaderMemberId));

        // Override the nano clock so the test controls time.
        m_aeronCluster->m_nanoClock = [this]() -> std::int64_t { return m_nanoTime; };

        // Default: tryClaim returns m_ingressPublicationResult without filling the BufferClaim.
        // Tests call makeIngressPublicationReturn() to configure specific behaviour.
        m_aeronCluster->m_publicationTryClaim =
            [this](std::int32_t /*len*/, BufferClaim & /*bc*/) -> std::int64_t
            {
                return m_ingressPublicationResult;
            };

        // subscription poll: delivers a NewLeaderEvent fragment if pending, else 0.
        m_aeronCluster->m_subscriptionPoll =
            [this](const fragment_handler_t &handler, int /*limit*/) -> std::int32_t
            {
                return deliverPendingNewLeaderEvent(handler);
            };

        // egressImage closed: driven by m_egressImageClosed.
        m_aeronCluster->m_egressImageIsClosed = [this]() -> bool { return m_egressImageClosed; };

        // addPublicationFn: return nullptr (no real Aeron — tryClaim is overridden anyway).
        m_aeronCluster->m_addPublicationFn =
            [](const std::string &, std::int32_t) -> std::shared_ptr<Publication>
            {
                return nullptr;
            };
    }

protected:
    // -----------------------------------------------------------------------
    // Helpers (mirrors Java helper methods)
    // -----------------------------------------------------------------------

    void makeIngressPublicationReturn(std::int64_t result)
    {
        m_ingressPublicationResult = result;
        if (result > 0)
        {
            // Positive result: fill the BufferClaim so the code can commit it.
            m_aeronCluster->m_publicationTryClaim =
                [this, result](std::int32_t len, BufferClaim &bc) -> std::int64_t
                {
                    bc.wrap(m_bufferData.data(), len);
                    return result;
                };
        }
        else
        {
            m_aeronCluster->m_publicationTryClaim =
                [result](std::int32_t /*len*/, BufferClaim & /*bc*/) -> std::int64_t
                {
                    return result;
                };
        }
    }

    void makeEgressSubscriptionDeliverNewLeaderEvent()
    {
        m_newLeaderEventPending = true;
    }

    // friend access to AeronCluster private members must go through fixture methods
    // because TEST_F generates a subclass and friend status is not inherited.
    bool ingressPublicationIsNull() const
    {
        return m_aeronCluster->m_publication == nullptr;
    }

    // -----------------------------------------------------------------------
    // State
    // -----------------------------------------------------------------------
    std::int64_t m_nanoTime;
    std::int32_t m_leadershipTermId;
    std::int32_t m_leaderMemberId;
    bool m_newLeaderEventPending;
    bool m_egressImageClosed;
    std::int64_t m_ingressPublicationResult;

    std::vector<std::uint8_t> m_bufferData;
    std::vector<std::uint8_t> m_appMessageData;

    AtomicBuffer m_buffer;
    AtomicBuffer m_appMessage;
    Header m_header;

    std::shared_ptr<MockEgressListener> m_egressListener;
    std::shared_ptr<AeronCluster> m_aeronCluster;

private:
    // -----------------------------------------------------------------------
    // Build a bare Header suitable for feeding to the fragment handler.
    // -----------------------------------------------------------------------
    Header createTestHeader()
    {
        static std::vector<std::uint8_t> frameBuffer(256, 0);
        auto *frame = reinterpret_cast<std::uint8_t *>(frameBuffer.data());

        frame[DataFrameHeader::FRAME_LENGTH_FIELD_OFFSET] = 0;
        frame[DataFrameHeader::VERSION_FIELD_OFFSET] = DataFrameHeader::CURRENT_VERSION;
        frame[DataFrameHeader::FLAGS_FIELD_OFFSET] = 0;
        frame[DataFrameHeader::TYPE_FIELD_OFFSET] = DataFrameHeader::HDR_TYPE_DATA;

        auto *aeronHeader = new aeron_header_t{};
        aeronHeader->frame = reinterpret_cast<aeron_data_header_t *>(frame);
        aeronHeader->fragmented_frame_length = NULL_VALUE;
        aeronHeader->initial_term_id = 0;
        aeronHeader->position_bits_to_shift = 0;
        aeronHeader->context = nullptr; // no Image context → egressImage will be nullptr after event
        return Header(aeronHeader);
    }

    // -----------------------------------------------------------------------
    // Deliver a NewLeaderEvent SBE fragment if m_newLeaderEventPending.
    // Mirrors the Java mock's thenAnswer for egressSubscription.poll().
    // -----------------------------------------------------------------------
    std::int32_t deliverPendingNewLeaderEvent(const fragment_handler_t &handler)
    {
        if (!m_newLeaderEventPending)
        {
            return 0;
        }
        m_newLeaderEventPending = false;

        // Advance term/member IDs exactly as the Java test does.
        ++m_leadershipTermId;
        ++m_leaderMemberId;

        // Lay out a minimal Aeron data frame header at offset 0, then the SBE
        // message starting at DataFrameHeader::HEADER_LENGTH (= the Java
        // DataHeaderFlyweight.HEADER_LENGTH).
        const int frameOffset = 0;
        const int msgOffset = DataFrameHeader::LENGTH; // = sizeof(aeron_header_values_frame_t)

        std::fill(m_bufferData.begin(), m_bufferData.end(), 0);

        // Set UNFRAGMENTED flag at frameOffset (matches Java FrameDescriptor.frameFlags).
        m_bufferData[frameOffset + DataFrameHeader::FLAGS_FIELD_OFFSET] =
            static_cast<std::uint8_t>(FrameDescriptor::UNFRAGMENTED);

        // Encode the NewLeaderEvent SBE message.
        NewLeaderEvent encoder;
        char *base = reinterpret_cast<char *>(m_bufferData.data()) + msgOffset;
        encoder.wrapAndApplyHeader(base, 0, static_cast<std::uint64_t>(m_bufferData.size() - msgOffset));
        encoder.clusterSessionId(CLUSTER_SESSION_ID);
        encoder.leadershipTermId(m_leadershipTermId);
        encoder.leaderMemberId(m_leaderMemberId);
        encoder.putIngressEndpoints(INGRESS_ENDPOINTS.c_str(),
            static_cast<std::uint32_t>(INGRESS_ENDPOINTS.size()));

        const int length = static_cast<int>(
            MessageHeader::encodedLength() + encoder.encodedLength());

        handler(m_buffer, msgOffset, length, m_header);
        return 1;
    }
};

// Parameterized instantiation for {withIngressDisconnect, withAppMessages}
INSTANTIATE_TEST_SUITE_P(AeronClusterTestSuite, AeronClusterTestFixture,
    testing::Values(
        std::make_tuple(false, false),
        std::make_tuple(false, true),
        std::make_tuple(true, false),
        std::make_tuple(true, true)));

// ============================================================================
// Test cases  (1:1 with Java AeronClusterTest)
// ============================================================================

TEST_P(AeronClusterTestFixture, shouldStayConnectedAfterSuccessfulFailover)
{
    const bool withIngressDisconnect = std::get<0>(GetParam());
    const bool withAppMessages = std::get<1>(GetParam());

    const std::int64_t initialResult = withIngressDisconnect ? NOT_CONNECTED : 128;
    makeIngressPublicationReturn(initialResult);

    if (withAppMessages)
    {
        EXPECT_EQ(initialResult, m_aeronCluster->offer(m_appMessage, 0, 8));
    }
    else
    {
        EXPECT_EQ(!withIngressDisconnect, m_aeronCluster->sendKeepAlive());
    }

    m_nanoTime += m_aeronCluster->context()->newLeaderTimeoutNs() - 1;

    makeEgressSubscriptionDeliverNewLeaderEvent();

    EXPECT_CALL(*m_egressListener,
        onNewLeader(CLUSTER_SESSION_ID, m_leadershipTermId + 1, m_leaderMemberId + 1, INGRESS_ENDPOINTS))
        .Times(1);

    EXPECT_EQ(1, m_aeronCluster->pollEgress());
    EXPECT_EQ(0, m_aeronCluster->pollEgress());

    m_nanoTime += m_aeronCluster->context()->messageTimeoutNs() - 1;

    makeIngressPublicationReturn(256);
    if (withAppMessages)
    {
        EXPECT_EQ(256, m_aeronCluster->offer(m_appMessage, 0, 8));
    }
    else
    {
        EXPECT_TRUE(m_aeronCluster->sendKeepAlive());
    }

    m_nanoTime += 1;

    EXPECT_EQ(0, m_aeronCluster->pollEgress());
    EXPECT_FALSE(m_aeronCluster->isClosed());
}

TEST_P(AeronClusterTestFixture, shouldCloseItselfWhenDisconnectedForLongerThanNewLeaderTimeout)
{
    const bool withAppMessages = std::get<1>(GetParam());

    makeIngressPublicationReturn(NOT_CONNECTED);

    if (withAppMessages)
    {
        EXPECT_EQ(NOT_CONNECTED, m_aeronCluster->offer(m_appMessage, 0, 8));
    }
    else
    {
        EXPECT_FALSE(m_aeronCluster->sendKeepAlive());
    }

    m_nanoTime += m_aeronCluster->context()->newLeaderTimeoutNs() - 1;

    EXPECT_EQ(0, m_aeronCluster->pollEgress());
    EXPECT_FALSE(m_aeronCluster->isClosed());

    m_nanoTime += 1;

    EXPECT_EQ(1, m_aeronCluster->pollEgress());
    EXPECT_TRUE(m_aeronCluster->isClosed());
}

TEST_P(AeronClusterTestFixture,
    shouldCloseItselfWhenUnableToSendMessageForLongerThanNewLeaderConnectionTimeout)
{
    const bool withAppMessages = std::get<1>(GetParam());

    makeIngressPublicationReturn(NOT_CONNECTED);

    if (withAppMessages)
    {
        EXPECT_EQ(NOT_CONNECTED, m_aeronCluster->offer(m_appMessage, 0, 8));
    }
    else
    {
        EXPECT_FALSE(m_aeronCluster->sendKeepAlive());
    }

    m_nanoTime += m_aeronCluster->context()->newLeaderTimeoutNs() / 2;

    makeEgressSubscriptionDeliverNewLeaderEvent();

    EXPECT_CALL(*m_egressListener,
        onNewLeader(CLUSTER_SESSION_ID, m_leadershipTermId + 1, m_leaderMemberId + 1, INGRESS_ENDPOINTS))
        .Times(1);

    EXPECT_EQ(1, m_aeronCluster->pollEgress());
    EXPECT_FALSE(m_aeronCluster->isClosed());

    m_nanoTime += m_aeronCluster->context()->messageTimeoutNs() - 1;

    if (withAppMessages)
    {
        EXPECT_EQ(NOT_CONNECTED, m_aeronCluster->offer(m_appMessage, 0, 8));
    }
    else
    {
        EXPECT_FALSE(m_aeronCluster->sendKeepAlive());
    }

    m_nanoTime += 1;

    EXPECT_EQ(1, m_aeronCluster->pollEgress());
    EXPECT_TRUE(m_aeronCluster->isClosed());
}

// Non-parameterised tests use the first parameter set; the fixture's TearDown
// still closes the cluster so the second parameter doesn't matter.

TEST_F(AeronClusterTestFixture, shouldCloseIngressPublicationWhenEgressImageCloses)
{
    // in CONNECTED state
    m_egressImageClosed = true;
    EXPECT_EQ(1, m_aeronCluster->pollEgress());
    // C++ equivalent: verify publication was closed (m_publication reset to null after close())
    EXPECT_TRUE(ingressPublicationIsNull());

    m_egressImageClosed = false;
    makeEgressSubscriptionDeliverNewLeaderEvent();

    EXPECT_CALL(*m_egressListener,
        onNewLeader(CLUSTER_SESSION_ID, m_leadershipTermId + 1, m_leaderMemberId + 1, INGRESS_ENDPOINTS))
        .Times(1);
    EXPECT_EQ(1, m_aeronCluster->pollEgress());
    // After onNewLeader: addPublicationFn returns nullptr, so publication is still null
    EXPECT_TRUE(ingressPublicationIsNull());

    // In AWAIT_NEW_LEADER_CONNECTION state: another egressImage close
    m_egressImageClosed = true;
    EXPECT_EQ(1, m_aeronCluster->pollEgress());
    EXPECT_TRUE(ingressPublicationIsNull());
}

TEST_F(AeronClusterTestFixture, shouldCloseItselfAfterReachingMaxPositionOnTheIngressPublication)
{
    makeIngressPublicationReturn(MAX_POSITION_EXCEEDED);

    EXPECT_EQ(MAX_POSITION_EXCEEDED, m_aeronCluster->offer(m_appMessage, 0, 8));
    // publication should have been closed and nulled by trackIngressPublicationResult
    EXPECT_TRUE(ingressPublicationIsNull());

    EXPECT_EQ(1, m_aeronCluster->pollStateChanges());
    EXPECT_TRUE(m_aeronCluster->isClosed());
}
