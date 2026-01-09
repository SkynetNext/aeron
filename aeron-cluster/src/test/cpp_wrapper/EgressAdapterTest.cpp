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

#include <gtest/gtest.h>
#include <gmock/gmock.h>

#include "EmbeddedMediaDriver.h"
#include "cluster/client/EgressAdapter.h"
#include "cluster/client/EgressListener.h"
#include "cluster/client/EgressListenerExtension.h"
#include "cluster/client/ClusterExceptions.h"
#include "Aeron.h"
#include "Subscription.h"
#include "concurrent/logbuffer/Header.h"
#include "concurrent/logbuffer/DataFrameHeader.h"
#include "concurrent/AtomicBuffer.h"
#include "generated/aeron_cluster_codecs/MessageHeader.h"
#include "generated/aeron_cluster_codecs/SessionMessageHeader.h"
#include "generated/aeron_cluster_codecs/SessionEvent.h"
#include "generated/aeron_cluster_codecs/NewLeaderEvent.h"
#include "generated/aeron_cluster_codecs/AdminResponse.h"
#include "generated/aeron_cluster_codecs/EventCode.h"
#include "generated/aeron_cluster_codecs/AdminRequestType.h"
#include "generated/aeron_cluster_codecs/AdminResponseCode.h"

extern "C"
{
#include "aeron_image.h"
}

using namespace aeron;
using namespace aeron::cluster::client;
using namespace aeron::concurrent;
using namespace aeron::concurrent::logbuffer;
using namespace testing;

class EgressAdapterTest : public testing::Test
{
public:
    EgressAdapterTest() :
        m_bufferData(512, 0),
        m_buffer(m_bufferData.data(), static_cast<util::index_t>(m_bufferData.size())),
        m_header(createTestHeader())
    {
        m_driver.start();
        Context ctx;
        m_aeron = Aeron::connect(ctx);
        
        // Create a subscription for testing
        std::int64_t subId = m_aeron->addSubscription("aeron:udp?endpoint=localhost:24325", 10);
        m_subscription = m_aeron->findSubscription(subId);
        
        while (!m_subscription)
        {
            std::this_thread::yield();
            m_subscription = m_aeron->findSubscription(subId);
        }
    }

    ~EgressAdapterTest() override
    {
        delete m_header.hdr();
        m_driver.stop();
    }

private:
    Header createTestHeader()
    {
        // Allocate frame buffer (must be valid for aeron_header_values to work)
        static std::vector<std::uint8_t> frameBuffer(256, 0);
        auto *frame = reinterpret_cast<std::uint8_t *>(frameBuffer.data());
        
        // Initialize frame header fields
        using namespace aeron::concurrent::logbuffer;
        frame[DataFrameHeader::FRAME_LENGTH_FIELD_OFFSET] = 0;
        frame[DataFrameHeader::VERSION_FIELD_OFFSET] = DataFrameHeader::CURRENT_VERSION;
        frame[DataFrameHeader::FLAGS_FIELD_OFFSET] = 0;
        frame[DataFrameHeader::TYPE_FIELD_OFFSET] = DataFrameHeader::HDR_TYPE_DATA;
        
        auto *aeronHeader = new aeron_header_t{};
        aeronHeader->frame = frame;
        aeronHeader->fragmented_frame_length = NULL_VALUE;
        aeronHeader->initial_term_id = 0;
        aeronHeader->position_bits_to_shift = 0;
        aeronHeader->context = nullptr;
        return Header(aeronHeader);
    }

protected:
    EmbeddedMediaDriver m_driver;
    std::shared_ptr<Aeron> m_aeron;
    std::shared_ptr<Subscription> m_subscription;
    std::vector<std::uint8_t> m_bufferData;
    AtomicBuffer m_buffer;
    Header m_header;
};

// Mock EgressListener
class MockEgressListener : public EgressListener
{
public:
    MOCK_METHOD(void, onMessage, (
        std::int64_t clusterSessionId,
        std::int64_t timestamp,
        AtomicBuffer& buffer,
        util::index_t offset,
        util::index_t length,
        Header& header), (override));

    MOCK_METHOD(void, onSessionEvent, (
        std::int64_t correlationId,
        std::int64_t clusterSessionId,
        std::int64_t leadershipTermId,
        std::int32_t leaderMemberId,
        EventCode::Value code,
        const std::string& detail), (override));

    MOCK_METHOD(void, onNewLeader, (
        std::int64_t clusterSessionId,
        std::int64_t leadershipTermId,
        std::int32_t leaderMemberId,
        const std::string& ingressEndpoints), (override));

    MOCK_METHOD(void, onAdminResponse, (
        std::int64_t clusterSessionId,
        std::int64_t correlationId,
        AdminRequestType::Value requestType,
        AdminResponseCode::Value responseCode,
        const std::string& message,
        AtomicBuffer& buffer,
        util::index_t offset,
        util::index_t length), (override));
};

// Mock EgressListenerExtension
class MockEgressListenerExtension : public EgressListenerExtension
{
public:
    MOCK_METHOD(void, onExtensionMessage, (
        std::int32_t actingBlockLength,
        std::int32_t templateId,
        std::int32_t schemaId,
        std::int32_t actingVersion,
        AtomicBuffer& buffer,
        std::int32_t offset,
        std::int32_t length), (override));
};

TEST_F(EgressAdapterTest, onFragmentShouldDelegateToEgressListenerOnUnknownSchemaId)
{
    const std::int32_t schemaId = 17;
    const std::int32_t templateId = 19;
    
    MessageHeader messageHeader;
    messageHeader.wrap(reinterpret_cast<char *>(m_buffer.buffer()), 0, MessageHeader::sbeSchemaVersion(), m_buffer.capacity());
    messageHeader.blockLength(0);
    messageHeader.templateId(templateId);
    messageHeader.schemaId(schemaId);
    messageHeader.version(0);
    
    auto listenerExtension = std::make_shared<MockEgressListenerExtension>();
    auto listener = std::make_shared<MockEgressListener>();
    
    EgressAdapter adapter(listener, listenerExtension, 0, m_subscription, 3);
    
    EXPECT_CALL(*listenerExtension, onExtensionMessage(
        testing::_,
        templateId,
        schemaId,
        0,
        testing::_,
        static_cast<std::int32_t>(MessageHeader::encodedLength()),
        static_cast<std::int32_t>(MessageHeader::encodedLength()))).Times(1);
    
    adapter.onFragment(m_buffer, 0, static_cast<std::int32_t>(MessageHeader::encodedLength() * 2), m_header);
}

TEST_F(EgressAdapterTest, defaultEgressListenerBehaviourShouldThrowClusterExceptionOnUnknownSchemaId)
{
    // EgressListener is abstract, use MockEgressListener instead
    auto listener = std::make_shared<MockEgressListener>();
    EgressAdapter adapter(listener, 42, m_subscription, 5);
    
    // Create a message with wrong schema ID
    MessageHeader messageHeader;
    messageHeader.wrap(reinterpret_cast<char *>(m_buffer.buffer()), 0, MessageHeader::sbeSchemaVersion(), m_buffer.capacity());
    messageHeader.blockLength(0);
    messageHeader.templateId(0);
    messageHeader.schemaId(0); // Wrong schema ID
    messageHeader.version(0);
    
    EXPECT_THROW(
        {
            adapter.onFragment(m_buffer, 0, static_cast<std::int32_t>(64), m_header);
        },
        ClusterException);
}

TEST_F(EgressAdapterTest, onFragmentShouldInvokeOnMessageCallbackIfSessionIdMatches)
{
    const std::int32_t offset = 4;
    const std::int64_t sessionId = 2973438724LL;
    const std::int64_t timestamp = -46328746238764832LL;
    
    MessageHeader messageHeader;
    SessionMessageHeader encoder;
    
    messageHeader.wrap(reinterpret_cast<char *>(m_buffer.buffer()), offset, MessageHeader::sbeSchemaVersion(), m_buffer.capacity());
    messageHeader.blockLength(encoder.sbeBlockLength());
    messageHeader.templateId(encoder.sbeTemplateId());
    messageHeader.schemaId(encoder.sbeSchemaId());
    messageHeader.version(encoder.sbeSchemaVersion());
    
    encoder.wrapForEncode(reinterpret_cast<char *>(m_buffer.buffer()), offset + messageHeader.encodedLength(), m_buffer.capacity());
    encoder.clusterSessionId(sessionId);
    encoder.timestamp(timestamp);
    
    auto egressListener = std::make_shared<MockEgressListener>();
    EgressAdapter adapter(egressListener, sessionId, m_subscription, 3);
    
    const std::int32_t totalLength = static_cast<std::int32_t>(messageHeader.encodedLength() + encoder.encodedLength());
    const std::int32_t payloadLength = totalLength - AeronCluster::SESSION_HEADER_LENGTH;
    
    EXPECT_CALL(*egressListener, onMessage(
        sessionId,
        timestamp,
        testing::_,
        offset + AeronCluster::SESSION_HEADER_LENGTH,
        payloadLength,
        testing::_)).Times(1);
    
    adapter.onFragment(m_buffer, static_cast<std::int32_t>(offset), totalLength, m_header);
}

TEST_F(EgressAdapterTest, onFragmentIsANoOpIfSessionIdDoesNotMatchOnSessionMessage)
{
    const std::int32_t offset = 18;
    const std::int64_t sessionId = 21;
    const std::int64_t timestamp = 1000;
    
    MessageHeader messageHeader;
    SessionMessageHeader encoder;
    
    messageHeader.wrap(reinterpret_cast<char *>(m_buffer.buffer()), offset, MessageHeader::sbeSchemaVersion(), m_buffer.capacity());
    messageHeader.blockLength(encoder.sbeBlockLength());
    messageHeader.templateId(encoder.sbeTemplateId());
    messageHeader.schemaId(encoder.sbeSchemaId());
    messageHeader.version(encoder.sbeSchemaVersion());
    
    encoder.wrapForEncode(reinterpret_cast<char *>(m_buffer.buffer()), offset + messageHeader.encodedLength(), m_buffer.capacity());
    encoder.clusterSessionId(sessionId);
    encoder.timestamp(timestamp);
    
    auto egressListener = std::make_shared<MockEgressListener>();
    EgressAdapter adapter(egressListener, -19, m_subscription, 3);
    
    const std::int32_t totalLength = static_cast<std::int32_t>(messageHeader.encodedLength() + encoder.encodedLength());
    
    EXPECT_CALL(*egressListener, onMessage(testing::_, testing::_, testing::_, testing::_, testing::_, testing::_)).Times(0);
    
    adapter.onFragment(m_buffer, static_cast<std::int32_t>(offset), totalLength, m_header);
}

TEST_F(EgressAdapterTest, onFragmentShouldInvokeOnSessionEventCallbackIfSessionIdMatches)
{
    const std::int32_t offset = 8;
    const std::int64_t clusterSessionId = 42;
    const std::int64_t correlationId = 777;
    const std::int64_t leadershipTermId = 6;
    const std::int32_t leaderMemberId = 3;
    const EventCode::Value eventCode = EventCode::Value::REDIRECT;
    const std::int32_t version = 18;
    const std::string eventDetail = "Event details";
    
    MessageHeader messageHeader;
    SessionEvent encoder;
    
    messageHeader.wrap(reinterpret_cast<char *>(m_buffer.buffer()), offset, MessageHeader::sbeSchemaVersion(), m_buffer.capacity());
    messageHeader.blockLength(encoder.sbeBlockLength());
    messageHeader.templateId(encoder.sbeTemplateId());
    messageHeader.schemaId(encoder.sbeSchemaId());
    messageHeader.version(encoder.sbeSchemaVersion());
    
    encoder.wrapForEncode(reinterpret_cast<char *>(m_buffer.buffer()), offset + messageHeader.encodedLength(), m_buffer.capacity());
    encoder.clusterSessionId(clusterSessionId);
    encoder.correlationId(correlationId);
    encoder.leadershipTermId(leadershipTermId);
    encoder.leaderMemberId(leaderMemberId);
    encoder.code(eventCode);
    encoder.version(version);
    encoder.putDetail(eventDetail.data(), static_cast<std::uint32_t>(eventDetail.length()));
    
    auto egressListener = std::make_shared<MockEgressListener>();
    EgressAdapter adapter(egressListener, clusterSessionId, m_subscription, 10);
    
    EXPECT_CALL(*egressListener, onSessionEvent(
        correlationId,
        clusterSessionId,
        leadershipTermId,
        leaderMemberId,
        eventCode,
        eventDetail)).Times(1);
    
    adapter.onFragment(m_buffer, static_cast<std::int32_t>(offset), static_cast<std::int32_t>(encoder.encodedLength()), m_header);
}

TEST_F(EgressAdapterTest, onFragmentIsANoOpIfSessionIdDoesNotMatchOnSessionEvent)
{
    const std::int32_t offset = 8;
    const std::int64_t clusterSessionId = 42;
    const std::int64_t correlationId = 777;
    const std::int64_t leadershipTermId = 6;
    const std::int32_t leaderMemberId = 3;
    const EventCode::Value eventCode = EventCode::Value::REDIRECT;
    const std::int32_t version = 18;
    const std::string eventDetail = "Event details";
    
    MessageHeader messageHeader;
    SessionEvent encoder;
    
    messageHeader.wrap(reinterpret_cast<char *>(m_buffer.buffer()), offset, MessageHeader::sbeSchemaVersion(), m_buffer.capacity());
    messageHeader.blockLength(encoder.sbeBlockLength());
    messageHeader.templateId(encoder.sbeTemplateId());
    messageHeader.schemaId(encoder.sbeSchemaId());
    messageHeader.version(encoder.sbeSchemaVersion());
    
    encoder.wrapForEncode(reinterpret_cast<char *>(m_buffer.buffer()), offset + messageHeader.encodedLength(), m_buffer.capacity());
    encoder.clusterSessionId(clusterSessionId);
    encoder.correlationId(correlationId);
    encoder.leadershipTermId(leadershipTermId);
    encoder.leaderMemberId(leaderMemberId);
    encoder.code(eventCode);
    encoder.version(version);
    encoder.putDetail(eventDetail.data(), static_cast<std::uint32_t>(eventDetail.length()));
    
    auto egressListener = std::make_shared<MockEgressListener>();
    EgressAdapter adapter(egressListener, clusterSessionId + 1, m_subscription, 3);
    
    EXPECT_CALL(*egressListener, onSessionEvent(testing::_, testing::_, testing::_, testing::_, testing::_, testing::_)).Times(0);
    
    adapter.onFragment(m_buffer, static_cast<std::int32_t>(offset), static_cast<std::int32_t>(encoder.encodedLength()), m_header);
}

TEST_F(EgressAdapterTest, onFragmentShouldInvokeOnNewLeaderCallbackIfSessionIdMatches)
{
    const std::int32_t offset = 0;
    const std::int64_t clusterSessionId = 0;
    const std::int64_t leadershipTermId = 6;
    const std::int32_t leaderMemberId = 9999;
    const std::string ingressEndpoints = "ingress endpoints ...";
    
    MessageHeader messageHeader;
    NewLeaderEvent encoder;
    
    messageHeader.wrap(reinterpret_cast<char *>(m_buffer.buffer()), offset, MessageHeader::sbeSchemaVersion(), m_buffer.capacity());
    messageHeader.blockLength(encoder.sbeBlockLength());
    messageHeader.templateId(encoder.sbeTemplateId());
    messageHeader.schemaId(encoder.sbeSchemaId());
    messageHeader.version(encoder.sbeSchemaVersion());
    
    encoder.wrapForEncode(reinterpret_cast<char *>(m_buffer.buffer()), offset + messageHeader.encodedLength(), m_buffer.capacity());
    encoder.leadershipTermId(leadershipTermId);
    encoder.clusterSessionId(clusterSessionId);
    encoder.leaderMemberId(leaderMemberId);
    encoder.putIngressEndpoints(ingressEndpoints.data(), static_cast<std::uint32_t>(ingressEndpoints.length()));
    
    auto egressListener = std::make_shared<MockEgressListener>();
    EgressAdapter adapter(egressListener, clusterSessionId, m_subscription, 10);
    
    EXPECT_CALL(*egressListener, onNewLeader(
        clusterSessionId,
        leadershipTermId,
        leaderMemberId,
        ingressEndpoints)).Times(1);
    
    adapter.onFragment(m_buffer, static_cast<std::int32_t>(offset), static_cast<std::int32_t>(encoder.encodedLength()), m_header);
}

TEST_F(EgressAdapterTest, onFragmentIsANoOpIfSessionIdDoesNotMatchOnNewLeader)
{
    const std::int32_t offset = 0;
    const std::int64_t clusterSessionId = -100;
    const std::int64_t leadershipTermId = 6;
    const std::int32_t leaderMemberId = 9999;
    const std::string ingressEndpoints = "ingress endpoints ...";
    
    MessageHeader messageHeader;
    NewLeaderEvent encoder;
    
    messageHeader.wrap(reinterpret_cast<char *>(m_buffer.buffer()), offset, MessageHeader::sbeSchemaVersion(), m_buffer.capacity());
    messageHeader.blockLength(encoder.sbeBlockLength());
    messageHeader.templateId(encoder.sbeTemplateId());
    messageHeader.schemaId(encoder.sbeSchemaId());
    messageHeader.version(encoder.sbeSchemaVersion());
    
    encoder.wrapForEncode(reinterpret_cast<char *>(m_buffer.buffer()), offset + messageHeader.encodedLength(), m_buffer.capacity());
    encoder.leadershipTermId(leadershipTermId);
    encoder.clusterSessionId(clusterSessionId);
    encoder.leaderMemberId(leaderMemberId);
    encoder.putIngressEndpoints(ingressEndpoints.data(), static_cast<std::uint32_t>(ingressEndpoints.length()));
    
    auto egressListener = std::make_shared<MockEgressListener>();
    EgressAdapter adapter(egressListener, 0, m_subscription, 10);
    
    EXPECT_CALL(*egressListener, onNewLeader(testing::_, testing::_, testing::_, testing::_)).Times(0);
    
    adapter.onFragment(m_buffer, static_cast<std::int32_t>(offset), static_cast<std::int32_t>(encoder.encodedLength()), m_header);
}

TEST_F(EgressAdapterTest, onFragmentShouldInvokeOnAdminResponseCallbackIfSessionIdMatches)
{
    const std::int32_t offset = 24;
    const std::int64_t clusterSessionId = 18;
    const std::int64_t correlationId = 3274239749237498239LL;
    const AdminRequestType::Value type = AdminRequestType::Value::SNAPSHOT;
    const AdminResponseCode::Value responseCode = AdminResponseCode::Value::UNAUTHORISED_ACCESS;
    const std::string message = "Unauthorised access detected!";
    const std::vector<std::uint8_t> payload = {0x1, 0x2, 0x3};
    
    MessageHeader messageHeader;
    AdminResponse encoder;
    
    messageHeader.wrap(reinterpret_cast<char *>(m_buffer.buffer()), offset, MessageHeader::sbeSchemaVersion(), m_buffer.capacity());
    messageHeader.blockLength(encoder.sbeBlockLength());
    messageHeader.templateId(encoder.sbeTemplateId());
    messageHeader.schemaId(encoder.sbeSchemaId());
    messageHeader.version(encoder.sbeSchemaVersion());
    
    encoder.wrapForEncode(reinterpret_cast<char *>(m_buffer.buffer()), offset + messageHeader.encodedLength(), m_buffer.capacity());
    encoder.clusterSessionId(clusterSessionId);
    encoder.correlationId(correlationId);
    encoder.requestType(type);
    encoder.responseCode(responseCode);
    encoder.putMessage(message.data(), static_cast<std::uint32_t>(message.length()));
    encoder.putPayload(reinterpret_cast<const char *>(payload.data()), static_cast<std::uint32_t>(payload.size()));
    
    auto egressListener = std::make_shared<MockEgressListener>();
    EgressAdapter adapter(egressListener, clusterSessionId, m_subscription, 10);
    
    const std::int32_t payloadOffset = static_cast<std::int32_t>(
        offset + messageHeader.encodedLength() + encoder.encodedLength() - static_cast<std::int32_t>(payload.size()));
    
    EXPECT_CALL(*egressListener, onAdminResponse(
        clusterSessionId,
        correlationId,
        type,
        responseCode,
        message,
        testing::_,
        payloadOffset,
        static_cast<util::index_t>(payload.size()))).Times(1);
    
    adapter.onFragment(m_buffer, static_cast<std::int32_t>(offset), static_cast<std::int32_t>(encoder.encodedLength()), m_header);
}

TEST_F(EgressAdapterTest, onFragmentIsANoOpIfSessionIdDoesNotMatchOnAdminResponse)
{
    const std::int32_t offset = 24;
    const std::int64_t clusterSessionId = 18;
    const std::int64_t correlationId = 3274239749237498239LL;
    const AdminRequestType::Value type = AdminRequestType::Value::SNAPSHOT;
    const AdminResponseCode::Value responseCode = AdminResponseCode::Value::OK;
    const std::string message = "Unauthorised access detected!";
    const std::vector<std::uint8_t> payload = {0x1, 0x2, 0x3};
    
    MessageHeader messageHeader;
    AdminResponse encoder;
    
    messageHeader.wrap(reinterpret_cast<char *>(m_buffer.buffer()), offset, MessageHeader::sbeSchemaVersion(), m_buffer.capacity());
    messageHeader.blockLength(encoder.sbeBlockLength());
    messageHeader.templateId(encoder.sbeTemplateId());
    messageHeader.schemaId(encoder.sbeSchemaId());
    messageHeader.version(encoder.sbeSchemaVersion());
    
    encoder.wrapForEncode(reinterpret_cast<char *>(m_buffer.buffer()), offset + messageHeader.encodedLength(), m_buffer.capacity());
    encoder.clusterSessionId(clusterSessionId);
    encoder.correlationId(correlationId);
    encoder.requestType(type);
    encoder.responseCode(responseCode);
    encoder.putMessage(message.data(), static_cast<std::uint32_t>(message.length()));
    encoder.putPayload(reinterpret_cast<const char *>(payload.data()), static_cast<std::uint32_t>(payload.size()));
    
    auto egressListener = std::make_shared<MockEgressListener>();
    EgressAdapter adapter(egressListener, -clusterSessionId, m_subscription, 10);
    
    EXPECT_CALL(*egressListener, onAdminResponse(testing::_, testing::_, testing::_, testing::_, testing::_, testing::_, testing::_, testing::_)).Times(0);
    
    adapter.onFragment(m_buffer, static_cast<std::int32_t>(offset), static_cast<std::int32_t>(encoder.encodedLength()), m_header);
}

