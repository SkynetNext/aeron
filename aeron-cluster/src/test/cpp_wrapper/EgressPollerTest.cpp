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
#include "cluster/client/EgressPoller.h"
#include "Aeron.h"
#include "Subscription.h"
#include "concurrent/logbuffer/Header.h"
#include "concurrent/logbuffer/DataFrameHeader.h"
#include "concurrent/AtomicBuffer.h"
#include "generated/aeron_cluster_codecs/MessageHeader.h"
#include "generated/aeron_cluster_codecs/SessionMessageHeader.h"
#include "generated/aeron_archive_client/MessageHeader.h"
#include "generated/aeron_archive_client/ControlResponse.h"

// Undefine Windows ERROR macro to avoid conflict with ControlResponseCode::Value::ERROR
#ifdef ERROR
#undef ERROR
#endif

extern "C"
{
#include "aeron_image.h"
}

using namespace aeron;
using namespace aeron::cluster::client;
using namespace aeron::concurrent;
using namespace aeron::concurrent::logbuffer;
using namespace testing;

class EgressPollerTestFixture : public testing::Test
{
public:
    EgressPollerTestFixture() :
        m_bufferData(1024, 0),
        m_buffer(m_bufferData.data(), static_cast<util::index_t>(m_bufferData.size())),
        m_header(createTestHeader())
    {
        m_driver.start();
        
        Context ctx;
        m_aeron = Aeron::connect(ctx);
        
        // Create a subscription for testing (we only need the reference)
        std::int64_t subId = m_aeron->addSubscription("aeron:udp?endpoint=localhost:24325", 10);
        m_subscription = m_aeron->findSubscription(subId);
        
        // Wait for subscription to be available
        while (!m_subscription)
        {
            std::this_thread::yield();
            m_subscription = m_aeron->findSubscription(subId);
        }
        
        m_egressPoller = std::make_unique<EgressPoller>(*m_subscription, 10);
    }

    ~EgressPollerTestFixture() override
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
    std::unique_ptr<EgressPoller> m_egressPoller;
    std::vector<std::uint8_t> m_bufferData;
    AtomicBuffer m_buffer;
    Header m_header;
};

TEST_F(EgressPollerTestFixture, shouldIgnoreUnknownMessageSchema)
{
    const std::int32_t offset = 64;
    
    // Create a ControlResponse message from archive codec (unknown schema for cluster)
    aeron::archive::client::MessageHeader archiveMessageHeader;
    aeron::archive::client::ControlResponse controlResponseEncoder;
    
    // Encode archive message header
    archiveMessageHeader.wrap(reinterpret_cast<char *>(m_buffer.buffer()), offset, archiveMessageHeader.sbeSchemaVersion(), m_buffer.capacity());
    archiveMessageHeader.blockLength(controlResponseEncoder.sbeBlockLength());
    archiveMessageHeader.templateId(controlResponseEncoder.sbeTemplateId());
    archiveMessageHeader.schemaId(controlResponseEncoder.sbeSchemaId());
    archiveMessageHeader.version(controlResponseEncoder.sbeSchemaVersion());
    
    // Encode control response
    controlResponseEncoder.wrapForEncode(reinterpret_cast<char *>(m_buffer.buffer()), offset + archiveMessageHeader.encodedLength(), m_buffer.capacity());
    controlResponseEncoder.correlationId(42);
    controlResponseEncoder.controlSessionId(0);
    // Use static_cast to avoid Windows ERROR macro conflict
    controlResponseEncoder.code(static_cast<aeron::archive::client::ControlResponseCode::Value>(1));
    controlResponseEncoder.putErrorMessage("test", 4);
    
    const std::int32_t length = archiveMessageHeader.encodedLength() + controlResponseEncoder.encodedLength();
    
    const ControlledPollAction action = m_egressPoller->onFragment(m_buffer, offset, length, m_header);
    
    EXPECT_EQ(ControlledPollAction::CONTINUE, action);
    EXPECT_FALSE(m_egressPoller->isPollComplete());
}

TEST_F(EgressPollerTestFixture, shouldHandleSessionMessage)
{
    const std::int32_t offset = 16;
    const std::int64_t clusterSessionId = 7777;
    const std::int64_t leadershipTermId = 5;
    
    // Create SessionMessageHeader
    MessageHeader messageHeader;
    SessionMessageHeader encoder;
    
    // Encode message header
    messageHeader.wrap(reinterpret_cast<char *>(m_buffer.buffer()), offset, messageHeader.sbeSchemaVersion(), m_buffer.capacity());
    messageHeader.blockLength(encoder.sbeBlockLength());
    messageHeader.templateId(encoder.sbeTemplateId());
    messageHeader.schemaId(encoder.sbeSchemaId());
    messageHeader.version(encoder.sbeSchemaVersion());
    
    // Encode session message header
    encoder.wrapForEncode(reinterpret_cast<char *>(m_buffer.buffer()), offset + messageHeader.encodedLength(), m_buffer.capacity());
    encoder.clusterSessionId(clusterSessionId);
    encoder.leadershipTermId(leadershipTermId);
    
    const std::int32_t length = static_cast<std::int32_t>(messageHeader.encodedLength() + encoder.encodedLength());
    
    ControlledPollAction action = m_egressPoller->onFragment(m_buffer, offset, length, m_header);
    
    EXPECT_EQ(ControlledPollAction::BREAK, action);
    EXPECT_TRUE(m_egressPoller->isPollComplete());
    EXPECT_EQ(clusterSessionId, m_egressPoller->clusterSessionId());
    EXPECT_EQ(leadershipTermId, m_egressPoller->leadershipTermId());
    
    // Second call should return ABORT since isPollComplete is true
    action = m_egressPoller->onFragment(m_buffer, offset, length, m_header);
    EXPECT_EQ(ControlledPollAction::ABORT, action);
}
