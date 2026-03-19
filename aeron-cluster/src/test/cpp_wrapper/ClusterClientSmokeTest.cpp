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

/**
 * End-to-end smoke test: C++ AeronCluster client connecting to a running Java cluster.
 *
 * Prerequisites (not automated here — must be started externally before running this test):
 *   1. An Aeron MediaDriver on the default aeron directory (/dev/shm/aeron or tmp)
 *   2. A Java AeronCluster with an echo ClusteredService bound to ingress port 9010
 *      (single-node cluster; run io.aeron.cluster.ClusterNodeTest or equivalent)
 *
 * What this test verifies:
 *   - SessionConnectRequest reaches the cluster (correct SBE encoding, schemaId=111)
 *   - SessionEvent(OK) is received back (EgressPoller correctly decodes it)
 *   - Offer of a 4-byte message succeeds
 *   - Echo response is received via pollEgress
 *   - SessionClose is sent cleanly on close()
 *
 * To run manually (once XGuard allowlist issue is resolved):
 *   ./build/binaries/clusterClientSmokeTestW
 */

#include <gtest/gtest.h>

#include <chrono>
#include <thread>
#include <atomic>

#include "cluster/client/AeronCluster.h"
#include "cluster/client/EgressListener.h"
#include "concurrent/AtomicBuffer.h"

using namespace aeron;
using namespace aeron::cluster::client;
using namespace aeron::concurrent;

// ============================================================================
// Helpers
// ============================================================================

static constexpr int INGRESS_STREAM_ID = 101;
static constexpr int EGRESS_STREAM_ID  = 102;
static constexpr const char* INGRESS_CHANNEL  = "aeron:udp";
static constexpr const char* INGRESS_ENDPOINT = "0=localhost:9010";
static constexpr const char* EGRESS_CHANNEL   = "aeron:udp?endpoint=localhost:0";

static constexpr std::int64_t CONNECT_TIMEOUT_NS = 10'000'000'000LL; // 10 s
static constexpr std::int64_t POLL_TIMEOUT_NS    =  5'000'000'000LL; //  5 s

// Counts messages/events received from the cluster.
struct TestEgressListener : public EgressListener
{
    std::atomic<int> messageCount{0};
    std::atomic<int> newLeaderCount{0};
    std::int64_t lastSessionId = -1;

    void onMessage(
        std::int64_t clusterSessionId,
        std::int64_t /*timestamp*/,
        AtomicBuffer & /*buffer*/,
        util::index_t /*offset*/,
        util::index_t /*length*/,
        aeron::concurrent::logbuffer::Header & /*header*/) override
    {
        lastSessionId = clusterSessionId;
        messageCount.fetch_add(1, std::memory_order_relaxed);
    }

    void onNewLeader(
        std::int64_t /*clusterSessionId*/,
        std::int64_t /*leadershipTermId*/,
        std::int32_t /*leaderMemberId*/,
        const std::string & /*ingressEndpoints*/) override
    {
        newLeaderCount.fetch_add(1, std::memory_order_relaxed);
    }
};

// Poll egress until predicate returns true or timeout.
template<typename Pred>
static bool pollUntil(AeronCluster &cluster, Pred &&pred, std::int64_t timeoutNs)
{
    const auto deadline = std::chrono::steady_clock::now() +
        std::chrono::nanoseconds(timeoutNs);
    while (std::chrono::steady_clock::now() < deadline)
    {
        cluster.pollEgress();
        if (pred()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return false;
}

// ============================================================================
// Smoke test
// ============================================================================

// This test is DISABLED by default — it requires a live Java cluster.
// Enable by removing the DISABLED_ prefix and starting the Java cluster first.
TEST(ClusterClientSmokeTest, DISABLED_connectSendReceiveClose)
{
    auto listener = std::make_shared<TestEgressListener>();

    auto ctx = std::make_shared<AeronCluster::Context>();
    ctx->ingressChannel(INGRESS_CHANNEL)
        .ingressEndpoints(INGRESS_ENDPOINT)
        .ingressStreamId(INGRESS_STREAM_ID)
        .egressChannel(EGRESS_CHANNEL)
        .egressStreamId(EGRESS_STREAM_ID)
        .messageTimeoutNs(CONNECT_TIMEOUT_NS)
        .egressListener(listener)
        .ownsAeronClient(true);

    // --- Connect ---
    std::shared_ptr<AeronCluster> cluster;
    ASSERT_NO_THROW(cluster = AeronCluster::connect(ctx))
        << "Failed to connect to Java cluster on localhost:9010. "
           "Is the Java cluster running?";

    EXPECT_GT(cluster->clusterSessionId(), -1) << "Expected valid session id";
    EXPECT_GE(cluster->leadershipTermId(), 0);
    EXPECT_GE(cluster->leaderMemberId(), 0);

    // --- Offer a 4-byte message ---
    std::vector<std::uint8_t> payload = {0x01, 0x02, 0x03, 0x04};
    AtomicBuffer msg(payload.data(), static_cast<util::index_t>(payload.size()));

    std::int64_t offerResult = BACK_PRESSURED;
    const auto offerDeadline = std::chrono::steady_clock::now() +
        std::chrono::seconds(5);
    while (offerResult < 0 && std::chrono::steady_clock::now() < offerDeadline)
    {
        offerResult = cluster->offer(msg, 0, static_cast<std::int32_t>(payload.size()));
        if (offerResult == BACK_PRESSURED || offerResult == ADMIN_ACTION)
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    }
    EXPECT_GT(offerResult, 0) << "offer() failed with result=" << offerResult;

    // --- Receive echo response ---
    // The echo ClusteredService should reflect the message back.
    const bool received = pollUntil(
        *cluster,
        [&listener]() { return listener->messageCount.load() >= 1; },
        POLL_TIMEOUT_NS);
    EXPECT_TRUE(received) << "Timed out waiting for echo response";
    EXPECT_EQ(listener->lastSessionId, cluster->clusterSessionId());

    // --- Close cleanly ---
    ASSERT_NO_THROW(cluster->close());
    EXPECT_TRUE(cluster->isClosed());
}

// ============================================================================
// Static codec sanity checks (no cluster needed — always runs)
// ============================================================================

TEST(ClusterClientSmokeTest, codecSchemaIdMatchesJava)
{
    // Java: MessageHeaderDecoder.SCHEMA_ID = 111
    // C++: SessionMessageHeader::sbeSchemaId() = 111 (after schemaId fix)
    using namespace aeron::cluster::codecs;
    EXPECT_EQ(111, static_cast<int>(SessionMessageHeader::sbeSchemaId()))
        << "C++ schemaId must match Java SCHEMA_ID=111";
}

TEST(ClusterClientSmokeTest, codecTemplateIdsMatchXml)
{
    using namespace aeron::cluster::codecs;
    // Verified against aeron-cluster-codecs.xml
    EXPECT_EQ(1,  static_cast<int>(SessionMessageHeader::sbeTemplateId()));
    EXPECT_EQ(2,  static_cast<int>(SessionEvent::sbeTemplateId()));
    EXPECT_EQ(3,  static_cast<int>(SessionConnectRequest::sbeTemplateId()));
    EXPECT_EQ(4,  static_cast<int>(SessionCloseRequest::sbeTemplateId()));
    EXPECT_EQ(5,  static_cast<int>(SessionKeepAlive::sbeTemplateId()));
    EXPECT_EQ(6,  static_cast<int>(NewLeaderEvent::sbeTemplateId()));
}

TEST(ClusterClientSmokeTest, sessionHeaderLengthMatchesJava)
{
    // Java: AeronCluster.SESSION_HEADER_LENGTH =
    //   MessageHeaderEncoder.ENCODED_LENGTH + SessionMessageHeaderEncoder.BLOCK_LENGTH
    // Must equal C++ AeronCluster::SESSION_HEADER_LENGTH
    using namespace aeron::cluster::codecs;
    const int expected =
        static_cast<int>(MessageHeader::encodedLength()) +
        static_cast<int>(SessionMessageHeader::sbeBlockLength());
    EXPECT_EQ(AeronCluster::SESSION_HEADER_LENGTH, expected);
}
