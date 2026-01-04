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

#include "Publication.h"
#include "concurrent/AtomicBuffer.h"
#include "generated/aeron_cluster_client/MessageHeader.h"
#include "generated/aeron_cluster_client/SessionMessageHeader.h"
#include "Aeron.h"

namespace aeron { namespace cluster { namespace client
{

using namespace aeron::concurrent;

/**
 * Encapsulate applying a client message header for ingress to the cluster.
 * 
 * The client message header is applied to the Publication before the offered buffer.
 * 
 * Note: This class is NOT threadsafe for updating clusterSessionId() or
 * leadershipTermId(). Each publisher thread requires its own instance.
 */
class IngressSessionDecorator
{
public:
    /**
     * Length of the session header that will be prepended to the message.
     */
    static constexpr int HEADER_LENGTH =
        static_cast<int>(MessageHeader::encodedLength() + SessionMessageHeader::SBE_BLOCK_LENGTH);

    using this_t = IngressSessionDecorator;

    /**
     * Construct a new ingress session header wrapper that defaults all fields to Aeron::NULL_VALUE.
     */
    IngressSessionDecorator() :
        IngressSessionDecorator(aeron::NULL_VALUE, aeron::NULL_VALUE)
    {
    }

    /**
     * Construct a new session header wrapper.
     *
     * @param clusterSessionId that has been allocated by the cluster.
     * @param leadershipTermId of the current leader.
     */
    IngressSessionDecorator(std::int64_t clusterSessionId, std::int64_t leadershipTermId) :
        m_headerBufferData(HEADER_LENGTH),
        m_headerBuffer(m_headerBufferData.data(), m_headerBufferData.size())
    {
        SessionMessageHeader sessionMessageHeaderEncoder;
        MessageHeader messageHeaderEncoder;
        sessionMessageHeaderEncoder
            .wrapAndApplyHeader(m_headerBuffer, 0, messageHeaderEncoder)
            .leadershipTermId(leadershipTermId)
            .clusterSessionId(clusterSessionId)
            .timestamp(aeron::NULL_VALUE);
    }

    /**
     * Reset the cluster session id in the header.
     *
     * @param clusterSessionId to be set in the header.
     * @return this for a fluent API.
     */
    this_t& clusterSessionId(std::int64_t clusterSessionId)
    {
        // Re-wrap to update the session id
        SessionMessageHeader sessionMessageHeaderEncoder;
        MessageHeader messageHeaderEncoder;
        sessionMessageHeaderEncoder.wrap(m_headerBuffer, 0, messageHeaderEncoder);
        sessionMessageHeaderEncoder.clusterSessionId(clusterSessionId);
        return *this;
    }

    /**
     * Reset the leadership term id in the header.
     *
     * @param leadershipTermId to be set in the header.
     * @return this for a fluent API.
     */
    this_t& leadershipTermId(std::int64_t leadershipTermId)
    {
        // Re-wrap to update the leadership term id
        SessionMessageHeader sessionMessageHeaderEncoder;
        MessageHeader messageHeaderEncoder;
        sessionMessageHeaderEncoder.wrap(m_headerBuffer, 0, messageHeaderEncoder);
        sessionMessageHeaderEncoder.leadershipTermId(leadershipTermId);
        return *this;
    }

    /**
     * Non-blocking publish of a partial buffer containing a message plus session header to a cluster.
     * 
     * This version of the method will set the timestamp value in the header to Aeron::NULL_VALUE.
     *
     * @param publication to be offered to.
     * @param buffer      containing message.
     * @param offset      offset in the buffer at which the encoded message begins.
     * @param length      in bytes of the encoded message.
     * @return the same as Publication::offer(AtomicBuffer, int, int).
     */
    std::int64_t offer(
        std::shared_ptr<Publication> publication,
        AtomicBuffer &buffer,
        std::int32_t offset,
        std::int32_t length)
    {
        return publication->offer(m_headerBuffer, 0, HEADER_LENGTH, buffer, offset, length);
    }

private:
    std::vector<std::uint8_t> m_headerBufferData;
    AtomicBuffer m_headerBuffer;
};

}}}

