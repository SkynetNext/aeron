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

#include <string>
#include <vector>
#include "Publication.h"
#include "concurrent/logbuffer/BufferClaim.h"
#include "concurrent/AtomicBuffer.h"
#include "client/AeronCluster.h"

namespace aeron { namespace cluster { namespace service
{

using namespace aeron::concurrent;
using namespace aeron::concurrent::logbuffer;

/**
 * Session representing a connected external client to the cluster.
 */
class ClientSession
{
public:
    /**
     * Return value to indicate egress to a session is mocked out by the cluster when in follower mode.
     */
    static constexpr std::int64_t MOCKED_OFFER = 1;

    virtual ~ClientSession() = default;

    /**
     * Cluster session identity uniquely allocated when the session was opened.
     *
     * @return the cluster session identity uniquely allocated when the session was opened.
     */
    virtual std::int64_t id() = 0;

    /**
     * The response channel stream id for responding to the client.
     *
     * @return response channel stream id for responding to the client.
     */
    virtual std::int32_t responseStreamId() = 0;

    /**
     * The response channel for responding to the client.
     *
     * @return response channel for responding to the client.
     */
    virtual std::string responseChannel() = 0;

    /**
     * Cluster session encoded principal from when the session was authenticated.
     *
     * @return The encoded Principal passed. May be 0 length to indicate none present.
     */
    virtual std::vector<std::uint8_t> encodedPrincipal() = 0;

    /**
     * Close of this ContainerClientSession by sending the request to the consensus module.
     * 
     * This method is idempotent.
     */
    virtual void close() = 0;

    /**
     * Indicates that a request to close this session has been made.
     *
     * @return whether a request to close this session has been made.
     */
    virtual bool isClosing() = 0;

    /**
     * Non-blocking publish of a partial buffer containing a message to a cluster.
     *
     * @param buffer containing message.
     * @param offset offset in the buffer at which the encoded message begins.
     * @param length in bytes of the encoded message.
     * @return the same as Publication::offer(AtomicBuffer, int, int) when in Cluster::Role::LEADER,
     * otherwise MOCKED_OFFER when a follower.
     */
    virtual std::int64_t offer(AtomicBuffer& buffer, std::int32_t offset, std::int32_t length) = 0;

    /**
     * Try to claim a range in the publication log into which a message can be written with zero copy semantics.
     * Once the message has been written then BufferClaim::commit() should be called thus making it available.
     * 
     * On successful claim, the Cluster egress header will be written to the start of the claimed buffer section.
     * Clients MUST write into the claimed buffer region at offset + AeronCluster::SESSION_HEADER_LENGTH.
     *
     * @param length      of the range to claim in bytes. The additional bytes for the session header will be added.
     * @param bufferClaim to be populated if the claim succeeds.
     * @return The new stream position, otherwise a negative error value as specified in
     *         Publication::tryClaim(int, BufferClaim) when in Cluster::Role::LEADER,
     *         otherwise MOCKED_OFFER when a follower.
     */
    virtual std::int64_t tryClaim(std::int32_t length, BufferClaim& bufferClaim) = 0;
};

}}}

