#pragma once
#include <string>
#include <vector>
#include <memory>

#include "Aeron.h"
#include "Publication.h"
#include "concurrent/logbuffer/BufferClaim.h"
#include "concurrent/AtomicBuffer.h"
#include "ClientSession.h"
#include "cluster/client/ClusterExceptions.h"
#include "util/Exceptions.h"

namespace aeron { namespace cluster { namespace service
{
using namespace aeron::concurrent;
using namespace aeron::concurrent::logbuffer;
using namespace aeron::util;

class ClusteredServiceAgent; // Forward declaration

class ContainerClientSession : public ClientSession
{
public:
    ContainerClientSession(
        std::int64_t sessionId,
        std::int32_t responseStreamId,
        const std::string& responseChannel,
        const std::vector<std::uint8_t>& encodedPrincipal,
        ClusteredServiceAgent& clusteredServiceAgent);

    std::int64_t id() override;
    std::int32_t responseStreamId() override;
    std::string responseChannel() override;
    std::vector<std::uint8_t> encodedPrincipal() override;
    void close() override;
    bool isClosing() override;
    std::int64_t offer(AtomicBuffer& buffer, std::int32_t offset, std::int32_t length) override;
    // Note: C++ does not have DirectBufferVector equivalent directly, needs adaptation or alternative.
    std::int64_t tryClaim(std::int32_t length, BufferClaim& bufferClaim) override;

    void connect(std::shared_ptr<Aeron> aeron);
    void markClosing();
    void resetClosing();
    void disconnect(const exception_handler_t& errorHandler);

    std::string toString() const;

private:
    std::int64_t m_id;
    std::int32_t m_responseStreamId;
    std::string m_responseChannel;
    std::vector<std::uint8_t> m_encodedPrincipal;

    ClusteredServiceAgent& m_clusteredServiceAgent;
    std::shared_ptr<Publication> m_responsePublication;
    bool m_isClosing = false;
};

}}}

