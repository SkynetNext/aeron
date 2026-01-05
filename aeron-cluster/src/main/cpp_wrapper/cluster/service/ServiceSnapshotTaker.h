#pragma once
#include <memory>
#include <string>
#include <vector>

#include "SnapshotTaker.h"
#include "ClientSession.h"
#include "ExclusivePublication.h"
#include "concurrent/AtomicBuffer.h"
#include "concurrent/IdleStrategy.h"
#include "concurrent/AgentInvoker.h"
#include "generated/aeron_cluster_client/MessageHeader.h"
#include "generated/aeron_cluster_client/ClientSession.h"

namespace aeron { namespace cluster { namespace service
{
using namespace aeron::concurrent;
using namespace aeron::cluster::codecs;

class ServiceSnapshotTaker : public SnapshotTaker
{
public:
    ServiceSnapshotTaker(
        std::shared_ptr<ExclusivePublication> publication,
        std::shared_ptr<IdleStrategy> idleStrategy,
        std::shared_ptr<void> aeronClientInvoker);

    void snapshotSession(ClientSession& session);

private:
    void encodeSession(
        ClientSession& session,
        const std::string& responseChannel,
        const std::vector<std::uint8_t>& encodedPrincipal,
        AtomicBuffer& buffer,
        std::int32_t offset);

    std::vector<std::uint8_t> m_offerBufferData;
    AtomicBuffer m_offerBuffer;
    ClientSessionEncoder m_clientSessionEncoder;
};

}}}

