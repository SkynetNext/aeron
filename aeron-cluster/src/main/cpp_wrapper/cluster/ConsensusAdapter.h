#pragma once
#include <memory>
#include <vector>
#include <string>
#include "Subscription.h"
#include "archive/client/AeronArchive.h"
#include "../client/ClusterExceptions.h"
#include "FragmentAssembler.h"
#include "concurrent/logbuffer/Header.h"
#include "concurrent/AtomicBuffer.h"
#include "util/CloseHelper.h"
#include "generated/aeron_cluster_client/MessageHeader.h"
#include "generated/aeron_cluster_client/CanvassPosition.h"
#include "generated/aeron_cluster_client/RequestVote.h"
#include "generated/aeron_cluster_client/Vote.h"
#include "generated/aeron_cluster_client/NewLeadershipTerm.h"
#include "generated/aeron_cluster_client/AppendPosition.h"
#include "generated/aeron_cluster_client/CommitPosition.h"
#include "generated/aeron_cluster_client/CatchupPosition.h"
#include "generated/aeron_cluster_client/StopCatchup.h"
#include "generated/aeron_cluster_client/TerminationPosition.h"
#include "generated/aeron_cluster_client/TerminationAck.h"
#include "generated/aeron_cluster_client/BackupQuery.h"
#include "generated/aeron_cluster_client/ChallengeResponse.h"
#include "generated/aeron_cluster_client/HeartbeatRequest.h"
#include "generated/aeron_cluster_client/StandbySnapshot.h"
#include "generated/aeron_cluster_client/BooleanType.h"
#include "StandbySnapshotEntry.h"

namespace aeron { namespace cluster
{
using namespace aeron::archive::client;
using namespace aeron::concurrent;
using namespace aeron::concurrent::logbuffer;
using namespace aeron::cluster::codecs;
using namespace aeron::util;

class ConsensusModuleAgent; // Forward declaration

class ConsensusAdapter
{
public:
    static constexpr std::int32_t FRAGMENT_LIMIT = 10;

    ConsensusAdapter(
        std::shared_ptr<Subscription> subscription,
        ConsensusModuleAgent& consensusModuleAgent);

    ~ConsensusAdapter() = default; // AutoCloseable equivalent

    void close();

    std::int32_t poll();

    std::int32_t poll(std::int32_t limit);

    void onFragment(
        AtomicBuffer& buffer,
        std::int32_t offset,
        std::int32_t length,
        Header& header);

private:
    MessageHeaderDecoder m_messageHeaderDecoder;
    CanvassPositionDecoder m_canvassPositionDecoder;
    RequestVoteDecoder m_requestVoteDecoder;
    VoteDecoder m_voteDecoder;
    NewLeadershipTermDecoder m_newLeadershipTermDecoder;
    AppendPositionDecoder m_appendPositionDecoder;
    CommitPositionDecoder m_commitPositionDecoder;
    CatchupPositionDecoder m_catchupPositionDecoder;
    StopCatchupDecoder m_stopCatchupDecoder;
    TerminationPositionDecoder m_terminationPositionDecoder;
    TerminationAckDecoder m_terminationAckDecoder;
    BackupQueryDecoder m_backupQueryDecoder;
    ChallengeResponseDecoder m_challengeResponseDecoder;
    HeartbeatRequestDecoder m_heartbeatRequestDecoder;
    StandbySnapshotDecoder m_standbySnapshotDecoder;
    FragmentAssembler m_fragmentAssembler;
    std::shared_ptr<Subscription> m_subscription;
    ConsensusModuleAgent& m_consensusModuleAgent;
};

// Implementation
inline ConsensusAdapter::ConsensusAdapter(
    std::shared_ptr<Subscription> subscription,
    ConsensusModuleAgent& consensusModuleAgent) :
    m_fragmentAssembler([this](AtomicBuffer& buffer, std::int32_t offset, std::int32_t length, Header& header)
    {
        this->onFragment(buffer, offset, length, header);
    }),
    m_subscription(subscription),
    m_consensusModuleAgent(consensusModuleAgent)
{
}

inline void ConsensusAdapter::close()
{
    CloseHelper::close(m_subscription);
}

inline std::int32_t ConsensusAdapter::poll()
{
    return m_subscription->poll(m_fragmentAssembler.handler(), FRAGMENT_LIMIT);
}

inline std::int32_t ConsensusAdapter::poll(std::int32_t limit)
{
    return m_subscription->poll(m_fragmentAssembler.handler(), limit);
}

inline void ConsensusAdapter::onFragment(
    AtomicBuffer& buffer,
    std::int32_t offset,
    std::int32_t length,
    Header& header)
{
    m_messageHeaderDecoder.wrap(buffer, offset);

    const std::int32_t schemaId = m_messageHeaderDecoder.sbeSchemaId();
    if (schemaId != MessageHeader::sbeSchemaId())
    {
        throw ClusterException(
            "expected schemaId=" + std::to_string(MessageHeader::sbeSchemaId()) + ", actual=" + std::to_string(schemaId),
            SOURCEINFO);
    }

    switch (m_messageHeaderDecoder.sbeTemplateId())
    {
        case CanvassPosition::sbeTemplateId():
            m_canvassPositionDecoder.wrap(
                buffer,
                offset + MessageHeader::encodedLength(),
                m_messageHeaderDecoder.sbeBlockLength(),
                m_messageHeaderDecoder.sbeVersion());

            m_consensusModuleAgent.onCanvassPosition(
                m_canvassPositionDecoder.logLeadershipTermId(),
                m_canvassPositionDecoder.logPosition(),
                m_canvassPositionDecoder.leadershipTermId(),
                m_canvassPositionDecoder.followerMemberId(),
                m_canvassPositionDecoder.protocolVersion());
            break;

        case RequestVote::sbeTemplateId():
            m_requestVoteDecoder.wrap(
                buffer,
                offset + MessageHeader::encodedLength(),
                m_messageHeaderDecoder.sbeBlockLength(),
                m_messageHeaderDecoder.sbeVersion());

            m_consensusModuleAgent.onRequestVote(
                m_requestVoteDecoder.logLeadershipTermId(),
                m_requestVoteDecoder.logPosition(),
                m_requestVoteDecoder.candidateTermId(),
                m_requestVoteDecoder.candidateMemberId(),
                m_requestVoteDecoder.protocolVersion());
            break;

        case Vote::sbeTemplateId():
            m_voteDecoder.wrap(
                buffer,
                offset + MessageHeader::encodedLength(),
                m_messageHeaderDecoder.sbeBlockLength(),
                m_messageHeaderDecoder.sbeVersion());

            m_consensusModuleAgent.onVote(
                m_voteDecoder.candidateTermId(),
                m_voteDecoder.logLeadershipTermId(),
                m_voteDecoder.logPosition(),
                m_voteDecoder.candidateMemberId(),
                m_voteDecoder.followerMemberId(),
                m_voteDecoder.vote() == BooleanType::TRUE);
            break;

        case NewLeadershipTerm::sbeTemplateId():
            m_newLeadershipTermDecoder.wrap(
                buffer,
                offset + MessageHeader::encodedLength(),
                m_messageHeaderDecoder.sbeBlockLength(),
                m_messageHeaderDecoder.sbeVersion());

            {
                const std::int64_t rawCommitPosition = m_newLeadershipTermDecoder.commitPosition();
                const std::int64_t commitPosition =
                    m_newLeadershipTermDecoder.commitPositionNullValue() == rawCommitPosition ?
                        AeronArchive::NULL_POSITION : rawCommitPosition;

                m_consensusModuleAgent.onNewLeadershipTerm(
                    m_newLeadershipTermDecoder.logLeadershipTermId(),
                    m_newLeadershipTermDecoder.nextLeadershipTermId(),
                    m_newLeadershipTermDecoder.nextTermBaseLogPosition(),
                    m_newLeadershipTermDecoder.nextLogPosition(),
                    m_newLeadershipTermDecoder.leadershipTermId(),
                    m_newLeadershipTermDecoder.termBaseLogPosition(),
                    m_newLeadershipTermDecoder.logPosition(),
                    commitPosition,
                    m_newLeadershipTermDecoder.leaderRecordingId(),
                    m_newLeadershipTermDecoder.timestamp(),
                    m_newLeadershipTermDecoder.leaderMemberId(),
                    m_newLeadershipTermDecoder.logSessionId(),
                    m_newLeadershipTermDecoder.appVersion(),
                    m_newLeadershipTermDecoder.isStartup() == BooleanType::TRUE);
            }
            break;

        case AppendPosition::sbeTemplateId():
            m_appendPositionDecoder.wrap(
                buffer,
                offset + MessageHeader::encodedLength(),
                m_messageHeaderDecoder.sbeBlockLength(),
                m_messageHeaderDecoder.sbeVersion());

            {
                const std::int16_t flagsDecodedValue = m_appendPositionDecoder.flags();
                const std::int16_t flags = m_appendPositionDecoder.flagsNullValue() == flagsDecodedValue ?
                    0 : flagsDecodedValue; // APPEND_POSITION_FLAG_NONE = 0

                m_consensusModuleAgent.onAppendPosition(
                    m_appendPositionDecoder.leadershipTermId(),
                    m_appendPositionDecoder.logPosition(),
                    m_appendPositionDecoder.followerMemberId(),
                    flags);
            }
            break;

        case CommitPosition::sbeTemplateId():
            m_commitPositionDecoder.wrap(
                buffer,
                offset + MessageHeader::encodedLength(),
                m_messageHeaderDecoder.sbeBlockLength(),
                m_messageHeaderDecoder.sbeVersion());

            m_consensusModuleAgent.onCommitPosition(
                m_commitPositionDecoder.leadershipTermId(),
                m_commitPositionDecoder.logPosition(),
                m_commitPositionDecoder.leaderMemberId());
            break;

        case CatchupPosition::sbeTemplateId():
            m_catchupPositionDecoder.wrap(
                buffer,
                offset + MessageHeader::encodedLength(),
                m_messageHeaderDecoder.sbeBlockLength(),
                m_messageHeaderDecoder.sbeVersion());

            m_consensusModuleAgent.onCatchupPosition(
                m_catchupPositionDecoder.leadershipTermId(),
                m_catchupPositionDecoder.logPosition(),
                m_catchupPositionDecoder.followerMemberId(),
                m_catchupPositionDecoder.catchupEndpoint());
            break;

        case StopCatchup::sbeTemplateId():
            m_stopCatchupDecoder.wrap(
                buffer,
                offset + MessageHeader::encodedLength(),
                m_messageHeaderDecoder.sbeBlockLength(),
                m_messageHeaderDecoder.sbeVersion());

            m_consensusModuleAgent.onStopCatchup(
                m_stopCatchupDecoder.leadershipTermId(),
                m_stopCatchupDecoder.followerMemberId());
            break;

        case TerminationPosition::sbeTemplateId():
            m_terminationPositionDecoder.wrap(
                buffer,
                offset + MessageHeader::encodedLength(),
                m_messageHeaderDecoder.sbeBlockLength(),
                m_messageHeaderDecoder.sbeVersion());

            m_consensusModuleAgent.onTerminationPosition(
                m_terminationPositionDecoder.leadershipTermId(),
                m_terminationPositionDecoder.logPosition());
            break;

        case TerminationAck::sbeTemplateId():
            m_terminationAckDecoder.wrap(
                buffer,
                offset + MessageHeader::encodedLength(),
                m_messageHeaderDecoder.sbeBlockLength(),
                m_messageHeaderDecoder.sbeVersion());

            m_consensusModuleAgent.onTerminationAck(
                m_terminationAckDecoder.leadershipTermId(),
                m_terminationAckDecoder.logPosition(),
                m_terminationAckDecoder.memberId());
            break;

        case BackupQuery::sbeTemplateId():
        {
            m_backupQueryDecoder.wrap(
                buffer,
                offset + MessageHeader::encodedLength(),
                m_messageHeaderDecoder.sbeBlockLength(),
                m_messageHeaderDecoder.sbeVersion());

            const std::string responseChannel = m_backupQueryDecoder.responseChannel();
            const std::int32_t credentialsLength = m_backupQueryDecoder.encodedCredentialsLength();
            std::vector<std::uint8_t> credentials;
            
            if (credentialsLength > 0)
            {
                credentials.resize(credentialsLength);
                m_backupQueryDecoder.getEncodedCredentials(credentials.data(), 0, credentials.size());
            }

            m_consensusModuleAgent.onBackupQuery(
                m_backupQueryDecoder.correlationId(),
                m_backupQueryDecoder.responseStreamId(),
                m_backupQueryDecoder.version(),
                responseChannel,
                credentials,
                header);
            break;
        }

        case ChallengeResponse::sbeTemplateId():
        {
            m_challengeResponseDecoder.wrap(
                buffer,
                offset + MessageHeader::encodedLength(),
                m_messageHeaderDecoder.sbeBlockLength(),
                m_messageHeaderDecoder.sbeVersion());

            std::vector<std::uint8_t> credentials(m_challengeResponseDecoder.encodedCredentialsLength());
            m_challengeResponseDecoder.getEncodedCredentials(credentials.data(), 0, credentials.size());

            m_consensusModuleAgent.onConsensusChallengeResponse(
                m_challengeResponseDecoder.correlationId(),
                m_challengeResponseDecoder.clusterSessionId(),
                credentials);
            break;
        }

        case HeartbeatRequest::sbeTemplateId():
        {
            m_heartbeatRequestDecoder.wrap(
                buffer,
                offset + MessageHeader::encodedLength(),
                m_messageHeaderDecoder.sbeBlockLength(),
                m_messageHeaderDecoder.sbeVersion());

            const std::string responseChannel = m_heartbeatRequestDecoder.responseChannel();
            const std::int32_t credentialsLength = m_heartbeatRequestDecoder.encodedCredentialsLength();
            std::vector<std::uint8_t> credentials;
            
            if (credentialsLength > 0)
            {
                credentials.resize(credentialsLength);
                m_heartbeatRequestDecoder.getEncodedCredentials(credentials.data(), 0, credentials.size());
            }
            else
            {
                m_heartbeatRequestDecoder.skipEncodedCredentials();
            }

            m_consensusModuleAgent.onHeartbeatRequest(
                m_heartbeatRequestDecoder.correlationId(),
                m_heartbeatRequestDecoder.responseStreamId(),
                responseChannel,
                credentials,
                header);
            break;
        }

        case StandbySnapshot::sbeTemplateId():
        {
            m_standbySnapshotDecoder.wrap(
                buffer,
                offset + MessageHeader::encodedLength(),
                m_messageHeaderDecoder.sbeBlockLength(),
                m_messageHeaderDecoder.sbeVersion());

            const std::int64_t correlationId = m_standbySnapshotDecoder.correlationId();
            const std::int32_t version = m_standbySnapshotDecoder.version();
            const std::int32_t responseStreamId = m_standbySnapshotDecoder.responseStreamId();
            std::vector<StandbySnapshotEntry> standbySnapshotEntries;

            for (const auto& standbySnapshot : m_standbySnapshotDecoder.snapshots())
            {
                standbySnapshotEntries.emplace_back(
                    standbySnapshot.recordingId(),
                    standbySnapshot.leadershipTermId(),
                    standbySnapshot.termBaseLogPosition(),
                    standbySnapshot.logPosition(),
                    standbySnapshot.timestamp(),
                    standbySnapshot.serviceId(),
                    standbySnapshot.archiveEndpoint());
            }

            const std::string responseChannel = m_standbySnapshotDecoder.responseChannel();
            std::vector<std::uint8_t> encodedCredentials;
            
            if (0 == m_standbySnapshotDecoder.encodedCredentialsLength())
            {
                m_standbySnapshotDecoder.skipEncodedCredentials();
            }
            else
            {
                encodedCredentials.resize(m_standbySnapshotDecoder.encodedCredentialsLength());
                m_standbySnapshotDecoder.getEncodedCredentials(encodedCredentials.data(), 0, encodedCredentials.size());
            }

            m_consensusModuleAgent.onStandbySnapshot(
                correlationId,
                version,
                standbySnapshotEntries,
                responseStreamId,
                responseChannel,
                encodedCredentials,
                header);
            break;
        }
    }
}

}}

