#pragma once

#include "Aeron.h"
#include "ChannelUri.h"
#include "ExclusivePublication.h"
#include <algorithm>
#include <chrono>
#include <cstdint>
#include <memory>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

// CommonContext constants are available through ChannelUri.h
#include "client/ClusterEvent.h"
#include "client/ClusterExceptions.h"
#include "client/archive/AeronArchive.h"
#include "util/CloseHelper.h"
#include "util/Exceptions.h"

namespace aeron {
namespace cluster {

using namespace aeron::util;
using namespace aeron::cluster::client; // For ClusterException

/**
 * Represents a member of the cluster that participates in consensus for storing
 * state from the perspective of any single member.
 */
class ClusterMember {
public:
  static std::vector<ClusterMember> EMPTY_MEMBERS;

  /**
   * Construct a new member of the cluster.
   */
  ClusterMember(std::int32_t id, const std::string &ingressEndpoint,
                const std::string &consensusEndpoint,
                const std::string &logEndpoint,
                const std::string &catchupEndpoint,
                const std::string &archiveEndpoint,
                const std::string &endpoints);

  /**
   * Construct a new member of the cluster with additional endpoints.
   */
  ClusterMember(std::int32_t id, const std::string &ingressEndpoint,
                const std::string &consensusEndpoint,
                const std::string &logEndpoint,
                const std::string &catchupEndpoint,
                const std::string &archiveEndpoint,
                const std::string &archiveResponseEndpoint,
                const std::string &egressResponseEndpoint,
                const std::string &endpoints);

  /**
   * Reset the state of a cluster member, so it can be canvassed and
   * reestablished.
   */
  void reset();

  ClusterMember &isLeader(bool isLeader);
  bool isLeader() const;

  ClusterMember &isBallotSent(bool isBallotSent);
  bool isBallotSent() const;

  ClusterMember &hasTerminated(bool hasTerminated);
  bool hasTerminated() const;

  ClusterMember &id(std::int32_t id);
  std::int32_t id() const;

  ClusterMember &vote(std::shared_ptr<bool> vote);
  std::shared_ptr<bool> vote() const;

  ClusterMember &leadershipTermId(std::int64_t leadershipTermId);
  std::int64_t leadershipTermId() const;

  ClusterMember &logPosition(std::int64_t logPosition);
  std::int64_t logPosition() const;

  ClusterMember &candidateTermId(std::int64_t candidateTermId);
  std::int64_t candidateTermId() const;

  ClusterMember &catchupReplaySessionId(std::int64_t replaySessionId);
  std::int64_t catchupReplaySessionId() const;

  ClusterMember &catchupReplayCorrelationId(std::int64_t correlationId);
  std::int64_t catchupReplayCorrelationId() const;

  ClusterMember &correlationId(std::int64_t correlationId);
  std::int64_t correlationId() const;

  ClusterMember &timeOfLastAppendPositionNs(std::int64_t timeNs);
  std::int64_t timeOfLastAppendPositionNs() const;

  std::string consensusEndpoint() const;
  std::string ingressEndpoint() const;
  std::string logEndpoint() const;
  std::string catchupEndpoint() const;
  std::string archiveEndpoint() const;
  std::string archiveResponseEndpoint() const;
  std::string egressResponseEndpoint() const;
  std::string endpoints() const;

  std::shared_ptr<ExclusivePublication> publication() const;
  void publication(std::shared_ptr<ExclusivePublication> publication);

  void closePublication(const exception_handler_t &errorHandler);

  /**
   * Parse the details for cluster members from a string.
   */
  static std::vector<ClusterMember> parse(const std::string &value);

  /**
   * Parse a string containing the endpoints for a cluster node.
   */
  static ClusterMember parseEndpoints(std::int32_t id,
                                      const std::string &endpoints);

  /**
   * Encode member endpoints from a cluster members array to a String.
   */
  static std::string
  encodeAsString(const std::vector<ClusterMember> &clusterMembers);

  /**
   * Copy votes from one array of members to another where the ids match.
   */
  static void copyVotes(const std::vector<ClusterMember> &srcMembers,
                        std::vector<ClusterMember> &dstMembers);

  /**
   * Add the publications for sending consensus messages to the other members of
   * the cluster.
   */
  static void addConsensusPublications(std::vector<ClusterMember> &members,
                                       const ClusterMember &thisMember,
                                       const std::string &channelTemplate,
                                       std::int32_t streamId,
                                       bool bindConsensusControl,
                                       std::shared_ptr<Aeron> aeron,
                                       const exception_handler_t &errorHandler);

  /**
   * Add an exclusive Publication for communicating to a member on the consensus
   * channel.
   */
  static void addConsensusPublication(const ClusterMember &thisMember,
                                      ClusterMember &otherMember,
                                      const std::string &channelTemplate,
                                      std::int32_t streamId,
                                      bool bindConsensusControl,
                                      std::shared_ptr<Aeron> aeron,
                                      const exception_handler_t &errorHandler);

  /**
   * Try and add an exclusive Publication for communicating to a member on the
   * consensus channel.
   */
  static void tryAddPublication(ClusterMember &member, std::int32_t streamId,
                                std::shared_ptr<Aeron> aeron,
                                const exception_handler_t &errorHandler);

  /**
   * Close the publications associated with members of the cluster used for the
   * consensus protocol.
   */
  static void
  closeConsensusPublications(const exception_handler_t &errorHandler,
                             std::vector<ClusterMember> &clusterMembers);

  /**
   * Populate map of ClusterMembers which can be looked up by id.
   */
  static void addClusterMemberIds(
      std::vector<ClusterMember> &clusterMembers,
      std::unordered_map<std::int32_t, ClusterMember *> &clusterMemberByIdMap);

  /**
   * Check if the cluster leader has an active quorum of cluster followers.
   */
  static bool hasActiveQuorum(const std::vector<ClusterMember> &clusterMembers,
                              std::int64_t nowNs, std::int64_t timeoutNs);

  /**
   * The threshold of clusters members required to achieve quorum given a count
   * of cluster members.
   */
  static std::int32_t quorumThreshold(std::int32_t memberCount);

  /**
   * Calculate the position reached by a quorum of cluster members.
   */
  static std::int64_t
  quorumPosition(const std::vector<ClusterMember> &members,
                 std::vector<std::int64_t> &rankedPositions);

  /**
   * Reset the log position of all the members to the provided value.
   */
  static void resetLogPositions(std::vector<ClusterMember> &clusterMembers,
                                std::int64_t logPosition);

  /**
   * Has the voting members of a cluster arrived at provided position in their
   * log.
   */
  static bool
  hasVotersAtPosition(const std::vector<ClusterMember> &clusterMembers,
                      std::int64_t position, std::int64_t leadershipTermId);

  /**
   * Has a quorum of members of appended a position to their local log.
   */
  static bool
  hasQuorumAtPosition(const std::vector<ClusterMember> &clusterMembers,
                      std::int64_t position, std::int64_t leadershipTermId);

  /**
   * Reset the state of all cluster members.
   */
  static void reset(std::vector<ClusterMember> &members);

  /**
   * Become a candidate by voting for yourself and resetting the other votes.
   */
  static void becomeCandidate(std::vector<ClusterMember> &members,
                              std::int64_t candidateTermId,
                              std::int32_t candidateMemberId);

  /**
   * Is a member considered unanimously to be leader after voting.
   */
  static bool
  isUnanimousLeader(const std::vector<ClusterMember> &clusterMembers,
                    std::int64_t candidateTermId,
                    std::int32_t gracefulClosedLeaderId);

  /**
   * Is this member considered leader by a quorum of members.
   */
  static bool isQuorumLeader(const std::vector<ClusterMember> &clusterMembers,
                             std::int64_t candidateTermId);

  /**
   * Determine which member of a cluster this is and check endpoints.
   */
  static ClusterMember
  determineMember(const std::vector<ClusterMember> &clusterMembers,
                  std::int32_t memberId, const std::string &memberEndpoints);

  /**
   * Check the member with the memberEndpoints.
   */
  static void validateMemberEndpoints(const ClusterMember &member,
                                      const std::string &memberEndpoints);

  /**
   * Are two cluster members using the same endpoints?
   */
  static bool areSameEndpoints(const ClusterMember &lhs,
                               const ClusterMember &rhs);

  /**
   * Is the member considered a candidate by a unanimous view.
   */
  static bool
  isUnanimousCandidate(const std::vector<ClusterMember> &clusterMembers,
                       const ClusterMember &candidate,
                       std::int32_t gracefulClosedLeaderId);

  /**
   * Has the member achieved a quorum view to be a suitable candidate.
   */
  static bool
  isQuorumCandidate(const std::vector<ClusterMember> &clusterMembers,
                    const ClusterMember &candidate);

  /**
   * Compare two log positions.
   */
  static std::int32_t compareLog(std::int64_t lhsLogLeadershipTermId,
                                 std::int64_t lhsLogPosition,
                                 std::int64_t rhsLogLeadershipTermId,
                                 std::int64_t rhsLogPosition);

  /**
   * Compare two members' logs.
   */
  static std::int32_t compareLog(const ClusterMember &lhs,
                                 const ClusterMember &rhs);

  /**
   * Find a ClusterMember with a given id.
   */
  static ClusterMember *findMember(std::vector<ClusterMember> &clusterMembers,
                                   std::int32_t memberId);

  /**
   * Create a string of ingress endpoints by member id.
   */
  static std::string
  ingressEndpoints(const std::vector<ClusterMember> &members);

  /**
   * Run through the list of cluster members and set the isLeader field.
   */
  static void setIsLeader(std::vector<ClusterMember> &clusterMembers,
                          std::int32_t leaderMemberId);

  std::string toString() const;

private:
  static void setControlEndpoint(ChannelUri &channelUri, bool shouldBind,
                                 const std::string &endpoint);

  static std::string replacePortWithWildcard(const std::string &endpoint);

  static void parseMember(const std::string &idAndEndpoints,
                          std::vector<ClusterMember> &members);

  bool m_isBallotSent = false;
  bool m_isLeader = false;
  bool m_hasTerminated = false;
  std::int32_t m_id;
  std::int64_t m_leadershipTermId = 0;           // NULL_VALUE
  std::int64_t m_candidateTermId = 0;            // NULL_VALUE
  std::int64_t m_catchupReplaySessionId = 0;     // NULL_VALUE
  std::int64_t m_catchupReplayCorrelationId = 0; // NULL_VALUE
  std::int64_t m_changeCorrelationId = 0;        // NULL_VALUE
  std::int64_t m_logPosition = 0;                // NULL_POSITION
  std::int64_t m_timeOfLastAppendPositionNs = 0; // NULL_VALUE
  std::shared_ptr<ExclusivePublication> m_publication;
  std::string m_consensusChannel;
  std::string m_consensusEndpoint;
  std::string m_ingressEndpoint;
  std::string m_logEndpoint;
  std::string m_catchupEndpoint;
  std::string m_archiveEndpoint;
  std::string m_archiveResponseEndpoint;
  std::string m_egressResponseEndpoint;
  std::string m_endpoints;
  std::shared_ptr<bool> m_vote;
};

// Inline implementations for header-only library

inline ClusterMember::ClusterMember(std::int32_t id,
                                    const std::string &ingressEndpoint,
                                    const std::string &consensusEndpoint,
                                    const std::string &logEndpoint,
                                    const std::string &catchupEndpoint,
                                    const std::string &archiveEndpoint,
                                    const std::string &endpoints)
    : ClusterMember(id, ingressEndpoint, consensusEndpoint, logEndpoint,
                    catchupEndpoint, archiveEndpoint, "", "", endpoints) {}

inline ClusterMember::ClusterMember(
    std::int32_t id, const std::string &ingressEndpoint,
    const std::string &consensusEndpoint, const std::string &logEndpoint,
    const std::string &catchupEndpoint, const std::string &archiveEndpoint,
    const std::string &archiveResponseEndpoint,
    const std::string &egressResponseEndpoint, const std::string &endpoints)
    : m_id(id), m_ingressEndpoint(ingressEndpoint),
      m_consensusEndpoint(consensusEndpoint), m_logEndpoint(logEndpoint),
      m_catchupEndpoint(catchupEndpoint), m_archiveEndpoint(archiveEndpoint),
      m_archiveResponseEndpoint(archiveResponseEndpoint),
      m_egressResponseEndpoint(egressResponseEndpoint), m_endpoints(endpoints) {
}

inline void ClusterMember::reset() {
  m_isBallotSent = false;
  m_isLeader = false;
  m_hasTerminated = false;
  m_vote.reset();
  m_candidateTermId = 0;  // NULL_VALUE
  m_leadershipTermId = 0; // NULL_VALUE
  m_logPosition = 0;      // NULL_POSITION
}

inline ClusterMember &ClusterMember::isLeader(bool isLeader) {
  m_isLeader = isLeader;
  return *this;
}

inline bool ClusterMember::isLeader() const { return m_isLeader; }

inline ClusterMember &ClusterMember::isBallotSent(bool isBallotSent) {
  m_isBallotSent = isBallotSent;
  return *this;
}

inline bool ClusterMember::isBallotSent() const { return m_isBallotSent; }

inline ClusterMember &ClusterMember::hasTerminated(bool hasTerminated) {
  m_hasTerminated = hasTerminated;
  return *this;
}

inline bool ClusterMember::hasTerminated() const { return m_hasTerminated; }

inline ClusterMember &ClusterMember::id(std::int32_t id) {
  m_id = id;
  return *this;
}

inline std::int32_t ClusterMember::id() const { return m_id; }

inline ClusterMember &ClusterMember::vote(std::shared_ptr<bool> vote) {
  m_vote = vote;
  return *this;
}

inline std::shared_ptr<bool> ClusterMember::vote() const { return m_vote; }

inline ClusterMember &
ClusterMember::leadershipTermId(std::int64_t leadershipTermId) {
  m_leadershipTermId = leadershipTermId;
  return *this;
}

inline std::int64_t ClusterMember::leadershipTermId() const {
  return m_leadershipTermId;
}

inline ClusterMember &ClusterMember::logPosition(std::int64_t logPosition) {
  m_logPosition = logPosition;
  return *this;
}

inline std::int64_t ClusterMember::logPosition() const { return m_logPosition; }

inline ClusterMember &
ClusterMember::candidateTermId(std::int64_t candidateTermId) {
  m_candidateTermId = candidateTermId;
  return *this;
}

inline std::int64_t ClusterMember::candidateTermId() const {
  return m_candidateTermId;
}

inline ClusterMember &
ClusterMember::catchupReplaySessionId(std::int64_t replaySessionId) {
  m_catchupReplaySessionId = replaySessionId;
  return *this;
}

inline std::int64_t ClusterMember::catchupReplaySessionId() const {
  return m_catchupReplaySessionId;
}

inline ClusterMember &
ClusterMember::catchupReplayCorrelationId(std::int64_t correlationId) {
  m_catchupReplayCorrelationId = correlationId;
  return *this;
}

inline std::int64_t ClusterMember::catchupReplayCorrelationId() const {
  return m_catchupReplayCorrelationId;
}

inline ClusterMember &ClusterMember::correlationId(std::int64_t correlationId) {
  m_changeCorrelationId = correlationId;
  return *this;
}

inline std::int64_t ClusterMember::correlationId() const {
  return m_changeCorrelationId;
}

inline ClusterMember &
ClusterMember::timeOfLastAppendPositionNs(std::int64_t timeNs) {
  m_timeOfLastAppendPositionNs = timeNs;
  return *this;
}

inline std::int64_t ClusterMember::timeOfLastAppendPositionNs() const {
  return m_timeOfLastAppendPositionNs;
}

inline std::string ClusterMember::consensusEndpoint() const {
  return m_consensusEndpoint;
}

inline std::string ClusterMember::ingressEndpoint() const {
  return m_ingressEndpoint;
}

inline std::string ClusterMember::logEndpoint() const { return m_logEndpoint; }

inline std::string ClusterMember::catchupEndpoint() const {
  return m_catchupEndpoint;
}

inline std::string ClusterMember::archiveEndpoint() const {
  return m_archiveEndpoint;
}

inline std::string ClusterMember::archiveResponseEndpoint() const {
  return m_archiveResponseEndpoint;
}

inline std::string ClusterMember::egressResponseEndpoint() const {
  return m_egressResponseEndpoint;
}

inline std::string ClusterMember::endpoints() const { return m_endpoints; }

inline std::shared_ptr<ExclusivePublication>
ClusterMember::publication() const {
  return m_publication;
}

inline void
ClusterMember::publication(std::shared_ptr<ExclusivePublication> publication) {
  m_publication = publication;
}

inline void
ClusterMember::closePublication(const exception_handler_t &errorHandler) {
  CloseHelper::close(errorHandler, m_publication);
  m_publication.reset();
}

inline std::int32_t ClusterMember::quorumThreshold(std::int32_t memberCount) {
  return (memberCount >> 1) + 1;
}

inline std::int32_t ClusterMember::compareLog(
    std::int64_t lhsLogLeadershipTermId, std::int64_t lhsLogPosition,
    std::int64_t rhsLogLeadershipTermId, std::int64_t rhsLogPosition) {
  if (lhsLogLeadershipTermId > rhsLogLeadershipTermId) {
    return 1;
  } else if (lhsLogLeadershipTermId < rhsLogLeadershipTermId) {
    return -1;
  } else if (lhsLogPosition > rhsLogPosition) {
    return 1;
  } else if (lhsLogPosition < rhsLogPosition) {
    return -1;
  }
  return 0;
}

inline std::int32_t ClusterMember::compareLog(const ClusterMember &lhs,
                                              const ClusterMember &rhs) {
  return compareLog(lhs.m_leadershipTermId, lhs.m_logPosition,
                    rhs.m_leadershipTermId, rhs.m_logPosition);
}

inline ClusterMember *
ClusterMember::findMember(std::vector<ClusterMember> &clusterMembers,
                          std::int32_t memberId) {
  for (auto &member : clusterMembers) {
    if (memberId == member.m_id) {
      return &member;
    }
  }
  return nullptr;
}

inline void
ClusterMember::setIsLeader(std::vector<ClusterMember> &clusterMembers,
                           std::int32_t leaderMemberId) {
  for (auto &clusterMember : clusterMembers) {
    clusterMember.isLeader(clusterMember.id() == leaderMemberId);
  }
}

inline void
ClusterMember::resetLogPositions(std::vector<ClusterMember> &clusterMembers,
                                 std::int64_t logPosition) {
  for (auto &member : clusterMembers) {
    member.logPosition(logPosition);
  }
}

inline void ClusterMember::reset(std::vector<ClusterMember> &members) {
  for (auto &member : members) {
    member.reset();
  }
}

inline bool ClusterMember::areSameEndpoints(const ClusterMember &lhs,
                                            const ClusterMember &rhs) {
  return lhs.m_ingressEndpoint == rhs.m_ingressEndpoint &&
         lhs.m_consensusEndpoint == rhs.m_consensusEndpoint &&
         lhs.m_logEndpoint == rhs.m_logEndpoint &&
         lhs.m_catchupEndpoint == rhs.m_catchupEndpoint &&
         lhs.m_archiveEndpoint == rhs.m_archiveEndpoint;
}

// Static method implementations

inline std::vector<ClusterMember> ClusterMember::EMPTY_MEMBERS{};

inline std::vector<ClusterMember>
ClusterMember::parse(const std::string &value) {
  if (value.empty()) {
    return EMPTY_MEMBERS;
  }

  std::vector<ClusterMember> members;
  std::string::size_type start = 0;
  std::string::size_type pos = 0;

  // Split by '|'
  while ((pos = value.find('|', start)) != std::string::npos) {
    std::string memberValue = value.substr(start, pos - start);
    parseMember(memberValue, members);
    start = pos + 1;
  }

  // Last member
  if (start < value.length()) {
    std::string memberValue = value.substr(start);
    parseMember(memberValue, members);
  }

  return members;
}

inline ClusterMember
ClusterMember::parseEndpoints(std::int32_t id, const std::string &endpoints) {
  std::vector<std::string> memberAttributes;
  std::string::size_type start = 0;
  std::string::size_type pos = 0;

  // Split by ','
  while ((pos = endpoints.find(',', start)) != std::string::npos) {
    memberAttributes.push_back(endpoints.substr(start, pos - start));
    start = pos + 1;
  }
  memberAttributes.push_back(endpoints.substr(start));

  if (memberAttributes.size() != 5) {
    throw ClusterException("invalid member value: " + endpoints, SOURCEINFO);
  }

  return ClusterMember(id, memberAttributes[0], memberAttributes[1],
                       memberAttributes[2], memberAttributes[3],
                       memberAttributes[4], endpoints);
}

inline std::string ClusterMember::encodeAsString(
    const std::vector<ClusterMember> &clusterMembers) {
  if (clusterMembers.empty()) {
    return "";
  }

  std::string result;
  for (std::size_t i = 0; i < clusterMembers.size(); i++) {
    const auto &member = clusterMembers[i];
    result += std::to_string(member.m_id);
    result += ',';
    result += member.m_endpoints;

    if (i < clusterMembers.size() - 1) {
      result += '|';
    }
  }

  return result;
}

inline void
ClusterMember::copyVotes(const std::vector<ClusterMember> &srcMembers,
                         std::vector<ClusterMember> &dstMembers) {
  for (const auto &srcMember : srcMembers) {
    ClusterMember *dstMember = findMember(dstMembers, srcMember.m_id);
    if (dstMember) {
      dstMember->vote(srcMember.m_vote);
    }
  }
}

inline void ClusterMember::addConsensusPublications(
    std::vector<ClusterMember> &members, const ClusterMember &thisMember,
    const std::string &channelTemplate, std::int32_t streamId,
    bool bindConsensusControl, std::shared_ptr<Aeron> aeron,
    const exception_handler_t &errorHandler) {
  std::shared_ptr<ChannelUri> channelUri = ChannelUri::parse(channelTemplate);

  for (auto &member : members) {
    if (member.m_id != thisMember.m_id) {
      channelUri->put(ENDPOINT_PARAM_NAME, member.m_consensusEndpoint);
      setControlEndpoint(*channelUri, bindConsensusControl,
                         thisMember.m_consensusEndpoint);
      member.m_consensusChannel = channelUri->toString();
      tryAddPublication(member, streamId, aeron, errorHandler);
    }
  }
}

inline void ClusterMember::addConsensusPublication(
    const ClusterMember &thisMember, ClusterMember &otherMember,
    const std::string &channelTemplate, std::int32_t streamId,
    bool bindConsensusControl, std::shared_ptr<Aeron> aeron,
    const exception_handler_t &errorHandler) {
  if (otherMember.m_consensusChannel.empty()) {
    std::shared_ptr<ChannelUri> channelUri = ChannelUri::parse(channelTemplate);
    channelUri->put(ENDPOINT_PARAM_NAME, otherMember.m_consensusEndpoint);
    setControlEndpoint(*channelUri, bindConsensusControl,
                       thisMember.m_consensusEndpoint);
    otherMember.m_consensusChannel = channelUri->toString();
  }

  tryAddPublication(otherMember, streamId, aeron, errorHandler);
}

inline void
ClusterMember::tryAddPublication(ClusterMember &member, std::int32_t streamId,
                                 std::shared_ptr<Aeron> aeron,
                                 const exception_handler_t &errorHandler) {
  try {
    std::int64_t registrationId =
        aeron->addExclusivePublication(member.m_consensusChannel, streamId);
    // Wait for publication to be available
    while (!member.m_publication) {
      member.m_publication = aeron->findExclusivePublication(registrationId);
      if (!member.m_publication) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
      }
    }
  } catch (const RegistrationException &ex) {
    errorHandler(
        ClusterException("failed to add consensus publication for member: " +
                             std::to_string(member.m_id) + " - " + ex.what(),
                         SOURCEINFO));
  }
}

inline void ClusterMember::closeConsensusPublications(
    const exception_handler_t &errorHandler,
    std::vector<ClusterMember> &clusterMembers) {
  for (auto &member : clusterMembers) {
    member.closePublication(errorHandler);
  }
}

inline void ClusterMember::addClusterMemberIds(
    std::vector<ClusterMember> &clusterMembers,
    std::unordered_map<std::int32_t, ClusterMember *> &clusterMemberByIdMap) {
  for (auto &member : clusterMembers) {
    clusterMemberByIdMap[member.m_id] = &member;
  }
}

inline bool
ClusterMember::hasActiveQuorum(const std::vector<ClusterMember> &clusterMembers,
                               std::int64_t nowNs, std::int64_t timeoutNs) {
  std::int32_t threshold =
      quorumThreshold(static_cast<std::int32_t>(clusterMembers.size()));

  for (const auto &member : clusterMembers) {
    if (member.m_isLeader ||
        nowNs <= (member.m_timeOfLastAppendPositionNs + timeoutNs)) {
      if (--threshold <= 0) {
        return true;
      }
    }
  }

  return false;
}

inline std::int64_t
ClusterMember::quorumPosition(const std::vector<ClusterMember> &members,
                              std::vector<std::int64_t> &rankedPositions) {
  const std::size_t length = rankedPositions.size();
  for (std::size_t i = 0; i < length; i++) {
    rankedPositions[i] = 0;
  }

  for (const auto &member : members) {
    std::int64_t newPosition = member.m_logPosition;

    for (std::size_t i = 0; i < length; i++) {
      const std::int64_t rankedPosition = rankedPositions[i];

      if (newPosition > rankedPosition) {
        rankedPositions[i] = newPosition;
        newPosition = rankedPosition;
      }
    }
  }

  return rankedPositions[length - 1];
}

inline bool ClusterMember::hasVotersAtPosition(
    const std::vector<ClusterMember> &clusterMembers, std::int64_t position,
    std::int64_t leadershipTermId) {
  for (const auto &member : clusterMembers) {
    if (member.m_vote && (member.m_logPosition < position ||
                          member.m_leadershipTermId != leadershipTermId)) {
      return false;
    }
  }

  return true;
}

inline bool ClusterMember::hasQuorumAtPosition(
    const std::vector<ClusterMember> &clusterMembers, std::int64_t position,
    std::int64_t leadershipTermId) {
  std::int32_t votes = 0;

  for (const auto &member : clusterMembers) {
    if (member.m_leadershipTermId == leadershipTermId &&
        member.m_logPosition >= position) {
      ++votes;
    }
  }

  return votes >=
         quorumThreshold(static_cast<std::int32_t>(clusterMembers.size()));
}

inline void ClusterMember::becomeCandidate(std::vector<ClusterMember> &members,
                                           std::int64_t candidateTermId,
                                           std::int32_t candidateMemberId) {
  for (auto &member : members) {
    if (member.m_id == candidateMemberId) {
      member.vote(std::make_shared<bool>(true))
          .candidateTermId(candidateTermId)
          .isBallotSent(true);
    } else {
      member.vote(nullptr)
          .candidateTermId(0) // NULL_VALUE
          .isBallotSent(false);
    }
  }
}

inline bool ClusterMember::isUnanimousLeader(
    const std::vector<ClusterMember> &clusterMembers,
    std::int64_t candidateTermId, std::int32_t gracefulClosedLeaderId) {
  std::int32_t votes = 0;

  for (const auto &member : clusterMembers) {
    if (member.m_id == gracefulClosedLeaderId) {
      continue;
    }

    if (candidateTermId != member.m_candidateTermId || !member.m_vote ||
        !*member.m_vote) {
      return false;
    }

    votes++;
  }

  return votes >=
         quorumThreshold(static_cast<std::int32_t>(clusterMembers.size()));
}

inline bool
ClusterMember::isQuorumLeader(const std::vector<ClusterMember> &clusterMembers,
                              std::int64_t candidateTermId) {
  std::int32_t votes = 0;

  for (const auto &member : clusterMembers) {
    if (candidateTermId == member.m_candidateTermId) {
      if (member.m_vote && !*member.m_vote) // Boolean.FALSE
      {
        return false;
      }

      if (member.m_vote && *member.m_vote) // Boolean.TRUE
      {
        ++votes;
      }
    }
  }

  return votes >=
         quorumThreshold(static_cast<std::int32_t>(clusterMembers.size()));
}

inline ClusterMember
ClusterMember::determineMember(const std::vector<ClusterMember> &clusterMembers,
                               std::int32_t memberId,
                               const std::string &memberEndpoints) {
  ClusterMember *member =
      (0 != memberId)
          ? findMember(const_cast<std::vector<ClusterMember> &>(clusterMembers),
                       memberId)
          : nullptr;

  if ((clusterMembers.empty() || 0 == clusterMembers.size()) && !member) {
    return parseEndpoints(0, memberEndpoints); // NULL_VALUE
  } else {
    if (!member) {
      throw ClusterException("memberId=" + std::to_string(memberId) +
                                 " not found in clusterMembers",
                             SOURCEINFO);
    }

    if (!memberEndpoints.empty()) {
      validateMemberEndpoints(*member, memberEndpoints);
    }

    return *member;
  }
}

inline void
ClusterMember::validateMemberEndpoints(const ClusterMember &member,
                                       const std::string &memberEndpoints) {
  ClusterMember endpoints = parseEndpoints(0, memberEndpoints); // NULL_VALUE

  if (!areSameEndpoints(member, endpoints)) {
    throw ClusterException("clusterMembers and endpoints differ: " +
                               member.m_endpoints + " != " + memberEndpoints,
                           SOURCEINFO);
  }
}

inline bool ClusterMember::isUnanimousCandidate(
    const std::vector<ClusterMember> &clusterMembers,
    const ClusterMember &candidate, std::int32_t gracefulClosedLeaderId) {
  std::int32_t possibleVotes = 0;

  for (const auto &member : clusterMembers) {
    if (member.m_id == gracefulClosedLeaderId) {
      continue;
    }

    if (0 == member.m_logPosition ||
        compareLog(candidate, member) < 0) // NULL_POSITION
    {
      return false;
    }

    possibleVotes++;
  }

  return possibleVotes >=
         quorumThreshold(static_cast<std::int32_t>(clusterMembers.size()));
}

inline bool ClusterMember::isQuorumCandidate(
    const std::vector<ClusterMember> &clusterMembers,
    const ClusterMember &candidate) {
  std::int32_t possibleVotes = 0;

  for (const auto &member : clusterMembers) {
    if (0 == member.m_logPosition ||
        compareLog(candidate, member) < 0) // NULL_POSITION
    {
      continue;
    }

    ++possibleVotes;
  }

  return possibleVotes >=
         quorumThreshold(static_cast<std::int32_t>(clusterMembers.size()));
}

inline std::string
ClusterMember::ingressEndpoints(const std::vector<ClusterMember> &members) {
  std::string result;
  result.reserve(100);

  for (std::size_t i = 0; i < members.size(); i++) {
    if (0 != i) {
      result += ',';
    }

    const auto &member = members[i];
    result += std::to_string(member.m_id);
    result += '=';
    result += member.m_ingressEndpoint;
  }

  return result;
}

inline std::string ClusterMember::toString() const {
  return std::string("ClusterMember{") + "id=" + std::to_string(m_id) +
         ", isBallotSent=" + (m_isBallotSent ? "true" : "false") +
         ", isLeader=" + (m_isLeader ? "true" : "false") +
         ", leadershipTermId=" + std::to_string(m_leadershipTermId) +
         ", logPosition=" + std::to_string(m_logPosition) +
         ", candidateTermId=" + std::to_string(m_candidateTermId) +
         ", catchupReplaySessionId=" +
         std::to_string(m_catchupReplaySessionId) +
         ", correlationId=" + std::to_string(m_changeCorrelationId) +
         ", timeOfLastAppendPositionNs=" +
         std::to_string(m_timeOfLastAppendPositionNs) + ", ingressEndpoint='" +
         m_ingressEndpoint + '\'' + ", consensusEndpoint='" +
         m_consensusEndpoint + '\'' + ", logEndpoint='" + m_logEndpoint + '\'' +
         ", catchupEndpoint='" + m_catchupEndpoint + '\'' +
         ", archiveEndpoint='" + m_archiveEndpoint + '\'' + ", endpoints='" +
         m_endpoints + '\'' +
         ", vote=" + (m_vote ? (*m_vote ? "true" : "false") : "null") + "}";
}

inline void ClusterMember::setControlEndpoint(ChannelUri &channelUri,
                                              bool shouldBind,
                                              const std::string &endpoint) {
  if (!shouldBind) {
    return;
  }

  std::string controlEndpoint = replacePortWithWildcard(endpoint);
  if (controlEndpoint.empty()) {
    return;
  }

  channelUri.put(MDC_CONTROL_PARAM_NAME, controlEndpoint);
}

inline std::string
ClusterMember::replacePortWithWildcard(const std::string &endpoint) {
  if (endpoint.empty()) {
    return "";
  }

  const std::string::size_type i = endpoint.find_last_of(':');
  if (std::string::npos == i) {
    return "";
  }

  return endpoint.substr(0, i + 1) + "0";
}

inline void ClusterMember::parseMember(const std::string &idAndEndpoints,
                                       std::vector<ClusterMember> &members) {
  std::vector<std::string> memberAttributes;
  std::string::size_type start = 0;
  std::string::size_type pos = 0;

  // Split by ','
  while ((pos = idAndEndpoints.find(',', start)) != std::string::npos) {
    memberAttributes.push_back(idAndEndpoints.substr(start, pos - start));
    start = pos + 1;
  }
  memberAttributes.push_back(idAndEndpoints.substr(start));

  if (memberAttributes.size() < 6 || 8 < memberAttributes.size()) {
    throw ClusterException("invalid member value: " + idAndEndpoints,
                           SOURCEINFO);
  }

  std::int32_t clusterMemberId;
  try {
    clusterMemberId = static_cast<std::int32_t>(std::stoi(memberAttributes[0]));
  } catch (const std::exception &ex) {
    throw ClusterException(
        "invalid cluster member id, must be an integer value: " +
            std::string(ex.what()),
        SOURCEINFO);
  }

  const std::string archiveResponseEndpoint =
      (6 < memberAttributes.size()) ? memberAttributes[6] : "";
  const std::string egressResponseEndpoint =
      (7 < memberAttributes.size()) ? memberAttributes[7] : "";

  std::string endpoints = memberAttributes[1] + "," + memberAttributes[2] +
                          "," + memberAttributes[3] + "," +
                          memberAttributes[4] + "," + memberAttributes[5];

  if (!archiveResponseEndpoint.empty()) {
    endpoints += "," + archiveResponseEndpoint;
  }

  if (!egressResponseEndpoint.empty()) {
    endpoints += "," + egressResponseEndpoint;
  }

  members.emplace_back(
      clusterMemberId, memberAttributes[1], memberAttributes[2],
      memberAttributes[3], memberAttributes[4], memberAttributes[5],
      archiveResponseEndpoint, egressResponseEndpoint, endpoints);
}

} // namespace cluster
} // namespace aeron
