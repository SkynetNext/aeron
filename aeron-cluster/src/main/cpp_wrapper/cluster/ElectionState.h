#pragma once
#include <vector>
#include "../client/ClusterExceptions.h"
#include "concurrent/AtomicCounter.h"

namespace aeron { namespace cluster
{
using namespace aeron::concurrent;

/**
 * Election states for a ConsensusModule which get represented by a code() stored in a
 * Counter of the type ConsensusModule.Configuration.ELECTION_STATE_TYPE_ID.
 */
enum class ElectionState : std::int32_t
{
    /**
     * Consolidate local state and prepare for new leadership.
     */
    INIT = 0,

    /**
     * Canvass members for current state and to assess if a successful leadership attempt can be mounted.
     */
    CANVASS = 1,

    /**
     * Nominate member for new leadership by requesting votes.
     */
    NOMINATE = 2,

    /**
     * Await ballot outcome from members on candidacy for leadership.
     */
    CANDIDATE_BALLOT = 3,

    /**
     * Await ballot outcome after voting for a candidate.
     */
    FOLLOWER_BALLOT = 4,

    /**
     * Wait for followers to replicate any missing log entries to track commit position.
     */
    LEADER_LOG_REPLICATION = 5,

    /**
     * Replay local log in preparation for new leadership term.
     */
    LEADER_REPLAY = 6,

    /**
     * Initialise state for new leadership term.
     */
    LEADER_INIT = 7,

    /**
     * Publish new leadership term and commit position, while awaiting followers ready.
     */
    LEADER_READY = 8,

    /**
     * Replicate missing log entries from the leader.
     */
    FOLLOWER_LOG_REPLICATION = 9,

    /**
     * Replay local log in preparation for following new leader.
     */
    FOLLOWER_REPLAY = 10,

    /**
     * Initialise catch-up in preparation of receiving a replay from the leader to catch up in current term.
     */
    FOLLOWER_CATCHUP_INIT = 11,

    /**
     * Await joining a replay from leader to catch-up.
     */
    FOLLOWER_CATCHUP_AWAIT = 12,

    /**
     * Catch-up to leader until live log can be added and merged.
     */
    FOLLOWER_CATCHUP = 13,

    /**
     * Initialise follower in preparation for joining the live log.
     */
    FOLLOWER_LOG_INIT = 14,

    /**
     * Await joining the live log from the leader.
     */
    FOLLOWER_LOG_AWAIT = 15,

    /**
     * Publish append position to leader to signify ready for new term.
     */
    FOLLOWER_READY = 16,

    /**
     * Election is closed after new leader is established.
     */
    CLOSED = 17
};

/**
 * Code stored in a Counter to represent the election state.
 *
 * @param state the election state.
 * @return code stored in a Counter to represent the election state.
 */
inline std::int32_t code(ElectionState state)
{
    return static_cast<std::int32_t>(state);
}

/**
 * Get the enum value for a given code stored in a counter.
 *
 * @param code representing election state.
 * @return the enum value for a given code stored in a counter.
 */
inline ElectionState getElectionState(std::int64_t code)
{
    if (code < 0 || code > 17)
    {
        throw ClusterException("invalid election state counter code: " + std::to_string(code));
    }

    return static_cast<ElectionState>(code);
}

/**
 * Get the ElectionState value based on the value stored in an AtomicCounter.
 *
 * @param counter to read the value for matching against code().
 * @return the ElectionState value based on the value stored in an AtomicCounter.
 */
inline ElectionState getElectionState(std::shared_ptr<AtomicCounter> counter)
{
    if (counter->isClosed())
    {
        return ElectionState::CLOSED;
    }

    return getElectionState(counter->get());
}

}}

