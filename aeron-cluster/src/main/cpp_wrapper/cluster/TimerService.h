#pragma once
#include <functional>
#include <cstdint>

namespace aeron { namespace cluster
{

/**
 * Timer service providing API to schedule timers in the Cluster.
 */
class TimerService
{
public:
    /**
     * Called from the poll().
     */
    using TimerHandler = std::function<bool(std::int64_t correlationId)>;

    /**
     * Called from the snapshot().
     */
    using TimerSnapshotTaker = std::function<void(std::int64_t correlationId, std::int64_t deadline)>;

    /**
     * Max number of the timers to expire per single poll().
     */
    static constexpr std::int32_t POLL_LIMIT = 20;

    virtual ~TimerService() = default;

    /**
     * Poll for expired timers. For each expired timer it invokes TimerHandler::onTimerEvent(correlationId) on the
     * TimerHandler that was provided upon creation of this TimerService instance.
     * Poll can be terminated early if POLL_LIMIT is reached or if TimerHandler returns false.
     *
     * @param now current time.
     * @return number of timers expired, never exceeds POLL_LIMIT.
     */
    virtual std::int32_t poll(std::int64_t now) = 0;

    /**
     * Schedule timer with the given correlationId and deadline.
     *
     * @param correlationId to associate with the timer.
     * @param deadline      when the timer expires.
     */
    virtual void scheduleTimerForCorrelationId(std::int64_t correlationId, std::int64_t deadline) = 0;

    /**
     * Cancels timer by its correlationId.
     *
     * @param correlationId of the timer to cancel.
     * @return true if the timer was cancelled.
     */
    virtual bool cancelTimerByCorrelationId(std::int64_t correlationId) = 0;

    /**
     * Takes a snapshot of the timer service state, i.e. serializes all non-expired timers.
     *
     * @param snapshotTaker to take a snapshot.
     */
    virtual void snapshot(TimerSnapshotTaker snapshotTaker) = 0;

    /**
     * Set the current time from the Cluster in case the underlying implementation depends on it.
     *
     * @param now the current time.
     */
    virtual void currentTime(std::int64_t now) = 0;
};

}}

