#pragma once
#include <memory>
#include <vector>
#include <unordered_map>
#include <algorithm>
#include "TimerService.h"
#include "Aeron.h"
#include "util/ArrayUtil.h"

namespace aeron { namespace cluster
{
using namespace aeron::util;

/**
 * Implementation of the TimerService that uses a priority heap to order the timestamps.
 *
 * <p>
 * <b>Caveats</b>
 * <p>
 * Timers with the same deadline are not be ordered with one another. In contrast, the timers with different deadlines
 * are guaranteed to expire in order even after Cluster restart, i.e. when the deadlines are in the past.
 * <p>
 * <b>Note:</b> Not thread safe.
 */
class PriorityHeapTimerService : public TimerService
{
public:
    explicit PriorityHeapTimerService(TimerHandler timerHandler);

    std::int32_t poll(std::int64_t now) override;

    void scheduleTimerForCorrelationId(std::int64_t correlationId, std::int64_t deadline) override;

    bool cancelTimerByCorrelationId(std::int64_t correlationId) override;

    void snapshot(TimerSnapshotTaker snapshotTaker) override;

    void currentTime(std::int64_t now) override;

private:
    static constexpr std::int32_t MIN_CAPACITY = 8;

    class Timer
    {
    public:
        std::int64_t correlationId;
        std::int64_t deadline;
        std::int32_t index;

        Timer(std::int64_t correlationId, std::int64_t deadline, std::int32_t index);

        void reset(std::int64_t correlationId, std::int64_t deadline, std::int32_t index);

        std::string toString() const;
    };

    static void shiftUp(std::vector<Timer*>& timers, std::int32_t startIndex, Timer* timer);

    static void shiftDown(std::vector<Timer*>& timers, std::int32_t size, std::int32_t startIndex, Timer* timer);

    void ensureCapacity(std::int32_t requiredCapacity);

    void addToFreeList(Timer* timer);

    TimerHandler m_timerHandler;
    std::unordered_map<std::int64_t, Timer*> m_timerByCorrelationId;
    std::vector<Timer*> m_timers;
    std::vector<Timer*> m_freeTimers;
    std::int32_t m_size = 0;
    std::int32_t m_freeTimerCount = 0;
};

}}

