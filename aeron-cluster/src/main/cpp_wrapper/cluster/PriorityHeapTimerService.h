#pragma once
#include <memory>
#include <vector>
#include <unordered_map>
#include <algorithm>
#include <limits>
#include <stdexcept>
#include <string>
#include "TimerService.h"
#include "Aeron.h"

namespace aeron { namespace cluster
{

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

// Implementation

inline PriorityHeapTimerService::PriorityHeapTimerService(TimerHandler timerHandler) :
    m_timerHandler(timerHandler)
{
    if (!timerHandler)
    {
        throw std::invalid_argument("TimerHandler");
    }
}

inline std::int32_t PriorityHeapTimerService::poll(std::int64_t now)
{
    std::int32_t expiredTimers = 0;

    while (m_size > 0 && expiredTimers < POLL_LIMIT)
    {
        Timer* timer = m_timers[0];
        if (timer->deadline > now)
        {
            break;
        }

        if (!m_timerHandler(timer->correlationId))
        {
            break;
        }

        expiredTimers++;
        const std::int32_t lastIndex = --m_size;
        Timer* lastTimer = m_timers[lastIndex];
        m_timers[lastIndex] = nullptr;

        if (0 != lastIndex)
        {
            shiftDown(m_timers, lastIndex, 0, lastTimer);
        }

        m_timerByCorrelationId.erase(timer->correlationId);
        addToFreeList(timer);
    }

    return expiredTimers;
}

inline void PriorityHeapTimerService::scheduleTimerForCorrelationId(std::int64_t correlationId, std::int64_t deadline)
{
    auto it = m_timerByCorrelationId.find(correlationId);
    if (it != m_timerByCorrelationId.end())
    {
        Timer* existingTimer = it->second;
        if (deadline < existingTimer->deadline)
        {
            existingTimer->deadline = deadline;
            shiftUp(m_timers, existingTimer->index, existingTimer);
        }
        else if (deadline > existingTimer->deadline)
        {
            existingTimer->deadline = deadline;
            shiftDown(m_timers, m_size, existingTimer->index, existingTimer);
        }
    }
    else
    {
        ensureCapacity(m_size + 1);

        const std::int32_t index = m_size++;
        Timer* timer;
        if (m_freeTimerCount > 0)
        {
            const std::int32_t freeIndex = --m_freeTimerCount;
            timer = m_freeTimers[freeIndex];
            m_freeTimers[freeIndex] = nullptr;
            timer->reset(correlationId, deadline, index);
        }
        else
        {
            timer = new Timer(correlationId, deadline, index);
        }

        m_timerByCorrelationId[correlationId] = timer;
        shiftUp(m_timers, index, timer);
    }
}

inline bool PriorityHeapTimerService::cancelTimerByCorrelationId(std::int64_t correlationId)
{
    auto it = m_timerByCorrelationId.find(correlationId);
    if (it == m_timerByCorrelationId.end())
    {
        return false;
    }

    Timer* removedTimer = it->second;
    m_timerByCorrelationId.erase(it);

    const std::int32_t lastIndex = --m_size;
    Timer* lastTimer = m_timers[lastIndex];
    m_timers[lastIndex] = nullptr;

    if (lastIndex != removedTimer->index)
    {
        shiftDown(m_timers, lastIndex, removedTimer->index, lastTimer);
        if (m_timers[removedTimer->index] == lastTimer)
        {
            shiftUp(m_timers, removedTimer->index, lastTimer);
        }
    }

    addToFreeList(removedTimer);

    return true;
}

inline void PriorityHeapTimerService::snapshot(TimerSnapshotTaker snapshotTaker)
{
    for (std::int32_t i = 0; i < m_size; i++)
    {
        Timer* timer = m_timers[i];
        snapshotTaker(timer->correlationId, timer->deadline);
    }
}

inline void PriorityHeapTimerService::currentTime(std::int64_t now)
{
    // No-op in this implementation
}

inline void PriorityHeapTimerService::shiftUp(std::vector<Timer*>& timers, std::int32_t startIndex, Timer* timer)
{
    std::int32_t index = startIndex;
    while (index > 0)
    {
        const std::int32_t prevIndex = (index - 1) >> 1;
        Timer* prevTimer = timers[prevIndex];

        if (timer->deadline >= prevTimer->deadline)
        {
            break;
        }

        timers[index] = prevTimer;
        prevTimer->index = index;
        index = prevIndex;
    }

    timers[index] = timer;
    timer->index = index;
}

inline void PriorityHeapTimerService::shiftDown(
    std::vector<Timer*>& timers,
    std::int32_t size,
    std::int32_t startIndex,
    Timer* timer)
{
    const std::int32_t half = size >> 1;
    std::int32_t index = startIndex;
    while (index < half)
    {
        std::int32_t nextIndex = (index << 1) + 1;
        const std::int32_t right = nextIndex + 1;
        Timer* nextTimer = timers[nextIndex];

        if (right < size && nextTimer->deadline > timers[right]->deadline)
        {
            nextIndex = right;
            nextTimer = timers[nextIndex];
        }

        if (timer->deadline < nextTimer->deadline)
        {
            break;
        }

        timers[index] = nextTimer;
        nextTimer->index = index;
        index = nextIndex;
    }

    timers[index] = timer;
    timer->index = index;
}

inline void PriorityHeapTimerService::ensureCapacity(std::int32_t requiredCapacity)
{
    const std::int32_t currentCapacity = static_cast<std::int32_t>(m_timers.size());
    static constexpr std::int32_t MAX_CAPACITY = std::numeric_limits<std::int32_t>::max() - 8;

    if (requiredCapacity > currentCapacity)
    {
        if (requiredCapacity > MAX_CAPACITY)
        {
            throw std::runtime_error("max capacity reached: " + std::to_string(MAX_CAPACITY));
        }

        if (m_timers.empty())
        {
            m_timers.resize(MIN_CAPACITY, nullptr);
            m_freeTimers.resize(MIN_CAPACITY, nullptr);
        }
        else
        {
            std::int32_t newCapacity = currentCapacity + (currentCapacity >> 1);
            if (newCapacity < 0 || newCapacity > MAX_CAPACITY)
            {
                newCapacity = MAX_CAPACITY;
            }

            m_timers.resize(newCapacity, nullptr);
            m_freeTimers.resize(newCapacity, nullptr);
        }
    }
}

inline void PriorityHeapTimerService::addToFreeList(Timer* timer)
{
    timer->reset(aeron::NULL_VALUE, aeron::NULL_VALUE, aeron::NULL_VALUE);
    if (m_freeTimerCount >= static_cast<std::int32_t>(m_freeTimers.size()))
    {
        m_freeTimers.resize(m_freeTimerCount + 1, nullptr);
    }
    m_freeTimers[m_freeTimerCount++] = timer;
}

// Timer nested class implementation

inline PriorityHeapTimerService::Timer::Timer(std::int64_t correlationId, std::int64_t deadline, std::int32_t index)
{
    reset(correlationId, deadline, index);
}

inline void PriorityHeapTimerService::Timer::reset(std::int64_t correlationId, std::int64_t deadline, std::int32_t index)
{
    this->correlationId = correlationId;
    this->deadline = deadline;
    this->index = index;
}

inline std::string PriorityHeapTimerService::Timer::toString() const
{
    return "PriorityHeapTimerService.Timer{" +
        "correlationId=" + std::to_string(correlationId) +
        ", deadline=" + std::to_string(deadline) +
        ", index=" + std::to_string(index) +
        "}";
}

}}

