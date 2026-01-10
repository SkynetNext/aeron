#pragma once
#include <memory>
#include <unordered_map>
#include <chrono>
#include "TimerService.h"
// TODO: DeadlineTimerWheel needs to be implemented
// #include "concurrent/DeadlineTimerWheel.h"

namespace aeron { namespace cluster
{
using namespace aeron::concurrent;

/**
 * Implementation of the TimerService that is based on DeadlineTimerWheel.
 *
 * <p>
 * <b>Caveats</b>
 * <p>
 * Timers that expire in the same tick are not be ordered with one another. As ticks are
 * fairly coarse resolution normally, this means that some timers may expire out of order.
 * <p>
 * Upon Cluster restart the expiration order of the already expired timers is <em>not guaranteed</em>, i.e. timers with
 * the later deadlines might expire before timers with the earlier deadlines. To avoid this behavior use the
 * PriorityHeapTimerService instead.
 * <p>
 * <b>Note:</b> Not thread safe.
 */
// TODO: DeadlineTimerWheel needs to be implemented
// class WheelTimerService : public DeadlineTimerWheel, public DeadlineTimerWheel::TimerHandler, public TimerService
class WheelTimerService : public TimerService
{
public:
    WheelTimerService(
        TimerService::TimerHandler timerHandler,
        std::chrono::milliseconds::rep timeUnit,
        std::int64_t startTime,
        std::int64_t tickResolution,
        std::int32_t ticksPerWheel);

    std::int32_t poll(std::int64_t now) override;

    bool onTimerExpiry(
        std::chrono::milliseconds::rep timeUnit,
        std::int64_t now,
        std::int64_t timerId);

    void scheduleTimerForCorrelationId(std::int64_t correlationId, std::int64_t deadline) override;

    bool cancelTimerByCorrelationId(std::int64_t correlationId) override;

    void snapshot(TimerSnapshotTaker snapshotTaker) override;

    void currentTime(std::int64_t now) override;

private:
    TimerService::TimerHandler m_timerHandler;
    std::unordered_map<std::int64_t, std::int64_t> m_timerIdByCorrelationIdMap;
    std::unordered_map<std::int64_t, std::int64_t> m_correlationIdByTimerIdMap;
    bool m_isAbort = false;
};

}}

