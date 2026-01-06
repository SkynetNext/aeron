#pragma once
#include <memory>
#include <chrono>
#include "TimerService.h"
#include "TimerServiceSupplier.h"
#include "WheelTimerService.h"
#include "util/BitUtil.h"

namespace aeron { namespace cluster
{
using namespace aeron::util;

/**
 * Supplies an instance of a WheelTimerService based on the configuration given to the constructor.
 */
class WheelTimerServiceSupplier : public TimerServiceSupplier
{
public:
    WheelTimerServiceSupplier(
        std::chrono::milliseconds::rep timeUnit,
        std::int64_t startTime,
        std::int64_t tickResolution,
        std::int32_t ticksPerWheel);

    std::shared_ptr<TimerService> newInstance(
        std::chrono::milliseconds::rep clusterTimeUnit,
        TimerService::TimerHandler timerHandler) override;

private:
    std::chrono::milliseconds::rep m_timeUnit;
    std::int64_t m_startTime;
    std::int64_t m_tickResolution;
    std::int32_t m_ticksPerWheel;
};

// Implementation
inline WheelTimerServiceSupplier::WheelTimerServiceSupplier(
    std::chrono::milliseconds::rep timeUnit,
    std::int64_t startTime,
    std::int64_t tickResolution,
    std::int32_t ticksPerWheel) :
    m_timeUnit(timeUnit),
    m_startTime(startTime),
    m_tickResolution(tickResolution),
    m_ticksPerWheel(ticksPerWheel)
{
}

inline std::shared_ptr<TimerService> WheelTimerServiceSupplier::newInstance(
    std::chrono::milliseconds::rep clusterTimeUnit,
    TimerService::TimerHandler timerHandler)
{
    // Convert time units: clusterTimeUnit.convert(startTime, timeUnit)
    // timeUnit and clusterTimeUnit are multipliers representing nanoseconds per unit:
    // - 1 = milliseconds (1 ms = 1,000,000 ns)
    // - 1000 = microseconds (1 μs = 1,000 ns)
    // - 1000000 = nanoseconds (1 ns = 1 ns)
    // To convert from timeUnit to clusterTimeUnit: value * (clusterTimeUnit / timeUnit)
    // Example: convert 1 millisecond to nanoseconds: 1 * (1000000 / 1) = 1,000,000
    const std::int64_t startTimeInClusterTimeUnits = m_startTime * (clusterTimeUnit / m_timeUnit);
    const std::int64_t resolutionInClusterTimeUnits = m_tickResolution * (clusterTimeUnit / m_timeUnit);

    // Java: findNextPositivePowerOfTwo(resolutionInClusterTimeUnits)
    // C++: BitUtil::findNextPowerOfTwo, but need to ensure it's positive
    std::int64_t resolution = resolutionInClusterTimeUnits;
    if (resolution <= 0)
    {
        resolution = 1;
    }
    const std::int64_t tickResolution = static_cast<std::int64_t>(BitUtil::findNextPowerOfTwo(resolution));

    return std::make_shared<WheelTimerService>(
        timerHandler,
        clusterTimeUnit,
        startTimeInClusterTimeUnits,
        tickResolution,
        m_ticksPerWheel);
}

}}
