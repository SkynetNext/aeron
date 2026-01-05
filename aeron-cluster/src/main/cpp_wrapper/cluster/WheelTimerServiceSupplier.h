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

}}

