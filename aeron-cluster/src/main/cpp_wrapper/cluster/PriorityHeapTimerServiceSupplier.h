#pragma once
#include <memory>
#include <chrono>
#include "TimerService.h"
#include "TimerServiceSupplier.h"
#include "PriorityHeapTimerService.h"

namespace aeron { namespace cluster
{

/**
 * Supplies instances of the PriorityHeapTimerService.
 */
class PriorityHeapTimerServiceSupplier : public TimerServiceSupplier
{
public:
    std::shared_ptr<TimerService> newInstance(
        std::chrono::milliseconds::rep timeUnit,
        TimerService::TimerHandler timerHandler) override;
};

// Implementation
inline std::shared_ptr<TimerService> PriorityHeapTimerServiceSupplier::newInstance(
    std::chrono::milliseconds::rep timeUnit,
    TimerService::TimerHandler timerHandler)
{
    return std::make_shared<PriorityHeapTimerService>(timerHandler);
}

}}
