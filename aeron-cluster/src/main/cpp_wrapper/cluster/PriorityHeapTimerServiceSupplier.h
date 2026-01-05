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

}}

