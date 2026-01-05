#pragma once
#include <memory>
#include <chrono>
#include "TimerService.h"

namespace aeron { namespace cluster
{

/**
 * Supplier of TimerService implementations to be used by the ConsensusModule.
 */
class TimerServiceSupplier
{
public:
    virtual ~TimerServiceSupplier() = default;

    /**
     * New instance of the TimerService.
     *
     * @param timeUnit      units to be used by the underlying timer service.
     * @param timerHandler  that must be invoked for each expired timer.
     * @return              timer service instance ready for immediate usage.
     */
    virtual std::shared_ptr<TimerService> newInstance(
        std::chrono::milliseconds::rep timeUnit,
        TimerService::TimerHandler timerHandler) = 0;
};

}}

