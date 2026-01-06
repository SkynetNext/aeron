#pragma once
#include <memory>
#include <string>
#include "Aeron.h"
#include "AeronCounters.h"
#include "../client/ClusterExceptions.h"
#include "../service/ClusterCounters.h"
#include "concurrent/AtomicCounter.h"
#include "concurrent/CountersReader.h"

namespace aeron { namespace cluster
{
using namespace aeron::concurrent;

/**
 * Toggle control ToggleStates for a cluster node such as REPLICATE_STANDBY_SNAPSHOT.
 * This can only be applied to individual nodes and does not apply across the cluster.
 */
class NodeControl
{
public:
    /**
     * Toggle states for controlling the cluster node once it has entered the active state after initialising.
     * The toggle can only we switched into a new state from NEUTRAL and will be reset by the
     * ConsensusModule once the triggered action is complete.
     */
    enum class ToggleState : std::int32_t
    {
        /**
         * Inactive state, not accepting new actions.
         */
        INACTIVE = 0,

        /**
         * Neutral state ready to accept a new action.
         */
        NEUTRAL = 1,

        /**
         * Trigger a replication of the most resent standby snapshots.
         */
        REPLICATE_STANDBY_SNAPSHOT = 2
    };

    /**
     * Counter type id for the control toggle.
     */
    static constexpr std::int32_t CONTROL_TOGGLE_TYPE_ID = AeronCounters::NODE_CONTROL_TOGGLE_TYPE_ID;

    /**
     * Code to be used as the indicator in the control toggle counter.
     *
     * @param state the toggle state.
     * @return code to be used as the indicator in the control toggle counter.
     */
    static std::int32_t code(ToggleState state)
    {
        return static_cast<std::int32_t>(state);
    }

    /**
     * Toggle the control counter to trigger the requested ToggleState.
     *
     * @param controlToggle to change to the trigger state.
     * @param targetState the target toggle state.
     * @return true if the counter toggles or false if it is in a state other than NEUTRAL.
     */
    static bool toggle(std::shared_ptr<AtomicCounter> controlToggle, ToggleState targetState)
    {
        return controlToggle->compareAndSet(code(ToggleState::NEUTRAL), code(targetState));
    }

    /**
     * Reset the toggle to the NEUTRAL state.
     *
     * @param controlToggle to be reset.
     */
    static void reset(std::shared_ptr<AtomicCounter> controlToggle)
    {
        controlToggle->set(code(ToggleState::NEUTRAL));
    }

    /**
     * Activate the toggle by setting it to the NEUTRAL state.
     *
     * @param controlToggle to be activated.
     */
    static void activate(std::shared_ptr<AtomicCounter> controlToggle)
    {
        controlToggle->set(code(ToggleState::NEUTRAL));
    }

    /**
     * Deactivate the toggle by setting it to the INACTIVE state.
     *
     * @param controlToggle to be deactivated.
     */
    static void deactivate(std::shared_ptr<AtomicCounter> controlToggle)
    {
        controlToggle->set(code(ToggleState::INACTIVE));
    }

    /**
     * Get the ToggleState for a given control toggle.
     *
     * @param controlToggle to get the current state for.
     * @return the state for the current control toggle.
     * @throws ClusterException if the counter is not one of the valid values.
     */
    static ToggleState get(std::shared_ptr<AtomicCounter> controlToggle)
    {
        if (controlToggle->isClosed())
        {
            throw ClusterException("counter is closed", SOURCEINFO);
        }

        const std::int64_t toggleValue = controlToggle->get();
        if (toggleValue < 0 || toggleValue > 2)
        {
            throw ClusterException("invalid toggle value: " + std::to_string(toggleValue), SOURCEINFO);
        }

        return static_cast<ToggleState>(toggleValue);
    }

    /**
     * Find the control toggle counter or return null if not found.
     *
     * @param counters  to search within.
     * @param clusterId to which the allocated counter belongs.
     * @return the control toggle counter or return null if not found.
     */
    static std::shared_ptr<AtomicCounter> findControlToggle(CountersReader& counters, std::int32_t clusterId)
    {
        const std::int32_t counterId = ClusterCounters::find(counters, CONTROL_TOGGLE_TYPE_ID, clusterId);
        if (Aeron::NULL_VALUE != counterId)
        {
            return std::make_shared<AtomicCounter>(counters.valuesBuffer(), counterId, nullptr);
        }

        return nullptr;
    }
};

}}

