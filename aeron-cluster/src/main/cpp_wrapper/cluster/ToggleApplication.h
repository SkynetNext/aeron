#pragma once
#include <memory>
#include "concurrent/AtomicCounter.h"
#include "concurrent/CountersReader.h"
#include "ClusterControl.h"
#include "NodeControl.h"

namespace aeron { namespace cluster
{
using namespace aeron::concurrent;

/**
 * Generic interface for toggle application.
 */
template<typename T>
class ToggleApplication
{
public:
    virtual ~ToggleApplication() = default;

    virtual T get(std::shared_ptr<AtomicCounter> counter) = 0;

    virtual bool apply(std::shared_ptr<AtomicCounter> counter, T targetState) = 0;

    virtual std::shared_ptr<AtomicCounter> find(CountersReader& countersReader, std::int32_t clusterId) = 0;

    virtual bool isNeutral(T toggleState) = 0;
};

/**
 * Cluster control toggle application.
 */
class ClusterControlToggleApplication : public ToggleApplication<ClusterControl::ToggleState>
{
public:
    static const ClusterControlToggleApplication INSTANCE;

    ClusterControl::ToggleState get(std::shared_ptr<AtomicCounter> counter) override;

    bool apply(std::shared_ptr<AtomicCounter> counter, ClusterControl::ToggleState targetState) override;

    std::shared_ptr<AtomicCounter> find(CountersReader& countersReader, std::int32_t clusterId) override;

    bool isNeutral(ClusterControl::ToggleState toggleState) override;
};

/**
 * Node control toggle application.
 */
class NodeControlToggleApplication : public ToggleApplication<NodeControl::ToggleState>
{
public:
    static const NodeControlToggleApplication INSTANCE;

    NodeControl::ToggleState get(std::shared_ptr<AtomicCounter> counter) override;

    bool apply(std::shared_ptr<AtomicCounter> counter, NodeControl::ToggleState targetState) override;

    std::shared_ptr<AtomicCounter> find(CountersReader& countersReader, std::int32_t clusterId) override;

    bool isNeutral(NodeControl::ToggleState toggleState) override;
};

}}

