/*
 * Copyright 2014-2025 Justin Zhu.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once

#include <string>
#include "Aeron.h"
#include "Counter.h"
#include "concurrent/AtomicBuffer.h"
#include "concurrent/CountersReader.h"
#include "AeronCounters.h"
#include "../client/ClusterExceptions.h"

namespace aeron { namespace cluster { namespace service
{

using namespace aeron::concurrent;
using namespace aeron::util;

/**
 * For allocating and finding cluster associated counters identified by clusterId.
 */
class ClusterCounters
{
public:
    /**
     * Suffix for clusterId in the counter label.
     */
    static constexpr const char* CLUSTER_ID_LABEL_SUFFIX = " - clusterId=";
    static constexpr const char* SERVICE_ID_SUFFIX = " serviceId=";

private:
    ClusterCounters() = delete;

public:
    /**
     * Allocate a counter to represent component state within a cluster.
     *
     * @param aeron      to allocate the counter.
     * @param tempBuffer temporary storage to create label and metadata.
     * @param name       of the counter for the label.
     * @param typeId     for the counter.
     * @param clusterId  to which the allocated counter belongs.
     * @return the new Counter.
     */
    static std::shared_ptr<Counter> allocate(
        std::shared_ptr<Aeron> aeron,
        AtomicBuffer& tempBuffer,
        const std::string& name,
        std::int32_t typeId,
        std::int32_t clusterId)
    {
        std::int32_t index = 0;
        tempBuffer.putInt32(index, clusterId);
        index += sizeof(std::int32_t);
        const std::int32_t keyLength = index;

        index += tempBuffer.putStringWithoutLength(index, name);
        index += tempBuffer.putStringWithoutLength(index, CLUSTER_ID_LABEL_SUFFIX);
        // TODO: Implement putIntAscii equivalent
        std::string clusterIdStr = std::to_string(clusterId);
        index += tempBuffer.putStringWithoutLength(index, clusterIdStr);

        // Create label string from buffer
        std::string label;
        label.reserve(index - keyLength);
        for (std::int32_t i = keyLength; i < index; i++)
        {
            label += static_cast<char>(tempBuffer.getUInt8(i));
        }
        
        std::int64_t registrationId = aeron->addCounter(
            typeId,
            tempBuffer.buffer(),
            keyLength,
            label);
        return aeron->findCounter(registrationId);
    }

    /**
     * Allocate a counter to represent component state within a cluster and append a version info to its label.
     *
     * @param aeron          to allocate the counter.
     * @param tempBuffer     temporary storage to create label and metadata.
     * @param name           of the counter for the label.
     * @param typeId         for the counter.
     * @param clusterId      to which the allocated counter belongs.
     * @param version        of the component.
     * @param commitHashCode Git commit SHA of the component.
     * @return the new Counter.
     */
    static std::shared_ptr<Counter> allocateVersioned(
        std::shared_ptr<Aeron> aeron,
        AtomicBuffer& tempBuffer,
        const std::string& name,
        std::int32_t typeId,
        std::int32_t clusterId,
        const std::string& version,
        const std::string& commitHashCode)
    {
        std::int32_t index = 0;
        tempBuffer.putInt32(index, clusterId);
        index += sizeof(std::int32_t);
        const std::int32_t keyLength = index;

        index += tempBuffer.putStringWithoutLength(index, name);
        index += tempBuffer.putStringWithoutLength(index, CLUSTER_ID_LABEL_SUFFIX);
        std::string clusterIdStr = std::to_string(clusterId);
        index += tempBuffer.putStringWithoutLength(index, clusterIdStr);
        
        // TODO: Implement appendVersionInfo equivalent
        std::string versionInfo = " version=" + version + " commit=" + commitHashCode;
        index += tempBuffer.putStringWithoutLength(index, versionInfo);

        std::string label = tempBuffer.getStringWithoutLength(keyLength, static_cast<std::size_t>(index - keyLength));
        std::int64_t registrationId = aeron->addCounter(
            typeId,
            tempBuffer.buffer(),
            keyLength,
            label);
        return aeron->findCounter(registrationId);
    }

    /**
     * Find the counter id for a type of counter in a cluster.
     *
     * @param counters  to search within.
     * @param typeId    of the counter.
     * @param clusterId to which the allocated counter belongs.
     * @return the matching counter id or Aeron::NULL_VALUE if not found.
     */
    static std::int32_t find(CountersReader& counters, std::int32_t typeId, std::int32_t clusterId)
    {
        AtomicBuffer buffer = counters.metaDataBuffer();

        for (std::int32_t i = 0, size = counters.maxCounterId(); i < size; i++)
        {
            const std::int32_t counterState = counters.getCounterState(i);

            if (counterState == CountersReader::RECORD_ALLOCATED)
            {
                if (counters.getCounterTypeId(i) == typeId &&
                    buffer.getInt32(CountersReader::metadataOffset(i) + CountersReader::KEY_OFFSET) == clusterId)
                {
                    return i;
                }
            }
            else if (CountersReader::RECORD_UNUSED == counterState)
            {
                break;
            }
        }

        return aeron::NULL_VALUE;
    }

    /**
     * Allocate a counter to represent component state within a cluster.
     *
     * @param aeron      to allocate the counter.
     * @param tempBuffer temporary storage to create label and metadata.
     * @param name       of the counter for the label.
     * @param typeId     for the counter.
     * @param clusterId  to which the allocated counter belongs.
     * @param serviceId  to which the allocated counter belongs.
     * @return the Counter for the commit position.
     */
    static std::shared_ptr<Counter> allocateServiceCounter(
        std::shared_ptr<Aeron> aeron,
        AtomicBuffer& tempBuffer,
        const std::string& name,
        std::int32_t typeId,
        std::int32_t clusterId,
        std::int32_t serviceId)
    {
        std::int32_t index = 0;
        tempBuffer.putInt32(index, clusterId);
        index += sizeof(std::int32_t);
        tempBuffer.putInt32(index, serviceId);
        index += sizeof(std::int32_t);
        const std::int32_t keyLength = index;

        index += tempBuffer.putStringWithoutLength(index, name);
        index += tempBuffer.putStringWithoutLength(index, CLUSTER_ID_LABEL_SUFFIX);
        std::string clusterIdStr = std::to_string(clusterId);
        index += tempBuffer.putStringWithoutLength(index, clusterIdStr);
        index += tempBuffer.putStringWithoutLength(index, SERVICE_ID_SUFFIX);
        std::string serviceIdStr = std::to_string(serviceId);
        index += tempBuffer.putStringWithoutLength(index, serviceIdStr);

        std::string label = tempBuffer.getStringWithoutLength(keyLength, static_cast<std::size_t>(index - keyLength));
        std::int64_t registrationId = aeron->addCounter(
            typeId,
            tempBuffer.buffer(),
            keyLength,
            label);
        return aeron->findCounter(registrationId);
    }

    /**
     * Allocate a service error counter.
     */
    static std::shared_ptr<Counter> allocateServiceErrorCounter(
        std::shared_ptr<Aeron> aeron,
        AtomicBuffer& tempBuffer,
        std::int32_t clusterId,
        std::int32_t serviceId,
        const std::string& version,
        const std::string& gitSha)
    {
        std::int32_t index = 0;
        tempBuffer.putInt32(index, clusterId);
        index += sizeof(std::int32_t);
        tempBuffer.putInt32(index, serviceId);
        index += sizeof(std::int32_t);
        const std::int32_t keyLength = index;

        index += tempBuffer.putStringWithoutLength(index, "Cluster Container Errors");
        index += tempBuffer.putStringWithoutLength(index, CLUSTER_ID_LABEL_SUFFIX);
        std::string clusterIdStr = std::to_string(clusterId);
        index += tempBuffer.putStringWithoutLength(index, clusterIdStr);
        index += tempBuffer.putStringWithoutLength(index, SERVICE_ID_SUFFIX);
        std::string serviceIdStr = std::to_string(serviceId);
        index += tempBuffer.putStringWithoutLength(index, serviceIdStr);
        
        // TODO: Implement appendVersionInfo equivalent
        std::string versionInfo = " version=" + version + " commit=" + gitSha;
        index += tempBuffer.putStringWithoutLength(index, versionInfo);

        return aeron->addCounter(
            AeronCounters::CLUSTER_CLUSTERED_SERVICE_ERROR_COUNT_TYPE_ID,
            tempBuffer,
            0,
            keyLength,
            tempBuffer,
            keyLength,
            index - keyLength);
    }
};

}}}

