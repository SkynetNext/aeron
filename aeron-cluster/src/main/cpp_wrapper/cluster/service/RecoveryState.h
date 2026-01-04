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

#include <vector>
#include <string>
#include "Aeron.h"
#include "Counter.h"
#include "concurrent/AtomicBuffer.h"
#include "concurrent/CountersReader.h"
#include "AeronCounters.h"
#include "util/BitUtil.h"
#include "../client/ClusterExceptions.h"

namespace aeron { namespace cluster { namespace service
{

using namespace aeron::concurrent;
using namespace aeron::util;

/**
 * Counter representing the Recovery State for the cluster.
 * 
 * Key layout as follows:
 * <pre>
 *   0                   1                   2                   3
 *   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 *  |                     Leadership Term ID                        |
 *  |                                                               |
 *  +---------------------------------------------------------------+
 *  |                  Log position for Snapshot                    |
 *  |                                                               |
 *  +---------------------------------------------------------------+
 *  |              Timestamp at beginning of Recovery               |
 *  |                                                               |
 *  +---------------------------------------------------------------+
 *  |                         Cluster ID                            |
 *  +---------------------------------------------------------------+
 *  |                     Count of Services                         |
 *  +---------------------------------------------------------------+
 *  |             Snapshot Recording ID (Service ID 0)              |
 *  |                                                               |
 *  +---------------------------------------------------------------+
 *  |             Snapshot Recording ID (Service ID n)              |
 *  |                                                               |
 *  +---------------------------------------------------------------+
 * </pre>
 */
class RecoveryState
{
public:
    /**
     * Type id of a recovery state counter.
     */
    static constexpr std::int32_t RECOVERY_STATE_TYPE_ID = AeronCounters::CLUSTER_RECOVERY_STATE_TYPE_ID;

    /**
     * Human-readable name for the counter.
     */
    static constexpr const char* NAME = "Cluster recovery: leadershipTermId=";

    /**
     * Offset of the term-id field.
     */
    static constexpr std::int32_t LEADERSHIP_TERM_ID_OFFSET = 0;
    
    /**
     * Offset of the log-position field.
     */
    static constexpr std::int32_t LOG_POSITION_OFFSET = LEADERSHIP_TERM_ID_OFFSET + sizeof(std::int64_t);
    
    /**
     * Offset of the timestamp field.
     */
    static constexpr std::int32_t TIMESTAMP_OFFSET = LOG_POSITION_OFFSET + sizeof(std::int64_t);
    
    /**
     * Offset of the cluster-id field.
     */
    static constexpr std::int32_t CLUSTER_ID_OFFSET = TIMESTAMP_OFFSET + sizeof(std::int64_t);
    
    /**
     * Offset of the service-count field.
     */
    static constexpr std::int32_t SERVICE_COUNT_OFFSET = CLUSTER_ID_OFFSET + sizeof(std::int32_t);
    
    /**
     * Offset of the snapshot-recording-ids field.
     */
    static constexpr std::int32_t SNAPSHOT_RECORDING_IDS_OFFSET = SERVICE_COUNT_OFFSET + sizeof(std::int32_t);

private:
    RecoveryState() = delete;

public:
    /**
     * Allocate a counter to represent the snapshot services should load on start.
     *
     * @param aeron                to allocate the counter.
     * @param leadershipTermId     at which the snapshot was taken.
     * @param logPosition          at which the snapshot was taken.
     * @param timestamp            the snapshot was taken.
     * @param clusterId            which identifies the cluster instance.
     * @param snapshotRecordingIds for the services to use during recovery indexed by service id.
     * @return the Counter for the recovery state.
     */
    static std::shared_ptr<Counter> allocate(
        std::shared_ptr<Aeron> aeron,
        std::int64_t leadershipTermId,
        std::int64_t logPosition,
        std::int64_t timestamp,
        std::int32_t clusterId,
        const std::vector<std::int64_t>& snapshotRecordingIds)
    {
        // Use a vector as expandable buffer
        std::vector<std::uint8_t> bufferData(256);
        AtomicBuffer buffer(bufferData.data(), bufferData.size());

        buffer.putInt64(LEADERSHIP_TERM_ID_OFFSET, leadershipTermId);
        buffer.putInt64(LOG_POSITION_OFFSET, logPosition);
        buffer.putInt64(TIMESTAMP_OFFSET, timestamp);
        buffer.putInt32(CLUSTER_ID_OFFSET, clusterId);

        const std::int32_t serviceCount = static_cast<std::int32_t>(snapshotRecordingIds.size());
        buffer.putInt32(SERVICE_COUNT_OFFSET, serviceCount);

        const std::int32_t keyLength = SNAPSHOT_RECORDING_IDS_OFFSET + (serviceCount * static_cast<std::int32_t>(sizeof(std::int64_t)));
        if (keyLength > CountersReader::MAX_KEY_LENGTH)
        {
            throw ClusterException(
                std::to_string(keyLength) + " exceeds max key length " + std::to_string(CountersReader::MAX_KEY_LENGTH),
                SOURCEINFO);
        }

        for (std::int32_t i = 0; i < serviceCount; i++)
        {
            buffer.putInt64(SNAPSHOT_RECORDING_IDS_OFFSET + (i * static_cast<std::int32_t>(sizeof(std::int64_t))), snapshotRecordingIds[i]);
        }

        const std::int32_t labelOffset = static_cast<std::int32_t>(BitUtil::align(keyLength, sizeof(std::int32_t)));
        std::int32_t labelLength = 0;
        
        std::string nameStr = NAME;
        labelLength += buffer.putStringWithoutLength(labelOffset + labelLength, nameStr);
        
        std::string termIdStr = std::to_string(leadershipTermId);
        labelLength += buffer.putStringWithoutLength(labelOffset + labelLength, termIdStr);
        
        std::string logPosStr = " logPosition=";
        labelLength += buffer.putStringWithoutLength(labelOffset + labelLength, logPosStr);
        
        std::string logPosValueStr = std::to_string(logPosition);
        labelLength += buffer.putStringWithoutLength(labelOffset + labelLength, logPosValueStr);
        
        std::string clusterIdStr = " clusterId=";
        labelLength += buffer.putStringWithoutLength(labelOffset + labelLength, clusterIdStr);
        
        std::string clusterIdValueStr = std::to_string(clusterId);
        labelLength += buffer.putStringWithoutLength(labelOffset + labelLength, clusterIdValueStr);

        std::string label = buffer.getStringWithoutLength(labelOffset, static_cast<std::size_t>(labelLength));
        
        std::int64_t registrationId = aeron->addCounter(
            RECOVERY_STATE_TYPE_ID,
            buffer.buffer(),
            keyLength,
            label);
        return aeron->findCounter(registrationId);
    }

    /**
     * Find the active counter id for recovery state.
     *
     * @param counters  to search within.
     * @param clusterId to constrain the search.
     * @return the counter id if found otherwise CountersReader::NULL_COUNTER_ID.
     */
    static std::int32_t findCounterId(CountersReader& counters, std::int32_t clusterId)
    {
        AtomicBuffer buffer = counters.metaDataBuffer();

        for (std::int32_t i = 0, size = counters.maxCounterId(); i < size; i++)
        {
            const std::int32_t counterState = counters.getCounterState(i);
            if (counterState == CountersReader::RECORD_ALLOCATED && 
                counters.getCounterTypeId(i) == RECOVERY_STATE_TYPE_ID)
            {
                if (buffer.getInt32(CountersReader::metadataOffset(i) + CountersReader::KEY_OFFSET + CLUSTER_ID_OFFSET) == clusterId)
                {
                    return i;
                }
            }
            else if (CountersReader::RECORD_UNUSED == counterState)
            {
                break;
            }
        }

        return CountersReader::NULL_COUNTER_ID;
    }

    /**
     * Get the leadership term id for the snapshot state. Aeron::NULL_VALUE if no snapshot for recovery.
     *
     * @param counters  to search within.
     * @param counterId for the active recovery counter.
     * @return the leadership term id if found otherwise Aeron::NULL_VALUE.
     */
    static std::int64_t getLeadershipTermId(CountersReader& counters, std::int32_t counterId)
    {
        AtomicBuffer buffer = counters.metaDataBuffer();

        if (counters.getCounterState(counterId) == CountersReader::RECORD_ALLOCATED &&
            counters.getCounterTypeId(counterId) == RECOVERY_STATE_TYPE_ID)
        {
            return buffer.getInt64(CountersReader::metadataOffset(counterId) + CountersReader::KEY_OFFSET + LEADERSHIP_TERM_ID_OFFSET);
        }

        return aeron::NULL_VALUE;
    }

    /**
     * Get the position at which the snapshot was taken. Aeron::NULL_VALUE if no snapshot for recovery.
     *
     * @param counters  to search within.
     * @param counterId for the active recovery counter.
     * @return the log position if found otherwise Aeron::NULL_VALUE.
     */
    static std::int64_t getLogPosition(CountersReader& counters, std::int32_t counterId)
    {
        AtomicBuffer buffer = counters.metaDataBuffer();

        if (counters.getCounterState(counterId) == CountersReader::RECORD_ALLOCATED &&
            counters.getCounterTypeId(counterId) == RECOVERY_STATE_TYPE_ID)
        {
            return buffer.getInt64(CountersReader::metadataOffset(counterId) + CountersReader::KEY_OFFSET + LOG_POSITION_OFFSET);
        }

        return aeron::NULL_VALUE;
    }

    /**
     * Get the timestamp at the beginning of recovery. Aeron::NULL_VALUE if no snapshot for recovery.
     *
     * @param counters  to search within.
     * @param counterId for the active recovery counter.
     * @return the timestamp if found otherwise Aeron::NULL_VALUE.
     */
    static std::int64_t getTimestamp(CountersReader& counters, std::int32_t counterId)
    {
        AtomicBuffer buffer = counters.metaDataBuffer();

        if (counters.getCounterState(counterId) == CountersReader::RECORD_ALLOCATED &&
            counters.getCounterTypeId(counterId) == RECOVERY_STATE_TYPE_ID)
        {
            return buffer.getInt64(CountersReader::metadataOffset(counterId) + CountersReader::KEY_OFFSET + TIMESTAMP_OFFSET);
        }

        return aeron::NULL_VALUE;
    }

    /**
     * Get the recording id of the snapshot for a service.
     *
     * @param counters  to search within.
     * @param counterId for the active recovery counter.
     * @param serviceId for the snapshot required.
     * @return the count of replay terms if found otherwise Aeron::NULL_VALUE.
     */
    static std::int64_t getSnapshotRecordingId(CountersReader& counters, std::int32_t counterId, std::int32_t serviceId)
    {
        AtomicBuffer buffer = counters.metaDataBuffer();

        if (counters.getCounterState(counterId) == CountersReader::RECORD_ALLOCATED &&
            counters.getCounterTypeId(counterId) == RECOVERY_STATE_TYPE_ID)
        {
            const std::int32_t recordOffset = CountersReader::metadataOffset(counterId);

            const std::int32_t serviceCount = buffer.getInt32(recordOffset + CountersReader::KEY_OFFSET + SERVICE_COUNT_OFFSET);
            if (serviceId < 0 || serviceId >= serviceCount)
            {
                throw ClusterException(
                    "invalid serviceId " + std::to_string(serviceId) + " for count of " + std::to_string(serviceCount),
                    SOURCEINFO);
            }

            return buffer.getInt64(
                recordOffset + CountersReader::KEY_OFFSET + SNAPSHOT_RECORDING_IDS_OFFSET + 
                (serviceId * static_cast<std::int32_t>(sizeof(std::int64_t))));
        }

        throw ClusterException("active counter not found " + std::to_string(counterId), SOURCEINFO);
    }
};

}}}

