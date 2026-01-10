/*
 * Copyright 2014-2025 Real Logic Limited.
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

#include <filesystem>
#include <fstream>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <memory>
#include <vector>

#include "client/archive/AeronArchive.h"
#include "cluster/ConsensusModuleAgent.h"
#include "cluster/RecordingLog.h"
#include "cluster/client/ClusterExceptions.h"

using namespace aeron;
using namespace aeron::cluster;
using namespace aeron::archive::client;
using namespace testing;

class RecordingLogTest : public testing::Test {
public:
  RecordingLogTest() {
    // Create temporary directory for each test
    m_tempDir = std::filesystem::temp_directory_path() /
                ("RecordingLogTest_" + std::to_string(std::time(nullptr)));
    std::filesystem::create_directories(m_tempDir);
  }

  ~RecordingLogTest() override {
    // Clean up temporary directory
    if (std::filesystem::exists(m_tempDir)) {
      std::filesystem::remove_all(m_tempDir);
    }
  }

protected:
  std::filesystem::path m_tempDir;
  static constexpr std::int64_t RECORDING_ID = 9234236;
  static constexpr std::int32_t SERVICE_ID =
      aeron::cluster::ConsensusModuleAgent::SERVICE_ID;
};

TEST_F(RecordingLogTest, shouldCreateNewIndex) {
  RecordingLog recordingLog(m_tempDir.string(), true);
  EXPECT_EQ(0, recordingLog.entries().size());
}

TEST_F(RecordingLogTest, shouldAppendAndThenReloadLatestSnapshot) {
  const RecordingLog::Entry expectedEntry(1, 3, 2, 777, 4, aeron::NULL_VALUE,
                                          RecordingLog::ENTRY_TYPE_SNAPSHOT,
                                          nullptr, true, 0);

  {
    RecordingLog recordingLog(m_tempDir.string(), true);
    recordingLog.appendSnapshot(expectedEntry.recordingId,
                                expectedEntry.leadershipTermId,
                                expectedEntry.termBaseLogPosition, 777,
                                expectedEntry.timestamp, SERVICE_ID);
  }

  {
    RecordingLog recordingLog(m_tempDir.string(), true);
    EXPECT_EQ(1, recordingLog.entries().size());

    RecordingLog::Entry *snapshot = recordingLog.getLatestSnapshot(SERVICE_ID);
    EXPECT_NE(nullptr, snapshot);
    EXPECT_EQ(expectedEntry.recordingId, snapshot->recordingId);
    EXPECT_EQ(expectedEntry.leadershipTermId, snapshot->leadershipTermId);
    EXPECT_EQ(expectedEntry.termBaseLogPosition, snapshot->termBaseLogPosition);
    EXPECT_EQ(expectedEntry.logPosition, snapshot->logPosition);
    EXPECT_EQ(expectedEntry.timestamp, snapshot->timestamp);
    EXPECT_EQ(SERVICE_ID, snapshot->serviceId);
    EXPECT_EQ(RecordingLog::ENTRY_TYPE_SNAPSHOT, snapshot->type);
    EXPECT_TRUE(snapshot->isValid);
  }
}

TEST_F(RecordingLogTest, shouldIgnoreIncompleteSnapshotInRecoveryPlan) {
  const int serviceCount = 1;

  {
    RecordingLog recordingLog(m_tempDir.string(), true);
    recordingLog.appendSnapshot(1L, 1L, 0, 777L, 0, 0);
    recordingLog.appendSnapshot(2L, 1L, 0, 777L, 0, SERVICE_ID);
    recordingLog.appendSnapshot(3L, 1L, 0, 888L, 0, 0);
  }

  {
    RecordingLog recordingLog(m_tempDir.string(), true);
    EXPECT_EQ(3, recordingLog.entries().size());

    // Create mock archive
    auto mockArchive = std::make_shared<AeronArchive>();
    // Note: In real implementation, we would mock AeronArchive methods
    // For now, we'll test the basic structure

    RecordingLog::RecoveryPlan recoveryPlan = recordingLog.createRecoveryPlan(
        mockArchive, serviceCount, aeron::NULL_VALUE);
    EXPECT_EQ(2, recoveryPlan.snapshots.size());
    EXPECT_EQ(SERVICE_ID, recoveryPlan.snapshots[0].serviceId);
    EXPECT_EQ(2L, recoveryPlan.snapshots[0].recordingId);
    EXPECT_EQ(0, recoveryPlan.snapshots[1].serviceId);
    EXPECT_EQ(1L, recoveryPlan.snapshots[1].recordingId);
  }
}

TEST_F(RecordingLogTest, shouldAppendAndThenCommitTermPosition) {
  const std::int64_t newPosition = 9999L;
  {
    RecordingLog recordingLog(m_tempDir.string(), true);
    const std::int64_t recordingId = 1L;
    const std::int64_t leadershipTermId = 1111L;
    const std::int64_t logPosition = 2222L;
    const std::int64_t timestamp = 3333L;

    recordingLog.appendTerm(recordingId, leadershipTermId, logPosition,
                            timestamp);
    recordingLog.commitLogPosition(leadershipTermId, newPosition);
  }

  {
    RecordingLog recordingLog(m_tempDir.string(), true);
    EXPECT_EQ(1, recordingLog.entries().size());

    const RecordingLog::Entry &actualEntry = recordingLog.entries()[0];
    EXPECT_EQ(newPosition, actualEntry.logPosition);
  }
}

TEST_F(RecordingLogTest, appendTermShouldRejectNullValueAsRecordingId) {
  RecordingLog recordingLog(m_tempDir.string(), true);
  EXPECT_THROW(
      { recordingLog.appendTerm(aeron::NULL_VALUE, 0, 0, 0); },
      ClusterException);
  EXPECT_EQ(0, recordingLog.entries().size());
}

TEST_F(RecordingLogTest, appendSnapshotShouldRejectNullValueAsRecordingId) {
  RecordingLog recordingLog(m_tempDir.string(), true);
  EXPECT_THROW(
      { recordingLog.appendSnapshot(aeron::NULL_VALUE, 0, 0, 0, 0, 0); },
      ClusterException);
  EXPECT_EQ(0, recordingLog.entries().size());
}

TEST_F(RecordingLogTest, typeAsString) {
  EXPECT_EQ("TERM", RecordingLog::typeAsString(RecordingLog::ENTRY_TYPE_TERM));
  EXPECT_EQ("SNAPSHOT",
            RecordingLog::typeAsString(RecordingLog::ENTRY_TYPE_SNAPSHOT));
  EXPECT_EQ("UNKNOWN", RecordingLog::typeAsString(-5));
  EXPECT_EQ("UNKNOWN", RecordingLog::typeAsString(36542364));
}
