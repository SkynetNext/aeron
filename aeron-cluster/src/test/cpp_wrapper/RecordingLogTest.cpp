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

    // Note: Archive is not used in this test since we only have snapshots
    // and no log entries. Passing nullptr is safe here.
    std::shared_ptr<AeronArchive> mockArchive = nullptr;

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

    const auto &entries = recordingLog.entries();
    EXPECT_EQ(newPosition, entries[0].logPosition);
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

TEST_F(RecordingLogTest, shouldRemoveEntry) {
  {
    RecordingLog recordingLog(m_tempDir.string(), true);
    const std::int64_t recordingId = 1L;
    const std::int64_t leadershipTermId1 = 3L;
    const std::int64_t termBaseLogPosition1 = 2L;
    const std::int64_t timestamp1 = 4L;

    recordingLog.appendTerm(recordingId, leadershipTermId1,
                            termBaseLogPosition1, timestamp1);

    const std::int64_t leadershipTermId2 = 4L;
    const std::int64_t termBaseLogPosition2 = 3L;
    const std::int64_t timestamp2 = 5L;
    recordingLog.appendTerm(recordingId, leadershipTermId2,
                            termBaseLogPosition2, timestamp2);

    recordingLog.removeEntry(leadershipTermId2,
                             recordingLog.nextEntryIndex() - 1);
    EXPECT_EQ(1, recordingLog.entries().size());
  }

  {
    RecordingLog recordingLog(m_tempDir.string(), true);
    EXPECT_EQ(1, recordingLog.entries().size());
    EXPECT_EQ(2, recordingLog.nextEntryIndex());
  }
}

TEST_F(RecordingLogTest, shouldInvalidateLatestSnapshot) {
  const std::int64_t termBaseLogPosition = 0L;
  const std::int64_t logIncrement = 640L;
  std::int64_t leadershipTermId = 7L;
  std::int64_t logPosition = 0L;
  std::int64_t timestamp = 1000L;

  {
    RecordingLog recordingLog(m_tempDir.string(), true);
    recordingLog.appendTerm(1, leadershipTermId, termBaseLogPosition,
                            timestamp);

    timestamp += 1;
    logPosition += logIncrement;

    recordingLog.appendSnapshot(2, leadershipTermId, termBaseLogPosition,
                                logPosition, timestamp, 0);
    recordingLog.appendSnapshot(3, leadershipTermId, termBaseLogPosition,
                                logPosition, timestamp, SERVICE_ID);

    timestamp += 1;
    logPosition += logIncrement;

    recordingLog.appendSnapshot(4, leadershipTermId, termBaseLogPosition,
                                logPosition, timestamp, 0);
    recordingLog.appendSnapshot(5, leadershipTermId, termBaseLogPosition,
                                logPosition, timestamp, SERVICE_ID);

    leadershipTermId++;
    recordingLog.appendTerm(1, leadershipTermId, logPosition, timestamp);

    EXPECT_TRUE(recordingLog.invalidateLatestSnapshot());
    EXPECT_EQ(6, recordingLog.entries().size());
  }

  {
    RecordingLog recordingLog(m_tempDir.string(), true);
    EXPECT_EQ(6, recordingLog.entries().size());

    EXPECT_TRUE(recordingLog.entries()[0].isValid);
    EXPECT_TRUE(recordingLog.entries()[1].isValid);
    EXPECT_TRUE(recordingLog.entries()[2].isValid);
    EXPECT_FALSE(recordingLog.entries()[3].isValid);
    EXPECT_FALSE(recordingLog.entries()[4].isValid);
    EXPECT_TRUE(recordingLog.entries()[5].isValid);

    const auto &entries = recordingLog.entries();
    RecordingLog::Entry *latestServiceSnapshot =
        recordingLog.getLatestSnapshot(0);
    EXPECT_NE(nullptr, latestServiceSnapshot);
    EXPECT_EQ(&entries[1], latestServiceSnapshot);

    RecordingLog::Entry *latestCmSnapshot =
        recordingLog.getLatestSnapshot(SERVICE_ID);
    EXPECT_NE(nullptr, latestCmSnapshot);
    EXPECT_EQ(&entries[2], latestCmSnapshot);

    EXPECT_TRUE(recordingLog.invalidateLatestSnapshot());
    EXPECT_EQ(6, recordingLog.entries().size());
  }

  {
    RecordingLog recordingLog(m_tempDir.string(), true);
    EXPECT_EQ(6, recordingLog.entries().size());

    EXPECT_TRUE(recordingLog.entries()[0].isValid);
    EXPECT_FALSE(recordingLog.entries()[1].isValid);
    EXPECT_FALSE(recordingLog.entries()[2].isValid);
    EXPECT_FALSE(recordingLog.entries()[3].isValid);
    EXPECT_FALSE(recordingLog.entries()[4].isValid);
    EXPECT_TRUE(recordingLog.entries()[5].isValid);

    EXPECT_FALSE(recordingLog.invalidateLatestSnapshot());
    const auto &entries = recordingLog.entries();
    const auto termEntry = recordingLog.getTermEntry(leadershipTermId);
    EXPECT_NE(nullptr, termEntry);
    EXPECT_EQ(&entries[5], termEntry);
  }
}

TEST_F(RecordingLogTest, appendTermShouldNotAcceptDifferentRecordingIds) {
  {
    RecordingLog recordingLog(m_tempDir.string(), true);
    recordingLog.appendTerm(42, 0, 0, 0);

    EXPECT_THROW({ recordingLog.appendTerm(21, 1, 0, 0); }, ClusterException);
    EXPECT_EQ(1, recordingLog.entries().size());
  }

  {
    RecordingLog recordingLog(m_tempDir.string(), true);
    EXPECT_THROW(
        { recordingLog.appendTerm(-5, -5, -5, -5); }, ClusterException);
    EXPECT_EQ(1, recordingLog.entries().size());
  }
}

TEST_F(RecordingLogTest,
       appendTermShouldOnlyAllowASingleValidTermForTheSameLeadershipTermId) {
  RecordingLog recordingLog(m_tempDir.string(), true);
  recordingLog.appendTerm(8, 0, 0, 0);
  recordingLog.appendTerm(8, 1, 1, 1);

  recordingLog.invalidateEntry(0);
  recordingLog.appendTerm(8, 0, 100, 100);

  EXPECT_THROW({ recordingLog.appendTerm(8, 1, 5, 5); }, ClusterException);
  EXPECT_EQ(3, recordingLog.entries().size());
}

TEST_F(RecordingLogTest, shouldIgnoreInvalidTermInRecoveryPlan) {
  const int serviceCount = 1;
  const std::int64_t removedLeadershipTerm = 11L;

  {
    RecordingLog recordingLog(m_tempDir.string(), true);
    recordingLog.appendTerm(0L, 9L, 444, 0);
    recordingLog.appendTerm(0L, 10L, 666, 0);
    recordingLog.appendSnapshot(1L, 10L, 666, 777L, 0, 0);
    recordingLog.appendSnapshot(2L, 10L, 666, 777L, 0, SERVICE_ID);
    recordingLog.appendSnapshot(3L, 10L, 666, 888L, 0, 0);
    recordingLog.appendSnapshot(4L, 10L, 666, 888L, 0, SERVICE_ID);
    recordingLog.appendTerm(0L, removedLeadershipTerm, 999, 0);

    RecordingLog::Entry *lastTerm = recordingLog.findLastTerm();
    EXPECT_NE(nullptr, lastTerm);
    EXPECT_EQ(999L, lastTerm->termBaseLogPosition);

    recordingLog.invalidateEntry(6);
  }

  {
    RecordingLog recordingLog(m_tempDir.string(), true);
    // Note: Archive is not used in this test since the term is invalid.
    // Passing nullptr is safe here.
    std::shared_ptr<AeronArchive> mockArchive = nullptr;

    RecordingLog::RecoveryPlan recoveryPlan = recordingLog.createRecoveryPlan(
        mockArchive, serviceCount, aeron::NULL_VALUE);
    EXPECT_EQ(0L, recoveryPlan.log->recordingId);
    EXPECT_EQ(10L, recoveryPlan.log->leadershipTermId);
    EXPECT_EQ(666, recoveryPlan.log->termBaseLogPosition);

    RecordingLog::Entry *lastTerm = recordingLog.findLastTerm();
    EXPECT_NE(nullptr, lastTerm);
    EXPECT_EQ(0L, lastTerm->recordingId);
    EXPECT_EQ(0L, recordingLog.findLastTermRecordingId());
    EXPECT_TRUE(recordingLog.isUnknown(removedLeadershipTerm));
    EXPECT_EQ(aeron::NULL_VALUE,
              recordingLog.getTermTimestamp(removedLeadershipTerm));

    EXPECT_THROW(
        { recordingLog.getTermEntry(removedLeadershipTerm); },
        ClusterException);
    EXPECT_THROW(
        { recordingLog.commitLogPosition(removedLeadershipTerm, 99L); },
        ClusterException);
  }
}

TEST_F(RecordingLogTest, shouldIgnoreInvalidMidSnapshotInRecoveryPlan) {
  const int serviceCount = 1;

  {
    RecordingLog recordingLog(m_tempDir.string(), true);
    recordingLog.appendSnapshot(1, 1L, 0, 777L, 0, 0);
    recordingLog.appendSnapshot(2, 1L, 0, 777L, 0, SERVICE_ID);
    recordingLog.appendSnapshot(3, 1L, 0, 888L, 0, 0);
    recordingLog.appendSnapshot(4, 1L, 0, 888L, 0, SERVICE_ID);
    recordingLog.appendSnapshot(5, 1L, 0, 999L, 0, 0);
    recordingLog.appendSnapshot(6, 1L, 0, 999L, 0, SERVICE_ID);

    recordingLog.invalidateEntry(2);
    recordingLog.invalidateEntry(3);
  }

  {
    RecordingLog recordingLog(m_tempDir.string(), true);
    // Note: Archive is not used in this test since we only have snapshots
    // and no log entries. Passing nullptr is safe here.
    std::shared_ptr<AeronArchive> mockArchive = nullptr;
    RecordingLog::RecoveryPlan recoveryPlan = recordingLog.createRecoveryPlan(
        mockArchive, serviceCount, aeron::NULL_VALUE);
    EXPECT_EQ(2, recoveryPlan.snapshots.size());
    EXPECT_EQ(SERVICE_ID, recoveryPlan.snapshots[0].serviceId);
    EXPECT_EQ(6L, recoveryPlan.snapshots[0].recordingId);
    EXPECT_EQ(0, recoveryPlan.snapshots[1].serviceId);
    EXPECT_EQ(5L, recoveryPlan.snapshots[1].recordingId);
  }
}

TEST_F(RecordingLogTest, shouldNotAllowInvalidateOfSnapshotWithoutParentTerm) {
  RecordingLog recordingLog(m_tempDir.string(), true);
  recordingLog.appendSnapshot(-10, 1L, 0, 777L, 0, 0);
  recordingLog.appendSnapshot(-11, 1L, 0, 777L, 0, SERVICE_ID);

  EXPECT_THROW({ recordingLog.invalidateLatestSnapshot(); }, ClusterException);
}

TEST_F(RecordingLogTest, typeAsString) {
  EXPECT_EQ("TERM", RecordingLog::typeAsString(RecordingLog::ENTRY_TYPE_TERM));
  EXPECT_EQ("SNAPSHOT",
            RecordingLog::typeAsString(RecordingLog::ENTRY_TYPE_SNAPSHOT));
  EXPECT_EQ("UNKNOWN", RecordingLog::typeAsString(-5));
  EXPECT_EQ("UNKNOWN", RecordingLog::typeAsString(36542364));
}
