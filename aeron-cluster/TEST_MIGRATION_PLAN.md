# Aeron Cluster C++ 测试迁移计划

## 当前状态

### 已迁移测试（5个）

| C++ 测试文件 | Java 对应文件 | 状态 | 完成度 |
|------------|--------------|------|--------|
| `EgressPollerTest.cpp` | `EgressPollerTest.java` | ✅ 完整 | 100% |
| `EgressAdapterTest.cpp` | `EgressAdapterTest.java` | ✅ 完整 | 100% |
| `AeronClusterContextTest.cpp` | `AeronClusterContextTest.java` | ⚠️ 基本完整 | 90% |
| `AeronClusterAsyncConnectTest.cpp` | `AeronClusterAsyncConnectTest.java` | ⚠️ 部分实现 | 40% |
| `AeronClusterTest.cpp` | `AeronClusterTest.java` | ❌ 未实现 | 0% (全部跳过) |

**总体完成度**: 约 66% (3.3/5)

### 未迁移测试（约31个）

#### 客户端测试（已完成）
- ✅ 所有客户端测试已迁移

#### 服务端测试（4个）
1. `ClusteredServiceAgentTest.java`
2. `ClusteredServiceContainerContextTest.java`
3. `ClusterMarkFileTest.java`
4. `ServiceSnapshotTakerTest.java`

#### 核心模块测试（约27个）
1. `AuthenticationTest.java` - 认证测试
2. `ClusterBackupAgentTest.java` - 备份代理测试
3. `ClusterBackupContextTest.java` - 备份上下文测试
4. `ClusterMemberTest.java` - 集群成员测试
5. `ClusterNodeRestartTest.java` - 节点重启测试
6. `ClusterNodeTest.java` - 集群节点测试
7. `ClusterTimerTest.java` - 集群定时器测试
8. `ClusterWithNoServicesTest.java` - 无服务集群测试
9. `ConsensusModuleAgentTest.java` - 共识模块代理测试
10. `ConsensusModuleConfigurationTest.java` - 共识模块配置测试
11. `ConsensusModuleContextTest.java` - 共识模块上下文测试
12. `ConsensusModuleSnapshotTakerTest.java` - 共识模块快照测试
13. `ElectionTest.java` - 选举测试
14. `IngressAdapterTest.java` - 入口适配器测试
15. `LogSourceValidatorTest.java` - 日志源验证测试
16. `NameResolutionClusterNodeTest.java` - 名称解析测试
17. `NodeStateFileTest.java` - 节点状态文件测试
18. `PendingServiceMessageTrackerTest.java` - 待处理服务消息跟踪测试
19. `PriorityHeapTimerServiceTest.java` - 优先级堆定时器测试
20. `PriorityHeapTimerServiceClusterTimeTest.java` - 优先级堆定时器集群时间测试
21. `PublicationGroupTest.java` - 发布组测试
22. `RecordingLogTest.java` - 录制日志测试
23. `RecordingReplicationTest.java` - 录制复制测试
24. `SessionEventCodecCompatibilityTest.java` - 会话事件编解码兼容性测试
25. `SnapshotReplicationTest.java` - 快照复制测试
26. `StandbySnapshotReplicatorTest.java` - 备用快照复制器测试
27. `WheelTimerServiceClusterTimeTest.java` - 轮盘定时器集群时间测试

## 迁移优先级

### P0 - 高优先级（核心功能测试）
1. `ConsensusModuleAgentTest.java` - 核心共识模块测试
2. `ElectionTest.java` - 选举机制测试
3. `ClusterNodeTest.java` - 集群节点基本功能测试
4. `RecordingLogTest.java` - 录制日志测试（关键功能）
5. `RecordingReplicationTest.java` - 录制复制测试（关键功能）

### P1 - 中优先级（重要功能测试）
6. `AuthenticationTest.java` - 认证功能测试
7. `ClusterMemberTest.java` - 集群成员管理测试
8. `ConsensusModuleContextTest.java` - 上下文配置测试
9. `IngressAdapterTest.java` - 入口适配器测试
10. `PendingServiceMessageTrackerTest.java` - 消息跟踪测试

### P2 - 低优先级（辅助功能测试）
11. 其他定时器服务测试
12. 快照相关测试
13. 备份相关测试
14. 其他辅助功能测试

## 迁移步骤

### 阶段 1: 完善现有测试（1-2周）

#### 1.1 修复 `AeronClusterTest.cpp`
**问题**: 无法mock非虚类（Publication/Subscription/Image）

**解决方案选项**:
- **选项A（推荐）**: 重构 `AeronCluster` 使用接口
  - 创建 `IPublication`、`ISubscription`、`IImage` 接口
  - 让 `Publication`、`Subscription`、`Image` 实现这些接口
  - 使用 gmock 进行mock
  - **优点**: 符合面向接口编程，易于测试
  - **缺点**: 需要修改生产代码

- **选项B**: 使用测试替身（Test Doubles）
  - 创建可替换的测试类
  - 需要 friend 访问权限或修改类设计
  - **优点**: 不需要大幅修改生产代码
  - **缺点**: 可能违反封装原则

- **选项C**: 使用真实对象进行集成测试
  - 使用 `EmbeddedMediaDriver` 创建真实对象
  - **优点**: 测试真实行为
  - **缺点**: 测试速度慢，难以控制边界情况

**建议**: 采用选项A，但分阶段实施。先实现接口，然后逐步迁移。

#### 1.2 完善 `AeronClusterAsyncConnectTest.cpp`
- 实现完整的异步连接流程
- 添加超时和错误处理测试
- 添加多端点连接测试

#### 1.3 完善 `AeronClusterContextTest.cpp`
- 补充环境变量测试的完整实现
- 添加边界条件测试

### 阶段 2: 迁移核心模块测试（4-6周）

#### 2.1 迁移 P0 优先级测试
1. **ConsensusModuleAgentTest.cpp**
   - 对应 `ConsensusModuleAgentTest.java`
   - 测试共识模块代理的核心功能
   - 需要mock `ConsensusModuleAgent` 的依赖

2. **ElectionTest.cpp**
   - 对应 `ElectionTest.java`
   - 测试选举机制
   - 需要mock集群成员和网络通信

3. **ClusterNodeTest.cpp**
   - 对应 `ClusterNodeTest.java`
   - 测试集群节点的基本功能
   - 需要完整的集群环境

4. **RecordingLogTest.cpp**
   - 对应 `RecordingLogTest.java`
   - 测试录制日志功能
   - 需要mock Archive相关功能

5. **RecordingReplicationTest.cpp**
   - 对应 `RecordingReplicationTest.java`
   - 测试录制复制功能
   - 需要mock Archive相关功能

#### 2.2 迁移 P1 优先级测试
6. **AuthenticationTest.cpp**
7. **ClusterMemberTest.cpp**
8. **ConsensusModuleContextTest.cpp**
9. **IngressAdapterTest.cpp**
10. **PendingServiceMessageTrackerTest.cpp**

### 阶段 3: 迁移服务端测试（2-3周）

#### 3.1 服务端测试迁移
1. **ClusteredServiceAgentTest.cpp**
2. **ClusteredServiceContainerContextTest.cpp**
3. **ClusterMarkFileTest.cpp**
4. **ServiceSnapshotTakerTest.cpp**

### 阶段 4: 迁移辅助功能测试（3-4周）

#### 4.1 定时器服务测试
- `PriorityHeapTimerServiceTest.cpp`
- `PriorityHeapTimerServiceClusterTimeTest.cpp`
- `WheelTimerServiceClusterTimeTest.cpp`
- `ClusterTimerTest.cpp`

#### 4.2 快照相关测试
- `SnapshotReplicationTest.cpp`
- `StandbySnapshotReplicatorTest.cpp`
- `ConsensusModuleSnapshotTakerTest.cpp`

#### 4.3 备份相关测试
- `ClusterBackupAgentTest.cpp`
- `ClusterBackupContextTest.cpp`

#### 4.4 其他辅助测试
- `LogSourceValidatorTest.cpp`
- `NameResolutionClusterNodeTest.cpp`
- `NodeStateFileTest.cpp`
- `PublicationGroupTest.cpp`
- `SessionEventCodecCompatibilityTest.cpp`
- `ClusterWithNoServicesTest.cpp`
- `ClusterNodeRestartTest.cpp`
- `ConsensusModuleConfigurationTest.cpp`

## 技术挑战和解决方案

### 挑战 1: Mock非虚类
**问题**: C++无法直接mock非虚类（如 `Publication`、`Subscription`、`Image`）

**解决方案**: 
- 创建接口抽象层
- 使用依赖注入
- 使用测试替身模式

### 挑战 2: 异步操作测试
**问题**: Java使用Future/CompletableFuture，C++需要不同的异步模式

**解决方案**:
- 使用 `std::future` 和 `std::promise`
- 使用回调函数
- 使用条件变量进行同步

### 挑战 3: 线程安全测试
**问题**: 需要测试多线程环境下的行为

**解决方案**:
- 使用 `std::thread` 和 `std::mutex`
- 使用 `std::atomic` 进行同步
- 使用 `std::condition_variable` 进行等待

### 挑战 4: 时间相关测试
**问题**: 需要控制时间进行超时测试

**解决方案**:
- 创建可mock的时钟接口
- 使用 `std::chrono` 进行时间管理
- 在测试中注入时间控制

### 挑战 5: 文件系统操作测试
**问题**: 需要测试文件读写操作

**解决方案**:
- 使用临时目录
- 使用文件系统mock库（如 `fakefs`）
- 在测试后清理临时文件

## 测试框架使用

### GoogleTest/GMock
- 使用 `TEST_F` 进行测试用例定义
- 使用 `EXPECT_*` 和 `ASSERT_*` 进行断言
- 使用 `MOCK_METHOD` 进行mock定义
- 使用 `EmbeddedMediaDriver` 进行集成测试

### 测试结构
```cpp
class TestNameFixture : public testing::Test {
public:
    void SetUp() override {
        // 测试前准备
    }
    
    void TearDown() override {
        // 测试后清理
    }
    
protected:
    // 测试数据
};

TEST_F(TestNameFixture, testCaseName) {
    // 测试代码
}
```

## 时间估算

- **阶段1（完善现有测试）**: 1-2周
- **阶段2（核心模块测试）**: 4-6周
- **阶段3（服务端测试）**: 2-3周
- **阶段4（辅助功能测试）**: 3-4周

**总计**: 10-15周（约2.5-4个月）

## 验收标准

1. ✅ 所有P0优先级测试迁移完成并通过
2. ✅ 所有P1优先级测试迁移完成并通过
3. ✅ 测试覆盖率 >= 80%
4. ✅ 所有测试在Linux和Windows上都能通过
5. ✅ 测试执行时间 < 5分钟（所有测试）

## 注意事项

1. **1:1翻译原则**: 严格遵循Java测试的逻辑和断言
2. **测试隔离**: 每个测试应该独立，不依赖其他测试
3. **资源清理**: 确保测试后正确清理资源（文件、网络连接等）
4. **跨平台兼容**: 确保测试在Linux和Windows上都能运行
5. **性能考虑**: 避免测试执行时间过长
6. **文档更新**: 及时更新测试文档和注释

## 更新日志

- **2026-01-XX**: 初始版本，制定迁移计划
- **2026-01-XX**: 修复Linux编译错误（include路径问题）
