# Aeron Cluster C++ 移植计划

## 项目概述

本项目旨在将 Java 版本的 `aeron-cluster` **1:1 移植**到 C++ 版本，与 `aeron-client` 的 `cpp_wrapper` 代码组织、编译方式等保持一致。

**仓库地址**: https://github.com/SkynetNext/aeron

## ⚠️ 重要原则：1:1 翻译

**本项目采用 1:1 翻译原则**：
- **功能对等**: Java 版本的每个功能都要在 C++ 版本中有对应实现
- **API 对等**: C++ API 设计要与 Java API 保持功能对等（允许 C++ 风格的命名和用法）
- **行为一致**: C++ 版本的行为要与 Java 版本保持一致
- **协议兼容**: 生成的 codecs 必须与 Java 版本二进制兼容

## 目标

1. **功能完整性**: 完整移植 Java 版本 `aeron-cluster` 的核心功能到 C++（1:1 翻译）
2. **代码一致性**: 与 `aeron-client/cpp_wrapper` 的代码组织、命名规范、编译方式保持一致
3. **性能优化**: 保持 Aeron 的高性能特性，确保 C++ 版本性能不劣于 Java 版本
4. **API 兼容性**: 提供与 Java 版本功能对等的 C++ API

## 参考架构

### aeron-client cpp_wrapper 组织方式

```
aeron-client/src/main/cpp_wrapper/
├── CMakeLists.txt              # Header-only INTERFACE 库配置
├── Aeron.h                     # 主入口类
├── Publication.h
├── Subscription.h
├── Context.h
├── concurrent/                 # 并发相关工具
│   ├── AgentRunner.h
│   ├── AgentInvoker.h
│   ├── AtomicBuffer.h
│   └── ...
├── util/                       # 工具类
│   ├── Exceptions.h           # 异常处理（继承 std::exception）
│   ├── StringUtil.h
│   └── ...
└── status/                     # 状态相关
```

**关键特性**:
- **Header-only 库**: 使用 CMake `INTERFACE` 库，所有代码在头文件中
- **C++20 标准**: 使用 C++20 特性
- **异常处理**: 使用 `std::exception` 及其派生类（`util/Exceptions.h`）
- **命名空间**: 使用 `namespace aeron`
- **依赖关系**: 依赖 C API (`aeronc.h`)

### aeron-archive cpp_wrapper 组织方式（参考）

```
aeron-archive/src/main/cpp_wrapper/
├── CMakeLists.txt
└── client/
    ├── archive/
    │   ├── AeronArchive.h
    │   ├── ArchiveContext.h
    │   └── ...
    └── util/
        └── ArchiveExceptions.h
```

## 代码约束和规范

### 1. 编译选项

- **C++ 标准**: **C++20**（默认）
- **RTTI**: 根据 `aeron-client` 实际情况决定（需要确认是否禁用）
- **异常**: **使用异常**（`aeron-client` 使用 `std::exception`）
- **编译器**: 支持 GCC、Clang、MSVC（需支持 C++20）

### 2. 代码风格

- **Header-only**: 所有实现代码在 `.h` 文件中
- **命名规范**: 
  - 类名：PascalCase（如 `AeronCluster`）
  - 成员变量：`m_` 前缀（如 `m_context`）
  - 函数名：camelCase（如 `connect()`）
- **命名空间**: `namespace aeron::cluster` 或 `namespace aeron::cluster::client`
- **异常**: 使用 `aeron::util::Exceptions.h` 中定义的异常类型
- **代码注释**: **必须使用英文**（与 `aeron-client` 保持一致）
- **文档语言**: 所有技术文档、API 文档、README 等**必须使用英文**

### 3. 依赖关系

- **必需依赖**:
  - `aeron_client_wrapper` (aeron-client cpp_wrapper)
  - `aeron` (C API)
  - `aeron_archive_wrapper` (如果使用 Archive 功能)
- **测试依赖**:
  - `googletest` (gtest/gmock) - 用于单元测试和集成测试
  - `aeron_driver` - 用于嵌入式媒体驱动测试
- **可选依赖**:
  - SBE codecs（需要从 Java 生成的 codecs 移植或重新生成）

## 移植计划

### 阶段 1: 基础设施搭建

#### 1.1 目录结构创建

```
aeron-cluster/src/main/cpp_wrapper/
├── CMakeLists.txt
├── client/
│   ├── cluster/
│   │   ├── AeronCluster.h          # 主客户端类
│   │   ├── ClusterContext.h       # 上下文配置
│   │   ├── EgressPoller.h         # Egress 消息轮询
│   │   ├── EgressListener.h        # Egress 监听器接口
│   │   └── ...
│   └── util/
│       └── ClusterExceptions.h    # Cluster 特定异常
└── service/                        # 服务端（可选，后续阶段）
    └── ...
```

#### 1.2 CMakeLists.txt 配置

参考 `aeron-archive/src/main/cpp_wrapper/CMakeLists.txt`，创建类似的配置：

```cmake
# Header-only library
add_library(aeron_cluster_wrapper INTERFACE)
add_library(aeron::aeron_cluster_wrapper ALIAS aeron_cluster_wrapper)

target_include_directories(aeron_cluster_wrapper
    INTERFACE
    "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>"
    "$<BUILD_INTERFACE:${AERON_CLIENT_WRAPPER_SOURCE_PATH}>"
    "$<INSTALL_INTERFACE:include/wrapper>"
)

target_link_libraries(aeron_cluster_wrapper 
    INTERFACE 
    aeron_client_wrapper
    ${CMAKE_THREAD_LIBS_INIT}
)
```

#### 1.3 根 CMakeLists.txt 集成

在根 `CMakeLists.txt` 中添加：

```cmake
# cluster source
set(AERON_CLUSTER_WRAPPER_SOURCE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/aeron-cluster/src/main/cpp_wrapper")

if (BUILD_AERON_CLUSTER_API)
    add_subdirectory(${AERON_CLUSTER_WRAPPER_SOURCE_PATH})
    if (AERON_TESTS)
        set(AERON_CLUSTER_WRAPPER_TEST_PATH "${CMAKE_CURRENT_SOURCE_DIR}/aeron-cluster/src/test/cpp_wrapper")
        add_subdirectory(${AERON_CLUSTER_WRAPPER_TEST_PATH})
    endif ()
endif (BUILD_AERON_CLUSTER_API)
```

### 阶段 2: 客户端 API 移植

#### 2.1 核心类映射

| Java 类 | C++ 类 | 优先级 | 说明 |
|---------|--------|--------|------|
| `AeronCluster` | `AeronCluster` | P0 | 主客户端入口 |
| `AeronCluster.Context` | `ClusterContext` | P0 | 配置上下文 |
| `EgressPoller` | `EgressPoller` | P0 | Egress 消息轮询 |
| `EgressListener` | `EgressListener` | P0 | Egress 监听器接口 |
| `EgressAdapter` | `EgressAdapter` | P1 | Egress 适配器 |
| `ControlledEgressAdapter` | `ControlledEgressAdapter` | P1 | 受控 Egress 适配器 |
| `ClusterException` | `ClusterException` | P0 | Cluster 异常 |

#### 2.2 关键功能实现

**AeronCluster.h**:
- 连接管理（`connect()`, `close()`）
- 会话管理（`clusterSessionId()`, `leadershipTermId()`）
- 消息发送（`offer()`, `tryClaim()`）
- 状态查询（`isClosed()`, `isConnected()`）

**EgressPoller.h**:
- 消息轮询（`poll()`）
- 事件处理（`newLeaderEvent()`, `sessionEvent()`）
- Fragment 处理

**ClusterContext.h**:
- 配置项（ingress channels, egress channel, 超时等）
- 异常处理器
- 认证相关

#### 2.3 Codec 处理

**方案**：使用 SBE 官方工具从 XML schema 生成 C++ codecs（唯一方案）

**需要处理的 XML Schema 文件**（共 3 个）：
1. `aeron-cluster/src/main/resources/cluster/aeron-cluster-codecs.xml` - 主协议消息（30+ 个消息类型）
2. `aeron-cluster/src/main/resources/cluster/aeron-cluster-mark-codecs.xml` - Mark 文件
3. `aeron-cluster/src/main/resources/cluster/aeron-cluster-node-state-codecs.xml` - 节点状态

**实施步骤**：
1. 配置 SBE 工具（参考 `aeron-archive/build.gradle` 中的 `generateCppCodecs` 任务）
2. 在 CMakeLists.txt 中添加自定义命令，构建时自动生成三个 XML 的 C++ codecs
3. 将生成的 codecs 集成到项目构建系统
4. 验证生成的 codecs 与 Java 版本二进制兼容

**关键 Codecs**（从 `aeron-cluster-codecs.xml` 生成）:
- `SessionMessageHeaderEncoder/Decoder`, `SessionEventDecoder`, `NewLeaderEventDecoder`
- `AdminRequestEncoder/Decoder`, `SessionConnectRequestEncoder` 等

### 阶段 3: 服务端 API 移植（可选）

服务端功能更复杂，建议在客户端稳定后再进行：

#### 3.1 核心类映射

| Java 类 | C++ 类 | 优先级 | 说明 |
|---------|--------|--------|------|
| `ConsensusModule` | `ConsensusModule` | P2 | 共识模块 |
| `ClusteredService` | `ClusteredService` | P2 | 集群服务接口 |
| `ClusteredServiceContainer` | `ClusteredServiceContainer` | P2 | 服务容器 |
| `Cluster` | `Cluster` | P2 | 集群主类 |

### 阶段 4: 测试移植（1:1 翻译）

**测试框架**: 使用 **GoogleTest (googletest/gmock)**，与 `aeron-client` 保持一致

#### 4.1 单元测试

创建 `aeron-cluster/src/test/cpp_wrapper/` 目录，参考 `aeron-client/src/test/cpp_wrapper/` 的测试结构。

**测试框架配置**:
- 使用 `gtest/gtest.h` 和 `gmock/gmock.h`
- 链接 `gmock_main` 库
- 使用 `testing::Test` 作为测试基类
- 使用 `TEST_F` 宏定义测试用例

**关键测试**（1:1 翻译 Java 测试）:
- `AeronClusterTest.cpp`: 基本连接和消息发送（对应 Java `AeronClusterTest.java`）
- `EgressPollerTest.cpp`: Egress 消息轮询（对应 Java `EgressPollerTest.java`）
- `ClusterContextTest.cpp`: 配置测试（对应 Java `AeronClusterContextTest.java`）
- `AuthenticationTest.cpp`: 认证测试（对应 Java `AuthenticationTest.java`）
- 其他 Java 测试的对应 C++ 版本

**CMakeLists.txt 配置**:
- 参考 `aeron-client/src/test/cpp_wrapper/CMakeLists.txt`
- 使用 `aeron_cluster_wrapper_test()` 函数创建测试
- 链接 `aeron_cluster_wrapper`, `aeron_driver`, `gmock_main`

#### 4.2 集成测试

- 与 Java 版本的互操作性测试
- 性能对比测试
- 端到端集群功能测试

## 实施步骤

### Step 1: 环境准备
- [ ] 确认编译选项（RTTI、异常等）
- [ ] 创建目录结构
- [ ] 创建基础 CMakeLists.txt

### Step 2: 基础框架
- [ ] 实现 `ClusterExceptions.h`
- [ ] 实现 `ClusterContext.h`
- [ ] 创建 `AeronCluster.h` 骨架

### Step 3: Codec 处理
- [ ] 配置 SBE 工具（下载或使用 Gradle 依赖）
- [ ] 创建 CMake 自定义命令生成 C++ codecs（三个 XML 文件）
- [ ] 验证生成的 codecs 与 Java 版本兼容
- [ ] 集成生成的 codecs 到项目构建系统

### Step 4: 核心功能
- [ ] 实现 `AeronCluster::connect()`
- [ ] 实现 `AeronCluster::offer()`
- [ ] 实现 `EgressPoller::poll()`
- [ ] 实现基本消息发送/接收

### Step 5: 完善功能
- [ ] 实现会话管理
- [ ] 实现 Leader 变更处理
- [ ] 实现超时和重试机制
- [ ] 实现认证支持

### Step 6: 测试和文档
- [ ] 创建测试目录结构 `aeron-cluster/src/test/cpp_wrapper/`
- [ ] 配置测试 CMakeLists.txt（使用 googletest，参考 `aeron-client/src/test/cpp_wrapper/CMakeLists.txt`）
- [ ] 1:1 翻译 Java 单元测试到 C++（使用 googletest/gmock）
- [ ] 编写集成测试
- [ ] 编写示例代码
- [ ] 更新文档（英文）

## 技术难点和解决方案

### 1. Codec 生成

**问题**: Java 使用 SBE 生成的 codecs，C++ 需要对应的实现。

**解决方案**: 使用 SBE 官方 C++ 工具从三个 XML schema 文件生成 codecs
- 参考 `aeron-archive/build.gradle` 中的 `generateCppCodecs` 任务
- 在 CMakeLists.txt 中添加自定义命令，构建时自动生成
- 确保与 Java 版本二进制兼容

### 2. 异步操作

**问题**: Java 版本使用回调、Future 等异步机制。

**解决方案**:
- C++ 版本使用 `std::function` 回调
- 对于需要等待的操作，提供同步和异步两种接口

### 3. 内存管理

**问题**: Java 自动内存管理，C++ 需要手动管理。

**解决方案**:
- 使用 RAII 原则
- 智能指针（`std::shared_ptr`, `std::unique_ptr`）
- 参考 `aeron-client` 的内存管理方式

### 4. 线程安全

**问题**: Java 版本的线程安全模型需要移植。

**解决方案**:
- 参考 Java 版本的线程安全注释
- 使用 `std::mutex` 等标准库工具
- 遵循 `aeron-client` 的线程安全模式

## 代码示例

### 基本使用（目标 API）

```cpp
#include "client/cluster/AeronCluster.h"

// 创建上下文
aeron::cluster::client::ClusterContext context;
context.ingressEndpoints("localhost:9010,localhost:9011,localhost:9012")
       .egressChannel("aeron:udp?endpoint=localhost:0");

// 连接
auto cluster = aeron::cluster::client::AeronCluster::connect(context);

// 发送消息
std::vector<std::uint8_t> message = {1, 2, 3, 4};
long result = cluster->offer(message.data(), message.size());

// 轮询 egress
aeron::cluster::client::EgressPoller poller(*cluster);
while (poller.poll() > 0)
{
    // 处理消息
}

// 关闭
cluster->close();
```

## 参考资源

1. **aeron-client cpp_wrapper**: `aeron-client/src/main/cpp_wrapper/`
2. **aeron-archive cpp_wrapper**: `aeron-archive/src/main/cpp_wrapper/`
3. **Java 实现**: `aeron-cluster/src/main/java/io/aeron/cluster/`
4. **Aeron Wiki**: https://github.com/aeron-io/aeron/wiki
5. **SBE (Simple Binary Encoding)**: 
   - 官方仓库: https://github.com/aeron-io/simple-binary-encoding
   - 支持 C++ 代码生成，用于从 XML schema 生成 codecs

## 时间估算

- **阶段 1（基础设施）**: 1-2 周
- **阶段 2（客户端 API）**: 4-6 周
- **阶段 3（服务端 API）**: 6-8 周（可选）
- **阶段 4（测试）**: 2-3 周

**总计**: 约 3-4 个月（仅客户端），6-8 个月（包含服务端）

## 注意事项

1. **保持一致性**: 严格遵循 `aeron-client` 的代码风格和组织方式
2. **性能优先**: 确保 C++ 版本性能不劣于 Java 版本
3. **向后兼容**: API 设计时考虑未来扩展
4. **文档完善**: 及时更新文档和示例代码
5. **测试覆盖**: 确保关键功能有充分的测试覆盖
6. **语言规范**: 
   - **代码注释必须使用英文**（与 `aeron-client` 保持一致）
   - **所有技术文档、API 文档、README 等必须使用英文**
   - 仅此移植计划文档（CPP_PORT_PLAN.md）使用中文

## 更新日志

- **2026-01-04**: 初始版本，制定移植计划

