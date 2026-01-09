# 运行 Cluster 测试

## 前置条件

设置 Java 环境变量：

```bash
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export BUILD_JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export BUILD_JAVA_VERSION=17
```

## 构建项目

### Debug 模式（推荐，包含调试符号）

```bash
# 1. 进入构建目录
cd /data/aeron/build

# 2. 清理 CMake 缓存（重要！）
rm -f CMakeCache.txt

# 3. 配置 CMake（Debug 模式，包含调试符号）
cmake .. -DCMAKE_BUILD_TYPE=Debug \
         -DBUILD_AERON_CLUSTER_API=ON \
         -DAERON_TESTS=ON \
         -DAERON_UNIT_TESTS=ON

# 4. 编译
make -j$(nproc)
```

### Release 模式（生产环境）

```bash
cd /data/aeron/build
rm -f CMakeCache.txt
cmake .. -DCMAKE_BUILD_TYPE=Release \
         -DBUILD_AERON_CLUSTER_API=ON \
         -DAERON_TESTS=ON \
         -DAERON_UNIT_TESTS=ON
make -j$(nproc)
```

## 运行测试

### 运行所有 Cluster 测试

```bash
cd /data/aeron/build
ctest -R egressPollerTestW -R aeronClusterContextTestW -R aeronClusterTestW -R egressAdapterTestW -R aeronClusterAsyncConnectTestW --output-on-failure
```

### 单独运行每个测试

```bash
cd /data/aeron/build

# 测试 1: EgressPoller
ctest -R egressPollerTestW --output-on-failure

# 测试 2: AeronClusterContext
ctest -R aeronClusterContextTestW --output-on-failure

# 测试 3: AeronCluster
ctest -R aeronClusterTestW --output-on-failure

# 测试 4: EgressAdapter
ctest -R egressAdapterTestW --output-on-failure

# 测试 5: AeronClusterAsyncConnect
ctest -R aeronClusterAsyncConnectTestW --output-on-failure
```

## 调试段错误

如果测试出现段错误（SegFault），可以使用 gdb 调试：

```bash
# 在 build 目录下直接运行（无需切换目录）
cd /data/aeron/build
gdb --args ./binaries/aeronClusterTestW

# 在 gdb 中：
#   - 输入 run 运行程序
#   - 崩溃后输入 bt 查看完整堆栈跟踪
#   - 输入 quit 退出
```

### 使用 Core Dump 调试

```bash
# 1. 启用 core dump
ulimit -c unlimited

# 2. 运行测试（会生成 core 文件）
cd /data/aeron/build
./binaries/aeronClusterTestW

# 3. 使用 gdb 分析 core dump
cd /data/aeron/build
gdb ./binaries/aeronClusterTestW core
# 在 gdb 中输入 bt 查看堆栈
```




