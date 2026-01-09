# 运行 Cluster 测试

## 运行所有 5 个 Cluster 测试

```bash

export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export BUILD_JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export BUILD_JAVA_VERSION=17

# 2. 清理 CMake 缓存（重要！）
cd /data/aeron/build
rm -f CMakeCache.txt

# 3. 重新配置 CMake（让 CMake 捕获新的环境变量）
cmake .. -DCMAKE_BUILD_TYPE=Release \
         -DBUILD_AERON_CLUSTER_API=ON \
         -DAERON_TESTS=ON \
         -DAERON_UNIT_TESTS=ON

         make -j$(nproc)

cd /data/aeron/build
ctest -R egressPollerTestW -R aeronClusterContextTestW -R aeronClusterTestW -R egressAdapterTestW -R aeronClusterAsyncConnectTestW --output-on-failure
```




