# YCSB TiKV 测试工具

YCSB (Yahoo! Cloud System Benchmark) 是一个用于测试云数据库性能的基准测试工具。本项目是YCSB的TiKV绑定版本，支持测试TiKV的性能测试

## 项目结构

```
tikv/                        # YCSB核心模块
├── src/                        # tikv
├── workloads/                  # 工作负载配置文件
├── build.sh                    # 编译打包脚本
├── run_fast_test.sh            # 运行测试脚本
└── build/                      # 构建输出目录
    └── obhbase-1.0-SNAPSHOT-jar-with-dependencies.jar
```

## 快速开始

### 1. 环境要求

- **Java**: JDK 1.7 或更高版本
- **Maven**: 3.x 版本
- **操作系统**: Linux/macOS/Windows

### 2. 编译打包

```bash
# 编译打包项目
./build.sh

# 仅清理构建产物
./build.sh clean
```

### 3. 配置workload

在使用前，需要配置workload文件中的OceanBase连接参数。编辑对应的workload文件（如`workloads/workload_scan`）：
- scan: workloads/workload_scan

```bash
# 编辑workload文件
vim workloads/workload_scan
```

#### Tikv连接参数
```
# 指定连接的pd地址
tikv.pd.addr=ip1,ip2,ip3
```

##### YCSB测试通用参数

| 参数名 | 说明 | 默认值 |
|--------|------|--------|
| `operationcount` | 操作总数 | - |
| `recordcount` | 记录总数 | - |
| `requestdistribution` | 请求分布模式 | uniform |

注意：没有指定insertstart和insertcount的情况下
- load测试载入数据的起点是从0开始

### 4. 快速运行测试
#### 4.1. 进行scan测试
```bash
# 先导入数据，这一步会交互式地让用户选择load哪种测试的数据（read/scan/batchread），会分别对应取读对应的workload文件
./run_fast_test.sh load

# 如果想使用自己自定义的workload文件，可以显式指定
./run_fast_test.sh load workloads/my_workload

# 扫描数据
./run_fast_test.sh scan
```
#### 4.2. 进行put测试
```bash
# 运行put测试
./run_fast_test.sh put
```
#### 4.3. 进行batch_put测试
```bash
# 运行batch_put测试
./run_fast_test.sh batch_put
```
#### 4.4. 进行read测试
```bash
# 同上，需要先load数据
./run_fast_test.sh load

# 运行
./run_fast_test.sh read
```
#### 4.5. 进行batch_read测试
```bash
# 同上，需要先load数据
./run_fast_test.sh load

# 运行
./run_fast_test.sh batch_read
```

#### 4.6. 进行自定义workload的测试
```bash
# 1. 自定义一个worload文件，位置在/path/to/custom/workload
# 1.1 （可选）如果需要提前载入数据，使用这个文件进行数据加载
./run_fast_test.sh load /path/to/custom/workload
# 2. 运行这个测试
./run_fast_test.sh worload /path/to/custom/workload
```