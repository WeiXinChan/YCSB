# YCSB OBHBase 测试工具

YCSB (Yahoo! Cloud System Benchmark) 是一个用于测试云数据库性能的基准测试工具。本项目是YCSB的OBHBase绑定版本，支持测试OceanBase HBase模式和竞品原生Hbase、Lindorm宽表引擎的性能。

## 更新说明

| 版本 | 日期 | 主要更新内容 |
|------|------|-------------|
| v1.2.0 | 2025-08-29 | 1. 新增原生HBase连接模式支持，兼容Lindorm等HBase兼容产品 <br> 2. 增加网络连接测试和ZooKeeper连接测试功能 <br> 3. 改进错误处理和调试信息输出 <br> 4. 支持两种连接模式：OBKV模式和标准HBase模式 <br> 5. 优化HBase超时配置，防止连接卡死 |
| v1.1.0 | 2025-08-22 | 1. 增加batch put和batch get测试接口及相应的配置项 <br> 2. 更新run_fast_test.sh脚本 |
| v1.1.0 | 2025-08-11 | 1. 修复预建分区表脚本生成的range分区范围有误，同时支持设置前缀'0'填充长度<br> 2. key生成算法优化，使测试过程中的数据分布更均衡<br>3. 生成的key前缀'0'填充长度从1位调整为12位<br> |
| v1.0.0 | 初始版本 | 1. 基于ycsb实现ob-hbase的put/read/scan性能测试<br>2. 提供支持快速测试的编译、运行脚本<br>3. 支持ODP和直连两种连接模式<br>4. 支持预分区表创建和测试 |

## 项目结构

```
YCSB/
├── core/                       # YCSB核心模块
├── obhbase/                    # OBHBase绑定模块
├── workloads/                  # 工作负载配置文件
├── build.sh                    # 编译打包脚本
├── run.sh                      # 运行测试脚本
|—— create_partitioned_table.sh # 创建预分区表脚本
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

#### 支持测试的产品类型

##### 1. OBKV模式（OceanBase专用）
- 设置 `isObkv=true`
- 使用OceanBase特有的连接参数
- 适用于测试OceanBase数据库

###### OceanBase连接参数（OBKV模式必填

| 参数名 | 说明 | 必填 |
|--------|------|------|
| `hbase.oceanbase.odpMode` | 连接模式选择 | 是 |
| `hbase.oceanbase.odpAddr` | ODP代理地址 | ODP模式必填 |
| `hbase.oceanbase.odpPort` | ODP代理端口 | ODP模式必填 |
| `hbase.oceanbase.paramURL` | 直连模式连接URL | 直连模式必填 |
| `hbase.oceanbase.sysUserName` | 系统租户用户名 | 直连模式必填 |
| `hbase.oceanbase.sysPassword` | 系统租户密码 | 直连模式必填 |
| `hbase.oceanbase.fullUserName` | 业务租户用户名 | 是 |
| `hbase.oceanbase.password` | 业务租户密码 | 是 |
| `hbase.oceanbase.database` | 数据库名 | 是 |
| `hbase.oceanbase.table` | 表名 | 是 |
| `hbase.oceanbase.columnFamily` | 列族名 | 是 |
**注意：** 其他客户端参数设置，可以参考obkv-hbase-client和obkv-table-client支持的参数设置

##### 2. 原生HBase模式
- 设置 `isObkv=false`
- 使用标准HBase连接参数
- 适用于测试Lindorm、原生HBase等兼容产品

###### 原生HBase连接参数（原生HBase模式必填）

| 参数名 | 说明 | 默认值 | 必填 |
|--------|------|--------|------|
| `hbase.zookeeper.quorum` | ZooKeeper集群地址 | 127.0.0.1 | 是 |
| `hbase.zookeeper.property.clientPort` | ZooKeeper端口 | 2181 | 是 |
| `hbase.client.username` | HBase用户名 | root | 否 |
| `hbase.client.password` | HBase密码 | root | 否 |
| `hbase.client.operation.timeout` | 操作超时时间(ms) | 10000 | 否 |
| `hbase.client.scanner.timeout.period` | 扫描超时时间(ms) | 10000 | 否 |
| `hbase.rpc.timeout` | RPC超时时间(ms) | 10000 | 否 |
| `hbase.client.retries.number` | 重试次数 | 1 | 否 |
| `hbase.client.pause` | 重试间隔(ms) | 100 | 否 |

##### YCSB测试通用参数

| 参数名 | 说明 | 默认值 |
|--------|------|--------|
| `operationcount` | 操作总数 | - |
| `recordcount` | 记录总数 | - |
| `requestdistribution` | 请求分布模式 | uniform |

注意：没有指定insertstart和insertcount的情况下
- load测试载入数据的起点是从0开始

### 4. 配置示例

#### 4.1 OBKV模式配置示例

适用于OceanBase数据库的配置：

```properties
# 连接模式
isObkv=true
debug=false

# 直连模式配置
hbase.oceanbase.odpMode=false
hbase.oceanbase.paramURL=
hbase.oceanbase.sysUserName=
hbase.oceanbase.sysPassword=
hbase.oceanbase.fullUserName=
hbase.oceanbase.password=

# 表配置
hbase.oceanbase.database=test
hbase.oceanbase.table=test
hbase.oceanbase.columnFamily=cf
```

#### 4.2 原生HBase模式配置示例

适用于Lindorm、原生HBase等兼容产品的配置：

```properties
# 连接模式
isObkv=false
debug=false

# HBase连接配置
hbase.zookeeper.quorum=
hbase.zookeeper.property.clientPort=
hbase.client.username=
hbase.client.password=

# 超时配置
hbase.client.operation.timeout=10000
hbase.client.scanner.timeout.period=10000
hbase.rpc.timeout=10000
hbase.client.retries.number=1
hbase.client.pause=100

# 表配置（使用原生HBase参数）
hbase.oceanbase.database=test
hbase.oceanbase.table=test
hbase.oceanbase.columnFamily=cf
```

### 5. 预建表（仅针对OBKV模式）
#### 使用 create_partitioned_table.sh 脚本生成建表语句

**脚本参数说明：**
```bash
# 语法：./create_partitioned_table.sh <记录总数> <分区数> [零填充长度]
# 示例：记录数 3 千万, 分区数 20 个, 零填充长度 12 位
./create_partitioned_table.sh 30000000 20 12
```

**重要更新（v1.1.0）：**
- 修复了分区范围计算错误，确保数据分布更均衡
- 新增零填充长度参数，支持自定义key前缀的零填充位数
- 默认零填充长度从1位调整为12位，与YCSB配置保持一致
（**注意：** YCSB通过zeropadding配置项控制生成数据前缀'0'的填充长度，如果预建表脚本显式指定了零填充长度，需要在测试时同步需要zeropadding的值）
- 这一步中会根据要测试的数据量生成对应的range分区表，为了测试中压力均匀分布，后面的各类测试会基于这个数据量生成压力（**注意：**  如果后面测试的recordcount与当前建表的不一致，建议重新建表，避免测试压力不均匀）


#### 在 OceanBase 数据库中执行上述脚本生成的建表语句
```bash
CREATE TABLE test$family (
    K varbinary(1024) NOT NULL,
    Q varbinary(256) NOT NULL,
    T bigint(20) NOT NULL,
    V varbinary(1024) DEFAULT NULL,
    PRIMARY KEY (K, Q, T)
)
PARTITION BY RANGE COLUMNS(K) (
    PARTITION p0 VALUES LESS THAN ('00000001500000'),
    PARTITION p1 VALUES LESS THAN ('00000003000000'),
    PARTITION p2 VALUES LESS THAN ('00000004500000'),
    PARTITION p3 VALUES LESS THAN ('00000006000000'),
    PARTITION p4 VALUES LESS THAN ('00000007500000'),
    PARTITION p5 VALUES LESS THAN ('00000009000000'),
    PARTITION p6 VALUES LESS THAN ('00000010500000'),
    PARTITION p7 VALUES LESS THAN ('00000012000000'),
    PARTITION p8 VALUES LESS THAN ('00000013500000'),
    PARTITION p9 VALUES LESS THAN ('00000015000000'),
    PARTITION p10 VALUES LESS THAN ('00000016500000'),
    PARTITION p11 VALUES LESS THAN ('00000018000000'),
    PARTITION p12 VALUES LESS THAN ('00000019500000'),
    PARTITION p13 VALUES LESS THAN ('00000021000000'),
    PARTITION p14 VALUES LESS THAN ('00000022500000'),
    PARTITION p15 VALUES LESS THAN ('00000024000000'),
    PARTITION p16 VALUES LESS THAN ('00000025500000'),
    PARTITION p17 VALUES LESS THAN ('00000027000000'),
    PARTITION p18 VALUES LESS THAN ('00000028500000'),
    PARTITION p19 VALUES LESS THAN (MAXVALUE)
);
```

**分区范围说明：**
- 分区边界值现在包含12位零填充前缀（如：'00000001500000'）
- 与YCSB生成的key格式完全匹配，确保数据分布均衡
- 支持更大的数据量测试，避免分区倾斜问题

### 6. 快速运行测试
#### 6.1. 进行scan测试
```bash
# 先导入数据，这一步会交互式地让用户选择load哪种测试的数据（read/scan/batchread），会分别对应取读对应的workload文件
./run_fast_test.sh load

# 如果想使用自己自定义的workload文件，可以显式指定
./run_fast_test.sh load workloads/my_workload

# 确定每个分区的key数量是否均衡
select count(1) from test$family partition(p0);
select count(1) from test$family partition(p1);
select count(1) from test$family partition(p2);

# 扫描数据
./run_fast_test.sh scan
```
#### 6.2. 进行put测试
```bash
# 运行put测试
./run_fast_test.sh put
```
#### 6.3. 进行batch_put测试
```bash
# 运行batch_put测试
./run_fast_test.sh batch_put
```
#### 6.4. 进行read测试
```bash
# 同上，需要先load数据
./run_fast_test.sh load

# 运行
./run_fast_test.sh read
```
#### 6.5. 进行batch_read测试
```bash
# 同上，需要先load数据
./run_fast_test.sh load

# 运行
./run_fast_test.sh batch_read
```

#### 6.6. 进行自定义workload的测试
```bash
# 1. 自定义一个worload文件，位置在/path/to/custom/workload
# 1.1 （可选）如果需要提前载入数据，使用这个文件进行数据加载
./run_fast_test.sh load /path/to/custom/workload
# 2. 运行这个测试
./run_fast_test.sh worload /path/to/custom/workload
```